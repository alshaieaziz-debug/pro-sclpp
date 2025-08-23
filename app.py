# app.py — Binance USDT-M Scalper (MARKET entry + on-exchange SL/TP, fee-aware PnL)
# ✅ Supports both Original and Router paths (switch in .env CONSENSUS_ROUTER=True/False)
# ✅ Market entry with fill polling
# ✅ Fee-aware PnL (taker/maker % + slippage cushion)
# ✅ Configurable TP1/TP2/TS widening
# ✅ Maker trailing with panic fallback
# ✅ Telegram alerts, daily stats, health server

import sys, subprocess, os
def _ensure(pkgs):
    import importlib
    miss=[]
    for p in pkgs:
        base=p.split("==")[0].split(">=")[0].split("[")[0].replace("-","_")
        try: importlib.import_module(base)
        except Exception: miss.append(p)
    if miss:
        print("Installing:", miss, flush=True)
        subprocess.check_call([sys.executable,"-m","pip","install","--upgrade",*miss])
_ensure([
    "aiohttp>=3.9.5","uvloop>=0.19.0","pydantic>=2.7.0","pydantic-settings>=2.2.1",
    "structlog>=24.1.0","tzdata>=2024.1","orjson>=3.10.7",
])

import asyncio, aiohttp, orjson, time, math, hmac, hashlib, urllib.parse, random
from typing import List, Dict, Any, Optional, Tuple, Deque
from datetime import datetime, timedelta, timezone
from collections import deque, defaultdict
from zoneinfo import ZoneInfo
import structlog, uvloop
from aiohttp import web
from pydantic_settings import BaseSettings, SettingsConfigDict

# ---------------- Settings ----------------
class Settings(BaseSettings):
    SYMBOLS: str | List[str] = "ALL"
    TIMEFRAMES: List[str] = ["5m","15m"]
    MAX_SYMBOLS: int = 100
    WS_CHUNK_SIZE: int = 50
    BACKFILL_LIMIT: int = 60

    # -------- Risk & targets (Original path) --------
    ATR_LEN: int = 14
    ATR_SL_MULT: float = 1.8
    TP1_MULT: float = 1.5
    TP2_MULT: float = 3.0
    TRAIL_ATR_MULT: float = 1.8

    # -------- Router (percent not bps) --------
    CONSENSUS_ROUTER: bool = False
    ROUTER_SL_PCT: float = 0.25
    ROUTER_TP1_PCT: float = 0.80
    ROUTER_TP2_PCT: float | None = 1.60
    TRAIL_ATR1M_MULT: float = 1.8

    # Fees/slippage (percent)
    FEE_TAKER_PCT: float = 0.04
    FEE_MAKER_PCT: float = 0.02
    SLIPPAGE_PCT: float = 0.01

    # Maker trailing
    TRAIL_MAKER: bool = True
    TRAIL_REPRICE_GAP_PCT: float = 0.01
    TRAIL_PANIC_PCT: float = 0.05
    TRAIL_PANIC_TIMEOUT_SEC: int = 2

    DRY_RUN: bool = True
    MAX_RISK_PCT: float = 1.0
    ACCOUNT_EQUITY_USDT: Optional[float] = None

    TELEGRAM_BOT_TOKEN: Optional[str] = None
    TELEGRAM_CHAT_ID: Optional[str] = None

    HOST: str = "0.0.0.0"
    PORT: int = 8080
    TZ: str = "Asia/Riyadh"

    BINANCE_API_KEY: Optional[str] = os.getenv("BINANCE_API_KEY")
    BINANCE_API_SECRET: Optional[str] = os.getenv("BINANCE_API_SECRET")
    POSITION_MODE: str = os.getenv("POSITION_MODE", "ONE_WAY")
    LEVERAGE: int = int(os.getenv("LEVERAGE", "5"))

    model_config = SettingsConfigDict(env_file=".env", case_sensitive=False)

S = Settings()

# ---------------- Utils ----------------
def now_utc(): return datetime.now(timezone.utc)
def to_tz(dt, tz): return dt.astimezone(ZoneInfo(tz))
def riyadh_now(): return to_tz(now_utc(), S.TZ)
def pct_to_mult(p): return p/100.0
def fee_cushion_pct(exit_kind:str)->float:
    exit_fee = S.FEE_MAKER_PCT if exit_kind=="maker" else S.FEE_TAKER_PCT
    return S.FEE_TAKER_PCT + exit_fee + S.SLIPPAGE_PCT
def be_with_fees(entry:float, side:str, exit_kind:str)->float:
    c = pct_to_mult(fee_cushion_pct(exit_kind))
    return entry*(1+c) if side=="LONG" else entry*(1-c)

# ---------------- Binance REST/WS client ----------------
BASE_REST = "https://fapi.binance.com"
BASE_WS   = "wss://fstream.binance.com/stream"

class BinanceClient:
    def __init__(self, session:aiohttp.ClientSession):
        self.s = session
        self.key = S.BINANCE_API_KEY
        self.secret = (S.BINANCE_API_SECRET or "").encode()

    async def _signed(self, method:str, path:str, params:dict|None=None):
        if params is None: params={}
        params["timestamp"]=int(time.time()*1000)
        q=urllib.parse.urlencode(params, doseq=True)
        sig=hmac.new(self.secret, q.encode(), hashlib.sha256).hexdigest()
        headers={"X-MBX-APIKEY":self.key} if self.key else {}
        url=f"{BASE_REST}{path}?{q}&signature={sig}"
        async with self.s.request(method,url,headers=headers) as r:
            txt=await r.text()
            if r.status!=200: raise RuntimeError(f"Binance {method} {path} {r.status}: {txt}")
            return orjson.loads(txt)

    async def place_order(self,symbol,side,type_,**kwargs):
        return await self._signed("POST","/fapi/v1/order",{"symbol":symbol,"side":side,"type":type_,**kwargs})
    async def cancel_all(self,symbol):
        return await self._signed("DELETE","/fapi/v1/allOpenOrders",{"symbol":symbol})
    async def get_order(self,symbol,orderId):
        return await self._signed("GET","/fapi/v1/order",{"symbol":symbol,"orderId":orderId})
    async def get_position_risk(self,symbol):
        return await self._signed("GET","/fapi/v2/positionRisk",{"symbol":symbol})
    async def get_balance_usdt(self):
        d=await self._signed("GET","/fapi/v2/balance",{})
        for it in d:
            if it.get("asset")=="USDT": return float(it.get("balance",0))
        return None
# ---------------- TA helpers ----------------
from typing import Deque, NamedTuple

def body_ratio(o,h,l,c):
    rng = max(h-l, 1e-12)
    return abs(c-o)/rng

def sma(vals: List[float], n:int)->Optional[float]:
    if n<=0 or len(vals)<n: return None
    return sum(vals[-n:])/n

def atr(highs: List[float], lows: List[float], closes: List[float], length: int) -> Optional[float]:
    if len(closes) < length + 1: return None
    trs=[]
    for i in range(1,len(closes)):
        trs.append(max(highs[i]-lows[i], abs(highs[i]-closes[i-1]), abs(lows[i]-closes[i-1])))
    if len(trs) < length: return None
    return sum(trs[-length:]) / length

def rsi(closes: List[float], length:int=14)->Optional[float]:
    if len(closes) < length+1: return None
    gains=0.0; losses=0.0
    for i in range(-length,0):
        diff=closes[i]-closes[i-1]
        if diff>=0: gains+=diff
        else: losses-=diff
    avg_gain=gains/length; avg_loss=losses/length
    if avg_loss==0: return 100.0
    rs=avg_gain/avg_loss
    return 100.0 - (100.0/(1.0+rs))

def fmt_pct(a: float, b: float) -> float:
    if b == 0: return 0.0
    return (a/b - 1.0) * 100.0

# ---------------- Data classes ----------------
class Candle:
    __slots__=("open_time","open","high","low","close","volume","close_time")
    def __init__(self, ot,o,h,l,c,v,ct):
        self.open_time=ot; self.open=o; self.high=h; self.low=l; self.close=c; self.volume=v; self.close_time=ct

class BookTop:
    __slots__=("bid_price","bid_qty","ask_price","ask_qty")
    def __init__(self, bp,bq,ap,aq): self.bid_price=bp; self.bid_qty=bq; self.ask_price=ap; self.ask_qty=aq

class Signal:
    def __init__(self, **kw): self.__dict__.update(kw)

# ---------------- Trackers ----------------
class OrderBookTracker:
    def __init__(self): self.tops:Dict[str,BookTop]={}
    def update_book_ticker(self, sym:str, d:dict):
        self.tops[sym.upper()] = BookTop(float(d.get("b",0)),float(d.get("B",0)),float(d.get("a",0)),float(d.get("A",0)))
    def top(self, sym:str)->Optional[BookTop]: return self.tops.get(sym.upper())

class CVDTracker:
    """CVD from aggTrade: buyer_is_maker means sell-side aggressor (negative)."""
    def __init__(self, window_sec:int=300):
        self.window_sec=window_sec
        self.buff:Dict[str,Deque[Tuple[float,float]]] = defaultdict(lambda: deque())
    def on_agg(self, sym:str, price:float, qty:float, buyer_is_maker:bool):
        sign = -1.0 if buyer_is_maker else +1.0
        ts = time.time()
        dq = sign * qty
        q = self.buff[sym]; q.append((ts, dq))
        cutoff = ts - self.window_sec
        while q and q[0][0] < cutoff: q.popleft()
    def slope(self, sym:str)->float:
        q=self.buff.get(sym)
        if not q or len(q)<2: return 0.0
        total = sum(d for _,d in q)
        span = q[-1][0]-q[0][0]
        return total / max(span, 1e-6)

class DepthTracker:
    """Detect opposite walls near breakout band using per-side snapshot medians."""
    def __init__(self):
        self.last_depth: Dict[str, Dict[str, List[Tuple[float,float]]]] = {}
    def on_depth5(self, sym:str, data:dict):
        bids = [(float(p), float(q)) for p,q in data.get("b", [])]
        asks = [(float(p), float(q)) for p,q in data.get("a", [])]
        self.last_depth[sym]= {"bids": bids, "asks": asks}
    @staticmethod
    def _median(vals: List[float]) -> float:
        if not vals: return 0.0
        s=sorted(vals); n=len(s)
        return s[n//2] if n%2==1 else 0.5*(s[n//2-1]+s[n//2])
    def has_opposite_wall(self, sym:str, side:str, ref_price:float, band_bp:int, mult:float)->bool:
        d = self.last_depth.get(sym)
        if not d or ref_price<=0: return False
        band = ref_price*(band_bp/10_000)
        if side=="LONG":
            window = [q for p,q in d["asks"] if ref_price < p <= ref_price+band]
            med = self._median([q for _,q in d["asks"]])
            wall_sz = sum(window)
            return med>0 and wall_sz > mult*med
        else:
            window = [q for p,q in d["bids"] if ref_price-band <= p < ref_price]
            med = self._median([q for _,q in d["bids"]])
            wall_sz = sum(window)
            return med>0 and wall_sz > mult*med

# ---------------- Breakout Engine ----------------
class RetestState:
    def __init__(self, level:float, direction:str, expire_bars:int, pad_bp:int):
        self.level=level; self.direction=direction; self.remaining=expire_bars
        self.touched=False; self.pad_bp=pad_bp; self.await_next_close=False
    def step(self): self.remaining-=1; return self.remaining>0
    def in_band(self, price:float)->bool:
        band=self.level*(self.pad_bp/10_000); return abs(price-self.level)<=band

class BreakoutEngine:
    def __init__(self, settings: Settings):
        self.s=settings
        self.buffers:Dict[Tuple[str,str],Dict[str,Deque[float]]]={}
        self.retests:Dict[Tuple[str,str],List[RetestState]]={}
    def _buf(self, sym, tf):
        k=(sym,tf)
        if k not in self.buffers:
            self.buffers[k]={"o":deque(maxlen=600),"h":deque(maxlen=600),"l":deque(maxlen=600),"c":deque(maxlen=600),"v":deque(maxlen=600)}
        return self.buffers[k]
    def _ret(self, sym, tf):
        k=(sym,tf)
        if k not in self.retests: self.retests[k]=[]
        return self.retests[k]
    def add_candle(self, sym, tf, cndl:Candle):
        b=self._buf(sym,tf)
        b["o"].append(cndl.open); b["h"].append(cndl.high); b["l"].append(cndl.low); b["c"].append(cndl.close); b["v"].append(cndl.volume)
        for r in list(self._ret(sym,tf)): r.step()
        self.retests[(sym,tf)]=[r for r in self._ret(sym,tf) if r.remaining>0]
    def _prior_high_low(self, hs, ls, N=20):
        if len(hs)<N+1: return None,None
        return max(list(hs)[-N-1:-1]), min(list(ls)[-N-1:-1])
    def _sweep_reclaim(self, hs, ls, os, cs, side:str)->bool:
        if len(hs)<self.s.SWEEP_LOOKBACK+1 if hasattr(self.s,'SWEEP_LOOKBACK') else 16: 
            return False
        look = getattr(self.s,'SWEEP_LOOKBACK',15)
        buf = getattr(self.s,'RECLAIM_BUFFER_BP',6)/10_000
        min_body = getattr(self.s,'RECLAIM_BODY_RATIO_MIN',0.55)
        prev_high=max(list(hs)[-look-1:-1]); prev_low=min(list(ls)[-look-1:-1])
        c, h, l, o = cs[-1], hs[-1], ls[-1], os[-1]
        br = body_ratio(o,h,l,c)
        if side=="LONG":
            swept=h>prev_high; back_inside=c<=prev_high*(1+buf)
            return swept and back_inside and (br>=min_body)
        else:
            swept=l<prev_low; back_inside=c>=prev_low*(1-buf)
            return swept and back_inside and (br>=min_body)
    def _progress_retests(self, sym, tf, o,h,l,c):
        lst=self._ret(sym,tf)
        if not lst: return None
        pad_bp = getattr(self.s,'BREAKOUT_PAD_BP',10)
        r=lst[-1]
        if r.direction=="LONG":
            touched=r.in_band(l[-1])
            if touched and not r.touched: r.touched=True; r.await_next_close=True; return None
            if r.touched and r.await_next_close and c[-1]>r.level*(1+pad_bp/10_000):
                r.await_next_close=False; return True
        else:
            touched=r.in_band(h[-1])
            if touched and not r.touched: r.touched=True; r.await_next_close=True; return None
            if r.touched and r.await_next_close and c[-1]<r.level*(1-pad_bp/10_000):
                r.await_next_close=False; return True
        return None
    def on_closed_bar(self, sym, tf)->Optional[Signal]:
        b=self._buf(sym,tf); o,h,l,c,v=list(b["o"]),list(b["h"]),list(b["l"]),list(b["c"]),list(b["v"])
        if len(c)<max(25, S.ATR_LEN+1): return None
        ph,pl=self._prior_high_low(h,l,20)
        if ph is None: return None
        br=body_ratio(o[-1],h[-1],l[-1],c[-1])
        pad=getattr(S,'BREAKOUT_PAD_BP',10)/10_000
        side=None; level=None
        rng=max(h[-1]-l[-1],1e-12)
        wick_max = getattr(S,'WICK_SIDE_MAX',0.40)
        body_min = getattr(S,'BODY_RATIO_MIN',0.60)

        if c[-1]>ph*(1+pad) and br>=body_min:
            upper_wick=(h[-1]-max(c[-1],o[-1]))/rng
            if upper_wick<=wick_max: side,level="LONG",ph
        elif c[-1]<pl*(1-pad) and br>=body_min:
            lower_wick=(min(c[-1],o[-1])-l[-1])/rng
            if lower_wick<=wick_max: side,level="SHORT",pl

        # manage pending retests
        self._progress_retests(sym,tf,o,h,l,c)

        if not side: return None
        sr=self._sweep_reclaim(h,l,o,c,side)
        expire_bars = getattr(S,'RETEST_BARS',2)
        ret_pad = getattr(S,'RETEST_MAX_BP',8)
        self._ret(sym,tf).append(RetestState(level,side,expire_bars,ret_pad))
        atr_val = atr(list(h), list(l), list(c), S.ATR_LEN)

        return Signal(symbol=sym, tf=tf, side=side, price=c[-1], level=level,
                      body_ratio=br, sweep_reclaim=sr, atr_val=atr_val, htf_bias_ok=True)
# ---------------- Logging ----------------
def setup_logging(level: str = "INFO"):
    timestamper = structlog.processors.TimeStamper(fmt="iso", utc=True)
    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            timestamper,
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.JSONRenderer(serializer=orjson.dumps),
        ],
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )
    return structlog.get_logger()
log = setup_logging("INFO")

# ---------------- Globals & state ----------------
BASE_REST = "https://fapi.binance.com"
BASE_WS   = "wss://fstream.binance.com/stream"

REST_SESSION: Optional[aiohttp.ClientSession] = None
BINANCE_CLIENT: Optional["BinanceClient"] = None

STATE={"start_time":int(time.time()),"last_kline":{},"last_alert":{},"symbols":[], "timeframes":S.TIMEFRAMES}
OB=None  # set later
BE=None
CVD=None
DEP=None

LAST_TRADE_TS: Dict[str, int] = {}
OPEN_TRADES: Dict[str, Dict[str, Any]] = {}

# ---------------- Telegram ----------------
LAST_TELEGRAM = {"ok": None, "status": None, "error": None, "ts": None}
class Telegram:
    def __init__(self, token: str, chat_id: str):
        self.api = f"https://api.telegram.org/bot{token}" if token else None
        self.chat_id = chat_id
    async def send(self, text: str) -> bool:
        global LAST_TELEGRAM
        ts = int(time.time())
        if not self.api or not self.chat_id:
            LAST_TELEGRAM = {"ok": True, "status": "NO_CREDS", "error": None, "ts": ts}
            log.info("alert", text=text)
            return True
        payload = {"chat_id": self.chat_id, "text": text, "parse_mode": "HTML", "disable_web_page_preview": True}
        for attempt in (1, 2):
            try:
                async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=5)) as s:
                    async with s.post(f"{self.api}/sendMessage", json=payload) as r:
                        body = await r.text()
                        if r.status == 200:
                            LAST_TELEGRAM = {"ok": True, "status": 200, "error": None, "ts": ts}
                            return True
                        LAST_TELEGRAM = {"ok": False, "status": r.status, "error": body[:300], "ts": ts}
                        log.warning("telegram_err", status=r.status, body=body)
            except Exception as e:
                LAST_TELEGRAM = {"ok": False, "status": "EXC", "error": str(e), "ts": ts}
                log.warning("telegram_exc", error=str(e))
            await asyncio.sleep(0.2)
        return False

async def alert(text:str, symbol:str) -> bool:
    STATE["last_alert"]={"symbol":symbol,"text":text,"ts":int(time.time())}
    tg = Telegram(S.TELEGRAM_BOT_TOKEN or "", S.TELEGRAM_CHAT_ID or "")
    return await tg.send(text)

# ---------------- HTTP /healthz ----------------
TRACE_DECISIONS = True
TRACE_SAMPLE = 0.15
REASONS = defaultdict(int)
LAST_DROPS = deque(maxlen=50)
def _maybe_debug_drop(symbol: str, tf: str, reason: str, ctx: dict):
    REASONS[reason] += 1
    ctx_slim = {k: v for k, v in ctx.items() if isinstance(v, (int, float, str, bool))}
    LAST_DROPS.append({"sym": symbol, "tf": tf, "reason": reason, **ctx_slim})
    if TRACE_DECISIONS and random.random() < TRACE_SAMPLE:
        log.info("drop_reason", symbol=symbol, tf=tf, reason=reason, **ctx_slim)

class HealthServer:
    def __init__(self, state:dict):
        self.app=web.Application(); self.state=state
        self.app.add_routes([web.get("/healthz", self.health)])
        self.runner=None; self.site=None
    async def start(self, host:str, port:int):
        self.runner=web.AppRunner(self.app); await self.runner.setup()
        self.site=web.TCPSite(self.runner, host=host, port=port); await self.site.start()
    async def health(self, req):
        return web.json_response({
            "ok":True,
            "uptime_sec":int(time.time()-self.state.get("start_time",time.time())),
            "last_kline":self.state.get("last_kline",{}),
            "last_alert":self.state.get("last_alert",{}),
            "last_telegram": LAST_TELEGRAM,
            "symbols":self.state.get("symbols",[]),
            "timeframes":self.state.get("timeframes",[]),
            "open_trades": list(OPEN_TRADES.keys()),
            "drop_reasons": dict(REASONS),
            "recent_drops": list(LAST_DROPS),
        })

# ---------------- Daily stats ----------------
DAILY_STATS: Dict[str, Dict[str, Any]] = defaultdict(lambda: {
    "count": 0, "wins_tp2": 0, "wins_ts": 0, "losses_sl": 0,
    "sum_R": 0.0, "best": None, "worst": None
})
def local_day_key(dt: Optional[datetime]=None) -> str:
    d = riyadh_now() if dt is None else to_tz(dt, S.TZ)
    return d.strftime("%Y-%m-%d")
def stats_add_trade_result(symbol: str, R: float, outcome: str):
    key = local_day_key(); st = DAILY_STATS[key]
    st["count"] += 1; st["sum_R"] += R
    if outcome == "TP2": st["wins_tp2"] += 1
    elif outcome == "TS": st["wins_ts"] += 1
    elif outcome == "SL": st["losses_sl"] += 1
    if st["best"] is None or R > st["best"][1]: st["best"] = (symbol, R)
    if st["worst"] is None or R < st["worst"][1]: st["worst"] = (symbol, R)
def compose_daily_summary() -> str:
    key = local_day_key(); st = DAILY_STATS.get(key, {})
    if not st or st["count"] == 0: return f"📅 Daily Stats – {key}\nNo trades today."
    wins = st["wins_tp2"] + st["wins_ts"]; losses = st["losses_sl"]
    hit = (wins / st["count"]) * 100.0 if st["count"] > 0 else 0.0
    avg_R = (st["sum_R"] / st["count"]) if st["count"] > 0 else 0.0
    best = f"{st['best'][0]} {st['best'][1]:+.2f}R" if st.get("best") else "—"
    worst = f"{st['worst'][0]} {st['worst'][1]:+.2f}R" if st.get("worst") else "—"
    return (f"📅 Daily Stats – {key}\nTrades: {st['count']}\n"
            f"Wins: {wins} (TP2 {st['wins_tp2']}, TS {st['wins_ts']}) | Losses: {losses}\n"
            f"Hit rate: {hit:.1f}% | Net: {st['sum_R']:+.2f}R | Avg: {avg_R:+.2f}R\n"
            f"Best: {best}   Worst: {worst}")
async def daily_stats_loop():
    while True:
        try: await alert(compose_daily_summary(), "DAILY")
        except Exception as e: log.warning("daily_stats_err", error=str(e))
        await asyncio.sleep(24 * 3600)

# ---------------- Exchange filters (tick/step/mins) ----------------
FILTERS: Dict[str, Dict[str, float]] = {}
async def load_exchange_info(session: aiohttp.ClientSession):
    global FILTERS
    async with session.get(f"{BASE_REST}/fapi/v1/exchangeInfo") as r:
        r.raise_for_status()
        data = await r.json()
    filters = {}
    for s in data.get("symbols", []):
        sym = s.get("symbol"); 
        if not sym: continue
        tick = step = min_qty = min_notional = None
        for f in s.get("filters", []):
            t = f.get("filterType")
            if t == "PRICE_FILTER":
                tick = float(f.get("tickSize", "0.0001"))
            elif t == "LOT_SIZE":
                step = float(f.get("stepSize", "0.001"))
                min_qty = float(f.get("minQty", "0.0"))
            elif t == "MIN_NOTIONAL":
                min_notional = float(f.get("notional", "0.0"))
        filters[sym] = {"tickSize": tick or 0.0001, "stepSize": step or 0.001,
                        "minQty": min_qty or 0.0, "minNotional": min_notional or 0.0}
    FILTERS = filters

def quantize(symbol: str, price: Optional[float]=None, qty: Optional[float]=None) -> Tuple[Optional[float], Optional[float]]:
    f = FILTERS.get(symbol.upper(), {"tickSize": 0.0001, "stepSize": 0.001, "minQty": 0.0, "minNotional": 0.0})
    out_p = price
    out_q = qty
    if price is not None:
        ts = f["tickSize"] or 0.0001
        out_p = math.floor(price / ts) * ts
    if qty is not None:
        ss = f["stepSize"] or 0.001
        out_q = math.floor(qty / ss) * ss
    return out_p, out_q

async def discover_perp_usdt_symbols(session: aiohttp.ClientSession, max_symbols:int) -> List[str]:
    async with session.get(f"{BASE_REST}/fapi/v1/exchangeInfo") as r:
        r.raise_for_status()
        data = await r.json()
    out=[]
    for s in data.get("symbols", []):
        try:
            if s.get("contractType")=="PERPETUAL" and s.get("quoteAsset")=="USDT" and s.get("status")=="TRADING":
                out.append(s["symbol"])
        except Exception:
            continue
    out = sorted(out)
    if max_symbols>0: out = out[:max_symbols]
    return out

# ---------------- Qty, entry & brackets ----------------
async def compute_qty(symbol: str, entry: float, sl: float) -> float:
    risk = abs(entry - sl)
    if risk <= 0: return 0.0
    eq = S.ACCOUNT_EQUITY_USDT
    if eq is None and not S.DRY_RUN:
        try:
            eq = await BINANCE_CLIENT.get_balance_usdt()
        except Exception:
            eq = None
    if eq is None: eq = 1000.0
    risk_cash = eq * (S.MAX_RISK_PCT/100.0)
    qty = risk_cash / risk
    _, q = quantize(symbol, None, qty)
    return max(q or 0.0, 0.0)

def _parse_avg_price(order_resp: dict, fallback: float) -> float:
    try:
        ap = order_resp.get("avgPrice")
        if ap is not None and ap != "0": return float(ap)
    except Exception: pass
    try:
        cum_quote = float(order_resp.get("cumQuote", "0") or 0.0)
        executed = float(order_resp.get("executedQty", "0") or 0.0)
        if executed > 0 and cum_quote > 0: return cum_quote / executed
    except Exception: pass
    try:
        fills = order_resp.get("fills") or []
        if fills:
            num = sum(float(f["price"]) * float(f.get("qty", f.get("commissionAsset", 0)) or 0.0) for f in fills)
            den = sum(float(f.get("qty", 0.0)) for f in fills)
            if den > 0: return num / den
    except Exception: pass
    return float(fallback)

def _is_hedge() -> bool:
    return (S.POSITION_MODE or "ONE_WAY").upper() == "HEDGE"
def _pos_side(side: str) -> str:
    return "LONG" if side == "LONG" else "SHORT"

async def place_reduceonly_limit(sym: str, side: str, limit_price: float, qty: float) -> Optional[int]:
    """Maker reduceOnly LIMIT at price; returns orderId or None."""
    if S.DRY_RUN: return None
    px, q = quantize(sym, limit_price, qty)
    kwargs = {"timeInForce": "GTC", "reduceOnly": True}
    if _is_hedge(): kwargs["positionSide"] = _pos_side(side)
    resp = await BINANCE_CLIENT.place_order(
        sym, "SELL" if side == "LONG" else "BUY", "LIMIT",
        price=f"{(px or limit_price):.10f}", quantity=f"{(q or qty):.10f}", **kwargs
    )
    return int(resp.get("orderId"))

async def place_entry_and_brackets(sym: str, side: str, entry_price_ref: float, sl_price: float, tp_limit: Optional[float]):
    """
    ENTER FIRST: send MARKET, poll until filled, then place SL/TP (reduceOnly).
    Returns dict with order ids + qty + entryAvg. DRY_RUN returns placeholders.
    """
    if S.DRY_RUN:
        return {"entryId": None, "slId": None, "tpId": None, "qty": 0.0, "entryAvg": entry_price_ref}

    qty = await compute_qty(sym, entry_price_ref, sl_price)
    f = FILTERS.get(sym, {"minQty":0.0, "minNotional":0.0})
    est_notional = (entry_price_ref or 0.0) * (qty or 0.0)
    if qty <= 0 or qty < (f["minQty"] or 0.0) or est_notional < (f["minNotional"] or 0.0):
        raise RuntimeError(f"Qty/notional too small (qty={qty}, notional≈{est_notional}, minQty={f.get('minQty')}, minNotional={f.get('minNotional')})")

    # ENTRY: MARKET
    entry_kwargs = {}
    if _is_hedge():
        entry_kwargs["positionSide"] = _pos_side(side)
    entry_resp = await BINANCE_CLIENT.place_order(
        sym, "BUY" if side=="LONG" else "SELL", "MARKET",
        quantity=f"{qty:.10f}", **entry_kwargs
    )
    order_id = entry_resp.get("orderId")
    avg = _parse_avg_price(entry_resp, 0.0)
    executed = float(entry_resp.get("executedQty", "0") or 0.0)

    # POLL for confirmed fill
    for _ in range(20):
        if avg > 0 and executed > 0:
            break
        await asyncio.sleep(0.1)
        q = await BINANCE_CLIENT.get_order(sym, order_id)
        avg = _parse_avg_price(q, avg or 0.0)
        executed = float(q.get("executedQty", "0") or 0.0)
        status = (q.get("status") or "").upper()
        if status in ("FILLED", "PARTIALLY_FILLED") and executed > 0 and avg > 0:
            break
        if status in ("CANCELED", "REJECTED", "EXPIRED"):
            raise RuntimeError(f"Entry {status.lower()}: {q}")

    if executed <= 0 or avg <= 0:
        try: await BINANCE_CLIENT.cancel_all(sym)
        except Exception: pass
        raise RuntimeError("Entry not filled (avg/executed still zero after polling)")

    entry_avg = avg

    # SL: STOP_MARKET reduceOnly/closePosition
    sl_p, _ = quantize(sym, sl_price, None)
    sl_kwargs = {"workingType": "MARK_PRICE"}
    if _is_hedge(): sl_kwargs.update({"reduceOnly": True, "positionSide": _pos_side(side)})
    else: sl_kwargs.update({"closePosition": True})
    sl_resp = await BINANCE_CLIENT.place_order(
        sym, "SELL" if side=="LONG" else "BUY", "STOP_MARKET",
        stopPrice=f"{(sl_p or sl_price):.10f}", **sl_kwargs
    )
    sl_id = sl_resp.get("orderId")

    # TP: LIMIT reduceOnly
    tp_id = None
    if tp_limit is not None:
        tp_p, _ = quantize(sym, tp_limit, None)
        tp_kwargs = {"timeInForce":"GTC","quantity":f"{qty:.10f}","reduceOnly":True}
        if _is_hedge(): tp_kwargs["positionSide"] = _pos_side(side)
        tp_resp = await BINANCE_CLIENT.place_order(
            sym, "SELL" if side=="LONG" else "BUY", "LIMIT",
            price=f"{(tp_p or tp_limit):.10f}", **tp_kwargs
        )
        tp_id = tp_resp.get("orderId")

    return {"entryId": order_id, "slId": sl_id, "tpId": tp_id, "qty": executed, "entryAvg": entry_avg}

async def force_close_market(sym: str, side_hint: Optional[str]=None) -> Optional[Tuple[dict, Optional[float]]]:
    if S.DRY_RUN:
        return None
    pos = await BINANCE_CLIENT.get_position_risk(sym)
    if not pos:
        try: await BINANCE_CLIENT.cancel_all(sym)
        except Exception: pass
        return None
    raw_amt = float(pos[0].get("positionAmt", "0") if isinstance(pos,list) else pos.get("positionAmt","0"))
    if abs(raw_amt) < 1e-12:
        try: await BINANCE_CLIENT.cancel_all(sym)
        except Exception: pass
        return None
    side = "SELL" if raw_amt > 0 else "BUY"
    if side_hint and side_hint in ("BUY","SELL"): side = side_hint
    step = (FILTERS.get(sym, {}) or {}).get("stepSize", 0.001) or 0.001
    q = math.floor(abs(raw_amt) / step) * step
    if q <= 0: q = step
    # try market
    for _ in range(3):
        try:
            kwargs = {"reduceOnly": True}
            if _is_hedge():
                kwargs["positionSide"] = "LONG" if raw_amt > 0 else "SHORT"
            resp = await BINANCE_CLIENT.place_order(sym, side, "MARKET", quantity=f"{q:.10f}", **kwargs)
            exit_avg = _parse_avg_price(resp, 0.0) or None
            return resp, exit_avg
        except Exception:
            await asyncio.sleep(0.2)
    return None

# ---------------- PnL (after fees) ----------------
def pnl_after_fees(entry: float, exit_: float, side: str, risk: float, exit_kind: str) -> Tuple[float,float]:
    if entry <= 0:
        return 0.0, 0.0
    fee_entry = entry * pct_to_mult(S.FEE_TAKER_PCT)
    exit_fee_pct = S.FEE_MAKER_PCT if exit_kind == "maker" else S.FEE_TAKER_PCT
    fee_exit  = exit_ * pct_to_mult(exit_fee_pct)
    gross = (exit_ - entry) if side=="LONG" else (entry - exit_)
    net = gross - (fee_entry + fee_exit)
    net_pct = (net / entry) * 100.0
    net_R = 0.0 if risk <= 0 else (net / risk)
    return net_pct, net_R

# ---------------- WS handlers ----------------
def _atr1m_bps(sym:str)->Optional[float]:
    b = BE.buffers.get((sym,"1m")) if BE else None
    if not b or len(b["c"]) < 20: return None
    a = atr(list(b["h"]), list(b["l"]), list(b["c"]), 14)
    c = b["c"][-1]
    if a is None or c<=0: return None
    return (a / c) * 10_000.0  # bps

async def on_kline(symbol:str, k:dict):
    sym=symbol.upper(); tf=k["i"]
    def _f(x): 
        try: return float(x)
        except: return 0.0
    cndl=Candle(int(k["t"]), _f(k["o"]), _f(k["h"]), _f(k["l"]), _f(k["c"]), _f(k.get("q", 0.0)), int(k["T"]))
    BE.add_candle(sym, tf, cndl)
    STATE["last_kline"]={"symbol":sym,"tf":tf,"t":k["T"]}

    sig=BE.on_closed_bar(sym, tf)
    if not sig: return

    # optional HTF bias (5m gated by 15m)
    if tf=="5m":
        buf=BE.buffers.get((sym,"15m"))
        if buf and len(buf["c"])>=50:
            htf_closes = list(buf["c"])
            htf_sma = sma(htf_closes, 50)
            htf_close = htf_closes[-1]
            htf_slope = htf_close - htf_closes[-50]
            sig.htf_bias_ok = ((sig.side=="LONG"  and htf_close>htf_sma and htf_slope>0) or
                               (sig.side=="SHORT" and htf_close<htf_sma and htf_slope<0))
            if not sig.htf_bias_ok: return

    # simple vol Z using last 30 quote bars (already filled)
    buf=BE.buffers.get((sym,tf)); vol_z=0.0
    if buf and len(buf["v"])>=30:
        vols=list(buf["v"]); win=vols[-30:-1]
        if win:
            mean=sum(win)/len(win); var=sum((x-mean)**2 for x in win)/len(win); std=math.sqrt(var) if var>0 else 1.0
            vol_z=(vols[-1]-mean)/std
    if vol_z < 1.5: return

    # cooldown / duplicate
    now_ts = int(time.time())
    if now_ts - LAST_TRADE_TS.get(sym, 0) < 4*3600: return
    if sym in OPEN_TRADES: return

    top = OB.top(sym)
    bid = top.bid_price if top else sig.price
    ask = top.ask_price if top else sig.price
    mid = (bid + ask)/2.0
    atr_val = sig.atr_val if sig.atr_val and sig.atr_val>0 else (sig.price*0.003)

    if S.CONSENSUS_ROUTER:
        pad = getattr(S,'BREAKOUT_PAD_BP',10)/10_000
        entry = sig.level * (1 + pad) if sig.side=="LONG" else sig.level * (1 - pad)

        # SL/TP in PERCENT (not bps)
        sl  = entry * (1 - S.ROUTER_SL_PCT/100.0) if sig.side=="LONG" else entry * (1 + S.ROUTER_SL_PCT/100.0)
        tp1 = entry * (1 + S.ROUTER_TP1_PCT/100.0) if sig.side=="LONG" else entry * (1 - S.ROUTER_TP1_PCT/100.0)
        tp2 = None if S.ROUTER_TP2_PCT is None else (entry * (1 + S.ROUTER_TP2_PCT/100.0) if sig.side=="LONG" else entry * (1 - S.ROUTER_TP2_PCT/100.0))

        a1_bps = _atr1m_bps(sym) or 10.0
        trail_dist = (a1_bps/10_000.0) * mid * S.TRAIL_ATR1M_MULT

        # place orders (choose whether exchange holds TP1 or TP2)
        tp_on_exchange = tp2 if tp2 is not None else tp1
        try:
            orders = await place_entry_and_brackets(sym=sym, side=sig.side, entry_price_ref=entry, sl_price=sl, tp_limit=tp_on_exchange)
        except Exception as e:
            await alert(f"🚫 ORDER FAILED (Router) {sym} {tf}\n{sig.side} entry≈{entry:.6f} SL≈{sl:.6f} TP≈{tp_on_exchange:.6f}\nErr: {e}", sym); return

        actual_entry = orders.get("entryAvg", entry)
        OPEN_TRADES[sym] = {
            "symbol": sym, "side": sig.side, "tf": tf,
            "entry": actual_entry, "sl": sl, "tp1": tp1, "tp2": tp2,
            "risk": abs(actual_entry - sl),
            "opened_ts": now_ts,
            "tp1_hit": False, "trail_active": True, "trail_peak": mid,
            "trail_dist": trail_dist, "atr_at_entry": atr_val,
            "order_ids": orders,
            "filled_qty": float(orders.get("qty", 0.0))
        }
        LAST_TRADE_TS[sym] = now_ts
        qty_txt = f"{orders.get('qty',0):.4f}" if not S.DRY_RUN else "—"
        await alert(
            f"🛒 EXECUTED (Router) {sym} {tf}\nEntry: <b>{actual_entry:.6f}</b> | SL: <b>{sl:.6f}</b> | TP1: <b>{tp1:.6f}</b>{'' if tp2 is None else f' | TP2: <b>{tp2:.6f}</b>'}\nQty: {qty_txt}",
            sym
        )
        return

    # -------- Original R:R path (configurable TP1/TP2/TS) --------
    entry = sig.price
    sl    = entry - atr_val * S.ATR_SL_MULT if sig.side=="LONG" else entry + atr_val * S.ATR_SL_MULT
    risk  = abs(entry - sl)
    tp1   = entry + risk * S.TP1_MULT if sig.side=="LONG" else entry - risk * S.TP1_MULT
    tp2   = entry + risk * S.TP2_MULT if sig.side=="LONG" else entry - risk * S.TP2_MULT

    try:
        orders = await place_entry_and_brackets(sym=sym, side=sig.side, entry_price_ref=entry, sl_price=sl, tp_limit=tp2)
    except Exception as e:
        await alert(f"🚫 ORDER FAILED {sym} {tf}\n{sig.side} entry≈{entry:.6f} SL≈{sl:.6f} TP2≈{tp2:.6f}\nErr: {e}", sym); return

    actual_entry = orders.get("entryAvg", entry)
    risk = abs(actual_entry - sl)
    OPEN_TRADES[sym] = {
        "symbol": sym, "side": sig.side, "tf": tf,
        "entry": actual_entry, "sl": sl, "tp1": tp1, "tp2": tp2,
        "risk": risk, "opened_ts": now_ts,
        "tp1_hit": False, "trail_active": False, "trail_peak": None,
        "trail_dist": atr_val * S.TRAIL_ATR_MULT, "atr_at_entry": atr_val,
        "order_ids": orders, "filled_qty": float(orders.get("qty", 0.0))
    }
    LAST_TRADE_TS[sym] = now_ts
    sl_pct=fmt_pct(sl, actual_entry); tp1_pct=fmt_pct(tp1, actual_entry); tp2_pct=fmt_pct(tp2, actual_entry)
    qty_txt = f"{orders.get('qty',0):.4f}" if not S.DRY_RUN else "—"
    await alert(
        f"🛒 EXECUTED {sym} {tf}\nEntry: <b>{actual_entry:.6f}</b>\nSL: <b>{sl:.6f}</b> ({sl_pct:+.2f}%) | TP1: <b>{tp1:.6f}</b> ({tp1_pct:+.2f}%) | TP2: <b>{tp2:.6f}</b> ({tp2_pct:+.2f}%)\nQty: {qty_txt}",
        sym
    )

async def on_bookticker(symbol:str, data:dict):
    global OPEN_TRADES
    if OB: OB.update_book_ticker(symbol, data)
    sym = symbol.upper()
    trade = OPEN_TRADES.get(sym)
    if not trade: return
    top = OB.top(sym)
    if not top: return
    bid = top.bid_price; ask = top.ask_price
    mid = (ask + bid) / 2
    if mid <= 0: return

    side = trade["side"]
    entry = trade["entry"]; sl = trade["sl"]; tp1 = trade["tp1"]; tp2 = trade["tp2"]
    trail_dist = trade["trail_dist"]

    if not trade["tp1_hit"]:
        if side == "LONG":
            if bid <= sl: await _close_trade(sym, "SL", bid, entry, trade["risk"]); return
            if ask >= tp1:
                trade["tp1_hit"] = True; trade["trail_active"] = True; trade["trail_peak"] = mid
                trade["be_fee"] = be_with_fees(entry, side, exit_kind="taker")
        else:
            if ask >= sl: await _close_trade(sym, "SL", ask, entry, trade["risk"]); return
            if bid <= tp1:
                trade["tp1_hit"] = True; trade["trail_active"] = True; trade["trail_peak"] = mid
                trade["be_fee"] = be_with_fees(entry, side, exit_kind="taker")
        return

    # Trailing after TP1
    if side == "LONG":
        if trade["trail_active"]:
            trade["trail_peak"] = max(trade.get("trail_peak") or mid, mid)
            raw_trail_stop = trade["trail_peak"] - trail_dist
            be_min = trade.get("be_fee", entry)
            trail_stop = max(raw_trail_stop, be_min)
            trade["trail_stop"] = trail_stop

            # maker trailing w/ panic
            if S.TRAIL_MAKER and not S.DRY_RUN:
                qty = trade.get("filled_qty", 0.0)
                if qty > 0:
                    need_new = False
                    last_px = trade.get("trail_order_px")
                    gap = abs((trail_stop - (last_px or trail_stop)) / max(trail_stop, 1e-12)) * 100.0
                    if trade.get("trail_order_id") is None or gap >= S.TRAIL_REPRICE_GAP_PCT:
                        need_new = True
                    if need_new and trade.get("trail_order_id"):
                        try: await BINANCE_CLIENT.cancel_all(sym)
                        except Exception: pass
                        trade["trail_order_id"] = None; trade["trail_order_px"] = None
                    if need_new:
                        try:
                            oid = await place_reduceonly_limit(sym, side, trail_stop, qty)
                            trade["trail_order_id"] = oid; trade["trail_order_px"] = trail_stop
                            trade["trail_order_since"] = time.time()
                        except Exception:
                            await _close_trade(sym, "TS", trail_stop, entry, trade["risk"]); return
                    crossed = (bid <= trail_stop * (1 - S.TRAIL_PANIC_PCT/100.0))
                    aged = (time.time() - (trade.get("trail_order_since") or time.time())) >= S.TRAIL_PANIC_TIMEOUT_SEC
                    if crossed or aged:
                        try: await BINANCE_CLIENT.cancel_all(sym)
                        except Exception: pass
                        await _close_trade(sym, "TS", trail_stop, entry, trade["risk"]); return
            else:
                if bid <= trail_stop:
                    await _close_trade(sym, "TS", trail_stop, entry, trade["risk"]); return

        if tp2 and ask >= tp2:
            await _close_trade(sym, "TP2", tp2, entry, trade["risk"]); return

    else:  # SHORT
        if trade["trail_active"]:
            trade["trail_peak"] = min(trade.get("trail_peak") or mid, mid)
            raw_trail_stop = trade["trail_peak"] + trail_dist
            be_max = trade.get("be_fee", entry)
            trail_stop = min(raw_trail_stop, be_max)
            trade["trail_stop"] = trail_stop

            if S.TRAIL_MAKER and not S.DRY_RUN:
                qty = trade.get("filled_qty", 0.0)
                if qty > 0:
                    need_new = False
                    last_px = trade.get("trail_order_px")
                    gap = abs((trail_stop - (last_px or trail_stop)) / max(trail_stop, 1e-12)) * 100.0
                    if trade.get("trail_order_id") is None or gap >= S.TRAIL_REPRICE_GAP_PCT:
                        need_new = True
                    if need_new and trade.get("trail_order_id"):
                        try: await BINANCE_CLIENT.cancel_all(sym)
                        except Exception: pass
                        trade["trail_order_id"] = None; trade["trail_order_px"] = None
                    if need_new:
                        try:
                            oid = await place_reduceonly_limit(sym, side, trail_stop, qty)
                            trade["trail_order_id"] = oid; trade["trail_order_px"] = trail_stop
                            trade["trail_order_since"] = time.time()
                        except Exception:
                            await _close_trade(sym, "TS", trail_stop, entry, trade["risk"]); return
                    crossed = (ask >= trail_stop * (1 + S.TRAIL_PANIC_PCT/100.0))
                    aged = (time.time() - (trade.get("trail_order_since") or time.time())) >= S.TRAIL_PANIC_TIMEOUT_SEC
                    if crossed or aged:
                        try: await BINANCE_CLIENT.cancel_all(sym)
                        except Exception: pass
                        await _close_trade(sym, "TS", trail_stop, entry, trade["risk"]); return
            else:
                if ask >= trail_stop:
                    await _close_trade(sym, "TS", trail_stop, entry, trade["risk"]); return

        if tp2 and bid <= tp2:
            await _close_trade(sym, "TP2", tp2, entry, trade["risk"]); return

async def on_aggtrade(symbol:str, data:dict):
    pass  # (optional CVD here)

async def on_depth(symbol:str, data:dict):
    pass  # (optional walls here)

# ---------------- Trade close & alert ----------------
async def _close_trade(symbol: str, outcome: str, exit_price: float, entry: float, risk: float):
    trade = OPEN_TRADES.pop(symbol, None)
    if not trade: return

    # Exchange flatten – try MARKET and take its avg fill
    exchange_closed = True
    actual_exit = exit_price
    if not S.DRY_RUN:
        try:
            resp = await force_close_market(symbol)
            if isinstance(resp, tuple) and resp:
                _, exit_avg = resp
                if exit_avg:
                    actual_exit = exit_avg
        except Exception as e:
            exchange_closed = False
            log.warning("force_close_err", symbol=symbol, error=str(e))

    exit_kind = "maker" if outcome == "TP2" else "taker"
    net_pct, net_R = pnl_after_fees(entry, actual_exit, trade["side"], risk, exit_kind)
    dur_min = (int(time.time()) - trade["opened_ts"]) / 60.0
    stats_add_trade_result(symbol, net_R, outcome)
    status_emoji = "✅" if exchange_closed or S.DRY_RUN else "⚠️"

    msg = (f"{status_emoji} <b>RESULT</b> {symbol}\n"
           f"Outcome: <b>{outcome}</b>\n"
           f"Entry: {entry:.6f} → Exit: {actual_exit:.6f}  (PnL net: {net_pct:+.2f}% | {net_R:+.2f}R)\n"
           f"TP1 hit: {'✅' if trade.get('tp1_hit') else '—'}\n"
           f"Fees(%): taker {S.FEE_TAKER_PCT:.3f}, maker {S.FEE_MAKER_PCT:.3f}, slip {S.SLIPPAGE_PCT:.3f}\n"
           f"Duration: {dur_min:.1f} min")
    await alert(msg, symbol)
# ---------------- WS stream wrappers ----------------
class WSStream:
    def __init__(self, symbols: List[str], timeframes: List[str], on_kline, on_bookticker, on_aggtrade, on_depth):
        self.symbols=[s.lower() for s in symbols]; self.timeframes=timeframes
        self.on_kline=on_kline; self.on_bookticker=on_bookticker
        self.on_aggtrade=on_aggtrade; self.on_depth=on_depth
    def _streams(self)->str:
        parts=[]
        for s in self.symbols:
            for tf in self.timeframes: parts.append(f"{s}@kline_{tf}")
            parts.append(f"{s}@bookTicker")
            parts.append(f"{s}@aggTrade")
            parts.append(f"{s}@depth5@100ms")
        return f"{BASE_WS}?streams={'/'.join(parts)}"
    async def run(self):
        url=self._streams()
        async with aiohttp.ClientSession() as sess:
            async with sess.ws_connect(url, heartbeat=30, autoping=True, compress=15) as ws:
                async for msg in ws:
                    if msg.type==aiohttp.WSMsgType.TEXT:
                        payload=msg.json(); stream=payload.get("stream",""); data=payload.get("data",{})
                        if "kline" in stream and data.get("k",{}).get("x"): await self.on_kline(data["s"], data["k"])
                        elif "bookTicker" in stream: await self.on_bookticker(data["s"], data)
                        elif "aggTrade" in stream: await self.on_aggtrade(data["s"], data)
                        elif "depth5" in stream: await self.on_depth(data["s"], data)
                    elif msg.type==aiohttp.WSMsgType.ERROR:
                        break

class WSStreamMulti:
    def __init__(self, all_symbols: List[str], timeframes: List[str], on_kline, on_bookticker, on_aggtrade, on_depth, chunk_size:int):
        self.chunks=[all_symbols[i:i+chunk_size] for i in range(0,len(all_symbols),chunk_size)]
        self.timeframes=timeframes; self.on_kline=on_kline; self.on_bookticker=on_bookticker
        self.on_aggtrade=on_aggtrade; self.on_depth=on_depth
    async def run(self):
        tasks=[asyncio.create_task(WSStream(c,self.timeframes,self.on_kline,self.on_bookticker,self.on_aggtrade,self.on_depth).run()) for c in self.chunks]
        await asyncio.gather(*tasks)

# ---------------- Backfill ----------------
async def small_backfill(client:"BinanceClient", sym:str, tf:str, limit:int):
    # REST kline fields: [0]t, [1]o,[2]h,[3]l,[4]c,[5]base vol,[6]T,[7]quote vol, ...
    try:
        async with REST_SESSION.get(f"{BASE_REST}/fapi/v1/klines",
                                    params={"symbol":sym,"interval":tf,"limit":limit}) as r:
            r.raise_for_status(); data = await r.json()
    except Exception as e:
        log.warning("backfill_err", sym=sym, tf=tf, err=str(e)); return
    for k in data[:-1]:
        o,h,l,c = float(k[1]),float(k[2]),float(k[3]),float(k[4])
        q_quote = float(k[7]) if len(k)>7 else 0.0
        BE.add_candle(sym, tf, Candle(int(k[0]), o, h, l, c, q_quote, int(k[6])))

# ---------------- Bootstrap ----------------
async def start_http_server(state:dict):
    server = HealthServer(state); await server.start(S.HOST, S.PORT)
    log.info("http_started", host=S.HOST, port=S.PORT)

async def configure_account(symbols: List[str]):
    # Position mode / leverage best-effort
    try:
        # Futures position mode endpoint: /fapi/v1/positionSide/dual
        dual = (S.POSITION_MODE or "ONE_WAY").upper() == "HEDGE"
        await BINANCE_CLIENT._signed("POST","/fapi/v1/positionSide/dual",
                                     {"dualSidePosition": "true" if dual else "false"})
    except Exception as e:
        log.info("posmode_info", error=str(e))
    for sym in symbols:
        try:
            await BINANCE_CLIENT._signed("POST","/fapi/v1/marginType", {"symbol": sym, "marginType": "ISOLATED"})
        except Exception as e:
            log.info("margin_info", symbol=sym, error=str(e))
        try:
            await BINANCE_CLIENT._signed("POST","/fapi/v1/leverage", {"symbol": sym, "leverage": int(S.LEVERAGE)})
        except Exception as e:
            log.info("lev_info", symbol=sym, error=str(e))

# ---------------- Main ----------------
async def main():
    global REST_SESSION, BINANCE_CLIENT, OB, BE, CVD, DEP

    log.info("boot",
             tfs=S.TIMEFRAMES, router=S.CONSENSUS_ROUTER,
             atr_sl=S.ATR_SL_MULT, tp1_mult=S.TP1_MULT, tp2_mult=S.TP2_MULT,
             router_sl=S.ROUTER_SL_PCT, router_tp1=S.ROUTER_TP1_PCT, router_tp2=S.ROUTER_TP2_PCT,
             fees={"taker":S.FEE_TAKER_PCT,"maker":S.FEE_MAKER_PCT,"slip":S.SLIPPAGE_PCT},
             trail={"orig":S.TRAIL_ATR_MULT,"router":S.TRAIL_ATR1M_MULT})

    asyncio.create_task(start_http_server(STATE))
    asyncio.create_task(daily_stats_loop())

    REST_SESSION = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10))
    BINANCE_CLIENT = BinanceClient(REST_SESSION)

    # load filters
    await load_exchange_info(REST_SESSION)

    # Symbols
    if S.SYMBOLS=="ALL" or (isinstance(S.SYMBOLS,str) and S.SYMBOLS.upper()=="ALL"):
        symbols=await discover_perp_usdt_symbols(REST_SESSION, S.MAX_SYMBOLS)
    elif isinstance(S.SYMBOLS, (list, tuple)):
        symbols=list(S.SYMBOLS)
    else:
        symbols=[str(S.SYMBOLS)]
    STATE["symbols"]=symbols

    # Timeframes — if router is ON, ensure "1m" present internally
    tfs = list(S.TIMEFRAMES)
    if S.CONSENSUS_ROUTER and "1m" not in tfs:
        tfs = ["1m"] + tfs
    STATE["timeframes"]=tfs
    log.info("symbols_selected", count=len(symbols), tfs=tfs)

    # Account configure (best effort)
    if not S.DRY_RUN and S.BINANCE_API_KEY and S.BINANCE_API_SECRET:
        await configure_account(symbols)

    # Build trackers
    OB=OrderBookTracker()
    BE=BreakoutEngine(S)
    CVD=None  # plug if needed
    DEP=None  # plug if needed

    # Backfill
    for i, sym in enumerate(symbols):
        for tf in tfs:
            await small_backfill(BINANCE_CLIENT, sym, tf, limit=S.BACKFILL_LIMIT)
        if i % 10 == 0:
            await asyncio.sleep(0.1)

    # Streams
    ws_multi=WSStreamMulti(symbols, tfs, on_kline, on_bookticker, on_aggtrade, on_depth, chunk_size=S.WS_CHUNK_SIZE)
    await ws_multi.run()

if __name__=="__main__":
    try: uvloop.install()
    except Exception: pass
    try: asyncio.run(main())
    except KeyboardInterrupt: pass
        
