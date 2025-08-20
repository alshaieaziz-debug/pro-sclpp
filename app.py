# app.py — Binance USDT-M Perps Breakout Bot (FORCE EXEC FIRST, ALERT AFTER)
# Core: Breakout + body, quick retest, sweep & reclaim, HTF(15m) bias gating 5m
# Flow: CVD slope (aggTrade), liquidity wall avoidance (depth5), Volume Z-score
# OI (relaxed): z-score OR delta, direction-aligned
# Risk: R:R=1:2 (TP1, TP2), trailing only after TP1
# Ops: 4h cooldown per ticker, daily stats 22:00 Riyadh, /healthz, Telegram alerts
# Force: If FORCE_EXECUTE_ON_ALERT=true & DRY_RUN=false -> place MARKET order FIRST, then alert.

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
_ensure(["aiohttp>=3.9.5","uvloop>=0.19.0","pydantic>=2.7.0","pydantic-settings>=2.2.1",
         "structlog>=24.1.0","tzdata>=2024.1","orjson>=3.10.7"])

import asyncio, aiohttp, orjson, time, math, hmac, hashlib
from typing import List, Dict, Any, Optional, Deque, Tuple
from collections import deque, defaultdict
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
import structlog, uvloop
from aiohttp import web
from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import field_validator

# ---------------- Settings ----------------
class Settings(BaseSettings):
    # Universe & TFs
    SYMBOLS: str | List[str] = "ALL"        # "ALL" or CSV "BTCUSDT,ETHUSDT"
    TIMEFRAMES: List[str] = ["5m","15m"]
    MAX_SYMBOLS: int = 200
    WS_CHUNK_SIZE: int = 60
    BACKFILL_LIMIT: int = 60

    # Breakout logic
    BREAKOUT_PAD_BP: int = 10
    BODY_RATIO_MIN: float = 0.60
    RETEST_BARS: int = 2
    RETEST_MAX_BP: int = 8
    SWEEP_LOOKBACK: int = 15
    RECLAIM_BUFFER_BP: int = 6
    RECLAIM_BODY_RATIO_MIN: float = 0.55
    HTF_SMA_LEN: int = 50
    WICK_SIDE_MAX: float = 0.35

    # Volume Z-score
    VOL_Z_MIN: float = 1.5

    # OI relaxed
    OI_LOOKBACK: str = "5m"
    OI_Z_MIN: float = 1.0
    OI_DELTA_MIN: float = 0.005

    # CVD (aggTrade)
    ENABLE_CVD: bool = True
    CVD_WINDOW_SEC: int = 300
    CVD_REQUIRE_SIGN: bool = True
    CVD_MIN_SLOPE_ABS: float = 0.0

    # Liquidity walls (depth5)
    ENABLE_WALLS: bool = True
    WALL_BAND_BP: int = 15
    WALL_MULT: float = 2.5
    WALL_MEDIAN_WINDOW: int = 200

    # Risk & trade plan
    ATR_LEN: int = 14
    ATR_SL_MULT: float = 1.5
    ENTRY_MODE: str = "MARKET"             # MARKET or RETEST
    MAX_RISK_PCT: float = 1.0
    ACCOUNT_EQUITY_USDT: Optional[float] = None
    DRY_RUN: bool = True
    TRAIL_ATR_MULT: float = 1.0

    # Cooldown
    COOLDOWN_AFTER_TRADE_SEC: int = 4*3600

    # Force execution first
    FORCE_EXECUTE_ON_ALERT: bool = bool(os.getenv("FORCE_EXECUTE_ON_ALERT", "true").lower()=="true")
    AUTO_RAISE_TO_MIN_NOTIONAL: bool = True
    OVERRIDE_COOLDOWN_ON_ALERT: bool = True

    # Ops
    LOG_LEVEL: str = "INFO"
    DATA_DIR: str = "./data"

    # Telegram
    TELEGRAM_BOT_TOKEN: Optional[str] = os.getenv("TELEGRAM_BOT_TOKEN")
    TELEGRAM_CHAT_ID: Optional[str] = os.getenv("TELEGRAM_CHAT_ID")

    # Server
    HOST: str = "0.0.0.0"
    PORT: int = int(os.getenv("PORT","8080"))
    TZ: str = "Asia/Riyadh"

    model_config = SettingsConfigDict(env_file=".env", case_sensitive=False)
    @field_validator("SYMBOLS", mode="before")
    @classmethod
    def _split_symbols(cls, v):
        if isinstance(v, str) and v.strip().upper()!="ALL":
            return [s.strip().upper() for s in v.split(",") if s.strip()]
        return v
    @field_validator("TIMEFRAMES", mode="before")
    @classmethod
    def _split_tfs(cls, v):
        if isinstance(v, str):
            return [s.strip() for s in v.split(",") if s.strip()]
        return v

S = Settings()

def setup_logging(level="INFO"):
    timestamper = structlog.processors.TimeStamper(fmt="iso", utc=True)
    structlog.configure(
        processors=[structlog.contextvars.merge_contextvars, timestamper,
                    structlog.processors.StackInfoRenderer(),
                    structlog.processors.format_exc_info,
                    structlog.processors.JSONRenderer(serializer=orjson.dumps)],
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )
    return structlog.get_logger()
log = setup_logging(S.LOG_LEVEL)

# ---------------- Utils/TA ----------------
def now_utc() -> datetime: return datetime.now(timezone.utc)
def to_tz(dt: datetime, tz: str) -> datetime: return dt.astimezone(ZoneInfo(tz))
def riyadh_now() -> datetime: return to_tz(now_utc(), S.TZ)
def body_ratio(o,h,l,c): rng=max(h-l,1e-12); return abs(c-o)/rng
def atr(highs, lows, closes, length:int) -> Optional[float]:
    if len(closes) < length+1: return None
    trs=[]
    for i in range(1,len(closes)):
        trs.append(max(highs[i]-lows[i], abs(highs[i]-closes[i-1]), abs(lows[i]-closes[i-1])))
    if len(trs) < length: return None
    return sum(trs[-length:])/length
def _fmt_pct(a: float, b: float) -> float:
    if b == 0: return 0.0
    return (a/b - 1.0) * 100.0

# --------------- Data ----------------
class Candle:
    __slots__=("open_time","open","high","low","close","volume","close_time")
    def __init__(self, ot,o,h,l,c,v,ct):
        self.open_time=ot; self.open=o; self.high=h; self.low=l; self.close=c; self.volume=v; self.close_time=ct
class BookTop:
    __slots__=("bid_price","bid_qty","ask_price","ask_qty")
    def __init__(self, bp,bq,ap,aq): self.bid_price=bp; self.bid_qty=bq; self.ask_price=ap; self.ask_qty=aq
class Signal:
    def __init__(self, **kw): self.__dict__.update(kw)

# ------------- Binance REST/WS -------------
BASE_REST = "https://fapi.binance.com"
BASE_WS   = "wss://fstream.binance.com/stream"
REST_SESSION: Optional[aiohttp.ClientSession] = None
BINANCE_CLIENT = None

async def discover_perp_usdt_symbols(session: aiohttp.ClientSession, max_symbols:int) -> List[str]:
    async with session.get(f"{BASE_REST}/fapi/v1/exchangeInfo") as r:
        r.raise_for_status(); data = await r.json()
    out=[]
    for s in data.get("symbols", []):
        try:
            if s.get("contractType")=="PERPETUAL" and s.get("quoteAsset")=="USDT" and s.get("status")=="TRADING":
                out.append(s["symbol"])
        except: continue
    out = sorted(out)
    if max_symbols>0: out = out[:max_symbols]
    return out

class BinanceClient:
    def __init__(self, session: aiohttp.ClientSession):
        self.session = session
    async def klines(self, symbol:str, interval:str, limit:int=200):
        p={"symbol":symbol.upper(),"interval":interval,"limit":limit}
        async with self.session.get(f"{BASE_REST}/fapi/v1/klines", params=p) as r:
            r.raise_for_status(); return await r.json()
    async def open_interest_hist(self, symbol:str, period:str="5m", limit:int=30)->Optional[List[float]]:
        p={"symbol":symbol.upper(),"period":period,"limit":limit}
        async with self.session.get(f"{BASE_REST}/futures/data/openInterestHist", params=p) as r:
            if r.status!=200: return None
            d=await r.json()
            if not d: return None
            return [float(x["sumOpenInterest"]) for x in d]
    async def open_interest(self, symbol:str)->Optional[float]:
        async with self.session.get(f"{BASE_REST}/fapi/v1/openInterest", params={"symbol":symbol.upper()}) as r:
            if r.status!=200: return None
            d=await r.json()
            return float(d.get("openInterest", 0))

# --- Futures signing & requests ---
def _sign(params: dict, secret: str) -> str:
    qs = "&".join([f"{k}={params[k]}" for k in sorted(params)])
    return hmac.new(secret.encode(), qs.encode(), hashlib.sha256).hexdigest()

async def _fapi(method, path, params):
    api_key=os.getenv("BINANCE_API_KEY"); api_secret=os.getenv("BINANCE_API_SECRET")
    if not api_key or not api_secret: raise RuntimeError("Missing API keys")
    params=dict(params); params["timestamp"]=int(time.time()*1000); params["recvWindow"]=5000
    params["signature"]=_sign(params, api_secret)
    url=f"{BASE_REST}{path}"; headers={"X-MBX-APIKEY": api_key}
    async with REST_SESSION.request(method, url, params=params, headers=headers) as r:
        txt=await r.text()
        if r.status!=200: raise RuntimeError(f"Binance error {r.status}: {txt}")
        return await r.json()

_SYMBOL_FILTERS: dict[str, dict] = {}
async def load_symbol_filters():
    global _SYMBOL_FILTERS
    async with REST_SESSION.get(f"{BASE_REST}/fapi/v1/exchangeInfo") as r:
        r.raise_for_status(); info=await r.json()
    out={}
    for s in info.get("symbols", []):
        sym=s["symbol"]
        tick=next((f for f in s["filters"] if f["filterType"]=="PRICE_FILTER"), None)
        lot=next((f for f in s["filters"] if f["filterType"]=="LOT_SIZE"), None)
        out[sym]={"tickSize": float(tick["tickSize"]) if tick else 0.0001,
                  "stepSize": float(lot["stepSize"]) if lot else 0.001,
                  "minQty": float(lot["minQty"]) if lot else 0.0}
    _SYMBOL_FILTERS=out

def _round_to_step(x: float, step: float) -> float:
    if step<=0: return x
    return math.floor(x/step)*step
def round_price(sym: str, p: float) -> float:
    tick=_SYMBOL_FILTERS.get(sym,{}).get("tickSize",0.0001)
    return round(_round_to_step(p, tick), 8)
def round_qty(sym: str, q: float) -> float:
    step=_SYMBOL_FILTERS.get(sym,{}).get("stepSize",0.001)
    return round(_round_to_step(q, step), 8)

async def ensure_symbol_setup(sym: str):
    lev=int(os.getenv("LEVERAGE","5"))
    mt=os.getenv("MARGIN_TYPE","ISOLATED").upper()
    pm=os.getenv("POSITION_MODE","ONE_WAY").upper()
    try:
        await _fapi("POST","/fapi/v1/positionSide/dual",
                    {"dualSidePosition": "false" if pm=="ONE_WAY" else "true"})
    except Exception: pass
    try:
        await _fapi("POST","/fapi/v1/marginType", {"symbol":sym,"marginType":mt})
    except Exception: pass
    try:
        await _fapi("POST","/fapi/v1/leverage", {"symbol":sym,"leverage":lev})
    except Exception as e:
        log.warning("set_leverage_failed", symbol=sym, error=str(e))

def compute_qty(sym: str, entry: float, sl: float, equity: float,
                max_risk_pct: float, notional_max_pct: float, leverage: float) -> float:
    risk_usdt = equity * (max_risk_pct/100.0)
    risk_per_unit = abs(entry - sl)
    if risk_per_unit <= 0: return 0.0
    qty_risk = risk_usdt / risk_per_unit
    notional_cap = equity * (notional_max_pct/100.0) * leverage
    qty_cap = notional_cap / max(entry,1e-9)
    qty = max(0.0, min(qty_risk, qty_cap))
    return round_qty(sym, qty)

async def place_stop_market_close(sym: str, side: str, stop_price: float):
    opposite = "SELL" if side=="LONG" else "BUY"
    return await _fapi("POST","/fapi/v1/order",
        {"symbol":sym,"side":opposite,"type":"STOP_MARKET","stopPrice":f"{round_price(sym, stop_price):.8f}",
         "closePosition":"true","newClientOrderId":f"sl_{int(time.time()*1000)}"})

# Streams
class WSStream:
    def __init__(self, symbols, timeframes, on_kline, on_bookticker, on_aggtrade, on_depth):
        self.symbols=[s.lower() for s in symbols]; self.timeframes=timeframes
        self.on_kline=on_kline; self.on_bookticker=on_bookticker
        self.on_aggtrade=on_aggtrade; self.on_depth=on_depth
    def _streams(self)->str:
        parts=[]
        for s in self.symbols:
            for tf in self.timeframes: parts.append(f"{s}@kline_{tf}")
            parts.append(f"{s}@bookTicker"); parts.append(f"{s}@aggTrade"); parts.append(f"{s}@depth5@100ms")
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

class WSStreamMulti:
    def __init__(self, all_symbols, timeframes, on_kline, on_bookticker, on_aggtrade, on_depth, chunk_size:int):
        self.chunks=[all_symbols[i:i+chunk_size] for i in range(0,len(all_symbols),chunk_size)]
        self.timeframes=timeframes; self.on_kline=on_kline; self.on_bookticker=on_bookticker
        self.on_aggtrade=on_aggtrade; self.on_depth=on_depth
    async def run(self):
        await asyncio.gather(*[asyncio.create_task(
            WSStream(c,self.timeframes,self.on_kline,self.on_bookticker,self.on_aggtrade,self.on_depth).run()
        ) for c in self.chunks])

# ------------- OB, CVD & Depth -------------
class OrderBookTracker:
    def __init__(self): self.tops:Dict[str,BookTop]={}
    def update_book_ticker(self, sym:str, d:dict):
        self.tops[sym.upper()] = BookTop(float(d.get("b",0)),float(d.get("B",0)),float(d.get("a",0)),float(d.get("A",0)))
    def top(self, sym:str)->Optional[BookTop]: return self.tops.get(sym.upper())

class CVDTracker:
    def __init__(self, window_sec:int=300):
        self.window_sec=window_sec
        self.buff:Dict[str,Deque[Tuple[float,float]]] = defaultdict(lambda: deque())
    def on_agg(self, sym:str, price:float, qty:float, buyer_is_maker:bool):
        sign = -1.0 if buyer_is_maker else +1.0
        ts=time.time(); dq=sign*qty
        q=self.buff[sym]; q.append((ts, dq))
        cutoff=ts-self.window_sec
        while q and q[0][0]<cutoff: q.popleft()
    def slope(self, sym:str)->float:
        q=self.buff.get(sym)
        if not q or len(q)<2: return 0.0
        total=sum(d for _,d in q); span=q[-1][0]-q[0][0]
        return total/max(span,1e-6)

class DepthTracker:
    def __init__(self, median_win:int=200):
        self.top_sizes: Dict[str, Deque[float]] = defaultdict(lambda: deque(maxlen=median_win))
        self.last_depth: Dict[str, Dict[str, List[Tuple[float,float]]]] = {}
    def on_depth5(self, sym:str, data:dict):
        bids=[(float(p),float(q)) for p,q in data.get("b",[])]
        asks=[(float(p),float(q)) for p,q in data.get("a",[])]
        self.last_depth[sym]={"bids":bids,"asks":asks}
        if bids: self.top_sizes[sym].append(bids[0][1])
        if asks: self.top_sizes[sym].append(asks[0][1])
    def _median_top(self, sym:str)->float:
        arr=self.top_sizes.get(sym); 
        if not arr: return 0.0
        s=sorted(arr); n=len(s)
        return s[n//2] if n%2==1 else 0.5*(s[n//2-1]+s[n//2])
    def has_opposite_wall(self, sym:str, side:str, ref_price:float, band_bp:int, mult:float)->bool:
        d=self.last_depth.get(sym)
        if not d or ref_price<=0: return False
        med=self._median_top(sym)
        if med<=0: return False
        band=ref_price*(band_bp/10_000)
        if side=="LONG":
            wall_sz=sum(q for p,q in d["asks"] if ref_price < p <= ref_price+band)
            return wall_sz > mult*med
        else:
            wall_sz=sum(q for p,q in d["bids"] if ref_price-band <= p < ref_price)
            return wall_sz > mult*med

# ------------- Breakout Engine -------------
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
    def _sweep_reclaim(self, hs, ls, cs, side:str)->bool:
        if len(hs)<self.s.SWEEP_LOOKBACK+1: return False
        prev_high=max(list(hs)[-self.s.SWEEP_LOOKBACK-1:-1]); prev_low=min(list(ls)[-self.s.SWEEP_LOOKBACK-1:-1])
        c, h, l = cs[-1], hs[-1], ls[-1]
        if side=="LONG":
            swept=h>prev_high; back_inside=c<=prev_high*(1+self.s.RECLAIM_BUFFER_BP/10_000)
            return swept and back_inside and (self.s.RECLAIM_BODY_RATIO_MIN<=1.0)
        else:
            swept=l<prev_low; back_inside=c>=prev_low*(1-self.s.RECLAIM_BUFFER_BP/10_000)
            return swept and back_inside and (self.s.RECLAIM_BODY_RATIO_MIN<=1.0)
    def _progress_retests(self, sym, tf, o,h,l,c):
        lst=self._ret(sym,tf)
        if not lst: return None
        r=lst[-1]
        if r.direction=="LONG":
            touched=r.in_band(l[-1])
            if touched and not r.touched: r.touched=True; r.await_next_close=True; return None
            if r.touched and r.await_next_close and c[-1]>r.level*(1+self.s.BREAKOUT_PAD_BP/10_000):
                r.await_next_close=False; return True
        else:
            touched=r.in_band(h[-1])
            if touched and not r.touched: r.touched=True; r.await_next_close=True; return None
            if r.touched and r.await_next_close and c[-1]<r.level*(1-self.s.BREAKOUT_PAD_BP/10_000):
                r.await_next_close=False; return True
        return None
    def on_closed_bar(self, sym, tf)->Optional[Signal]:
        b=self._buf(sym,tf); o,h,l,c,v=list(b["o"]),list(b["h"]),list(b["l"]),list(b["c"]),list(b["v"])
        if len(c)<max(25, S.ATR_LEN+1): return None
        ph,pl=self._prior_high_low(h,l,20)
        if ph is None: return None
        br=body_ratio(o[-1],h[-1],l[-1],c[-1])
        pad=S.BREAKOUT_PAD_BP/10_000
        side=None; level=None
        if c[-1]>ph*(1+pad) and br>=S.BODY_RATIO_MIN: side,level="LONG",ph
        elif c[-1]<pl*(1-pad) and br>=S.BODY_RATIO_MIN: side,level="SHORT",pl
        self._progress_retests(sym,tf,o,h,l,c)
        if not side: return None
        sr=self._sweep_reclaim(h,l,c,side)
        self._ret(sym,tf).append(RetestState(level,side,S.RETEST_BARS,S.RETEST_MAX_BP))
        atr_val = atr(list(h), list(l), list(c), S.ATR_LEN)
        rng=max(h[-1]-l[-1],1e-12)
        if side=="LONG":
            upper_wick=(h[-1]-max(c[-1],o[-1]))/rng
            if upper_wick > S.WICK_SIDE_MAX: return None
        else:
            lower_wick=(min(c[-1],o[-1])-l[-1])/rng
            if lower_wick > S.WICK_SIDE_MAX: return None
        return Signal(symbol=sym, tf=tf, side=side, price=c[-1], level=level, body_ratio=br,
                      sweep_reclaim=sr, atr_val=atr_val, htf_bias_ok=True)

# ------------- Telegram -------------
class Telegram:
    def __init__(self, token:str, chat_id:str):
        self.api=f"https://api.telegram.org/bot{token}"; self.chat_id=chat_id
    async def send(self, text:str):
        async with aiohttp.ClientSession() as s:
            async with s.post(f"{self.api}/sendMessage", json={"chat_id": self.chat_id, "text": text,
                                                               "parse_mode":"HTML","disable_web_page_preview": True}) as r:
                if r.status!=200: print("Telegram error:", r.status, await r.text())

# ------------- HTTP server -------------
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
            "symbols":self.state.get("symbols",[]),
            "timeframes":self.state.get("timeframes",[]),
            "open_trades": list(OPEN_TRADES.keys()),
            "today_stats": DAILY_STATS.get(local_day_key(), {}),
        })

# ------------- Stats & helpers -------------
def local_day_key(dt: Optional[datetime]=None) -> str:
    d = riyadh_now() if dt is None else to_tz(dt, S.TZ)
    return d.strftime("%Y-%m-%d")

STATE={"start_time":int(time.time()),"last_kline":{},"last_alert":{},"symbols":[], "timeframes":S.TIMEFRAMES}
OB=OrderBookTracker()
BE=BreakoutEngine(S)
CVD = CVDTracker(S.CVD_WINDOW_SEC) if S.ENABLE_CVD else None
DEP = DepthTracker(S.WALL_MEDIAN_WINDOW) if S.ENABLE_WALLS else None

LAST_TRADE_TS: Dict[str, int] = {}
OPEN_TRADES: Dict[str, Dict[str, Any]] = {}

DAILY_STATS: Dict[str, Dict[str, Any]] = defaultdict(lambda: {
    "count": 0, "wins_tp2": 0, "wins_ts": 0, "losses_sl": 0,
    "sum_R": 0.0, "best": None, "worst": None
})
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
def seconds_until_next_2200_riyadh() -> float:
    now = riyadh_now(); target = now.replace(hour=22, minute=0, second=0, microsecond=0)
    if now >= target: target = target + timedelta(days=1)
    return (target - now).total_seconds()
async def daily_stats_loop():
    await asyncio.sleep(max(1.0, seconds_until_next_2200_riyadh()))
    while True:
        try: await alert(compose_daily_summary(), "DAILY")
        except Exception as e: log.warning("daily_stats_err", error=str(e))
        await asyncio.sleep(24 * 3600)
def _log_event(obj: Dict[str, Any]):
    try:
        os.makedirs(S.DATA_DIR, exist_ok=True)
        with open(os.path.join(S.DATA_DIR,"events.jsonl"),"ab") as f:
            f.write(orjson.dumps(obj)+b"\n")
    except Exception: pass

# ----------- OI relaxed -----------
async def fetch_oi_relaxed_ok(symbol: str, side: str) -> bool:
    try:
        hist = await BINANCE_CLIENT.open_interest_hist(symbol, period=S.OI_LOOKBACK, limit=30)
        cur  = await BINANCE_CLIENT.open_interest(symbol)
        if not hist or cur is None: return True
        prev = hist[:-1] if len(hist)>1 else hist
        if len(prev) < 5: return True
        mean = sum(prev)/len(prev)
        var  = sum((x-mean)**2 for x in prev)/len(prev)
        std  = var**0.5
        z = 0.0 if std==0 else (cur - mean)/std
        delta_pct = (cur-mean)/mean if mean>0 else 0.0
        dir_ok = (cur >= mean) if side=="LONG" else (cur <= mean)
        return dir_ok and ((z >= S.OI_Z_MIN) or (delta_pct >= S.OI_DELTA_MIN))
    except Exception as e:
        log.warning("oi_check_error", symbol=symbol, error=str(e))
        return True

# ----------- LIVE FORCE EXEC: place MARKET first, then alert -----------
async def live_force_execute(sym: str, side: str, entry: float, sl: float) -> Optional[Dict[str,Any]]:
    if os.getenv("DRY_RUN", str(S.DRY_RUN)).lower() == "true":
        return {"dry": True, "note": "DRY_RUN"}
    api_key=os.getenv("BINANCE_API_KEY"); api_secret=os.getenv("BINANCE_API_SECRET")
    if not api_key or not api_secret:
        log.warning("force_no_keys"); return None
    try:
        if not _SYMBOL_FILTERS: await load_symbol_filters()
        await ensure_symbol_setup(sym)
        equity = float(os.getenv("ACCOUNT_EQUITY_USDT","0") or 0) or (S.ACCOUNT_EQUITY_USDT or 1000.0)
        qty = compute_qty(sym, entry, sl, equity,
                          float(os.getenv("MAX_RISK_PCT", S.MAX_RISK_PCT)),
                          float(os.getenv("NOTIONAL_MAX_PCT","100")),
                          float(os.getenv("LEVERAGE","5")))
        f=_SYMBOL_FILTERS.get(sym,{}); min_qty=max(f.get("minQty",0.0),0.0)
        px = entry if entry>0 else (OB.top(sym).ask_price+OB.top(sym).bid_price)/2 if OB.top(sym) else entry
        if S.AUTO_RAISE_TO_MIN_NOTIONAL:
            qty = max(qty, min_qty)
            if qty*max(px,1e-9) < 5.0:  # conservative ~$5 min notional
                qty = max(qty, 5.0/max(px,1e-9))
                qty = round_qty(sym, qty)
        if qty <= 0:
            log.warning("force_qty_zero_skip", symbol=sym, entry=entry, sl=sl)
            return None
        # MARKET entry (guaranteed)
        resp = await _fapi("POST","/fapi/v1/order",
            {"symbol":sym,"side":("BUY" if side=="LONG" else "SELL"),"type":"MARKET",
             "quantity": f"{round_qty(sym, qty):.8f}"})
        log.info("force_market_ok", symbol=sym, resp=resp)
        # SL closePosition
        try:
            await place_stop_market_close(sym, side, sl)
        except Exception as e:
            log.warning("force_sl_place_fail", symbol=sym, error=str(e))
        return resp
    except Exception as e:
        log.error("force_market_fail", symbol=sym, error=str(e))
        return None

# ----------- Handlers -----------
async def on_kline(symbol:str, k:dict):
    sym=symbol.upper(); tf=k["i"]
    def _f(x): 
        try: return float(x)
        except: return 0.0
    cndl=Candle(int(k["t"]), _f(k["o"]), _f(k["h"]), _f(k["l"]), _f(k["c"]), _f(k.get("q", k.get("Q", 0))), int(k["T"]))
    BE.add_candle(sym, tf, cndl)
    STATE["last_kline"]={"symbol":sym,"tf":tf,"t":k["T"]}
    sig=BE.on_closed_bar(sym, tf)
    if not sig: return

    # HTF bias for 5m using 15m SMA
    if tf=="5m":
        buf=BE.buffers.get((sym,"15m"))
        if buf and len(buf["c"])>=S.HTF_SMA_LEN:
            htf=sum(list(buf["c"])[-S.HTF_SMA_LEN:])/S.HTF_SMA_LEN
            sig.htf_bias_ok = not ((sig.side=="LONG" and sig.price<=htf) or (sig.side=="SHORT" and sig.price>=htf))
        else: sig.htf_bias_ok=True
    if not getattr(sig,"htf_bias_ok",True): return

    # Volume Z-score (30-sample window)
    buf=BE.buffers.get((sym,tf)); vol_z=0.0
    if buf and len(buf["v"])>=30:
        vols=list(buf["v"]); win=vols[-30:-1]
        if win:
            mean=sum(win)/len(win); var=sum((x-mean)**2 for x in win)/len(win); std=(var**0.5) if var>0 else 1.0
            vol_z=(vols[-1]-mean)/std
    if vol_z < S.VOL_Z_MIN: return

    # OI relaxed check
    if not await fetch_oi_relaxed_ok(sym, sig.side): return

    # CVD slope sign
    if S.ENABLE_CVD and CVD:
        slope=CVD.slope(sym)
        if S.CVD_REQUIRE_SIGN:
            if sig.side=="LONG" and slope <= S.CVD_MIN_SLOPE_ABS: return
            if sig.side=="SHORT" and slope >= -S.CVD_MIN_SLOPE_ABS: return
    else:
        slope=0.0

    # Liquidity wall avoidance
    if S.ENABLE_WALLS and DEP:
        ref = sig.level*(1+S.BREAKOUT_PAD_BP/10_000) if sig.side=="LONG" else sig.level*(1-S.BREAKOUT_PAD_BP/10_000)
        if DEP.has_opposite_wall(sym, sig.side, ref, S.WALL_BAND_BP, S.WALL_MULT): return

    # Cooldown & duplicates
    now_ts=int(time.time())
    if not S.FORCE_EXECUTE_ON_ALERT:  # normal mode
        if now_ts - LAST_TRADE_TS.get(sym,0) < S.COOLDOWN_AFTER_TRADE_SEC: return
        if sym in OPEN_TRADES: return
    else:
        if S.OVERRIDE_COOLDOWN_ON_ALERT:
            LAST_TRADE_TS[sym]=0
        OPEN_TRADES.pop(sym, None)

    # Build plan (R:R 1:2)
    atr_val = sig.atr_val if sig.atr_val and sig.atr_val>0 else (sig.price*0.003)
    pad = S.BREAKOUT_PAD_BP/10_000
    if S.ENTRY_MODE.upper()=="RETEST":
        entry = sig.level*(1+pad) if sig.side=="LONG" else sig.level*(1-pad); entry_note="retest"
    else:
        entry = sig.price; entry_note="market"
    sl = entry - atr_val*S.ATR_SL_MULT if sig.side=="LONG" else entry + atr_val*S.ATR_SL_MULT
    risk = abs(entry-sl); 
    if risk<=0: return
    tp1 = entry + risk if sig.side=="LONG" else entry - risk
    tp2 = entry + 2*risk if sig.side=="LONG" else entry - 2*risk

    # FORCE EXECUTE FIRST (MARKET), THEN ALERT
    order_resp=None
    if S.FORCE_EXECUTE_ON_ALERT:
        order_resp = await live_force_execute(sym, sig.side, entry, sl)

    # Register trade locally (for TP/TS management)
    OPEN_TRADES[sym]={
        "symbol":sym, "side":sig.side, "tf":tf,
        "entry":entry, "sl":sl, "tp1":tp1, "tp2":tp2, "risk":risk,
        "opened_ts":now_ts, "tp1_hit":False, "trail_active":False,
        "trail_peak":None, "trail_dist":atr_val*S.TRAIL_ATR_MULT, "atr_at_entry":atr_val
    }
    LAST_TRADE_TS[sym]=now_ts

    # After execution -> send alert
    tz_dt=to_tz(now_utc(), S.TZ)
    sl_pct=_fmt_pct(sl, entry); tp1_pct=_fmt_pct(tp1, entry); tp2_pct=_fmt_pct(tp2, entry)
    direction="✅ LONG" if sig.side=="LONG" else "❌ SHORT"
    id_txt = f"\nOrder: {order_resp.get('orderId')}" if isinstance(order_resp, dict) and order_resp.get("orderId") else ""
    text=(f"{direction} <b>{sym}</b> <code>{tf}</code>\n"
          f"Entry: <b>{entry:.6f}</b> ({entry_note})\n"
          f"SL: {sl:.6f} ({sl_pct:+.2f}%)  |  TP1: {tp1:.6f} ({tp1_pct:+.2f}%)  |  TP2: {tp2:.6f} ({tp2_pct:+.2f}%)\n"
          f"VolZ: {vol_z:.2f}  CVD slope: {slope:.4f}  HTF: {'✅' if sig.htf_bias_ok else '❌'}  Sweep: {'✅' if sig.sweep_reclaim else '—'}\n"
          f"Time: {tz_dt.isoformat()}{id_txt}")
    await alert(text, sym)
    _log_event({"ts": now_ts, "type":"signal", "symbol":sym, "side":sig.side, "entry":entry, "sl":sl, "tp1":tp1, "tp2":tp2, "force":S.FORCE_EXECUTE_ON_ALERT})

async def on_bookticker(symbol:str, data:dict):
    OB.update_book_ticker(symbol, data)
    sym=symbol.upper()
    trade=OPEN_TRADES.get(sym)
    if not trade: return
    top=OB.top(sym); 
    if not top: return
    mid=(top.ask_price+top.bid_price)/2
    if mid<=0: return
    side=trade["side"]; entry=trade["entry"]; sl=trade["sl"]; tp1=trade["tp1"]; tp2=trade["tp2"]
    trail_dist=trade["trail_dist"]

    if not trade["tp1_hit"]:
        if side=="LONG":
            if mid<=sl: await _close_trade(sym,"SL",mid,entry,trade["risk"]); return
            if mid>=tp1:
                trade["tp1_hit"]=True; trade["trail_active"]=True; trade["trail_peak"]=mid
        else:
            if mid>=sl: await _close_trade(sym,"SL",mid,entry,trade["risk"]); return
            if mid<=tp1:
                trade["tp1_hit"]=True; trade["trail_active"]=True; trade["trail_peak"]=mid
        return

    if side=="LONG":
        if trade["trail_active"]:
            trade["trail_peak"]=max(trade["trail_peak"], mid)
            trail_stop=trade["trail_peak"]-trail_dist
            if mid<=trail_stop:
                await _close_trade(sym,"TS",trail_stop,entry,trade["risk"]); return
        if mid>=tp2:
            await _close_trade(sym,"TP2",tp2,entry,trade["risk"]); return
    else:
        if trade["trail_active"]:
            trade["trail_peak"]=min(trade["trail_peak"], mid) if trade["trail_peak"] is not None else mid
            trail_stop=trade["trail_peak"]+trail_dist
            if mid>=trail_stop:
                await _close_trade(sym,"TS",trail_stop,entry,trade["risk"]); return
        if mid<=tp2:
            await _close_trade(sym,"TP2",tp2,entry,trade["risk"]); return

async def on_aggtrade(symbol:str, data:dict):
    if not S.ENABLE_CVD or not CVD: return
    sym = symbol.upper()
    try:
        p=float(data.get("p",0)); q=float(data.get("q",0)); m=bool(data.get("m",False))
        if q>0: CVD.on_agg(sym, p, q, m)
    except Exception: pass

async def on_depth(symbol:str, data:dict):
    if not S.ENABLE_WALLS or not DEP: return
    try: DEP.on_depth5(symbol.upper(), data)
    except Exception: pass

async def _close_trade(symbol: str, outcome: str, exit_price: float, entry: float, risk: float):
    trade=OPEN_TRADES.pop(symbol, None)
    if not trade: return
    if risk<=0: R=0.0
    else:
        if outcome in ("TP2","TS"):
            R = (exit_price-entry)/risk if trade["side"]=="LONG" else (entry-exit_price)/risk
        else: R=-1.0
    stats_add_trade_result(symbol, R, outcome)
    pl_pct=_fmt_pct(exit_price, entry); dur_min=(int(time.time())-trade["opened_ts"])/60.0
    msg=(f"📌 <b>RESULT</b> {symbol}\n"
         f"Outcome: <b>{outcome}</b>\n"
         f"Entry: {entry:.6f} → Exit: {exit_price:.6f}  (PnL: {pl_pct:+.2f}% | {R:+.2f}R)\n"
         f"TP1 hit: {'✅' if trade.get('tp1_hit') else '—'}\n"
         f"Duration: {dur_min:.1f} min  |  Trail ATR: {trade.get('atr_at_entry'):.6f} × {S.TRAIL_ATR_MULT:.2f}")
    await alert(msg, symbol)
    _log_event({"ts": int(time.time()), "type":"result","symbol":symbol,"outcome":outcome,"entry":entry,"exit":exit_price,"pnl_pct":pl_pct,"R":R,"tp1_hit":trade.get("tp1_hit")})

# ------------- Alerts & Main -------------
async def alert(text:str, symbol:str):
    if not S.TELEGRAM_BOT_TOKEN or not S.TELEGRAM_CHAT_ID:
        log.info("alert_log", text=text); return
    try:
        tg=Telegram(S.TELEGRAM_BOT_TOKEN, S.TELEGRAM_CHAT_ID)
        await tg.send(text)
        STATE["last_alert"]={"symbol":symbol,"text":text,"ts":int(time.time())}
    except Exception as e:
        log.warning("telegram_send_fail", error=str(e))

async def start_http_server(state:dict):
    server=HealthServer(state); await server.start(S.HOST, S.PORT)
    log.info("http_started", host=S.HOST, port=S.PORT)

async def small_backfill(client:"BinanceClient", sym:str, tf:str):
    data=await client.klines(sym, tf, limit=S.BACKFILL_LIMIT)
    for k in data[:-1]:
        o,h,l,c = float(k[1]),float(k[2]),float(k[3]),float(k[4])
        q = float(k[5]) if len(k)>5 else 0.0
        BE.add_candle(sym, tf, Candle(int(k[0]), o, h, l, c, q, int(k[6])))

async def main():
    global REST_SESSION, BINANCE_CLIENT
    log.info("boot", tfs=S.TIMEFRAMES, rr="1:2", trailing_after_tp1=True,
             cooldown_after_trade_sec=S.COOLDOWN_AFTER_TRADE_SEC,
             force_execute_on_alert=S.FORCE_EXECUTE_ON_ALERT)
    asyncio.create_task(start_http_server(STATE))
    asyncio.create_task(daily_stats_loop())

    REST_SESSION=aiohttp.ClientSession()
    BINANCE_CLIENT=BinanceClient(REST_SESSION)

    # Symbols
    if S.SYMBOLS=="ALL" or (isinstance(S.SYMBOLS,str) and S.SYMBOLS.upper()=="ALL"):
        symbols=await discover_perp_usdt_symbols(REST_SESSION, S.MAX_SYMBOLS)
    elif isinstance(S.SYMBOLS,(list,tuple)):
        symbols=list(S.SYMBOLS)
    else:
        symbols=[str(S.SYMBOLS)]
    STATE["symbols"]=symbols
    log.info("symbols_selected", count=len(symbols))

    # Backfill
    for i, sym in enumerate(symbols):
        for tf in S.TIMEFRAMES:
            try: await small_backfill(BINANCE_CLIENT, sym, tf)
            except Exception as e: log.warning("backfill_error", symbol=sym, tf=tf, error=str(e))
        if i % 10 == 0: await asyncio.sleep(0.2)

    # Streams
    ws_multi=WSStreamMulti(symbols, S.TIMEFRAMES, on_kline, on_bookticker, on_aggtrade, on_depth, chunk_size=S.WS_CHUNK_SIZE)
    await ws_multi.run()

if __name__=="__main__":
    try: uvloop.install()
    except Exception: pass
    try: asyncio.run(main())
    except KeyboardInterrupt: pass