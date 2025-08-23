# app.py — Binance USDT-M Scalper (ENTER FIRST → then alert; uses actual fill prices)
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

import asyncio, aiohttp, orjson, time, math, random, hmac, hashlib, urllib.parse
from typing import List, Dict, Any, Optional, Deque, Tuple, Literal
from collections import deque, defaultdict
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
import structlog, uvloop
from aiohttp import web
from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import field_validator

# ---------------- Settings ----------------
class Settings(BaseSettings):
    SYMBOLS: str | List[str] = "ALL"
    TIMEFRAMES: List[str] = ["5m","15m"]
    MAX_SYMBOLS: int = 200
    WS_CHUNK_SIZE: int = 60
    BACKFILL_LIMIT: int = 60

    BREAKOUT_PAD_BP: int = 10
    BODY_RATIO_MIN: float = 0.60
    RETEST_BARS: int = 2
    RETEST_MAX_BP: int = 8
    SWEEP_LOOKBACK: int = 15
    RECLAIM_BUFFER_BP: int = 6
    RECLAIM_BODY_RATIO_MIN: float = 0.55
    HTF_SMA_LEN: int = 50
    WICK_SIDE_MAX: float = 0.40

    VOL_Z_MIN: float = 1.5

    OI_LOOKBACK: str = "5m"
    OI_Z_MIN: float = 1.0
    OI_DELTA_MIN: float = 0.005

    ENABLE_CVD: bool = True
    CVD_WINDOW_SEC: int = 300
    CVD_REQUIRE_SIGN: bool = True
    CVD_MIN_SLOPE_ABS: float = 0.0001

    ENABLE_WALLS: bool = True
    WALL_BAND_BP: int = 15
    WALL_MULT: float = 2.5

    ATR_LEN: int = 14
    ATR_SL_MULT: float = 1.5
    ENTRY_MODE: str = "MARKET"
    MAX_RISK_PCT: float = 1.0
    ACCOUNT_EQUITY_USDT: Optional[float] = None
    DRY_RUN: bool = True
    TRAIL_ATR_MULT: float = 1.0

    COOLDOWN_AFTER_TRADE_SEC: int = 14400

    TRACE_DECISIONS: bool = True
    TRACE_SAMPLE: float = 0.15

    LOG_LEVEL: str = "INFO"
    DATA_DIR: str = "./data"

    TELEGRAM_BOT_TOKEN: Optional[str] = None
    TELEGRAM_CHAT_ID: Optional[str] = None

    HOST: str = "0.0.0.0"
    PORT: int = int(os.getenv("PORT", "8080"))
    TZ: str = "Asia/Riyadh"

    CONSENSUS_ROUTER: bool = False
    SPREAD_BPS_MAX: float = 8.0
    ATR1M_MIN_BPS: float = 5.0
    ATR1M_MAX_BPS: float = 80.0
    RSI_LEN: int = 14
    RSI_HIGH: float = 60.0
    RSI_LOW: float = 40.0
    CONS_MIN: float = 0.60
    PMM_IMB_GREEN: float = 0.60
    PMM_SIZE_BOOST: float = 0.20
    GRID_STEP_BP: float = 25.0
    GRID_SIZE_BOOST: float = 0.10
    MAX_ORDER_VALUE_USDT: Optional[float] = None
    SL_BPS: float = 12.0
    TP1_BPS: float = 30.0
    TRAIL_ATR1M_MULT: float = 1.2
    TIME_STOP_SEC_MIN: int = 180
    TIME_STOP_SEC_MAX: int = 480
    MAX_CONSEC_LOSSES: int = 6
    DAILY_DD_HALT_PCT: float = 5.0
    TOXIC_IMB_THRESH: float = 0.70

    BINANCE_API_KEY: Optional[str] = os.getenv("BINANCE_API_KEY")
    BINANCE_API_SECRET: Optional[str] = os.getenv("BINANCE_API_SECRET")
    MARGIN_TYPE: str = os.getenv("MARGIN_TYPE", "ISOLATED")
    POSITION_MODE: str = os.getenv("POSITION_MODE", "ONE_WAY")
    LEVERAGE: int = int(os.getenv("LEVERAGE", "5"))
    NOTIONAL_MAX_PCT: Optional[float] = float(os.getenv("NOTIONAL_MAX_PCT", "100"))

    SL_WORKING_TYPE: Literal["CONTRACT_PRICE","MARK_PRICE"] = "MARK_PRICE"

    model_config = SettingsConfigDict(env_file=".env", case_sensitive=False)
    @field_validator("SYMBOLS", mode="before")
    @classmethod
    def _split_symbols(cls, v):
        if isinstance(v, str) and v.strip().upper() != "ALL":
            return [s.strip().upper() for s in v.split(",") if s.strip()]
        return v
    @field_validator("TIMEFRAMES", mode="before")
    @classmethod
    def _split_tfs(cls, v):
        if isinstance(v, str):
            return [s.strip() for s in v.split(",") if s.strip()]
        return v

S = Settings()

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
log = setup_logging(S.LOG_LEVEL)

# ---------------- Utils/TA ----------------
def now_utc() -> datetime: return datetime.now(timezone.utc)
def to_tz(dt: datetime, tz: str) -> datetime: return dt.astimezone(ZoneInfo(tz))
def riyadh_now() -> datetime: return to_tz(now_utc(), S.TZ)
def body_ratio(o,h,l,c): rng=max(h-l,1e-12); return abs(c-o)/rng
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
def bps(x: float) -> float: return x*10_000.0
def from_bps(bps_val: float, price: float, side: str, sign: int) -> float:
    delta = price * (bps_val/10_000.0)
    return price + sign*delta
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

FILTERS: Dict[str, Dict[str, float]] = {}

def _to_float(x, default=0.0):
    try: return float(x)
    except: return default

async def load_exchange_info(session: aiohttp.ClientSession):
    global FILTERS
    async with session.get(f"{BASE_REST}/fapi/v1/exchangeInfo") as r:
        r.raise_for_status()
        data = await r.json()
    filters = {}
    for s in data.get("symbols", []):
        sym = s.get("symbol")
        if not sym: continue
        tick = step = min_qty = min_notional = None
        for f in s.get("filters", []):
            t = f.get("filterType")
            if t == "PRICE_FILTER":
                tick = _to_float(f.get("tickSize", "0.0001"), 0.0001)
            elif t == "LOT_SIZE":
                step = _to_float(f.get("stepSize", "0.001"), 0.001)
                min_qty = _to_float(f.get("minQty", "0.0"), 0.0)
            elif t == "MIN_NOTIONAL":
                min_notional = _to_float(f.get("notional", "0.0"), 0.0)
        filters[sym] = {"tickSize": tick or 0.0001, "stepSize": step or 0.001,
                        "minQty": min_qty or 0.0, "minNotional": min_notional or 0.0}
    FILTERS = filters
    return data

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

class BinanceClient:
    def __init__(self, session: aiohttp.ClientSession):
        self.session = session
        self.key = S.BINANCE_API_KEY
        self.secret = (S.BINANCE_API_SECRET or "").encode()
        self.time_offset_ms = 0
    async def sync_time(self):
        async with self.session.get(f"{BASE_REST}/fapi/v1/time") as r:
            r.raise_for_status()
            srv = await r.json()
        self.time_offset_ms = int(srv["serverTime"]) - int(time.time()*1000)
    async def _signed(self, method: str, path: str, params: Dict[str, Any] | None = None):
        if params is None: params = {}
        if abs(self.time_offset_ms) > 5000:
            await self.sync_time()
        params["timestamp"] = int(time.time()*1000) + self.time_offset_ms
        params.setdefault("recvWindow", 5000)
        q = urllib.parse.urlencode(params, doseq=True)
        sig = hmac.new(self.secret, q.encode(), hashlib.sha256).hexdigest()
        headers = {"X-MBX-APIKEY": self.key} if self.key else {}
        url = f"{BASE_REST}{path}?{q}&signature={sig}"
        async with self.session.request(method, url, headers=headers) as r:
            txt = await r.text()
            if r.status != 200:
                raise RuntimeError(f"Binance {method} {path} {r.status}: {txt}")
            return orjson.loads(txt)
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
    async def set_margin_type(self, symbol: str, mtype: str):
        return await self._signed("POST", "/fapi/v1/marginType", {"symbol": symbol, "marginType": mtype})
    async def set_leverage(self, symbol: str, leverage: int):
        return await self._signed("POST", "/fapi/v1/leverage", {"symbol": symbol, "leverage": leverage})
    async def set_position_mode(self, dualSide: bool):
        return await self._signed("POST", "/fapi/v1/positionSide/dual", {"dualSidePosition": "true" if dualSide else "false"})
    async def place_order(self, symbol: str, side: str, type_: str, **kwargs):
        return await self._signed("POST", "/fapi/v1/order", {"symbol": symbol, "side": side, "type": type_, **kwargs})
    async def cancel_all(self, symbol: str):
        return await self._signed("DELETE", "/fapi/v1/allOpenOrders", {"symbol": symbol})
    async def get_balance_usdt(self) -> Optional[float]:
        d = await self._signed("GET", "/fapi/v2/balance", {})
        for it in d:
            if it.get("asset") == "USDT":
                return float(it.get("balance", 0.0))
        return None
    async def get_position_risk(self, symbol: str):
        d = await self._signed("GET", "/fapi/v2/positionRisk", {"symbol": symbol})
        return d[0] if d else None

# Streams
class WSStream:
    def __init__(self, symbols: List[str], timeframes: List[str], on_kline, on_bookticker, on_aggtrade, on_depth):
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
    def __init__(self, all_symbols: List[str], timeframes: List[str], on_kline, on_bookticker, on_aggtrade, on_depth, chunk_size:int):
        self.chunks=[all_symbols[i:i+chunk_size] for i in range(0,len(all_symbols),chunk_size)]
        self.timeframes=timeframes; self.on_kline=on_kline; self.on_bookticker=on_bookticker
        self.on_aggtrade=on_aggtrade; self.on_depth=on_depth
    async def run(self):
        tasks=[asyncio.create_task(WSStream(c,self.timeframes,self.on_kline,self.on_bookticker,self.on_aggtrade,self.on_depth).run()) for c in self.chunks]
        await asyncio.gather(*tasks)

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
    def _sweep_reclaim(self, hs, ls, os, cs, side:str)->bool:
        if len(hs)<self.s.SWEEP_LOOKBACK+1: return False
        prev_high=max(list(hs)[-self.s.SWEEP_LOOKBACK-1:-1]); prev_low=min(list(ls)[-self.s.SWEEP_LOOKBACK-1:-1])
        c, h, l, o = cs[-1], hs[-1], ls[-1], os[-1]
        br = body_ratio(o,h,l,c)
        if side=="LONG":
            swept=h>prev_high; back_inside=c<=prev_high*(1+self.s.RECLAIM_BUFFER_BP/10_000)
            return swept and back_inside and (br>=self.s.RECLAIM_BODY_RATIO_MIN)
        else:
            swept=l<prev_low; back_inside=c>=prev_low*(1-self.s.RECLAIM_BUFFER_BP/10_000)
            return swept and back_inside and (br>=self.s.RECLAIM_BODY_RATIO_MIN)
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
        rng=max(h[-1]-l[-1],1e-12)
        if c[-1]>ph*(1+pad) and br>=S.BODY_RATIO_MIN:
            upper_wick=(h[-1]-max(c[-1],o[-1]))/rng
            if upper_wick<=S.WICK_SIDE_MAX: side,level="LONG",ph
        elif c[-1]<pl*(1-pad) and br>=S.BODY_RATIO_MIN:
            lower_wick=(min(c[-1],o[-1])-l[-1])/rng
            if lower_wick<=S.WICK_SIDE_MAX: side,level="SHORT",pl
        self._progress_retests(sym,tf,o,h,l,c)
        if not side: return None
        sr=self._sweep_reclaim(h,l,o,c,side)
        self._ret(sym,tf).append(RetestState(level,side,S.RETEST_BARS,S.RETEST_MAX_BP))
        atr_val = atr(list(h), list(l), list(c), S.ATR_LEN)
        return Signal(symbol=sym, tf=tf, side=side, price=c[-1], level=level, body_ratio=br,
                      sweep_reclaim=sr, atr_val=atr_val, htf_bias_ok=True)

# ------------- Telegram -------------
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

# ------------- Diagnostics / HTTP -------------
TRACE_DECISIONS = S.TRACE_DECISIONS
TRACE_SAMPLE = float(S.TRACE_SAMPLE)
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
            "last_order": LAST_ORDER,
            "symbols":self.state.get("symbols",[]),
            "timeframes":self.state.get("timeframes",[]),
            "open_trades": list(OPEN_TRADES.keys()),
            "today_stats": DAILY_STATS.get(local_day_key(), {}),
            "drop_reasons": dict(REASONS),
            "recent_drops": list(LAST_DROPS),
        })

def local_day_key(dt: Optional[datetime]=None) -> str:
    d = riyadh_now() if dt is None else to_tz(dt, S.TZ)
    return d.strftime("%Y-%m-%d")

STATE={"start_time":int(time.time()),"last_kline":{},"last_alert":{},"symbols":[], "timeframes":S.TIMEFRAMES}
OB=OrderBookTracker()
BE=BreakoutEngine(S)
CVD = CVDTracker(S.CVD_WINDOW_SEC) if S.ENABLE_CVD else None
DEP = DepthTracker() if S.ENABLE_WALLS else None

LAST_TRADE_TS: Dict[str, int] = {}
OPEN_TRADES: Dict[str, Dict[str, Any]] = {}
CONSEC_LOSSES: int = 0

DAILY_STATS: Dict[str, Dict[str, Any]] = defaultdict(lambda: {
    "count": 0, "wins_tp2": 0, "wins_ts": 0, "losses_sl": 0,
    "sum_R": 0.0, "best": None, "worst": None
})
def stats_add_trade_result(symbol: str, R: float, outcome: str):
    global CONSEC_LOSSES
    key = local_day_key(); st = DAILY_STATS[key]
    st["count"] += 1; st["sum_R"] += R
    if outcome == "TP2": st["wins_tp2"] += 1; CONSEC_LOSSES = 0
    elif outcome == "TS": st["wins_ts"] += 1; CONSEC_LOSSES = 0
    elif outcome == "SL": st["losses_sl"] += 1; CONSEC_LOSSES += 1
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
            f"Best: {best}   Worst: {worst}   Consecutive losses: {CONSEC_LOSSES}")
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
    os.makedirs(S.DATA_DIR, exist_ok=True)
    with open(os.path.join(S.DATA_DIR,"events.jsonl"),"ab") as f:
        f.write(orjson.dumps(obj)+b"\n")

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
        std  = math.sqrt(var) if var>0 else 0.0
        z = 0.0 if std==0 else (cur - mean)/std
        delta_pct = (cur-mean)/mean if mean>0 else 0.0
        step = cur - prev[-1]
        trend_ok = (step >= 0) if side=="LONG" else (step <= 0)
        signal_ok = (z >= S.OI_Z_MIN) or (delta_pct >= S.OI_DELTA_MIN)
        if z >= (S.OI_Z_MIN * 1.5):
            return True
        return signal_ok and trend_ok
    except Exception as e:
        log.warning("oi_check_error", symbol=symbol, error=str(e))
        return True

# ---------- Consensus helpers ----------
def _spread_bps(sym:str)->Optional[float]:
    top = OB.top(sym)
    if not top: return None
    mid = (top.ask_price + top.bid_price)/2.0
    if mid <= 0: return None
    return bps((top.ask_price - top.bid_price)/mid)
def _atr1m_bps(sym:str)->Optional[float]:
    b = BE.buffers.get((sym,"1m"))
    if not b or len(b["c"]) < S.RSI_LEN + 15: return None
    a = atr(list(b["h"]), list(b["l"]), list(b["c"]), 14)
    c = b["c"][-1]
    if a is None or c<=0: return None
    return bps(a / c)
def _rsi_dir_conf(sym:str)->Tuple[Optional[str], float, Optional[float]]:
    b = BE.buffers.get((sym,"1m"))
    if not b: return None, 0.0, None
    r = rsi(list(b["c"]), S.RSI_LEN)
    if r is None: return None, 0.0, None
    if r >= S.RSI_HIGH:
        val = (r - S.RSI_HIGH) / max(1.0, 80.0 - S.RSI_HIGH); conf = max(0.0, min(1.0, val))
        return "LONG", conf, r
    elif r <= S.RSI_LOW:
        val = (S.RSI_LOW - r) / max(1.0, S.RSI_LOW - 20.0); conf = max(0.0, min(1.0, val))
        return "SHORT", conf, r
    else:
        return None, 0.0, r

# ----------- Execution helpers -----------
def _parse_avg_price(order_resp: dict, fallback: float) -> float:
    """
    Try to derive actual fill price from Binance response.
    Priority: avgPrice -> (cumQuote / executedQty) -> fills[] -> fallback
    """
    try:
        ap = order_resp.get("avgPrice")
        if ap is not None and ap != "0":
            return float(ap)
    except Exception:
        pass
    try:
        cum_quote = float(order_resp.get("cumQuote", "0") or 0.0)
        executed = float(order_resp.get("executedQty", "0") or 0.0)
        if executed > 0 and cum_quote > 0:
            return cum_quote / executed
    except Exception:
        pass
    try:
        fills = order_resp.get("fills") or []
        if fills:
            num = sum(float(f["price"]) * float(f.get("qty", f.get("commissionAsset", 0)) or 0.0) for f in fills)
            den = sum(float(f.get("qty", 0.0)) for f in fills)
            if den > 0:
                return num / den
    except Exception:
        pass
    return float(fallback)

async def compute_qty(symbol: str, entry: float, sl: float) -> float:
    risk = abs(entry - sl)
    if risk <= 0: return 0.0
    eq = S.ACCOUNT_EQUITY_USDT
    if not eq and not S.DRY_RUN:
        try: eq = await BINANCE_CLIENT.get_balance_usdt()
        except Exception: eq = None
    if not eq: eq = 1000.0
    risk_cash = eq * (S.MAX_RISK_PCT/100.0)
    qty = risk_cash / risk
    if S.NOTIONAL_MAX_PCT and S.NOTIONAL_MAX_PCT > 0:
        max_notional = eq * (S.NOTIONAL_MAX_PCT/100.0) * float(S.LEVERAGE)
        qty = min(qty, max_notional / max(entry,1e-9))
    _, q = quantize(symbol, None, qty)
    return max(q or 0.0, 0.0)

LAST_ORDER = {"phase": None, "ok": None, "msg": None, "ts": None}
def _ord_status(phase, ok, msg=None):
    global LAST_ORDER
    LAST_ORDER = {"phase": phase, "ok": ok, "msg": (msg or "")[:300], "ts": int(time.time())}

def _is_hedge() -> bool:
    return (S.POSITION_MODE or "ONE_WAY").upper() == "HEDGE"
def _pos_side(side: str) -> str:
    return "LONG" if side == "LONG" else "SHORT"

async def place_entry_and_brackets(sym: str, side: str, entry_price_ref: float, sl_price: float, tp_limit: Optional[float]):
    """
    ENTER FIRST: MARKET (hard-locked) → SL STOP_MARKET → TP LIMIT reduceOnly.
    Returns dict with order ids + qty + entryAvg. DRY_RUN returns placeholders.
    """
    if S.DRY_RUN:
        return {"entryId": None, "slId": None, "tpId": None, "qty": 0.0, "entryAvg": entry_price_ref}

    qty = await compute_qty(sym, entry_price_ref, sl_price)
    f = FILTERS.get(sym, {"minQty":0.0, "minNotional":0.0})
    if qty <= 0 or qty < (f["minQty"] or 0.0):
        raise RuntimeError(f"Computed quantity too small: {qty}")

    # ---------- ENTRY: ALWAYS MARKET ----------
    try:
        entry_kwargs = {}
        if _is_hedge():
            entry_kwargs["positionSide"] = _pos_side(side)
        entry_resp = await BINANCE_CLIENT.place_order(
            sym,
            "BUY" if side=="LONG" else "SELL",
            "MARKET",
            quantity=f"{qty:.10f}",
            **entry_kwargs
        )
        _ord_status("ENTRY", True)
    except Exception as e:
        _ord_status("ENTRY", False, str(e))
        raise
    entry_id = entry_resp.get("orderId")
    entry_avg = _parse_avg_price(entry_resp, entry_price_ref)

    # SL: STOP_MARKET (closePosition in one-way; reduceOnly+positionSide in hedge)
    sl_p, _ = quantize(sym, sl_price, None)
    try:
        sl_kwargs = {"workingType": S.SL_WORKING_TYPE}
        if _is_hedge():
            sl_kwargs.update({"reduceOnly": True, "positionSide": _pos_side(side)})
        else:
            sl_kwargs.update({"closePosition": True})
        sl_resp = await BINANCE_CLIENT.place_order(
            sym,
            "SELL" if side=="LONG" else "BUY",
            "STOP_MARKET",
            stopPrice=f"{(sl_p or sl_price):.10f}",
            **sl_kwargs
        )
        _ord_status("SL", True)
    except Exception as e:
        _ord_status("SL", False, str(e))
        raise
    sl_id = sl_resp.get("orderId")

    # TP: LIMIT reduceOnly (+ positionSide in hedge)
    tp_id = None
    if tp_limit is not None:
        tp_p, _ = quantize(sym, tp_limit, None)
        try:
            tp_kwargs = {"timeInForce":"GTC","quantity":f"{qty:.10f}","reduceOnly":True}
            if _is_hedge():
                tp_kwargs["positionSide"] = _pos_side(side)
            tp_resp = await BINANCE_CLIENT.place_order(
                sym,
                "SELL" if side=="LONG" else "BUY",
                "LIMIT",
                price=f"{(tp_p or tp_limit):.10f}",
                **tp_kwargs
            )
            _ord_status("TP", True)
            tp_id = tp_resp.get("orderId")
        except Exception as e:
            _ord_status("TP", False, str(e))
            raise

    return {"entryId": entry_id, "slId": sl_id, "tpId": tp_id, "qty": qty, "entryAvg": entry_avg}

async def force_close_market(sym: str, side_hint: Optional[str]=None) -> Optional[Tuple[dict, Optional[float]]]:
    """
    Try MARKET reduceOnly close. Return (resp, exit_avg_price) if success.
    If fallback STOP_MARKET is used, exit_avg may be None (unknown until trigger).
    """
    if S.DRY_RUN:
        return None
    pos = await BINANCE_CLIENT.get_position_risk(sym)
    if not pos:
        try: await BINANCE_CLIENT.cancel_all(sym)
        except Exception: pass
        return None
    raw_amt = float(pos.get("positionAmt", "0") or "0")
    if abs(raw_amt) < 1e-12:
        try: await BINANCE_CLIENT.cancel_all(sym)
        except Exception: pass
        return None
    side = "SELL" if raw_amt > 0 else "BUY"
    if side_hint and side_hint in ("BUY","SELL"):
        side = side_hint
    step = (FILTERS.get(sym, {}) or {}).get("stepSize", 0.001) or 0.001
    q = math.floor(abs(raw_amt) / step) * step
    if q <= 0: q = step
    last_err = None
    for attempt in range(3):
        try:
            kwargs = {"reduceOnly": True}
            if _is_hedge():
                kwargs["positionSide"] = "LONG" if raw_amt > 0 else "SHORT"
            resp = await BINANCE_CLIENT.place_order(sym, side, "MARKET", quantity=f"{q:.10f}", **kwargs)
            exit_avg = _parse_avg_price(resp, 0.0) or None
            return resp, exit_avg
        except Exception as e:
            last_err = str(e)
            await asyncio.sleep(0.2 * (attempt + 1))
    try:
        top = OB.top(sym)
        ref = (top.bid_price if side == "SELL" else top.ask_price) if top else None
        sp, _ = quantize(sym, ref or 0.0, None)
        kwargs = {"workingType":"MARK_PRICE"}
        if _is_hedge():
            kwargs.update({"reduceOnly": True, "positionSide": "LONG" if raw_amt > 0 else "SHORT"})
        else:
            kwargs["closePosition"] = True
        resp = await BINANCE_CLIENT.place_order(
            sym, side, "STOP_MARKET",
            stopPrice=f"{(sp or ref or 0.0):.10f}", **kwargs
        )
        return resp, None
    except Exception as e2:
        raise RuntimeError(f"force_close failed; market_err={last_err} stop_err={str(e2)}")

async def place_entry_and_brackets_safe(sym, side, entry_price_ref, sl_price, tp_limit):
    try:
        out = await place_entry_and_brackets(sym, side, entry_price_ref, sl_price, tp_limit)
        return out, None
    except Exception as e:
        return None, str(e)[:500]

# ------------- Handlers -----------
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

    if tf=="5m":
        buf=BE.buffers.get((sym,"15m"))
        if buf and len(buf["c"])>=S.HTF_SMA_LEN:
            htf_closes = list(buf["c"])
            htf_sma = sma(htf_closes, S.HTF_SMA_LEN)
            htf_close = htf_closes[-1]
            htf_slope = htf_close - htf_closes[-S.HTF_SMA_LEN]
            sig.htf_bias_ok = ((sig.side=="LONG"  and htf_close>htf_sma and htf_slope>0) or
                               (sig.side=="SHORT" and htf_close<htf_sma and htf_slope<0))
            if not sig.htf_bias_ok: return

    buf=BE.buffers.get((sym,tf))
    vol_z = 0.0
    if buf and len(buf["v"])>=30:
        vols=list(buf["v"]); win=vols[-30:-1]
        if win:
            mean=sum(win)/len(win); var=sum((x-mean)**2 for x in win)/len(win); std=math.sqrt(var) if var>0 else 1.0
            vol_z=(vols[-1]-mean)/std
    if vol_z < S.VOL_Z_MIN: return

    if not await fetch_oi_relaxed_ok(sym, sig.side): return

    if S.ENABLE_CVD and CVD:
        slope = CVD.slope(sym)
        if S.CVD_REQUIRE_SIGN:
            if sig.side=="LONG" and slope <= S.CVD_MIN_SLOPE_ABS: return
            if sig.side=="SHORT" and slope >= -S.CVD_MIN_SLOPE_ABS: return
    else:
        slope = 0.0

    if S.ENABLE_WALLS and DEP:
        ref = sig.level * (1+S.BREAKOUT_PAD_BP/10_000) if sig.side=="LONG" else sig.level*(1-S.BREAKOUT_PAD_BP/10_000)
        if DEP.has_opposite_wall(sym, sig.side, ref, S.WALL_BAND_BP, S.WALL_MULT): return

    now_ts = int(time.time())
    if now_ts - LAST_TRADE_TS.get(sym, 0) < S.COOLDOWN_AFTER_TRADE_SEC: return
    if sym in OPEN_TRADES: return

    top = OB.top(sym)
    bid = top.bid_price if top else sig.price
    ask = top.ask_price if top else sig.price
    mid = (bid + ask)/2.0
    atr_val = sig.atr_val if sig.atr_val and sig.atr_val>0 else (sig.price*0.003)
    pad = S.BREAKOUT_PAD_BP / 10_000

    if S.CONSENSUS_ROUTER:
        sp = _spread_bps(sym)
        if sp is None or sp > S.SPREAD_BPS_MAX: return
        a1 = _atr1m_bps(sym)
        if a1 is None or a1 < S.ATR1M_MIN_BPS or a1 > S.ATR1M_MAX_BPS: return
        rsi_dir, rsi_conf, rsi_val = _rsi_dir_conf(sym)
        if rsi_dir is None or rsi_dir != sig.side: return
        br_conf = max(0.0, min(1.0, (sig.body_ratio - S.BODY_RATIO_MIN) / max(1e-6, 1.0 - S.BODY_RATIO_MIN)))
        if rsi_conf < S.CONS_MIN or br_conf < S.CONS_MIN: return

        entry = sig.level * (1 + pad) if sig.side=="LONG" else sig.level * (1 - pad)
        sl    = from_bps(S.SL_BPS, entry, sig.side, -1 if sig.side=="LONG" else +1)
        tp1   = from_bps(S.TP1_BPS, entry, sig.side, +1 if sig.side=="LONG" else -1)
        a1    = _atr1m_bps(sym) or 10.0
        trail_dist = (a1/10_000.0) * mid * S.TRAIL_ATR1M_MULT
        risk  = abs(entry - sl)

        orders, err = await place_entry_and_brackets_safe(sym=sym, side=sig.side, entry_price_ref=entry, sl_price=sl, tp_limit=tp1)
        if err:
            await alert(f"🚫 ORDER FAILED (Router) {sym} {tf}\n{sig.side} entry≈{entry:.6f} SL≈{sl:.6f} TP1≈{tp1:.6f}\nErr: {err}", sym)
            return

        actual_entry = orders.get("entryAvg", entry)
        OPEN_TRADES[sym] = {
            "symbol": sym, "side": sig.side, "tf": tf,
            "entry": actual_entry, "sl": sl, "tp1": tp1, "tp2": None,
            "risk": abs(actual_entry - sl),
            "opened_ts": now_ts,
            "tp1_hit": False, "trail_active": True, "trail_peak": mid,
            "trail_dist": trail_dist, "atr_at_entry": atr_val,
            "time_stop": now_ts + int(min(max(S.TIME_STOP_SEC_MIN, 240), S.TIME_STOP_SEC_MAX)),
            "order_ids": orders
        }
        LAST_TRADE_TS[sym] = now_ts
        qty_txt = f"{orders.get('qty',0):.4f}" if not S.DRY_RUN else "—"
        await alert(
            f"🛒 EXECUTED (Router) {sym} {tf}\nEntry: <b>{actual_entry:.6f}</b> | SL: <b>{sl:.6f}</b> | TP1: <b>{tp1:.6f}</b>\nQty: {qty_txt}\nVolZ: {vol_z:.2f}",
            sym
        )
        _log_event({"ts": now_ts, "type": "signal_consensus", "symbol": sym, "side": sig.side, "entry": actual_entry, "sl": sl, "tp1": tp1})
        return

    entry = sig.price
    sl    = entry - atr_val * S.ATR_SL_MULT if sig.side=="LONG" else entry + atr_val * S.ATR_SL_MULT
    tp1   = entry + abs(entry - sl) if sig.side=="LONG" else entry - abs(entry - sl)
    tp2   = entry + 2*abs(entry - sl) if sig.side=="LONG" else entry - 2*abs(entry - sl)

    orders, err = await place_entry_and_brackets_safe(sym=sym, side=sig.side, entry_price_ref=entry, sl_price=sl, tp_limit=tp2)
    if err:
        await alert(f"🚫 ORDER FAILED {sym} {tf}\n{sig.side} entry≈{entry:.6f} SL≈{sl:.6f} TP2≈{tp2:.6f}\nErr: {err}", sym)
        return

    actual_entry = orders.get("entryAvg", entry)
    risk = abs(actual_entry - sl)
    OPEN_TRADES[sym] = {
        "symbol": sym, "side": sig.side, "tf": tf,
        "entry": actual_entry, "sl": sl, "tp1": tp1, "tp2": tp2,
        "risk": risk, "opened_ts": now_ts,
        "tp1_hit": False, "trail_active": False, "trail_peak": None,
        "trail_dist": (sig.atr_val if sig.atr_val else entry*0.003) * S.TRAIL_ATR_MULT,
        "atr_at_entry": sig.atr_val if sig.atr_val else entry*0.003,
        "time_stop": None, "order_ids": orders
    }
    LAST_TRADE_TS[sym] = now_ts
    sl_pct=_fmt_pct(sl, actual_entry); tp1_pct=_fmt_pct(tp1, actual_entry); tp2_pct=_fmt_pct(tp2, actual_entry)
    qty_txt = f"{orders.get('qty',0):.4f}" if not S.DRY_RUN else "—"
    await alert(
        f"🛒 EXECUTED {sym} {tf}\nEntry: <b>{actual_entry:.6f}</b>\nSL: <b>{sl:.6f}</b> ({sl_pct:+.2f}%) | TP1: <b>{tp1:.6f}</b> ({tp1_pct:+.2f}%) | TP2: <b>{tp2:.6f}</b> ({tp2_pct:+.2f}%)\nQty: {qty_txt}",
        sym
    )
    _log_event({"ts": now_ts, "type": "signal", "symbol": sym, "side": sig.side, "entry": actual_entry, "sl": sl, "tp1": tp1, "tp2": tp2})

async def on_bookticker(symbol:str, data:dict):
    OB.update_book_ticker(symbol, data)
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

    if S.CONSENSUS_ROUTER and trade.get("time_stop") and int(time.time()) >= trade["time_stop"] and not trade["tp1_hit"]:
        await _close_trade(sym, "TS", mid, entry, trade["risk"]); return

    if not trade["tp1_hit"]:
        if side == "LONG":
            if bid <= sl: await _close_trade(sym, "SL", bid, entry, trade["risk"]); return
            if ask >= tp1:
                trade["tp1_hit"] = True; trade["trail_active"] = True; trade["trail_peak"] = mid
        else:
            if ask >= sl: await _close_trade(sym, "SL", ask, entry, trade["risk"]); return
            if bid <= tp1:
                trade["tp1_hit"] = True; trade["trail_active"] = True; trade["trail_peak"] = mid
        return

    if side == "LONG":
        if trade["trail_active"]:
            tp = trade.get("trail_peak"); tp = mid if tp is None else max(tp, mid)
            trade["trail_peak"] = tp
            trail_stop = tp - trail_dist
            if bid <= trail_stop:
                await _close_trade(sym, "TS", trail_stop, entry, trade["risk"]); return
        if not S.CONSENSUS_ROUTER and tp2 and ask >= tp2:
            await _close_trade(sym, "TP2", tp2, entry, trade["risk"]); return
    else:
        if trade["trail_active"]:
            tp = trade.get("trail_peak"); tp = mid if tp is None else min(tp, mid)
            trade["trail_peak"] = tp
            trail_stop = tp + trail_dist
            if ask >= trail_stop:
                await _close_trade(sym, "TS", trail_stop, entry, trade["risk"]); return
        if not S.CONSENSUS_ROUTER and tp2 and bid <= tp2:
            await _close_trade(sym, "TP2", tp2, entry, trade["risk"]); return

async def on_aggtrade(symbol:str, data:dict):
    if not S.ENABLE_CVD or not CVD: return
    sym = symbol.upper()
    try:
        p = float(data.get("p", 0)); q = float(data.get("q", 0)); m = bool(data.get("m", False))
        if q>0: CVD.on_agg(sym, p, q, m)
    except Exception:
        pass

async def on_depth(symbol:str, data:dict):
    if not S.ENABLE_WALLS or not DEP: return
    sym = symbol.upper()
    try:
        DEP.on_depth5(sym, data)
    except Exception:
        pass

async def _close_trade(symbol: str, outcome: str, exit_price: float, entry: float, risk: float):
    trade = OPEN_TRADES.pop(symbol, None)
    if not trade: return

    # Try to flatten on Binance and take the exchange average price for accuracy
    exchange_closed = True
    close_err = None
    actual_exit = exit_price
    if not S.DRY_RUN:
        try:
            resp = await force_close_market(symbol)
            if isinstance(resp, tuple) and resp:
                _, exit_avg = resp
                if exit_avg:
                    actual_exit = exit_avg
            pos = await BINANCE_CLIENT.get_position_risk(symbol)
            amt = 0.0 if not pos else float(pos.get("positionAmt", "0") or "0")
            exchange_closed = (abs(amt) < 1e-12)
            if not exchange_closed:
                close_err = f"Position still open: {amt}"
        except Exception as e:
            exchange_closed = False
            close_err = str(e)

    # Use actual_exit for PnL/R
    if risk <= 0: R = 0.0
    else:
        if outcome in ("TP2", "TS"):
            R = (actual_exit - entry)/risk if trade["side"]=="LONG" else (entry - actual_exit)/risk
        else:
            R = -1.0
    pl_pct = _fmt_pct(actual_exit, entry)
    dur_min = (int(time.time()) - trade["opened_ts"]) / 60.0
    status_emoji = "✅" if exchange_closed or S.DRY_RUN else "⚠️"
    extra = "" if exchange_closed or S.DRY_RUN else f"\nExchange close failed: {close_err}"

    stats_add_trade_result(symbol, R, outcome)
    msg = (f"{status_emoji} <b>RESULT</b> {symbol}\n"
           f"Outcome: <b>{outcome}</b>\n"
           f"Entry: {entry:.6f} → Exit: {actual_exit:.6f}  (PnL: {pl_pct:+.2f}% | {R:+.2f}R)\n"
           f"TP1 hit: {'✅' if trade.get('tp1_hit') else '—'}\n"
           f"Duration: {dur_min:.1f} min{extra}")
    await alert(msg, symbol)
    _log_event({"ts": int(time.time()), "type": "result", "symbol": symbol, "outcome": outcome, "entry": entry, "exit": actual_exit, "pnl_pct": pl_pct, "R": R, "tp1_hit": trade.get('tp1_hit'), "exchange_closed": exchange_closed, "close_err": close_err})

# ------------- Alerts & Main -------------
async def start_http_server(state:dict):
    server = HealthServer(state); await server.start(S.HOST, S.PORT)
    log.info("http_started", host=S.HOST, port=S.PORT)

async def small_backfill(client:"BinanceClient", sym:str, tf:str):
    data=await client.klines(sym, tf, limit=S.BACKFILL_LIMIT)
    for k in data[:-1]:
        o,h,l,c = float(k[1]),float(k[2]),float(k[3]),float(k[4])
        q_quote = float(k[7]) if len(k)>7 else 0.0
        BE.add_candle(sym, tf, Candle(int(k[0]), o, h, l, c, q_quote, int(k[6])))

async def main():
    global REST_SESSION, BINANCE_CLIENT
    log.info("boot", tfs=S.TIMEFRAMES, rr="1:2", trailing_after_tp1=True, cooldown_after_trade_sec=S.COOLDOWN_AFTER_TRADE_SEC, consensus=S.CONSENSUS_ROUTER)

    asyncio.create_task(start_http_server(STATE))
    asyncio.create_task(daily_stats_loop())

    REST_SESSION = aiohttp.ClientSession()
    BINANCE_CLIENT = BinanceClient(REST_SESSION)
    try:
        await BINANCE_CLIENT.sync_time()
    except Exception as e:
        log.warning("time_sync_warn", error=str(e))
    try:
        await load_exchange_info(REST_SESSION)
    except Exception as e:
        log.warning("exchange_info_warn", error=str(e))

    if S.SYMBOLS=="ALL" or (isinstance(S.SYMBOLS,str) and S.SYMBOLS.upper()=="ALL"):
        symbols=await discover_perp_usdt_symbols(REST_SESSION, S.MAX_SYMBOLS)
    elif isinstance(S.SYMBOLS, (list, tuple)):
        symbols=list(S.SYMBOLS)
    else:
        symbols=[str(S.SYMBOLS)]
    STATE["symbols"]=symbols

    tfs = list(S.TIMEFRAMES)
    if S.CONSENSUS_ROUTER and "1m" not in tfs:
        tfs = ["1m"] + tfs
    STATE["timeframes"]=tfs
    log.info("symbols_selected", count=len(symbols), tfs=tfs)

    try:
        await BINANCE_CLIENT.set_position_mode(dualSide = (S.POSITION_MODE.upper()=="HEDGE"))
    except Exception as e:
        log.warning("position_mode_warn", error=str(e))
    for sym in symbols:
        try:
            await BINANCE_CLIENT.set_margin_type(sym, S.MARGIN_TYPE.upper())
        except Exception as e:
            log.info("margin_type_info", symbol=sym, error=str(e))
        try:
            await BINANCE_CLIENT.set_leverage(sym, int(S.LEVERAGE))
        except Exception as e:
            log.info("leverage_info", symbol=sym, error=str(e))

    for i, sym in enumerate(symbols):
        for tf in tfs:
            try: await small_backfill(BINANCE_CLIENT, sym, tf)
            except Exception as e: log.warning("backfill_error", symbol=sym, tf=tf, error=str(e))
        if i % 10 == 0: await asyncio.sleep(0.2)

    ws_multi=WSStreamMulti(symbols, tfs, on_kline, on_bookticker, on_aggtrade, on_depth, chunk_size=S.WS_CHUNK_SIZE)
    await ws_multi.run()

if __name__=="__main__":
    try: uvloop.install()
    except Exception: pass
    try: asyncio.run(main())
    except KeyboardInterrupt: pass