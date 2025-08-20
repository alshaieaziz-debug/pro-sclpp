# app.py — Binance USDT-M Scalper with Optional Consensus Router
# (unchanged strategy) + LIVE TRADING LAYER (signature, sizing caps, lot/tick rounding)

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
    SYMBOLS: str | List[str] = "ALL"
    TIMEFRAMES: List[str] = ["5m","15m"]
    MAX_SYMBOLS: int = 200
    WS_CHUNK_SIZE: int = 60
    BACKFILL_LIMIT: int = 60

    # Breakout & confirmations
    BREAKOUT_PAD_BP: int = 10
    BODY_RATIO_MIN: float = 0.60
    RETEST_BARS: int = 2
    RETEST_MAX_BP: int = 8
    SWEEP_LOOKBACK: int = 15
    RECLAIM_BUFFER_BP: int = 6
    RECLAIM_BODY_RATIO_MIN: float = 0.55
    HTF_SMA_LEN: int = 50
    WICK_SIDE_MAX: float = 0.40

    # Volume (Z-score on QUOTE)
    VOL_Z_MIN: float = 1.5

    # OI (eased)
    OI_LOOKBACK: str = "5m"
    OI_Z_MIN: float = 1.0
    OI_DELTA_MIN: float = 0.005

    # CVD / depth
    ENABLE_CVD: bool = True
    CVD_WINDOW_SEC: int = 300
    CVD_REQUIRE_SIGN: bool = True
    CVD_MIN_SLOPE_ABS: float = 0.0001
    ENABLE_WALLS: bool = True
    WALL_BAND_BP: int = 15
    WALL_MULT: float = 2.5

    # Risk (R:R=1:2)
    ATR_LEN: int = 14
    ATR_SL_MULT: float = 1.5
    ENTRY_MODE: str = "MARKET"           # MARKET | RETEST (post-only limit)
    MAX_RISK_PCT: float = 1.0
    ACCOUNT_EQUITY_USDT: Optional[float] = None
    DRY_RUN: bool = True
    TRAIL_ATR_MULT: float = 1.0

    COOLDOWN_AFTER_TRADE_SEC: int = 14400

    # Diagnostics
    TRACE_DECISIONS: bool = True
    TRACE_SAMPLE: float = 0.15

    # Ops
    LOG_LEVEL: str = "INFO"
    DATA_DIR: str = "./data"

    # Telegram
    TELEGRAM_BOT_TOKEN: Optional[str] = None
    TELEGRAM_CHAT_ID: Optional[str] = None

    # Server
    HOST: str = "0.0.0.0"
    PORT: int = int(os.getenv("PORT", "8080"))
    TZ: str = "Asia/Riyadh"

    # ---------- Consensus Router (optional) ----------
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

    # ---------- LIVE trading knobs ----------
    BINANCE_API_KEY: Optional[str] = None
    BINANCE_API_SECRET: Optional[str] = None
    POSITION_MODE: str = "ONE_WAY"     # ONE_WAY | HEDGE
    MARGIN_TYPE: str = "ISOLATED"      # ISOLATED | CROSS
    LEVERAGE: int = 10
    NOTIONAL_MAX_PCT: float = 100.0    # % of equity * leverage (cap); safety 0.95 applied
    RECV_WINDOW_MS: int = 5000
    POST_ONLY_ON_RETEST: bool = True   # uses GTX

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
def atr(highs, lows, closes, length:int)->Optional[float]:
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

# ---------- SIGNED Futures trading client ----------
class FuturesTrader:
    def __init__(self, key:str, secret:str, recv_window:int=5000):
        self.key=key; self.secret=secret.encode(); self.recv=recv_window
        self.time_offset_ms=0
        self._filters: Dict[str, Dict[str, float]] = {}
        self._setup_done: Dict[str, bool] = {}
    async def _sync_time(self, sess:aiohttp.ClientSession):
        try:
            async with sess.get(f"{BASE_REST}/fapi/v1/time") as r:
                srv = await r.json()
            self.time_offset_ms = int(srv["serverTime"]) - int(time.time()*1000)
        except Exception:
            self.time_offset_ms = 0
    def _ts(self)->int:
        return int(time.time()*1000) + self.time_offset_ms
    def _sign(self, qs:str)->str:
        return hmac.new(self.secret, qs.encode(), hashlib.sha256).hexdigest()
    async def _signed(self, method:str, path:str, params:Dict[str,Any], sess:aiohttp.ClientSession):
        if self.time_offset_ms == 0:
            await self._sync_time(sess)
        params = {k:v for k,v in params.items() if v is not None}
        params["timestamp"] = self._ts()
        params["recvWindow"] = self.recv
        qs = urllib.parse.urlencode(params, doseq=True)
        sig = self._sign(qs)
        url = f"{BASE_REST}{path}?{qs}&signature={sig}"
        headers={"X-MBX-APIKEY": self.key}
        if method=="GET":
            async with sess.get(url, headers=headers) as r:
                txt = await r.text()
                if r.status!=200: raise RuntimeError(f"{r.status} {txt}")
                return orjson.loads(txt)
        elif method=="POST":
            async with sess.post(url, headers=headers) as r:
                txt = await r.text()
                if r.status!=200: raise RuntimeError(f"{r.status} {txt}")
                return orjson.loads(txt)
        else:
            raise RuntimeError("method not supported")
    async def ensure_setup(self, symbol:str, sess:aiohttp.ClientSession):
        if self._setup_done.get(symbol): return
        try:
            # position mode
            await self._signed("POST","/fapi/v1/positionSide/dual",
                               {"dualSidePosition": "false" if S.POSITION_MODE.upper()=="ONE_WAY" else "true"},
                               sess)
        except Exception as e:
            log.warning("ensure_posmode", symbol=symbol, err=str(e))
        try:
            await self._signed("POST","/fapi/v1/marginType",
                               {"symbol":symbol, "marginType": S.MARGIN_TYPE.upper()},
                               sess)
        except Exception as e:
            # ignore if already set
            if "No need to change" not in str(e):
                log.warning("ensure_margin", symbol=symbol, err=str(e))
        try:
            await self._signed("POST","/fapi/v1/leverage",
                               {"symbol":symbol, "leverage": int(S.LEVERAGE)},
                               sess)
        except Exception as e:
            log.warning("ensure_lev", symbol=symbol, err=str(e))
        self._setup_done[symbol]=True
    async def get_equity(self, sess:aiohttp.ClientSession)->float:
        try:
            d = await self._signed("GET","/fapi/v2/balance", {}, sess)
            for row in d:
                if row.get("asset")=="USDT":
                    # prefer availableBalance; fallback to balance
                    ab = float(row.get("availableBalance", row.get("balance", 0)))
                    return max(0.0, ab)
        except Exception as e:
            log.warning("equity_err", err=str(e))
        return 0.0
    async def exchange_filters(self, symbol:str, sess:aiohttp.ClientSession)->Dict[str,float]:
        if symbol in self._filters: return self._filters[symbol]
        async with sess.get(f"{BASE_REST}/fapi/v1/exchangeInfo") as r:
            r.raise_for_status(); info = await r.json()
        for s in info.get("symbols", []):
            if s.get("symbol")==symbol:
                flt = {"stepSize":1.0,"tickSize":1.0,"minQty":0.0,"minNotional":5.0}
                for f in s.get("filters", []):
                    t=f.get("filterType")
                    if t=="LOT_SIZE":
                        flt["stepSize"]=float(f["stepSize"]); flt["minQty"]=float(f["minQty"])
                    elif t in ("PRICE_FILTER","PERCENT_PRICE"):
                        if "tickSize" in f: flt["tickSize"]=float(f["tickSize"])
                    elif t in ("MIN_NOTIONAL","NOTIONAL"):
                        # USDT-M uses NOTIONAL
                        if "notional" in f: flt["minNotional"]=float(f["notional"])
                self._filters[symbol]=flt
                return flt
        raise RuntimeError("symbol filters missing")
    @staticmethod
    def _round_step(x:float, step:float)->float:
        if step<=0: return x
        return math.floor(x/step)*step
    @staticmethod
    def _round_tick(x:float, tick:float)->float:
        if tick<=0: return x
        return round(FuturesTrader._round_step(x, tick), 8)
    async def place(self, symbol:str, side:str, qty:float, order_type:str, price:Optional[float],
                    reduce_only:bool, post_only:bool, sess:aiohttp.ClientSession):
        p = {"symbol":symbol, "side":side, "type":order_type}
        if order_type=="MARKET":
            p["quantity"]=qty
        else:
            p["timeInForce"]="GTX" if post_only else "GTC"
            p["price"]=price
            p["quantity"]=qty
        if reduce_only: p["reduceOnly"]="true"
        p["newOrderRespType"]="RESULT"
        return await self._signed("POST","/fapi/v1/order", p, sess)

TRADER: Optional[FuturesTrader] = None

# Streams per chunk unchanged...
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

# ------------- OB, CVD & Depth (unchanged) -------------
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
        sign = -1.0 if buyer_is_maker else +1.0; ts = time.time()
        dq = sign * qty
        q = self.buff[sym]; q.append((ts, dq))
        cutoff = ts - self.window_sec
        while q and q[0][0] < cutoff: q.popleft()
    def slope(self, sym:str)->float:
        q=self.buff.get(sym)
        if not q or len(q)<2: return 0.0
        total = sum(d for _,d in q); span = q[-1][0]-q[0][0]
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

# ------------- Breakout Engine (unchanged) -------------
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
class Telegram:
    def __init__(self, token:str, chat_id:str):
        self.api=f"https://api.telegram.org/bot{token}"; self.chat_id=chat_id
    async def send(self, text:str):
        async with aiohttp.ClientSession() as s:
            async with s.post(f"{self.api}/sendMessage", json={
                "chat_id": self.chat_id, "text": text, "parse_mode":"HTML", "disable_web_page_preview": True
            }) as r:
                if r.status!=200: print("Telegram error:", r.status, await r.text())

# ------------- Diagnostics -------------
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
            "drop_reasons": dict(REASONS),
            "recent_drops": list(LAST_DROPS),
        })

# ------------- Stats & helpers -------------
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
        if z >= (S.OI_Z_MIN * 1.5): return True
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
        conf = min(1.0, max(0.0, (r - S.RSI_HIGH) / max(1.0, 80.0 - S.RSI_HIGH)))
        return "LONG", conf, r
    elif r <= S.RSI_LOW:
        conf = min(1.0, max(0.0, (S.RSI_LOW - r) / max(1.0, S.RSI_LOW - 20.0)))
        return "SHORT", conf, r
    else:
        return None, 0.0, r
def _pmm_imbalance(sym:str)->Optional[float]:
    top = OB.top(sym)
    if not top: return None
    den = (top.bid_qty + top.ask_qty)
    if den <= 0: return None
    return float(top.bid_qty / den)
def _grid_aligned(price: float, step_bp: float, side: str) -> bool:
    if step_bp <= 0: return False
    step = step_bp/10_000.0
    grid = round(price / (price*step)) * (price*step)
    dist = abs(price - grid)
    return (dist <= price*step*0.15)

# ----------- LIVE: sizing helpers -----------
async def _auto_equity(sess:aiohttp.ClientSession)->float:
    if not TRADER: return 0.0
    return await TRADER.get_equity(sess)

async def _cap_and_round(symbol:str, side:str, desired_qty:float, ref_price:float, sess:aiohttp.ClientSession)->Tuple[Optional[float], Dict[str,float]]:
    """Apply notional cap by equity*leverage, minNotional & lot/tick rounding."""
    filt = await TRADER.exchange_filters(symbol, sess)
    step = float(filt["stepSize"]); minQty=float(filt["minQty"]); tick=float(filt["tickSize"]); minNotional=float(filt["minNotional"])

    # notional cap
    if S.ACCOUNT_EQUITY_USDT and S.ACCOUNT_EQUITY_USDT>0:
        eq = float(S.ACCOUNT_EQUITY_USDT)
    else:
        eq = await _auto_equity(sess)
    lev = max(1, int(S.LEVERAGE))
    cap_notional = eq*lev*(S.NOTIONAL_MAX_PCT/100.0)*0.95  # 5% headroom
    desired_notional = desired_qty*ref_price
    if desired_notional > cap_notional and cap_notional>0:
        desired_qty = cap_notional / max(ref_price,1e-12)

    # round to lot
    qty = FuturesTrader._round_step(max(desired_qty, minQty), step)

    # ensure minNotional after rounding
    if qty*ref_price < max(5.0, minNotional):
        qty = FuturesTrader._round_step((max(5.0, minNotional) / max(ref_price,1e-12)), step)

    if qty < minQty or qty<=0:
        return None, {"minQty":minQty,"step":step,"tick":tick,"minNotional":minNotional,"eq":eq,"lev":lev,"capNotional":cap_notional}

    return qty, {"minQty":minQty,"step":step,"tick":tick,"minNotional":minNotional,"eq":eq,"lev":lev,"capNotional":cap_notional}

# ----------- Handlers -----------
async def on_kline(symbol:str, k:dict):
    sym=symbol.upper(); tf=k["i"]
    def _f(x):
        try: return float(x)
        except: return 0.0
    cndl=Candle(int(k["t"]), _f(k["o"]), _f(k["h"]), _f(k["l"]), _f(k["c"]), _f(k.get("q", 0.0)), int(k["T"]))
    BE.add_candle(sym, tf, cndl)
    STATE["last_kline"]={"symbol":sym,"tf":tf,"t":k["T"]}

    sig=BE.on_closed_bar(sym, tf)
    if not sig:
        _maybe_debug_drop(sym, tf, "no_candidate", {"close": cndl.close})
        return

    # HTF bias for 5m
    if tf=="5m":
        buf=BE.buffers.get((sym,"15m"))
        if buf and len(buf["c"])>=S.HTF_SMA_LEN:
            htf_closes=list(buf["c"])
            htf_sma=sma(htf_closes, S.HTF_SMA_LEN)
            htf_close=htf_closes[-1]
            htf_slope=htf_close - htf_closes[-S.HTF_SMA_LEN]
            sig.htf_bias_ok = ((sig.side=="LONG"  and htf_close>htf_sma and htf_slope>0) or
                               (sig.side=="SHORT" and htf_close<htf_sma and htf_slope<0))
            if not sig.htf_bias_ok:
                _maybe_debug_drop(sym, tf, "htf_bias_block", {"htf_close": round(htf_close,6), "htf_sma": round(htf_sma or 0.0,6), "slope": round(htf_slope,6), "side": sig.side})
                return
        else:
            sig.htf_bias_ok=True

    # quote vol Z
    buf=BE.buffers.get((sym,tf))
    vol_z = 0.0
    if buf and len(buf["v"])>=30:
        vols=list(buf["v"]); win=vols[-30:-1]
        if win:
            mean=sum(win)/len(win); var=sum((x-mean)**2 for x in win)/len(win); std=math.sqrt(var) if var>0 else 1.0
            vol_z=(vols[-1]-mean)/std
    if vol_z < S.VOL_Z_MIN:
        _maybe_debug_drop(sym, tf, "vol_z_low", {"vol_z": round(vol_z,3), "min": S.VOL_Z_MIN})
        return

    if not await fetch_oi_relaxed_ok(sym, sig.side):
        _maybe_debug_drop(sym, tf, "oi_block", {"side": sig.side})
        return

    # CVD sign
    if S.ENABLE_CVD and CVD:
        slope = CVD.slope(sym)
        if S.CVD_REQUIRE_SIGN:
            if sig.side=="LONG" and slope <= S.CVD_MIN_SLOPE_ABS:
                _maybe_debug_drop(sym, tf, "cvd_block", {"slope": round(slope,6), "need": f">{S.CVD_MIN_SLOPE_ABS}"}); return
            if sig.side=="SHORT" and slope >= -S.CVD_MIN_SLOPE_ABS:
                _maybe_debug_drop(sym, tf, "cvd_block", {"slope": round(slope,6), "need": f"<{-S.CVD_MIN_SLOPE_ABS}"}); return
    else:
        slope = 0.0

    # Wall avoid
    if S.ENABLE_WALLS and DEP:
        ref = sig.level * (1+S.BREAKOUT_PAD_BP/10_000) if sig.side=="LONG" else sig.level*(1-S.BREAKOUT_PAD_BP/10_000)
        if DEP.has_opposite_wall(sym, sig.side, ref, S.WALL_BAND_BP, S.WALL_MULT):
            _maybe_debug_drop(sym, tf, "wall_block", {"ref": round(ref,6), "band_bp": S.WALL_BAND_BP, "mult": S.WALL_MULT})
            return

    # Optional consensus router gates (unchanged) ...
    if S.CONSENSUS_ROUTER:
        sp = _spread_bps(sym)
        if sp is None or sp > S.SPREAD_BPS_MAX:
            _maybe_debug_drop(sym, tf, "spread_gate", {"spread_bps": round(sp or -1,2), "max": S.SPREAD_BPS_MAX}); return
        a1 = _atr1m_bps(sym)
        if a1 is None or a1 < S.ATR1M_MIN_BPS or a1 > S.ATR1M_MAX_BPS:
            _maybe_debug_drop(sym, tf, "atr1m_gate", {"atr1m_bps": round(a1 or -1,2), "min": S.ATR1M_MIN_BPS, "max": S.ATR1M_MAX_BPS}); return
        if OB.top(sym) is None or DEP.last_depth.get(sym) is None:
            _maybe_debug_drop(sym, tf, "data_gate", {}); return
        rsi_dir, rsi_conf, rsi_val = _rsi_dir_conf(sym)
        if rsi_dir is None or rsi_dir != sig.side:
            _maybe_debug_drop(sym, tf, "rsi_disagree", {"side": sig.side, "rsi_dir": rsi_dir, "rsi": rsi_val}); return
        br_conf = max(0.0, min(1.0, (sig.body_ratio - S.BODY_RATIO_MIN) / max(1e-6, 1.0 - S.BODY_RATIO_MIN)))
        if rsi_conf < S.CONS_MIN or br_conf < S.CONS_MIN:
            _maybe_debug_drop(sym, tf, "conf_low", {"rsi_conf": round(rsi_conf,2), "br_conf": round(br_conf,2), "min": S.CONS_MIN}); return

    # cooldown/dup
    now_ts = int(time.time())
    if now_ts - LAST_TRADE_TS.get(sym, 0) < S.COOLDOWN_AFTER_TRADE_SEC:
        _maybe_debug_drop(sym, tf, "cooldown", {"cooldown_sec": S.COOLDOWN_AFTER_TRADE_SEC}); return
    if sym in OPEN_TRADES:
        _maybe_debug_drop(sym, tf, "already_open", {}); return

    # ---------- Build plan ----------
    top = OB.top(sym)
    mid = (top.ask_price + top.bid_price)/2.0 if top else sig.price
    atr_val = sig.atr_val if sig.atr_val and sig.atr_val>0 else (sig.price*0.003)
    pad = S.BREAKOUT_PAD_BP / 10_000

    use_router = S.CONSENSUS_ROUTER
    if use_router:
        entry = sig.level * (1 + pad) if sig.side=="LONG" else sig.level * (1 - pad)
        sl = from_bps(S.SL_BPS, entry, sig.side, -1 if sig.side=="LONG" else +1)
        tp1 = from_bps(S.TP1_BPS, entry, sig.side, +1 if sig.side=="LONG" else -1)
        a1 = _atr1m_bps(sym) or 10.0
        trail_dist = (a1/10_000.0) * mid * S.TRAIL_ATR1M_MULT
        risk = abs(entry - sl)
    else:
        if S.ENTRY_MODE.upper() == "RETEST":
            entry = sig.level * (1 + pad) if sig.side == "LONG" else sig.level * (1 - pad)
        else:
            entry = sig.price
        sl = entry - atr_val * S.ATR_SL_MULT if sig.side=="LONG" else entry + atr_val * S.ATR_SL_MULT
        risk = abs(entry - sl)
        tp1 = entry + risk if sig.side=="LONG" else entry - risk
        tp2 = entry + 2*risk if sig.side=="LONG" else entry - 2*risk
        trail_dist = atr_val * S.TRAIL_ATR_MULT

    if risk <= 0:
        _maybe_debug_drop(sym, tf, "zero_risk", {}); return

    # --- qty from risk ---
    q_base = 0.0
    if S.ACCOUNT_EQUITY_USDT and S.ACCOUNT_EQUITY_USDT > 0:
        q_base = (S.ACCOUNT_EQUITY_USDT * (S.MAX_RISK_PCT / 100.0)) / risk
    else:
        # if equity is not set, compute after auto-fetch & cap rounding
        q_base = (max(5.0, risk*10)/risk)  # tiny fallback, will be capped

    # boosters (router only) just affect display; we size by q_base then cap
    q_desired = q_base

    # CAP & ROUND to filters and margin
    async with aiohttp.ClientSession() as sess_tmp:
        if TRADER and not S.DRY_RUN:
            await TRADER.ensure_setup(sym, sess_tmp)
        qty, meta = await _cap_and_round(sym, sig.side, q_desired, entry, sess_tmp)

    if qty is None or qty<=0:
        await alert(f"❗️ORDER BLOCKED {sym}\nReason: qty below exchange limits\n"
                    f"Desired≈{q_desired:.6f}  minQty≈{meta.get('minQty')}  minNotional≈{meta.get('minNotional')}\n"
                    f"Eq≈{meta.get('eq')} Lev={meta.get('lev')} CapNotional≈{meta.get('capNotional'):.2f}", sym)
        return

    # -------- open local trade state --------
    if use_router:
        OPEN_TRADES[sym] = {"symbol": sym, "side": sig.side, "tf": tf,
                            "entry": entry, "sl": sl, "tp1": tp1, "tp2": None,
                            "risk": risk, "opened_ts": now_ts, "tp1_hit": False,
                            "trail_active": False, "trail_peak": None,
                            "trail_dist": trail_dist, "atr_at_entry": atr_val,
                            "time_stop": now_ts + int(min(max(S.TIME_STOP_SEC_MIN, 240), S.TIME_STOP_SEC_MAX))}
    else:
        OPEN_TRADES[sym] = {"symbol": sym, "side": sig.side, "tf": tf,
                            "entry": entry, "sl": sl, "tp1": tp1, "tp2": (None if use_router else (entry + 2*risk if sig.side=='LONG' else entry - 2*risk)),
                            "risk": risk, "opened_ts": now_ts, "tp1_hit": False,
                            "trail_active": False, "trail_peak": None,
                            "trail_dist": trail_dist, "atr_at_entry": atr_val,
                            "time_stop": None}
    LAST_TRADE_TS[sym] = now_ts

    # ---------- place order if LIVE ----------
    placed_txt=""
    if TRADER and not S.DRY_RUN:
        try:
            post_only = (S.ENTRY_MODE.upper()=="RETEST" and S.POST_ONLY_ON_RETEST)
            order_type = "MARKET" if S.ENTRY_MODE.upper()=="MARKET" else "LIMIT"
            limit_price = FuturesTrader._round_tick(entry, (await TRADER.exchange_filters(sym, aiohttp.ClientSession())).get("tickSize", 0.0)) if order_type=="LIMIT" else None
            async with aiohttp.ClientSession() as sess_live:
                await TRADER.ensure_setup(sym, sess_live)
                res = await TRADER.place(sym, "BUY" if sig.side=="LONG" else "SELL",
                                         qty, order_type, limit_price, False, post_only, sess_live)
            placed_txt = f"\n\n<b>LIVE</b> {order_type} placed • Qty {qty}"
        except Exception as e:
            await alert(f"❗️ORDER ERROR {sym} ({sig.side})\n{str(e)}\n"
                        f"Qty:{qty}  Price≈{entry}\n"
                        f"Equity:{meta.get('eq')}  Risk%:{S.MAX_RISK_PCT}  "
                        f"NotionalCap%:{S.NOTIONAL_MAX_PCT} Lev:{S.LEVERAGE}", sym)
            # keep local state so we can “paper manage” if you still want
            placed_txt = "\n\n<b>LIVE</b> order failed — managing as paper."

    # ---------- alerts ----------
    if use_router:
        text=(f"{'✅ LONG' if sig.side=='LONG' else '❌ SHORT'} <b>{sym}</b> <code>{tf}</code> (Consensus)\n"
              f"Price: <b>{sig.price:.6f}</b>  Level: {sig.level:.6f}  Body: {sig.body_ratio:.2f}\n"
              f"VolZ: {vol_z:.2f}  CVD slope: {slope:.4f}  Spread(bps): {(_spread_bps(sym) or 0):.2f}\n"
              f"HTF: {'✅' if sig.htf_bias_ok else '❌'}  Sweep/Reclaim: {'✅' if sig.sweep_reclaim else '—'}\n"
              f"\n<b>Plan</b> ({'limit-maker' if S.ENTRY_MODE.upper()=='RETEST' else 'market'})\n"
              f"Entry: <b>{entry:.6f}</b>  SL(~{S.SL_BPS:.0f}bps): <b>{sl:.6f}</b>  TP1(~{S.TP1_BPS:.0f}bps): <b>{tp1:.6f}</b>\n"
              f"Trail after TP1: ATR(1m)×{S.TRAIL_ATR1M_MULT:.2f}  Qty: {qty:.6f}{placed_txt}")
        await alert(text, sym)
        _log_event({"ts": now_ts, "type": "signal_consensus", "symbol": sym, "side": sig.side, "entry": entry, "sl": sl, "tp1": tp1})
        return

    # original path
    sl_pct=_fmt_pct(sl, entry); tp1_pct=_fmt_pct(tp1, entry)
    tp2 = OPEN_TRADES[sym]["tp2"]
    tp2_pct=_fmt_pct(tp2, entry) if tp2 else 0.0
    text=(f"{'✅ LONG' if sig.side=='LONG' else '❌ SHORT'} <b>{sym}</b> <code>{tf}</code>\n"
          f"Price: <b>{sig.price:.6f}</b>  Level: {sig.level:.6f}  Body: {sig.body_ratio:.2f}\n"
          f"VolZ: {vol_z:.2f}  CVD slope: {slope:.4f}\n"
          f"HTF: {'✅' if sig.htf_bias_ok else '❌'}  Sweep/Reclaim: {'✅' if sig.sweep_reclaim else '—'}\n"
          f"\n<b>Plan</b> ({'retest' if S.ENTRY_MODE.upper()=='RETEST' else 'market'}, R:R=1:2)\n"
          f"Entry: <b>{entry:.6f}</b>\n"
          f"SL: <b>{sl:.6f}</b> ({sl_pct:+.2f}%)  TP1: <b>{tp1:.6f}</b> ({tp1_pct:+.2f}%)\n"
          f"TP2: <b>{tp2:.6f}</b> ({tp2_pct:+.2f}%)\n"
          f"Qty: {qty:.6f}{placed_txt}")
    await alert(text, sym)
    _log_event({"ts": now_ts, "type": "signal", "symbol": sym, "side": sig.side, "entry": entry, "sl": sl, "tp1": tp1, "tp2": tp2})

# ----- on_bookticker / on_aggtrade / on_depth / _close_trade (unchanged) -----
async def on_bookticker(symbol:str, data:dict):
    OB.update_book_ticker(symbol, data)
    sym = symbol.upper()
    trade = OPEN_TRADES.get(sym)
    if not trade: return
    top = OB.top(sym)
    if not top: return
    mid = (top.ask_price + top.bid_price) / 2
    if mid <= 0: return
    side = trade["side"]; entry = trade["entry"]; sl = trade["sl"]; tp1 = trade["tp1"]; tp2 = trade["tp2"]
    trail_dist = trade["trail_dist"]

    if S.CONSENSUS_ROUTER:
        imb = _pmm_imbalance(sym)
        if imb is not None:
            against = (imb < (1.0 - S.TOXIC_IMB_THRESH)) if side=="LONG" else (imb > S.TOXIC_IMB_THRESH)
            if against and not trade["tp1_hit"]:
                if side=="LONG": trade["sl"] = max(trade["sl"], entry - (entry - trade["sl"])*0.5)
                else: trade["sl"] = min(trade["sl"], entry + (trade["sl"] - entry)*0.5)
    if S.CONSENSUS_ROUTER and trade.get("time_stop") and int(time.time()) >= trade["time_stop"] and not trade["tp1_hit"]:
        await _close_trade(sym, "TS", mid, entry, trade["risk"]); return

    if not trade["tp1_hit"]:
        if side == "LONG":
            if mid <= sl: await _close_trade(sym, "SL", mid, entry, trade["risk"]); return
            if mid >= tp1:
                trade["tp1_hit"] = True; trade["trail_active"] = True; trade["trail_peak"] = mid
        else:
            if mid >= sl: await _close_trade(sym, "SL", mid, entry, trade["risk"]); return
            if mid <= tp1:
                trade["tp1_hit"] = True; trade["trail_active"] = True; trade["trail_peak"] = mid
        return

    if side == "LONG":
        if trade["trail_active"]:
            trade["trail_peak"] = max(trade["trail_peak"], mid)
            trail_stop = trade["trail_peak"] - trail_dist
            if mid <= trail_stop: await _close_trade(sym, "TS", trail_stop, entry, trade["risk"]); return
        if not S.CONSENSUS_ROUTER and tp2 and mid >= tp2:
            await _close_trade(sym, "TP2", tp2, entry, trade["risk"]); return
    else:
        if trade["trail_active"]:
            trade["trail_peak"] = min(trade["trail_peak"], mid) if trade["trail_peak"] is not None else mid
            trail_stop = trade["trail_peak"] + trail_dist
            if mid >= trail_stop: await _close_trade(sym, "TS", trail_stop, entry, trade["risk"]); return
        if not S.CONSENSUS_ROUTER and tp2 and mid <= tp2:
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
    if risk <= 0: R = 0.0
    else:
        if outcome in ("TP2", "TS"):
            R = (exit_price - entry)/risk if trade["side"]=="LONG" else (entry - exit_price)/risk
        else: R = -1.0
    stats_add_trade_result(symbol, R, outcome)
    pl_pct = _fmt_pct(exit_price, entry)
    dur_min = (int(time.time()) - trade["opened_ts"]) / 60.0
    msg = (f"📌 <b>RESULT</b> {symbol}\n"
           f"Outcome: <b>{outcome}</b>\n"
           f"Entry: {entry:.6f} → Exit: {exit_price:.6f}  (PnL: {pl_pct:+.2f}% | {R:+.2f}R)\n"
           f"TP1 hit: {'✅' if trade.get('tp1_hit') else '—'}\n"
           f"Duration: {dur_min:.1f} min")
    await alert(msg, symbol)
    _log_event({"ts": int(time.time()), "type": "result", "symbol": symbol, "outcome": outcome, "entry": entry, "exit": exit_price, "pnl_pct": pl_pct, "R": R, "tp1_hit": trade.get("tp1_hit")})

# ------------- Alerts & Main -------------
async def alert(text:str, symbol:str):
    if not S.TELEGRAM_BOT_TOKEN or not S.TELEGRAM_CHAT_ID:
        log.info("alert", text=text); return
    tg=Telegram(S.TELEGRAM_BOT_TOKEN, S.TELEGRAM_CHAT_ID)
    await tg.send(text)
    STATE["last_alert"]={"symbol":symbol,"text":text,"ts":int(time.time())}

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
    global REST_SESSION, BINANCE_CLIENT, TRADER
    log.info("boot", tfs=S.TIMEFRAMES, rr="1:2", trailing_after_tp1=True, cooldown_after_trade_sec=S.COOLDOWN_AFTER_TRADE_SEC, consensus=S.CONSENSUS_ROUTER)
    asyncio.create_task(start_http_server(STATE))
    asyncio.create_task(daily_stats_loop())

    REST_SESSION = aiohttp.ClientSession()
    BINANCE_CLIENT = BinanceClient(REST_SESSION)

    # live trader
    if not S.DRY_RUN and S.BINANCE_API_KEY and S.BINANCE_API_SECRET:
        TRADER = FuturesTrader(S.BINANCE_API_KEY, S.BINANCE_API_SECRET, S.RECV_WINDOW_MS)
    else:
        TRADER = None

    # Symbols
    if S.SYMBOLS=="ALL" or (isinstance(S.SYMBOLS,str) and S.SYMBOLS.upper()=="ALL"):
        symbols=await discover_perp_usdt_symbols(REST_SESSION, S.MAX_SYMBOLS)
    elif isinstance(S.SYMBOLS, (list, tuple)):
        symbols=list(S.SYMBOLS)
    else:
        symbols=[str(S.SYMBOLS)]
    STATE["symbols"]=symbols

    # Timeframes (router needs 1m)
    tfs = list(S.TIMEFRAMES)
    if S.CONSENSUS_ROUTER and "1m" not in tfs:
        tfs = ["1m"] + tfs
    STATE["timeframes"]=tfs
    log.info("symbols_selected", count=len(symbols), tfs=tfs)

    # Backfill
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