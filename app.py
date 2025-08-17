# Masterpiece Scalper — Breakout + Regime/Breadth + A-Grade Patterns + Flow/Walls
# Runs on Binance USDT-M Futures public data only (DRY_RUN). Telegram + /healthz included.

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

import asyncio, aiohttp, orjson, math, time, random
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
    # Universe / feeds
    SYMBOLS: str | List[str] = "ALL"
    MAX_SYMBOLS: int = 180
    TIMEFRAMES: List[str] = ["1m","3m","5m","15m"]   # internal default; no need to set in .env
    WS_CHUNK_SIZE: int = 50
    BACKFILL_LIMIT: int = 120

    # Eligibility floors (mute junk)
    MIN_24H_QVOL_USDT: float = 40_000_000
    MIN_1M_NOTIONAL_USDT: float = 150_000
    MIN_TOB_QTY: float = 4_000.0
    MIN_DEPTH10BP_USDT: float = 250_000.0

    # Breakout core
    LOOKBACK_BREAK: int = 22
    BREAKOUT_PAD_BP: int = 10
    BODY_RATIO_MIN: float = 0.60
    RETEST_BARS: int = 2
    RETEST_MAX_BP: int = 8
    SWEEP_LOOKBACK: int = 15
    RECLAIM_BUFFER_BP: int = 6
    RECLAIM_BODY_RATIO_MIN: float = 0.55
    HTF_SMA_LEN: int = 50
    WICK_SIDE_MAX: float = 0.35
    ATR_LEN: int = 14
    ATR_SL_MULT: float = 1.5

    # Flow confirmations (adaptive)
    VOL_Z_MIN_TREND: float = 1.4
    VOL_Z_MIN_CHOP: float = 1.6
    OI_LOOKBACK: str = "5m"
    OI_Z_MIN: float = 0.9
    OI_DELTA_MIN: float = 0.004      # 0.4%
    CVD_WINDOW_SEC: int = 300
    TOB_IMB_MIN: float = 0.02        # mild bias

    # Walls/Depth
    ENABLE_WALLS: bool = True
    WALL_BAND_BP_TREND: int = 12
    WALL_BAND_BP_SHOCK: int = 20
    WALL_MULT: float = 2.5
    WALL_MEDIAN_WINDOW: int = 200

    # BTC regime & breadth
    REGIME_SMA: int = 50
    SHOCK_ATRZ_MIN: float = 1.2          # 5m ATR% z-score
    BREADTH_MIN_SIDE: float = 0.55       # 55% aligned
    BREADTH_STRONG: float = 0.65

    # Time windows (Riyadh local time)
    TRADE_WINDOWS: str = "09:00-15:00,16:00-21:00"  # good hours; A-grade only outside

    # A-grade patterns config
    ENABLE_A1: bool = True
    ENABLE_A2: bool = True
    ENABLE_A3: bool = True

    # Entry/targets
    ENTRY_MODE: str = "RETEST"           # MARKET or RETEST (A2 may MARKET)
    TP2_MULT_TREND: float = 3.0
    TP2_MULT_CHOP: float = 2.0
    TRAIL_ATR_MULT: float = 1.0
    TIME_STOP_BARS: int = 10             # if TP1 not reached in N*5m bars => exit

    # Portfolio & risk
    DRY_RUN: bool = True
    MAX_RISK_PCT: float = 0.8
    ACCOUNT_EQUITY_USDT: Optional[float] = None
    MAX_CONCURRENT: int = 4
    MAX_PER_SIDE: int = 3
    COOLDOWN_SEC: int = 4*3600
    DAILY_DD_HALT_R: float = -4.0
    SYMBOL_FAST_SL_MIN_BARS: int = 2
    SYMBOL_FAST_SL_COOLDOWN_MULT: float = 2.0

    # Execution realism
    SIM_SLIPPAGE_BP: int = 2
    IMPACT_CAP_BP: int = 8

    # Ops
    LOG_LEVEL: str = "INFO"
    DATA_DIR: str = "./data"
    HOST: str = "0.0.0.0"
    PORT: int = int(os.getenv("PORT", "8080"))
    TZ: str = "Asia/Riyadh"

    # Telegram
    TELEGRAM_BOT_TOKEN: Optional[str] = None
    TELEGRAM_CHAT_ID: Optional[str] = None

    model_config = SettingsConfigDict(env_file=".env", case_sensitive=False)
    @field_validator("SYMBOLS", mode="before")
    @classmethod
    def _split_symbols(cls, v):
        if isinstance(v, str) and v.strip().upper() != "ALL":
            return [s.strip().upper() for s in v.split(",") if s.strip()]
        return v

S = Settings()

def setup_logging():
    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.TimeStamper(fmt="iso", utc=True),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.JSONRenderer(serializer=orjson.dumps),
        ],
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )
    return structlog.get_logger()
log = setup_logging()

# ---------------- Utils/TA ----------------
def now_utc() -> datetime: return datetime.now(timezone.utc)
def to_tz(dt: datetime, tz: str) -> datetime: return dt.astimezone(ZoneInfo(tz))
def riyadh_now() -> datetime: return to_tz(now_utc(), S.TZ)
def in_trade_windows(dt: datetime) -> bool:
    try:
        parts = [w.strip() for w in S.TRADE_WINDOWS.split(",") if w.strip()]
        hhmm = dt.strftime("%H:%M")
        for p in parts:
            lo,hi = p.split("-")
            if lo <= hhmm <= hi: return True
    except Exception: return True
    return False
def body_ratio(o,h,l,c): rng=max(h-l,1e-12); return abs(c-o)/rng
def atr(highs: List[float], lows: List[float], closes: List[float], length: int) -> Optional[float]:
    if len(closes) < length + 1: return None
    trs=[]
    for i in range(1,len(closes)):
        trs.append(max(highs[i]-lows[i], abs(highs[i]-closes[i-1]), abs(lows[i]-closes[i-1])))
    if len(trs) < length: return None
    return sum(trs[-length:]) / length
def sma(vals: List[float], n:int)->Optional[float]:
    if n<=0 or len(vals)<n: return None
    return sum(vals[-n:])/n
def pct(a,b): 
    if b==0: return 0.0
    return (a/b-1.0)*100.0

# ---------------- Data structs ----------------
class Candle:
    __slots__=("open_time","open","high","low","close","volume","close_time")
    def __init__(self, ot,o,h,l,c,v,ct):
        self.open_time=ot; self.open=o; self.high=h; self.low=l; self.close=c; self.volume=v; self.close_time=ct
class BookTop:
    __slots__=("bid_price","bid_qty","ask_price","ask_qty")
    def __init__(self, bp,bq,ap,aq): self.bid_price=bp; self.bid_qty=bq; self.ask_price=ap; self.ask_qty=aq
class Signal:
    def __init__(self, **kw): self.__dict__.update(kw)

# ---------------- Binance endpoints ----------------
BASE_REST = "https://fapi.binance.com"
BASE_WS   = "wss://fstream.binance.com/stream"
REST_SESSION: Optional[aiohttp.ClientSession] = None

async def discover_perp_usdt_symbols(session: aiohttp.ClientSession, max_symbols:int) -> List[str]:
    async with session.get(f"{BASE_REST}/fapi/v1/exchangeInfo") as r:
        r.raise_for_status()
        data = await r.json()
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
        self.s = session
    async def klines(self, symbol:str, interval:str, limit:int=200):
        p={"symbol":symbol.upper(),"interval":interval,"limit":limit}
        async with self.s.get(f"{BASE_REST}/fapi/v1/klines", params=p) as r:
            r.raise_for_status(); return await r.json()
    async def ticker_24h(self, symbol:str):
        async with self.s.get(f"{BASE_REST}/fapi/v1/ticker/24hr", params={"symbol":symbol}) as r:
            r.raise_for_status(); return await r.json()
    async def open_interest_hist(self, symbol:str, period:str="5m", limit:int=30)->Optional[List[float]]:
        p={"symbol":symbol.upper(),"period":period,"limit":limit}
        async with self.s.get(f"{BASE_REST}/futures/data/openInterestHist", params=p) as r:
            if r.status!=200: return None
            d=await r.json()
            if not d: return None
            return [float(x["sumOpenInterest"]) for x in d]
    async def open_interest(self, symbol:str)->Optional[float]:
        async with self.s.get(f"{BASE_REST}/fapi/v1/openInterest", params={"symbol":symbol.upper()}) as r:
            if r.status!=200: return None
            d=await r.json(); return float(d.get("openInterest", 0.0))

# ---------------- Live trackers ----------------
class OrderBookTracker:
    def __init__(self): self.tops:Dict[str,BookTop]={}
    def update(self, sym:str, d:dict):
        self.tops[sym] = BookTop(float(d.get("b",0)),float(d.get("B",0)),float(d.get("a",0)),float(d.get("A",0)))
    def top(self, sym:str)->Optional[BookTop]: return self.tops.get(sym)

class CVDTracker:
    def __init__(self, window_sec:int=300):
        self.window_sec=window_sec
        self.buff:Dict[str,Deque[Tuple[float,float]]] = defaultdict(lambda: deque())
    def on_agg(self, sym:str, price:float, qty:float, buyer_is_maker:bool):
        sign = -1.0 if buyer_is_maker else +1.0
        ts = time.time()
        if qty>0: self.buff[sym].append((ts, sign*qty))
        cutoff = ts - self.window_sec
        q = self.buff[sym]
        while q and q[0][0] < cutoff: q.popleft()
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
        med=self._median_top(sym); 
        if med<=0: return False
        band=ref_price*(band_bp/10_000)
        if side=="LONG":
            wall_sz=sum(q for p,q in d["asks"] if ref_price < p <= ref_price+band)
            return wall_sz > mult*med
        else:
            wall_sz=sum(q for p,q in d["bids"] if ref_price-band <= p < ref_price)
            return wall_sz > mult*med
    def depth10bp_notional(self, sym:str, mid:float)->float:
        d=self.last_depth.get(sym); 
        if not d or mid<=0: return 0.0
        band=mid*0.001
        bids=sum(p*q for p,q in d["bids"] if mid-band<=p<mid)
        asks=sum(p*q for p,q in d["asks"] if mid<p<=mid+band)
        return bids+asks

class ForceOrderTracker:
    """Liquidation bursts detector per symbol."""
    def __init__(self):
        self.events: Dict[str, Deque[Tuple[float,float]]] = defaultdict(lambda: deque(maxlen=200))
    def on_force(self, sym:str, data:dict):
        try:
            ts=time.time()
            s = data.get("o",{})
            notional = float(s.get("q",0))*float(s.get("p",0))
            self.events[sym].append((ts, notional))
        except: pass
    def burst(self, sym:str, win_sec:int=120, mult:float=3.0)->bool:
        q=self.events.get(sym)
        if not q: return False
        now=time.time(); arr=[n for t,n in q if now-t<=win_sec]
        if len(arr)<3: return False
        mean=sum(arr)/len(arr); last=arr[-1]
        return last > mult*mean if mean>0 else False

# ---------------- Engines & buffers ----------------
class Buffers:
    def __init__(self): self.map:Dict[Tuple[str,str],Dict[str,Deque[float]]]={}
    def buf(self, sym, tf):
        k=(sym,tf)
        if k not in self.map:
            self.map[k]={"o":deque(maxlen=700),"h":deque(maxlen=700),"l":deque(maxlen=700),"c":deque(maxlen=700),"v":deque(maxlen=700)}
        return self.map[k]
    def add(self, sym, tf, cndl:Candle):
        b=self.buf(sym,tf)
        b["o"].append(cndl.open); b["h"].append(cndl.high); b["l"].append(cndl.low); b["c"].append(cndl.close); b["v"].append(cndl.volume)

class BreakoutEngine:
    def __init__(self): 
        self.retests:Dict[Tuple[str,str],List[Any]]={}
    class Retest:
        def __init__(self, level:float, side:str, expire:int, pad_bp:int):
            self.level=level; self.side=side; self.remain=expire; self.touched=False; self.await_close=False; self.pad_bp=pad_bp
        def step(self): self.remain-=1; return self.remain>0
        def in_band(self, price:float)->bool:
            band=self.level*(self.pad_bp/10_000); return abs(price-self.level)<=band
    def _ret(self,sym,tf):
        k=(sym,tf)
        if k not in self.retests: self.retests[k]=[]
        return self.retests[k]
    def on_closed(self, sym, tf, o,h,l,c,v)->Optional[Signal]:
        if len(c) < max(25, S.ATR_LEN+1): return None
        N=S.LOOKBACK_BREAK
        ph=max(list(h)[-N-1:-1]); pl=min(list(l)[-N-1:-1])
        br=body_ratio(o[-1],h[-1],l[-1],c[-1])
        pad=S.BREAKOUT_PAD_BP/10_000
        side=None; level=None
        if c[-1]>ph*(1+pad) and br>=S.BODY_RATIO_MIN: side,level="LONG",ph
        elif c[-1]<pl*(1-pad) and br>=S.BODY_RATIO_MIN: side,level="SHORT",pl
        # quick retest tracking
        self._ret(sym,tf).append(BreakoutEngine.Retest(level,side,S.RETEST_BARS,S.RETEST_MAX_BP)) if side else None
        # sweep/reclaim guard via wick-side
        rng=max(h[-1]-l[-1],1e-12)
        if side=="LONG":
            upper=(h[-1]-max(c[-1],o[-1]))/rng
            if upper > S.WICK_SIDE_MAX: return None
        elif side=="SHORT":
            lower=(min(c[-1],o[-1])-l[-1])/rng
            if lower > S.WICK_SIDE_MAX: return None
        if side:
            atrv=atr(list(h),list(l),list(c),S.ATR_LEN)
            return Signal(symbol=sym, tf=tf, side=side, price=c[-1], level=level, body_ratio=br, atr=atrv)
        return None
    def progress_retest(self, sym, tf, o,h,l,c)->Optional[bool]:
        lst=self._ret(sym,tf)
        if not lst: return None
        r=lst[-1]; 
        if r.side=="LONG":
            if r.in_band(l[-1]) and not r.touched: r.touched=True; r.await_close=True; return None
            if r.touched and r.await_close and c[-1]>r.level*(1+S.BREAKOUT_PAD_BP/10_000):
                r.await_close=False; return True
        else:
            if r.in_band(h[-1]) and not r.touched: r.touched=True; r.await_close=True; return None
            if r.touched and r.await_close and c[-1]<r.level*(1-S.BREAKOUT_PAD_BP/10_000):
                r.await_close=False; return True
        return None

# ---------------- Regime & breadth ----------------
def atr_percent_z(h,l,c, look=50)->float:
    if len(c)<look+1: return 0.0
    trs=[]
    for i in range(1,len(c)):
        tr=max(h[i]-l[i], abs(h[i]-c[i-1]), abs(l[i]-c[i-1]))
        trs.append(tr/max(c[i-1],1e-12))
    if len(trs)<look: return 0.0
    vals=trs[-look:]
    mean=sum(vals)/len(vals); var=sum((x-mean)**2 for x in vals)/len(vals); std=math.sqrt(var) if var>0 else 1.0
    return (vals[-1]-mean)/std

def btc_regime(bufs:Buffers, sym:str="BTCUSDT")->str:
    b=bufs.map.get((sym,"15m"))
    if not b or len(b["c"])<S.REGIME_SMA+2: return "Chop"
    c=list(b["c"]); h=list(b["h"]); l=list(b["l"])
    sma50=sma(c,S.REGIME_SMA); sma49=sma(c[:-1],S.REGIME_SMA)
    slope=(sma50 - (sma49 if sma49 else sma50))
    atrz=atr_percent_z(h,l,c,50)
    price=c[-1]
    above=price> (sma50 or price)
    if abs(atrz)>=S.SHOCK_ATRZ_MIN:
        return "ShockUp" if above else "ShockDown"
    if above and slope>=0: return "TrendUp"
    if (not above) and slope<=0: return "TrendDown"
    return "Chop"

class Breadth:
    def __init__(self): self.last_ratio_up=0.0; self.last_ratio_down=0.0
    def update(self, bufs:Buffers, symbols:List[str]):
        ups=downs=elig=0
        for s in symbols:
            b=bufs.map.get((s,"5m")); 
            if not b or len(b["c"])<30: continue
            elig+=1
            c=list(b["c"]); h=list(b["h"]); l=list(b["l"]); v=list(b["v"])
            # volZ
            vols=v[-30:-1]; 
            if not vols: continue
            m=sum(vols)/len(vols); vv=sum((x-m)**2 for x in vols)/len(vols); sd=math.sqrt(vv) if vv>0 else 1.0
            volz=(v[-1]-m)/sd
            if volz<1.5: continue
            N=22
            ph=max(h[-N-1:-1]); pl=min(l[-N-1:-1])
            if c[-1]>ph: ups+=1
            if c[-1]<pl: downs+=1
        if elig>0:
            self.last_ratio_up = ups/elig
            self.last_ratio_down = downs/elig

BREADTH=Breadth()

# ---------------- Eligibility cache ----------------
ELIGIBLE: Dict[str,bool]={}
LAST_24H: Dict[str,dict]={}
ONE_MIN_NOTIONAL: Dict[str,float]=defaultdict(float)   # approx from 1m candle
TOB_EMA: Dict[str,float]=defaultdict(float)

# ---------------- Global state ----------------
STATE={"start_time":int(time.time()),"symbols":[], "last_alert":{}, "regime":"Chop"}
OB=OrderBookTracker()
CVD=CVDTracker(S.CVD_WINDOW_SEC)
DEP=DepthTracker(S.WALL_MEDIAN_WINDOW) if S.ENABLE_WALLS else None
FORCE=ForceOrderTracker()
BUFS=Buffers()
BE=BreakoutEngine()

OPEN_TRADES: Dict[str, Dict[str,Any]]={}
LAST_TRADE_TS: Dict[str,int]=defaultdict(int)
PER_SIDE_OPEN={"LONG":0,"SHORT":0}
TODAY_KEY=lambda: to_tz(now_utc(), S.TZ).strftime("%Y-%m-%d")
DAILY_R: Dict[str,float]=defaultdict(float)
DAILY_STATS: Dict[str, Dict[str, Any]] = defaultdict(lambda: {"count":0,"wins_tp2":0,"wins_ts":0,"losses_sl":0,"sum_R":0.0,"best":None,"worst":None})

# ---------------- Helpers ----------------
def vol_z(buf, length=30)->float:
    v=list(buf["v"])
    if len(v)<length+1: return 0.0
    win=v[-length:-1]; 
    m=sum(win)/len(win); vv=sum((x-m)**2 for x in win)/len(win); sd=math.sqrt(vv) if vv>0 else 1.0
    return (v[-1]-m)/sd

async def oi_ok(symbol:str, side:str, session:BinanceClient)->bool:
    try:
        hist=await session.open_interest_hist(symbol, period=S.OI_LOOKBACK, limit=30)
        cur=await session.open_interest(symbol)
        if not hist or cur is None: return True
        prev=hist[:-1] if len(hist)>1 else hist
        if len(prev)<5: return True
        m=sum(prev)/len(prev); var=sum((x-m)**2 for x in prev)/len(prev); sd=math.sqrt(var) if var>0 else 1.0
        z=(cur-m)/sd if sd>0 else 0.0
        d=(cur-m)/m if m>0 else 0.0
        dir_ok = (cur>=m) if side=="LONG" else (cur<=m)
        return dir_ok and (z>=S.OI_Z_MIN or d>=S.OI_DELTA_MIN)
    except Exception as e:
        log.warning("oi_err", symbol=symbol, err=str(e)); return True

def tob_imb(sym:str)->float:
    t=OB.top(sym)
    if not t: return 0.0
    a=t.ask_qty; b=t.bid_qty
    denom=(a+b) if (a+b)>0 else 1.0
    return (b-a)/denom  # + = bid-dominant

def walls_clear(sym:str, side:str, ref:float, regime:str)->bool:
    if not DEP or not S.ENABLE_WALLS: return True
    band = S.WALL_BAND_BP_SHOCK if "Shock" in regime else S.WALL_BAND_BP_TREND
    return not DEP.has_opposite_wall(sym, side, ref, band, S.WALL_MULT)

def choose_tp2_mult(regime:str)->float:
    return S.TP2_MULT_TREND if regime in ("TrendUp","TrendDown","ShockUp","ShockDown") else S.TP2_MULT_CHOP

def regime_allows_side(regime:str, side:str)->bool:
    if regime=="TrendUp": return side=="LONG"
    if regime=="TrendDown": return side=="SHORT"
    if regime=="ShockUp": return side=="LONG"
    if regime=="ShockDown": return side=="SHORT"
    return True

def breadth_allows_side(side:str)->bool:
    up=BREADTH.last_ratio_up; dn=BREADTH.last_ratio_down
    if side=="LONG": return up>=S.BREADTH_MIN_SIDE
    else: return dn>=S.BREADTH_MIN_SIDE

def risk_unit_usdt(entry:float, sl:float)->float:
    if not S.ACCOUNT_EQUITY_USDT or S.ACCOUNT_EQUITY_USDT<=0: return 0.0
    risk_usdt = S.ACCOUNT_EQUITY_USDT * (S.MAX_RISK_PCT/100.0)
    return risk_usdt

def calc_qty(entry:float, sl:float)->float:
    risk_usdt=risk_unit_usdt(entry, sl)
    if risk_usdt<=0: return 0.0
    risk_per_unit=abs(entry-sl)
    return risk_usdt/max(risk_per_unit,1e-12)

def impact_ok(sym:str, side:str, qty:float, ref:float)->bool:
    if qty<=0 or not DEP: return True
    d=DEP.last_depth.get(sym)
    if not d or ref<=0: return True
    # approximate VWAP with up to 5 levels
    levels = d["asks"] if side=="LONG" else d["bids"]
    remain=qty; cost=0.0; got=0.0
    if side=="LONG":
        for p,q in levels:
            take=min(remain,q); cost+=p*take; got+=take; remain-=take
            if remain<=0: break
    else:
        # selling into bids
        for p,q in levels:
            take=min(remain,q); cost+=p*take; got+=take; remain-=take
            if remain<=0: break
    if got<=0 or remain>0: return False  # too thin
    vwap=cost/got
    bps=abs(vwap-ref)/ref*10_000
    return bps <= S.IMPACT_CAP_BP

def add_stat(symbol:str, R:float, outcome:str):
    k=TODAY_KEY(); d=DAILY_STATS[k]
    d["count"]+=1; d["sum_R"]+=R
    if outcome=="TP2": d["wins_tp2"]+=1
    elif outcome=="TS": d["wins_ts"]+=1
    elif outcome=="SL": d["losses_sl"]+=1
    if d.get("best") is None or R > d["best"][1]: d["best"]=(symbol,R)
    if d.get("worst") is None or R < d["worst"][1]: d["worst"]=(symbol,R)
    DAILY_R[k]=d["sum_R"]

def compose_daily()->str:
    k=TODAY_KEY(); st=DAILY_STATS.get(k, {})
    if not st or st["count"]==0: return f"📅 {k} — No trades."
    wins=st["wins_tp2"]+st["wins_ts"]; losses=st["losses_sl"]
    hit= (wins/st["count"]*100) if st["count"]>0 else 0.0
    avg=(st["sum_R"]/st["count"]) if st["count"]>0 else 0.0
    best=f"{st['best'][0]} {st['best'][1]:+.2f}R" if st.get("best") else "—"
    worst=f"{st['worst'][0]} {st['worst'][1]:+.2f}R" if st.get("worst") else "—"
    return (f"📅 {k}\nTrades: {st['count']} | Wins: {wins} (TP2 {st['wins_tp2']}, TS {st['wins_ts']}) | "
            f"Losses: {losses}\nHit: {hit:.1f}% | Net: {st['sum_R']:+.2f}R | Avg: {avg:+.2f}R\n"
            f"Best: {best}   Worst: {worst}")

# ---------------- Telegram & HTTP ----------------
class Telegram:
    def __init__(self, token:str, chat_id:str):
        self.api=f"https://api.telegram.org/bot{token}"; self.chat_id=chat_id
    async def send(self, text:str):
        async with aiohttp.ClientSession() as s:
            await s.post(f"{self.api}/sendMessage", json={"chat_id":self.chat_id,"text":text,"parse_mode":"HTML","disable_web_page_preview":True})

async def alert(text:str, symbol:str="SYS"):
    if not S.TELEGRAM_BOT_TOKEN or not S.TELEGRAM_CHAT_ID:
        log.info("alert", text=text); return
    try:
        await Telegram(S.TELEGRAM_BOT_TOKEN,S.TELEGRAM_CHAT_ID).send(text)
        STATE["last_alert"]={"symbol":symbol,"ts":int(time.time())}
    except Exception as e:
        log.warning("tg_err", err=str(e))

class Health:
    def __init__(self): 
        self.app=web.Application()
        self.app.add_routes([web.get("/healthz", self.h)])
        self.runner=None
    async def start(self):
        self.runner=web.AppRunner(self.app); await self.runner.setup()
        site=web.TCPSite(self.runner, S.HOST, S.PORT); await site.start()
        log.info("http_started", host=S.HOST, port=S.PORT)
    async def h(self, req):
        return web.json_response({
            "ok":True, "regime": STATE.get("regime","?"),
            "breadth_up": round(BREADTH.last_ratio_up,3), "breadth_down": round(BREADTH.last_ratio_down,3),
            "open_trades": list(OPEN_TRADES.keys()),
            "per_side": PER_SIDE_OPEN, "today_R": DAILY_R.get(TODAY_KEY(),0.0)
        })

# ---------------- Streams ----------------
class WSStream:
    def __init__(self, syms:List[str], on_k, on_bt, on_agg, on_depth, on_force):
        self.syms=[s.lower() for s in syms]; self.on_k=on_k; self.on_bt=on_bt; self.on_agg=on_agg; self.on_depth=on_depth; self.on_force=on_force
    def _streams(self)->str:
        parts=[]
        for s in self.syms:
            for tf in S.TIMEFRAMES: parts.append(f"{s}@kline_{tf}")
            parts.append(f"{s}@bookTicker")
            parts.append(f"{s}@aggTrade")
            parts.append(f"{s}@depth5@100ms")
            parts.append(f"{s}@forceOrder")
        return f"{BASE_WS}?streams={'/'.join(parts)}"
    async def run(self):
        url=self._streams()
        async with aiohttp.ClientSession() as sess:
            async with sess.ws_connect(url, heartbeat=30, autoping=True, compress=15) as ws:
                async for msg in ws:
                    if msg.type==aiohttp.WSMsgType.TEXT:
                        payload=msg.json(); stream=payload.get("stream",""); data=payload.get("data",{})
                        st=stream.split("@")[1] if "@" in stream else ""
                        sym=data.get("s", stream.split("@")[0].upper())
                        if "kline" in st and data.get("k",{}).get("x"): await self.on_k(sym.upper(), data["k"])
                        elif "bookTicker" in st: await self.on_bt(sym.upper(), data)
                        elif "aggTrade" in st: await self.on_agg(sym.upper(), data)
                        elif "depth5" in st: await self.on_depth(sym.upper(), data)
                        elif "forceOrder" in st: await self.on_force(sym.upper(), data)
                    elif msg.type==aiohttp.WSMsgType.ERROR: break

class WSStreamMulti:
    def __init__(self, syms:List[str], on_k, on_bt, on_agg, on_depth, on_force):
        self.chunks=[syms[i:i+S.WS_CHUNK_SIZE] for i in range(0,len(syms),S.WS_CHUNK_SIZE)]
        self.on_k=on_k; self.on_bt=on_bt; self.on_agg=on_agg; self.on_depth=on_depth; self.on_force=on_force
    async def run(self):
        await asyncio.gather(*[asyncio.create_task(WSStream(c,self.on_k,self.on_bt,self.on_agg,self.on_depth,self.on_force).run()) for c in self.chunks])

# ---------------- Eligibility & 24h refresh ----------------
async def refresh_24h(client:BinanceClient, symbols:List[str]):
    for s in symbols:
        try:
            d=await client.ticker_24h(s)
            LAST_24H[s]=d
            qvol=float(d.get("quoteVolume", 0.0))
            ELIGIBLE[s] = qvol >= S.MIN_24H_QVOL_USDT
        except Exception as e:
            log.warning("24h_err", symbol=s, err=str(e))
        await asyncio.sleep(0.05)

async def periodic_24h(client:BinanceClient, symbols:List[str]):
    while True:
        try: await refresh_24h(client, symbols)
        except Exception as e: log.warning("24h_loop_err", err=str(e))
        await asyncio.sleep(300)

# ---------------- Daily stats loop ----------------
def secs_to_22_riyadh()->float:
    n=riyadh_now(); t=n.replace(hour=22,minute=0,second=0,microsecond=0)
    if n>=t: t=t+timedelta(days=1)
    return (t-n).total_seconds()

async def stats_loop():
    await asyncio.sleep(max(1.0, secs_to_22_riyadh()))
    while True:
        try: await alert(compose_daily(), "DAILY")
        except Exception as e: log.warning("stats_err", err=str(e))
        await asyncio.sleep(24*3600)

# ---------------- Handlers ----------------
async def on_kline(sym:str, k:dict):
    tf=k["i"]
    def f(x): 
        try: return float(x)
        except: return 0.0
    cndl=Candle(int(k["t"]), f(k["o"]), f(k["h"]), f(k["l"]), f(k["c"]), f(k.get("q",k.get("Q",0))), int(k["T"]))
    BUFS.add(sym, tf, cndl)

    # 1m notional approx (for eligibility)
    if tf=="1m":
        ONE_MIN_NOTIONAL[sym]=cndl.volume*cndl.close

    # On 5m close: compute regime, breadth, breakout engine
    if tf=="15m" and sym=="BTCUSDT":
        STATE["regime"]=btc_regime(BUFS,"BTCUSDT")
    if tf=="5m":
        BREADTH.update(BUFS, STATE["symbols"])
        buf=BUFS.map.get((sym,"5m"))
        sig=BE.on_closed(sym,"5m", list(buf["o"]),list(buf["h"]),list(buf["l"]),list(buf["c"]),list(buf["v"]))
        if sig: await maybe_trade(sym, sig)

    # progress quick retests
    if tf in ("5m",):
        buf=BUFS.map.get((sym,tf))
        BE.progress_retest(sym, tf, list(buf["o"]),list(buf["h"]),list(buf["l"]),list(buf["c"]))

async def on_bookticker(sym:str, data:dict):
    OB.update(sym, data)
    # trade management on mid updates
    trade=OPEN_TRADES.get(sym); 
    if not trade: return
    t=OB.top(sym); 
    if not t: return
    mid=(t.ask_price+t.bid_price)/2
    if mid<=0: return
    side=trade["side"]; entry=trade["entry"]; sl=trade["sl"]; tp1=trade["tp1"]; tp2=trade["tp2"]
    # pre BE mid drift management
    if not trade["tp1_hit"]:
        if side=="LONG":
            if mid<=sl: return await close_trade(sym,"SL", mid, entry, trade)
            if mid>=tp1:
                trade["tp1_hit"]=True; trade["trail_active"]=True; trade["trail_anchor"]=mid
        else:
            if mid>=sl: return await close_trade(sym,"SL", mid, entry, trade)
            if mid<=tp1:
                trade["tp1_hit"]=True; trade["trail_active"]=True; trade["trail_anchor"]=mid
        return
    # trailing after TP1
    if side=="LONG":
        trade["trail_anchor"]=max(trade["trail_anchor"], mid)
        trail_stop=trade["trail_anchor"] - trade["trail_dist"]
        if mid<=trail_stop: return await close_trade(sym,"TS", trail_stop, entry, trade)
        if mid>=tp2: return await close_trade(sym,"TP2", tp2, entry, trade)
    else:
        trade["trail_anchor"]=min(trade["trail_anchor"], mid)
        trail_stop=trade["trail_anchor"] + trade["trail_dist"]
        if mid>=trail_stop: return await close_trade(sym,"TS", trail_stop, entry, trade)
        if mid<=tp2: return await close_trade(sym,"TP2", tp2, entry, trade)

async def on_aggtrade(sym:str, data:dict):
    try:
        p=float(data.get("p",0)); q=float(data.get("q",0)); m=bool(data.get("m",False))
        if q>0: CVD.on_agg(sym,p,q,m)
    except: pass

async def on_depth(sym:str, data:dict):
    if DEP: DEP.on_depth5(sym, data)

async def on_force(sym:str, data:dict):
    FORCE.on_force(sym, data)

# ---------------- Trade decision ----------------
async def maybe_trade(sym:str, sig:Signal):
    # eligibility floors
    if not ELIGIBLE.get(sym, False): return
    if ONE_MIN_NOTIONAL.get(sym,0.0) < S.MIN_1M_NOTIONAL_USDT: return
    t=OB.top(sym)
    if not t: return
    mid=(t.ask_price+t.bid_price)/2
    if TOB_EMA.get(sym,0.0) < S.MIN_TOB_QTY:
        # Update EMA cheaply
        TOB_EMA[sym]=TOB_EMA.get(sym,0.0)*0.9 + (t.ask_qty+t.bid_qty)*0.1
        return
    if DEP and DEP.depth10bp_notional(sym, mid) < S.MIN_DEPTH10BP_USDT: return

    # time windows
    if not in_trade_windows(riyadh_now()):
        # only A-grade allowed outside; handled in blend below
        pass

    # HTF bias gate for 5m via 15m SMA
    htf_ok=True
    b15=BUFS.map.get((sym,"15m"))
    if b15 and len(b15["c"])>=S.HTF_SMA_LEN:
        htf=sma(list(b15["c"]), S.HTF_SMA_LEN)
        if sig.side=="LONG" and sig.price<= (htf or sig.price): htf_ok=False
        if sig.side=="SHORT" and sig.price>= (htf or sig.price): htf_ok=False
    if not htf_ok: return

    # Regime & breadth
    regime=STATE.get("regime","Chop")
    if not regime_allows_side(regime, sig.side): return
    if not breadth_allows_side(sig.side): return

    # Flow confirmations
    b5=BUFS.map.get((sym,"5m"))
    vZ=vol_z(b5,30) if b5 else 0.0
    vZ_req = S.VOL_Z_MIN_TREND if regime in ("TrendUp","TrendDown","ShockUp","ShockDown") else S.VOL_Z_MIN_CHOP
    if vZ < vZ_req: return

    # OI (eased)
    client=BinanceClient(REST_SESSION)
    if not await oi_ok(sym, sig.side, client): return

    # CVD slope
    cvd=CVD.slope(sym)
    if sig.side=="LONG" and cvd<=0: return
    if sig.side=="SHORT" and cvd>=0: return

    # TOB imbalance
    imb=tob_imb(sym)
    if sig.side=="LONG" and imb < S.TOB_IMB_MIN: return
    if sig.side=="SHORT" and imb > -S.TOB_IMB_MIN: return

    # Opposite walls
    ref = sig.level*(1+S.BREAKOUT_PAD_BP/10_000) if sig.side=="LONG" else sig.level*(1-S.BREAKOUT_PAD_BP/10_000)
    if not walls_clear(sym, sig.side, ref, regime): return

    # A2: liquidation flush-reclaim (optional additional entry path)
    a2_ok=False
    if S.ENABLE_A2 and FORCE.burst(sym,120,3.0):
        # reclaim body & flow flip check (use current cvd sign and vZ already)
        a2_ok = True

    # A3: BTC lead-lag impulse
    a3_ok=False
    if S.ENABLE_A3:
        b_btc=BUFS.map.get(("BTCUSDT","1m"))
        if b_btc and len(b_btc["c"])>=10:
            o1,h1,l1,c1 = b_btc["o"][-1], b_btc["h"][-1], b_btc["l"][-1], b_btc["c"][-1]
            br_btc=body_ratio(o1,h1,l1,c1); vZ_btc=vol_z( BUFS.map.get(("BTCUSDT","1m")), 10)
            if br_btc>=0.6 and vZ_btc>=1.2:
                a3_ok=True

    # Portfolio caps & halts
    if DAILY_R.get(TODAY_KEY(),0.0) <= S.DAILY_DD_HALT_R: return
    if len(OPEN_TRADES)>=S.MAX_CONCURRENT: return
    if PER_SIDE_OPEN[sig.side]>=S.MAX_PER_SIDE: return
    now=int(time.time())
    if now - LAST_TRADE_TS.get(sym,0) < S.COOLDOWN_SEC: return
    if sym in OPEN_TRADES: return
    # time-stop control attaches per trade

    # Entry & plan
    atrv = sig.atr if sig.atr and sig.atr>0 else sig.price*0.003
    pad=S.BREAKOUT_PAD_BP/10_000
    entry_note="retest"
    if (S.ENTRY_MODE.upper()=="RETEST") and (not a2_ok):
        entry = sig.level*(1+pad) if sig.side=="LONG" else sig.level*(1-pad)
    else:
        entry=sig.price; entry_note="market"
    sl = entry - atrv*S.ATR_SL_MULT if sig.side=="LONG" else entry + atrv*S.ATR_SL_MULT
    risk = abs(entry-sl); 
    if risk<=0: return
    tp1 = entry + risk if sig.side=="LONG" else entry - risk
    tp2_mult = choose_tp2_mult(regime)
    tp2 = entry + tp2_mult*risk if sig.side=="LONG" else entry - tp2_mult*risk

    # Sizing / impact check
    qty=0.0; qty_txt="—"
    if S.ACCOUNT_EQUITY_USDT and S.ACCOUNT_EQUITY_USDT>0:
        qty = calc_qty(entry, sl)
        # impact vs depth
        if not impact_ok(sym, sig.side, qty, entry): 
            return
        qty_txt=f"{qty:.4f}"

    # Slippage sim for info
    slip = entry*(S.SIM_SLIPPAGE_BP/10_000)

    # Open trade
    OPEN_TRADES[sym]={
        "symbol":sym,"side":sig.side,"tf":"5m",
        "entry":entry,"sl":sl,"tp1":tp1,"tp2":tp2,"risk":risk,
        "opened_ts":now,"tp1_hit":False,"trail_active":False,
        "trail_anchor":None,"trail_dist":atrv*S.TRAIL_ATR_MULT,
        "time_stop_at": now + S.TIME_STOP_BARS*5*60,
        "fast_sl_bars":0
    }
    LAST_TRADE_TS[sym]=now; PER_SIDE_OPEN[sig.side]+=1

    # Alert
    text=(f"{'✅ LONG' if sig.side=='LONG' else '❌ SHORT'} <b>{sym}</b> <code>5m</code>\n"
          f"Regime: <b>{regime}</b> | Breadth: ↑{BREADTH.last_ratio_up:.2f} ↓{BREADTH.last_ratio_down:.2f}\n"
          f"VolZ: {vZ:.2f}  OI: ok  CVD: {cvd:.3f}  TOB: {imb:+.2f}\n"
          f"Level: {sig.level:.6f}  Entry({entry_note}): <b>{entry:.6f}</b>\n"
          f"SL: {sl:.6f}  TP1: {tp1:.6f}  TP2({tp2_mult:.1f}R): {tp2:.6f}\n"
          f"Trail after TP1: ATR×{S.TRAIL_ATR_MULT:.2f}  Risk%: {S.MAX_RISK_PCT:.2f}%  Qty: {qty_txt}")
    await alert(text, sym)

async def close_trade(sym:str, outcome:str, exit_price:float, entry:float, tr:dict):
    OPEN_TRADES.pop(sym, None)
    PER_SIDE_OPEN[tr["side"]]-=1 if PER_SIDE_OPEN[tr["side"]]>0 else 0
    # R calc
    R = -1.0
    if outcome in ("TP2","TS"):
        R = (exit_price-entry)/tr["risk"] if tr["side"]=="LONG" else (entry-exit_price)/tr["risk"]
    add_stat(sym, R, outcome)
    msg=(f"📌 RESULT {sym} — <b>{outcome}</b>\n"
         f"Entry {entry:.6f} → Exit {exit_price:.6f}  PnL: {pct(exit_price,entry):+.2f}% | {R:+.2f}R\n"
         f"TP1 hit: {'✅' if tr.get('tp1_hit') else '—'}  Trail: {tr.get('trail_dist'):.6f}")
    await alert(msg, sym)

# ---------------- Backfill & bootstrap ----------------
async def backfill(client:BinanceClient, symbols:List[str]):
    for s in symbols:
        for tf in S.TIMEFRAMES:
            try:
                data=await client.klines(s, tf, limit=S.BACKFILL_LIMIT)
                for k in data[:-1]:
                    o,h,l,c=float(k[1]),float(k[2]),float(k[3]),float(k[4]); q=float(k[5]) if len(k)>5 else 0.0
                    BUFS.add(s, tf, Candle(int(k[0]),o,h,l,c,q,int(k[6])))
                if tf=="1m" and data:
                    k=data[-2]; ONE_MIN_NOTIONAL[s]=float(k[7]) if len(k)>7 else float(k[5])*float(k[4])
            except Exception as e:
                log.warning("backfill_err",symbol=s,tf=tf,err=str(e))
        await asyncio.sleep(0.05)

# ---------------- Main ----------------
async def main():
    global REST_SESSION
    log.info("boot", rr="dynamic 1:2..1:3", trail_after_tp1=True, cooldown=S.COOLDOWN_SEC)
    asyncio.create_task(Health().start())
    asyncio.create_task(stats_loop())

    REST_SESSION=aiohttp.ClientSession()
    client=BinanceClient(REST_SESSION)

    # Symbols
    if S.SYMBOLS=="ALL" or (isinstance(S.SYMBOLS,str) and S.SYMBOLS.upper()=="ALL"):
        symbols=await discover_perp_usdt_symbols(REST_SESSION, S.MAX_SYMBOLS)
    elif isinstance(S.SYMBOLS,(list,tuple)):
        symbols=list(S.SYMBOLS)
    else:
        symbols=[str(S.SYMBOLS)]
    if "BTCUSDT" not in symbols: symbols=["BTCUSDT"]+symbols
    STATE["symbols"]=symbols
    await refresh_24h(client, symbols)
    await backfill(client, symbols)
    asyncio.create_task(periodic_24h(client, symbols))

    # Streams
    async def on_k(s,k): await on_kline(s,k)
    async def on_bt(s,d):
        OB.update(s,d)
        # smooth TOB EMA
        t=OB.top(s)
        if t: TOB_EMA[s]=TOB_EMA.get(s, t.ask_qty+t.bid_qty)*0.9 + (t.ask_qty+t.bid_qty)*0.1
        await on_bookticker(s,d)
    async def on_ag(s,d): await on_aggtrade(s,d)
    async def on_dp(s,d): await on_depth(s,d)
    async def on_fc(s,d): await on_force(s,d)

    w=WSStreamMulti(symbols, on_k, on_bt, on_ag, on_dp, on_fc)
    await w.run()

if __name__=="__main__":
    try: uvloop.install()
    except: pass
    try: asyncio.run(main())
    except KeyboardInterrupt: pass