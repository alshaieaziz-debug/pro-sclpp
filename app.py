# app.py — Binance USDT-M Scalper (MARKET entry first, on-exchange SL/TP2, fee-aware PnL)
# Paths: Original (ATR-based risk) or Router (percent targets + strict gates)
# Extras: HTF EMA(1h) trend filter, TP1>fees edge check, optional symbol liquidity pruning

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
    # Symbols / streams
    SYMBOLS: str | List[str] = "ALL"       # "ALL" or list "BTCUSDT,ETHUSDT"
    TIMEFRAMES: List[str] = ["1m","5m","15m","1h"]  # include 1h for HTF EMA
    MAX_SYMBOLS: int = 120
    WS_CHUNK_SIZE: int = 60
    BACKFILL_LIMIT: int = 60

    # ---- Original path (ATR risk, R multiples) ----
    ATR_LEN: int = 14
    ATR_SL_MULT: float = 1.8
    TP1_MULT: float = 1.5
    TP2_MULT: float = 3.0
    TRAIL_ATR_MULT: float = 1.8

    # ---- Router path (percent targets + strict gates) ----
    CONSENSUS_ROUTER: bool = False
    ROUTER_SL_PCT: float = 0.35
    ROUTER_TP1_PCT: float = 1.10
    ROUTER_TP2_PCT: Optional[float] = 2.20
    TRAIL_ATR1M_MULT: float = 2.2

    # Router quality gates
    SPREAD_BPS_MAX: float = 3.0
    ATR1M_MIN_BPS: float = 12.0
    ATR1M_MAX_BPS: float = 35.0
    RSI_HIGH: float = 70.0
    RSI_LOW: float = 30.0
    CONS_MIN: float = 0.85
    BODY_RATIO_MIN: float = 0.72
    VOL_Z_MIN: float = 3.0
    CVD_MIN_SLOPE_ABS: float = 0.00035
    BREAKOUT_PAD_BP: int = 15
    WICK_SIDE_MAX: float = 0.30

    # HTF trend filter (1h EMA)
    HTF_EMA_LEN: int = 200

    # Edge check
    MIN_EDGE_PCT: float = 0.05   # TP1 must exceed (fees+slip) by this margin

    # Fees/slippage (percent)
    FEE_TAKER_PCT: float = 0.04
    FEE_MAKER_PCT: float = 0.02
    SLIPPAGE_PCT: float = 0.01

    # Optional liquidity pruning
    MIN_24H_QUOTE_VOL_USDT: float = 0.0     # e.g. 250_000_000
    MIN_OB_NOTIONAL_USDT: float = 0.0       # e.g. 100_000

    # Risk sizing
    DRY_RUN: bool = True
    FIXED_RISK_USDT: Optional[float] = None  # if set, overrides MAX_RISK_PCT/equity
    ACCOUNT_EQUITY_USDT: Optional[float] = None
    MAX_RISK_PCT: float = 1.0        # of equity per trade
    NOTIONAL_MAX_PCT: Optional[float] = None # cap position notional % of equity*leverage
    LEVERAGE: int = int(os.getenv("LEVERAGE","5"))

    # Binance
    BINANCE_API_KEY: Optional[str] = os.getenv("BINANCE_API_KEY")
    BINANCE_API_SECRET: Optional[str] = os.getenv("BINANCE_API_SECRET")
    POSITION_MODE: str = os.getenv("POSITION_MODE","ONE_WAY")  # ONE_WAY/HEDGE

    # Ops
    TELEGRAM_BOT_TOKEN: Optional[str] = None
    TELEGRAM_CHAT_ID: Optional[str] = None
    HOST: str = "0.0.0.0"
    PORT: int = int(os.getenv("PORT","8080"))
    TZ: str = "Asia/Riyadh"
    LOG_LEVEL: str = "INFO"
    DATA_DIR: str = "./data"

    model_config = SettingsConfigDict(env_file=".env", case_sensitive=False)

S = Settings()

# ---------------- Utils & TA ----------------
def now_utc(): return datetime.now(timezone.utc)
def to_tz(dt, tz): return dt.astimezone(ZoneInfo(tz))
def riyadh_now(): return to_tz(now_utc(), S.TZ)

def pct_to_mult(p): return p/100.0
def fees_total_pct(exit_kind:str)->float:
    exit_fee = S.FEE_MAKER_PCT if exit_kind=="maker" else S.FEE_TAKER_PCT
    return S.FEE_TAKER_PCT + exit_fee + S.SLIPPAGE_PCT

def body_ratio(o,h,l,c):
    rng = max(h-l, 1e-12)
    return abs(c-o)/rng

def sma(vals: List[float], n:int)->Optional[float]:
    if n<=0 or len(vals)<n: return None
    return sum(vals[-n:])/n

def ema(vals: List[float], n:int)->Optional[float]:
    if n<=0 or len(vals)<n: return None
    k = 2.0/(n+1.0)
    e = vals[-n]
    for v in vals[-n+1:]:
        e = v*k + e*(1.0-k)
    return e

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

# ---------------- Client & filters ----------------
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
                try:
                    min_notional = float(f.get("notional", "0.0"))
                except:
                    min_notional = float(f.get("minNotional", "0.0"))
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

async def filter_symbols_by_24h_volume(session: aiohttp.ClientSession, symbols: List[str], min_quote_usdt: float) -> List[str]:
    if min_quote_usdt <= 0: return symbols
    kept=[]
    for sym in symbols:
        try:
            async with session.get(f"{BASE_REST}/fapi/v1/ticker/24hr", params={"symbol": sym}) as r:
                if r.status != 200: continue
                d = await r.json()
                q = float(d.get("quoteVolume", "0") or 0.0)
                if q >= min_quote_usdt:
                    kept.append(sym)
        except Exception:
            continue
    return kept
# ---------------- Data structures ----------------
class Candle:
    __slots__=("open_time","open","high","low","close","volume","close_time")
    def __init__(self, ot,o,h,l,c,v,ct):
        self.open_time=ot; self.open=o; self.high=h; self.low=l; self.close=c; self.volume=v; self.close_time=ct

class BookTop:
    __slots__=("bid_price","bid_qty","ask_price","ask_qty")
    def __init__(self, bp,bq,ap,aq): self.bid_price=bp; self.bid_qty=bq; self.ask_price=ap; self.ask_qty=aq

class Signal:
    def __init__(self, **kw): self.__dict__.update(kw)

# ---------------- Order book tracker ----------------
class OrderBookTracker:
    def __init__(self): self.tops:Dict[str,BookTop]={}
    def update_book_ticker(self, sym:str, d:dict):
        self.tops[sym.upper()] = BookTop(float(d.get("b",0)),float(d.get("B",0)),float(d.get("a",0)),float(d.get("A",0)))
    def top(self, sym:str)->Optional[BookTop]: return self.tops.get(sym.upper())

# ---------------- Breakout Engine ----------------
class BreakoutEngine:
    """
    Minimal, robust breakout detector:
      - Prior 20-bar high/low (excl. current)
      - Body ratio >= BODY_RATIO_MIN
      - Breakout pad BREAKOUT_PAD_BP
      - Wick-side max WICK_SIDE_MAX
      - Returns Signal with ATR value (for Original path risk)
    """
    def __init__(self, settings: Settings):
        self.s=settings
        self.buffers:Dict[Tuple[str,str],Dict[str,Deque[float]]]={}

    def _buf(self, sym, tf):
        k=(sym,tf)
        if k not in self.buffers:
            self.buffers[k]={"o":deque(maxlen=800),"h":deque(maxlen=800),"l":deque(maxlen=800),
                             "c":deque(maxlen=800),"v":deque(maxlen=800)}
        return self.buffers[k]

    def add_candle(self, sym, tf, cndl:Candle):
        b=self._buf(sym,tf)
        b["o"].append(cndl.open); b["h"].append(cndl.high); b["l"].append(cndl.low)
        b["c"].append(cndl.close); b["v"].append(cndl.volume)

    @staticmethod
    def _prior_high_low(hs: List[float], ls: List[float], N:int=20)->Tuple[Optional[float],Optional[float]]:
        if len(hs)<N+1 or len(ls)<N+1: return None, None
        return max(hs[-N-1:-1]), min(ls[-N-1:-1])

    def on_closed_bar(self, sym:str, tf:str)->Optional[Signal]:
        b=self._buf(sym,tf)
        o,h,l,c,v=list(b["o"]),list(b["h"]),list(b["l"]),list(b["c"]),list(b["v"])
        if len(c)<max(25, S.ATR_LEN+1): return None

        ph,pl=self._prior_high_low(h,l,20)
        if ph is None: return None

        br=body_ratio(o[-1],h[-1],l[-1],c[-1])
        pad=S.BREAKOUT_PAD_BP/10_000.0
        wick_max=S.WICK_SIDE_MAX
        side=None; level=None
        rng=max(h[-1]-l[-1],1e-12)

        if c[-1]>ph*(1+pad) and br>=S.BODY_RATIO_MIN:
            upper_wick=(h[-1]-max(c[-1],o[-1]))/rng
            if upper_wick<=wick_max: side,level="LONG",ph
        elif c[-1]<pl*(1-pad) and br>=S.BODY_RATIO_MIN:
            lower_wick=(min(c[-1],o[-1])-l[-1])/rng
            if lower_wick<=wick_max: side,level="SHORT",pl

        if not side: return None
        atr_val = atr(h,l,c,S.ATR_LEN)

        # quick volume z-score on last 30 bars of quote volume proxy
        vol_z=0.0
        if len(v)>=30:
            base=v[-30:-1]
            if base:
                mean=sum(base)/len(base)
                var=sum((x-mean)**2 for x in base)/len(base)
                std=math.sqrt(var) if var>0 else 1.0
                vol_z=(v[-1]-mean)/std

        return Signal(symbol=sym, tf=tf, side=side, price=c[-1], level=level,
                      body_ratio=br, atr_val=atr_val, vol_z=vol_z)

# ---------------- HTF EMA(1h) trend filter ----------------
def htf_trend_ok(be: BreakoutEngine, sym:str, side:str, ema_len:int)->bool:
    b = be.buffers.get((sym,"1h"))
    if not b or len(b["c"]) < ema_len+2: return False
    closes = list(b["c"])
    e = ema(closes, ema_len)
    if e is None: return False
    cur = closes[-1]
    prev = closes[-ema_len-1]  # slope proxy over EMA window
    slope = cur - prev
    if side=="LONG":
        return cur > e and slope > 0
    else:
        return cur < e and slope < 0

# ---------------- Micro helpers for gates ----------------
def spread_bps(top: Optional[BookTop])->Optional[float]:
    if not top: return None
    mid=(top.ask_price+top.bid_price)/2.0
    if mid<=0: return None
    return ((top.ask_price-top.bid_price)/mid)*10_000.0  # bps

def atr1m_bps(be: BreakoutEngine, sym:str)->Optional[float]:
    b = be.buffers.get((sym,"1m"))
    if not b or len(b["c"]) < 20: return None
    a = atr(list(b["h"]), list(b["l"]), list(b["c"]), 14)
    c = b["c"][-1]
    if a is None or c<=0: return None
    return (a / c) * 10_000.0

def vol_z_recent(be: BreakoutEngine, sym:str, tf:str)->float:
    b = be.buffers.get((sym,tf))
    if not b or len(b["v"])<30: return 0.0
    v=list(b["v"]); base=v[-30:-1]
    if not base: return 0.0
    mean=sum(base)/len(base); var=sum((x-mean)**2 for x in base)/len(base)
    std=math.sqrt(var) if var>0 else 1.0
    return (v[-1]-mean)/std

# ---------------- Router gate pack ----------------
def router_gate_checks(sym:str, sig:Signal, be:BreakoutEngine, ob:OrderBookTracker)->Tuple[bool,str,Dict[str,Any]]:
    """
    Returns (ok, reason, extras) without side-effects.
    Uses strict gates as configured in Settings.
    """
    extras={}

    # 0) HTF trend
    if not htf_trend_ok(be, sym, sig.side, S.HTF_EMA_LEN):
        return False, "htf_trend_block", extras

    # 1) Spread gate
    top = ob.top(sym)
    sp = spread_bps(top)
    if sp is None: return False, "no_orderbook", extras
    extras["spread_bps"]=round(sp,2)
    if sp > S.SPREAD_BPS_MAX: return False, "spread_gate", extras

    # 2) OB notional (min of bid/ask side)
    if S.MIN_OB_NOTIONAL_USDT and S.MIN_OB_NOTIONAL_USDT>0:
        mid=(top.ask_price+top.bid_price)/2.0
        if mid<=0: return False, "mid_zero", extras
        ob_notional = min(top.bid_qty, top.ask_qty) * mid
        extras["ob_notional"]=round(ob_notional,2)
        if ob_notional < S.MIN_OB_NOTIONAL_USDT: return False, "ob_notional_low", extras

    # 3) ATR(1m) window
    a1 = atr1m_bps(be, sym)
    if a1 is None: return False, "atr1m_none", extras
    extras["atr1m_bps"]=round(a1,2)
    if a1 < S.ATR1M_MIN_BPS or a1 > S.ATR1M_MAX_BPS: return False, "atr1m_gate", extras

    # 4) RSI(1m) agreement + confidence
    b1 = be.buffers.get((sym,"1m"))
    if not b1 or len(b1["c"])<15: return False, "rsi_data", extras
    r = rsi(list(b1["c"]), 14)
    if r is None: return False, "rsi_none", extras
    extras["rsi"]=round(r,1)
    if sig.side=="LONG" and r < S.RSI_HIGH: return False, "rsi_low_for_long", extras
    if sig.side=="SHORT" and r > S.RSI_LOW:  return False, "rsi_high_for_short", extras
    # simple confidence
    if sig.side == "LONG":
        rsi_conf = min(1.0, (r - S.RSI_HIGH) / max(1.0, 80.0 - S.RSI_HIGH))
    else:
        rsi_conf = min(1.0, (S.RSI_LOW - r) / max(1.0, S.RSI_LOW - 20.0))

    # 5) Body ratio confidence
    br_conf = max(0.0, min(1.0, (sig.body_ratio - S.BODY_RATIO_MIN) / max(1e-6, 1.0 - S.BODY_RATIO_MIN)))
    extras["rsi_conf"]=round(rsi_conf,2); extras["br_conf"]=round(br_conf,2)
    if rsi_conf < S.CONS_MIN or br_conf < S.CONS_MIN: return False, "consensus_low", extras

    # 6) Volume Z stricter
    # (Use signal's computed vol_z if present on same TF; otherwise recompute.)
    vz = sig.__dict__.get("vol_z", vol_z_recent(be, sym, sig.tf))
    extras["vol_z"]=round(vz,2)
    if vz < S.VOL_Z_MIN: return False, "volz_low", extras

    # 7) Optional CVD slope would go here (omitted to keep lean)
    return True, "ok", extras

# ---------------- Edge check: TP1 must beat costs ----------------
def tp1_edge_ok(entry: float, tp1: float) -> bool:
    if entry <= 0 or tp1 <= 0: return False
    gross = abs((tp1 - entry) / entry) * 100.0
    need = fees_total_pct("taker") + S.MIN_EDGE_PCT
    return gross > need
# ---------------- Execution helpers ----------------
async def compute_qty(symbol: str, entry: float, sl: float, client: BinanceClient) -> float:
    risk = abs(entry - sl)
    if risk <= 0: return 0.0
    eq = S.ACCOUNT_EQUITY_USDT
    if not eq and not S.DRY_RUN:
        try: eq = await client.get_balance_usdt()
        except Exception: eq = None
    if not eq: eq = 1000.0

    if S.FIXED_RISK_USDT:
        risk_cash = S.FIXED_RISK_USDT
    else:
        risk_cash = eq * (S.MAX_RISK_PCT/100.0)

    qty = risk_cash / risk
    if S.NOTIONAL_MAX_PCT and S.NOTIONAL_MAX_PCT>0:
        max_notional = eq * (S.NOTIONAL_MAX_PCT/100.0) * float(S.LEVERAGE)
        qty = min(qty, max_notional / max(entry,1e-9))
    _, q = quantize(symbol, None, qty)
    return max(q or 0.0, 0.0)

async def place_entry_and_brackets(client: BinanceClient, sym: str, side: str, entry: float, sl: float, tp: Optional[float]):
    """
    Always MARKET entry first, then reduceOnly STOP-MARKET SL + LIMIT TP2
    """
    if S.DRY_RUN:
        return {"entryId": None, "slId": None, "tpId": None, "qty": 1.0}

    qty = await compute_qty(sym, entry, sl, client)
    if qty <= 0: raise RuntimeError("qty<=0")

    # ENTRY: MARKET
    entry_resp = await client.place_order(sym, "BUY" if side=="LONG" else "SELL","MARKET", quantity=f"{qty:.6f}")
    entry_id = entry_resp.get("orderId")

    # Poll fill (simplified)
    await asyncio.sleep(0.3)

    # SL
    sl_price,_ = quantize(sym, sl, None)
    sl_resp = await client.place_order(sym, "SELL" if side=="LONG" else "BUY","STOP_MARKET",
                                       stopPrice=f"{sl_price:.6f}", reduceOnly=True,
                                       quantity=f"{qty:.6f}", workingType="CONTRACT_PRICE")
    sl_id = sl_resp.get("orderId")

    tp_id=None
    if tp:
        tp_price,_ = quantize(sym, tp, None)
        tp_resp = await client.place_order(sym, "SELL" if side=="LONG" else "BUY","LIMIT",
                                           price=f"{tp_price:.6f}", timeInForce="GTC",
                                           quantity=f"{qty:.6f}", reduceOnly=True)
        tp_id=tp_resp.get("orderId")

    return {"entryId": entry_id, "slId": sl_id, "tpId": tp_id, "qty": qty}

async def force_close_market(client: BinanceClient, sym: str):
    if S.DRY_RUN: return None
    try:
        pos = await client.get_position_risk(sym)
        if not pos: return None
        amt = float(pos[0].get("positionAmt","0"))
        if abs(amt)<1e-9: return None
        side = "SELL" if amt>0 else "BUY"
        qty = abs(amt)
        resp = await client.place_order(sym, side,"MARKET",quantity=f"{qty:.6f}", reduceOnly=True)
        await client.cancel_all(sym)
        return resp
    except Exception as e:
        return None

# ---------------- Trade state ----------------
OPEN_TRADES: Dict[str,Dict[str,Any]]={}
LAST_TRADE_TS: Dict[str,int]={}
DAILY_STATS=defaultdict(lambda: {"count":0,"wins":0,"losses":0,"sum_R":0.0})

def local_day_key(dt:Optional[datetime]=None)->str:
    d=riyadh_now() if dt is None else to_tz(dt,S.TZ)
    return d.strftime("%Y-%m-%d")

def stats_add(symbol:str, R:float, outcome:str):
    st=DAILY_STATS[local_day_key()]
    st["count"]+=1; st["sum_R"]+=R
    if R>0: st["wins"]+=1
    else: st["losses"]+=1

# ---------------- PnL with fees ----------------
def compute_R(entry:float, exit:float, sl:float, side:str, risk:float)->float:
    if risk<=0: return 0.0
    raw = (exit-entry) if side=="LONG" else (entry-exit)
    gross_pct = raw/entry*100.0
    net_pct = gross_pct - fees_total_pct("taker")  # assume taker exit
    return (net_pct/ (risk/entry*100.0))

# ---------------- Alerts ----------------
class Telegram:
    def __init__(self, token, chat_id):
        self.api=f"https://api.telegram.org/bot{token}"
        self.chat_id=chat_id
    async def send(self,text:str)->bool:
        try:
            async with aiohttp.ClientSession() as s:
                async with s.post(f"{self.api}/sendMessage",json={
                    "chat_id":self.chat_id,"text":text,"parse_mode":"HTML","disable_web_page_preview":True
                }) as r:
                    return r.status==200
        except: return False

async def alert(text:str):
    if not S.TELEGRAM_BOT_TOKEN or not S.TELEGRAM_CHAT_ID:
        print(text); return True
    tg=Telegram(S.TELEGRAM_BOT_TOKEN,S.TELEGRAM_CHAT_ID)
    return await tg.send(text)

# ---------------- HTTP server ----------------
class HealthServer:
    def __init__(self,state:dict):
        self.app=web.Application(); self.state=state
        self.app.add_routes([web.get("/healthz", self.health)])
    async def start(self,host:str,port:int):
        runner=web.AppRunner(self.app); await runner.setup()
        site=web.TCPSite(runner,host=host,port=port); await site.start()
    async def health(self,req):
        return web.json_response({
            "ok":True,"open_trades":list(OPEN_TRADES.keys()),
            "today_stats":DAILY_STATS.get(local_day_key(),{}),
            "state":self.state
        })
# ---------------- Runtime state ----------------
STATE = {"start_ts": int(time.time()), "last_kline": {}, "last_signal": {}}
REST_SESSION: Optional[aiohttp.ClientSession] = None
BINANCE_CLIENT: Optional[BinanceClient] = None
OB: Optional["OrderBookTracker"] = None
BE: Optional["BreakoutEngine"] = None

# ---------------- WS streams ----------------
BASE_WS = "wss://fstream.binance.com/stream"

class WSStream:
    def __init__(self, symbols: List[str], timeframes: List[str], on_kline, on_bookticker):
        self.symbols=[s.lower() for s in symbols]
        self.timeframes=timeframes
        self.on_kline=on_kline
        self.on_bookticker=on_bookticker
    def _streams(self)->str:
        parts=[]
        for s in self.symbols:
            for tf in self.timeframes:
                parts.append(f"{s}@kline_{tf}")
            parts.append(f"{s}@bookTicker")
        return f"{BASE_WS}?streams={'/'.join(parts)}"
    async def run(self):
        url=self._streams()
        async with aiohttp.ClientSession() as sess:
            async with sess.ws_connect(url, heartbeat=30, autoping=True, compress=15) as ws:
                async for msg in ws:
                    if msg.type==aiohttp.WSMsgType.TEXT:
                        payload=msg.json(); stream=payload.get("stream",""); data=payload.get("data",{})
                        if "kline" in stream:
                            k = data.get("k",{})
                            if k.get("x"):  # closed
                                await self.on_kline(data["s"], k)
                        elif "bookTicker" in stream:
                            await self.on_bookticker(data["s"], data)
                    elif msg.type==aiohttp.WSMsgType.ERROR:
                        break

class WSStreamMulti:
    def __init__(self, symbols: List[str], timeframes: List[str], on_kline, on_bookticker, chunk_size:int):
        self.chunks=[symbols[i:i+chunk_size] for i in range(0,len(symbols),chunk_size)]
        self.timeframes=timeframes
        self.on_kline=on_kline
        self.on_bookticker=on_bookticker
    async def run(self):
        tasks=[asyncio.create_task(WSStream(chunk,self.timeframes,self.on_kline,self.on_bookticker).run()) for chunk in self.chunks]
        await asyncio.gather(*tasks)

# ---------------- Backfill ----------------
async def small_backfill(session:aiohttp.ClientSession, be:"BreakoutEngine", sym:str, tf:str, limit:int):
    try:
        async with session.get(f"{BASE_REST}/fapi/v1/klines", params={"symbol":sym,"interval":tf,"limit":limit}) as r:
            r.raise_for_status()
            data = await r.json()
        for k in data[:-1]:
            o,h,l,c = float(k[1]),float(k[2]),float(k[3]),float(k[4])
            q_quote = float(k[7]) if len(k)>7 else 0.0
            be.add_candle(sym, tf, Candle(int(k[0]), o, h, l, c, q_quote, int(k[6])))
    except Exception as e:
        print("backfill_err", sym, tf, str(e))

# ---------------- Signal handling ----------------
async def on_kline(symbol:str, k:dict):
    global BE, OB, BINANCE_CLIENT
    sym = symbol.upper()
    tf = k["i"]

    def f(x): 
        try: return float(x)
        except: return 0.0

    cndl = Candle(int(k["t"]), f(k["o"]), f(k["h"]), f(k["l"]), f(k["c"]), f(k.get("q",0.0)), int(k["T"]))
    BE.add_candle(sym, tf, cndl)
    STATE["last_kline"] = {"sym": sym, "tf": tf, "t": k["T"], "c": cndl.close}

    sig = BE.on_closed_bar(sym, tf)
    if not sig: return

    # We only trade signals from 5m/15m (skip 1m/1h in terms of triggers)
    if tf not in ("5m","15m"):
        return

    # Basic recent volume check (fast path)
    if sig.vol_z < S.VOL_Z_MIN:
        return

    # Book top
    top = OB.top(sym)
    if not top: 
        return
    bid, ask = top.bid_price, top.ask_price
    mid = (bid+ask)/2.0 if (bid>0 and ask>0) else sig.price

    # ---------------- Router path ----------------
    if S.CONSENSUS_ROUTER:
        ok, reason, extras = router_gate_checks(sym, sig, BE, OB)
        if not ok:
            return

        # Plan using Router % targets
        pad = S.BREAKOUT_PAD_BP/10_000.0
        entry_ref = sig.level*(1+pad) if sig.side=="LONG" else sig.level*(1-pad)
        sl = entry_ref * (1 - S.ROUTER_SL_PCT/100.0) if sig.side=="LONG" else entry_ref * (1 + S.ROUTER_SL_PCT/100.0)
        tp1 = entry_ref * (1 + S.ROUTER_TP1_PCT/100.0) if sig.side=="LONG" else entry_ref * (1 - S.ROUTER_TP1_PCT/100.0)
        tp2 = None if S.ROUTER_TP2_PCT is None else (entry_ref * (1 + S.ROUTER_TP2_PCT/100.0) if sig.side=="LONG" else entry_ref * (1 - S.ROUTER_TP2_PCT/100.0))

        # Edge check: TP1 must cover costs with margin
        if not tp1_edge_ok(entry_ref, tp1):
            return

        # Execute: MARKET entry first, then SL/TP2 on exchange
        try:
            orders = await place_entry_and_brackets(BINANCE_CLIENT, sym, sig.side, entry_ref, sl, tp2 if tp2 else tp1)
        except Exception as e:
            await alert(f"🚫 ORDER FAIL {sym} {tf}\n{sig.side} {e}")
            return

        # Track trade
        risk = abs(entry_ref - sl)
        OPEN_TRADES[sym] = {
            "symbol": sym, "side": sig.side, "tf": tf,
            "entry": entry_ref, "sl": sl, "tp1": tp1, "tp2": tp2,
            "risk": risk, "opened_ts": int(time.time()),
            "tp1_hit": False, "trail_active": True, "trail_peak": mid,
            "trail_dist": (atr1m_bps(BE, sym) or 10.0)/10_000.0 * mid * S.TRAIL_ATR1M_MULT,
        }
        LAST_TRADE_TS[sym] = int(time.time())

        # Alert AFTER entry
        await alert(
            f"🛒 EXECUTED (Router) {sym} <code>{tf}</code>\n"
            f"{'LONG' if sig.side=='LONG' else 'SHORT'} entry≈<b>{entry_ref:.6f}</b>\n"
            f"SL: <b>{sl:.6f}</b> | TP1: <b>{tp1:.6f}</b>{'' if tp2 is None else f' | TP2: <b>{tp2:.6f}</b>'}\n"
            f"spread:{extras.get('spread_bps','?')}bps rsi:{extras.get('rsi','?')} vz:{extras.get('vol_z','?')}"
        )
        return

    # ---------------- Original path (ATR risk, R multiples) ----------------
    atr_val = sig.atr_val if sig.atr_val else (sig.price*0.003)
    entry = sig.price
    sl = entry - atr_val * S.ATR_SL_MULT if sig.side=="LONG" else entry + atr_val * S.ATR_SL_MULT
    risk = abs(entry - sl)
    if risk <= 0: 
        return
    tp1 = entry + risk * S.TP1_MULT if sig.side=="LONG" else entry - risk * S.TP1_MULT
    tp2 = entry + risk * S.TP2_MULT if sig.side=="LONG" else entry - risk * S.TP2_MULT

    # Edge check on TP1 vs fees
    if not tp1_edge_ok(entry, tp1):
        return

    try:
        orders = await place_entry_and_brackets(BINANCE_CLIENT, sym, sig.side, entry, sl, tp2)
    except Exception as e:
        await alert(f"🚫 ORDER FAIL {sym} {tf}\n{sig.side} {e}")
        return

    OPEN_TRADES[sym] = {
        "symbol": sym, "side": sig.side, "tf": tf,
        "entry": entry, "sl": sl, "tp1": tp1, "tp2": tp2,
        "risk": risk, "opened_ts": int(time.time()),
        "tp1_hit": False, "trail_active": False, "trail_peak": None,
        "trail_dist": atr_val * S.TRAIL_ATR_MULT,
    }
    LAST_TRADE_TS[sym] = int(time.time())

    await alert(
        f"🛒 EXECUTED {sym} <code>{tf}</code>\n"
        f"{'LONG' if sig.side=='LONG' else 'SHORT'} entry≈<b>{entry:.6f}</b>\n"
        f"SL: <b>{sl:.6f}</b> | TP1: <b>{tp1:.6f}</b> | TP2: <b>{tp2:.6f}</b>"
    )

# ---------------- BookTicker handling (TP1 detection + trailing) ----------------
async def on_bookticker(symbol:str, data:dict):
    sym = symbol.upper()
    if OB: OB.update_book_ticker(sym, data)
    tr = OPEN_TRADES.get(sym)
    if not tr: return
    top = OB.top(sym)
    if not top: return
    bid, ask = top.bid_price, top.ask_price
    mid = (bid+ask)/2.0
    if mid<=0: return

    side = tr["side"]; entry = tr["entry"]; sl = tr["sl"]; tp1 = tr["tp1"]; tp2 = tr["tp2"]
    risk = tr["risk"]

    # Pre-TP1: confirm SL/TP1 locally (exchange SL/TP2 are already resting)
    if not tr["tp1_hit"]:
        if side=="LONG":
            if bid <= sl:
                await close_and_report(sym, "SL", bid, entry, sl, side, risk); return
            if ask >= tp1:
                tr["tp1_hit"]=True; tr["trail_active"]=True; tr["trail_peak"]=mid
        else:
            if ask >= sl:
                await close_and_report(sym, "SL", ask, entry, sl, side, risk); return
            if bid <= tp1:
                tr["tp1_hit"]=True; tr["trail_active"]=True; tr["trail_peak"]=mid
        return

    # After TP1: trailing stop
    if tr["trail_active"]:
        if side=="LONG":
            tr["trail_peak"]=max(tr["trail_peak"], mid)
            trail_stop = tr["trail_peak"] - tr["trail_dist"]
            tr["trail_stop"]=trail_stop
            if bid <= trail_stop:
                await close_and_report(sym, "TS", trail_stop, entry, sl, side, risk); return
            if tp2 and ask >= tp2:
                await close_and_report(sym, "TP2", tp2, entry, sl, side, risk); return
        else:
            tr["trail_peak"]=min(tr["trail_peak"], mid) if tr["trail_peak"] is not None else mid
            trail_stop = tr["trail_peak"] + tr["trail_dist"]
            tr["trail_stop"]=trail_stop
            if ask >= trail_stop:
                await close_and_report(sym, "TS", trail_stop, entry, sl, side, risk); return
            if tp2 and bid <= tp2:
                await close_and_report(sym, "TP2", tp2, entry, sl, side, risk); return

# ---------------- Close + report ----------------
async def close_and_report(symbol:str, outcome:str, exit_px:float, entry:float, sl:float, side:str, risk:float):
    tr = OPEN_TRADES.pop(symbol, None)
    # Try flatten on exchange
    await force_close_market(BINANCE_CLIENT, symbol)
    # Compute R with taker exit cost
    R = compute_R(entry, exit_px, sl, side, risk)
    stats_add(symbol, R, outcome)
    dur_min = (int(time.time()) - (tr["opened_ts"] if tr else int(time.time()))) / 60.0
    await alert(
        f"📌 RESULT {symbol}\n"
        f"{outcome} | Entry {entry:.6f} → Exit {exit_px:.6f}\n"
        f"PnL: {R:+.2f}R (fees+slip included)\n"
        f"Duration: {dur_min:.1f}m"
    )

# ---------------- Main ----------------
async def main():
    global REST_SESSION, BINANCE_CLIENT, OB, BE

    # Logging-lite
    print("Booting... router:", S.CONSENSUS_ROUTER)

    # HTTP
    server = HealthServer(STATE)
    asyncio.create_task(server.start(S.HOST, S.PORT))

    REST_SESSION = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10))
    BINANCE_CLIENT = BinanceClient(REST_SESSION)

    # Exchange filters
    await load_exchange_info(REST_SESSION)

    # Symbols
    if S.SYMBOLS == "ALL" or (isinstance(S.SYMBOLS,str) and S.SYMBOLS.upper()=="ALL"):
        symbols = await discover_perp_usdt_symbols(REST_SESSION, S.MAX_SYMBOLS)
    elif isinstance(S.SYMBOLS, (list,tuple)):
        symbols = list(S.SYMBOLS)
    else:
        symbols = [str(S.SYMBOLS)]

    # Liquidity prune by 24h quote volume
    symbols = await filter_symbols_by_24h_volume(REST_SESSION, symbols, S.MIN_24H_QUOTE_VOL_USDT)

    # Trackers
    OB = OrderBookTracker()
    BE = BreakoutEngine(S)

    # Backfill 1m/5m/15m/1h
    for sym in symbols:
        for tf in S.TIMEFRAMES:
            await small_backfill(REST_SESSION, BE, sym, tf, S.BACKFILL_LIMIT)
        await asyncio.sleep(0.05)

    # Streams
    ws = WSStreamMulti(symbols, S.TIMEFRAMES, on_kline, on_bookticker, S.WS_CHUNK_SIZE)
    await ws.run()

if __name__=="__main__":
    try: uvloop.install()
    except Exception: pass
    try: asyncio.run(main())
    except KeyboardInterrupt: pass
