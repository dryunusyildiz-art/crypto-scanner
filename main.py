#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
================================================================================
  CRYPTO OPPORTUNITY & VOLATILITY SCANNER  (tek dosya / single file MVP)
================================================================================

Kısa zaman dilimlerinde volatilite / hacim / breakout / momentum fırsatlarini
tarayan; teknik analiz + piyasa mikro-yapisi kriterlerine gore 0-100 arasi
"Opportunity Score" ureten ve kriterlere uyan coinler icin Telegram'a alarm
gonderen bir tarama motoru.

  !!! Bu sistem YATIRIM TAVSIYESI URETMEZ. !!!
  Sadece teknik kriterlere dayali "izleme / firsat alarmi" gonderir.

Coklu zaman dilimi: her coin, TIMEFRAMES listesindeki HER zaman diliminde
(varsayilan 5m, 30m, 1h) ayri ayri taranir ve her biri icin bagimsiz skor,
yon ve alarm uretilir.

ASENKRON: veri cekimi ccxt.async_support + asyncio ile PARALEL yapilir.
  - Bir coinin order book + ticker + tum TF mumlari es zamanli cekilir.
  - Coinler, MAX_CONCURRENCY semaphore'u ile sinirli paralellikte taranir.
  - Bu sayede 50-100 coin taramasi saniyeler mertebesine iner.

Kullanim:
    1) pip install -r requirements.txt
    2) (opsiyonel) .env dosyasi olustur; Telegram/Binance anahtarlarini gir.
    3) python main.py            -> surekli tarama dongusu (async)
       python main.py --once     -> tek tur tarama (test icin)
       python main.py --selftest -> API'siz, sahte veriyle fonksiyon testi
       python main.py --tg-test  -> Telegram baglantisini test et

Bagimliliklar (requirements.txt):
    ccxt, pandas, numpy, requests, python-dotenv, aiohttp
--------------------------------------------------------------------------------
"""

from __future__ import annotations

import os
import sys
import time
import json
import math
import asyncio
import logging
import argparse
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional, Any

import numpy as np
import pandas as pd

# --- opsiyonel bagimliliklar (selftest anahtarsiz calissin diye korumali) ----
try:
    import ccxt.async_support as ccxt_async  # type: ignore
except Exception:  # pragma: no cover
    ccxt_async = None

# aiohttp + certifi: Windows'ta ccxt/aiohttp SSL sertifikasi bulamayabilir
# (CERTIFICATE_VERIFY_FAILED). certifi tabanli SSL context enjekte edecegiz.
import ssl
try:
    import aiohttp  # type: ignore
except Exception:  # pragma: no cover
    aiohttp = None
try:
    import certifi  # type: ignore
except Exception:  # pragma: no cover
    certifi = None

try:
    import requests  # type: ignore
except Exception:  # pragma: no cover
    requests = None

try:
    from dotenv import load_dotenv  # type: ignore
    load_dotenv()
except Exception:  # pragma: no cover
    pass


# ==============================================================================
# 1) KONFIGURASYON
# ==============================================================================
def _env(key: str, default: str) -> str:
    return os.getenv(key, default)


class Config:
    # --- Borsa ---
    EXCHANGE_NAME: str = _env("EXCHANGE_NAME", "binance")
    MARKET_TYPE: str = _env("MARKET_TYPE", "spot")   # "spot" | "future"

    # Binance SPOT public verilerini (OHLCV/ticker/orderbook) geo-engelsiz
    # 'data-api.binance.vision' uzerinden cek. api.binance.com bir bolgeden
    # engelliyse (HTTP 451) bu mirror sorunu cozer. Yalnizca binance + spot icin.
    USE_BINANCE_DATA_MIRROR: bool = _env("USE_BINANCE_DATA_MIRROR", "true").lower() == "true"
    BINANCE_DATA_MIRROR_URL: str = _env("BINANCE_DATA_MIRROR_URL",
                                        "https://data-api.binance.vision/api/v3")

    # --- Zaman dilimleri ---
    # Her coin bu zaman dilimlerinin HER BIRINDE ayri ayri taranir ve
    # her biri icin bagimsiz skor/yon/alarm uretilir.
    TIMEFRAMES: list[str] = [t.strip() for t in _env("TIMEFRAMES", "1h,4h").split(",") if t.strip()]
    TIMEFRAME_MAIN: str = _env("TIMEFRAME_MAIN", "5m")    # (VWAP/etiket referansi)
    TIMEFRAME_FAST: str = _env("TIMEFRAME_FAST", "1m")    # (rezerve) hizli tetik
    TIMEFRAME_TREND: str = _env("TIMEFRAME_TREND", "1h")  # (rezerve) trend filtresi
    OHLCV_LIMIT: int = int(_env("OHLCV_LIMIT", "200"))

    # --- Async paralellik ---
    # Ayni anda kac coinin verisi cekilsin (semaphore). Rate-limit'e dikkat;
    # 8-15 arasi Binance icin guvenli baslangic.
    MAX_CONCURRENCY: int = int(_env("MAX_CONCURRENCY", "10"))

    # --- Ag / baglanti ---
    REQUEST_TIMEOUT_MS: int = int(_env("REQUEST_TIMEOUT_MS", "20000"))   # tek istek zaman asimi
    # SSL dogrulamasi: True (guvenli, certifi CA paketi kullanilir). Son care olarak
    # SSL_VERIFY=false yaparak dogrulamayi kapatabilirsin (onerilmez, sadece test).
    SSL_VERIFY: bool = _env("SSL_VERIFY", "true").lower() == "true"

    # --- Dongu ---
    # Sabit kadans: her 5 dakikada bir, saat dilimine hizali (:00, :05, :10 ...).
    # Internet kesintisi olsa bile bir sonraki 5-dk tik'inde otomatik tekrar denenir.
    SCAN_INTERVAL_SECONDS: int = int(_env("SCAN_INTERVAL_SECONDS", "900"))

    # --- Skor esikleri ---
    MIN_OPPORTUNITY_SCORE_STRONG: float = float(_env("MIN_OPPORTUNITY_SCORE_STRONG", "75"))
    MIN_OPPORTUNITY_SCORE_WATCH: float = float(_env("MIN_OPPORTUNITY_SCORE_WATCH", "60"))
    MIN_VOLUME_RATIO_STRONG: float = float(_env("MIN_VOLUME_RATIO_STRONG", "2.0"))
    MIN_VOLUME_RATIO_WATCH: float = float(_env("MIN_VOLUME_RATIO_WATCH", "1.5"))

    # --- Spread esikleri (yuzde) ---
    SPREAD_EXCELLENT: float = 0.03
    SPREAD_OK: float = 0.07
    MAX_SPREAD_STRONG: float = float(_env("MAX_SPREAD_STRONG", "0.07"))
    MAX_SPREAD_ABSOLUTE: float = float(_env("MAX_SPREAD_ABSOLUTE", "0.20"))
    SPREAD_RISKY: float = 0.15

    # --- Order book / likidite ---
    DEPTH_BAND_PCT: float = 0.5          # en iyi fiyatin +-%0.5 bandi
    MIN_DEPTH_USDT: float = float(_env("MIN_DEPTH_USDT", "20000"))   # yeterli derinlik esigi (quote)
    ORDERBOOK_LIMIT: int = 50

    # --- Alarm tekrari onleme ---
    ALERT_COOLDOWN_MINUTES: int = int(_env("ALERT_COOLDOWN_MINUTES", "15"))
    SCORE_UPDATE_THRESHOLD: float = float(_env("SCORE_UPDATE_THRESHOLD", "10"))

    # --- Telegram ---
    TELEGRAM_BOT_TOKEN: str = _env("TELEGRAM_BOT_TOKEN", "")
    TELEGRAM_CHAT_ID: str = _env("TELEGRAM_CHAT_ID", "")
    TELEGRAM_ENABLED: bool = _env("TELEGRAM_ENABLED", "true").lower() == "true"

    # --- Binance API (opsiyonel; public veriler icin gerekmez) ---
    BINANCE_API_KEY: str = _env("BINANCE_API_KEY", "")
    BINANCE_API_SECRET: str = _env("BINANCE_API_SECRET", "")

    # --- Indikator parametreleri ---
    ATR_PERIOD: int = 14
    BB_PERIOD: int = 20
    BB_STD: float = 2.0
    RSI_PERIOD: int = 14
    ROC_PERIOD: int = 9
    ADX_PERIOD: int = 14
    EMA_FAST: int = 20
    EMA_SLOW: int = 50
    DONCHIAN_SHORT: int = 20
    DONCHIAN_LONG: int = 50
    VOL_MA_PERIOD: int = 20
    LOOKBACK_STAT: int = 100   # ATR%/BBWidth ortalama karsilastirma penceresi

    # --- Disclaimer ---
    DISCLAIMER: str = ("Bu mesaj yatirim tavsiyesi degildir. "
                       "Sadece teknik kriterlere dayali piyasa tarama alarmidir.")


# --- Taranacak coin listesi (kolayca degistirilebilir) -------------------------
SYMBOLS = [
    "BTC/USDT", "ETH/USDT", "XAU/USDT", "AAVE/USDT", "CHZ/USDT",
    "1INCH/USDT", "ADA/USDT", "ALGO/USDT", "API3/USDT", "ARB/USDT",
    "ATOM/USDT", "AVAX/USDT", "BEL/USDT", "BNB/USDT", "CAKE/USDT",
    "CELO/USDT", "DOGE/USDT", "DOT/USDT", "DYDX/USDT", "EGLD/USDT",
    "EIGEN/USDT", "ENJ/USDT", "FET/USDT", "FIL/USDT", "FLUX/USDT",
    "GALA/USDT", "HBAR/USDT", "INJ/USDT", "KAVA/USDT", "LINK/USDT",
    "MANA/USDT", "NEAR/USDT", "NEO/USDT", "PEPE/USDT", "POL/USDT",
    "RARE/USDT", "RENDER/USDT", "RVN/USDT", "SAND/USDT", "SHIB/USDT",
    "SNX/USDT", "SOL/USDT", "SPK/USDT", "SUI/USDT", "TAO/USDT",
    "TRU/USDT", "VET/USDT", "XAG/USDT", "XRP/USDT", "XTZ/USDT",
    "XVG/USDT", "ZRO/USDT",
]

STORAGE_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "alerts_cache.json")


# ==============================================================================
# 2) LOGLAMA
# ==============================================================================
def setup_logger() -> logging.Logger:
    logger = logging.getLogger("scanner")
    if logger.handlers:
        return logger
    logger.setLevel(logging.INFO)
    h = logging.StreamHandler(sys.stdout)
    fmt = logging.Formatter("%(asctime)s | %(levelname)-7s | %(message)s", "%H:%M:%S")
    h.setFormatter(fmt)
    logger.addHandler(h)
    return logger


log = setup_logger()


# ==============================================================================
# 3) INDIKATORLER  (pandas/numpy ile elle; ekstra bagimlilik yok)
# ==============================================================================
def ema(series: pd.Series, period: int) -> pd.Series:
    return series.ewm(span=period, adjust=False).mean()


def rma(series: pd.Series, period: int) -> pd.Series:
    """Wilder's smoothing (RSI/ATR/ADX icin)."""
    return series.ewm(alpha=1.0 / period, adjust=False).mean()


def true_range(df: pd.DataFrame) -> pd.Series:
    high, low, close = df["high"], df["low"], df["close"]
    prev_close = close.shift(1)
    tr = pd.concat([
        high - low,
        (high - prev_close).abs(),
        (low - prev_close).abs(),
    ], axis=1).max(axis=1)
    return tr


def atr(df: pd.DataFrame, period: int = 14) -> pd.Series:
    return rma(true_range(df), period)


def atr_percent(df: pd.DataFrame, period: int = 14) -> pd.Series:
    return atr(df, period) / df["close"] * 100.0


def rsi(series: pd.Series, period: int = 14) -> pd.Series:
    delta = series.diff()
    gain = delta.clip(lower=0.0)
    loss = -delta.clip(upper=0.0)
    avg_gain = rma(gain, period)
    avg_loss = rma(loss, period)
    rs = avg_gain / avg_loss.replace(0.0, np.nan)
    out = 100.0 - (100.0 / (1.0 + rs))
    return out.fillna(100.0)


def roc(series: pd.Series, period: int = 9) -> pd.Series:
    prev = series.shift(period)
    return (series - prev) / prev.replace(0.0, np.nan) * 100.0


def bollinger_width(series: pd.Series, period: int = 20, num_std: float = 2.0) -> pd.Series:
    mid = series.rolling(period).mean()
    std = series.rolling(period).std(ddof=0)
    upper = mid + num_std * std
    lower = mid - num_std * std
    width = (upper - lower) / mid.replace(0.0, np.nan)
    return width


def adx(df: pd.DataFrame, period: int = 14) -> pd.DataFrame:
    """Wilder ADX; adx, plus_di, minus_di kolonlari doner."""
    high, low = df["high"], df["low"]
    up_move = high.diff()
    down_move = -low.diff()
    plus_dm = np.where((up_move > down_move) & (up_move > 0), up_move, 0.0)
    minus_dm = np.where((down_move > up_move) & (down_move > 0), down_move, 0.0)
    plus_dm = pd.Series(plus_dm, index=df.index)
    minus_dm = pd.Series(minus_dm, index=df.index)

    tr = true_range(df)
    atr_ = rma(tr, period).replace(0.0, np.nan)
    plus_di = 100.0 * rma(plus_dm, period) / atr_
    minus_di = 100.0 * rma(minus_dm, period) / atr_
    dx = 100.0 * (plus_di - minus_di).abs() / (plus_di + minus_di).replace(0.0, np.nan)
    adx_ = rma(dx.fillna(0.0), period)
    return pd.DataFrame({"adx": adx_, "plus_di": plus_di, "minus_di": minus_di})


def session_vwap(df: pd.DataFrame) -> pd.Series:
    """UTC gununu baz alan seans VWAP (her gun sifirlanir)."""
    typical = (df["high"] + df["low"] + df["close"]) / 3.0
    tpv = (typical * df["volume"]).values
    vol_v = df["volume"].values
    day = pd.to_datetime(df["timestamp"], unit="ms", utc=True).dt.floor("D").values
    out = np.full(len(df), np.nan)
    cum_tpv = 0.0
    cum_vol = 0.0
    last_day = None
    for i in range(len(df)):
        if day[i] != last_day:
            cum_tpv = 0.0
            cum_vol = 0.0
            last_day = day[i]
        cum_tpv += tpv[i]
        cum_vol += vol_v[i]
        out[i] = cum_tpv / cum_vol if cum_vol > 0 else np.nan
    return pd.Series(out, index=df.index)


def donchian(df: pd.DataFrame, period: int) -> tuple[pd.Series, pd.Series]:
    """Onceki N mumun (mevcut mum haric) en yuksegi/en dusugu -> breakout referansi."""
    upper = df["high"].shift(1).rolling(period).max()
    lower = df["low"].shift(1).rolling(period).min()
    return upper, lower


def wick_ratio(row: pd.Series) -> float:
    rng = row["high"] - row["low"]
    if rng <= 0:
        return 0.0
    body_hi = max(row["open"], row["close"])
    body_lo = min(row["open"], row["close"])
    upper_wick = row["high"] - body_hi
    lower_wick = body_lo - row["low"]
    return float((upper_wick + lower_wick) / rng)


# ==============================================================================
# 4) VERI MODELLERI
# ==============================================================================
@dataclass
class MarketSnapshot:
    """Bir coin/zaman-dilimi icin tum ham + turetilmis veriler."""
    symbol: str
    price: float
    atr_pct: float
    atr_pct_avg: float
    bb_width: float
    bb_width_avg: float
    range_expansion: float          # son5 range / son50 range
    volume_ratio: float
    quote_volume_rising: bool
    vwap: float
    rsi: float
    roc: float
    adx: float
    adx_rising: bool
    ema20: float
    ema50: float
    ema20_slope_up: bool
    don_high_20: float
    don_low_20: float
    don_high_50: float
    don_low_50: float
    last_wick_ratio: float
    breakout_vol_ok: bool
    # mikro-yapi
    spread_pct: float
    bid_depth: float
    ask_depth: float
    total_depth: float
    imbalance: float


@dataclass
class Signal:
    symbol: str
    timeframe: str          # 5m | 30m | 1h ...
    direction: str          # LONG | SHORT | NEUTRAL
    score: float
    volatility: float
    volume: float
    momentum: float
    liquidity: float
    breakout: float
    risk_penalty: float
    reasons: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    price: float = 0.0
    invalidation: str = ""
    ex_symbol: str = ""     # borsadaki gercek parite (TradingView linki icin)
    tier: str = "NONE"      # STRONG | WATCH | NONE
    snap: Optional[MarketSnapshot] = None


# ==============================================================================
# 5) BORSA ISTEMCISI & VERI CEKME  (ASENKRON)
# ==============================================================================
class ExchangeClient:
    def __init__(self, cfg: Config):
        if ccxt_async is None:
            raise RuntimeError("ccxt yuklu degil. 'pip install ccxt' calistirin.")
        self.cfg = cfg
        # binance + futures -> 'binanceusdm' kullan. Bu sinif YALNIZCA
        # fapi.binance.com (USDT-M futures) uc noktalarini kullanir ve cogu
        # bolgede geo-engelli olan api.binance.com'a (spot) HIC gitmez.
        # Boylece "ExchangeNotAvailable ... api.binance.com/api/v3/exchangeInfo"
        # (HTTP 451) hatasi ortadan kalkar. Spot modunda duz 'binance'.
        name = cfg.EXCHANGE_NAME.strip().lower()   # ccxt sinif adlari KUCUK harf
        self.market_type = cfg.MARKET_TYPE.strip().lower()
        if name == "binance" and self.market_type == "future":
            name = "binanceusdm"
        self.name = name

        options: dict[str, Any] = {"adjustForTimeDifference": True}
        if name == "binance":
            # duz binance'te sadece ilgili market tipini yukle (gereksiz cagri yok)
            options["defaultType"] = "future" if self.market_type == "future" else "spot"
            options["fetchMarkets"] = ["linear" if self.market_type == "future" else "spot"]

        params: dict[str, Any] = {
            "enableRateLimit": True,
            "timeout": cfg.REQUEST_TIMEOUT_MS,
            "options": options,
        }
        if cfg.BINANCE_API_KEY and cfg.BINANCE_API_SECRET:
            params["apiKey"] = cfg.BINANCE_API_KEY
            params["secret"] = cfg.BINANCE_API_SECRET
        exchange_cls = getattr(ccxt_async, name)
        self.ex = exchange_cls(params)

        # Binance SPOT: public veri uc noktasini geo-engelsiz mirror'a yonlendir.
        # (api.binance.com bolgesel engelliyse OHLCV/ticker/orderbook yine calisir.)
        if name == "binance" and self.market_type == "spot" and cfg.USE_BINANCE_DATA_MIRROR:
            try:
                self.ex.urls["api"]["public"] = cfg.BINANCE_DATA_MIRROR_URL
                self._mirror = cfg.BINANCE_DATA_MIRROR_URL
            except Exception:
                self._mirror = ""
        else:
            self._mirror = ""
        self.markets: dict = {}

        # SSL context: certifi CA paketiyle (Windows'ta aiohttp'nin sertifika
        # bulamama sorununu -CERTIFICATE_VERIFY_FAILED- cozer).
        self._ssl_context: Optional[ssl.SSLContext] = None
        self._session = None
        if not cfg.SSL_VERIFY:
            ctx = ssl.create_default_context()
            ctx.check_hostname = False
            ctx.verify_mode = ssl.CERT_NONE
            self._ssl_context = ctx
        elif certifi is not None:
            self._ssl_context = ssl.create_default_context(cafile=certifi.where())

    async def _prepare_session(self) -> None:
        """ccxt'nin kullanacagi aiohttp oturumunu ozel ayarlarla olustur.

        1) ThreadedResolver: aiohttp varsayilan olarak aiodns (c-ares) kullanir;
           bu, Windows'ta sistem DNS sunucularini okuyamayip "Could not contact
           DNS servers" hatasi verir. ThreadedResolver, isletim sisteminin normal
           getaddrinfo'sunu (urllib gibi) kullanir -> DNS sorunu cozulur.
        2) certifi SSL context: Windows'ta sertifika bulamama sorununu onler.
        3) trust_env=True: sistem proxy ayarlarini kullanir (varsa)."""
        if self._session is None and aiohttp is not None:
            try:
                resolver = aiohttp.ThreadedResolver()
            except Exception:
                resolver = None
            conn_kwargs: dict[str, Any] = {}
            if self._ssl_context is not None:
                conn_kwargs["ssl"] = self._ssl_context
            if resolver is not None:
                conn_kwargs["resolver"] = resolver
            connector = aiohttp.TCPConnector(**conn_kwargs)
            self._session = aiohttp.ClientSession(connector=connector, trust_env=True)
            self.ex.session = self._session   # ccxt bu oturumu kullanir

    async def load_markets(self) -> None:
        await self._prepare_session()
        if self._mirror:
            log.info("Binance spot public verisi mirror uzerinden: %s", self._mirror)
        self.markets = await self.ex.load_markets()
        log.info("Markets yuklendi: %d parite (%s | %s)",
                 len(self.markets), self.name, self.cfg.MARKET_TYPE)

    async def close(self) -> None:
        try:
            await self.ex.close()
        except Exception:
            pass
        try:
            if self._session is not None:
                await self._session.close()
        except Exception:
            pass

    def resolve_symbol(self, symbol: str) -> Optional[str]:
        """Verilen 'BASE/USDT' sembolunu aktif borsa sembolune cevir; yoksa None.

        Not: Binance Futures'ta bazi meme coinler 1000x katli listelenir
        (PEPE -> 1000PEPE, SHIB -> 1000SHIB, BONK -> 1000BONK, FLOKI -> 1000FLOKI...).
        Bu yuzden hem duz hem '1000' onekli varyantlar denenir.
        """
        base, _, quote = symbol.partition("/")
        bases = [base]
        if not base.startswith("1000"):
            bases.append("1000" + base)          # 1000PEPE, 1000SHIB, ...
        if base.startswith("1000"):
            bases.append(base[4:])               # tersi de denensin

        candidates: list[str] = []
        for b in bases:
            pair = f"{b}/{quote}"
            if self.market_type == "future":
                candidates.append(f"{pair}:{quote}")   # USDT-M perpetual (ccxt formati)
                candidates.append(pair)
            else:
                candidates.append(pair)

        for c in candidates:
            m = self.markets.get(c)
            if m and m.get("active", True):
                return c
        return None

    async def fetch_ohlcv(self, symbol: str, timeframe: str, limit: int) -> Optional[pd.DataFrame]:
        try:
            raw = await self.ex.fetch_ohlcv(symbol, timeframe=timeframe, limit=limit)
            if not raw or len(raw) < 60:
                return None
            df = pd.DataFrame(raw, columns=["timestamp", "open", "high", "low", "close", "volume"])
            df = df.astype({"open": float, "high": float, "low": float,
                            "close": float, "volume": float})
            return df
        except Exception as e:
            log.warning("OHLCV alinamadi %s %s: %s", symbol, timeframe, e)
            return None

    async def fetch_ticker(self, symbol: str) -> Optional[dict]:
        try:
            return await self.ex.fetch_ticker(symbol)
        except Exception as e:
            log.warning("Ticker alinamadi %s: %s", symbol, e)
            return None

    async def fetch_order_book(self, symbol: str, limit: int) -> Optional[dict]:
        try:
            return await self.ex.fetch_order_book(symbol, limit=limit)
        except Exception as e:
            log.warning("Order book alinamadi %s: %s", symbol, e)
            return None


# ==============================================================================
# 6) VERI KALITE KONTROLLERI
# ==============================================================================
def validate_ohlcv(df: Optional[pd.DataFrame]) -> tuple[bool, str]:
    if df is None or len(df) < 60:
        return False, "yetersiz mum"
    if df[["open", "high", "low", "close"]].le(0).any().any():
        return False, "OHLC sifir/negatif"
    if df["close"].isna().any():
        return False, "NaN close"
    # eksik mum (bosluk) kontrolu
    diffs = df["timestamp"].diff().dropna()
    if len(diffs) > 5:
        common = diffs.mode().iloc[0]
        if (diffs > common * 2.5).sum() > 2:
            return False, "eksik mum (bosluk)"
    return True, "ok"


# ==============================================================================
# 7) SNAPSHOT INSA (indikator hesaplari)
# ==============================================================================
def build_snapshot(cfg: Config, symbol: str, df: pd.DataFrame,
                   order_book: Optional[dict], ticker: Optional[dict]) -> MarketSnapshot:
    close = df["close"]
    price = float(close.iloc[-1])

    # --- Volatilite ---
    atrp = atr_percent(df, cfg.ATR_PERIOD)
    atr_now = float(atrp.iloc[-1])
    atr_avg = float(atrp.tail(cfg.LOOKBACK_STAT).mean())

    bbw = bollinger_width(close, cfg.BB_PERIOD, cfg.BB_STD)
    bbw_last = bbw.iloc[-1]
    bbw_now = float(bbw_last) if not math.isnan(bbw_last) else 0.0
    bbw_avg = float(bbw.tail(cfg.LOOKBACK_STAT).mean())
    if math.isnan(bbw_avg):
        bbw_avg = 0.0

    rng = df["high"] - df["low"]
    range5 = float(rng.tail(5).mean())
    range50 = float(rng.tail(50).mean())
    range_expansion = range5 / range50 if range50 > 0 else 1.0

    # --- Hacim ---
    vol_ma = df["volume"].rolling(cfg.VOL_MA_PERIOD).mean()
    vol_ratio = float(df["volume"].iloc[-1] / vol_ma.iloc[-1]) if vol_ma.iloc[-1] > 0 else 0.0
    qv_rising = float(df["volume"].tail(3).mean()) > float(df["volume"].tail(20).mean())

    # --- Momentum ---
    vwap_series = session_vwap(df)
    vwap_now = float(vwap_series.iloc[-1])
    rsi_now = float(rsi(close, cfg.RSI_PERIOD).iloc[-1])
    roc_val = roc(close, cfg.ROC_PERIOD).iloc[-1]
    roc_now = float(roc_val) if not math.isnan(roc_val) else 0.0
    adx_df = adx(df, cfg.ADX_PERIOD)
    adx_now = float(adx_df["adx"].iloc[-1])
    adx_prev = float(adx_df["adx"].iloc[-3]) if len(adx_df) > 3 else adx_now
    adx_rising = adx_now > adx_prev
    ema20_series = ema(close, cfg.EMA_FAST)
    ema50_series = ema(close, cfg.EMA_SLOW)
    ema20_now = float(ema20_series.iloc[-1])
    ema50_now = float(ema50_series.iloc[-1])
    ema20_slope_up = ema20_now > float(ema20_series.iloc[-4])

    # --- Breakout referanslari ---
    dh20, dl20 = donchian(df, cfg.DONCHIAN_SHORT)
    dh50, dl50 = donchian(df, cfg.DONCHIAN_LONG)
    don_high_20 = float(dh20.iloc[-1]) if not math.isnan(dh20.iloc[-1]) else price
    don_low_20 = float(dl20.iloc[-1]) if not math.isnan(dl20.iloc[-1]) else price
    don_high_50 = float(dh50.iloc[-1]) if not math.isnan(dh50.iloc[-1]) else price
    don_low_50 = float(dl50.iloc[-1]) if not math.isnan(dl50.iloc[-1]) else price
    breakout_vol_ok = vol_ratio >= 1.5

    last_wick = wick_ratio(df.iloc[-1])

    # --- Mikro-yapi (order book) ---
    spread_pct, bid_depth, ask_depth, total_depth, imbalance = compute_microstructure(
        cfg, order_book, ticker, price)

    return MarketSnapshot(
        symbol=symbol, price=price,
        atr_pct=atr_now, atr_pct_avg=atr_avg,
        bb_width=bbw_now, bb_width_avg=bbw_avg,
        range_expansion=range_expansion,
        volume_ratio=vol_ratio, quote_volume_rising=qv_rising,
        vwap=vwap_now, rsi=rsi_now, roc=roc_now,
        adx=adx_now, adx_rising=adx_rising,
        ema20=ema20_now, ema50=ema50_now, ema20_slope_up=ema20_slope_up,
        don_high_20=don_high_20, don_low_20=don_low_20,
        don_high_50=don_high_50, don_low_50=don_low_50,
        last_wick_ratio=last_wick, breakout_vol_ok=breakout_vol_ok,
        spread_pct=spread_pct, bid_depth=bid_depth, ask_depth=ask_depth,
        total_depth=total_depth, imbalance=imbalance,
    )


def compute_microstructure(cfg: Config, order_book: Optional[dict],
                           ticker: Optional[dict], price: float
                           ) -> tuple[float, float, float, float, float]:
    """Spread%, bid/ask derinligi (quote USDT), toplam derinlik ve imbalance."""
    spread_pct = float("nan")
    bid_depth = ask_depth = total_depth = 0.0
    imbalance = 0.0

    if order_book and order_book.get("bids") and order_book.get("asks"):
        best_bid = order_book["bids"][0][0]
        best_ask = order_book["asks"][0][0]
        mid = (best_bid + best_ask) / 2.0
        if mid > 0:
            spread_pct = (best_ask - best_bid) / mid * 100.0
        band = cfg.DEPTH_BAND_PCT / 100.0
        bid_lo = best_bid * (1 - band)
        ask_hi = best_ask * (1 + band)
        for p, q in order_book["bids"]:
            if p >= bid_lo:
                bid_depth += p * q     # quote (USDT) cinsinden
        for p, q in order_book["asks"]:
            if p <= ask_hi:
                ask_depth += p * q
        total_depth = bid_depth + ask_depth
        denom = bid_depth + ask_depth
        imbalance = (bid_depth - ask_depth) / denom if denom > 0 else 0.0
    elif ticker:
        bid = ticker.get("bid")
        ask = ticker.get("ask")
        if bid and ask and (bid + ask) > 0:
            mid = (bid + ask) / 2.0
            spread_pct = (ask - bid) / mid * 100.0

    return spread_pct, bid_depth, ask_depth, total_depth, imbalance


# ==============================================================================
# 8) SKORLAMA MOTORU
# ==============================================================================
def score_volatility(s: MarketSnapshot) -> tuple[float, list[str]]:
    pts = 0.0
    reasons: list[str] = []
    if s.atr_pct_avg > 0:
        r = s.atr_pct / s.atr_pct_avg
        if r >= 1.5:
            pts += 12
            reasons.append(f"ATR% ortalamanin {r:.2f}x uzerinde")
        elif r >= 1.2:
            pts += 8
            reasons.append(f"ATR% ortalamanin {r:.2f}x uzerinde")
    if s.bb_width_avg > 0 and s.bb_width >= 1.2 * s.bb_width_avg:
        pts += 6
        reasons.append("BB Width genisliyor")
    if s.range_expansion >= 1.5:
        pts += 7
        reasons.append(f"Range genislemesi {s.range_expansion:.2f}x")
    return min(pts, 25.0), reasons


def score_volume(s: MarketSnapshot) -> tuple[float, list[str]]:
    pts = 0.0
    reasons: list[str] = []
    vr = s.volume_ratio
    if vr > 3.0:
        pts += 18
    elif vr > 2.0:
        pts += 12
    elif vr > 1.5:
        pts += 6
    if vr > 1.5:
        reasons.append(f"Volume Ratio: {vr:.2f}x")
    if s.quote_volume_rising:
        pts += 2
    return min(pts, 20.0), reasons


def score_momentum(s: MarketSnapshot) -> tuple[float, float, list[str], list[str]]:
    """long_score, short_score, long_reasons, short_reasons."""
    lp = 0.0
    sp = 0.0
    lr: list[str] = []
    sr: list[str] = []
    # LONG
    if s.price > s.vwap:
        lp += 4; lr.append("Close > VWAP")
    if s.rsi > 50:
        lp += 4; lr.append(f"RSI: {s.rsi:.0f}")
    if s.rsi > 60:
        lp += 3
    if s.roc > 0:
        lp += 3; lr.append(f"ROC: {s.roc:+.2f}%")
    if s.price > s.ema20 > s.ema50:
        lp += 4; lr.append("Close > EMA20 > EMA50")
    if s.adx > 20 and s.adx_rising:
        lp += 2; lr.append(f"ADX: {s.adx:.0f} yukseliyor")
    # SHORT
    if s.price < s.vwap:
        sp += 4; sr.append("Close < VWAP")
    if s.rsi < 50:
        sp += 4; sr.append(f"RSI: {s.rsi:.0f}")
    if s.rsi < 40:
        sp += 3
    if s.roc < 0:
        sp += 3; sr.append(f"ROC: {s.roc:+.2f}%")
    if s.price < s.ema20 < s.ema50:
        sp += 4; sr.append("Close < EMA20 < EMA50")
    if s.adx > 20 and s.adx_rising:
        sp += 2; sr.append(f"ADX: {s.adx:.0f} yukseliyor")
    return min(lp, 20.0), min(sp, 20.0), lr, sr


def score_liquidity(cfg: Config, s: MarketSnapshot, direction: str) -> tuple[float, list[str]]:
    pts = 0.0
    reasons: list[str] = []
    sp = s.spread_pct
    if not math.isnan(sp):
        if sp < cfg.SPREAD_EXCELLENT:
            pts += 6; reasons.append(f"Spread: {sp:.3f}% (cok iyi)")
        elif sp < cfg.SPREAD_OK:
            pts += 4; reasons.append(f"Spread: {sp:.3f}%")
        elif sp < cfg.SPREAD_RISKY:
            pts += 2
    if s.total_depth >= cfg.MIN_DEPTH_USDT:
        pts += 5; reasons.append("Derinlik yeterli")
    # slippage yaklasik dusukse (spread cok darsa + derinlik iyi) +2
    if not math.isnan(sp) and sp < cfg.SPREAD_EXCELLENT and s.total_depth >= cfg.MIN_DEPTH_USDT:
        pts += 2
    # imbalance yonu destekliyorsa
    if direction == "LONG" and s.imbalance > 0.15:
        pts += 2; reasons.append("Bid imbalance destekliyor")
    elif direction == "SHORT" and s.imbalance < -0.15:
        pts += 2; reasons.append("Ask imbalance destekliyor")
    return min(pts, 15.0), reasons


def score_breakout(s: MarketSnapshot, direction: str) -> tuple[float, list[str]]:
    pts = 0.0
    reasons: list[str] = []
    if direction == "LONG":
        if s.price > s.don_high_20:
            pts += 8; reasons.append("Donchian 20 breakout")
            if s.price > s.don_high_50:
                pts += 4; reasons.append("Donchian 50 breakout")
            if s.breakout_vol_ok:
                pts += 3
    elif direction == "SHORT":
        if s.price < s.don_low_20:
            pts += 8; reasons.append("Donchian 20 breakdown")
            if s.price < s.don_low_50:
                pts += 4; reasons.append("Donchian 50 breakdown")
            if s.breakout_vol_ok:
                pts += 3
    return min(pts, 15.0), reasons


def score_risk(cfg: Config, s: MarketSnapshot) -> tuple[float, list[str]]:
    penalty = 0.0
    warnings: list[str] = []
    sp = s.spread_pct
    if math.isnan(sp) or sp > cfg.SPREAD_RISKY:
        penalty += 8; warnings.append("Spread yuksek/olcum yok")
    if s.total_depth > 0 and s.total_depth < cfg.MIN_DEPTH_USDT * 0.5:
        penalty += 8; warnings.append("Derinlik cok dusuk")
    if s.last_wick_ratio > 0.60:
        penalty += 5; warnings.append(f"Asiri fitil ({s.last_wick_ratio:.2f})")
    # hacim var ama fiyat ilerlemiyorsa (buyuk hacim + kucuk govde)
    if s.volume_ratio > 2.0 and abs(s.roc) < 0.1:
        penalty += 5; warnings.append("Hacim var, fiyat ilerlemiyor")
    if s.adx < 15 and s.range_expansion < 1.1:
        penalty += 4; warnings.append("ADX dusuk / yatay piyasa")
    return min(penalty, 30.0), warnings


def determine_direction(long_m: float, short_m: float, s: MarketSnapshot) -> str:
    """Momentum + trend tabanli baskin yon."""
    if abs(long_m - short_m) < 3:
        # momentum belirsiz -> trend/EMA yapisi ile karar ver
        if s.price > s.ema20 > s.ema50:
            return "LONG"
        if s.price < s.ema20 < s.ema50:
            return "SHORT"
        return "NEUTRAL"
    return "LONG" if long_m > short_m else "SHORT"


def evaluate(cfg: Config, s: MarketSnapshot, timeframe: str) -> Signal:
    vol_s, vol_r = score_volatility(s)
    volu_s, volu_r = score_volume(s)
    long_m, short_m, long_r, short_r = score_momentum(s)

    direction = determine_direction(long_m, short_m, s)
    if direction == "LONG":
        momentum, mom_reasons = long_m, long_r
    elif direction == "SHORT":
        momentum, mom_reasons = short_m, short_r
    else:
        momentum, mom_reasons = max(long_m, short_m), []

    liq_s, liq_r = score_liquidity(cfg, s, direction)
    brk_s, brk_r = score_breakout(s, direction)
    risk_p, risk_w = score_risk(cfg, s)

    raw = vol_s + volu_s + momentum + liq_s + brk_s - risk_p
    score = float(max(0.0, min(100.0, raw)))

    reasons = vol_r + volu_r + mom_reasons + brk_r + liq_r

    # teknik invalidation seviyesi (tavsiye degil, sadece teknik referans)
    if direction == "LONG":
        invalidation = f"VWAP alti {timeframe} kapanis ({fmt_price(s.vwap)})"
    elif direction == "SHORT":
        invalidation = f"VWAP ustu {timeframe} kapanis ({fmt_price(s.vwap)})"
    else:
        invalidation = "-"

    return Signal(
        symbol=s.symbol, timeframe=timeframe, direction=direction, score=score,
        volatility=vol_s, volume=volu_s, momentum=momentum,
        liquidity=liq_s, breakout=brk_s, risk_penalty=risk_p,
        reasons=reasons, warnings=risk_w, price=s.price,
        invalidation=invalidation, snap=s,
    )


# ==============================================================================
# 9) ALARM SEVIYESI (STRONG / WATCH / NONE)
# ==============================================================================
def classify_alert(cfg: Config, sig: Signal) -> str:
    s = sig.snap
    if s is None:
        return "NONE"
    sp = s.spread_pct
    spread_ok = (not math.isnan(sp)) and sp <= cfg.MAX_SPREAD_STRONG
    spread_acceptable = (not math.isnan(sp)) and sp <= cfg.MAX_SPREAD_ABSOLUTE
    atr_above = s.atr_pct_avg > 0 and s.atr_pct > s.atr_pct_avg
    vol_expanding = (s.bb_width_avg > 0 and s.bb_width >= 1.2 * s.bb_width_avg) or s.range_expansion >= 1.3

    # --- bastirma kurallari ---
    if sig.direction == "NEUTRAL":
        return "NONE"
    if math.isnan(sp) or sp > cfg.MAX_SPREAD_ABSOLUTE:
        return "NONE"
    if s.total_depth > 0 and s.total_depth < cfg.MIN_DEPTH_USDT * 0.5:
        return "NONE"
    if s.last_wick_ratio > 0.60:
        return "NONE"
    if sig.score < cfg.MIN_OPPORTUNITY_SCORE_WATCH:
        return "NONE"

    # --- guclu firsat ---
    if (sig.score >= cfg.MIN_OPPORTUNITY_SCORE_STRONG
            and s.volume_ratio >= cfg.MIN_VOLUME_RATIO_STRONG
            and atr_above and spread_ok and sig.risk_penalty <= 15):
        return "STRONG"

    # --- izleme ---
    # Ust sinir YOK: GUCLU olamayan (or. ATR% ortalama ustu degil) ama skoru >=60
    # olan sinyaller olu bolgeye dusmesin, en azindan IZLEME olarak bildirilsin.
    if (sig.score >= cfg.MIN_OPPORTUNITY_SCORE_WATCH
            and s.volume_ratio >= cfg.MIN_VOLUME_RATIO_WATCH
            and vol_expanding and spread_acceptable):
        return "WATCH"

    return "NONE"


# ==============================================================================
# 10) TELEGRAM
# ==============================================================================
def fmt_price(x: float) -> str:
    if x == 0 or (isinstance(x, float) and math.isnan(x)):
        return "0"
    if x >= 1000:
        return f"{x:,.2f}"
    if x >= 1:
        return f"{x:.4g}"
    return f"{x:.6g}"


# TradingView zaman dilimi kodlari
_TV_INTERVAL = {
    "1m": "1", "3m": "3", "5m": "5", "15m": "15", "30m": "30",
    "1h": "60", "2h": "120", "4h": "240", "6h": "360", "12h": "720",
    "1d": "D", "1w": "W",
}


def tradingview_link(cfg: Config, sig: "Signal") -> str:
    """Coinin ilgili paritedeki TradingView 'Super Grafik' (Supercharts) linki.

    Ornek: BINANCE:BTCUSDT.P (USDT-M perpetual) veya BINANCE:BTCUSDT (spot),
    dogru zaman dilimi (interval) parametresiyle. 1000x'li pariteler
    (1000PEPEUSDT vb.) borsadaki gercek sembol uzerinden dogru olusturulur.
    """
    ex = sig.ex_symbol or sig.symbol
    is_future = ":" in ex                      # 'BTC/USDT:USDT' -> perpetual
    core = ex.split(":")[0]                    # "BASE/QUOTE"
    pair = core.replace("/", "").upper()       # "BASEQUOTE"
    # borsa onekini TradingView koduna cevir (binance/binanceusdm -> BINANCE)
    ex_name = cfg.EXCHANGE_NAME.strip().lower()
    tv_ex = {"binance": "BINANCE", "binanceusdm": "BINANCE",
             "okx": "OKX", "bybit": "BYBIT", "kucoin": "KUCOIN",
             "gateio": "GATEIO", "mexc": "MEXC", "bitget": "BITGET"}.get(ex_name, ex_name.upper())
    tv_symbol = f"{tv_ex}:{pair}"
    if is_future:
        tv_symbol += ".P"                      # perpetual son eki
    url = f"https://www.tradingview.com/chart/?symbol={tv_symbol}"
    interval = _TV_INTERVAL.get(sig.timeframe, "")
    if interval:
        url += f"&interval={interval}"
    return url


def build_message(cfg: Config, sig: Signal) -> str:
    s = sig.snap
    head = "🚨 GUCLU FIRSAT ALARMI" if sig.tier == "STRONG" else "👀 IZLEME LISTESI"
    lines = [
        head,
        f"Coin: {sig.symbol}",
        f"Yon: {sig.direction}",
        f"Skor: {sig.score:.0f}/100",
        f"Zaman Dilimi: {sig.timeframe}",
        "",
        "Nedenler:",
    ]
    for r in sig.reasons[:9]:
        lines.append(f"✅ {r}")
    for w in sig.warnings[:3]:
        lines.append(f"⚠️ {w}")

    ref = s.don_high_20 if sig.direction == "LONG" else s.don_low_20
    ref_lbl = "Donchian 20 High" if sig.direction == "LONG" else "Donchian 20 Low"
    spread_txt = f"{s.spread_pct:.3f}%" if not math.isnan(s.spread_pct) else "n/a"
    lines += [
        "",
        "Onemli Seviyeler:",
        f"- Son fiyat: {fmt_price(s.price)}",
        f"- {ref_lbl}: {fmt_price(ref)}",
        f"- VWAP: {fmt_price(s.vwap)}",
        f"- Teknik invalidation: {sig.invalidation}",
        "",
        f"📊 Super Grafik: {tradingview_link(cfg, sig)}",
        "",
        f"Risk Notu: Risk Penalty {sig.risk_penalty:.0f}/30 | Spread {spread_txt}",
        "",
        f"ℹ️ {cfg.DISCLAIMER}",
    ]
    return "\n".join(lines)


def send_telegram(cfg: Config, text: str) -> bool:
    """Senkron HTTP (requests). Async dongude asyncio.to_thread ile cagrilir."""
    if not cfg.TELEGRAM_ENABLED:
        log.info("[TG kapali] mesaj gonderilmedi")
        return False
    if requests is None:
        log.warning("requests yuklu degil; Telegram atlaniyor")
        return False
    if not cfg.TELEGRAM_BOT_TOKEN or not cfg.TELEGRAM_CHAT_ID:
        log.warning("Telegram token/chat_id eksik")
        return False
    url = f"https://api.telegram.org/bot{cfg.TELEGRAM_BOT_TOKEN}/sendMessage"
    try:
        r = requests.post(url, json={
            "chat_id": cfg.TELEGRAM_CHAT_ID,
            "text": text,
            "disable_web_page_preview": True,
        }, timeout=15)
        if r.status_code == 200 and r.json().get("ok"):
            return True
        log.warning("Telegram hata: %s %s", r.status_code, r.text[:200])
        return False
    except Exception as e:
        log.warning("Telegram gonderim hatasi: %s", e)
        return False


# ==============================================================================
# 11) COOLDOWN / ALARM CACHE
# ==============================================================================
class CooldownManager:
    def __init__(self, cfg: Config, path: str = STORAGE_FILE):
        self.cfg = cfg
        self.path = path
        self.state: dict = self._load()

    def _load(self) -> dict:
        try:
            with open(self.path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return {}

    def _save(self) -> None:
        try:
            with open(self.path, "w", encoding="utf-8") as f:
                json.dump(self.state, f, ensure_ascii=False, indent=2)
        except Exception as e:
            log.warning("Cache yazilamadi: %s", e)

    @staticmethod
    def _key(sig: Signal) -> str:
        # zaman dilimi bazli: ayni coin farkli TF'lerde ayri alarm atabilir
        return f"{sig.symbol}|{sig.timeframe}"

    def should_alert(self, sig: Signal) -> bool:
        key = self._key(sig)
        now = time.time()
        prev = self.state.get(key)
        if prev is None:
            return True
        # yon degistiyse yeni mesaj
        if prev.get("direction") != sig.direction:
            return True
        elapsed_min = (now - prev.get("ts", 0)) / 60.0
        if elapsed_min >= self.cfg.ALERT_COOLDOWN_MINUTES:
            return True
        # cooldown icinde ama skor belirgin artmissa guncelleme
        if sig.score - prev.get("score", 0) >= self.cfg.SCORE_UPDATE_THRESHOLD:
            return True
        return False

    def record(self, sig: Signal) -> None:
        self.state[self._key(sig)] = {
            "symbol": sig.symbol,
            "timeframe": sig.timeframe,
            "direction": sig.direction,
            "score": sig.score,
            "tier": sig.tier,
            "ts": time.time(),
        }
        self._save()


# ==============================================================================
# 12) TERMINAL OZET
# ==============================================================================
def print_summary(sig: Signal) -> None:
    s = sig.snap
    sp = f"{s.spread_pct:.3f}%" if s and not math.isnan(s.spread_pct) else "n/a"
    tag = {"STRONG": "🚨", "WATCH": "👀", "NONE": "  "}.get(sig.tier, "  ")
    log.info("%s %-13s %-4s %-7s skor=%5.1f vol=%.0f volu=%.0f mom=%.0f liq=%.0f brk=%.0f risk=-%.0f | VR=%.2f ATR%%=%.2f spread=%s",
             tag, sig.symbol, sig.timeframe, sig.direction, sig.score,
             sig.volatility, sig.volume, sig.momentum, sig.liquidity,
             sig.breakout, sig.risk_penalty,
             s.volume_ratio if s else 0.0, s.atr_pct if s else 0.0, sp)


# ==============================================================================
# 13) TEK COIN ISLEME  (async, coklu zaman dilimi paralel)
# ==============================================================================
async def process_symbol(cfg: Config, client: ExchangeClient, symbol: str,
                         sem: asyncio.Semaphore) -> list[Signal]:
    """Bir coini TIMEFRAMES listesindeki HER zaman diliminde ayri ayri tarar.

    Order book + ticker + tum TF OHLCV istekleri TEK SEFERDE, es zamanli
    (asyncio.gather) cekilir. Coinler arasi paralellik `sem` ile sinirlanir.
    """
    async with sem:
        ex_symbol = client.resolve_symbol(symbol)
        if ex_symbol is None:
            log.debug("Borsa'da yok/pasif: %s (atlandi)", symbol)
            return []

        # tum istekleri paralel baslat: [orderbook, ticker, ohlcv(tf1), ohlcv(tf2), ...]
        tasks = [
            client.fetch_order_book(ex_symbol, cfg.ORDERBOOK_LIMIT),
            client.fetch_ticker(ex_symbol),
        ]
        tasks += [client.fetch_ohlcv(ex_symbol, tf, cfg.OHLCV_LIMIT) for tf in cfg.TIMEFRAMES]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        order_book = results[0] if not isinstance(results[0], Exception) else None
        ticker = results[1] if not isinstance(results[1], Exception) else None
        ohlcv_results = results[2:]

        signals: list[Signal] = []
        for tf, df in zip(cfg.TIMEFRAMES, ohlcv_results):
            if isinstance(df, Exception):
                log.warning("OHLCV hata %s [%s]: %s (atlandi)", symbol, tf, df)
                continue
            ok, reason = validate_ohlcv(df)
            if not ok:
                log.warning("Veri kalitesi dusuk %s [%s]: %s (atlandi)", symbol, tf, reason)
                continue
            snap = build_snapshot(cfg, symbol, df, order_book, ticker)
            sig = evaluate(cfg, snap, tf)
            sig.ex_symbol = ex_symbol          # TradingView linki icin gercek parite
            sig.tier = classify_alert(cfg, sig)
            signals.append(sig)
        return signals


# ==============================================================================
# 14) TARAMA TURU  (async)
# ==============================================================================
async def scan_once(cfg: Config, client: ExchangeClient, cooldown: CooldownManager,
                    symbols: list[str]) -> None:
    t0 = time.time()
    log.info("=" * 78)
    log.info("Tarama basladi | %s | %d coin | tf=%s | paralellik=%d",
             datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"),
             len(symbols), ",".join(cfg.TIMEFRAMES), cfg.MAX_CONCURRENCY)
    log.info("=" * 78)

    sem = asyncio.Semaphore(cfg.MAX_CONCURRENCY)
    tasks = [process_symbol(cfg, client, s, sem) for s in symbols]
    per_symbol = await asyncio.gather(*tasks, return_exceptions=True)

    signals: list[Signal] = []
    for symbol, res in zip(symbols, per_symbol):
        if isinstance(res, Exception):
            log.warning("Islem hatasi %s: %s (atlandi)", symbol, res)
            continue
        for sig in res:
            signals.append(sig)

    # terminal ozeti (skora gore sirali)
    for sig in sorted(signals, key=lambda x: x.score, reverse=True):
        print_summary(sig)

    # alarm gonder (Telegram HTTP'yi ayri thread'de calistir; dongu bloklanmasin)
    alerts = 0
    for sig in sorted(signals, key=lambda x: x.score, reverse=True):
        if sig.tier in ("STRONG", "WATCH") and cooldown.should_alert(sig):
            msg = build_message(cfg, sig)
            ok = await asyncio.to_thread(send_telegram, cfg, msg)
            if ok:
                log.info("📨 Telegram gonderildi: %s [%s] (%s)", sig.symbol, sig.timeframe, sig.tier)
                cooldown.record(sig)
                alerts += 1

    dt = time.time() - t0
    log.info("-" * 78)
    log.info("Tarama bitti. Sinyal(coin x TF)=%d, alarm=%d, sure=%.1fs",
             len(signals), alerts, dt)


# ==============================================================================
# 15) SELFTEST (API'siz sahte veri)
# ==============================================================================
def make_fake_df(n: int = 200, trend: float = 0.0, vol: float = 0.01,
                 seed: int = 1, volume_spike: bool = True) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    price = 100.0
    rows = []
    start = int(datetime(2025, 1, 1, tzinfo=timezone.utc).timestamp() * 1000)
    for i in range(n):
        ret = trend + rng.normal(0, vol)
        open_ = price
        close = max(0.01, open_ * (1 + ret))
        high = max(open_, close) * (1 + abs(rng.normal(0, vol / 2)))
        low = min(open_, close) * (1 - abs(rng.normal(0, vol / 2)))
        base_vol = 1000 * (1 + abs(rng.normal(0, 0.3)))
        volume = base_vol * (3.2 if (volume_spike and i == n - 1) else 1.0)
        rows.append([start + i * 300_000, open_, high, low, close, volume])
        price = close
    return pd.DataFrame(rows, columns=["timestamp", "open", "high", "low", "close", "volume"]).astype(
        {"open": float, "high": float, "low": float, "close": float, "volume": float})


def fake_order_book(price: float, spread_pct: float = 0.02, depth: float = 120000.0) -> dict:
    half = price * spread_pct / 100.0 / 2.0
    best_bid = price - half
    best_ask = price + half
    per = depth / 20.0
    bids = [[best_bid * (1 - i * 0.0005), per / max(best_bid, 1e-9)] for i in range(20)]
    asks = [[best_ask * (1 + i * 0.0005), per / max(best_ask, 1e-9)] for i in range(20)]
    return {"bids": bids, "asks": asks}


def run_selftest(cfg: Config) -> None:
    log.info("SELFTEST: sahte veriyle indikator + skorlama testi")
    scenarios = [
        ("BULL/USDT", 0.006, 0.012, 7, True),    # yukari trend + hacim spike
        ("BEAR/USDT", -0.006, 0.012, 11, True),  # asagi trend + hacim spike
        ("FLAT/USDT", 0.0, 0.002, 3, False),     # yatay / dusuk vol
    ]
    printed = False
    for name, trend, vol, seed, spike in scenarios:
        df = make_fake_df(trend=trend, vol=vol, seed=seed, volume_spike=spike)
        price = float(df["close"].iloc[-1])
        ob = fake_order_book(price)
        snap = build_snapshot(cfg, name, df, ob,
                              {"bid": ob["bids"][0][0], "ask": ob["asks"][0][0]})
        sig = evaluate(cfg, snap, cfg.TIMEFRAME_MAIN)
        sig.tier = classify_alert(cfg, sig)
        print_summary(sig)
        if sig.tier != "NONE" and not printed:
            print("\n----- ORNEK TELEGRAM MESAJI -----")
            print(build_message(cfg, sig))
            print("---------------------------------\n")
            printed = True
    log.info("SELFTEST tamam. (Gercek veri icin: python main.py --once)")


# ==============================================================================
# 16) ASYNC RUNNER
# ==============================================================================
def seconds_to_next_tick(interval: int) -> float:
    """Epoch'a hizali bir sonraki tik'e kadar kalan saniye.

    interval=300 icin tikler saat :00, :05, :10 ... noktalarina denk gelir
    (cunku 3600 / 300 = 12, saat tam boluyor). Tarama ne kadar surerse sursun
    ya da hata alsa da kadans kaymaz; her zaman bir sonraki 5-dk sinirina hizalanir.
    """
    now = time.time()
    wait = interval - (now % interval)
    if wait < 1.0:            # tam sinirdaysak cift-tetiklemeyi onle
        wait += interval
    return wait


async def ensure_markets(client: ExchangeClient) -> None:
    """Markets yuklu degilse yukle. Internet ilk acilista yoksa, gelince toparlar."""
    if not client.markets:
        await client.load_markets()


async def run_check(cfg: Config) -> None:
    """Hizli teshis: markets yuklenir ve birkac coinde ornek OHLCV cekilir.

    5 dk beklemeden baglantinin/verinin calisip calismadigini gorursun.
    """
    client = ExchangeClient(cfg)
    try:
        log.info("BAGLANTI TESTI | borsa=%s | tip=%s", client.name, cfg.MARKET_TYPE)
        await client.load_markets()
        for s in SYMBOLS[:3]:
            ex_s = client.resolve_symbol(s)
            if not ex_s:
                log.warning("  %-10s -> borsada bulunamadi (atlanir)", s)
                continue
            df = await client.fetch_ohlcv(ex_s, cfg.TIMEFRAMES[0], 5)
            if df is not None and len(df) > 0:
                log.info("  %-10s (%s) OHLCV OK | son kapanis=%s",
                         s, ex_s, fmt_price(float(df['close'].iloc[-1])))
            else:
                log.warning("  %-10s (%s) OHLCV BOS", s, ex_s)
        log.info("BAGLANTI TESTI BASARILI ✅  (normal calistir: python main.py)")
    except Exception as e:
        log.error("BAGLANTI TESTI BASARISIZ [%s]: %s", type(e).__name__, e)
        log.error("-> Bu borsa/uc nokta senin agindan erisilemiyor olabilir. "
                  ".env'de EXCHANGE_NAME=bybit ya da okx deneyebilirsin.")
    finally:
        await client.close()


async def run_scanner(cfg: Config, once: bool) -> None:
    client = ExchangeClient(cfg)
    try:
        cooldown = CooldownManager(cfg)

        if once:
            await ensure_markets(client)
            await scan_once(cfg, client, cooldown, SYMBOLS)
            return

        interval = cfg.SCAN_INTERVAL_SECONDS
        log.info("Surekli tarama | her %ds (saat dilimine hizali: :00/:05/:10...) | "
                 "tf=%s | paralellik=%d | internet kesintisine dayanikli | Ctrl+C ile durdur",
                 interval, ",".join(cfg.TIMEFRAMES), cfg.MAX_CONCURRENCY)
        while True:
            try:
                # markets yoksa yukle (ilk acilista internet yoksa burada tekrar denenir)
                await ensure_markets(client)
                await scan_once(cfg, client, cooldown, SYMBOLS)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                # baglanti / API hatasi: SISTEM DURMAZ; sonraki 5-dk tik'inde
                # otomatik tekrar denenir (kadans korunur). Gercek nedeni gormek
                # icin hata tipi + tam mesaj birlikte yazilir.
                log.error("Tarama hatasi [%s]: %s | sonraki 5-dk tik'inde tekrar denenecek",
                          type(e).__name__, e)

            # bir sonraki sabit 5-dk sinirina kadar bekle (kayma yok)
            wait = seconds_to_next_tick(interval)
            next_utc = datetime.fromtimestamp(time.time() + wait, timezone.utc).strftime("%H:%M:%S")
            log.info("Sonraki tarama ~%.0f sn sonra (%s UTC)", wait, next_utc)
            await asyncio.sleep(wait)
    finally:
        await client.close()


# ==============================================================================
# 17) MAIN
# ==============================================================================
def _run(coro) -> None:
    """Async giris noktasi.

    Windows'ta aiohttp/aiodns DNS cozumlemesi varsayilan ProactorEventLoop ile
    sorun cikarabilir; SelectorEventLoop daha guvenlidir. Python 3.14'te
    deprecated olan set_event_loop_policy/WindowsSelectorEventLoopPolicy yerine
    dogrudan SelectorEventLoop olusturulur (uyari cikmaz)."""
    if sys.platform == "win32":
        loop = asyncio.SelectorEventLoop()
        try:
            asyncio.set_event_loop(loop)
            loop.run_until_complete(coro)
        finally:
            loop.close()
    else:
        asyncio.run(coro)


def main() -> None:
    parser = argparse.ArgumentParser(description="Crypto Opportunity Scanner (async)")
    parser.add_argument("--once", action="store_true", help="Tek tur tarama yap")
    parser.add_argument("--check", action="store_true", help="Hizli baglanti/veri testi")
    parser.add_argument("--selftest", action="store_true", help="API'siz sahte veri testi")
    parser.add_argument("--tg-test", action="store_true", help="Telegram baglanti testi")
    args = parser.parse_args()

    cfg = Config()

    if args.selftest:
        run_selftest(cfg)
        return

    if args.check:
        _run(run_check(cfg))
        return

    if args.tg_test:
        ok = send_telegram(cfg, "✅ Scanner Telegram testi.\n" + cfg.DISCLAIMER)
        log.info("Telegram testi: %s", "BASARILI" if ok else "BASARISIZ")
        return

    try:
        _run(run_scanner(cfg, once=args.once))
    except KeyboardInterrupt:
        log.info("Durduruldu (kullanici).")


if __name__ == "__main__":
    main()
