import os
import time
import requests
import numpy as np  # MEJORA 1+2: cálculos ATR y percentil
from datetime import datetime, timezone, timedelta
import pandas as pd             # MEJORA 12: datos para gráficos
import mplfinance as mpf        # MEJORA 12: gráficos candlestick
import matplotlib
matplotlib.use("Agg")           # MEJORA 12: backend sin display
try:
    from sheets import log_detection, update_choch, read_closed_trades
    SHEETS_ENABLED = True
    print("✅ Google Sheets integrado")
except Exception as e:
    SHEETS_ENABLED = False
    print(f"⚠️ Google Sheets no disponible: {e}")

TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN")
CHAT_ID        = os.environ.get("CHAT_ID")
TEST_MODE      = os.environ.get("TEST_MODE", "false").lower() == "true"

LOOKBACK        = 10            # fallback si el rango dinámico falla
BODY_MULTIPLIER = 1.8           # fallback si no hay datos históricos
SL_PERCENT      = 0.02
MIN_RR          = 1.2
SETUP_EXPIRY_H  = 12

# ─── MEJORA 1: CONFIG RANGO DINÁMICO POR ATR ────────────────
ATR_PERIOD          = 14       # período del ATR de referencia
ATR_LOOKBACK        = 20       # velas para calcular ATR de referencia
LOCAL_ATR_WINDOW    = 5        # ventana del ATR local
COMPRESSION_RATIO   = 0.70     # umbral: ATR_local < ATR_ref × 0.70 = compresión
MIN_RANGE_CANDLES   = 5        # mínimo de velas para el rango
MAX_RANGE_CANDLES   = 25       # máximo de velas para el rango

# ─── MEJORA 2: CONFIG SPIKE ADAPTATIVO POR VOLATILIDAD ──────
VOL_HIST_CANDLES    = 540      # ~90 días en 4H para distribución de ATR
VOL_HIST_REFRESH    = 14400    # refrescar cada 4h (segundos)
# Tabla: (percentil_max, multiplicador)
VOL_MULTIPLIER_TABLE = [
    (25,  1.4),   # mercado muy tranquilo
    (50,  1.6),   # normal-bajo
    (75,  1.8),   # normal-alto (valor original)
    (90,  2.2),   # alta volatilidad
    (100, 2.5),   # extrema (FOMC, CPI, halving)
]

# ─── MEJORA 5: CONFIG FILTRO DE VOLUMEN EN SPIKE ────────────
SPIKE_VOL_LOOKBACK  = 20       # velas para media de volumen
SPIKE_VOL_MIN       = 1.5      # mínimo: volumen >= 1.5x la media
SPIKE_VOL_MIN_ASIA  = 1.3      # mínimo en sesión asiática (volumen natural bajo)
SPIKE_VOL_HIGH      = 2.0      # umbral para "volumen alto" (bonus scoring)

# ─── MEJORA 7: CONFIG FILTRO DE EVENTOS MACRO ───────────────
MACRO_CALENDAR_URL  = "https://nfs.faireconomy.media/ff_calendar_thisweek.json"
MACRO_REFRESH       = 14400    # refrescar calendario cada 4h
MACRO_WINDOW_BEFORE = 2        # horas de pausa ANTES del evento
MACRO_WINDOW_AFTER  = 1        # horas de pausa DESPUÉS del evento
MACRO_HIGH_IMPACT   = [        # keywords de eventos alto impacto (USD)
    "federal funds rate", "fomc", "interest rate",
    "nonfarm payrolls", "non-farm", "nfp",
    "cpi", "consumer price", "core cpi",
    "ppi", "producer price",
    "gdp", "gross domestic",
    "fomc minutes", "fomc meeting",
    "fed chair", "powell",
    "unemployment rate", "initial jobless",
]

# ─── MEJORA 8: CONFIG SCORING DINÁMICO ──────────────────────
SCORING_MIN_TRADES  = 50       # mínimo de trades cerrados para recalibrar
SCORING_SMOOTHING   = 0.70     # 70% peso nuevo + 30% peso anterior
SCORING_RECALIB_H   = 168      # recalibrar cada 168h (1 semana)
# Pesos por defecto (los iniciales, antes de recalibrar)
DEFAULT_WEIGHTS = {
    "session_opt":     20,
    "bias_aligned":    15,
    "return_completo": 12,
    "liq_alta":        12,
    "vol_alto":        8,
    "eqhl":            5,
    "rr_alto":         15,
}
FACTOR_NAMES = list(DEFAULT_WEIGHTS.keys())

TIMEFRAMES = [
    {"interval": "4h", "label": "4H", "limit": 80},
    {"interval": "1h", "label": "1H", "limit": 80},
]
SYMBOLS = ["BTCUSDT"]

# ─── SESIONES ────────────────────────────────────────────────
def get_session(dt_utc):
    hour = dt_utc.hour
    if 7  <= hour < 10: return "Londres",     "🇬🇧"
    if 12 <= hour < 13: return "Overlap",     "⚡"
    if 13 <= hour < 16: return "Nueva York",  "🇺🇸"
    if 0  <= hour < 5:  return "Asia",        "🌏"
    if 5  <= hour < 7:  return "Pre-Londres", "🌅"
    if 10 <= hour < 13: return "Mid-sesion",  "⏸"
    return "Post-NY", "🌙"

SESSION_SCORE = {
    "Londres": 20, "Nueva York": 18, "Overlap": 15,
    "Asia": 10, "Pre-Londres": 8, "Mid-sesion": 5, "Post-NY": 5,
}

# ─── TELEGRAM ────────────────────────────────────────────────
def send_telegram(message):
    url     = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {"chat_id": CHAT_ID, "text": message, "parse_mode": "HTML"}
    try:
        r = requests.post(url, json=payload, timeout=10)
        if r.status_code != 200:
            print(f"❌ Telegram: {r.text}")
    except Exception as e:
        print(f"❌ Telegram error: {e}")

# ─── MEJORA 12: ENVIAR FOTO A TELEGRAM ──────────────────────
def send_telegram_photo(image_path, caption=""):
    """Envía una imagen a Telegram con caption opcional."""
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendPhoto"
    try:
        with open(image_path, "rb") as photo:
            r = requests.post(url, data={"chat_id": CHAT_ID, "caption": caption,
                              "parse_mode": "HTML"}, files={"photo": photo}, timeout=15)
            if r.status_code != 200:
                print(f"❌ Telegram foto: {r.text}")
    except Exception as e:
        print(f"❌ Telegram foto error: {e}")

# ─── MEJORA 12: GENERAR GRÁFICO DEL SETUP ───────────────────
def generate_chart(candles, detection, levels, title="", filename="/tmp/chart_setup.png"):
    """
    Genera gráfico candlestick con el setup AMD marcado.
    - Rango de acumulación como banda azul
    - Niveles SD como líneas horizontales
    - Entrada y SL marcados
    Retorna el path del archivo generado, o None si falla.
    """
    try:
        if not candles or len(candles) < 10:
            return None

        # Convertir a DataFrame para mplfinance
        df = pd.DataFrame(candles)
        df.index = pd.DatetimeIndex(df["time"])
        df = df.rename(columns={"open": "Open", "high": "High",
                                "low": "Low", "close": "Close",
                                "volume": "Volume"})
        # Últimas 40 velas para contexto
        df = df[["Open", "High", "Low", "Close", "Volume"]].tail(40)

        if len(df) < 5:
            return None

        mt = detection["type"]
        acc_high = detection["acc_high"]
        acc_low  = detection["acc_low"]

        # Líneas horizontales: niveles SD + entrada + SL
        hlines_prices = [
            levels["entry"], levels["sl"],
            levels["sd_m1"], levels["sd_m2"], levels["sd_m25"], levels["sd_m4"],
        ]
        hlines_colors = [
            "#FFD700", "#FF4444",       # entrada (oro), SL (rojo)
            "#888888", "#00CC66", "#00AA55", "#008844",  # SDs (grises → verdes)
        ]
        hlines_styles = [
            "-", "--",           # entrada sólida, SL dashed
            ":", "-", "-", ":",  # SDs
        ]
        hlines_widths = [1.2, 1.2, 0.6, 0.9, 0.9, 0.6]

        # Banda del rango de acumulación
        fill = dict(y1=acc_low, y2=acc_high, alpha=0.15, color="#4488FF")

        # Estilo oscuro
        mc = mpf.make_marketcolors(up="#26A69A", down="#EF5350",
                                    edge="inherit", wick="inherit",
                                    volume={"up": "#26A69A", "down": "#EF5350"})
        s = mpf.make_mpf_style(base_mpf_style="nightclouds", marketcolors=mc,
                               gridstyle=":", gridcolor="#333333",
                               facecolor="#1A1A2E", figcolor="#1A1A2E",
                               rc={"font.size": 8})

        # Título
        direction = "🔴 BAJISTA" if mt == "BAJISTA" else "🟢 ALCISTA"
        chart_title = f"{title} | {direction}" if title else direction

        # Generar
        fig, axes = mpf.plot(
            df, type="candle", style=s, volume=True,
            title=chart_title,
            hlines=dict(hlines=hlines_prices, colors=hlines_colors,
                        linestyle=hlines_styles, linewidths=hlines_widths),
            fill_between=fill,
            figsize=(10, 6), returnfig=True,
            tight_layout=True,
        )

        # Anotar niveles en el eje de precio
        ax = axes[0]
        y_min, y_max = ax.get_ylim()
        x_pos = len(df) - 1  # extremo derecho

        labels = [
            (levels["entry"], "OTE",     "#FFD700"),
            (levels["sl"],    "SL",      "#FF4444"),
            (levels["sd_m1"], "-1SD",    "#888888"),
            (levels["sd_m2"], "TP1",     "#00CC66"),
            (levels["sd_m25"],"TP2",     "#00AA55"),
            (levels["sd_m4"], "-4SD",    "#008844"),
        ]
        for price, lbl, color in labels:
            if y_min <= price <= y_max:
                ax.text(x_pos + 0.5, price, f" {lbl} ${price:,.0f}",
                        fontsize=7, color=color, va="center",
                        bbox=dict(boxstyle="round,pad=0.1", fc="#1A1A2E", ec=color, alpha=0.8))

        # Rango label
        mid_range = (acc_high + acc_low) / 2
        if y_min <= mid_range <= y_max:
            ax.text(1, mid_range, " RANGO", fontsize=7, color="#4488FF",
                    va="center", alpha=0.7)

        fig.savefig(filename, dpi=120, bbox_inches="tight",
                    facecolor="#1A1A2E", edgecolor="none")
        import matplotlib.pyplot as plt
        plt.close(fig)

        print(f"  📸 Gráfico generado: {filename}")
        return filename

    except Exception as e:
        print(f"  ⚠️ Error generando gráfico: {e}")
        return None

# ─── MEJORA 7: CALENDARIO DE EVENTOS MACRO ──────────────────
def fetch_macro_events():
    """
    Descarga el calendario económico de la semana (ForexFactory).
    Filtra solo eventos de alto impacto que afecten USD.
    Retorna lista de dicts con 'title' y 'datetime' (UTC).
    """
    try:
        r = requests.get(MACRO_CALENDAR_URL, timeout=10)
        if r.status_code != 200:
            print(f"  ⚠️ Calendario macro: HTTP {r.status_code}")
            return []

        events = r.json()
        filtered = []

        for ev in events:
            # Solo USD y alto impacto
            if ev.get("country", "") != "USD":
                continue
            if ev.get("impact", "").lower() != "high":
                continue

            # Verificar si es un evento relevante
            title = ev.get("title", "").lower()
            is_relevant = any(kw in title for kw in MACRO_HIGH_IMPACT)
            if not is_relevant:
                continue

            # Parsear fecha y hora
            date_str = ev.get("date", "")
            time_str = ev.get("time", "")
            if not date_str:
                continue

            try:
                if time_str and time_str.lower() not in ["", "all day", "tentative"]:
                    # Formato: "8:30am" → convertir a datetime UTC (FF usa ET, UTC-4/-5)
                    dt_str = f"{date_str} {time_str}"
                    dt_et = datetime.strptime(dt_str, "%Y-%m-%d %I:%M%p")
                    # ForexFactory usa Eastern Time → UTC+4 (aproximado, sin DST exacto)
                    dt_utc = dt_et.replace(tzinfo=timezone.utc) + timedelta(hours=4)
                else:
                    # Evento sin hora específica → usar 14:00 UTC como default
                    dt_utc = datetime.strptime(date_str, "%Y-%m-%d").replace(
                        hour=14, tzinfo=timezone.utc)

                filtered.append({
                    "title": ev.get("title", ""),
                    "datetime": dt_utc,
                    "impact": ev.get("impact", "High"),
                })
            except (ValueError, TypeError):
                continue

        print(f"  📅 Calendario macro: {len(filtered)} eventos de alto impacto esta semana")
        for ev in filtered:
            print(f"     → {ev['datetime'].strftime('%d/%m %H:%M')} UTC: {ev['title']}")
        return filtered

    except Exception as e:
        print(f"  ⚠️ Error calendario macro: {e}")
        return []

def is_in_macro_window(macro_events, now_utc):
    """
    Verifica si estamos dentro de la ventana de protección de algún evento.
    Ventana: MACRO_WINDOW_BEFORE horas antes → MACRO_WINDOW_AFTER horas después.
    Retorna: (in_window: bool, event_title: str or None, minutes_to_event: int or None)
    """
    for ev in macro_events:
        ev_time = ev["datetime"]
        window_start = ev_time - timedelta(hours=MACRO_WINDOW_BEFORE)
        window_end   = ev_time + timedelta(hours=MACRO_WINDOW_AFTER)

        if window_start <= now_utc <= window_end:
            minutes_to = int((ev_time - now_utc).total_seconds() / 60)
            return True, ev["title"], minutes_to

    return False, None, None

# ─── MEJORA 8: RECALIBRACIÓN DE PESOS ────────────────────────
def recalibrate_weights(current_weights):
    """
    Lee trades cerrados de Sheets y recalibra pesos del scoring.
    Calcula correlación de cada factor con el resultado (win/loss).
    Aplica suavizado 70/30 con pesos anteriores.
    Retorna dict de pesos nuevos o current_weights si no hay datos suficientes.
    """
    if not SHEETS_ENABLED:
        return current_weights

    try:
        trades = read_closed_trades()
    except Exception as e:
        print(f"  ⚠️ Error leyendo trades para recalibrar: {e}")
        return current_weights

    if len(trades) < SCORING_MIN_TRADES:
        print(f"  📊 Recalibración: {len(trades)}/{SCORING_MIN_TRADES} trades — insuficientes, usando pesos actuales")
        return current_weights

    # Calcular correlación de cada factor con el resultado
    wins = [t["win"] for t in trades]
    win_rate = np.mean(wins)

    correlations = {}
    for factor in FACTOR_NAMES:
        factor_vals = [t.get(factor, 0) for t in trades]

        # Correlación: winrate cuando factor=1 vs winrate cuando factor=0
        with_factor    = [w for w, f in zip(wins, factor_vals) if f == 1]
        without_factor = [w for w, f in zip(wins, factor_vals) if f == 0]

        wr_with    = np.mean(with_factor) if with_factor else win_rate
        wr_without = np.mean(without_factor) if without_factor else win_rate

        # Correlación = diferencia en winrate (puede ser negativa)
        correlations[factor] = max(0, wr_with - wr_without)

    # Normalizar correlaciones a pesos (suma = 100)
    total_corr = sum(correlations.values())
    if total_corr == 0:
        print("  ⚠️ Recalibración: correlaciones todas 0, manteniendo pesos actuales")
        return current_weights

    raw_weights = {}
    for factor in FACTOR_NAMES:
        raw_weights[factor] = (correlations[factor] / total_corr) * 100

    # Suavizado: 70% pesos nuevos + 30% pesos anteriores
    new_weights = {}
    for factor in FACTOR_NAMES:
        old_w = current_weights.get(factor, DEFAULT_WEIGHTS[factor])
        new_w = raw_weights[factor]
        new_weights[factor] = round(SCORING_SMOOTHING * new_w + (1 - SCORING_SMOOTHING) * old_w, 1)

    print(f"  📊 Recalibración completada ({len(trades)} trades):")
    for factor in FACTOR_NAMES:
        old = current_weights.get(factor, DEFAULT_WEIGHTS[factor])
        new = new_weights[factor]
        arrow = "↑" if new > old else ("↓" if new < old else "=")
        print(f"     {factor}: {old:.1f} → {new:.1f} {arrow}")

    return new_weights

# ─── VELAS BINANCE ───────────────────────────────────────────
def get_candles(symbol, interval="4h", limit=80):
    url    = "https://api.binance.com/api/v3/klines"
    params = {"symbol": symbol, "interval": interval, "limit": limit}
    try:
        r    = requests.get(url, params=params, timeout=10)
        data = r.json()
        if not isinstance(data, list):
            return []
        return [{
            "time":       datetime.fromtimestamp(c[0] / 1000, tz=timezone.utc),
            "open":       float(c[1]),
            "high":       float(c[2]),
            "low":        float(c[3]),
            "close":      float(c[4]),
            "volume":     float(c[5]),
            "body_high":  max(float(c[1]), float(c[4])),
            "body_low":   min(float(c[1]), float(c[4])),
            "body_size":  abs(float(c[4]) - float(c[1])),
            "total_size": abs(float(c[2]) - float(c[3])),
            "is_bullish": float(c[4]) >= float(c[1])
        } for c in data]
    except Exception as e:
        print(f"❌ Velas {symbol} {interval}: {e}")
        return []

def get_current_price(symbol):
    try:
        r = requests.get("https://api.binance.com/api/v3/ticker/price",
                         params={"symbol": symbol}, timeout=5)
        return float(r.json()["price"])
    except:
        return None

# ─── CIERRE DE VELA ──────────────────────────────────────────
def candle_closed(candle, interval):
    secs = {"1h": 3600, "4h": 14400, "1d": 86400, "15m": 900}.get(interval, 3600)
    return datetime.now(timezone.utc).timestamp() >= candle["time"].timestamp() + secs

def get_closed(candles, interval):
    return [c for c in candles if candle_closed(c, interval)]

# ─── SESGO MACRO ─────────────────────────────────────────────
def get_macro_bias(candles_1d):
    if len(candles_1d) < 20:
        return "NEUTRAL"
    closed = get_closed(candles_1d, "1d")
    if len(closed) < 20:
        return "NEUTRAL"
    closes = [c["close"] for c in closed[-20:]]
    ema20  = sum(closes) / len(closes)
    price  = closes[-1]
    highs  = [c["high"] for c in closed[-20:]]
    lows   = [c["low"]  for c in closed[-20:]]
    hh = max(highs[-10:]) > max(highs[-20:-10])
    hl = min(lows[-10:])  > min(lows[-20:-10])
    lh = max(highs[-10:]) < max(highs[-20:-10])
    ll = min(lows[-10:])  < min(lows[-20:-10])
    if hh and hl and price > ema20: return "BULLISH"
    if lh and ll and price < ema20: return "BEARISH"
    return "NEUTRAL"

# ─── ESTRUCTURA 4H ───────────────────────────────────────────
def get_structure_bias(candles_4h):
    closed = get_closed(candles_4h, "4h")
    if len(closed) < 15:
        return "NEUTRAL", None
    recent     = closed[-15:]
    swing_high = max(c["high"] for c in recent[:-3])
    swing_low  = min(c["low"]  for c in recent[:-3])
    last_close = recent[-1]["close"]
    if last_close > swing_high: return "BULLISH", swing_high
    if last_close < swing_low:  return "BEARISH", swing_low
    return "NEUTRAL", None

# ─── PD ARRAYS ───────────────────────────────────────────────
def find_pd_arrays(candles, interval):
    closed    = get_closed(candles, interval)
    fvgs, obs = [], []
    for i in range(1, len(closed) - 1):
        prev, curr, nxt = closed[i-1], closed[i], closed[i+1]
        if curr["low"] > prev["high"] and nxt["close"] > curr["high"]:
            fvgs.append({"type": "alcista", "top": curr["low"],
                         "bottom": prev["high"], "mid": (curr["low"] + prev["high"]) / 2})
        if curr["high"] < prev["low"] and nxt["close"] < curr["low"]:
            fvgs.append({"type": "bajista", "top": prev["low"],
                         "bottom": curr["high"], "mid": (prev["low"] + curr["high"]) / 2})
        if (curr["close"] < curr["open"] and nxt["close"] > nxt["open"] and
                abs(nxt["close"] - nxt["open"]) > abs(curr["close"] - curr["open"]) * 1.5):
            obs.append({"type": "alcista", "top": curr["open"],
                        "bottom": curr["close"], "mid": (curr["open"] + curr["close"]) / 2})
        if (curr["close"] > curr["open"] and nxt["close"] < nxt["open"] and
                abs(nxt["close"] - nxt["open"]) > abs(curr["close"] - curr["open"]) * 1.5):
            obs.append({"type": "bajista", "top": curr["close"],
                        "bottom": curr["open"], "mid": (curr["close"] + curr["open"]) / 2})
    return fvgs[-10:], obs[-5:]

# ─── MEJORA 1: CÁLCULO DE TRUE RANGE Y ATR ──────────────────
def calc_true_range(candles):
    """Calcula True Range para cada vela (usa dicts del bot)."""
    if len(candles) < 2:
        return [c["high"] - c["low"] for c in candles]
    tr = [candles[0]["high"] - candles[0]["low"]]
    for i in range(1, len(candles)):
        h = candles[i]["high"]
        l = candles[i]["low"]
        cp = candles[i - 1]["close"]
        tr.append(max(h - l, abs(h - cp), abs(l - cp)))
    return tr

def calc_atr(candles, period=14):
    """ATR con media simple sobre las últimas 'period' velas."""
    if len(candles) < period:
        return None
    tr = calc_true_range(candles)
    return np.mean(tr[-period:])

def calc_atr_local(candles, window=5):
    """ATR local con ventana pequeña para medir volatilidad instantánea."""
    if len(candles) < 2:
        return candles[0]["high"] - candles[0]["low"] if candles else 0.0
    tr = calc_true_range(candles)
    return np.mean(tr[-window:]) if len(tr) >= window else np.mean(tr)

# ─── MEJORA 2: PERCENTIL DE VOLATILIDAD Y MULTIPLICADOR ─────
def calc_volatility_percentile(candles_hist, current_candles):
    """
    Calcula el percentil del ATR(14) actual respecto a la distribución
    histórica de ~90 días. Retorna (percentil 0-100, multiplicador).
    """
    if not candles_hist or len(candles_hist) < ATR_PERIOD + 10:
        return 50.0, BODY_MULTIPLIER  # fallback

    # ATR actual
    source = current_candles if len(current_candles) >= ATR_PERIOD else candles_hist
    atr_actual = calc_atr(source, ATR_PERIOD)
    if atr_actual is None or atr_actual == 0:
        return 50.0, BODY_MULTIPLIER

    # Distribución histórica de ATR(14)
    tr_hist = calc_true_range(candles_hist)
    atr_distribution = []
    for i in range(ATR_PERIOD, len(tr_hist)):
        atr_distribution.append(np.mean(tr_hist[i - ATR_PERIOD:i]))

    if not atr_distribution:
        return 50.0, BODY_MULTIPLIER

    # Percentil
    below = sum(1 for a in atr_distribution if a < atr_actual)
    percentile = (below / len(atr_distribution)) * 100

    # Multiplicador según tabla
    multiplier = BODY_MULTIPLIER
    for pct_max, mult in VOL_MULTIPLIER_TABLE:
        if percentile <= pct_max:
            multiplier = mult
            break

    return round(percentile, 1), multiplier

# ─── MEJORA 1: RANGO DE ACUMULACIÓN DINÁMICO ────────────────
def get_accumulation_range(prev_candles, all_candles=None, spike_index=None):
    """
    Detecta el rango de acumulación dinámico por compresión de ATR.
    Si all_candles y spike_index se proporcionan → usa detección dinámica.
    Si no → fallback al método fijo original (compatibilidad).
    Retorna: (acc_high, acc_low, num_candles, compression_level)
    """
    # ── Fallback: método fijo original ────────────────────────
    if all_candles is None or spike_index is None:
        h = max(c["body_high"] for c in prev_candles)
        l = min(c["body_low"]  for c in prev_candles)
        return h, l, len(prev_candles), 0.0

    # ── Detección dinámica por ATR ────────────────────────────
    ref_start = max(0, spike_index - ATR_LOOKBACK - MAX_RANGE_CANDLES)
    ref_end   = spike_index - MIN_RANGE_CANDLES
    if ref_end <= ref_start or ref_end < ATR_PERIOD:
        h = max(c["body_high"] for c in prev_candles)
        l = min(c["body_low"]  for c in prev_candles)
        return h, l, len(prev_candles), 0.0

    ref_candles = all_candles[ref_start:ref_end]
    atr_ref = calc_atr(ref_candles, ATR_PERIOD)
    if atr_ref is None or atr_ref == 0:
        h = max(c["body_high"] for c in prev_candles)
        l = min(c["body_low"]  for c in prev_candles)
        return h, l, len(prev_candles), 0.0

    umbral = atr_ref * COMPRESSION_RATIO

    end_idx   = spike_index - 1
    start_idx = end_idx
    count     = 1
    atrs_locales = []

    for i in range(end_idx, max(end_idx - MAX_RANGE_CANDLES, -1), -1):
        local_start = max(0, i - LOCAL_ATR_WINDOW + 1)
        local_slice = all_candles[local_start:i + 1]
        if len(local_slice) < 2:
            break
        atr_local = calc_atr_local(local_slice, LOCAL_ATR_WINDOW)
        atrs_locales.append(atr_local)
        if atr_local < umbral:
            start_idx = i
            count = end_idx - start_idx + 1
        else:
            if count >= MIN_RANGE_CANDLES:
                break
            start_idx = i
            count = end_idx - start_idx + 1

    if count < MIN_RANGE_CANDLES:
        start_idx = end_idx - MIN_RANGE_CANDLES + 1
        if start_idx < 0:
            start_idx = 0
        count = end_idx - start_idx + 1

    rango_candles = all_candles[start_idx:end_idx + 1]
    if not rango_candles:
        h = max(c["body_high"] for c in prev_candles)
        l = min(c["body_low"]  for c in prev_candles)
        return h, l, len(prev_candles), 0.0

    range_high = max(c["body_high"] for c in rango_candles)
    range_low  = min(c["body_low"]  for c in rango_candles)

    if range_high <= range_low:
        h = max(c["body_high"] for c in prev_candles)
        l = min(c["body_low"]  for c in prev_candles)
        return h, l, len(prev_candles), 0.0

    atr_local_medio = np.mean(atrs_locales) if atrs_locales else 0.0
    compression = (atr_local_medio / atr_ref) if atr_ref > 0 else 0.0

    return range_high, range_low, count, compression

# ─── MEJORA 3: ANÁLISIS DE LIQUIDEZ POR CONTEO DE TOQUES ────
def analyze_liquidity(prev_candles, acc_high, acc_low, manip_type):
    """
    Analiza la liquidez acumulada en el nivel barrido por el spike.
    Cuenta toques al Range High/Low y detecta Equal Highs/Lows.

    Retorna dict:
      has_liq:     bool  — hay liquidez suficiente (compatibilidad)
      touches:     int   — toques dentro del 0.15% del nivel
      liq_level:   str   — "BAJA" / "NORMAL" / "ALTA"
      has_eqhl:    bool  — hay Equal Highs o Equal Lows
      eqhl_count:  int   — cuántos EQH/EQL detectados
    """
    if not prev_candles:
        return {"has_liq": False, "touches": 0, "liq_level": "BAJA",
                "has_eqhl": False, "eqhl_count": 0}

    # ── Conteo de toques al nivel barrido ─────────────────────
    if manip_type == "BAJISTA":
        level = acc_high
        prices = [c["high"] for c in prev_candles]
    else:
        level = acc_low
        prices = [c["low"] for c in prev_candles]

    touch_tol = level * 0.0015   # 0.15% de tolerancia
    touches = sum(1 for p in prices if abs(p - level) <= touch_tol)

    # ── Clasificación ─────────────────────────────────────────
    if touches >= 5:
        liq_level = "ALTA"
    elif touches >= 3:
        liq_level = "NORMAL"
    else:
        liq_level = "BAJA"

    # ── Detección de Equal Highs / Equal Lows ─────────────────
    # EQH/EQL = 2+ velas cuyos highs (o lows) están dentro del 0.05%
    eq_tol = level * 0.0005   # 0.05%
    eqhl_count = 0

    if manip_type == "BAJISTA":
        # Buscar Equal Highs cerca del Range High
        near_highs = [p for p in prices if abs(p - level) <= touch_tol]
        for i in range(len(near_highs)):
            for j in range(i + 1, len(near_highs)):
                if abs(near_highs[i] - near_highs[j]) <= eq_tol:
                    eqhl_count += 1
    else:
        # Buscar Equal Lows cerca del Range Low
        near_lows = [p for p in prices if abs(p - level) <= touch_tol]
        for i in range(len(near_lows)):
            for j in range(i + 1, len(near_lows)):
                if abs(near_lows[i] - near_lows[j]) <= eq_tol:
                    eqhl_count += 1

    has_eqhl = eqhl_count >= 1

    return {
        "has_liq":    touches >= 1,
        "touches":    touches,
        "liq_level":  liq_level,
        "has_eqhl":   has_eqhl,
        "eqhl_count": eqhl_count,
    }

# ─── MEJORA 4: RETORNO AL RANGO PROPORCIONAL ─────────────────
def verify_return_to_range(candles, interval, manip_candle, acc_high, acc_low,
                           range_num_candles=10, manip_type=None):
    """
    Verifica si el precio volvió al rango tras el spike.
    Ventana proporcional: 30% de la duración del rango, mín 2, máx 6.

    Retorna dict:
      returned:     bool  — retorno completo (cierre dentro del rango)
      return_type:  str   — "COMPLETO" / "PARCIAL" / "NINGUNO"
      return_candle: int  — en qué vela post-spike ocurrió (0 = no)
      window_used:  int   — ventana de velas usada
    """
    closed     = get_closed(candles, interval)
    manip_time = manip_candle["time"]
    post       = [c for c in closed if c["time"] > manip_time]

    # Ventana proporcional al rango
    window = max(2, min(6, round(range_num_candles * 0.30)))

    result = {
        "returned": False, "return_type": "NINGUNO",
        "return_candle": 0, "window_used": window
    }

    for i, c in enumerate(post[:window]):
        # Retorno completo: cierre dentro del rango
        if acc_low <= c["close"] <= acc_high:
            result["returned"]     = True
            result["return_type"]  = "COMPLETO"
            result["return_candle"] = i + 1
            return result

        # Retorno parcial: el high o low toca el rango pero no cierra dentro
        if manip_type == "BAJISTA":
            # Tras spike bajista, esperamos que el low baje hasta acc_high
            if c["low"] <= acc_high:
                if result["return_type"] == "NINGUNO":
                    result["return_type"]  = "PARCIAL"
                    result["return_candle"] = i + 1
        else:
            # Tras spike alcista, esperamos que el high suba hasta acc_low
            if c["high"] >= acc_low:
                if result["return_type"] == "NINGUNO":
                    result["return_type"]  = "PARCIAL"
                    result["return_candle"] = i + 1

    # Si hubo retorno parcial, marcamos returned como True (pero parcial)
    if result["return_type"] == "PARCIAL":
        result["returned"] = True

    return result

# ─── ÚLTIMO SPIKE ────────────────────────────────────────────
def is_last_spike(candles, interval, manip_candle, avg_size, manip_type):
    closed     = get_closed(candles, interval)
    manip_time = manip_candle["time"]
    post       = [c for c in closed if c["time"] > manip_time]
    for c in post:
        if c["total_size"] >= avg_size * 1.5:
            if manip_type == "ALCISTA" and c["low"] < manip_candle["low"]:
                return False
            if manip_type == "BAJISTA" and c["high"] > manip_candle["high"]:
                return False
    return True

# ─── DETECCIÓN DE MANIPULACIÓN ───────────────────────────────
def detect_manipulation(candles, interval, vol_percentile=50.0, adaptive_mult=None):
    """
    MEJORA 2: acepta vol_percentile y adaptive_mult.
    Si adaptive_mult se proporciona, lo usa en vez de BODY_MULTIPLIER fijo.
    """
    spike_multiplier = adaptive_mult if adaptive_mult is not None else BODY_MULTIPLIER

    closed = get_closed(candles, interval)
    if len(closed) < LOOKBACK + 1:
        return None

    # MEJORA 1: el spike siempre es la última vela cerrada
    manip       = closed[-1]
    spike_index = len(closed) - 1

    # MEJORA 1: rango dinámico por ATR
    prev_candles_fallback = closed[-(LOOKBACK + 1):-1]
    acc_high, acc_low, range_num_candles, compression_level = get_accumulation_range(
        prev_candles_fallback, all_candles=closed, spike_index=spike_index
    )
    acc_range = acc_high - acc_low
    if acc_range <= 0:
        return None

    # MEJORA 1: prev_candles del rango dinámico
    range_start = max(0, spike_index - range_num_candles)
    prev_candles = closed[range_start:spike_index]
    if not prev_candles:
        prev_candles = prev_candles_fallback

    avg_size   = sum(c["total_size"] for c in prev_candles) / len(prev_candles)
    manip_size = manip["total_size"]
    # MEJORA 2: usa spike_multiplier adaptativo
    if manip_size < avg_size * spike_multiplier:
        return None

    body_pct = manip["body_size"] / manip_size if manip_size > 0 else 0
    if body_pct < 0.40:
        return None

    manip_type = None
    wick_ratio = 0

    if manip["high"] > acc_high and not manip["is_bullish"]:
        upper_wick = manip["high"] - manip["open"]
        wick_ratio = upper_wick / manip_size if manip_size > 0 else 0
        if wick_ratio >= 0.15:
            manip_type = "BAJISTA"
    elif manip["low"] < acc_low and manip["is_bullish"]:
        lower_wick = manip["open"] - manip["low"]
        wick_ratio = lower_wick / manip_size if manip_size > 0 else 0
        if wick_ratio >= 0.15:
            manip_type = "ALCISTA"

    if not manip_type:
        return None

    # MEJORA 5: filtro de volumen en el spike
    session_name, session_emoji = get_session(manip["time"])
    vol_threshold = SPIKE_VOL_MIN_ASIA if session_name == "Asia" else SPIKE_VOL_MIN
    vol_candles = closed[max(0, spike_index - SPIKE_VOL_LOOKBACK):spike_index]
    if vol_candles:
        avg_volume = sum(c["volume"] for c in vol_candles) / len(vol_candles)
        spike_vol_ratio = manip["volume"] / avg_volume if avg_volume > 0 else 0
    else:
        avg_volume = 0
        spike_vol_ratio = 0

    if spike_vol_ratio < vol_threshold:
        return None  # spike sin volumen = no es barrida institucional

    if spike_vol_ratio >= SPIKE_VOL_HIGH:
        spike_vol_level = "ALTO"
    else:
        spike_vol_level = "CONFIRMADO"

    # MEJORA 3: análisis de liquidez completo
    liq_data = analyze_liquidity(prev_candles, acc_high, acc_low, manip_type)
    has_liq = liq_data["has_liq"]

    if not is_last_spike(candles, interval, manip, avg_size, manip_type):
        return None

    # MEJORA 4: retorno al rango proporcional
    return_data = verify_return_to_range(
        candles, interval, manip, acc_high, acc_low,
        range_num_candles=range_num_candles, manip_type=manip_type
    )
    returned = return_data["returned"]

    # ── ANCLAJE CORRECTO TTRADES ──────────────────────────────
    if manip_type == "BAJISTA":
        fib_1 = manip["high"]
        fib_0 = acc_low
    else:
        fib_1 = manip["low"]
        fib_0 = acc_high

    # Swing previo para CHoCH 15M
    if manip_type == "BAJISTA":
        swing_choch = min(c["low"] for c in prev_candles[-5:])
    else:
        swing_choch = max(c["high"] for c in prev_candles[-5:])

    return {
        "type":          manip_type,
        "candle":        manip,
        "session":       session_name,
        "session_emoji": session_emoji,
        "acc_high":      acc_high,
        "acc_low":       acc_low,
        "acc_range":     acc_range,
        "avg_size":      avg_size,
        "wick_ratio":    round(wick_ratio, 2),
        "returned":      returned,
        # MEJORA 4: retorno detallado
        "return_type":   return_data["return_type"],
        "return_candle": return_data["return_candle"],
        "return_window": return_data["window_used"],
        "has_liq":       has_liq,
        # MEJORA 3: liquidez detallada
        "liq_touches":   liq_data["touches"],
        "liq_level":     liq_data["liq_level"],
        "has_eqhl":      liq_data["has_eqhl"],
        "eqhl_count":    liq_data["eqhl_count"],
        "swing_choch":   swing_choch,
        "fib_1":         fib_1,
        "fib_0":         fib_0,
        # MEJORA 1
        "range_num_candles":  range_num_candles,
        "compression_level": round(compression_level, 4),
        # MEJORA 2
        "vol_percentile":   vol_percentile,
        "spike_multiplier": spike_multiplier,
        # MEJORA 5
        "spike_vol_ratio":  round(spike_vol_ratio, 2),
        "spike_vol_level":  spike_vol_level,
    }

# ─── MEJORA 6: CHoCH EN 15M CON FILTRO DE CALIDAD ───────────
def check_choch_15m(symbol, detection):
    """
    Verifica CHoCH en 15M con filtro de calidad.
    Condición A: cuerpo >= 50% del tamaño total de la vela.
    Condición B: cierre en el tercio favorable.

    Retorna: (confirmed, price, quality)
      confirmed: bool
      price: float o None
      quality: "FUERTE" / "DEBIL" / None
    """
    candles_15m = get_candles(symbol, "15m", 20)
    if not candles_15m:
        return False, None, None
    closed_15m = get_closed(candles_15m, "15m")
    if not closed_15m:
        return False, None, None

    swing      = detection["swing_choch"]
    manip_type = detection["type"]
    manip_time = detection["candle"]["time"]
    post       = [c for c in closed_15m if c["time"] > manip_time]

    first_break_idx = None  # índice de la primera vela que rompe el swing

    for i, c in enumerate(post):
        # ¿Rompió el swing?
        broke_swing = False
        if manip_type == "BAJISTA" and c["close"] < swing:
            broke_swing = True
        elif manip_type == "ALCISTA" and c["close"] > swing:
            broke_swing = True

        if not broke_swing:
            # Si ya hubo un break débil y pasaron 3 velas más sin fuerte → débil
            if first_break_idx is not None and i >= first_break_idx + 4:
                return True, post[first_break_idx]["close"], "DEBIL"
            continue

        # ── Evaluar calidad de la vela que rompe ──────────────
        candle_range = c["high"] - c["low"]
        if candle_range == 0:
            # Vela sin rango (doji perfecto) — no es calidad
            if first_break_idx is None:
                first_break_idx = i
            continue

        body = abs(c["close"] - c["open"])

        # Condición A: cuerpo >= 50% del tamaño total
        body_ok = (body / candle_range) >= 0.50

        # Condición B: cierre en el tercio favorable
        if manip_type == "BAJISTA":
            # Cierre en el tercio inferior
            third_ok = c["close"] <= c["high"] - candle_range * 0.66
        else:
            # Cierre en el tercio superior
            third_ok = c["close"] >= c["low"] + candle_range * 0.66

        if body_ok and third_ok:
            return True, c["close"], "FUERTE"
        else:
            # Rompió pero no cumple calidad — registrar como primer break
            if first_break_idx is None:
                first_break_idx = i

    # Si hubo algún break pero ninguno fue fuerte
    if first_break_idx is not None:
        return True, post[first_break_idx]["close"], "DEBIL"

    return False, None, None

# ─── CALCULAR NIVELES FIBONACCI (anclaje TTrades) ────────────
def calculate_levels(detection):
    mt    = detection["type"]
    fib_1 = detection["fib_1"]
    fib_0 = detection["fib_0"]
    rango = abs(fib_1 - fib_0)

    if mt == "BAJISTA":
        eq     = fib_1 - rango * 0.50
        entry  = fib_1 - rango * 0.705
        sl     = fib_1 * (1 + 0.005)
        sd_m1  = fib_0 - rango * 1.0
        sd_m2  = fib_0 - rango * 2.0
        sd_m25 = fib_0 - rango * 2.5
        sd_m4  = fib_0 - rango * 4.0
        tp1, tp2 = sd_m2, sd_m25
        rr = (entry - tp1) / (sl - entry) if (sl - entry) > 0 else 0
    else:
        eq     = fib_1 + rango * 0.50
        entry  = fib_1 + rango * 0.705
        sl     = fib_1 * (1 - 0.005)
        sd_m1  = fib_0 + rango * 1.0
        sd_m2  = fib_0 + rango * 2.0
        sd_m25 = fib_0 + rango * 2.5
        sd_m4  = fib_0 + rango * 4.0
        tp1, tp2 = sd_m2, sd_m25
        rr = (tp1 - entry) / (entry - sl) if (entry - sl) > 0 else 0

    return {
        "fib_1": fib_1, "fib_0": fib_0, "rango": rango,
        "eq": eq, "entry": entry, "sl": sl,
        "sd_m1": sd_m1, "sd_m2": sd_m2, "sd_m25": sd_m25, "sd_m4": sd_m4,
        "tp1": tp1, "tp2": tp2, "rr": rr
    }

# ─── CONFLUENCIAS ────────────────────────────────────────────
def detect_confluences(candles, levels, detection, interval, fvgs, obs, macro_bias, structure_bias):
    confs  = []
    entry  = levels["entry"]
    tp1    = levels["tp1"]
    tol    = levels["fib_1"] * 0.003
    closed = get_closed(candles, interval)
    mt     = detection["type"]

    if macro_bias == "BULLISH" and mt == "ALCISTA":
        confs.append("Alineado con sesgo Daily BULLISH")
    elif macro_bias == "BEARISH" and mt == "BAJISTA":
        confs.append("Alineado con sesgo Daily BEARISH")
    elif macro_bias != "NEUTRAL":
        confs.append(f"Contra sesgo Daily ({macro_bias})")

    if structure_bias == "BULLISH" and mt == "ALCISTA":
        confs.append("CHoCH 4H confirmado BULLISH")
    elif structure_bias == "BEARISH" and mt == "BAJISTA":
        confs.append("CHoCH 4H confirmado BEARISH")

    # MEJORA 4: confluencia de retorno detallada
    return_type = detection.get("return_type", "NINGUNO")
    ret_candle  = detection.get("return_candle", 0)
    ret_window  = detection.get("return_window", 3)
    if return_type == "COMPLETO":
        confs.append(f"Retorno COMPLETO al rango (vela {ret_candle}/{ret_window})")
    elif return_type == "PARCIAL":
        confs.append(f"Retorno parcial al rango (vela {ret_candle}/{ret_window})")
    # MEJORA 3: confluencia de liquidez detallada
    liq_level  = detection.get("liq_level", "BAJA")
    liq_touches = detection.get("liq_touches", 0)
    if liq_level == "ALTA":
        confs.append(f"Liquidez ALTA ({liq_touches} toques al nivel)")
    elif liq_level == "NORMAL":
        confs.append(f"Liquidez normal ({liq_touches} toques al nivel)")
    elif detection.get("has_liq"):
        confs.append(f"Liquidez baja ({liq_touches} toque)")
    if detection.get("has_eqhl"):
        eqn = detection.get("eqhl_count", 0)
        eqtype = "EQH" if mt == "BAJISTA" else "EQL"
        confs.append(f"{eqtype} detectados ({eqn} pares) — iman de liquidez")

    for fvg in fvgs:
        if abs(fvg["mid"] - entry) < tol * 5:
            confs.append(f"FVG {fvg['type']} en zona de entrada")
        elif abs(fvg["mid"] - tp1) < tol * 5:
            confs.append(f"FVG {fvg['type']} en TP1")
    for ob in obs:
        if abs(ob["mid"] - entry) < tol * 5:
            confs.append(f"Order Block {ob['type']} en zona de entrada")

    highs = [c["high"] for c in closed[-30:]]
    lows  = [c["low"]  for c in closed[-30:]]
    if sum(1 for h in highs if abs(h - tp1) < tol) >= 2:
        confs.append("Equal Highs cerca del TP1")
    if sum(1 for l in lows if abs(l - tp1) < tol) >= 2:
        confs.append("Equal Lows cerca del TP1")

    # MEJORA 5: volumen del spike ya calculado en detect_manipulation
    spike_vol_ratio = detection.get("spike_vol_ratio", 0)
    spike_vol_level = detection.get("spike_vol_level", "")
    if spike_vol_level == "ALTO":
        confs.append(f"Volumen ALTO en spike ({spike_vol_ratio:.1f}x la media)")
    elif spike_vol_level == "CONFIRMADO":
        confs.append(f"Volumen confirmado ({spike_vol_ratio:.1f}x la media)")
    if detection["wick_ratio"] > 0.40:
        confs.append(f"Wick pronunciado ({round(detection['wick_ratio']*100)}%)")

    return confs

# ─── SCORE ───────────────────────────────────────────────────
def quality_score(confs, rr, macro_bias, mt, detection, dynamic_weights=None):
    """
    MEJORA 8: acepta dynamic_weights opcionales.
    Si se proporcionan, usa pesos recalibrados. Si no, usa lógica original.
    """
    w = dynamic_weights if dynamic_weights else DEFAULT_WEIGHTS

    score = 0

    # Sesión (usa peso dinámico de session_opt)
    session = detection.get("session", "Post-NY")
    if session in ("Londres", "Nueva York", "Overlap"):
        score += w["session_opt"]
    else:
        # Sesiones secundarias: proporción del peso
        ratio = SESSION_SCORE.get(session, 5) / 20  # ratio vs máximo original
        score += round(w["session_opt"] * ratio)

    # Bias alineado
    aligned = (macro_bias == "BULLISH" and mt == "ALCISTA") or \
              (macro_bias == "BEARISH" and mt == "BAJISTA")
    if aligned:
        score += w["bias_aligned"]
    elif macro_bias == "NEUTRAL":
        score += round(w["bias_aligned"] * 0.47)  # ~7/15 del original

    # MEJORA 4: retorno
    return_type = detection.get("return_type", "NINGUNO")
    if return_type == "COMPLETO":
        score += w["return_completo"]
    elif return_type == "PARCIAL":
        score += round(w["return_completo"] * 0.5)

    # MEJORA 3: liquidez
    liq_level = detection.get("liq_level", "BAJA")
    if liq_level == "ALTA":
        score += w["liq_alta"]
    elif liq_level == "NORMAL":
        score += round(w["liq_alta"] * 0.67)
    elif detection.get("has_liq"):
        score += round(w["liq_alta"] * 0.33)

    # MEJORA 3: EQH/EQL
    if detection.get("has_eqhl"):
        score += w["eqhl"]

    # MEJORA 5: volumen
    spike_vol_level = detection.get("spike_vol_level", "")
    if spike_vol_level == "ALTO":
        score += w["vol_alto"]
    elif spike_vol_level == "CONFIRMADO":
        score += round(w["vol_alto"] * 0.5)

    # RR
    if rr >= 2.0:
        score += w["rr_alto"]
    elif rr >= 1.5:
        score += round(w["rr_alto"] * 0.53)
    elif rr >= 1.2:
        score += round(w["rr_alto"] * 0.27)

    # Confluencias puras (FVG, OB, EQH en TP, wick pronunciado)
    pure = [c for c in confs if not any(k in c.lower() for k in
            ["sesgo", "choch", "retorno", "liquidez", "volumen"])]
    score += min(len(pure) * 6, 18)

    return min(score, 100)

def score_emoji(score):
    if score >= 80: return "🔥 SETUP A+"
    if score >= 65: return "✅ SETUP A"
    if score >= 50: return "👍 SETUP B"
    return "⚠️ SETUP C"

# ─── ALERTA 1 — MANIPULACION DETECTADA ──────────────────────
def format_detection_alert(symbol, tf_label, detection, levels, score):
    mt         = detection["type"]
    c          = detection["candle"]
    e_type     = "🟢" if mt == "ALCISTA" else "🔴"
    time_str   = c["time"].strftime("%d/%m/%Y %H:%M UTC")
    sess_emoji = detection.get("session_emoji", "⏰")
    session    = detection.get("session", "—")
    swing      = detection["swing_choch"]
    direction  = "por debajo" if mt == "BAJISTA" else "por encima"
    # MEJORA 1
    range_n    = detection.get("range_num_candles", "?")
    compress   = detection.get("compression_level", 0)
    compress_s = f"{compress:.0%}" if compress else "N/A"
    # MEJORA 2
    vol_pct    = detection.get("vol_percentile", 50)
    spike_mult = detection.get("spike_multiplier", BODY_MULTIPLIER)
    # MEJORA 5
    svr        = detection.get("spike_vol_ratio", 0)
    svl        = detection.get("spike_vol_level", "?")

    return f"""
{score_emoji(score)} <b>MANIPULACION DETECTADA</b> — Score: {score}/100

{e_type} <b>{symbol} — {tf_label}</b> | Tipo: <b>{mt}</b>
{sess_emoji} Sesion: {session} | Vela: {time_str}

📦 <b>Rango Acumulacion (dinamico):</b>
  High: ${detection['acc_high']:,.2f} | Low: ${detection['acc_low']:,.2f}
  Velas: {range_n} | Compresion ATR: {compress_s}
  📊 Vol P{vol_pct:.0f} → umbral spike: {spike_mult:.1f}x
  🔊 Volumen spike: {svr:.1f}x ({svl})

📐 <b>Fibonacci (TTrades):</b>
  1 → ${levels['fib_1']:,.2f} (inicio manipulacion)
  0 → ${levels['fib_0']:,.2f} (Low rango acumulacion)
  Rango: ${levels['rango']:,.2f}
  EQ (50%): ${levels['eq']:,.2f}

📏 <b>SD Projections:</b>
  -1 SD:   ${levels['sd_m1']:,.2f}
  -2 SD:   <b>${levels['sd_m2']:,.2f}</b> — TP1
  -2.5 SD: <b>${levels['sd_m25']:,.2f}</b> — TP2
  -4 SD:   ${levels['sd_m4']:,.2f}

⏳ <b>Esperando CHoCH en 15M</b>
  Nivel a romper: <b>${swing:,.2f}</b>
  Condicion: cierre {direction} de ${swing:,.2f}

Te avisare cuando el CHoCH se confirme. 🎯
""".strip()

# ─── ALERTA 2 — CHoCH 15M CONFIRMADO ────────────────────────
def format_choch_alert(symbol, tf_label, detection, levels, confluences,
                       macro_bias, structure_bias, score, choch_price, current_price,
                       choch_quality="FUERTE"):
    mt         = detection["type"]
    c          = detection["candle"]
    e_type     = "🟢" if mt == "ALCISTA" else "🔴"
    e_dir      = "📈" if mt == "ALCISTA" else "📉"
    rr_emoji   = "✅" if levels["rr"] >= 1.5 else "⚠️"
    time_str   = c["time"].strftime("%d/%m/%Y %H:%M UTC")
    macro_str  = {"BULLISH": "🟢 BULLISH", "BEARISH": "🔴 BEARISH", "NEUTRAL": "⚪ NEUTRAL"}.get(macro_bias, "⚪")
    struct_str = {"BULLISH": "🟢 BULLISH", "BEARISH": "🔴 BEARISH", "NEUTRAL": "⚪ NEUTRAL"}.get(structure_bias, "⚪")
    # MEJORA 6: calidad del CHoCH
    choch_str  = "💪 FUERTE" if choch_quality == "FUERTE" else "⚠️ DEBIL"
    # MEJORA 4: retorno detallado en alerta
    ret_type   = detection.get("return_type", "NINGUNO")
    ret_candle = detection.get("return_candle", 0)
    ret_window = detection.get("return_window", 3)
    if ret_type == "COMPLETO":
        ret_str = f"✅ COMPLETO (vela {ret_candle}/{ret_window})"
    elif ret_type == "PARCIAL":
        ret_str = f"🟡 PARCIAL (vela {ret_candle}/{ret_window})"
    else:
        ret_str = "❌ NO"
    # MEJORA 3: liquidez detallada en alerta
    liq_lvl    = detection.get("liq_level", "BAJA")
    liq_tch    = detection.get("liq_touches", 0)
    liq_emoji  = "🟢" if liq_lvl == "ALTA" else ("🟡" if liq_lvl == "NORMAL" else "🔴")
    liq_str    = f"{liq_emoji} {liq_lvl} ({liq_tch} toques)"
    eqhl_str   = ""
    if detection.get("has_eqhl"):
        eqtype = "EQH" if detection["type"] == "BAJISTA" else "EQL"
        eqhl_str = f"\n  {eqtype} barridos:     ✅ ({detection.get('eqhl_count', 0)} pares)"
    sess_emoji = detection.get("session_emoji", "⏰")
    session    = detection.get("session", "—")
    # MEJORA 2
    vol_pct    = detection.get("vol_percentile", 50)
    spike_mult = detection.get("spike_multiplier", BODY_MULTIPLIER)
    # MEJORA 5
    svr        = detection.get("spike_vol_ratio", 0)
    svl        = detection.get("spike_vol_level", "?")

    conf_text = ("\n\n🔗 <b>CONFLUENCIAS:</b>\n" + "".join(f"  • {x}\n" for x in confluences)
                 if confluences else "\n\n🔗 <b>CONFLUENCIAS:</b> Ninguna detectada")

    return f"""
{score_emoji(score)} — Score: <b>{score}/100</b>

✅ <b>CHoCH 15M CONFIRMADO — ENTRADA AHORA</b>

{e_type} <b>Par:</b> {symbol} — {tf_label}
{e_dir} <b>Tipo:</b> {mt}
🕯 <b>Manipulacion:</b> {time_str}
{sess_emoji} <b>Sesion:</b> {session}
💰 <b>Precio actual:</b> ${current_price:,.2f}
📊 <b>CHoCH en:</b> ${choch_price:,.2f}
{choch_str} <b>Calidad CHoCH:</b> cuerpo >50% + cierre tercio favorable

📊 <b>CONTEXTO AMD:</b>
  Rango acumulacion: ${detection['acc_low']:,.2f} — ${detection['acc_high']:,.2f}
  Velas rango: {detection.get('range_num_candles', '?')} | Compresion: {detection.get('compression_level', 0):.0%}
  Vol P{vol_pct:.0f} → umbral spike: {spike_mult:.1f}x
  🔊 Volumen spike: {svr:.1f}x ({svl})
  Liquidez previa:   {liq_str}{eqhl_str}
  Retorno al rango:  {ret_str}
  Sesgo Daily:       {macro_str}
  Estructura 4H:     {struct_str}

📐 <b>FIBONACCI (TTrades):</b>
  1 → <b>${levels['fib_1']:,.2f}</b> (inicio manipulacion)
  0 → <b>${levels['fib_0']:,.2f}</b> (Low rango acumulacion)
  Rango: ${levels['rango']:,.2f}
  EQ (50%): ${levels['eq']:,.2f}

📏 <b>SD PROJECTIONS:</b>
  -1   SD: ${levels['sd_m1']:,.2f}
  -2   SD: <b>${levels['sd_m2']:,.2f}</b> — TP1
  -2.5 SD: <b>${levels['sd_m25']:,.2f}</b> — TP2
  -4   SD: ${levels['sd_m4']:,.2f}

🎯 <b>SETUP:</b>
  📍 Entrada OTE: <b>${levels['entry']:,.2f}</b>
  🛑 Stop Loss:   <b>${levels['sl']:,.2f}</b>
  🎯 TP1 -2 SD:   <b>${levels['tp1']:,.2f}</b>
  🎯 TP2 -2.5:    <b>${levels['tp2']:,.2f}</b>
  📊 R:R:         <b>{levels['rr']:.2f}</b> {rr_emoji}
{conf_text}
⚠️ <i>No es consejo financiero. Gestiona tu riesgo.</i>
""".strip()

# ─── TEST ────────────────────────────────────────────────────
def send_test_message():
    candles = get_candles("BTCUSDT", "4h", 80)
    if not candles:
        send_telegram("❌ Error obteniendo datos.")
        return
    price = candles[-1]["close"]
    rango = price * 0.03
    detection = {
        "type": "BAJISTA", "session": "Londres", "session_emoji": "🇬🇧",
        "candle": {
            "time": datetime.now(timezone.utc),
            "high": price + rango, "low": price * 0.99,
            "open": price + rango * 0.8, "close": price * 0.995,
            "volume": 999999, "body_high": price + rango * 0.8,
            "body_low": price * 0.995, "body_size": rango * 0.8,
            "total_size": rango * 1.01, "is_bullish": False
        },
        "acc_high": price + rango * 0.2, "acc_low": price - rango * 0.3,
        "acc_range": rango * 0.5, "avg_size": rango * 0.5,
        "wick_ratio": 0.45, "returned": True,
        # MEJORA 4
        "return_type": "COMPLETO", "return_candle": 2, "return_window": 4,
        "has_liq": True,
        # MEJORA 3
        "liq_touches": 4, "liq_level": "NORMAL", "has_eqhl": True, "eqhl_count": 2,
        "swing_choch": price - rango * 0.15,
        "fib_1": price + rango,
        "fib_0": price - rango * 0.3,
        # MEJORA 1
        "range_num_candles": 13,
        "compression_level": 0.48,
        # MEJORA 2
        "vol_percentile": 62.3,
        "spike_multiplier": 1.8,
        # MEJORA 5
        "spike_vol_ratio": 2.1,
        "spike_vol_level": "ALTO",
    }
    levels = calculate_levels(detection)
    confluences = [
        "Alineado con sesgo Daily BEARISH",
        "Precio retorno al rango tras el spike",
        "Liquidez previa visible en el nivel barrido",
        "FVG bajista en zona de entrada",
        "Volumen elevado (2.1x la media)",
    ]
    score = quality_score(confluences, levels["rr"], "BEARISH", "BAJISTA", detection)

    send_telegram("🧪 <b>TEST v23 — Alerta 1</b>\n\n" +
                  format_detection_alert("BTCUSDT", "4H", detection, levels, score))
    time.sleep(2)
    send_telegram("🧪 <b>TEST v23 — Alerta 2: CHoCH confirmado</b>\n\n" +
                  format_choch_alert("BTCUSDT", "4H", detection, levels, confluences,
                                     "BEARISH", "BEARISH", score,
                                     detection["swing_choch"] * 0.999, price * 0.997,
                                     "FUERTE"))
    print("✅ Test v23 enviado")

# ─── MAIN ────────────────────────────────────────────────────
def main():
    print("🤖 Bot TTrades v23 — TODAS las mejoras integradas (1-12)")

    if TEST_MODE:
        send_test_message()
        return

    send_telegram(
        "🤖 <b>Bot TTrades AMD v23 — VERSIÓN FINAL</b>\n\n"
        "✅ Mejora 1:  Rango acumulacion dinamico (ATR)\n"
        "✅ Mejora 2:  Spike adaptativo (percentil vol)\n"
        "✅ Mejora 3:  Liquidez real (toques + EQH/EQL)\n"
        "✅ Mejora 4:  Retorno proporcional (30% duracion)\n"
        "✅ Mejora 5:  Filtro volumen spike (1.5x/1.3x Asia)\n"
        "✅ Mejora 6:  CHoCH calidad (fuerte vs debil)\n"
        "✅ Mejora 7:  Pausa eventos macro (FOMC/CPI/NFP)\n"
        "✅ Mejora 8:  Scoring dinamico (feedback loop)\n"
        "✅ Mejora 9:  Invalidacion activa post-entrada\n"
        "✅ Mejora 10: Caducidad por estructura\n"
        "✅ Mejora 11: Alertas TPs en tiempo real\n"
        "✅ Mejora 12: Graficos del setup en cada alerta\n\n"
        "📸 Alertas con grafico: deteccion + CHoCH\n"
        "BTCUSDT en <b>4H y 1H</b>. Revision cada minuto. ⚡"
    )

    pending_setups = {}
    candles_1d    = {}
    candles_4h    = {}
    last_1d_fetch = 0
    last_4h_fetch = 0
    alerted       = {}
    # MEJORA 2: cache de velas históricas para percentil de volatilidad
    candles_hist    = {}   # {sym_interval: candles}
    last_hist_fetch = 0
    # MEJORA 7: cache de eventos macro
    macro_events     = []
    last_macro_fetch = 0
    macro_paused     = False
    # MEJORA 8: pesos dinámicos del scoring
    dynamic_weights    = dict(DEFAULT_WEIGHTS)  # empieza con defaults
    last_recalib       = 0

    while True:
        now = datetime.now(timezone.utc)
        print(f"\n[{now.strftime('%H:%M:%S')}] Revisando...")

        if time.time() - last_1d_fetch > 14400:
            for sym in SYMBOLS:
                candles_1d[sym] = get_candles(sym, "1d", 80)
            last_1d_fetch = time.time()

        if time.time() - last_4h_fetch > 3600:
            for sym in SYMBOLS:
                candles_4h[sym] = get_candles(sym, "4h", 80)
            last_4h_fetch = time.time()

        # MEJORA 2: fetch velas históricas para percentil (cada 4h)
        if time.time() - last_hist_fetch > VOL_HIST_REFRESH:
            for sym in SYMBOLS:
                for tf in TIMEFRAMES:
                    hist_key = f"{sym}_{tf['interval']}"
                    hist = get_candles(sym, tf["interval"], VOL_HIST_CANDLES)
                    if hist:
                        candles_hist[hist_key] = hist
                        print(f"  📊 Hist {hist_key}: {len(hist)} velas cargadas")
            last_hist_fetch = time.time()

        # MEJORA 7: fetch calendario macro (cada 4h)
        if time.time() - last_macro_fetch > MACRO_REFRESH:
            macro_events = fetch_macro_events()
            last_macro_fetch = time.time()

        # MEJORA 8: recalibrar pesos del scoring (cada semana)
        if time.time() - last_recalib > SCORING_RECALIB_H * 3600:
            print("  📊 Iniciando recalibración de pesos...")
            dynamic_weights = recalibrate_weights(dynamic_weights)
            last_recalib = time.time()

        # MEJORA 7: verificar si estamos en ventana de evento macro
        in_macro, macro_title, macro_minutes = is_in_macro_window(macro_events, now)
        if in_macro:
            if not macro_paused:
                macro_paused = True
                if macro_minutes and macro_minutes > 0:
                    time_str = f"en {macro_minutes} min"
                else:
                    time_str = f"hace {abs(macro_minutes)} min" if macro_minutes else "ahora"
                send_telegram(
                    f"⏸ <b>Bot pausado — Evento macro</b>\n\n"
                    f"📅 {macro_title}\n"
                    f"⏰ {time_str}\n"
                    f"Pausa: {MACRO_WINDOW_BEFORE}h antes → {MACRO_WINDOW_AFTER}h despues\n\n"
                    f"<i>La deteccion de manipulacion se reanudara automaticamente.</i>"
                )
                print(f"  ⏸ PAUSA MACRO: {macro_title} ({time_str})")
            else:
                print(f"  ⏸ Macro activo: {macro_title}")
            time.sleep(60)
            continue
        else:
            if macro_paused:
                macro_paused = False
                send_telegram("▶️ <b>Bot reanudado</b> — Ventana macro finalizada.")
                print("  ▶️ Bot reanudado tras evento macro")

        for sym in SYMBOLS:
            c1d = candles_1d.get(sym, [])
            c4h = candles_4h.get(sym, [])

            macro_bias        = get_macro_bias(c1d)
            structure_bias, _ = get_structure_bias(c4h)
            fvgs_4h, obs_4h   = find_pd_arrays(c4h, "4h") if c4h else ([], [])

            for tf in TIMEFRAMES:
                interval = tf["interval"]
                label    = tf["label"]
                key      = f"{sym}_{interval}"

                try:
                    candles = get_candles(sym, interval, tf["limit"])
                    if not candles:
                        continue
                    closed = get_closed(candles, interval)
                    if not closed:
                        continue

                    last_closed_time = str(closed[-1]["time"])
                    if alerted.get(key) == last_closed_time:
                        continue

                    # MEJORA 2: calcular percentil y multiplicador adaptativo
                    hist_key = f"{sym}_{interval}"
                    hist_data = candles_hist.get(hist_key, [])
                    vol_pct, vol_mult = calc_volatility_percentile(hist_data, closed)

                    detection = detect_manipulation(candles, interval, vol_pct, vol_mult)
                    if not detection:
                        print(f"  {label} {sym}: sin manipulacion (umbral {vol_mult:.1f}x, P{vol_pct:.0f})")
                        alerted[key] = last_closed_time
                        continue

                    levels = calculate_levels(detection)
                    if levels["rr"] < MIN_RR:
                        print(f"  {label} {sym}: RR insuficiente")
                        alerted[key] = last_closed_time
                        continue

                    fvgs, obs   = find_pd_arrays(candles, interval)
                    confluences = detect_confluences(
                        candles, levels, detection, interval,
                        fvgs + fvgs_4h, obs + obs_4h, macro_bias, structure_bias
                    )
                    score = quality_score(confluences, levels["rr"],
                                         macro_bias, detection["type"], detection,
                                         dynamic_weights)

                    if score < 40:
                        print(f"  {label} {sym}: score bajo ({score})")
                        alerted[key] = last_closed_time
                        continue

                    setup_key = f"{sym}_{interval}_{last_closed_time}"
                    if setup_key not in pending_setups:
                        msg = format_detection_alert(sym, label, detection, levels, score)
                        send_telegram(msg)
                        print(f"  ✅ Alerta 1 — {label} {sym} | CHoCH swing: ${detection['swing_choch']:,.2f}")

                        # MEJORA 12: generar y enviar gráfico del setup
                        try:
                            chart_path = generate_chart(
                                closed, detection, levels,
                                title=f"{sym} {label} — Alerta 1",
                                filename=f"/tmp/chart_{sym}_{interval}.png"
                            )
                            if chart_path:
                                send_telegram_photo(chart_path,
                                    f"📸 {sym} {label} — Setup {detection['type']} | Score: {score}/100")
                                os.remove(chart_path)
                        except Exception as e:
                            print(f"  ⚠️ Error gráfico alerta 1: {e}")

                        # Registrar en Google Sheets
                        if SHEETS_ENABLED:
                            try:
                                log_detection(detection, levels, score, setup_key)
                            except Exception as e:
                                print(f"  ⚠️ Error Sheets alerta 1: {e}")

                        pending_setups[setup_key] = {
                            "sym": sym, "label": label, "interval": interval,
                            "detection": detection, "levels": levels,
                            "confluences": confluences, "score": score,
                            "macro_bias": macro_bias, "structure_bias": structure_bias,
                            "detected_at": now, "choch_alerted": False,
                            # MEJORA 9: campos para invalidación post-entrada
                            "choch_entry_price": None,
                            "invalidated": False,
                            "weakened_alerted": False,
                            # MEJORA 11: tracking de TPs
                            "tp_hit": {"sd_m1": False, "sd_m2": False, "sd_m25": False, "sd_m4": False},
                            "sl_hit": False,
                        }

                    alerted[key] = last_closed_time

                except Exception as e:
                    print(f"  ❌ Error {label} {sym}: {e}")

            # ── PASO 2: vigilar CHoCH 15M ──
            current_price = get_current_price(sym)
            expired_keys  = []

            for setup_key, setup in pending_setups.items():
                if setup["sym"] != sym:
                    continue

                age = (now - setup["detected_at"]).total_seconds() / 3600
                if age > SETUP_EXPIRY_H:
                    print(f"  ⏰ Setup caducado: {setup_key}")
                    expired_keys.append(setup_key)
                    send_telegram(
                        f"⏰ <b>Setup caducado</b>\n"
                        f"{sym} {setup['label']} — CHoCH no confirmado en {SETUP_EXPIRY_H}h.\n"
                        f"Setup descartado."
                    )
                    continue

                # ── MEJORA 10: INVALIDACIÓN POR ESTRUCTURA ────────────
                # Solo aplica a setups esperando CHoCH (no post-entrada)
                if not setup["choch_alerted"] and current_price:
                    det  = setup["detection"]
                    mt   = det["type"]
                    fib0 = det["fib_0"]
                    fib1 = det["fib_1"]

                    # Check 1: precio alcanzó fib_0 sin CHoCH → el move ya pasó
                    fib0_reached = False
                    if mt == "BAJISTA" and current_price <= fib0:
                        fib0_reached = True
                    elif mt == "ALCISTA" and current_price >= fib0:
                        fib0_reached = True

                    if fib0_reached:
                        expired_keys.append(setup_key)
                        send_telegram(
                            f"📉 <b>Setup descartado — Movimiento sin CHoCH</b>\n"
                            f"{sym} {setup['label']}\n\n"
                            f"El precio alcanzó el nivel 0 (${fib0:,.2f}) "
                            f"sin confirmar CHoCH en 15M.\n"
                            f"El movimiento ocurrió sin nuestra confirmación.\n"
                            f"Setup descartado."
                        )
                        print(f"  📉 Descartado — precio alcanzó fib0 ${fib0:,.2f} sin CHoCH")
                        continue

                    # Check 2: nuevo spike mayor en la misma dirección → ancla obsoleta
                    # Check 3: spike en dirección contraria → mercado cambió
                    try:
                        struct_candles = get_candles(sym, setup["interval"], 10)
                        if struct_candles:
                            struct_closed = get_closed(struct_candles, setup["interval"])
                            manip_time = det["candle"]["time"]
                            new_candles = [c for c in struct_closed if c["time"] > manip_time]

                            for nc in new_candles:
                                # Spike mayor en misma dirección → reemplazar
                                if mt == "BAJISTA" and nc["high"] > fib1 and \
                                   nc["total_size"] >= det["avg_size"] * 1.5:
                                    expired_keys.append(setup_key)
                                    send_telegram(
                                        f"🔄 <b>Setup reemplazado — Nuevo spike mayor</b>\n"
                                        f"{sym} {setup['label']}\n\n"
                                        f"Nuevo high ${nc['high']:,.2f} supera spike original ${fib1:,.2f}.\n"
                                        f"El bot detectará el nuevo spike en el próximo ciclo."
                                    )
                                    print(f"  🔄 Spike mayor detectado — reemplaza setup")
                                    break

                                if mt == "ALCISTA" and nc["low"] < fib1 and \
                                   nc["total_size"] >= det["avg_size"] * 1.5:
                                    expired_keys.append(setup_key)
                                    send_telegram(
                                        f"🔄 <b>Setup reemplazado — Nuevo spike mayor</b>\n"
                                        f"{sym} {setup['label']}\n\n"
                                        f"Nuevo low ${nc['low']:,.2f} supera spike original ${fib1:,.2f}.\n"
                                        f"El bot detectará el nuevo spike en el próximo ciclo."
                                    )
                                    print(f"  🔄 Spike mayor detectado — reemplaza setup")
                                    break

                                # Spike grande en dirección contraria → mercado cambió
                                if mt == "BAJISTA" and nc["is_bullish"] and \
                                   nc["total_size"] >= det["avg_size"] * 1.8 and \
                                   nc["low"] < det["acc_low"]:
                                    expired_keys.append(setup_key)
                                    send_telegram(
                                        f"↩️ <b>Setup descartado — Cambio de dirección</b>\n"
                                        f"{sym} {setup['label']}\n\n"
                                        f"Spike alcista detectado en dirección contraria.\n"
                                        f"El mercado cambió de opinión. Setup invalidado."
                                    )
                                    print(f"  ↩️ Spike contrario — setup descartado")
                                    break

                                if mt == "ALCISTA" and not nc["is_bullish"] and \
                                   nc["total_size"] >= det["avg_size"] * 1.8 and \
                                   nc["high"] > det["acc_high"]:
                                    expired_keys.append(setup_key)
                                    send_telegram(
                                        f"↩️ <b>Setup descartado — Cambio de dirección</b>\n"
                                        f"{sym} {setup['label']}\n\n"
                                        f"Spike bajista detectado en dirección contraria.\n"
                                        f"El mercado cambió de opinión. Setup invalidado."
                                    )
                                    print(f"  ↩️ Spike contrario — setup descartado")
                                    break

                            if setup_key in expired_keys:
                                continue
                    except Exception as e:
                        print(f"  ⚠️ Error check estructura: {e}")

                if setup["choch_alerted"]:
                    # ── MEJORA 9: MONITOREO POST-ENTRADA ──────────────
                    if setup["invalidated"]:
                        continue  # ya invalidado, esperando limpieza

                    if not current_price:
                        continue

                    det  = setup["detection"]
                    mt   = det["type"]
                    fib1 = det["fib_1"]  # nivel de anclaje (spike high/low)
                    entry_price = setup.get("choch_entry_price", current_price)

                    # ── Invalidación completa: precio cierra más allá del spike ──
                    candles_15m = get_candles(sym, "15m", 5)
                    if candles_15m:
                        closed_15m = get_closed(candles_15m, "15m")
                        for c15 in closed_15m:
                            invalidated = False
                            if mt == "BAJISTA" and c15["close"] > fib1:
                                invalidated = True
                            elif mt == "ALCISTA" and c15["close"] < fib1:
                                invalidated = True

                            if invalidated:
                                setup["invalidated"] = True
                                expired_keys.append(setup_key)
                                send_telegram(
                                    f"🚫 <b>SETUP INVALIDADO</b> — {sym} {setup['label']}\n\n"
                                    f"El precio cerró {'por encima' if mt == 'BAJISTA' else 'por debajo'} "
                                    f"del nivel de anclaje (${fib1:,.2f}).\n"
                                    f"  Precio actual: ${current_price:,.2f}\n"
                                    f"  Entrada fue:   ${entry_price:,.2f}\n\n"
                                    f"⚠️ <i>El setup perdió su lógica. Considerar cerrar posición.</i>"
                                )
                                print(f"  🚫 INVALIDADO — {sym} cierre {'>' if mt == 'BAJISTA' else '<'} spike ${fib1:,.2f}")
                                break

                    if setup["invalidated"]:
                        continue

                    # ── Invalidación parcial: retroceso >80% del movimiento ──
                    if not setup["weakened_alerted"] and entry_price:
                        move = abs(current_price - entry_price)
                        total_move = abs(fib1 - entry_price)

                        if total_move > 0:
                            if mt == "BAJISTA" and current_price > entry_price:
                                retrace_pct = (current_price - entry_price) / (fib1 - entry_price) if (fib1 - entry_price) != 0 else 0
                            elif mt == "ALCISTA" and current_price < entry_price:
                                retrace_pct = (entry_price - current_price) / (entry_price - fib1) if (entry_price - fib1) != 0 else 0
                            else:
                                retrace_pct = 0

                            if retrace_pct >= 0.80:
                                setup["weakened_alerted"] = True
                                send_telegram(
                                    f"⚠️ <b>SETUP DEBILITADO</b> — {sym} {setup['label']}\n\n"
                                    f"El precio ha retrocedido {retrace_pct:.0%} del movimiento post-CHoCH.\n"
                                    f"  Entrada:       ${entry_price:,.2f}\n"
                                    f"  Precio actual: ${current_price:,.2f}\n"
                                    f"  Spike (fib 1): ${fib1:,.2f}\n\n"
                                    f"💡 <i>Considerar mover SL a breakeven o reducir posición.</i>"
                                )
                                print(f"  ⚠️ Setup debilitado — retroceso {retrace_pct:.0%}")

                    # ── MEJORA 11: TRACKING DE TPs ────────────────────
                    if not setup["sl_hit"]:
                        lvls = setup["levels"]
                        mt   = setup["detection"]["type"]
                        entry_price = setup.get("choch_entry_price", 0)

                        # Definir TPs con nombres y sugerencias
                        tp_config = [
                            ("sd_m1",  lvls["sd_m1"],  "-1 SD",   "💡 Mover SL a breakeven"),
                            ("sd_m2",  lvls["sd_m2"],  "-2 SD",   "💡 Mover SL a -1 SD"),
                            ("sd_m25", lvls["sd_m25"], "-2.5 SD", "💡 Mover SL a -2 SD"),
                            ("sd_m4",  lvls["sd_m4"],  "-4 SD",   "🏆 Target final alcanzado"),
                        ]

                        # Check SL
                        sl_price = lvls["sl"]
                        sl_hit = False
                        if mt == "BAJISTA" and current_price >= sl_price:
                            sl_hit = True
                        elif mt == "ALCISTA" and current_price <= sl_price:
                            sl_hit = True

                        if sl_hit:
                            setup["sl_hit"] = True
                            tps_reached = [k for k, v in setup["tp_hit"].items() if v]
                            send_telegram(
                                f"🛑 <b>STOP LOSS TOCADO</b> — {sym} {setup['label']}\n\n"
                                f"  Entrada:  ${entry_price:,.2f}\n"
                                f"  SL:       ${sl_price:,.2f}\n"
                                f"  Precio:   ${current_price:,.2f}\n"
                                f"  TPs alcanzados: {len(tps_reached)}/4\n\n"
                                f"{'✅ Parciales tomados: ' + ', '.join(tps_reached) if tps_reached else '❌ Ningún TP alcanzado'}"
                            )
                            expired_keys.append(setup_key)
                            print(f"  🛑 SL tocado — {sym} ({len(tps_reached)} TPs alcanzados)")
                        else:
                            # Check cada TP en orden
                            for tp_key, tp_price, tp_label, tp_advice in tp_config:
                                if setup["tp_hit"][tp_key]:
                                    continue  # ya alertado

                                tp_reached = False
                                if mt == "BAJISTA" and current_price <= tp_price:
                                    tp_reached = True
                                elif mt == "ALCISTA" and current_price >= tp_price:
                                    tp_reached = True

                                if tp_reached:
                                    setup["tp_hit"][tp_key] = True
                                    tps_done = sum(1 for v in setup["tp_hit"].values() if v)
                                    gain_pct = abs(current_price - entry_price) / entry_price * 100 if entry_price else 0
                                    rr_real  = abs(current_price - entry_price) / abs(sl_price - entry_price) if abs(sl_price - entry_price) > 0 else 0

                                    send_telegram(
                                        f"🎯 <b>TP ALCANZADO — {tp_label}</b>\n"
                                        f"{sym} {setup['label']}\n\n"
                                        f"  Nivel:    ${tp_price:,.2f} ({tp_label})\n"
                                        f"  Precio:   ${current_price:,.2f}\n"
                                        f"  Entrada:  ${entry_price:,.2f}\n"
                                        f"  Ganancia: {gain_pct:.2f}% | RR real: {rr_real:.1f}:1\n"
                                        f"  TPs:      {tps_done}/4\n\n"
                                        f"{tp_advice}"
                                    )
                                    print(f"  🎯 {tp_label} alcanzado — {sym} ({tps_done}/4)")

                                    # Si todos los TPs alcanzados → resumen final
                                    if tps_done == 4:
                                        send_telegram(
                                            f"🏆 <b>TRADE COMPLETO</b> — {sym} {setup['label']}\n\n"
                                            f"Los 4 targets SD alcanzados.\n"
                                            f"  Entrada: ${entry_price:,.2f}\n"
                                            f"  Precio:  ${current_price:,.2f}\n"
                                            f"  RR final: {rr_real:.1f}:1 🔥\n\n"
                                            f"✅ <i>Considerar cerrar posición completa.</i>"
                                        )
                                        expired_keys.append(setup_key)

                    continue

                choch_confirmed, choch_price, choch_quality = check_choch_15m(sym, setup["detection"])

                if choch_confirmed and current_price:
                    # MEJORA 9: guardar precio de entrada para monitoreo
                    setup["choch_entry_price"] = choch_price

                    if choch_quality == "FUERTE":
                        # CHoCH fuerte — alerta completa de entrada
                        msg = format_choch_alert(
                            sym, setup["label"],
                            setup["detection"], setup["levels"],
                            setup["confluences"], setup["macro_bias"],
                            setup["structure_bias"], setup["score"],
                            choch_price, current_price, choch_quality
                        )
                        send_telegram(msg)
                        setup["choch_alerted"] = True
                        print(f"  🎯 Alerta 2 CHoCH FUERTE — {sym} en ${choch_price:,.2f}")

                        # MEJORA 12: gráfico actualizado con CHoCH
                        try:
                            choch_candles = get_candles(sym, setup["interval"], 80)
                            if choch_candles:
                                choch_closed = get_closed(choch_candles, setup["interval"])
                                chart_path = generate_chart(
                                    choch_closed, setup["detection"], setup["levels"],
                                    title=f"{sym} {setup['label']} — CHoCH FUERTE",
                                    filename=f"/tmp/chart_choch_{sym}.png"
                                )
                                if chart_path:
                                    send_telegram_photo(chart_path,
                                        f"📸 {sym} — CHoCH confirmado en ${choch_price:,.2f}")
                                    os.remove(chart_path)
                        except Exception as e:
                            print(f"  ⚠️ Error gráfico alerta 2: {e}")
                    else:
                        # CHoCH débil — alerta informativa, no de entrada
                        send_telegram(
                            f"⚠️ <b>CHoCH 15M DEBIL</b> — {sym} {setup['label']}\n\n"
                            f"El precio rompió ${setup['detection']['swing_choch']:,.2f} "
                            f"pero la vela de ruptura no tiene momentum suficiente.\n"
                            f"  Precio ruptura: ${choch_price:,.2f}\n"
                            f"  Precio actual:  ${current_price:,.2f}\n\n"
                            f"⚠️ <i>Setup válido pero entrada a tu criterio.</i>"
                        )
                        setup["choch_alerted"] = True
                        print(f"  ⚠️ Alerta 2 CHoCH DEBIL — {sym} en ${choch_price:,.2f}")

                    # Actualizar Google Sheets
                    if SHEETS_ENABLED:
                        try:
                            update_choch(setup_key, choch_price,
                                        setup["confluences"], current_price,
                                        setup["macro_bias"], setup["detection"]["type"])
                        except Exception as e:
                            print(f"  ⚠️ Error Sheets alerta 2: {e}")
                else:
                    swing = setup["detection"]["swing_choch"]
                    dist  = abs(current_price - swing) / swing * 100 if current_price else 0
                    print(f"  Esperando CHoCH ${swing:,.2f} | precio ${current_price:,.2f} ({dist:.1f}% lejos)")

            for k in expired_keys:
                pending_setups.pop(k, None)

        time.sleep(60)

if __name__ == "__main__":
    main()
