import os
import time
import json                     # PRO 5: persistencia de estado
import requests
import numpy as np  # MEJORA 1+2: cálculos ATR y percentil
from datetime import datetime, timezone, timedelta
from threading import Lock      # PRO 3: rate limiter
try:
    from sheets import log_detection, update_choch, read_closed_trades, auto_close_trade, generate_weekly_report
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

# ─── PRO 17: RANGO MÍNIMO POR PAR ──────────────────────────
MIN_RANGE = {
    "BTCUSDT": 200,     # $200 mín (~0.25% a $80K)
    "ETHUSDT": 15,      # $15 mín (~0.6% a $2,400)
    "SOLUSDT": 0.80,    # $0.80 mín (~0.9% a $85)
    "BNBUSDT": 3.0,     # $3 mín (~0.5% a $630)
    "XRPUSDT": 0.015,   # $0.015 mín (~1% a $1.45)
}
MIN_RANGE_DEFAULT = 0.005  # 0.5% del precio como fallback

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

# ─── PRO 3: CONFIG ANTI-BAN BINANCE ─────────────────────────
API_CACHE_TTL       = 30       # segundos de cache para velas
PRICE_CACHE_TTL     = 5        # segundos de cache para precio
API_MAX_CALLS_MIN   = 50       # máximo de calls por minuto
API_RETRY_WAIT      = 2        # segundos entre retries
API_MAX_RETRIES     = 3        # máximo de reintentos

# ─── PRO 4: CONFIG HEARTBEAT ────────────────────────────────
HEARTBEAT_INTERVAL  = 21600    # cada 6h (segundos)

# ─── PRO 5: CONFIG PERSISTENCIA ─────────────────────────────
STATE_FILE          = "/tmp/bot_state.json"

# ─── PRO 7: CONFIG DIVERGENCIA VOLUMEN ──────────────────────
VOL_DIV_CANDLES     = 3        # velas post-spike para verificar divergencia
VOL_DIV_THRESHOLD   = 0.70     # volumen decreciente = <70% del spike

TIMEFRAMES = [
    {"interval": "1d", "label": "1D", "limit": 80},
    {"interval": "4h", "label": "4H", "limit": 80},
    {"interval": "1h", "label": "1H", "limit": 80},
]
SYMBOLS = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT", "BNBUSDT"]

# ─── PRO 12: MARKET ALIGNMENT ───────────────────────────────
MARKET_ALIGN_MIN = 3   # mínimo de pares alineados para considerar confluencia

# ─── PRO 15: PING-PONG SD STRATEGY ─────────────────────────
PP_EXTREME_LOOKBACK  = 30    # velas para determinar si es nuevo high/low
PP_MIN_ANCHOR_SCORE  = 65    # score mínimo del anchor para activar ping-pong
PP_MAX_TRADES        = 5     # máximo de trades por secuencia
PP_MAX_FAILS         = 2     # fallos consecutivos → mata secuencia
PP_SIGNAL_VSHAPE_PCT = 0.60  # recuperación mínima para V-shape
PP_SIGNAL_WICK_PCT   = 0.60  # wick mínimo para pin bar
PP_SIGNAL_VOL_MIN    = 1.3   # volumen mínimo en la señal
PP_SL_FACTOR         = 0.50  # SL = 50% del gap al siguiente SD
PP_VALID_PAIRS       = ["BTCUSDT", "ETHUSDT", "SOLUSDT"]  # solo pares líquidos

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

# ─── PRO 5: PERSISTENCIA DE ESTADO ──────────────────────────
def save_state(pending_setups, dynamic_weights):
    """Guarda estado en JSON para sobrevivir reinicios."""
    try:
        state = {
            "saved_at": datetime.now(timezone.utc).isoformat(),
            "dynamic_weights": dynamic_weights,
            "setups": {}
        }
        for key, setup in pending_setups.items():
            s = dict(setup)
            # Serializar datetime objects
            s["detected_at"] = s["detected_at"].isoformat()
            det = dict(s["detection"])
            det["candle"] = dict(det["candle"])
            det["candle"]["time"] = det["candle"]["time"].isoformat()
            s["detection"] = det
            state["setups"][key] = s
        with open(STATE_FILE, "w") as f:
            json.dump(state, f, indent=2)
    except Exception as e:
        print(f"  ⚠️ Error guardando estado: {e}")

def load_state():
    """Carga estado desde JSON. Retorna (pending_setups, dynamic_weights) o defaults."""
    try:
        if not os.path.exists(STATE_FILE):
            return {}, dict(DEFAULT_WEIGHTS)
        with open(STATE_FILE, "r") as f:
            state = json.load(f)
        setups = {}
        for key, s in state.get("setups", {}).items():
            s["detected_at"] = datetime.fromisoformat(s["detected_at"])
            s["detection"]["candle"]["time"] = datetime.fromisoformat(s["detection"]["candle"]["time"])
            # Reconstruir tp_hit como dict si es necesario
            if "tp_hit" not in s:
                s["tp_hit"] = {"sd_m1": False, "sd_m2": False, "sd_m25": False, "sd_m4": False}
            if "sl_hit" not in s:
                s["sl_hit"] = False
            if "invalidated" not in s:
                s["invalidated"] = False
            if "weakened_alerted" not in s:
                s["weakened_alerted"] = False
            if "choch_entry_price" not in s:
                s["choch_entry_price"] = None
            setups[key] = s
        weights = state.get("dynamic_weights", dict(DEFAULT_WEIGHTS))
        print(f"  📂 Estado cargado: {len(setups)} setups pendientes")
        return setups, weights
    except Exception as e:
        print(f"  ⚠️ Error cargando estado: {e}")
        return {}, dict(DEFAULT_WEIGHTS)

SESSION_SCORE = {
    "Londres": 20, "Nueva York": 18, "Overlap": 15,
    "Asia": 10, "Pre-Londres": 8, "Mid-sesion": 5, "Post-NY": 5,
}

# ─── PRO 12: MARKET ALIGNMENT ───────────────────────────────
def calc_market_alignment(all_biases, current_sym, manip_type):
    """
    Calcula cuántos pares del mercado están alineados con la dirección del setup.
    Retorna dict:
      aligned_count: int — cuántos pares tienen el mismo sesgo
      total_pairs:   int — total de pares con sesgo definido
      aligned_pct:   float — porcentaje de alineación
      is_aligned:    bool — hay confluencia de mercado (≥ MARKET_ALIGN_MIN)
      details:       str — lista de pares y sus sesgos
    """
    if not all_biases:
        return {"aligned_count": 0, "total_pairs": 0, "aligned_pct": 0,
                "is_aligned": False, "details": "Sin datos"}

    # Mapear dirección del setup al sesgo esperado
    expected_bias = "BULLISH" if manip_type == "ALCISTA" else "BEARISH"

    aligned = []
    against = []
    neutral = []

    for sym, bias in all_biases.items():
        if sym == current_sym:
            continue  # no contar el propio par
        if bias == expected_bias:
            aligned.append(sym.replace("USDT", ""))
        elif bias == "NEUTRAL":
            neutral.append(sym.replace("USDT", ""))
        else:
            against.append(sym.replace("USDT", ""))

    total_defined = len(aligned) + len(against)
    aligned_pct = (len(aligned) / total_defined * 100) if total_defined > 0 else 0

    details_parts = []
    if aligned:  details_parts.append(f"✅ {', '.join(aligned)}")
    if against:  details_parts.append(f"❌ {', '.join(against)}")
    if neutral:  details_parts.append(f"⚪ {', '.join(neutral)}")
    details = " | ".join(details_parts)

    return {
        "aligned_count": len(aligned),
        "total_pairs":   total_defined,
        "aligned_pct":   aligned_pct,
        "is_aligned":    len(aligned) >= MARKET_ALIGN_MIN,
        "details":       details,
    }

# ─── PRO 15: PING-PONG — FUNCIONES ─────────────────────────

def is_at_extreme(candles, interval, manip_type, spike_price):
    """
    Verifica si el spike está en un nuevo high/low de las últimas N velas.
    Esto valida que el anchor es en un extremo del mercado.
    """
    closed = get_closed(candles, interval)
    if len(closed) < PP_EXTREME_LOOKBACK:
        return False

    lookback = closed[-PP_EXTREME_LOOKBACK:]

    if manip_type == "BAJISTA":
        # El spike high debe ser el más alto (o top 3) de las últimas 30 velas
        max_high = max(c["high"] for c in lookback)
        return spike_price >= max_high * 0.998  # tolerancia 0.2%
    else:
        # El spike low debe ser el más bajo (o bottom 3) de las últimas 30 velas
        min_low = min(c["low"] for c in lookback)
        return spike_price <= min_low * 1.002

def detect_sd_reversal(symbol, sd_level, direction, interval="15m"):
    """
    Detecta señales de rebote ligeras en un nivel SD.
    direction: "LONG" (buscamos rebote alcista) o "SHORT" (rebote bajista)
    
    Señales:
    - V-shape: 2-3 velas de rechazo con recuperación ≥60%
    - BOS: cierre por encima/debajo del high/low anterior
    - Pin bar: wick ≥60% del tamaño total tocando el nivel
    - Volumen: spike ≥1.3x en la vela que toca el SD
    
    Retorna dict con señal detectada o None.
    """
    candles = get_candles(symbol, interval, 10)
    if not candles or len(candles) < 4:
        return None

    closed = get_closed(candles, interval)
    if len(closed) < 3:
        return None

    tol = sd_level * 0.003  # 0.3% de tolerancia al nivel
    last3 = closed[-3:]
    last2 = closed[-2:]
    last1 = closed[-1]

    # Verificar que el precio está cerca del nivel SD
    price_near_sd = abs(last1["close"] - sd_level) <= tol or \
                    abs(last1["low"] - sd_level) <= tol or \
                    abs(last1["high"] - sd_level) <= tol

    if not price_near_sd:
        # Verificar con las últimas 2 velas
        price_near_sd = any(
            min(c["low"], c["close"]) <= sd_level + tol and
            max(c["high"], c["close"]) >= sd_level - tol
            for c in last2
        )

    if not price_near_sd:
        return None

    # Volumen promedio para comparar
    avg_vol = sum(c["volume"] for c in closed[:-1]) / max(len(closed) - 1, 1)
    vol_ratio = last1["volume"] / avg_vol if avg_vol > 0 else 0

    signal = None

    if direction == "LONG":
        # ── V-SHAPE: caída seguida de recuperación ≥60% ──
        if len(last3) >= 3:
            down_candle = last3[0]
            recovery = last3[1:]
            if not down_candle["is_bullish"]:  # vela bajista
                drop = down_candle["body_high"] - down_candle["body_low"]
                if drop > 0:
                    max_recovery = max(c["close"] for c in recovery)
                    rec_pct = (max_recovery - down_candle["body_low"]) / drop
                    if rec_pct >= PP_SIGNAL_VSHAPE_PCT:
                        signal = {"type": "V-shape", "recovery": round(rec_pct, 2)}

        # ── PIN BAR: wick inferior largo tocando el SD ──
        if not signal and last1["is_bullish"]:
            wick_lower = last1["body_low"] - last1["low"]
            total = last1["total_size"]
            if total > 0 and wick_lower / total >= PP_SIGNAL_WICK_PCT:
                if last1["low"] <= sd_level + tol:
                    signal = {"type": "Pin bar", "wick_pct": round(wick_lower / total, 2)}

        # ── BOS: cierre por encima del high de la vela anterior ──
        if not signal and len(last2) >= 2:
            prev_high = last2[0]["high"]
            if last1["close"] > prev_high and last1["is_bullish"]:
                signal = {"type": "BOS alcista", "break_level": prev_high}

    else:  # direction == "SHORT"
        # ── V-SHAPE inverso: subida seguida de caída ≥60% ──
        if len(last3) >= 3:
            up_candle = last3[0]
            drop_after = last3[1:]
            if up_candle["is_bullish"]:
                rise = up_candle["body_high"] - up_candle["body_low"]
                if rise > 0:
                    min_drop = min(c["close"] for c in drop_after)
                    drop_pct = (up_candle["body_high"] - min_drop) / rise
                    if drop_pct >= PP_SIGNAL_VSHAPE_PCT:
                        signal = {"type": "V-shape inverso", "recovery": round(drop_pct, 2)}

        # ── PIN BAR: wick superior largo tocando el SD ──
        if not signal and not last1["is_bullish"]:
            wick_upper = last1["high"] - last1["body_high"]
            total = last1["total_size"]
            if total > 0 and wick_upper / total >= PP_SIGNAL_WICK_PCT:
                if last1["high"] >= sd_level - tol:
                    signal = {"type": "Pin bar bajista", "wick_pct": round(wick_upper / total, 2)}

        # ── BOS: cierre por debajo del low de la vela anterior ──
        if not signal and len(last2) >= 2:
            prev_low = last2[0]["low"]
            if last1["close"] < prev_low and not last1["is_bullish"]:
                signal = {"type": "BOS bajista", "break_level": prev_low}

    if signal:
        signal["vol_ratio"] = round(vol_ratio, 1)
        signal["price"] = last1["close"]
        signal["time"] = last1["time"]

    return signal

def create_pingpong_sequence(sym, detection, levels, score, interval):
    """Crea una secuencia de ping-pong a partir de un anchor válido."""
    mt = detection["type"]

    # Mapa de niveles SD para el ping-pong
    sd_map = [
        {"level": levels["fib_0"],  "name": "0 (rango)",    "price": levels["fib_0"]},
        {"level": levels["sd_m1"],  "name": "-1 SD",        "price": levels["sd_m1"]},
        {"level": levels["sd_m2"],  "name": "-2 SD (TP1)",  "price": levels["sd_m2"]},
        {"level": levels["sd_m25"], "name": "-2.5 SD",      "price": levels["sd_m25"]},
        {"level": levels["sd_m4"],  "name": "-4 SD",        "price": levels["sd_m4"]},
    ]

    return {
        "sym": sym,
        "interval": interval,
        "anchor_type": mt,
        "anchor_score": score,
        "fib_1": levels["fib_1"],
        "fib_0": levels["fib_0"],
        "rango": levels["rango"],
        "sd_map": sd_map,
        "levels": levels,
        "trades": [],           # historial: [{num, type, entry, sl, tp, result}]
        "trade_count": 0,
        "consecutive_fails": 0,
        "active": True,
        "current_trade": None,  # trade activo actualmente
        "created_at": datetime.now(timezone.utc),
    }

def get_pp_next_trade(sequence, current_price):
    """
    Determina el próximo trade del ping-pong basado en el precio actual.
    Retorna dict con nivel SD objetivo, dirección, SL y TP, o None.
    """
    if not sequence["active"]:
        return None
    if sequence["trade_count"] >= PP_MAX_TRADES:
        return None
    if sequence["current_trade"] is not None:
        return None  # ya hay un trade activo

    mt = sequence["anchor_type"]
    levels = sequence["levels"]
    rango = sequence["rango"]

    # Definir los niveles SD ordenados según dirección del anchor
    if mt == "BAJISTA":
        # Anchor fue short → SD levels van hacia abajo
        bounce_levels = [
            (levels["sd_m1"],  "LONG",  "-1 SD",   levels["fib_0"], levels["sd_m2"]),
            (levels["sd_m2"],  "LONG",  "-2 SD",   levels["sd_m1"], levels["sd_m25"]),
            (levels["sd_m25"], "LONG",  "-2.5 SD", levels["sd_m2"], levels["sd_m4"]),
        ]
        rejection_levels = [
            (levels["fib_0"],  "SHORT", "0 (rango)", levels["fib_1"], levels["sd_m1"]),
            (levels["sd_m1"],  "SHORT", "-1 SD",     levels["fib_0"], levels["sd_m2"]),
            (levels["sd_m2"],  "SHORT", "-2 SD",     levels["sd_m1"], levels["sd_m25"]),
        ]
    else:
        # Anchor fue long → SD levels van hacia arriba
        bounce_levels = [
            (levels["sd_m1"],  "SHORT", "-1 SD",   levels["fib_0"], levels["sd_m2"]),
            (levels["sd_m2"],  "SHORT", "-2 SD",   levels["sd_m1"], levels["sd_m25"]),
            (levels["sd_m25"], "SHORT", "-2.5 SD", levels["sd_m2"], levels["sd_m4"]),
        ]
        rejection_levels = [
            (levels["fib_0"],  "LONG",  "0 (rango)", levels["fib_1"], levels["sd_m1"]),
            (levels["sd_m1"],  "LONG",  "-1 SD",     levels["fib_0"], levels["sd_m2"]),
            (levels["sd_m2"],  "LONG",  "-2 SD",     levels["sd_m1"], levels["sd_m25"]),
        ]

    tol = rango * 0.15  # 15% del rango como tolerancia al nivel

    # Buscar si el precio está cerca de algún nivel de rebote (LONG zones)
    for sd_price, direction, sd_name, tp_level, sl_ref in bounce_levels:
        if abs(current_price - sd_price) <= tol:
            sl_gap = abs(sd_price - sl_ref)
            sl = sd_price - sl_gap * PP_SL_FACTOR if direction == "LONG" else sd_price + sl_gap * PP_SL_FACTOR
            rr = abs(tp_level - sd_price) / abs(sl - sd_price) if abs(sl - sd_price) > 0 else 0
            if rr >= 1.2:
                return {
                    "sd_name": sd_name, "direction": direction,
                    "entry_level": sd_price, "sl": sl, "tp": tp_level,
                    "rr": round(rr, 1),
                }

    # Buscar si el precio está cerca de algún nivel de rechazo (SHORT zones)
    for sd_price, direction, sd_name, sl_ref, tp_level in rejection_levels:
        if abs(current_price - sd_price) <= tol:
            sl_gap = abs(sl_ref - sd_price)
            sl = sd_price + sl_gap * PP_SL_FACTOR if direction == "SHORT" else sd_price - sl_gap * PP_SL_FACTOR
            rr = abs(tp_level - sd_price) / abs(sl - sd_price) if abs(sl - sd_price) > 0 else 0
            if rr >= 1.0:
                return {
                    "sd_name": sd_name, "direction": direction,
                    "entry_level": sd_price, "sl": sl, "tp": tp_level,
                    "rr": round(rr, 1),
                }

    return None

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

# ─── PRO 3: CACHE Y RATE LIMITER PARA BINANCE ───────────────
_api_cache = {}       # {cache_key: (timestamp, data)}
_api_call_times = []  # timestamps de calls recientes
_api_lock = Lock()
_error_count = 0      # PRO 4: contador de errores

def _check_rate_limit():
    """Verifica que no excedemos el límite de calls por minuto."""
    global _api_call_times
    now = time.time()
    _api_call_times = [t for t in _api_call_times if now - t < 60]
    if len(_api_call_times) >= API_MAX_CALLS_MIN:
        wait = 60 - (now - _api_call_times[0])
        if wait > 0:
            print(f"  ⏳ Rate limit — esperando {wait:.0f}s")
            time.sleep(wait)
    _api_call_times.append(now)

# ─── VELAS BINANCE (con cache + rate limit) ──────────────────
def get_candles(symbol, interval="4h", limit=80):
    global _error_count
    cache_key = f"candles_{symbol}_{interval}_{limit}"

    with _api_lock:
        # Check cache
        if cache_key in _api_cache:
            ts, data = _api_cache[cache_key]
            if time.time() - ts < API_CACHE_TTL:
                return data

        _check_rate_limit()

    url    = "https://api.binance.com/api/v3/klines"
    params = {"symbol": symbol, "interval": interval, "limit": limit}

    for attempt in range(API_MAX_RETRIES):
        try:
            r    = requests.get(url, params=params, timeout=10)
            if r.status_code == 429:  # rate limited by Binance
                wait = API_RETRY_WAIT * (attempt + 1)
                print(f"  ⚠️ Binance 429 — retry en {wait}s")
                time.sleep(wait)
                continue
            data = r.json()
            if not isinstance(data, list):
                return []
            result = [{
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
            with _api_lock:
                _api_cache[cache_key] = (time.time(), result)
            return result
        except Exception as e:
            _error_count += 1
            if attempt < API_MAX_RETRIES - 1:
                time.sleep(API_RETRY_WAIT)
            else:
                print(f"❌ Velas {symbol} {interval}: {e}")
                return []
    return []

def get_current_price(symbol):
    global _error_count
    cache_key = f"price_{symbol}"

    with _api_lock:
        if cache_key in _api_cache:
            ts, data = _api_cache[cache_key]
            if time.time() - ts < PRICE_CACHE_TTL:
                return data

        _check_rate_limit()

    try:
        r = requests.get("https://api.binance.com/api/v3/ticker/price",
                         params={"symbol": symbol}, timeout=5)
        price = float(r.json()["price"])
        with _api_lock:
            _api_cache[cache_key] = (time.time(), price)
        return price
    except Exception as e:
        _error_count += 1
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

# ─── PRO 16: ANÁLISIS AVANZADO DE LIQUIDEZ ──────────────────

def find_swing_points(candles, lookback=3):
    """
    Detecta swing highs y swing lows en un array de velas.
    Un swing high = high mayor que los 'lookback' vecinos a cada lado.
    Retorna {"highs": [(index, price)], "lows": [(index, price)]}
    """
    highs, lows = [], []
    if len(candles) < lookback * 2 + 1:
        return {"highs": highs, "lows": lows}

    for i in range(lookback, len(candles) - lookback):
        # Swing high: high[i] > todos los highs vecinos
        is_high = all(candles[i]["high"] >= candles[i + j]["high"]
                      for j in range(-lookback, lookback + 1) if j != 0)
        if is_high:
            highs.append((i, candles[i]["high"]))

        # Swing low: low[i] < todos los lows vecinos
        is_low = all(candles[i]["low"] <= candles[i + j]["low"]
                     for j in range(-lookback, lookback + 1) if j != 0)
        if is_low:
            lows.append((i, candles[i]["low"]))

    return {"highs": highs, "lows": lows}

def cluster_levels(points, tolerance_pct=0.003):
    """
    Agrupa niveles de precio cercanos en clusters (pools de liquidez).
    Retorna lista de dicts: [{price, count, levels}]
    """
    if not points:
        return []

    sorted_pts = sorted(points, key=lambda x: x[1])
    clusters = []
    current = [sorted_pts[0]]

    for i in range(1, len(sorted_pts)):
        price = sorted_pts[i][1]
        cluster_avg = sum(p[1] for p in current) / len(current)
        if abs(price - cluster_avg) / cluster_avg <= tolerance_pct:
            current.append(sorted_pts[i])
        else:
            avg = sum(p[1] for p in current) / len(current)
            clusters.append({"price": avg, "count": len(current), "levels": current})
            current = [sorted_pts[i]]

    if current:
        avg = sum(p[1] for p in current) / len(current)
        clusters.append({"price": avg, "count": len(current), "levels": current})

    return sorted(clusters, key=lambda x: x["count"], reverse=True)

def get_round_number(price, symbol):
    """Retorna el número redondo más cercano según el símbolo."""
    if "BTC" in symbol:
        step = 5000    # $80K, $85K, $90K
    elif "ETH" in symbol:
        step = 100     # $2,400, $2,500
    elif "SOL" in symbol:
        step = 5       # $80, $85, $90
    elif "BNB" in symbol:
        step = 50      # $600, $650
    elif "XRP" in symbol:
        step = 0.10    # $2.00, $2.10
    else:
        step = price * 0.02  # 2% genérico

    nearest = round(price / step) * step
    return nearest, step

def calc_liquidity_score(candles, interval, detection, levels, symbol):
    """
    Calcula un Liquidity Score completo (0-100) con 4 capas:
    1. Liquidez barrida (cuántos swings se barrieron)
    2. Liquidez en dirección del TP (imanes)
    3. Números redondos cerca de targets
    4. Liquidez restante (riesgo)

    Retorna dict con score, detalles y texto para alerta.
    """
    closed = get_closed(candles, interval)
    if len(closed) < 15:
        return {"score": 50, "details": [], "swept": 0, "total_swings": 0,
                "pools_at_tp": [], "round_numbers": [], "remaining": [],
                "alert_text": "💧 Liquidez: datos insuficientes"}

    mt = detection["type"]
    fib_1 = detection["fib_1"]
    rango = levels["rango"]

    # ── Capa 1: Liquidez barrida ──
    swings = find_swing_points(closed[-50:], lookback=2)
    tol = rango * 0.10  # 10% del rango como tolerancia

    if mt == "BAJISTA":
        target_swings = swings["highs"]
        swept = sum(1 for _, p in target_swings if p <= fib_1 + tol)
    else:
        target_swings = swings["lows"]
        swept = sum(1 for _, p in target_swings if p >= fib_1 - tol)

    total_swings = len(target_swings)
    swept_pct = (swept / total_swings * 100) if total_swings > 0 else 0

    # ── Capa 2: Pools de liquidez en dirección del TP ──
    if mt == "BAJISTA":
        tp_swings = swings["lows"]
    else:
        tp_swings = swings["highs"]

    tp_clusters = cluster_levels(tp_swings)
    pools_at_tp = []
    sd_levels_list = [levels["sd_m1"], levels["sd_m2"], levels["sd_m25"], levels["sd_m4"]]
    for cluster in tp_clusters:
        for sd in sd_levels_list:
            if abs(cluster["price"] - sd) <= rango * 0.30:
                pools_at_tp.append({
                    "price": cluster["price"],
                    "count": cluster["count"],
                    "near_sd": sd,
                })
                break

    # ── Capa 3: Números redondos ──
    round_matches = []
    for sd in sd_levels_list:
        rn, step = get_round_number(sd, symbol)
        if abs(rn - sd) <= step * 0.15:  # dentro del 15% del step
            round_matches.append({"round": rn, "near_sd": sd, "dist_pct": abs(rn - sd) / sd * 100})

    # ── Capa 4: Liquidez restante (riesgo) ──
    remaining = []
    if mt == "BAJISTA":
        not_swept = [(i, p) for i, p in target_swings if p > fib_1 + tol]
        for _, p in not_swept:
            remaining.append(p)
    else:
        not_swept = [(i, p) for i, p in target_swings if p < fib_1 - tol]
        for _, p in not_swept:
            remaining.append(p)

    # ── Calcular score ──
    score = 50  # base

    # Capa 1: barrida (0-25 puntos)
    if swept_pct >= 80:
        score += 25
    elif swept_pct >= 60:
        score += 18
    elif swept_pct >= 40:
        score += 10
    elif swept_pct > 0:
        score += 5

    # Capa 2: pools en TP (0-25 puntos)
    if len(pools_at_tp) >= 3:
        score += 25
    elif len(pools_at_tp) >= 2:
        score += 18
    elif len(pools_at_tp) >= 1:
        score += 12

    # Capa 3: round numbers (0-15 puntos)
    if round_matches:
        score += min(len(round_matches) * 8, 15)

    # Capa 4: penalización por liquidez restante (-15 max)
    if len(remaining) >= 3:
        score -= 15
    elif len(remaining) >= 2:
        score -= 10
    elif len(remaining) >= 1:
        score -= 5

    score = max(0, min(100, score))

    # ── Formatear texto para alerta ──
    details = []
    details.append(f"Barrida: {swept}/{total_swings} swings ({swept_pct:.0f}%) {'✅' if swept_pct >= 60 else '⚠️'}")
    if pools_at_tp:
        pool_str = " + ".join(f"{p['count']} swings ${p['price']:,.2f}" for p in pools_at_tp[:3])
        details.append(f"Pool en TPs: {pool_str} 🎯")
    if round_matches:
        rn_str = ", ".join(f"${r['round']:,.0f}" for r in round_matches[:2])
        details.append(f"Round numbers: {rn_str} 💰")
    if remaining:
        rem_dir = "arriba" if mt == "BAJISTA" else "abajo"
        details.append(f"Liquidez restante {rem_dir}: {len(remaining)} swings ⚠️")

    alert_lines = "\n  ".join(details)
    alert_text = f"💧 <b>LIQUIDEZ</b> (Score {score}/100):\n  {alert_lines}"

    return {
        "score": score,
        "details": details,
        "swept": swept,
        "total_swings": total_swings,
        "swept_pct": swept_pct,
        "pools_at_tp": pools_at_tp,
        "round_numbers": round_matches,
        "remaining": remaining,
        "alert_text": alert_text,
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
    sweep_type = "DIRECTO"  # PRO 11: DIRECTO = vela cierra a favor, RETROACTIVO = confirmado por velas post

    # ── Detección directa (original): vela cierra en la dirección correcta ──
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

    # ── PRO 11: Detección retroactiva de sweep ─────────────────
    # Si la vela perfora el nivel PERO cierra en la dirección "incorrecta",
    # verificamos si las 1-3 velas siguientes confirman la inversión.
    # Esto captura el patrón ICT clásico: vela roja de sweep + recuperación verde.
    if not manip_type:
        post_manip = closed[spike_index + 1:spike_index + 4]  # máx 3 velas post

        # Setup ALCISTA retroactivo: vela perfora LOW, cierra ROJA, pero se recupera
        if manip["low"] < acc_low and not manip["is_bullish"] and len(post_manip) >= 1:
            lower_wick = manip["body_low"] - manip["low"]
            wick_ratio_check = lower_wick / manip_size if manip_size > 0 else 0

            if wick_ratio_check >= 0.10:  # wick inferior significativo
                manip_body = manip["body_high"] - manip["body_low"]
                no_new_low = all(c["low"] >= manip["low"] for c in post_manip)

                if no_new_low and manip_body > 0:
                    # Verificar recuperación: ¿las velas post recuperan ≥50% del cuerpo?
                    max_close_post = max(c["close"] for c in post_manip)
                    recovery = (max_close_post - manip["body_low"]) / manip_body

                    if recovery >= 0.50:
                        manip_type = "ALCISTA"
                        sweep_type = "RETROACTIVO"
                        wick_ratio = wick_ratio_check
                        print(f"  🔍 Sweep retroactivo ALCISTA detectado — recovery {recovery:.0%}")

        # Setup BAJISTA retroactivo: vela perfora HIGH, cierra VERDE, pero cae
        elif manip["high"] > acc_high and manip["is_bullish"] and len(post_manip) >= 1:
            upper_wick = manip["high"] - manip["body_high"]
            wick_ratio_check = upper_wick / manip_size if manip_size > 0 else 0

            if wick_ratio_check >= 0.10:  # wick superior significativo
                manip_body = manip["body_high"] - manip["body_low"]
                no_new_high = all(c["high"] <= manip["high"] for c in post_manip)

                if no_new_high and manip_body > 0:
                    # Verificar caída: ¿las velas post caen ≥50% del cuerpo?
                    min_close_post = min(c["close"] for c in post_manip)
                    recovery = (manip["body_high"] - min_close_post) / manip_body

                    if recovery >= 0.50:
                        manip_type = "BAJISTA"
                        sweep_type = "RETROACTIVO"
                        wick_ratio = wick_ratio_check
                        print(f"  🔍 Sweep retroactivo BAJISTA detectado — recovery {recovery:.0%}")

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

    # PRO 7: divergencia de volumen post-spike
    # Si el volumen de las velas siguientes decrece → más creíble
    vol_divergence = False
    post_spike = closed[spike_index + 1:spike_index + 1 + VOL_DIV_CANDLES] if spike_index + 1 < len(closed) else []
    if post_spike and len(post_spike) >= 2:
        post_vols = [c["volume"] for c in post_spike]
        avg_post_vol = np.mean(post_vols)
        if avg_post_vol < manip["volume"] * VOL_DIV_THRESHOLD:
            vol_divergence = True  # volumen decreciente = manipulación más creíble

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

    # Swing previo para CISD 15M
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
        # PRO 7
        "vol_divergence":   vol_divergence,
        # PRO 11
        "sweep_type":       sweep_type,
    }

# ─── PRO 13: PRE-ALERTA EN VELA ACTUAL (NO CERRADA) ─────────
def detect_live_spike(candles, interval, acc_high_cache=None, acc_low_cache=None):
    """
    Analiza la vela actual (no cerrada) para detectar manipulación en curso.
    Es una versión simplificada y rápida de detect_manipulation.
    Solo alerta si la vela actual YA muestra spike claro con volumen.
    Retorna dict con niveles OTE si detecta, None si no.
    """
    if not candles or len(candles) < LOOKBACK + 2:
        return None

    # La última vela puede no estar cerrada — esa es la que analizamos
    live = candles[-1]
    # Las cerradas previas para calcular rango
    prev = candles[-(LOOKBACK + 2):-1]

    # Rango de acumulación rápido (sin ATR dinámico para velocidad)
    acc_high = max(c["body_high"] for c in prev[-LOOKBACK:])
    acc_low  = min(c["body_low"]  for c in prev[-LOOKBACK:])
    acc_range = acc_high - acc_low
    if acc_range <= 0:
        return None

    # Tamaño de la vela actual
    live_size = live["high"] - live["low"]
    avg_size = sum(c["total_size"] for c in prev[-LOOKBACK:]) / LOOKBACK
    if live_size < avg_size * 2.0:  # PRO 13 ajustado: umbral 2.0x para reducir ruido
        return None

    # Volumen alto
    avg_vol = sum(c["volume"] for c in prev[-LOOKBACK:]) / LOOKBACK
    if avg_vol <= 0 or live["volume"] / avg_vol < 2.0:
        return None

    # ¿Perfora el rango?
    manip_type = None
    if live["high"] > acc_high:
        manip_type = "BAJISTA"
    elif live["low"] < acc_low:
        manip_type = "ALCISTA"

    if not manip_type:
        return None

    # Calcular OTE rápido
    if manip_type == "BAJISTA":
        fib_1 = live["high"]
        fib_0 = acc_low
    else:
        fib_1 = live["low"]
        fib_0 = acc_high

    rango = abs(fib_1 - fib_0)
    if rango <= 0:
        return None

    if manip_type == "BAJISTA":
        entry_cons = fib_1 - rango * 0.62
        entry      = fib_1 - rango * 0.705
        entry_agg  = fib_1 - rango * 0.79
        sl         = fib_1 * 1.005
        tp1        = fib_0 - rango * 2.0
    else:
        entry_cons = fib_1 + rango * 0.62
        entry      = fib_1 + rango * 0.705
        entry_agg  = fib_1 + rango * 0.79
        sl         = fib_1 * 0.995
        tp1        = fib_0 + rango * 2.0

    rr = abs(entry - tp1) / abs(sl - entry) if abs(sl - entry) > 0 else 0
    vol_ratio = live["volume"] / avg_vol

    return {
        "type": manip_type,
        "fib_1": fib_1, "fib_0": fib_0, "rango": rango,
        "entry_cons": entry_cons, "entry": entry, "entry_agg": entry_agg,
        "sl": sl, "tp1": tp1, "rr": round(rr, 2),
        "vol_ratio": round(vol_ratio, 1),
        "acc_high": acc_high, "acc_low": acc_low,
        "live_price": live["close"],
        "live_time": live["time"],
    }

# ─── MEJORA 6: CISD en 15M — TTrades Model ───────────
def check_cisd_15m(symbol, detection):
    """
    Verifica CISD (Change in State of Delivery) en 15M.
    
    Según TTrades/PDF:
    - CISD bearish: serie de velas alcistas → cierre por debajo del OPEN
      de la PRIMERA vela alcista de esa serie.
    - CISD bullish: serie de velas bajistas → cierre por encima del OPEN
      de la PRIMERA vela bajista de esa serie.
    
    CISD es más rápido que CHoCH porque no busca rotura de swing point,
    solo cambio en la "delivery" (dirección de cierre de velas).
    
    Retorna: (confirmed, price, quality)
      confirmed: bool
      price: float o None
      quality: "FUERTE" / "DEBIL" / None
    """
    candles_15m = get_candles(symbol, "15m", 30)
    if not candles_15m:
        return False, None, None
    closed_15m = get_closed(candles_15m, "15m")
    if not closed_15m:
        return False, None, None

    manip_type = detection["type"]
    manip_time = detection["candle"]["time"]
    post = [c for c in closed_15m if c["time"] > manip_time]

    if len(post) < 2:
        return False, None, None

    if manip_type == "BAJISTA":
        # Buscamos CISD bearish:
        # 1. Encontrar serie de velas alcistas (delivery alcista)
        # 2. Marcar OPEN de la primera alcista
        # 3. Buscar cierre por debajo de ese OPEN

        # Buscar la primera serie de velas alcistas post-manipulación
        first_bull_open = None
        series_started = False

        for i, c in enumerate(post):
            if c["is_bullish"]:
                if not series_started:
                    first_bull_open = c["open"]
                    series_started = True
            elif series_started and first_bull_open is not None:
                # Vela no alcista después de la serie → verificar CISD
                if c["close"] < first_bull_open:
                    # CISD confirmado — evaluar calidad
                    candle_range = c["high"] - c["low"]
                    if candle_range > 0:
                        body = abs(c["close"] - c["open"])
                        body_pct = body / candle_range
                        close_in_third = c["close"] <= c["high"] - candle_range * 0.66
                        if body_pct >= 0.50 and close_in_third:
                            return True, c["close"], "FUERTE"
                    return True, c["close"], "DEBIL"

        # Si no hubo serie → intentar con velas individuales
        # A veces solo hay 1 vela alcista seguida de 1 bajista que cierra debajo
        if first_bull_open is not None:
            for c in post:
                if not c["is_bullish"] and c["close"] < first_bull_open:
                    return True, c["close"], "DEBIL"

    else:
        # ALCISTA: buscamos CISD bullish
        # 1. Serie de velas bajistas (delivery bajista)
        # 2. OPEN de la primera bajista
        # 3. Cierre por encima de ese OPEN

        first_bear_open = None
        series_started = False

        for i, c in enumerate(post):
            if not c["is_bullish"]:
                if not series_started:
                    first_bear_open = c["open"]
                    series_started = True
            elif series_started and first_bear_open is not None:
                if c["close"] > first_bear_open:
                    candle_range = c["high"] - c["low"]
                    if candle_range > 0:
                        body = abs(c["close"] - c["open"])
                        body_pct = body / candle_range
                        close_in_third = c["close"] >= c["low"] + candle_range * 0.66
                        if body_pct >= 0.50 and close_in_third:
                            return True, c["close"], "FUERTE"
                    return True, c["close"], "DEBIL"

        if first_bear_open is not None:
            for c in post:
                if c["is_bullish"] and c["close"] > first_bear_open:
                    return True, c["close"], "DEBIL"

    return False, None, None

# ─── CALCULAR NIVELES FIBONACCI (anclaje TTrades) ────────────
def calculate_levels(detection):
    mt    = detection["type"]
    fib_1 = detection["fib_1"]
    fib_0 = detection["fib_0"]
    rango = abs(fib_1 - fib_0)

    if mt == "BAJISTA":
        eq     = fib_1 - rango * 0.50
        # PRO 10: ZONA OTE — 3 niveles de entrada
        entry_cons = fib_1 - rango * 0.62    # 62% — conservadora (mejor RR)
        entry      = fib_1 - rango * 0.705   # 70.5% — óptima (default)
        entry_agg  = fib_1 - rango * 0.79    # 79% — agresiva (más triggers)
        sl     = fib_1 * (1 + 0.005)
        sd_m1  = fib_0 - rango * 1.0
        sd_m2  = fib_0 - rango * 2.0
        sd_m25 = fib_0 - rango * 2.5
        sd_m4  = fib_0 - rango * 4.0
        tp1, tp2 = sd_m2, sd_m25
        rr      = (entry - tp1) / (sl - entry) if (sl - entry) > 0 else 0
        rr_cons = (entry_cons - tp1) / (sl - entry_cons) if (sl - entry_cons) > 0 else 0
        rr_agg  = (entry_agg - tp1) / (sl - entry_agg) if (sl - entry_agg) > 0 else 0
    else:
        eq     = fib_1 + rango * 0.50
        # PRO 10: ZONA OTE — 3 niveles
        entry_cons = fib_1 + rango * 0.62
        entry      = fib_1 + rango * 0.705
        entry_agg  = fib_1 + rango * 0.79
        sl     = fib_1 * (1 - 0.005)
        sd_m1  = fib_0 + rango * 1.0
        sd_m2  = fib_0 + rango * 2.0
        sd_m25 = fib_0 + rango * 2.5
        sd_m4  = fib_0 + rango * 4.0
        tp1, tp2 = sd_m2, sd_m25
        rr      = (tp1 - entry) / (entry - sl) if (entry - sl) > 0 else 0
        rr_cons = (tp1 - entry_cons) / (entry_cons - sl) if (entry_cons - sl) > 0 else 0
        rr_agg  = (tp1 - entry_agg) / (entry_agg - sl) if (entry_agg - sl) > 0 else 0

    # PRO 10: zona de entrada (límites de la zona OTE)
    if mt == "BAJISTA":
        zone_high = entry_cons   # 62% (más arriba)
        zone_low  = entry_agg    # 79% (más abajo)
    else:
        zone_high = entry_agg    # 79% (más arriba)
        zone_low  = entry_cons   # 62% (más abajo)

    return {
        "fib_1": fib_1, "fib_0": fib_0, "rango": rango,
        "eq": eq, "entry": entry, "sl": sl,
        "sd_m1": sd_m1, "sd_m2": sd_m2, "sd_m25": sd_m25, "sd_m4": sd_m4,
        "tp1": tp1, "tp2": tp2, "rr": rr,
        # PRO 10: zona OTE
        "entry_cons":  entry_cons,
        "entry_agg":   entry_agg,
        "rr_cons":     rr_cons,
        "rr_agg":      rr_agg,
        "zone_high":   zone_high,
        "zone_low":    zone_low,
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
        confs.append("CISD 4H confirmado BULLISH")
    elif structure_bias == "BEARISH" and mt == "BAJISTA":
        confs.append("CISD 4H confirmado BEARISH")

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
    # PRO 7: divergencia de volumen post-spike
    if detection.get("vol_divergence"):
        confs.append("Divergencia volumen post-spike (vol decreciente)")
    # PRO 11: sweep retroactivo
    if detection.get("sweep_type") == "RETROACTIVO":
        confs.append("Sweep retroactivo (vela trampa + recuperación post)")

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
def format_detection_alert(symbol, tf_label, detection, levels, score, liq_analysis=None):
    mt         = detection["type"]
    c          = detection["candle"]
    e_type     = "🟢" if mt == "ALCISTA" else "🔴"
    time_str   = c["time"].strftime("%d/%m/%Y %H:%M UTC")
    sess_emoji = detection.get("session_emoji", "⏰")
    session    = detection.get("session", "—")
    swing      = detection["swing_choch"]
    direction  = "por debajo" if mt == "BAJISTA" else "por encima"
    range_n    = detection.get("range_num_candles", "?")
    compress   = detection.get("compression_level", 0)
    compress_s = f"{compress:.0%}" if compress else "N/A"
    vol_pct    = detection.get("vol_percentile", 50)
    spike_mult = detection.get("spike_multiplier", BODY_MULTIPLIER)
    svr        = detection.get("spike_vol_ratio", 0)
    svl        = detection.get("spike_vol_level", "?")
    sweep      = detection.get("sweep_type", "DIRECTO")
    sweep_str  = "🔄 Sweep retroactivo (confirmado por velas post)" if sweep == "RETROACTIVO" else ""
    # PRO 16: liquidez
    liq_text   = liq_analysis["alert_text"] if liq_analysis else ""

    return f"""
{score_emoji(score)} <b>MANIPULACION DETECTADA</b> — Score: {score}/100

{e_type} <b>{symbol} — {tf_label}</b> | Tipo: <b>{mt}</b>
{sess_emoji} Sesion: {session} | Vela: {time_str}
{sweep_str}

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

{liq_text}

🎯 <b>ZONA DE ENTRADA OTE:</b>
  Conservadora (62%): ${levels['entry_cons']:,.2f}  | RR {levels['rr_cons']:.1f}:1
  Optima (70.5%):     <b>${levels['entry']:,.2f}</b>  | RR {levels['rr']:.1f}:1
  Agresiva (79%):     ${levels['entry_agg']:,.2f}  | RR {levels['rr_agg']:.1f}:1
  🛑 SL: ${levels['sl']:,.2f}

{_entry_recommendation(score, swing, direction)}
""".strip()

def _entry_recommendation(score, swing, direction):
    """Genera la recomendación de entrada según el score."""
    if score >= 75:
        return (
            "⚡ <b>ENTRADA DIRECTA — Score alto, confianza máxima</b>\n"
            "  → Coloca limit order en zona OTE AHORA\n"
            "  → No necesitas esperar CISD para entrar\n"
            "  → El CISD te llegará como confirmación bonus\n\n"
            "💡 <i>Setup de alta probabilidad. Actúa rápido.</i>"
        )
    elif score >= 50:
        return (
            f"⏳ <b>Esperar CISD en 15M para confirmar</b>\n"
            f"  Nivel a romper: <b>${swing:,.2f}</b>\n"
            f"  Condicion: cierre {direction} de ${swing:,.2f}\n\n"
            f"💡 <i>Setup sólido pero espera confirmación.\n"
            f"Si quieres ser agresivo, limit en zona OTE.</i>"
        )
    else:
        return (
            f"⚠️ <b>Setup débil — Solo entrar con CISD FUERTE</b>\n"
            f"  Nivel a romper: <b>${swing:,.2f}</b>\n"
            f"  Condicion: cierre {direction} de ${swing:,.2f}\n\n"
            f"⚠️ <i>Score bajo. No entrar sin confirmación clara.</i>"
        )

# ─── ALERTA 2 — CISD 15M CONFIRMADO ────────────────────────
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
    # MEJORA 6: calidad del CISD
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
    # PRO 11
    sweep      = detection.get("sweep_type", "DIRECTO")
    sweep_line = "\n🔄 <b>Sweep retroactivo</b> — vela trampa + recuperación confirmada" if sweep == "RETROACTIVO" else ""

    conf_text = ("\n\n🔗 <b>CONFLUENCIAS:</b>\n" + "".join(f"  • {x}\n" for x in confluences)
                 if confluences else "\n\n🔗 <b>CONFLUENCIAS:</b> Ninguna detectada")

    return f"""
{score_emoji(score)} — Score: <b>{score}/100</b>

✅ <b>CISD 15M CONFIRMADO — ENTRADA AHORA</b>

{e_type} <b>Par:</b> {symbol} — {tf_label}
{e_dir} <b>Tipo:</b> {mt}
🕯 <b>Manipulacion:</b> {time_str}
{sess_emoji} <b>Sesion:</b> {session}
💰 <b>Precio actual:</b> ${current_price:,.2f}
📊 <b>CISD en:</b> ${choch_price:,.2f}
{choch_str} <b>Calidad CISD:</b> cuerpo >50% + cierre tercio favorable{sweep_line}

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

🎯 <b>SETUP — Zona OTE:</b>
  📍 Entrada conservadora (62%): <b>${levels['entry_cons']:,.2f}</b>  | RR {levels['rr_cons']:.1f}:1
  📍 Entrada óptima (70.5%):     <b>${levels['entry']:,.2f}</b>  | RR {levels['rr']:.1f}:1 {rr_emoji}
  📍 Entrada agresiva (79%):     <b>${levels['entry_agg']:,.2f}</b>  | RR {levels['rr_agg']:.1f}:1
  🛑 Stop Loss:   <b>${levels['sl']:,.2f}</b>
  🎯 TP1 -2 SD:   <b>${levels['tp1']:,.2f}</b>
  🎯 TP2 -2.5:    <b>${levels['tp2']:,.2f}</b>
  💡 <i>Cualquier toque en la zona OTE es entrada válida</i>
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
        # PRO 7+11
        "vol_divergence": True,
        "sweep_type": "DIRECTO",
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

    send_telegram("🧪 <b>TEST v35 — Alerta 1</b>\n\n" +
                  format_detection_alert("BTCUSDT", "4H", detection, levels, score))
    time.sleep(2)
    send_telegram("🧪 <b>TEST v35 — Alerta 2: CISD confirmado</b>\n\n" +
                  format_choch_alert("BTCUSDT", "4H", detection, levels, confluences,
                                     "BEARISH", "BEARISH", score,
                                     detection["swing_choch"] * 0.999, price * 0.997,
                                     "FUERTE"))
    print("✅ Test v23 enviado")

# ─── MAIN ────────────────────────────────────────────────────
def main():
    print("🤖 Bot TTrades v35 — PRO Edition")

    if TEST_MODE:
        send_test_message()
        return

    send_telegram(
        "🤖 <b>Bot TTrades AMD v35 — PRO Edition + CISD Retroactivo</b>\n\n"
        "✅ Mejoras 1-12 completas\n"
        "🆕 PRO 1: Auto-cierre journal (Ganador/Perdedor)\n"
        "🆕 PRO 2: Resumen rendimiento semanal\n"
        "🆕 PRO 3: Anti-ban Binance (cache + rate limit)\n"
        "🆕 PRO 4: Heartbeat cada 6h\n"
        "🆕 PRO 5: Persistencia de estado (JSON)\n"
        "🆕 PRO 7: Divergencia volumen post-spike\n"
        "🆕 PRO 8: Confluencia multi-timeframe\n"
        "🆕 PRO 9: CISD retroactivo (movimientos agresivos)\n"
        "🆕 PRO 10: Zona OTE — 3 entradas (62%/70.5%/79%)\n"
        "🆕 PRO 11: Sweep retroactivo (vela trampa + recuperación)\n"
        "🆕 PRO 12: Market Alignment (5 pares como proxy mercado)\n"
        "🆕 PRO 13: Pre-alertas en vela actual (sin esperar cierre)\n"
        "🆕 PRO 14: CISD condicional (directo/esperar/fuerte)\n"
        "🆕 PRO 15: Ping-pong SD (rebotes entre niveles)\n"
        "🆕 PRO 16: Análisis avanzado de liquidez (4 capas)\n"
        "🆕 PRO 17: Filtro rango mínimo por par\n\n"
        "📊 Pares: BTC ETH SOL XRP BNB\n"
        "⏱ Timeframes: 1D + 4H + 1H\n"
        "⚡ Ciclo cada 15s. Modo velocidad. ⚡"
    )

    # PRO 5: cargar estado persistido
    pending_setups, dynamic_weights = load_state()
    candles_1d    = {}
    candles_4h    = {}
    last_1d_fetch = 0
    last_4h_fetch = 0
    alerted       = {}
    # PRO 13: tracking de pre-alertas para evitar duplicados
    pre_alerted   = {}  # {sym_interval: timestamp de última pre-alerta}
    # PRO 15: secuencias ping-pong activas
    pingpong_sequences = {}  # {sym_interval: sequence dict}
    # MEJORA 2: cache de velas históricas para percentil de volatilidad
    candles_hist    = {}
    last_hist_fetch = 0
    # MEJORA 7: cache de eventos macro
    macro_events     = []
    last_macro_fetch = 0
    macro_paused     = False
    # MEJORA 8: pesos dinámicos del scoring
    last_recalib     = 0
    # PRO 4: heartbeat
    last_heartbeat   = 0
    cycle_count      = 0
    # PRO 2: resumen semanal
    last_weekly       = 0

    while True:
        now = datetime.now(timezone.utc)
        cycle_count += 1
        print(f"\n[{now.strftime('%H:%M:%S')}] Revisando... (ciclo {cycle_count})")

        # PRO 4: heartbeat cada 6h
        if time.time() - last_heartbeat > HEARTBEAT_INTERVAL:
            pending_count = len(pending_setups)
            choch_count = sum(1 for s in pending_setups.values() if s.get("choch_alerted"))
            send_telegram(
                f"💓 <b>Heartbeat</b> — Bot activo\n\n"
                f"  Ciclos: {cycle_count}\n"
                f"  Setups pendientes: {pending_count} ({choch_count} post-CISD)\n"
                f"  Errores API: {_error_count}\n"
                f"  Pesos scoring: {'dinámicos' if dynamic_weights != DEFAULT_WEIGHTS else 'default'}"
            )
            last_heartbeat = time.time()

        # PRO 2: resumen semanal (domingos 00:00-01:00 UTC)
        if now.weekday() == 6 and now.hour == 0 and time.time() - last_weekly > 82800:
            if SHEETS_ENABLED:
                try:
                    report = generate_weekly_report()
                    if report:
                        send_telegram(
                            f"📊 <b>RESUMEN SEMANAL</b>\n\n"
                            f"  Trades: {report['total']} ({report['wins']}W / {report['losses']}L)\n"
                            f"  Winrate: {report['winrate']:.1f}%\n"
                            f"  RR medio ganador: {report['avg_rr_win']:.2f}\n"
                            f"  RR medio perdedor: {report['avg_rr_loss']:.2f}\n"
                            f"  Profit Factor: {report['profit_factor']:.2f}\n"
                            f"  Mejor trade: {report['best_rr']:.2f}R\n\n"
                            f"{'🔥' if report['winrate'] >= 55 else '👍' if report['winrate'] >= 45 else '⚠️'} "
                            f"{'Gran semana!' if report['winrate'] >= 55 else 'Semana sólida' if report['winrate'] >= 45 else 'Revisar estrategia'}"
                        )
                    else:
                        send_telegram("📊 <b>Resumen semanal:</b> Sin trades cerrados esta semana.")
                except Exception as e:
                    print(f"  ⚠️ Error resumen semanal: {e}")
            last_weekly = time.time()

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
            time.sleep(15)
            continue
        else:
            if macro_paused:
                macro_paused = False
                send_telegram("▶️ <b>Bot reanudado</b> — Ventana macro finalizada.")
                print("  ▶️ Bot reanudado tras evento macro")

        # PRO 12: calcular sesgo macro de TODOS los pares antes de procesar
        all_biases = {}
        for sym in SYMBOLS:
            c1d = candles_1d.get(sym, [])
            all_biases[sym] = get_macro_bias(c1d)
        aligned_str = " | ".join(f"{s.replace('USDT','')}: {b}" for s, b in all_biases.items())
        print(f"  📊 Market: {aligned_str}")

        for sym in SYMBOLS:
            c1d = candles_1d.get(sym, [])
            c4h = candles_4h.get(sym, [])

            macro_bias        = all_biases.get(sym, get_macro_bias(c1d))
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

                    # ── PRO 13: PRE-ALERTA EN VELA ACTUAL ─────────
                    pre_key = f"{sym}_{interval}"
                    live_spike = detect_live_spike(candles, interval)
                    if live_spike and pre_key not in pre_alerted:
                        # Solo pre-alertar si RR ≥ 1.5 y no hay ya un setup pendiente
                        has_pending = any(
                            s["sym"] == sym and s["interval"] == interval
                            for s in pending_setups.values()
                        )
                        if live_spike["rr"] >= 1.5 and not has_pending:
                            # PRO 17: filtro rango mínimo también en pre-alertas
                            min_rng = MIN_RANGE.get(sym, live_spike["fib_1"] * MIN_RANGE_DEFAULT)
                            if live_spike["rango"] < min_rng:
                                continue
                            mt = live_spike["type"]
                            e_type = "🟢" if mt == "ALCISTA" else "🔴"
                            send_telegram(
                                f"⚡ <b>PRE-ALERTA — SPIKE EN CURSO</b>\n\n"
                                f"{e_type} <b>{sym} — {label}</b> | Tipo: <b>{mt}</b>\n"
                                f"🔊 Volumen: {live_spike['vol_ratio']:.1f}x la media\n\n"
                                f"🎯 <b>ZONA OTE (si confirma):</b>\n"
                                f"  Conservadora (62%): ${live_spike['entry_cons']:,.2f}\n"
                                f"  Óptima (70.5%):     <b>${live_spike['entry']:,.2f}</b>\n"
                                f"  Agresiva (79%):     ${live_spike['entry_agg']:,.2f}\n"
                                f"  🛑 SL: ${live_spike['sl']:,.2f}\n"
                                f"  🎯 TP1: ${live_spike['tp1']:,.2f} | RR: {live_spike['rr']:.1f}:1\n\n"
                                f"⚠️ <i>Vela NO cerrada — puede invalidarse.\n"
                                f"Si quieres entrar agresivo, limit en zona OTE.\n"
                                f"La alerta completa llegará al cierre de vela.</i>"
                            )
                            pre_alerted[pre_key] = time.time()
                            print(f"  ⚡ PRE-ALERTA {label} {sym} {mt} — vol {live_spike['vol_ratio']:.1f}x")

                    # Limpiar pre-alertas antiguas (>4h)
                    pre_alerted = {k: v for k, v in pre_alerted.items()
                                   if time.time() - v < 14400}

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

                    # PRO 17: filtro de rango mínimo
                    min_rng = MIN_RANGE.get(sym, levels["fib_1"] * MIN_RANGE_DEFAULT)
                    if levels["rango"] < min_rng:
                        print(f"  {label} {sym}: rango ${levels['rango']:,.4f} < min ${min_rng} — descartado")
                        alerted[key] = last_closed_time
                        continue

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

                    # PRO 8: confluencia multi-timeframe
                    multi_tf_bonus = 0
                    det_high = detection["acc_high"]
                    det_low  = detection["acc_low"]
                    for pk, ps in pending_setups.items():
                        if ps["sym"] == sym and ps["interval"] != interval and \
                           ps["detection"]["type"] == detection["type"]:
                            other_high = ps["detection"]["acc_high"]
                            other_low  = ps["detection"]["acc_low"]
                            overlap = max(0, min(det_high, other_high) - max(det_low, other_low))
                            if overlap > 0:
                                multi_tf_bonus = 10
                                confluences.append(f"Confluencia multi-TF ({ps['label']} + {label})")
                                print(f"  🔗 Confluencia multi-TF: {label} + {ps['label']}")
                                break
                    score = min(score + multi_tf_bonus, 100)

                    # PRO 12: market alignment — confluencia de mercado
                    market_align = calc_market_alignment(all_biases, sym, detection["type"])
                    market_bonus = 0
                    if market_align["is_aligned"]:
                        market_bonus = 8
                        confluences.append(
                            f"Mercado alineado ({market_align['aligned_count']}/{market_align['total_pairs']} pares {market_align['details']})"
                        )
                        print(f"  🌐 Market alignment: {market_align['aligned_count']}/{market_align['total_pairs']} pares")
                    score = min(score + market_bonus, 100)

                    # PRO 16: análisis avanzado de liquidez
                    liq_analysis = calc_liquidity_score(candles, interval, detection, levels, sym)
                    liq_bonus = 0
                    if liq_analysis["score"] >= 75:
                        liq_bonus = 10
                    elif liq_analysis["score"] >= 60:
                        liq_bonus = 5
                    score = min(score + liq_bonus, 100)
                    # Añadir confluencias de liquidez
                    if liq_analysis["swept_pct"] >= 60:
                        confluences.append(f"Liquidez barrida {liq_analysis['swept']}/{liq_analysis['total_swings']} ({liq_analysis['swept_pct']:.0f}%)")
                    if liq_analysis["pools_at_tp"]:
                        confluences.append(f"Pool liquidez en TPs ({len(liq_analysis['pools_at_tp'])} clusters)")
                    if liq_analysis["round_numbers"]:
                        rns = ", ".join(f"${r['round']:,.0f}" for r in liq_analysis["round_numbers"][:2])
                        confluences.append(f"Round number cerca de SD: {rns}")

                    if score < 40:
                        print(f"  {label} {sym}: score bajo ({score})")
                        alerted[key] = last_closed_time
                        continue

                    setup_key = f"{sym}_{interval}_{last_closed_time}"
                    if setup_key not in pending_setups:
                        msg = format_detection_alert(sym, label, detection, levels, score, liq_analysis)
                        send_telegram(msg)

                        # PRO 14: determinar modo de entrada según score
                        if score >= 75:
                            entry_mode = "DIRECTA"
                            choch_done = True   # no esperar CISD
                            entry_price = levels["entry"]  # OTE como referencia
                            print(f"  ⚡ ENTRADA DIRECTA — {label} {sym} | Score {score} | OTE ${levels['entry']:,.2f}")
                        elif score >= 50:
                            entry_mode = "ESPERAR_CHOCH"
                            choch_done = False
                            entry_price = None
                            print(f"  ⏳ Esperar CISD — {label} {sym} | Score {score} | Swing ${detection['swing_choch']:,.2f}")
                        else:
                            entry_mode = "SOLO_CHOCH_FUERTE"
                            choch_done = False
                            entry_price = None
                            print(f"  ⚠️ Solo CISD fuerte — {label} {sym} | Score {score}")

                        # Registrar en Google Sheets
                        if SHEETS_ENABLED:
                            try:
                                log_detection(detection, levels, score, setup_key, sym)
                            except Exception as e:
                                print(f"  ⚠️ Error Sheets alerta 1: {e}")

                        pending_setups[setup_key] = {
                            "sym": sym, "label": label, "interval": interval,
                            "detection": detection, "levels": levels,
                            "confluences": confluences, "score": score,
                            "macro_bias": macro_bias, "structure_bias": structure_bias,
                            "detected_at": now,
                            # PRO 14: modo de entrada condicional
                            "entry_mode": entry_mode,
                            "choch_alerted": choch_done,
                            "choch_entry_price": entry_price,
                            "choch_bonus_sent": False,
                            # MEJORA 9: campos para invalidación post-entrada
                            "invalidated": False,
                            "weakened_alerted": False,
                            # MEJORA 11: tracking de TPs
                            "tp_hit": {"sd_m1": False, "sd_m2": False, "sd_m25": False, "sd_m4": False},
                            "sl_hit": False,
                        }

                        # PRO 15: crear secuencia ping-pong si el anchor es en extremo
                        pp_key = f"pp_{sym}_{interval}"
                        if sym in PP_VALID_PAIRS and score >= PP_MIN_ANCHOR_SCORE and \
                           pp_key not in pingpong_sequences:
                            try:
                                spike_price = detection["fib_1"]
                                if is_at_extreme(candles, interval, detection["type"], spike_price):
                                    pp_seq = create_pingpong_sequence(sym, detection, levels, score, interval)
                                    pingpong_sequences[pp_key] = pp_seq
                                    send_telegram(
                                        f"🏓 <b>PING-PONG ACTIVADO</b> — {sym} {label}\n\n"
                                        f"Anchor: {detection['type']} (score {score})\n"
                                        f"Spike en nuevo {'high' if detection['type'] == 'BAJISTA' else 'low'}\n\n"
                                        f"📊 <b>Mapa SD para ping-pong:</b>\n"
                                        f"  0 (rango): ${levels['fib_0']:,.2f}\n"
                                        f"  -1 SD:     ${levels['sd_m1']:,.2f}\n"
                                        f"  -2 SD:     ${levels['sd_m2']:,.2f}\n"
                                        f"  -2.5 SD:   ${levels['sd_m25']:,.2f}\n"
                                        f"  -4 SD:     ${levels['sd_m4']:,.2f}\n\n"
                                        f"Monitorizando rebotes en cada nivel.\n"
                                        f"Máx {PP_MAX_TRADES} trades. Se para con {PP_MAX_FAILS} fallos seguidos."
                                    )
                                    print(f"  🏓 Ping-pong activado — {sym} {label}")
                            except Exception as e:
                                print(f"  ⚠️ Error creando ping-pong: {e}")

                    alerted[key] = last_closed_time

                except Exception as e:
                    print(f"  ❌ Error {label} {sym}: {e}")

            # ── PASO 2: vigilar CISD 15M ──
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
                        f"{sym} {setup['label']} — CISD no confirmado en {SETUP_EXPIRY_H}h.\n"
                        f"Setup descartado."
                    )
                    continue

                # ── MEJORA 10: INVALIDACIÓN POR ESTRUCTURA ────────────
                # Solo aplica a setups esperando CISD (no post-entrada)
                if not setup["choch_alerted"] and current_price:
                    det  = setup["detection"]
                    mt   = det["type"]
                    fib0 = det["fib_0"]
                    fib1 = det["fib_1"]

                    # Check 1: precio alcanzó fib_0 sin CISD → el move ya pasó
                    fib0_reached = False
                    if mt == "BAJISTA" and current_price <= fib0:
                        fib0_reached = True
                    elif mt == "ALCISTA" and current_price >= fib0:
                        fib0_reached = True

                    if fib0_reached:
                        # PRO 9: ANTES de descartar, verificar CISD retroactivo
                        # ¿Hubo algún cierre de 15M debajo/encima del swing entre
                        # la manipulación y ahora? Si sí, fue un movimiento agresivo
                        # que el bot no capturó por timing pero el setup era válido.
                        retroactive_choch = False
                        retro_price = None
                        retro_time = None
                        try:
                            candles_15m_check = get_candles(sym, "15m", 50)
                            if candles_15m_check:
                                closed_15m_check = get_closed(candles_15m_check, "15m")
                                manip_time_check = det["candle"]["time"]
                                swing = det["swing_choch"]
                                post_15m = [c for c in closed_15m_check if c["time"] > manip_time_check]

                                for c15 in post_15m:
                                    if mt == "BAJISTA" and c15["close"] < swing:
                                        retroactive_choch = True
                                        retro_price = c15["close"]
                                        retro_time = c15["time"]
                                        break
                                    if mt == "ALCISTA" and c15["close"] > swing:
                                        retroactive_choch = True
                                        retro_price = c15["close"]
                                        retro_time = c15["time"]
                                        break
                        except Exception as e:
                            print(f"  ⚠️ Error check CISD retroactivo: {e}")

                        if retroactive_choch:
                            # Setup confirmado a posteriori — alerta especial
                            setup["choch_alerted"] = True
                            setup["choch_entry_price"] = retro_price
                            time_str = retro_time.strftime("%H:%M UTC") if retro_time else "?"

                            send_telegram(
                                f"⚡ <b>CISD RETROACTIVO DETECTADO</b> — {sym} {setup['label']}\n\n"
                                f"El precio se movió tan rápido que llegó al nivel 0 "
                                f"antes del ciclo de monitoreo, pero <b>SÍ hubo CISD</b> "
                                f"válido en 15M ({time_str}).\n\n"
                                f"  CISD en:      ${retro_price:,.2f}\n"
                                f"  Precio actual: ${current_price:,.2f}\n"
                                f"  Nivel 0:       ${fib0:,.2f}\n\n"
                                f"⚠️ <i>Movimiento agresivo. El setup se cumplió, "
                                f"pero la entrada OTE ya no es válida — precio fuera del rango. "
                                f"Útil para validar el modelo, no para entrar ahora.</i>"
                            )
                            print(f"  ⚡ CISD retroactivo detectado en ${retro_price:,.2f}")

                            # Registrar en Sheets como setup confirmado (no como descartado)
                            if SHEETS_ENABLED:
                                try:
                                    update_choch(setup_key, retro_price,
                                                setup["confluences"] + ["CISD RETROACTIVO"],
                                                current_price,
                                                setup["macro_bias"], setup["detection"]["type"])
                                except Exception as e:
                                    print(f"  ⚠️ Error Sheets CISD retroactivo: {e}")

                            expired_keys.append(setup_key)
                            continue

                        # Sin CISD retroactivo → descarte normal
                        expired_keys.append(setup_key)
                        send_telegram(
                            f"📉 <b>Setup descartado — Movimiento sin CISD</b>\n"
                            f"{sym} {setup['label']}\n\n"
                            f"El precio alcanzó el nivel 0 (${fib0:,.2f}) "
                            f"sin confirmar CISD en 15M.\n"
                            f"El movimiento ocurrió sin nuestra confirmación.\n"
                            f"Setup descartado."
                        )
                        print(f"  📉 Descartado — precio alcanzó fib0 ${fib0:,.2f} sin CISD (ni retroactivo)")
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

                    # PRO 14: bonus CISD para entradas directas
                    if setup.get("entry_mode") == "DIRECTA" and not setup.get("choch_bonus_sent"):
                        choch_ok, choch_p, choch_q = check_cisd_15m(sym, setup["detection"])
                        if choch_ok:
                            setup["choch_bonus_sent"] = True
                            setup["choch_entry_price"] = choch_p  # actualizar entrada real
                            q_str = "💪 FUERTE" if choch_q == "FUERTE" else "⚠️ DEBIL"
                            send_telegram(
                                f"✅ <b>CISD CONFIRMADO (bonus)</b> — {sym} {setup['label']}\n\n"
                                f"  Calidad: {q_str}\n"
                                f"  CISD en: ${choch_p:,.2f}\n"
                                f"  Precio actual: ${current_price:,.2f}\n\n"
                                f"💡 <i>Tu entrada directa queda confirmada por CISD.\n"
                                f"Mantener posición con confianza.</i>"
                            )
                            print(f"  ✅ Bonus CISD {choch_q} — {sym} en ${choch_p:,.2f}")
                            if SHEETS_ENABLED:
                                try:
                                    update_choch(setup_key, choch_p,
                                                setup["confluences"], current_price,
                                                setup["macro_bias"], setup["detection"]["type"])
                                except:
                                    pass

                    det  = setup["detection"]
                    mt   = det["type"]
                    fib1 = det["fib_1"]
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
                                    f"El precio ha retrocedido {retrace_pct:.0%} del movimiento post-CISD.\n"
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
                            rr_real = abs(current_price - entry_price) / abs(sl_price - entry_price) if abs(sl_price - entry_price) > 0 else 0
                            pnl_pct = (current_price - entry_price) / entry_price * 100 if mt == "ALCISTA" else (entry_price - current_price) / entry_price * 100
                            send_telegram(
                                f"🛑 <b>STOP LOSS TOCADO</b> — {sym} {setup['label']}\n\n"
                                f"  Entrada:  ${entry_price:,.2f}\n"
                                f"  SL:       ${sl_price:,.2f}\n"
                                f"  Precio:   ${current_price:,.2f}\n"
                                f"  TPs alcanzados: {len(tps_reached)}/4\n\n"
                                f"{'✅ Parciales tomados: ' + ', '.join(tps_reached) if tps_reached else '❌ Ningún TP alcanzado'}"
                            )
                            # PRO 1: auto-cierre en journal
                            if SHEETS_ENABLED:
                                try:
                                    resultado = "Ganador" if len(tps_reached) >= 2 else "Perdedor"
                                    auto_close_trade(setup_key, resultado, -rr_real, pnl_pct, f"{len(tps_reached)}/4")
                                except Exception as e:
                                    print(f"  ⚠️ Error auto-cierre: {e}")
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
                                        # PRO 1: auto-cierre en journal
                                        if SHEETS_ENABLED:
                                            try:
                                                pnl_pct = gain_pct if mt == "BAJISTA" else gain_pct
                                                auto_close_trade(setup_key, "Ganador", rr_real, pnl_pct, "4/4")
                                            except Exception as e:
                                                print(f"  ⚠️ Error auto-cierre: {e}")
                                        expired_keys.append(setup_key)

                    continue

                choch_confirmed, choch_price, choch_quality = check_cisd_15m(sym, setup["detection"])

                if choch_confirmed and current_price:
                    entry_mode = setup.get("entry_mode", "ESPERAR_CHOCH")

                    # PRO 14: si modo SOLO_CHOCH_FUERTE, rechazar débil
                    if entry_mode == "SOLO_CHOCH_FUERTE" and choch_quality != "FUERTE":
                        send_telegram(
                            f"⚠️ <b>CISD DEBIL RECHAZADO</b> — {sym} {setup['label']}\n\n"
                            f"Score {setup['score']}/100 — requiere CISD fuerte.\n"
                            f"  Precio ruptura: ${choch_price:,.2f}\n"
                            f"  Precio actual:  ${current_price:,.2f}\n\n"
                            f"⚠️ <i>Setup débil + CISD débil = no entrar.</i>"
                        )
                        print(f"  ⚠️ CISD débil rechazado para setup débil — {sym}")
                        # No marcar como alerted — seguir esperando uno fuerte
                        continue

                    # MEJORA 9: guardar precio de entrada para monitoreo
                    setup["choch_entry_price"] = choch_price

                    if choch_quality == "FUERTE":
                        # CISD fuerte — alerta completa de entrada
                        msg = format_choch_alert(
                            sym, setup["label"],
                            setup["detection"], setup["levels"],
                            setup["confluences"], setup["macro_bias"],
                            setup["structure_bias"], setup["score"],
                            choch_price, current_price, choch_quality
                        )
                        send_telegram(msg)
                        setup["choch_alerted"] = True
                        print(f"  🎯 Alerta 2 CISD FUERTE — {sym} en ${choch_price:,.2f}")
                    else:
                        # CISD débil — alerta informativa, no de entrada
                        send_telegram(
                            f"⚠️ <b>CISD 15M DEBIL</b> — {sym} {setup['label']}\n\n"
                            f"El precio rompió ${setup['detection']['swing_choch']:,.2f} "
                            f"pero la vela de ruptura no tiene momentum suficiente.\n"
                            f"  Precio ruptura: ${choch_price:,.2f}\n"
                            f"  Precio actual:  ${current_price:,.2f}\n\n"
                            f"⚠️ <i>Setup válido pero entrada a tu criterio.</i>"
                        )
                        setup["choch_alerted"] = True
                        print(f"  ⚠️ Alerta 2 CISD DEBIL — {sym} en ${choch_price:,.2f}")

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
                    print(f"  Esperando CISD ${swing:,.2f} | precio ${current_price:,.2f} ({dist:.1f}% lejos)")

            for k in expired_keys:
                pending_setups.pop(k, None)

            # ── PRO 15: MONITOREO PING-PONG SD ──────────────
            for pp_key, pp_seq in list(pingpong_sequences.items()):
                if pp_seq["sym"] != sym or not pp_seq["active"]:
                    continue

                if not current_price:
                    current_price = get_current_price(sym)
                if not current_price:
                    continue

                # Kill condition: precio fuera de rango extremo
                fib1 = pp_seq["fib_1"]
                sd4  = pp_seq["levels"]["sd_m4"]
                if pp_seq["anchor_type"] == "BAJISTA":
                    if current_price > fib1:
                        pp_seq["active"] = False
                        send_telegram(
                            f"🏓❌ <b>Ping-pong TERMINADO</b> — {sym}\n"
                            f"Precio (${current_price:,.2f}) superó el nivel 1.0 (${fib1:,.2f}).\n"
                            f"Trades: {pp_seq['trade_count']} realizados."
                        )
                        continue
                else:
                    if current_price < fib1:
                        pp_seq["active"] = False
                        send_telegram(
                            f"🏓❌ <b>Ping-pong TERMINADO</b> — {sym}\n"
                            f"Precio (${current_price:,.2f}) rompió el nivel 1.0 (${fib1:,.2f}).\n"
                            f"Trades: {pp_seq['trade_count']} realizados."
                        )
                        continue

                # Si hay trade activo, monitorear SL/TP
                ct = pp_seq["current_trade"]
                if ct:
                    tp_hit = False
                    sl_hit = False
                    if ct["direction"] == "LONG":
                        tp_hit = current_price >= ct["tp"]
                        sl_hit = current_price <= ct["sl"]
                    else:
                        tp_hit = current_price <= ct["tp"]
                        sl_hit = current_price >= ct["sl"]

                    if tp_hit:
                        ct["result"] = "WIN"
                        pp_seq["trades"].append(ct)
                        pp_seq["consecutive_fails"] = 0
                        rr_real = abs(current_price - ct["entry"]) / abs(ct["sl"] - ct["entry"]) if abs(ct["sl"] - ct["entry"]) > 0 else 0
                        send_telegram(
                            f"🏓✅ <b>PING-PONG TP ALCANZADO</b> — {sym}\n\n"
                            f"  Trade #{ct['num']}: {ct['direction']} desde {ct['sd_name']}\n"
                            f"  Entrada: ${ct['entry']:,.2f}\n"
                            f"  TP:      ${ct['tp']:,.2f}\n"
                            f"  Precio:  ${current_price:,.2f}\n"
                            f"  RR real: {rr_real:.1f}:1\n\n"
                            f"Secuencia: {sum(1 for t in pp_seq['trades'] if t['result']=='WIN')}W / "
                            f"{sum(1 for t in pp_seq['trades'] if t['result']=='LOSS')}L"
                        )
                        pp_seq["current_trade"] = None
                        # PRO 1: auto-cierre en journal
                        if SHEETS_ENABLED:
                            try:
                                pnl = abs(current_price - ct["entry"]) / ct["entry"] * 100
                                auto_close_trade(f"pp_{pp_key}_{ct['num']}", "Ganador", rr_real, pnl, f"PP #{ct['num']}")
                            except:
                                pass

                    elif sl_hit:
                        ct["result"] = "LOSS"
                        pp_seq["trades"].append(ct)
                        pp_seq["consecutive_fails"] += 1
                        send_telegram(
                            f"🏓🛑 <b>PING-PONG SL TOCADO</b> — {sym}\n\n"
                            f"  Trade #{ct['num']}: {ct['direction']} desde {ct['sd_name']}\n"
                            f"  Entrada: ${ct['entry']:,.2f}\n"
                            f"  SL:      ${ct['sl']:,.2f}\n"
                            f"  Precio:  ${current_price:,.2f}\n\n"
                            f"Fallos consecutivos: {pp_seq['consecutive_fails']}/{PP_MAX_FAILS}"
                        )
                        pp_seq["current_trade"] = None
                        # Kill si demasiados fallos
                        if pp_seq["consecutive_fails"] >= PP_MAX_FAILS:
                            pp_seq["active"] = False
                            send_telegram(
                                f"🏓❌ <b>Ping-pong TERMINADO</b> — {sym}\n"
                                f"{PP_MAX_FAILS} fallos consecutivos. Tendencia venció.\n"
                                f"Total: {sum(1 for t in pp_seq['trades'] if t['result']=='WIN')}W / "
                                f"{sum(1 for t in pp_seq['trades'] if t['result']=='LOSS')}L"
                            )
                    continue

                # Si no hay trade activo, buscar señal de rebote
                next_trade = get_pp_next_trade(pp_seq, current_price)
                if next_trade:
                    signal = detect_sd_reversal(
                        sym, next_trade["entry_level"],
                        next_trade["direction"], "15m"
                    )
                    if signal:
                        # PRO 16: check liquidez en este nivel SD
                        pp_liq_str = ""
                        try:
                            pp_candles = get_candles(sym, pp_seq["interval"], 50)
                            if pp_candles:
                                pp_swings = find_swing_points(get_closed(pp_candles, pp_seq["interval"]), lookback=2)
                                # Buscar swings cerca del nivel de entrada
                                sd_tol = pp_seq["rango"] * 0.15
                                if next_trade["direction"] == "LONG":
                                    nearby = [p for _, p in pp_swings["lows"] if abs(p - next_trade["entry_level"]) <= sd_tol]
                                else:
                                    nearby = [p for _, p in pp_swings["highs"] if abs(p - next_trade["entry_level"]) <= sd_tol]
                                rn, _ = get_round_number(next_trade["entry_level"], sym)
                                rn_near = abs(rn - next_trade["entry_level"]) <= pp_seq["rango"] * 0.20
                                if nearby or rn_near:
                                    parts = []
                                    if nearby:
                                        parts.append(f"{len(nearby)} swings")
                                    if rn_near:
                                        parts.append(f"round ${rn:,.0f}")
                                    pp_liq_str = f"\n💧 Liquidez: {' + '.join(parts)} en {next_trade['sd_name']}"
                        except:
                            pass

                        pp_seq["trade_count"] += 1
                        trade_num = pp_seq["trade_count"]
                        trade = {
                            "num": trade_num,
                            "direction": next_trade["direction"],
                            "sd_name": next_trade["sd_name"],
                            "entry": signal["price"],
                            "sl": next_trade["sl"],
                            "tp": next_trade["tp"],
                            "signal": signal["type"],
                            "result": None,
                        }
                        pp_seq["current_trade"] = trade
                        e_type = "🟢 LONG" if next_trade["direction"] == "LONG" else "🔴 SHORT"
                        wins = sum(1 for t in pp_seq["trades"] if t["result"] == "WIN")
                        losses = sum(1 for t in pp_seq["trades"] if t["result"] == "LOSS")
                        send_telegram(
                            f"🏓 <b>PING-PONG #{trade_num}</b> — {e_type}\n"
                            f"{sym} | Anchor: {pp_seq['anchor_type']} score {pp_seq['anchor_score']}\n"
                            f"Secuencia: {wins}W / {losses}L\n\n"
                            f"📍 Señal: <b>{signal['type']}</b> en ${signal['price']:,.2f}\n"
                            f"  Vol: {signal['vol_ratio']:.1f}x{pp_liq_str}\n\n"
                            f"🎯 Entrada: ${signal['price']:,.2f} ({next_trade['sd_name']})\n"
                            f"🛑 SL: ${next_trade['sl']:,.2f}\n"
                            f"✅ TP: ${next_trade['tp']:,.2f}\n"
                            f"📊 RR: {next_trade['rr']:.1f}:1\n\n"
                            f"Trade #{trade_num} de máx {PP_MAX_TRADES}"
                        )
                        print(f"  🏓 PP #{trade_num} {next_trade['direction']} {sym} en {next_trade['sd_name']}")

            # Limpiar secuencias muertas (>48h o inactivas)
            dead_pp = [k for k, v in pingpong_sequences.items()
                       if not v["active"] or
                       (now - v["created_at"]).total_seconds() > 172800]
            for k in dead_pp:
                pingpong_sequences.pop(k, None)

        # PRO 5: guardar estado cada ciclo
        try:
            save_state(pending_setups, dynamic_weights)
        except Exception as e:
            print(f"  ⚠️ Error guardando estado: {e}")

        time.sleep(15)

if __name__ == "__main__":
    main()
