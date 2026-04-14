import os
import time
import requests
from datetime import datetime, timezone, timedelta
try:
    from sheets import log_detection, update_choch
    SHEETS_ENABLED = True
    print("✅ Google Sheets integrado")
except Exception as e:
    SHEETS_ENABLED = False
    print(f"⚠️ Google Sheets no disponible: {e}")

TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN")
CHAT_ID        = os.environ.get("CHAT_ID")
TEST_MODE      = os.environ.get("TEST_MODE", "false").lower() == "true"

LOOKBACK        = 10
BODY_MULTIPLIER = 1.8
SL_PERCENT      = 0.02
MIN_RR          = 1.2
SETUP_EXPIRY_H  = 12

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
    secs = {"1h": 3600, "4h": 14400, "1d": 86400}.get(interval, 3600)
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

# ─── ACUMULACION POR CUERPOS ─────────────────────────────────
def get_accumulation_range(prev_candles):
    return max(c["body_high"] for c in prev_candles), min(c["body_low"] for c in prev_candles)

# ─── LIQUIDEZ PREVIA ─────────────────────────────────────────
def has_prior_liquidity(prev_candles, manip_candle, manip_type):
    tol = manip_candle["close"] * 0.001
    if manip_type == "ALCISTA":
        level = manip_candle["low"]
        return sum(1 for c in prev_candles if abs(c["low"] - level) < tol * 3) >= 1
    else:
        level = manip_candle["high"]
        return sum(1 for c in prev_candles if abs(c["high"] - level) < tol * 3) >= 1

# ─── RETORNO AL RANGO ────────────────────────────────────────
def verify_return_to_range(candles, interval, manip_candle, acc_high, acc_low):
    closed     = get_closed(candles, interval)
    manip_time = manip_candle["time"]
    post       = [c for c in closed if c["time"] > manip_time]
    for c in post[:3]:
        if acc_low <= c["body_low"] <= acc_high or acc_low <= c["body_high"] <= acc_high:
            return True
    return False

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
def detect_manipulation(candles, interval):
    closed = get_closed(candles, interval)
    if len(closed) < LOOKBACK + 1:
        return None

    prev_candles = closed[-(LOOKBACK + 1):-1]
    manip        = closed[-1]

    acc_high, acc_low = get_accumulation_range(prev_candles)
    acc_range = acc_high - acc_low
    if acc_range <= 0:
        return None

    avg_size   = sum(c["total_size"] for c in prev_candles) / len(prev_candles)
    manip_size = manip["total_size"]
    if manip_size < avg_size * BODY_MULTIPLIER:
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

    has_liq = has_prior_liquidity(prev_candles, manip, manip_type)
    if not is_last_spike(candles, interval, manip, avg_size, manip_type):
        return None

    returned = verify_return_to_range(candles, interval, manip, acc_high, acc_low)
    session_name, session_emoji = get_session(manip["time"])

    # ── ANCLAJE CORRECTO TTRADES ──────────────────────────────
    # BAJISTA: 1 = High absoluto del spike (wick incluido)
    #          0 = Low del rango de acumulacion previo
    # ALCISTA: 1 = Low absoluto del spike (wick incluido)
    #          0 = High del rango de acumulacion previo
    if manip_type == "BAJISTA":
        fib_1 = manip["high"]   # High del spike — punto de partida
        fib_0 = acc_low         # Low del rango de acumulacion
    else:
        fib_1 = manip["low"]    # Low del spike — punto de partida
        fib_0 = acc_high        # High del rango de acumulacion

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
        "has_liq":       has_liq,
        "swing_choch":   swing_choch,
        "fib_1":         fib_1,   # TTrades: nivel 1 (inicio manipulacion)
        "fib_0":         fib_0,   # TTrades: nivel 0 (fin del rango)
    }

# ─── CHoCH EN 15M ────────────────────────────────────────────
def check_choch_15m(symbol, detection):
    candles_15m = get_candles(symbol, "15m", 20)
    if not candles_15m:
        return False, None
    closed_15m = get_closed(candles_15m, "15m")
    if not closed_15m:
        return False, None

    swing      = detection["swing_choch"]
    manip_type = detection["type"]
    manip_time = detection["candle"]["time"]
    post       = [c for c in closed_15m if c["time"] > manip_time]

    for c in post:
        if manip_type == "BAJISTA" and c["close"] < swing:
            return True, c["close"]
        if manip_type == "ALCISTA" and c["close"] > swing:
            return True, c["close"]

    return False, None

# ─── CALCULAR NIVELES FIBONACCI (anclaje TTrades) ────────────
def calculate_levels(detection):
    mt    = detection["type"]
    fib_1 = detection["fib_1"]   # punto de partida (High/Low del spike)
    fib_0 = detection["fib_0"]   # punto final (Low/High del rango acumulacion)
    rango = abs(fib_1 - fib_0)   # rango completo manipulation leg

    if mt == "BAJISTA":
        # 1 arriba (High spike), 0 abajo (Low acumulacion)
        # SDs se proyectan HACIA ABAJO desde el nivel 0
        eq     = fib_1 - rango * 0.50   # equilibrium 50%
        entry  = fib_1 - rango * 0.705  # OTE ~70%
        sl     = fib_1 * (1 + 0.005)    # stop por encima del High del spike
        sd_m1  = fib_0 - rango * 1.0
        sd_m2  = fib_0 - rango * 2.0
        sd_m25 = fib_0 - rango * 2.5
        sd_m4  = fib_0 - rango * 4.0
        tp1, tp2 = sd_m2, sd_m25
        rr = (entry - tp1) / (sl - entry) if (sl - entry) > 0 else 0
    else:
        # 1 abajo (Low spike), 0 arriba (High acumulacion)
        # SDs se proyectan HACIA ARRIBA desde el nivel 0
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

    if detection["returned"]: confs.append("Precio retorno al rango tras el spike")
    if detection["has_liq"]:  confs.append("Liquidez previa visible en el nivel barrido")

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

    avg_vol   = sum(c["volume"] for c in closed[-LOOKBACK:]) / LOOKBACK
    vol_ratio = detection["candle"]["volume"] / avg_vol
    if vol_ratio > 1.6:
        confs.append(f"Volumen elevado ({vol_ratio:.1f}x la media)")
    if detection["wick_ratio"] > 0.40:
        confs.append(f"Wick pronunciado ({round(detection['wick_ratio']*100)}%)")

    return confs

# ─── SCORE ───────────────────────────────────────────────────
def quality_score(confs, rr, macro_bias, mt, detection):
    score = 0
    score += SESSION_SCORE.get(detection.get("session", "Post-NY"), 0)
    aligned = (macro_bias == "BULLISH" and mt == "ALCISTA") or \
              (macro_bias == "BEARISH" and mt == "BAJISTA")
    if aligned:               score += 15
    elif macro_bias == "NEUTRAL": score += 7
    if detection.get("returned"): score += 12
    if detection.get("has_liq"):  score += 8
    if rr >= 2.5:   score += 15
    elif rr >= 2.0: score += 12
    elif rr >= 1.5: score += 8
    elif rr >= 1.2: score += 4
    pure = [c for c in confs if not any(k in c.lower() for k in
            ["sesgo", "choch", "retorno", "liquidez"])]
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

    return f"""
{score_emoji(score)} <b>MANIPULACION DETECTADA</b> — Score: {score}/100

{e_type} <b>{symbol} — {tf_label}</b> | Tipo: <b>{mt}</b>
{sess_emoji} Sesion: {session} | Vela: {time_str}

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
                       macro_bias, structure_bias, score, choch_price, current_price):
    mt         = detection["type"]
    c          = detection["candle"]
    e_type     = "🟢" if mt == "ALCISTA" else "🔴"
    e_dir      = "📈" if mt == "ALCISTA" else "📉"
    rr_emoji   = "✅" if levels["rr"] >= 1.5 else "⚠️"
    time_str   = c["time"].strftime("%d/%m/%Y %H:%M UTC")
    macro_str  = {"BULLISH": "🟢 BULLISH", "BEARISH": "🔴 BEARISH", "NEUTRAL": "⚪ NEUTRAL"}.get(macro_bias, "⚪")
    struct_str = {"BULLISH": "🟢 BULLISH", "BEARISH": "🔴 BEARISH", "NEUTRAL": "⚪ NEUTRAL"}.get(structure_bias, "⚪")
    ret_str    = "✅" if detection["returned"] else "❌"
    liq_str    = "✅" if detection["has_liq"]  else "❌"
    sess_emoji = detection.get("session_emoji", "⏰")
    session    = detection.get("session", "—")

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

📊 <b>CONTEXTO AMD:</b>
  Rango acumulacion: ${detection['acc_low']:,.2f} — ${detection['acc_high']:,.2f}
  Liquidez previa:   {liq_str}
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
        "wick_ratio": 0.45, "returned": True, "has_liq": True,
        "swing_choch": price - rango * 0.15,
        "fib_1": price + rango,        # 1 = High del spike
        "fib_0": price - rango * 0.3,  # 0 = Low del rango acumulacion
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

    send_telegram("🧪 <b>TEST v11 — Alerta 1</b>\n\n" +
                  format_detection_alert("BTCUSDT", "4H", detection, levels, score))
    time.sleep(2)
    send_telegram("🧪 <b>TEST v11 — Alerta 2: CHoCH confirmado</b>\n\n" +
                  format_choch_alert("BTCUSDT", "4H", detection, levels, confluences,
                                     "BEARISH", "BEARISH", score,
                                     detection["swing_choch"] * 0.999, price * 0.997))
    print("✅ Test v11 enviado")

# ─── MAIN ────────────────────────────────────────────────────
def main():
    print("🤖 Bot TTrades v11 — Anclaje TTrades correcto + CHoCH 15M")

    if TEST_MODE:
        send_test_message()
        return

    send_telegram(
        "🤖 <b>Bot TTrades AMD v11 activado</b>\n\n"
        "Anclaje Fibonacci fiel al modelo TTrades:\n"
        "  1 = High/Low del spike (inicio manipulacion)\n"
        "  0 = Low/High del rango de acumulacion\n\n"
        "2 alertas por setup: deteccion + CHoCH 15M\n"
        "BTCUSDT en <b>4H y 1H</b>. Revision cada minuto. ⚡"
    )

    pending_setups = {}
    candles_1d    = {}
    candles_4h    = {}
    last_1d_fetch = 0
    last_4h_fetch = 0
    alerted       = {}

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

                    detection = detect_manipulation(candles, interval)
                    if not detection:
                        print(f"  {label} {sym}: sin manipulacion")
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
                                         macro_bias, detection["type"], detection)

                    if score < 40:
                        print(f"  {label} {sym}: score bajo ({score})")
                        alerted[key] = last_closed_time
                        continue

                    setup_key = f"{sym}_{interval}_{last_closed_time}"
                    if setup_key not in pending_setups:
                        msg = format_detection_alert(sym, label, detection, levels, score)
                        send_telegram(msg)
                        print(f"  ✅ Alerta 1 — {label} {sym} | CHoCH swing: ${detection['swing_choch']:,.2f}")

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
                            "detected_at": now, "choch_alerted": False
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

                if setup["choch_alerted"]:
                    continue

                choch_confirmed, choch_price = check_choch_15m(sym, setup["detection"])

                if choch_confirmed and current_price:
                    msg = format_choch_alert(
                        sym, setup["label"],
                        setup["detection"], setup["levels"],
                        setup["confluences"], setup["macro_bias"],
                        setup["structure_bias"], setup["score"],
                        choch_price, current_price
                    )
                    send_telegram(msg)
                    setup["choch_alerted"] = True
                    print(f"  🎯 Alerta 2 CHoCH — {sym} en ${choch_price:,.2f}")

                    # Actualizar Google Sheets
                    if SHEETS_ENABLED:
                        try:
                            update_choch(setup_key, choch_price,
                                        setup["confluences"], current_price)
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

# placeholder
