import os
import time
import requests
from datetime import datetime, timezone

TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN")
CHAT_ID        = os.environ.get("CHAT_ID")
TEST_MODE      = os.environ.get("TEST_MODE", "false").lower() == "true"

LOOKBACK        = 10
BODY_MULTIPLIER = 1.8
SL_PERCENT      = 0.02
MIN_RR          = 1.2

TIMEFRAMES = [
    {"interval": "1d",  "label": "1D",  "limit": 80},
    {"interval": "4h",  "label": "4H",  "limit": 80},
    {"interval": "1h",  "label": "1H",  "limit": 80},
]

SYMBOLS = ["BTCUSDT", "TOTAL3"]

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
    # TOTAL3 no está en Binance — usamos BTC como proxy de market cap
    btc_proxy = symbol == "TOTAL3"
    sym = "BTCUSDT" if btc_proxy else symbol
    url    = "https://api.binance.com/api/v3/klines"
    params = {"symbol": sym, "interval": interval, "limit": limit}
    try:
        r    = requests.get(url, params=params, timeout=10)
        data = r.json()
        if not isinstance(data, list):
            return []
        return [{
            "time":   datetime.fromtimestamp(c[0] / 1000, tz=timezone.utc),
            "open":   float(c[1]),
            "high":   float(c[2]),
            "low":    float(c[3]),
            "close":  float(c[4]),
            "volume": float(c[5])
        } for c in data]
    except Exception as e:
        print(f"❌ Velas {symbol} {interval}: {e}")
        return []

# ─── CIERRE DE VELA ──────────────────────────────────────────
def candle_closed(candle, interval):
    secs  = {"1h": 3600, "4h": 14400, "1d": 86400}.get(interval, 3600)
    close = candle["time"].timestamp() + secs
    return datetime.now(timezone.utc).timestamp() >= close

def get_closed(candles, interval):
    return [c for c in candles if candle_closed(c, interval)]

# ─── SESGO MACRO (Daily bias) ────────────────────────────────
def get_macro_bias(candles_1d):
    """
    Determina el sesgo macro en Daily usando estructura de mercado:
    - Bullish: último HH y HL confirmados (precio > EMA20 y haciendo higher highs)
    - Bearish: último LH y LL confirmados
    """
    if len(candles_1d) < 20:
        return "NEUTRAL"

    closed = get_closed(candles_1d, "1d")
    if len(closed) < 20:
        return "NEUTRAL"

    closes  = [c["close"] for c in closed[-20:]]
    highs   = [c["high"]  for c in closed[-20:]]
    lows    = [c["low"]   for c in closed[-20:]]

    ema20 = sum(closes) / len(closes)
    price = closes[-1]

    recent_high = max(highs[-10:])
    prev_high   = max(highs[-20:-10])
    recent_low  = min(lows[-10:])
    prev_low    = min(lows[-20:-10])

    hh = recent_high > prev_high
    hl = recent_low  > prev_low
    lh = recent_high < prev_high
    ll = recent_low  < prev_low

    above_ema = price > ema20

    if hh and hl and above_ema:
        return "BULLISH"
    elif lh and ll and not above_ema:
        return "BEARISH"
    else:
        return "NEUTRAL"

# ─── ESTRUCTURA 4H ───────────────────────────────────────────
def get_structure_bias(candles_4h):
    """
    Detecta CHoCH (Change of Character) en 4H:
    Si el precio rompe el último swing high/low con cierre,
    confirma cambio de estructura.
    """
    closed = get_closed(candles_4h, "4h")
    if len(closed) < 15:
        return "NEUTRAL", None

    recent = closed[-15:]
    highs  = [c["high"]  for c in recent]
    lows   = [c["low"]   for c in recent]
    closes = [c["close"] for c in recent]

    swing_high = max(highs[:-3])
    swing_low  = min(lows[:-3])
    last_close = closes[-1]

    if last_close > swing_high:
        return "BULLISH", swing_high
    elif last_close < swing_low:
        return "BEARISH", swing_low
    else:
        return "NEUTRAL", None

# ─── DETECCIÓN PD ARRAYS ─────────────────────────────────────
def find_pd_arrays(candles, interval):
    """
    Detecta Fair Value Gaps y Order Blocks relevantes.
    FVG: gap entre velas consecutivas
    OB: última vela bajista antes de un impulso alcista (y viceversa)
    """
    closed = get_closed(candles, interval)
    fvgs   = []
    obs    = []

    for i in range(1, len(closed) - 1):
        prev = closed[i - 1]
        curr = closed[i]
        nxt  = closed[i + 1]

        # FVG alcista: gap entre low de vela actual y high de vela anterior
        if curr["low"] > prev["high"] and nxt["close"] > curr["high"]:
            fvgs.append({
                "type":   "alcista",
                "top":    curr["low"],
                "bottom": prev["high"],
                "mid":    (curr["low"] + prev["high"]) / 2,
                "time":   curr["time"]
            })

        # FVG bajista
        if curr["high"] < prev["low"] and nxt["close"] < curr["low"]:
            fvgs.append({
                "type":   "bajista",
                "top":    prev["low"],
                "bottom": curr["high"],
                "mid":    (prev["low"] + curr["high"]) / 2,
                "time":   curr["time"]
            })

        # Order Block alcista: última vela bajista antes de impulso alcista fuerte
        if (curr["close"] < curr["open"] and
                nxt["close"] > nxt["open"] and
                abs(nxt["close"] - nxt["open"]) > abs(curr["close"] - curr["open"]) * 1.5):
            obs.append({
                "type":   "alcista",
                "top":    curr["open"],
                "bottom": curr["close"],
                "mid":    (curr["open"] + curr["close"]) / 2,
                "time":   curr["time"]
            })

        # Order Block bajista
        if (curr["close"] > curr["open"] and
                nxt["close"] < nxt["open"] and
                abs(nxt["close"] - nxt["open"]) > abs(curr["close"] - curr["open"]) * 1.5):
            obs.append({
                "type":   "bajista",
                "top":    curr["close"],
                "bottom": curr["open"],
                "mid":    (curr["close"] + curr["open"]) / 2,
                "time":   curr["time"]
            })

    return fvgs[-10:], obs[-5:]

# ─── DETECCIÓN MANIPULACIÓN ──────────────────────────────────
def detect_manipulation(candles, interval):
    closed = get_closed(candles, interval)
    if len(closed) < LOOKBACK + 1:
        return None

    prev_candles = closed[-(LOOKBACK + 1):-1]
    manip        = closed[-1]

    prev_high  = max(c["high"] for c in prev_candles)
    prev_low   = min(c["low"]  for c in prev_candles)
    avg_size   = sum(abs(c["high"] - c["low"]) for c in prev_candles) / len(prev_candles)
    manip_size = abs(manip["high"] - manip["low"])

    if manip_size < avg_size * BODY_MULTIPLIER:
        return None

    body     = abs(manip["close"] - manip["open"])
    body_pct = body / manip_size if manip_size > 0 else 0
    if body_pct < 0.4:
        return None

    manip_type = None
    if manip["low"] < prev_low and manip["close"] > manip["open"]:
        lower_wick = manip["open"] - manip["low"]
        wick_ratio = lower_wick / manip_size if manip_size > 0 else 0
        if wick_ratio >= 0.15:
            manip_type = "ALCISTA"
    elif manip["high"] > prev_high and manip["close"] < manip["open"]:
        upper_wick = manip["high"] - manip["open"]
        wick_ratio = upper_wick / manip_size if manip_size > 0 else 0
        if wick_ratio >= 0.15:
            manip_type = "BAJISTA"

    if not manip_type:
        return None

    wick_ratio = (
        (manip["open"] - manip["low"]) / manip_size
        if manip_type == "ALCISTA"
        else (manip["high"] - manip["open"]) / manip_size
    )

    return {
        "type":       manip_type,
        "candle":     manip,
        "prev_high":  prev_high,
        "prev_low":   prev_low,
        "avg_size":   avg_size,
        "wick_ratio": round(wick_ratio, 2)
    }

# ─── CHOCH CONFIRMATION ──────────────────────────────────────
def confirm_choch(candles, interval, manip_type):
    """
    Verifica que tras la manipulación hay un cambio de estructura
    en la dirección opuesta al spike (confirma la inversión real).
    """
    closed = get_closed(candles, interval)
    if len(closed) < 3:
        return False, None

    manip   = closed[-2] if len(closed) >= 2 else closed[-1]
    post    = closed[-1]

    if manip_type == "ALCISTA":
        # Tras manipulación bajista (sweep low), necesitamos que el precio
        # cierre por encima del open de la vela de manipulación
        choch_level = manip["open"]
        confirmed   = post["close"] > choch_level
    else:
        # Tras manipulación alcista (sweep high), cierre por debajo del open
        choch_level = manip["open"]
        confirmed   = post["close"] < choch_level

    return confirmed, choch_level

# ─── NIVELES FIBONACCI ───────────────────────────────────────
def calculate_levels(detection):
    c        = detection["candle"]
    mt       = detection["type"]
    fib_high = c["high"]
    fib_low  = c["low"]
    rango    = fib_high - fib_low

    if mt == "ALCISTA":
        entry  = fib_low  + rango * 0.705
        sl     = entry    * (1 - SL_PERCENT)
        sd_m1  = fib_high + rango * 1.0
        sd_m2  = fib_high + rango * 2.0
        sd_m25 = fib_high + rango * 2.5
        sd_m4  = fib_high + rango * 4.0
        tp1    = sd_m2
        tp2    = sd_m25
        rr     = (tp1 - entry) / (entry - sl) if (entry - sl) > 0 else 0
    else:
        entry  = fib_high - rango * 0.705
        sl     = entry    * (1 + SL_PERCENT)
        sd_m1  = fib_low  - rango * 1.0
        sd_m2  = fib_low  - rango * 2.0
        sd_m25 = fib_low  - rango * 2.5
        sd_m4  = fib_low  - rango * 4.0
        tp1    = sd_m2
        tp2    = sd_m25
        rr     = (entry - tp1) / (sl - entry) if (sl - entry) > 0 else 0

    return {
        "fib_high": fib_high, "fib_low": fib_low, "rango": rango,
        "entry": entry, "sl": sl,
        "sd_m1": sd_m1, "sd_m2": sd_m2, "sd_m25": sd_m25, "sd_m4": sd_m4,
        "tp1": tp1, "tp2": tp2, "rr": rr
    }

# ─── CONFLUENCIAS ────────────────────────────────────────────
def detect_confluences(candles, levels, detection, interval, fvgs, obs, macro_bias, structure_bias):
    confs  = []
    entry  = levels["entry"]
    tp1    = levels["tp1"]
    tp2    = levels["tp2"]
    tol    = levels["fib_high"] * 0.003
    closed = get_closed(candles, interval)
    mt     = detection["type"]

    # 1 — Alineación con sesgo macro
    if macro_bias == "BULLISH" and mt == "ALCISTA":
        confs.append("Alineado con sesgo Daily BULLISH")
    elif macro_bias == "BEARISH" and mt == "BAJISTA":
        confs.append("Alineado con sesgo Daily BEARISH")
    elif macro_bias != "NEUTRAL":
        confs.append(f"⚠️ Contra sesgo Daily ({macro_bias}) — precaución")

    # 2 — Alineación estructura 4H
    if structure_bias == "BULLISH" and mt == "ALCISTA":
        confs.append("CHoCH 4H confirmado BULLISH")
    elif structure_bias == "BEARISH" and mt == "BAJISTA":
        confs.append("CHoCH 4H confirmado BEARISH")

    # 3 — FVG cerca de entrada o targets
    for fvg in fvgs:
        if abs(fvg["mid"] - entry) < tol * 5:
            confs.append(f"FVG {fvg['type']} en zona de entrada")
        elif abs(fvg["mid"] - tp1) < tol * 5:
            confs.append(f"FVG {fvg['type']} en TP1")

    # 4 — Order Block cerca de entrada
    for ob in obs:
        if abs(ob["mid"] - entry) < tol * 5:
            confs.append(f"Order Block {ob['type']} en zona de entrada")

    # 5 — Equal Highs/Lows cerca de targets
    highs = [c["high"] for c in closed[-30:]]
    lows  = [c["low"]  for c in closed[-30:]]
    if sum(1 for h in highs if abs(h - tp1) < tol) >= 2:
        confs.append("Equal Highs cerca del TP1")
    if sum(1 for l in lows if abs(l - tp1) < tol) >= 2:
        confs.append("Equal Lows cerca del TP1")

    # 6 — Volumen elevado
    avg_vol = sum(c["volume"] for c in closed[-LOOKBACK:]) / LOOKBACK
    vol_ratio = detection["candle"]["volume"] / avg_vol
    if vol_ratio > 1.6:
        confs.append(f"Volumen elevado ({vol_ratio:.1f}x la media)")

    # 7 — Wick pronunciado
    if detection["wick_ratio"] > 0.40:
        confs.append(f"Wick de manipulación pronunciado ({round(detection['wick_ratio']*100)}%)")

    return confs

# ─── SCORE DE CALIDAD ────────────────────────────────────────
def quality_score(confs, rr, choch_confirmed, macro_bias, mt):
    score = 0

    # CHoCH confirmado = +30 pts (lo más importante)
    if choch_confirmed:
        score += 30

    # Alineación macro = +20 pts
    aligned = (macro_bias == "BULLISH" and mt == "ALCISTA") or \
              (macro_bias == "BEARISH" and mt == "BAJISTA")
    if aligned:
        score += 20
    elif macro_bias == "NEUTRAL":
        score += 10

    # RR
    if rr >= 2.5:   score += 20
    elif rr >= 2.0: score += 15
    elif rr >= 1.5: score += 10
    elif rr >= 1.2: score += 5

    # Confluencias (sin contar las de sesgo ya puntuadas)
    pure_confs = [c for c in confs if "sesgo" not in c.lower() and "choch" not in c.lower()]
    score += min(len(pure_confs) * 8, 30)

    return min(score, 100)

def score_emoji(score):
    if score >= 80: return "🔥 SETUP A+"
    if score >= 65: return "✅ SETUP A"
    if score >= 50: return "👍 SETUP B"
    return "⚠️ SETUP C"

# ─── FORMATEAR ALERTA ────────────────────────────────────────
def format_alert(symbol, tf_label, detection, levels, confluences,
                 choch_confirmed, choch_level, macro_bias, structure_bias, score):
    mt        = detection["type"]
    c         = detection["candle"]
    e_type    = "🟢" if mt == "ALCISTA" else "🔴"
    e_dir     = "📈" if mt == "ALCISTA" else "📉"
    rr_emoji  = "✅" if levels["rr"] >= 1.5 else "⚠️"
    time_str  = c["time"].strftime("%d/%m/%Y %H:%M UTC")
    choch_str = f"✅ Confirmado en ${choch_level:,.2f}" if choch_confirmed else "⏳ Pendiente"
    macro_str = {"BULLISH": "🟢 BULLISH", "BEARISH": "🔴 BEARISH", "NEUTRAL": "⚪ NEUTRAL"}.get(macro_bias, "⚪")
    struct_str = {"BULLISH": "🟢 BULLISH", "BEARISH": "🔴 BEARISH", "NEUTRAL": "⚪ NEUTRAL"}.get(structure_bias, "⚪")

    conf_text = ""
    if confluences:
        conf_text = "\n\n🔗 <b>CONFLUENCIAS:</b>\n" + "".join(f"  • {x}\n" for x in confluences)
    else:
        conf_text = "\n\n🔗 <b>CONFLUENCIAS:</b> Ninguna detectada"

    return f"""
{score_emoji(score)} — Score: <b>{score}/100</b>

⚡ <b>MANIPULACIÓN DETECTADA</b> ⚡

{e_type} <b>Par:</b> {symbol} — {tf_label}
{e_dir} <b>Tipo:</b> {mt}
🕯 <b>Vela cerrada:</b> {time_str}

📊 <b>CONTEXTO:</b>
  Sesgo Daily:    {macro_str}
  Estructura 4H:  {struct_str}
  CHoCH:          {choch_str}

📐 <b>FIBONACCI:</b>
  0 → High: <b>${levels['fib_high']:,.2f}</b>
  1 → Low:  <b>${levels['fib_low']:,.2f}</b>
  Rango:    <b>${levels['rango']:,.2f}</b>

📏 <b>SD PROJECTIONS:</b>
  -1   SD: ${levels['sd_m1']:,.2f}
  -2   SD: ${levels['sd_m2']:,.2f}
  -2.5 SD: ${levels['sd_m25']:,.2f}
  -4   SD: ${levels['sd_m4']:,.2f}

🎯 <b>SETUP:</b>
  📍 Entrada OTE: <b>${levels['entry']:,.2f}</b>
  🛑 Stop Loss:   <b>${levels['sl']:,.2f}</b> (2%)
  🎯 TP1 -2 SD:   <b>${levels['tp1']:,.2f}</b>
  🎯 TP2 -2.5 SD: <b>${levels['tp2']:,.2f}</b>
  📊 R:R:         <b>{levels['rr']:.2f}</b> {rr_emoji}
{conf_text}
⚠️ <i>No es consejo financiero. Gestiona tu riesgo.</i>
""".strip()

# ─── TEST ────────────────────────────────────────────────────
def send_test_message():
    candles = get_candles("BTCUSDT", "4h", 80)
    if not candles:
        send_telegram("❌ Error obteniendo datos en el test.")
        return
    price = candles[-1]["close"]
    rango = price * 0.03
    detection = {
        "type": "BAJISTA",
        "candle": {
            "time":   datetime.now(timezone.utc),
            "high":   price + rango, "low": price,
            "open":   price + rango, "close": price,
            "volume": 999999
        },
        "prev_high": price - rango,
        "prev_low":  price - rango * 2,
        "avg_size":  rango * 0.5,
        "wick_ratio": 0.45
    }
    levels      = calculate_levels(detection)
    confluences = [
        "Alineado con sesgo Daily BEARISH",
        "CHoCH 4H confirmado BEARISH",
        "FVG bajista en zona de entrada",
        "Order Block bajista en zona de entrada",
        "Volumen elevado (2.3x la media)",
        "Wick de manipulación pronunciado (45%)"
    ]
    score = quality_score(confluences, levels["rr"], True, "BEARISH", "BAJISTA")
    msg   = format_alert("BTCUSDT", "4H", detection, levels, confluences,
                         True, price + rango * 0.3, "BEARISH", "BEARISH", score)
    send_telegram("🧪 <b>MENSAJE DE PRUEBA</b>\n\n" + msg)
    print("✅ Test enviado")

# ─── MAIN ────────────────────────────────────────────────────
def main():
    print("🤖 Bot TTrades v5 — 4H + 1H, sesgo macro, CHoCH, PD Arrays")

    if TEST_MODE:
        send_test_message()
        return

    send_telegram(
        "🤖 <b>Bot TTrades AMD v5 activado</b>\n\n"
        "Monitorizando <b>BTCUSDT</b> en <b>4H y 1H</b>\n"
        "Con sesgo macro Daily, CHoCH y PD Arrays. ⚡\n"
        "Revisión cada minuto."
    )

    alerted      = {}
    candles_1d   = {}
    candles_4h   = {}
    last_1d_fetch = 0
    last_4h_fetch = 0

    while True:
        now = datetime.now(timezone.utc)
        print(f"\n[{now.strftime('%H:%M:%S')}] Revisando...")

        # Actualizar Daily cada 4 horas
        if time.time() - last_1d_fetch > 14400:
            for sym in SYMBOLS:
                candles_1d[sym] = get_candles(sym, "1d", 80)
            last_1d_fetch = time.time()
            print("  Daily actualizado")

        # Actualizar 4H cada hora
        if time.time() - last_4h_fetch > 3600:
            for sym in SYMBOLS:
                candles_4h[sym] = get_candles(sym, "4h", 80)
            last_4h_fetch = time.time()
            print("  4H actualizado")

        for sym in SYMBOLS:
            c1d = candles_1d.get(sym, [])
            c4h = candles_4h.get(sym, [])

            macro_bias               = get_macro_bias(c1d)
            structure_bias, _        = get_structure_bias(c4h)
            fvgs_4h, obs_4h          = find_pd_arrays(c4h, "4h") if c4h else ([], [])

            for tf in [t for t in TIMEFRAMES if t["interval"] != "1d"]:
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
                        print(f"  {label} {sym}: ya analizada")
                        continue

                    detection = detect_manipulation(candles, interval)
                    if not detection:
                        print(f"  {label} {sym}: sin manipulación")
                        alerted[key] = last_closed_time
                        continue

                    levels = calculate_levels(detection)
                    if levels["rr"] < MIN_RR:
                        print(f"  {label} {sym}: RR insuficiente ({levels['rr']:.2f})")
                        alerted[key] = last_closed_time
                        continue

                    # CHoCH
                    choch_ok, choch_lvl = confirm_choch(candles, interval, detection["type"])

                    # PD Arrays del timeframe actual
                    fvgs, obs = find_pd_arrays(candles, interval)

                    # Confluencias
                    confluences = detect_confluences(
                        candles, levels, detection, interval,
                        fvgs + fvgs_4h, obs + obs_4h,
                        macro_bias, structure_bias
                    )

                    # Score
                    score = quality_score(confluences, levels["rr"], choch_ok, macro_bias, detection["type"])

                    # Solo alertar si score >= 40 (filtra ruido)
                    if score < 40:
                        print(f"  {label} {sym}: score bajo ({score}) — descartado")
                        alerted[key] = last_closed_time
                        continue

                    msg = format_alert(
                        sym, label, detection, levels, confluences,
                        choch_ok, choch_lvl, macro_bias, structure_bias, score
                    )
                    send_telegram(msg)
                    alerted[key] = last_closed_time
                    print(f"  ✅ Alerta {label} {sym} — {detection['type']} score:{score} RR:{levels['rr']:.2f}")

                except Exception as e:
                    print(f"  ❌ Error {label} {sym}: {e}")

        time.sleep(60)

if __name__ == "__main__":
    main()
