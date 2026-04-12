import os
import time
import requests
from datetime import datetime, timezone

# ============================================================
# CONFIGURACIÓN
# ============================================================
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN")
CHAT_ID = os.environ.get("CHAT_ID")
TEST_MODE = os.environ.get("TEST_MODE", "false").lower() == "true"
CHECK_INTERVAL = 60 * 15  # cada 15 minutos

LOOKBACK = 10
BODY_MULTIPLIER = 1.8
SL_PERCENT = 0.02

SYMBOLS = ["BTCUSDT"]

# ============================================================
# TELEGRAM
# ============================================================
def send_telegram(message):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {
        "chat_id": CHAT_ID,
        "text": message,
        "parse_mode": "HTML"
    }
    try:
        r = requests.post(url, json=payload, timeout=10)
        if r.status_code == 200:
            print("✅ Mensaje enviado a Telegram")
        else:
            print(f"❌ Error Telegram: {r.text}")
    except Exception as e:
        print(f"❌ Error enviando mensaje: {e}")

# ============================================================
# VELAS BINANCE
# ============================================================
def get_candles(symbol, interval="4h", limit=50):
    url = "https://api.binance.com/api/v3/klines"
    params = {"symbol": symbol, "interval": interval, "limit": limit}
    try:
        response = requests.get(url, params=params, timeout=10)
        data = response.json()
        candles = []
        for c in data:
            candles.append({
                "time":   datetime.fromtimestamp(c[0] / 1000, tz=timezone.utc),
                "open":   float(c[1]),
                "high":   float(c[2]),
                "low":    float(c[3]),
                "close":  float(c[4]),
                "volume": float(c[5])
            })
        return candles
    except Exception as e:
        print(f"❌ Error obteniendo velas {symbol}: {e}")
        return []

# ============================================================
# DETECCIÓN DE MANIPULACIÓN
# ============================================================
def detect_manipulation(candles):
    if len(candles) < LOOKBACK + 2:
        return None

    prev_candles = candles[-(LOOKBACK + 2):-2]
    manip_candle = candles[-2]

    highs = [c["high"] for c in prev_candles]
    lows  = [c["low"]  for c in prev_candles]
    prev_high = max(highs)
    prev_low  = min(lows)

    avg_size   = sum(abs(c["high"] - c["low"]) for c in prev_candles) / len(prev_candles)
    manip_size = abs(manip_candle["high"] - manip_candle["low"])

    if manip_size < avg_size * BODY_MULTIPLIER:
        return None

    manipulation_type = None

    if (manip_candle["low"] < prev_low and
            manip_candle["close"] > manip_candle["open"]):
        manipulation_type = "ALCISTA"
    elif (manip_candle["high"] > prev_high and
            manip_candle["close"] < manip_candle["open"]):
        manipulation_type = "BAJISTA"

    if not manipulation_type:
        return None

    return {
        "type":       manipulation_type,
        "candle":     manip_candle,
        "prev_high":  prev_high,
        "prev_low":   prev_low,
        "avg_size":   avg_size
    }

# ============================================================
# NIVELES FIBONACCI + SD
# ============================================================
def calculate_levels(detection):
    candle     = detection["candle"]
    manip_type = detection["type"]
    fib_high   = candle["high"]
    fib_low    = candle["low"]
    rango      = fib_high - fib_low

    if manip_type == "ALCISTA":
        entry       = fib_low + (rango * 0.705)
        sl          = entry * (1 - SL_PERCENT)
        sd_minus1   = fib_high + rango * 1.0
        sd_minus2   = fib_high + rango * 2.0
        sd_minus2_5 = fib_high + rango * 2.5
        sd_minus4   = fib_high + rango * 4.0
        tp1, tp2    = sd_minus2, sd_minus2_5
        rr = (tp1 - entry) / (entry - sl) if (entry - sl) > 0 else 0
    else:
        entry       = fib_high - (rango * 0.705)
        sl          = entry * (1 + SL_PERCENT)
        sd_minus1   = fib_low - rango * 1.0
        sd_minus2   = fib_low - rango * 2.0
        sd_minus2_5 = fib_low - rango * 2.5
        sd_minus4   = fib_low - rango * 4.0
        tp1, tp2    = sd_minus2, sd_minus2_5
        rr = (entry - tp1) / (sl - entry) if (sl - entry) > 0 else 0

    return {
        "fib_high": fib_high, "fib_low": fib_low,
        "entry": entry, "sl": sl,
        "sd_minus1": sd_minus1, "sd_minus2": sd_minus2,
        "sd_minus2_5": sd_minus2_5, "sd_minus4": sd_minus4,
        "tp1": tp1, "tp2": tp2, "rr": rr
    }

# ============================================================
# CONFLUENCIAS
# ============================================================
def detect_confluences(candles, levels, detection):
    confluences = []
    tp1         = levels["tp1"]
    tp2         = levels["tp2"]
    entry       = levels["entry"]
    tolerance   = levels["fib_high"] * 0.002

    highs = [c["high"] for c in candles[-20:]]
    lows  = [c["low"]  for c in candles[-20:]]

    if any(abs(h - tp1) < tolerance or abs(h - tp2) < tolerance for h in highs):
        confluences.append("Equal Highs cerca del target")
    if any(abs(l - tp1) < tolerance or abs(l - tp2) < tolerance for l in lows):
        confluences.append("Equal Lows cerca del target")

    for i in range(1, len(candles) - 1):
        c_prev = candles[i - 1]
        c_curr = candles[i]
        if c_curr["low"] > c_prev["high"]:
            fvg_mid = (c_curr["low"] + c_prev["high"]) / 2
            if abs(fvg_mid - entry) < tolerance * 3:
                confluences.append("FVG (Fair Value Gap) cerca de la entrada")
                break
        if c_curr["high"] < c_prev["low"]:
            fvg_mid = (c_curr["high"] + c_prev["low"]) / 2
            if abs(fvg_mid - entry) < tolerance * 3:
                confluences.append("FVG (Fair Value Gap) cerca de la entrada")
                break

    avg_vol = sum(c["volume"] for c in candles[-LOOKBACK:]) / LOOKBACK
    if detection["candle"]["volume"] > avg_vol * 1.5:
        confluences.append("Volumen elevado en la manipulación")

    return confluences

# ============================================================
# FORMATEAR ALERTA
# ============================================================
def format_alert(symbol, detection, levels, confluences):
    manip_type   = detection["type"]
    candle       = detection["candle"]
    emoji_type   = "🟢" if manip_type == "ALCISTA" else "🔴"
    emoji_dir    = "📈" if manip_type == "ALCISTA" else "📉"
    rr_emoji     = "✅" if levels["rr"] >= 1.5 else "⚠️"
    time_str     = candle["time"].strftime("%d/%m/%Y %H:%M UTC")

    if confluences:
        conf_text = "\n\n🔗 <b>CONFLUENCIAS:</b>\n" + "".join(f"  ✅ {c}\n" for c in confluences)
    else:
        conf_text = "\n\n🔗 <b>CONFLUENCIAS:</b> Ninguna detectada"

    return f"""
⚡ <b>MANIPULACIÓN DETECTADA</b> ⚡

{emoji_type} <b>Par:</b> {symbol} — 4H
{emoji_dir} <b>Tipo:</b> {manip_type}
🕯️ <b>Vela:</b> {time_str}

📐 <b>FIBONACCI:</b>
  0 (High): <b>${levels['fib_high']:,.2f}</b>
  1 (Low):  <b>${levels['fib_low']:,.2f}</b>

📏 <b>SD PROJECTIONS:</b>
  -1   SD: ${levels['sd_minus1']:,.2f}
  -2   SD: ${levels['sd_minus2']:,.2f}
  -2.5 SD: ${levels['sd_minus2_5']:,.2f}
  -4   SD: ${levels['sd_minus4']:,.2f}

🎯 <b>SETUP:</b>
  📍 Entrada (OTE): <b>${levels['entry']:,.2f}</b>
  🛑 Stop Loss:     <b>${levels['sl']:,.2f}</b> (2%)
  🎯 TP1 (-2 SD):   <b>${levels['tp1']:,.2f}</b>
  🎯 TP2 (-2.5 SD): <b>${levels['tp2']:,.2f}</b>
  📊 R:R: <b>{levels['rr']:.2f}</b> {rr_emoji}
{conf_text}
⚠️ <i>No es consejo financiero. Gestiona siempre tu riesgo.</i>
""".strip()

# ============================================================
# MENSAJE DE PRUEBA
# ============================================================
def send_test_message():
    candles = get_candles("BTCUSDT")
    if not candles:
        send_telegram("❌ Error obteniendo datos de Binance en el test.")
        return

    price = candles[-1]["close"]
    rango = price * 0.03  # simulamos rango del 3%

    detection = {
        "type": "BAJISTA",
        "candle": {
            "time":   datetime.now(timezone.utc),
            "high":   price + rango,
            "low":    price,
            "open":   price + rango,
            "close":  price,
            "volume": 999999
        },
        "prev_high": price - rango,
        "prev_low":  price - rango * 2,
        "avg_size":  rango * 0.5
    }

    levels      = calculate_levels(detection)
    confluences = ["FVG (Fair Value Gap) cerca de la entrada", "Volumen elevado en la manipulación"]
    message     = format_alert("BTCUSDT", detection, levels, confluences)

    send_telegram("🧪 <b>MENSAJE DE PRUEBA</b> — así se verán las alertas reales:\n\n" + message)
    print("✅ Mensaje de prueba enviado")

# ============================================================
# LOOP PRINCIPAL
# ============================================================
def main():
    print("🤖 Bot TTrades AMD iniciado...")

    if TEST_MODE:
        print("🧪 Modo test activado — enviando mensaje de prueba...")
        send_test_message()
        print("✅ Test completado. Cambia TEST_MODE a false para modo normal.")
        return

    send_telegram("🤖 <b>Bot TTrades AMD activado</b>\n\nMonitorizando BTCUSDT en 4H.\nTe avisaré cuando detecte una manipulación. ⚡")

    analyzed_candles = {}

    while True:
        for symbol in SYMBOLS:
            try:
                print(f"[{datetime.now(timezone.utc).strftime('%H:%M:%S')}] Analizando {symbol}...")
                candles = get_candles(symbol)
                if not candles:
                    continue

                last_candle_time = str(candles[-2]["time"])
                if analyzed_candles.get(symbol) == last_candle_time:
                    print(f"  Ya analizada esta vela de {symbol}")
                    continue

                detection = detect_manipulation(candles)

                if detection:
                    levels      = calculate_levels(detection)
                    confluences = detect_confluences(candles, levels, detection)
                    if levels["rr"] >= 1.0:
                        message = format_alert(symbol, detection, levels, confluences)
                        send_telegram(message)
                        print(f"✅ Alerta enviada para {symbol}")
                        analyzed_candles[symbol] = last_candle_time
                    else:
                        print(f"⚠️ Manipulación en {symbol} pero RR insuficiente ({levels['rr']:.2f})")
                else:
                    print(f"  Sin manipulación en {symbol}")

            except Exception as e:
                print(f"❌ Error analizando {symbol}: {e}")

        time.sleep(CHECK_INTERVAL)

if __name__ == "__main__":
    main()
