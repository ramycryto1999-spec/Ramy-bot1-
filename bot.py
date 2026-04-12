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

TIMEFRAMES = [
    {"interval": "4h",  "label": "4H",  "lookback_candles": 50, "check_seconds": 60},
    {"interval": "1h",  "label": "1H",  "lookback_candles": 50, "check_seconds": 60},
]

SYMBOLS = ["BTCUSDT"]

def send_telegram(message):
    url     = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {"chat_id": CHAT_ID, "text": message, "parse_mode": "HTML"}
    try:
        r = requests.post(url, json=payload, timeout=10)
        if r.status_code == 200:
            print("✅ Telegram enviado")
        else:
            print(f"❌ Telegram error: {r.text}")
    except Exception as e:
        print(f"❌ Error Telegram: {e}")

def get_candles(symbol, interval="4h", limit=60):
    url    = "https://api.binance.com/api/v3/klines"
    params = {"symbol": symbol, "interval": interval, "limit": limit}
    try:
        r    = requests.get(url, params=params, timeout=10)
        data = r.json()
        return [{
            "time":   datetime.fromtimestamp(c[0] / 1000, tz=timezone.utc),
            "open":   float(c[1]),
            "high":   float(c[2]),
            "low":    float(c[3]),
            "close":  float(c[4]),
            "volume": float(c[5])
        } for c in data]
    except Exception as e:
        print(f"❌ Error velas {symbol} {interval}: {e}")
        return []

def candle_closed(candle, interval):
    now   = datetime.now(timezone.utc)
    secs  = {"1h": 3600, "4h": 14400}.get(interval, 3600)
    close = candle["time"].timestamp() + secs
    return now.timestamp() >= close

def detect_manipulation(candles, interval):
    if len(candles) < LOOKBACK + 2:
        return None

    closed = [c for c in candles if candle_closed(c, interval)]
    if len(closed) < LOOKBACK + 1:
        return None

    prev_candles = closed[-(LOOKBACK + 1):-1]
    manip_candle = closed[-1]

    prev_high = max(c["high"] for c in prev_candles)
    prev_low  = min(c["low"]  for c in prev_candles)
    avg_size  = sum(abs(c["high"] - c["low"]) for c in prev_candles) / len(prev_candles)
    manip_size = abs(manip_candle["high"] - manip_candle["low"])

    if manip_size < avg_size * BODY_MULTIPLIER:
        return None

    body      = abs(manip_candle["close"] - manip_candle["open"])
    body_pct  = body / manip_size if manip_size > 0 else 0
    if body_pct < 0.4:
        return None

    manip_type = None
    if manip_candle["low"] < prev_low and manip_candle["close"] > manip_candle["open"]:
        manip_type = "ALCISTA"
    elif manip_candle["high"] > prev_high and manip_candle["close"] < manip_candle["open"]:
        manip_type = "BAJISTA"

    if not manip_type:
        return None

    wick_ratio = 0
    if manip_type == "ALCISTA":
        lower_wick  = manip_candle["open"] - manip_candle["low"]
        wick_ratio  = lower_wick / manip_size if manip_size > 0 else 0
        if wick_ratio < 0.15:
            return None
    else:
        upper_wick = manip_candle["high"] - manip_candle["open"]
        wick_ratio = upper_wick / manip_size if manip_size > 0 else 0
        if wick_ratio < 0.15:
            return None

    return {
        "type":      manip_type,
        "candle":    manip_candle,
        "prev_high": prev_high,
        "prev_low":  prev_low,
        "avg_size":  avg_size,
        "wick_ratio": round(wick_ratio, 2)
    }

def calculate_levels(detection):
    c          = detection["candle"]
    mt         = detection["type"]
    fib_high   = c["high"]
    fib_low    = c["low"]
    rango      = fib_high - fib_low

    if mt == "ALCISTA":
        entry       = fib_low  + rango * 0.705
        sl          = entry    * (1 - SL_PERCENT)
        sd_m1       = fib_high + rango * 1.0
        sd_m2       = fib_high + rango * 2.0
        sd_m25      = fib_high + rango * 2.5
        sd_m4       = fib_high + rango * 4.0
        tp1, tp2    = sd_m2, sd_m25
        rr          = (tp1 - entry) / (entry - sl) if (entry - sl) > 0 else 0
    else:
        entry       = fib_high - rango * 0.705
        sl          = entry    * (1 + SL_PERCENT)
        sd_m1       = fib_low  - rango * 1.0
        sd_m2       = fib_low  - rango * 2.0
        sd_m25      = fib_low  - rango * 2.5
        sd_m4       = fib_low  - rango * 4.0
        tp1, tp2    = sd_m2, sd_m25
        rr          = (entry - tp1) / (sl - entry) if (sl - entry) > 0 else 0

    return {
        "fib_high": fib_high, "fib_low": fib_low, "rango": rango,
        "entry": entry, "sl": sl,
        "sd_m1": sd_m1, "sd_m2": sd_m2, "sd_m25": sd_m25, "sd_m4": sd_m4,
        "tp1": tp1, "tp2": tp2, "rr": rr
    }

def detect_confluences(candles, levels, detection):
    confs     = []
    entry     = levels["entry"]
    tp1       = levels["tp1"]
    tp2       = levels["tp2"]
    tol       = levels["fib_high"] * 0.002
    closed    = [c for c in candles if candle_closed(c, "4h")]

    highs = [c["high"] for c in closed[-30:]]
    lows  = [c["low"]  for c in closed[-30:]]

    eq_highs = sum(1 for h in highs if abs(h - tp1) < tol or abs(h - tp2) < tol)
    eq_lows  = sum(1 for l in lows  if abs(l - tp1) < tol or abs(l - tp2) < tol)
    if eq_highs >= 2:
        confs.append("Equal Highs cerca del target")
    if eq_lows >= 2:
        confs.append("Equal Lows cerca del target")

    for i in range(1, len(closed) - 1):
        prev, curr = closed[i-1], closed[i]
        if curr["low"] > prev["high"]:
            mid = (curr["low"] + prev["high"]) / 2
            if abs(mid - entry) < tol * 4:
                confs.append("FVG alcista cerca de la entrada")
                break
        if curr["high"] < prev["low"]:
            mid = (curr["high"] + prev["low"]) / 2
            if abs(mid - entry) < tol * 4:
                confs.append("FVG bajista cerca de la entrada")
                break

    avg_vol = sum(c["volume"] for c in closed[-LOOKBACK:]) / LOOKBACK
    if detection["candle"]["volume"] > avg_vol * 1.6:
        confs.append(f"Volumen elevado ({round(detection['candle']['volume']/avg_vol, 1)}x la media)")

    if detection["wick_ratio"] > 0.40:
        confs.append(f"Wick de manipulacion pronunciado ({round(detection['wick_ratio']*100)}%)")

    return confs

def format_alert(symbol, tf_label, detection, levels, confluences):
    mt         = detection["type"]
    c          = detection["candle"]
    e_type     = "🟢" if mt == "ALCISTA" else "🔴"
    e_dir      = "📈" if mt == "ALCISTA" else "📉"
    rr_emoji   = "✅" if levels["rr"] >= 1.5 else "⚠️"
    time_str   = c["time"].strftime("%d/%m/%Y %H:%M UTC")
    conf_text  = ("\n\n🔗 <b>CONFLUENCIAS:</b>\n" + "".join(f"  ✅ {x}\n" for x in confluences)
                  if confluences else "\n\n🔗 <b>CONFLUENCIAS:</b> Ninguna detectada")

    return f"""
⚡ <b>MANIPULACIÓN DETECTADA</b> ⚡

{e_type} <b>Par:</b> {symbol} — {tf_label}
{e_dir} <b>Tipo:</b> {mt}
🕯 <b>Vela cerrada:</b> {time_str}

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

def send_test_message():
    candles = get_candles("BTCUSDT", "4h", 60)
    if not candles:
        send_telegram("❌ Error obteniendo datos en el test.")
        return
    price = candles[-1]["close"]
    rango = price * 0.03
    detection = {
        "type": "BAJISTA",
        "candle": {
            "time": datetime.now(timezone.utc),
            "high": price + rango, "low": price,
            "open": price + rango, "close": price,
            "volume": 999999
        },
        "prev_high": price - rango,
        "prev_low":  price - rango * 2,
        "avg_size":  rango * 0.5,
        "wick_ratio": 0.45
    }
    levels     = calculate_levels(detection)
    confluences = ["FVG bajista cerca de la entrada", "Volumen elevado (2.1x la media)", "Wick pronunciado (45%)"]
    msg        = format_alert("BTCUSDT", "4H", detection, levels, confluences)
    send_telegram("🧪 <b>MENSAJE DE PRUEBA</b>\n\n" + msg)
    print("✅ Test enviado")

def main():
    print("🤖 Bot TTrades AMD iniciado — 4H + 1H, revisión cada minuto")

    if TEST_MODE:
        print("🧪 Modo test...")
        send_test_message()
        return

    send_telegram(
        "🤖 <b>Bot TTrades AMD activado</b>\n\n"
        "Monitorizando <b>BTCUSDT</b> en <b>4H y 1H</b>.\n"
        "Revisión cada minuto — solo alertas en cierre de vela real. ⚡"
    )

    alerted = {}

    while True:
        now = datetime.now(timezone.utc)
        print(f"\n[{now.strftime('%H:%M:%S')}] Revisando...")

        for tf in TIMEFRAMES:
            interval = tf["interval"]
            label    = tf["label"]

            for symbol in SYMBOLS:
                key = f"{symbol}_{interval}"
                try:
                    candles = get_candles(symbol, interval, tf["lookback_candles"])
                    if not candles or len(candles) < LOOKBACK + 2:
                        continue

                    closed_candles = [c for c in candles if candle_closed(c, interval)]
                    if not closed_candles:
                        continue

                    last_closed_time = str(closed_candles[-1]["time"])

                    if alerted.get(key) == last_closed_time:
                        print(f"  {label} {symbol}: vela ya analizada")
                        continue

                    detection = detect_manipulation(candles, interval)

                    if detection:
                        levels      = calculate_levels(detection)
                        confluences = detect_confluences(candles, levels, detection)

                        if levels["rr"] >= 1.2:
                            msg = format_alert(symbol, label, detection, levels, confluences)
                            send_telegram(msg)
                            alerted[key] = last_closed_time
                            print(f"  ✅ Alerta {label} {symbol} enviada — {detection['type']} RR:{levels['rr']:.2f}")
                        else:
                            print(f"  ⚠️ {label} {symbol}: manipulación detectada pero RR insuficiente ({levels['rr']:.2f})")
                            alerted[key] = last_closed_time
                    else:
                        print(f"  {label} {symbol}: sin manipulación")

                except Exception as e:
                    print(f"  ❌ Error {label} {symbol}: {e}")

        time.sleep(60)

if __name__ == "__main__":
    main()
