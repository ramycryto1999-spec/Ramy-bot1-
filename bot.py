import os
import time
import requests
from datetime import datetime, timezone

# ============================================================
# CONFIGURACIÓN — se leen desde variables de entorno (Railway)
# ============================================================
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN")
CHAT_ID = os.environ.get("CHAT_ID")
CHECK_INTERVAL = 60 * 60 * 4  # cada 4 horas (cierre de vela 4H)

# ============================================================
# PARÁMETROS DE DETECCIÓN
# ============================================================
LOOKBACK = 10          # velas previas para detectar sweep
BODY_MULTIPLIER = 1.8  # la vela de manipulación debe ser 1.8x la media
SL_PERCENT = 0.02      # 2% sobre el precio de entrada

# ============================================================
# PARES A MONITORIZAR
# ============================================================
SYMBOLS = ["BTCUSDT", "TOTAL"]  # BTC + Market Cap crypto

# ============================================================
# FUNCIONES DE TELEGRAM
# ============================================================
def send_telegram(message):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {
        "chat_id": CHAT_ID,
        "text": message,
        "parse_mode": "HTML"
    }
    try:
        requests.post(url, json=payload, timeout=10)
    except Exception as e:
        print(f"Error enviando mensaje: {e}")

# ============================================================
# OBTENER VELAS DE BINANCE (4H)
# ============================================================
def get_candles(symbol, interval="4h", limit=50):
    url = "https://api.binance.com/api/v3/klines"
    params = {
        "symbol": symbol,
        "interval": interval,
        "limit": limit
    }
    try:
        response = requests.get(url, params=params, timeout=10)
        data = response.json()
        candles = []
        for c in data:
            candles.append({
                "time": datetime.fromtimestamp(c[0] / 1000, tz=timezone.utc),
                "open":  float(c[1]),
                "high":  float(c[2]),
                "low":   float(c[3]),
                "close": float(c[4]),
                "volume": float(c[5])
            })
        return candles
    except Exception as e:
        print(f"Error obteniendo velas {symbol}: {e}")
        return []

# ============================================================
# OBTENER MARKET CAP TOTAL (via CoinGecko)
# ============================================================
def get_market_cap_candles():
    # CoinGecko no da velas OHLC de market cap gratis en 4H
    # Usamos BTC como proxy de la market cap total (alta correlación)
    # En producción se puede sustituir por una API premium
    return get_candles("BTCUSDT")

# ============================================================
# DETECTAR MANIPULACIÓN
# ============================================================
def detect_manipulation(candles):
    if len(candles) < LOOKBACK + 2:
        return None

    # La última vela cerrada es candles[-2] (candles[-1] es la vela actual abierta)
    prev_candles = candles[-(LOOKBACK + 2):-2]
    manip_candle = candles[-2]
    confirm_candle = candles[-1]

    highs = [c["high"] for c in prev_candles]
    lows  = [c["low"]  for c in prev_candles]

    prev_high = max(highs)
    prev_low  = min(lows)

    # Tamaño medio de las velas previas
    avg_size = sum(abs(c["high"] - c["low"]) for c in prev_candles) / len(prev_candles)
    manip_size = abs(manip_candle["high"] - manip_candle["low"])

    # ¿La vela es suficientemente grande?
    if manip_size < avg_size * BODY_MULTIPLIER:
        return None

    manipulation_type = None

    # MANIPULACIÓN ALCISTA — sweep de mínimos + cierre alcista
    if (manip_candle["low"] < prev_low and
        manip_candle["close"] > manip_candle["open"] and
        confirm_candle["close"] > confirm_candle["open"]):
        manipulation_type = "ALCISTA"

    # MANIPULACIÓN BAJISTA — sweep de máximos + cierre bajista
    elif (manip_candle["high"] > prev_high and
          manip_candle["close"] < manip_candle["open"] and
          confirm_candle["close"] < confirm_candle["open"]):
        manipulation_type = "BAJISTA"

    if not manipulation_type:
        return None

    return {
        "type": manipulation_type,
        "candle": manip_candle,
        "confirm_candle": confirm_candle,
        "prev_high": prev_high,
        "prev_low": prev_low,
        "avg_size": avg_size
    }

# ============================================================
# CALCULAR FIBONACCI + SD PROJECTIONS
# ============================================================
def calculate_levels(detection):
    candle = detection["candle"]
    manip_type = detection["type"]

    if manip_type == "ALCISTA":
        # Fib de Low (1) a High (0) — distribución sube
        fib_high = candle["high"]   # nivel 0
        fib_low  = candle["low"]    # nivel 1
        rango = fib_high - fib_low

        # Entrada óptima: retroceso al 50% (EQ) del rango de manipulación
        # zona entre 0.5 y 0.79 del rango desde el low
        entry = fib_low + (rango * 0.705)  # zona OTE ~70% retroceso

        sl = entry * (1 - SL_PERCENT)

        sd_minus1   = fib_high + rango * 1.0
        sd_minus2   = fib_high + rango * 2.0
        sd_minus2_5 = fib_high + rango * 2.5
        sd_minus4   = fib_high + rango * 4.0

        tp1 = sd_minus2
        tp2 = sd_minus2_5

        rr = (tp1 - entry) / (entry - sl) if (entry - sl) > 0 else 0

    else:  # BAJISTA
        fib_high = candle["high"]   # nivel 0
        fib_low  = candle["low"]    # nivel 1
        rango = fib_high - fib_low

        # Entrada óptima: retroceso al OTE (~70%) desde el high
        entry = fib_high - (rango * 0.705)

        sl = entry * (1 + SL_PERCENT)

        sd_minus1   = fib_low - rango * 1.0
        sd_minus2   = fib_low - rango * 2.0
        sd_minus2_5 = fib_low - rango * 2.5
        sd_minus4   = fib_low - rango * 4.0

        tp1 = sd_minus2
        tp2 = sd_minus2_5

        rr = (entry - tp1) / (sl - entry) if (sl - entry) > 0 else 0

    return {
        "fib_high":     fib_high,
        "fib_low":      fib_low,
        "entry":        entry,
        "sl":           sl,
        "sd_minus1":    sd_minus1,
        "sd_minus2":    sd_minus2,
        "sd_minus2_5":  sd_minus2_5,
        "sd_minus4":    sd_minus4,
        "tp1":          tp1,
        "tp2":          tp2,
        "rr":           rr
    }

# ============================================================
# DETECTAR CONFLUENCIAS
# ============================================================
def detect_confluences(candles, levels, detection):
    confluences = []
    tp1 = levels["tp1"]
    tp2 = levels["tp2"]
    tolerance = levels["fib_high"] * 0.002  # 0.2% de tolerancia

    # 1 — Equal Highs/Lows cerca de los targets
    highs = [c["high"] for c in candles[-20:]]
    lows  = [c["low"]  for c in candles[-20:]]

    equal_highs = [h for h in highs if abs(h - tp1) < tolerance or abs(h - tp2) < tolerance]
    equal_lows  = [l for l in lows  if abs(l - tp1) < tolerance or abs(l - tp2) < tolerance]

    if equal_highs:
        confluences.append("Equal Highs cerca del target")
    if equal_lows:
        confluences.append("Equal Lows cerca del target")

    # 2 — Fair Value Gap (FVG) cerca de la entrada
    entry = levels["entry"]
    for i in range(1, len(candles) - 1):
        c_prev = candles[i - 1]
        c_curr = candles[i]
        c_next = candles[i + 1]
        # FVG alcista: gap entre low de vela actual y high de vela anterior
        if c_curr["low"] > c_prev["high"]:
            fvg_mid = (c_curr["low"] + c_prev["high"]) / 2
            if abs(fvg_mid - entry) < tolerance * 3:
                confluences.append("FVG (Fair Value Gap) cerca de la entrada")
                break
        # FVG bajista
        if c_curr["high"] < c_prev["low"]:
            fvg_mid = (c_curr["high"] + c_prev["low"]) / 2
            if abs(fvg_mid - entry) < tolerance * 3:
                confluences.append("FVG (Fair Value Gap) cerca de la entrada")
                break

    # 3 — Volumen elevado en la vela de manipulación
    avg_vol = sum(c["volume"] for c in candles[-LOOKBACK:]) / LOOKBACK
    if detection["candle"]["volume"] > avg_vol * 1.5:
        confluences.append("Volumen elevado en la manipulación")

    return confluences

# ============================================================
# FORMATEAR Y ENVIAR ALERTA
# ============================================================
def format_alert(symbol, detection, levels, confluences):
    manip_type = detection["type"]
    candle = detection["candle"]
    emoji_type = "🟢" if manip_type == "ALCISTA" else "🔴"
    emoji_dir  = "📈" if manip_type == "ALCISTA" else "📉"
    rr_emoji   = "✅" if levels["rr"] >= 1.5 else "⚠️"

    time_str = candle["time"].strftime("%d/%m/%Y %H:%M UTC")

    confluences_text = ""
    if confluences:
        confluences_text = "\n\n🔗 <b>CONFLUENCIAS:</b>\n"
        for c in confluences:
            confluences_text += f"  ✅ {c}\n"
    else:
        confluences_text = "\n\n🔗 <b>CONFLUENCIAS:</b> Ninguna detectada"

    message = f"""
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
{confluences_text}
⚠️ <i>No es consejo financiero. Gestiona siempre tu riesgo.</i>
"""
    return message.strip()

# ============================================================
# LOOP PRINCIPAL
# ============================================================
def main():
    print("🤖 Bot TTrades AMD iniciado...")
    send_telegram("🤖 <b>Bot TTrades AMD activado</b>\n\nMonitorizando BTCUSDT y Market Cap en 4H.\nTe avisaré cuando detecte una manipulación.")

    analyzed_candles = {}  # para no repetir alertas de la misma vela

    while True:
        for symbol in SYMBOLS:
            try:
                print(f"[{datetime.now(timezone.utc).strftime('%H:%M:%S')}] Analizando {symbol}...")

                if symbol == "TOTAL":
                    candles = get_market_cap_candles()
                else:
                    candles = get_candles(symbol)

                if not candles:
                    continue

                # Evitar alertas duplicadas de la misma vela
                last_candle_time = str(candles[-2]["time"])
                if analyzed_candles.get(symbol) == last_candle_time:
                    continue

                detection = detect_manipulation(candles)

                if detection:
                    levels = calculate_levels(detection)
                    confluences = detect_confluences(candles, levels, detection)

                    # Solo alertar si RR >= 1.0 mínimo
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
                print(f"Error analizando {symbol}: {e}")

        # Esperar 15 minutos antes del próximo chequeo
        # (no esperamos 4H enteras para no perdernos el cierre exacto)
        time.sleep(60 * 15)

if __name__ == "__main__":
    main()
