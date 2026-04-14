import os
import json
from datetime import datetime, timezone
from google.oauth2 import service_account
from googleapiclient.discovery import build

SHEET_ID   = os.environ.get("SHEET_ID", "")
SHEET_NAME = os.environ.get("SHEET_NAME", "Trades")
CREDS_JSON = os.environ.get("GOOGLE_CREDENTIALS", "")

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]

def get_service():
    if not CREDS_JSON:
        print("❌ GOOGLE_CREDENTIALS no configurado")
        return None
    try:
        creds_dict = json.loads(CREDS_JSON)
        creds      = service_account.Credentials.from_service_account_info(
                        creds_dict, scopes=SCOPES)
        return build("sheets", "v4", credentials=creds, cache_discovery=False)
    except Exception as e:
        print(f"❌ Error Google Sheets auth: {e}")
        return None

def ensure_headers(service):
    """Crea las cabeceras si la hoja está vacía."""
    try:
        result = service.spreadsheets().values().get(
            spreadsheetId=SHEET_ID,
            range=f"{SHEET_NAME}!A1:R1"
        ).execute()
        if result.get("values"):
            return  # Ya tiene cabeceras

        headers = [[
            "Fecha", "Par", "TF", "Tipo", "Score", "Sesion",
            "Fib 1 (High spike)", "Fib 0 (Low acum)", "Rango",
            "EQ 50%", "-1 SD", "-2 SD (TP1)", "-2.5 SD (TP2)", "-4 SD",
            "Entrada OTE", "Stop Loss", "R:R", "Resultado",
            "CHoCH confirmado", "Confluencias", "Notas"
        ]]
        service.spreadsheets().values().append(
            spreadsheetId=SHEET_ID,
            range=f"{SHEET_NAME}!A1",
            valueInputOption="RAW",
            body={"values": headers}
        ).execute()
        print("✅ Cabeceras creadas en Google Sheets")
    except Exception as e:
        print(f"❌ Error creando cabeceras: {e}")

def find_setup_row(service, setup_key):
    """Busca una fila por el setup_key en la columna de notas (U)."""
    try:
        result = service.spreadsheets().values().get(
            spreadsheetId=SHEET_ID,
            range=f"{SHEET_NAME}!U:U"
        ).execute()
        values = result.get("values", [])
        for i, row in enumerate(values):
            if row and row[0] == setup_key:
                return i + 1  # fila en sheets (1-indexed)
        return None
    except:
        return None

def log_detection(detection, levels, score, setup_key):
    """Registra la alerta 1 — manipulacion detectada."""
    service = get_service()
    if not service:
        return

    ensure_headers(service)

    mt       = detection["type"]
    c        = detection["candle"]
    session  = detection.get("session", "—")
    now_str  = datetime.now(timezone.utc).strftime("%d/%m/%Y %H:%M")

    row = [[
        now_str,                          # Fecha
        "BTCUSDT",                        # Par
        "1H/4H",                          # TF
        mt,                               # Tipo
        score,                            # Score
        session,                          # Sesion
        round(levels["fib_1"], 2),        # Fib 1
        round(levels["fib_0"], 2),        # Fib 0
        round(levels["rango"], 2),        # Rango
        round(levels["eq"], 2),           # EQ 50%
        round(levels["sd_m1"], 2),        # -1 SD
        round(levels["sd_m2"], 2),        # -2 SD TP1
        round(levels["sd_m25"], 2),       # -2.5 SD TP2
        round(levels["sd_m4"], 2),        # -4 SD
        round(levels["entry"], 2),        # Entrada OTE
        round(levels["sl"], 2),           # Stop Loss
        round(levels["rr"], 2),           # R:R
        "Esperando CHoCH",                # Resultado
        "No",                             # CHoCH confirmado
        "",                               # Confluencias
        setup_key                         # Notas (clave para actualizar despues)
    ]]

    try:
        service.spreadsheets().values().append(
            spreadsheetId=SHEET_ID,
            range=f"{SHEET_NAME}!A:U",
            valueInputOption="RAW",
            insertDataOption="INSERT_ROWS",
            body={"values": row}
        ).execute()
        print(f"✅ Alerta 1 registrada en Google Sheets")
    except Exception as e:
        print(f"❌ Error escribiendo en Sheets: {e}")

def update_choch(setup_key, choch_price, confluences, current_price):
    """Actualiza la fila cuando el CHoCH se confirma — alerta 2."""
    service = get_service()
    if not service:
        return

    row_num = find_setup_row(service, setup_key)
    if not row_num:
        print(f"⚠️ No encontré la fila para {setup_key}")
        return

    conf_str = " | ".join(confluences) if confluences else "Ninguna"

    try:
        # Actualizar columnas: Resultado, CHoCH, Confluencias
        service.spreadsheets().values().update(
            spreadsheetId=SHEET_ID,
            range=f"{SHEET_NAME}!R{row_num}:T{row_num}",
            valueInputOption="RAW",
            body={"values": [[
                "Abierto",   # Resultado
                "Si",        # CHoCH confirmado
                conf_str     # Confluencias
            ]]}
        ).execute()
        print(f"✅ CHoCH actualizado en Google Sheets fila {row_num}")
    except Exception as e:
        print(f"❌ Error actualizando CHoCH: {e}")

def update_result(setup_key, resultado, r_real=""):
    """Actualiza el resultado final del trade (lo hace el usuario manualmente o futuro bot)."""
    service = get_service()
    if not service:
        return

    row_num = find_setup_row(service, setup_key)
    if not row_num:
        return

    try:
        service.spreadsheets().values().update(
            spreadsheetId=SHEET_ID,
            range=f"{SHEET_NAME}!R{row_num}",
            valueInputOption="RAW",
            body={"values": [[resultado]]}
        ).execute()
        print(f"✅ Resultado actualizado: {resultado}")
    except Exception as e:
        print(f"❌ Error actualizando resultado: {e}")
