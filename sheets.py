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
    """Verifica cabeceras — el journal ya las tiene del xlsx."""
    pass  # Las cabeceras ya existen en el journal

def find_setup_row(service, setup_key):
    """Busca una fila por el setup_key en la columna P (Notas)."""
    try:
        result = service.spreadsheets().values().get(
            spreadsheetId=SHEET_ID,
            range=f"{SHEET_NAME}!P:P"
        ).execute()
        values = result.get("values", [])
        for i, row in enumerate(values):
            if row and row[0] == setup_key:
                return i + 1
        return None
    except:
        return None

def log_detection(detection, levels, score, setup_key):
    """Registra la alerta 1 — manipulacion detectada."""
    service = get_service()
    if not service:
        return

    mt      = detection["type"]
    session = detection.get("session", "—")
    now_str = datetime.now(timezone.utc).strftime("%d/%m/%Y %H:%M")
    confs   = "Pendiente CHoCH"

    # Columnas del journal:
    # A=Fecha, B=Par, C=TF, D=Tipo, E=Entrada($), F=Stop Loss($),
    # G=TP1 -2SD($), H=TP2 -2.5SD($), I=R:R esperado, J=Resultado,
    # K=R realizado, L=PnL($), M=Confluencias, N=Fib High($), O=Fib Low($), P=Notas

    row = [[
        now_str,                        # A Fecha
        "BTCUSDT",                      # B Par
        "1H/4H",                        # C TF
        mt,                             # D Tipo
        round(levels["entry"], 2),      # E Entrada
        round(levels["sl"], 2),         # F Stop Loss
        round(levels["tp1"], 2),        # G TP1 -2SD
        round(levels["tp2"], 2),        # H TP2 -2.5SD
        round(levels["rr"], 2),         # I R:R esperado
        "Esperando CHoCH",              # J Resultado
        "",                             # K R realizado
        "",                             # L PnL
        confs,                          # M Confluencias
        round(levels["fib_1"], 2),      # N Fib High (1)
        round(levels["fib_0"], 2),      # O Fib Low (0)
        setup_key                       # P Notas (clave)
    ]]

    try:
        service.spreadsheets().values().append(
            spreadsheetId=SHEET_ID,
            range=f"{SHEET_NAME}!A:P",
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

    row_num  = find_setup_row(service, setup_key)
    if not row_num:
        print(f"⚠️ No encontré la fila para {setup_key}")
        return

    conf_str = " | ".join(confluences) if confluences else "Ninguna"

    try:
        service.spreadsheets().values().update(
            spreadsheetId=SHEET_ID,
            range=f"{SHEET_NAME}!J{row_num}",
            valueInputOption="RAW",
            body={"values": [["Abierto"]]}
        ).execute()
        service.spreadsheets().values().update(
            spreadsheetId=SHEET_ID,
            range=f"{SHEET_NAME}!M{row_num}",
            valueInputOption="RAW",
            body={"values": [[conf_str]]}
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
