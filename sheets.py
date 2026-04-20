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

def log_detection(detection, levels, score, setup_key, symbol="BTCUSDT"):
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
    # MEJORA 8: Q-W = factores booleanos para scoring dinámico
    # Q=session_opt, R=bias_aligned, S=return_completo, T=liq_alta,
    # U=vol_alto, V=eqhl, W=rr_alto

    # Calcular factores booleanos
    session_opt    = 1 if session in ("Londres", "Nueva York", "Overlap") else 0
    bias_aligned   = 1 if detection.get("type") in ("ALCISTA", "BAJISTA") else 0  # se actualiza en update_choch
    return_comp    = 1 if detection.get("return_type") == "COMPLETO" else 0
    liq_alta       = 1 if detection.get("liq_level") == "ALTA" else 0
    vol_alto       = 1 if detection.get("spike_vol_level") == "ALTO" else 0
    eqhl           = 1 if detection.get("has_eqhl") else 0
    rr_alto        = 1 if levels.get("rr", 0) >= 2.0 else 0

    row = [[
        now_str,                        # A Fecha
        symbol,                         # B Par (dinámico)
        "1H/4H/1D",                     # C TF
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
        setup_key,                      # P Notas (clave)
        # MEJORA 8: factores para scoring dinámico
        session_opt,                    # Q session_opt
        bias_aligned,                   # R bias_aligned (placeholder)
        return_comp,                    # S return_completo
        liq_alta,                       # T liq_alta
        vol_alto,                       # U vol_alto
        eqhl,                           # V eqhl
        rr_alto,                        # W rr_alto
    ]]

    try:
        service.spreadsheets().values().append(
            spreadsheetId=SHEET_ID,
            range=f"{SHEET_NAME}!A:W",
            valueInputOption="RAW",
            insertDataOption="INSERT_ROWS",
            body={"values": row}
        ).execute()
        print(f"✅ Alerta 1 registrada en Google Sheets (con factores)")
    except Exception as e:
        print(f"❌ Error escribiendo en Sheets: {e}")

def update_choch(setup_key, choch_price, confluences, current_price,
                 macro_bias="", manip_type=""):
    """Actualiza la fila cuando el CHoCH se confirma — alerta 2."""
    service = get_service()
    if not service:
        return

    row_num  = find_setup_row(service, setup_key)
    if not row_num:
        print(f"⚠️ No encontré la fila para {setup_key}")
        return

    conf_str = " | ".join(confluences) if confluences else "Ninguna"

    # MEJORA 8: calcular bias_aligned correctamente ahora que tenemos macro_bias
    bias_aligned = 0
    if (macro_bias == "BULLISH" and manip_type == "ALCISTA") or \
       (macro_bias == "BEARISH" and manip_type == "BAJISTA"):
        bias_aligned = 1

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
        # MEJORA 8: actualizar bias_aligned (columna R)
        service.spreadsheets().values().update(
            spreadsheetId=SHEET_ID,
            range=f"{SHEET_NAME}!R{row_num}",
            valueInputOption="RAW",
            body={"values": [[bias_aligned]]}
        ).execute()
        print(f"✅ CHoCH actualizado en Google Sheets fila {row_num}")
    except Exception as e:
        print(f"❌ Error actualizando CHoCH: {e}")

# ─── MEJORA 8: LECTURA DE TRADES CERRADOS ────────────────────
def read_closed_trades():
    """
    Lee todos los trades cerrados del journal para recalibrar pesos.
    Busca filas donde J (Resultado) = 'Ganador' o 'Perdedor'.
    Retorna lista de dicts con factores booleanos y resultado.
    """
    service = get_service()
    if not service:
        return []

    try:
        # Leer columnas J (resultado) y Q-W (factores)
        result = service.spreadsheets().values().get(
            spreadsheetId=SHEET_ID,
            range=f"{SHEET_NAME}!A:W"
        ).execute()
        rows = result.get("values", [])

        trades = []
        for row in rows[1:]:  # saltar cabecera
            if len(row) < 23:  # necesitamos hasta columna W (23)
                continue

            resultado = row[9] if len(row) > 9 else ""  # J = índice 9
            if resultado.lower() not in ("ganador", "perdedor"):
                continue

            try:
                trade = {
                    "win":             1 if resultado.lower() == "ganador" else 0,
                    "session_opt":     int(row[16]) if len(row) > 16 and row[16] != "" else 0,
                    "bias_aligned":    int(row[17]) if len(row) > 17 and row[17] != "" else 0,
                    "return_completo": int(row[18]) if len(row) > 18 and row[18] != "" else 0,
                    "liq_alta":        int(row[19]) if len(row) > 19 and row[19] != "" else 0,
                    "vol_alto":        int(row[20]) if len(row) > 20 and row[20] != "" else 0,
                    "eqhl":            int(row[21]) if len(row) > 21 and row[21] != "" else 0,
                    "rr_alto":         int(row[22]) if len(row) > 22 and row[22] != "" else 0,
                }
                trades.append(trade)
            except (ValueError, IndexError):
                continue

        print(f"📊 Trades cerrados leídos: {len(trades)} ({sum(t['win'] for t in trades)} ganadores)")
        return trades

    except Exception as e:
        print(f"❌ Error leyendo trades: {e}")
        return []

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
            range=f"{SHEET_NAME}!J{row_num}",
            valueInputOption="RAW",
            body={"values": [[resultado]]}
        ).execute()
        print(f"✅ Resultado actualizado: {resultado}")
    except Exception as e:
        print(f"❌ Error actualizando resultado: {e}")

# ─── PRO 1: AUTO-CIERRE DEL JOURNAL ─────────────────────────
def auto_close_trade(setup_key, resultado, rr_real, pnl_pct, tps_hit):
    """
    Escribe automáticamente Ganador/Perdedor, RR real y PnL en el journal.
    Se llama cuando el bot detecta SL tocado o todos los TPs alcanzados.
    """
    service = get_service()
    if not service:
        return

    row_num = find_setup_row(service, setup_key)
    if not row_num:
        print(f"⚠️ Auto-cierre: fila no encontrada para {setup_key}")
        return

    try:
        # J = Resultado, K = R realizado, L = PnL(%)
        service.spreadsheets().values().update(
            spreadsheetId=SHEET_ID,
            range=f"{SHEET_NAME}!J{row_num}:L{row_num}",
            valueInputOption="RAW",
            body={"values": [[resultado, round(rr_real, 2), f"{pnl_pct:.2f}%"]]}
        ).execute()
        # Añadir nota con TPs alcanzados en columna P (append al existing)
        existing = ""
        try:
            res = service.spreadsheets().values().get(
                spreadsheetId=SHEET_ID,
                range=f"{SHEET_NAME}!P{row_num}"
            ).execute()
            vals = res.get("values", [[""]])
            existing = vals[0][0] if vals and vals[0] else ""
        except:
            pass
        note = f"{existing} | TPs: {tps_hit}"
        service.spreadsheets().values().update(
            spreadsheetId=SHEET_ID,
            range=f"{SHEET_NAME}!P{row_num}",
            valueInputOption="RAW",
            body={"values": [[note]]}
        ).execute()
        print(f"✅ Auto-cierre: {resultado} | RR {rr_real:.2f} | PnL {pnl_pct:.2f}%")
    except Exception as e:
        print(f"❌ Error auto-cierre: {e}")

# ─── PRO 2: RESUMEN SEMANAL ─────────────────────────────────
def generate_weekly_report():
    """
    Lee trades de la última semana y genera estadísticas.
    Retorna dict con métricas o None si no hay datos.
    """
    service = get_service()
    if not service:
        return None

    try:
        result = service.spreadsheets().values().get(
            spreadsheetId=SHEET_ID,
            range=f"{SHEET_NAME}!A:W"
        ).execute()
        rows = result.get("values", [])

        # Filtrar trades cerrados de la última semana
        now = datetime.now(timezone.utc)
        week_ago = now - timedelta(days=7)
        trades = []

        for row in rows[1:]:
            if len(row) < 12:
                continue
            resultado = row[9] if len(row) > 9 else ""
            if resultado.lower() not in ("ganador", "perdedor"):
                continue

            # Parsear fecha (columna A)
            try:
                fecha = datetime.strptime(row[0], "%d/%m/%Y %H:%M").replace(tzinfo=timezone.utc)
                if fecha < week_ago:
                    continue
            except:
                continue

            rr_real = 0
            try:
                rr_real = float(row[10]) if len(row) > 10 and row[10] else 0
            except:
                pass

            trades.append({
                "win": resultado.lower() == "ganador",
                "rr": rr_real,
                "tipo": row[3] if len(row) > 3 else "",
            })

        if not trades:
            return None

        total = len(trades)
        wins = sum(1 for t in trades if t["win"])
        losses = total - wins
        winrate = (wins / total) * 100
        avg_rr_win = 0
        avg_rr_loss = 0
        rr_wins = [t["rr"] for t in trades if t["win"] and t["rr"] > 0]
        rr_losses = [abs(t["rr"]) for t in trades if not t["win"] and t["rr"] != 0]
        if rr_wins:
            avg_rr_win = sum(rr_wins) / len(rr_wins)
        if rr_losses:
            avg_rr_loss = sum(rr_losses) / len(rr_losses)
        profit_factor = (avg_rr_win * wins) / (avg_rr_loss * losses) if losses > 0 and avg_rr_loss > 0 else 0

        return {
            "total": total,
            "wins": wins,
            "losses": losses,
            "winrate": winrate,
            "avg_rr_win": avg_rr_win,
            "avg_rr_loss": avg_rr_loss,
            "profit_factor": profit_factor,
            "best_rr": max(rr_wins) if rr_wins else 0,
        }

    except Exception as e:
        print(f"❌ Error generando reporte: {e}")
        return None
