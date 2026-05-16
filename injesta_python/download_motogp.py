#!/usr/bin/env python3
"""
============================================================================
MotoGP API → CSV local : script de descarga
----------------------------------------------------------------------------
Descarga los datos nuevos de la API de MotoGP (api.micheleberardi.com) y
los guarda en la estructura de carpetas que espera ingest_motogp.py.

Flujo:
    1. Obtiene eventos del año indicado
    2. Filtra los eventos cuya fecha de inicio >= --from-date
    3. Para cada evento filtrado descarga:
       - Sesiones normales y sprint por categoría
       - Resultados de cada sesión
       - Standings por categoría
    4. Guarda todo como CSV en motogp_data_{year}_{year}/

Rate limit de la API: 50 llamadas/minuto · 200/hora · 500/día
El script respeta el límite con una pausa configurable entre llamadas.

Uso:
    python download_motogp.py --year 2026 --from-date 2026-05-09
    python download_motogp.py --year 2026 --from-date 2026-05-09 --to-date 2026-05-12

Variables de entorno:
    MOTOGP_API_TOKEN  — token JWT de la API (requerido)
============================================================================
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
import time
from datetime import date, datetime
from pathlib import Path

import pandas as pd
import requests

try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass


# ----------------------------------------------------------------------------
# Configuración
# ----------------------------------------------------------------------------
BASE_URL  = "https://api.micheleberardi.com/racing/v1.0"
# Token por defecto (demo). En producción usa la variable de entorno.
DEFAULT_TOKEN = os.getenv("MOTOGP_API_TOKEN",
                          "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9")

# Pausa entre llamadas a la API para no superar 50 llamadas/minuto
CALL_DELAY = 1.3   # segundos


# ----------------------------------------------------------------------------
# Cliente HTTP
# ----------------------------------------------------------------------------
def api_call(endpoint: str, params: dict, token: str) -> list | dict:
    """
    Llama a un endpoint POST de la API con los parámetros dados.
    Aplica el rate limit automáticamente (CALL_DELAY entre llamadas).
    Devuelve el JSON de la respuesta (lista o dict).
    """
    log = logging.getLogger("api")
    url = f"{BASE_URL}/{endpoint}"
    params = {**params, "token": token}

    time.sleep(CALL_DELAY)   # respetar rate limit

    try:
        resp = requests.post(url, params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        log.debug("  %s %s → %d items",
                  endpoint, {k: v for k, v in params.items() if k != "token"},
                  len(data) if isinstance(data, list) else 1)
        return data
    except requests.HTTPError as e:
        log.error("HTTP %s en %s: %s", resp.status_code, endpoint, e)
        return []
    except Exception as e:
        log.error("Error en %s: %s", endpoint, e)
        return []


def to_df(data: list | dict) -> pd.DataFrame:
    """Convierte respuesta JSON (lista de dicts o dict) a DataFrame plano."""
    if not data:
        return pd.DataFrame()
    if isinstance(data, dict):
        data = [data]
    return pd.json_normalize(data)


# ----------------------------------------------------------------------------
# Helpers de guardado
# ----------------------------------------------------------------------------
def save_csv(df: pd.DataFrame, path: Path) -> None:
    if df.empty:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False)
    logging.getLogger("save").info("  Guardado: %s (%d filas)", path.name, len(df))


def parse_event_date(event: dict) -> date | None:
    """Extrae la fecha de inicio del evento (intenta varios campos)."""
    for field in ("date_start", "dateStart", "start_date", "date"):
        val = event.get(field)
        if val:
            try:
                return datetime.fromisoformat(str(val)[:10]).date()
            except ValueError:
                continue
    return None


# ----------------------------------------------------------------------------
# Descarga por entidad
# ----------------------------------------------------------------------------
def download_seasons(year: int, token: str, out_dir: Path) -> None:
    log = logging.getLogger("seasons")
    log.info("Descargando seasons %d...", year)
    df = to_df(api_call("motogp-season", {"year": year}, token))
    save_csv(df, out_dir / "seasons.csv")


def download_categories(year: int, token: str, out_dir: Path) -> list[dict]:
    log = logging.getLogger("categories")
    log.info("Descargando categories %d...", year)
    data = api_call("motogp-category", {"year": year}, token)
    df = to_df(data)
    save_csv(df, out_dir / "categories" / f"categories_{year}.csv")
    return data if isinstance(data, list) else []


def download_events(year: int, token: str, out_dir: Path,
                    from_date: date | None,
                    to_date: date | None) -> list[dict]:
    log = logging.getLogger("events")
    log.info("Descargando events %d...", year)
    data = api_call("motogp-events", {"year": year}, token)
    if not data:
        return []

    # Filtrar por rango de fechas si se especificó
    if from_date or to_date:
        filtered = []
        for ev in (data if isinstance(data, list) else []):
            ev_date = parse_event_date(ev)
            if ev_date is None:
                filtered.append(ev)   # sin fecha → incluir por precaución
                continue
            if from_date and ev_date < from_date:
                continue
            if to_date and ev_date > to_date:
                continue
            filtered.append(ev)
        log.info("  Eventos filtrados: %d de %d total", len(filtered), len(data))
        data = filtered

    df = to_df(data)
    save_csv(df, out_dir / "events" / f"events_{year}.csv")
    return data if isinstance(data, list) else []


def download_sessions(year: int, event: dict, categories: list[dict],
                      token: str, out_dir: Path) -> list[dict]:
    """
    Descarga sesiones normales y sprint para un evento y todas sus categorías.
    Devuelve lista de todas las sesiones descargadas (para luego buscar resultados).
    """
    log = logging.getLogger("sessions")
    event_id   = event.get("id") or event.get("event_id")
    event_name = event.get("name", event_id)
    all_sessions: list[dict] = []

    for cat in categories:
        cat_id   = cat.get("id") or cat.get("category_id")
        cat_name = cat.get("name", cat_id)

        # Sesiones normales
        log.info("  Sesiones %s / %s", event_name, cat_name)
        sessions_data = api_call(
            "motogp-sessions",
            {"year": year, "eventid": event_id, "categoryid": cat_id},
            token,
        )
        if sessions_data:
            df = to_df(sessions_data)
            # Añadir metadatos de contexto para el ingest
            df["_year"]        = year
            df["_event_id"]    = event_id
            df["_category_id"] = cat_id
            save_csv(df, out_dir / "sessions" /
                     f"sessions_{event_id}_{cat_id}.csv")
            all_sessions.extend(
                sessions_data if isinstance(sessions_data, list) else []
            )

        # Sesiones sprint
        log.info("  Sesiones sprint %s / %s", event_name, cat_name)
        sprint_data = api_call(
            "motogp-sessions-spr",
            {"year": year, "eventid": event_id, "categoryid": cat_id},
            token,
        )
        if sprint_data:
            df = to_df(sprint_data)
            df["_year"]        = year
            df["_event_id"]    = event_id
            df["_category_id"] = cat_id
            save_csv(df, out_dir / "sessions_sprint" /
                     f"sessions_sprint_{event_id}_{cat_id}.csv")
            all_sessions.extend(
                sprint_data if isinstance(sprint_data, list) else []
            )

    return all_sessions


def download_results(year: int, event: dict, sessions: list[dict],
                     token: str, out_dir: Path) -> None:
    """Descarga resultados de cada sesión del evento."""
    log  = logging.getLogger("results")
    event_id = event.get("id") or event.get("event_id")

    for session in sessions:
        session_id   = session.get("id") or session.get("session_id")
        session_type = session.get("type", session_id)

        log.info("  Resultados sesión %s (%s)", session_type, session_id)
        data = api_call(
            "motogp-full-results",
            {"eventid": event_id, "year": year, "session": session_id},
            token,
        )
        if data:
            df = to_df(data)
            df["_year"]       = year
            df["_event_id"]   = event_id
            df["_session_id"] = session_id
            save_csv(df, out_dir / "results" /
                     f"results_{event_id}_{session_id}.csv")


def download_standings(year: int, categories: list[dict],
                       token: str, out_dir: Path) -> None:
    """Descarga standings por categoría."""
    log = logging.getLogger("standings")
    for cat in categories:
        cat_id   = cat.get("id") or cat.get("category_id")
        cat_name = cat.get("name", cat_id)
        log.info("  Standings %s %s", year, cat_name)
        data = api_call(
            "motogp-world-standing-riders",
            {"year": year, "categoryid": cat_id},
            token,
        )
        if data:
            df = to_df(data)
            df["_year"]        = year
            df["_category_id"] = cat_id
            save_csv(df, out_dir / "standings" /
                     f"standings_{year}_{cat_id}.csv")


def download_calendar(token: str, out_dir: Path) -> None:
    """Descarga calendario completo y próximos eventos."""
    log = logging.getLogger("calendar")
    for filter_type in ("full", "upcoming"):
        log.info("  Calendario %s", filter_type)
        data = api_call("motogp-calendar", {"filter": filter_type}, token)
        if data:
            df = to_df(data) if isinstance(data, list) else pd.DataFrame([data])
            save_csv(df, out_dir / "calendar" / f"calendar_{filter_type}.csv")


# ----------------------------------------------------------------------------
# Main
# ----------------------------------------------------------------------------
def main() -> int:
    parser = argparse.ArgumentParser(
        description="Descarga datos MotoGP de la API a CSV locales",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--year", type=int, required=True,
                        help="Año a descargar (ej: 2026)")
    parser.add_argument("--from-date", type=str, default=None,
                        help="Descargar solo eventos desde esta fecha (YYYY-MM-DD)")
    parser.add_argument("--to-date", type=str, default=None,
                        help="Descargar solo eventos hasta esta fecha (YYYY-MM-DD)")
    parser.add_argument("--out-dir", type=Path, default=Path("."),
                        help="Directorio raíz donde crear motogp_data_YYYY_YYYY/")
    parser.add_argument("--skip-standings", action="store_true",
                        help="No descargar standings (ahorra llamadas a la API)")
    parser.add_argument("--skip-calendar", action="store_true",
                        help="No descargar calendario")
    parser.add_argument("--token", type=str, default=DEFAULT_TOKEN,
                        help="Token JWT de la API")
    parser.add_argument("--log-level", default="INFO",
                        choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    args = parser.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s | %(levelname)-7s | %(name)-10s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    log = logging.getLogger("main")

    # Parsear fechas
    from_date = date.fromisoformat(args.from_date) if args.from_date else None
    to_date   = date.fromisoformat(args.to_date)   if args.to_date   else None

    if from_date:
        log.info("Filtrando eventos desde %s", from_date)
    if to_date:
        log.info("Filtrando eventos hasta %s", to_date)

    # Directorio de salida: motogp_data_2026_2026/
    out_dir = args.out_dir / f"motogp_data_{args.year}_{args.year}"
    out_dir.mkdir(parents=True, exist_ok=True)
    log.info("Directorio de salida: %s", out_dir)

    token = args.token
    year  = args.year

    # 1. Seasons
    download_seasons(year, token, out_dir)

    # 2. Categories (necesarias para sessions y standings)
    categories = download_categories(year, token, out_dir)
    if not categories:
        log.error("No se obtuvieron categorías para %d — abortando", year)
        return 1

    # 3. Events filtrados por fecha
    events = download_events(year, token, out_dir, from_date, to_date)
    if not events:
        log.warning("No hay eventos para el rango de fechas indicado")
        return 0

    # 4. Sessions y Results por evento
    for event in events:
        event_name = event.get("name", event.get("id"))
        log.info("=== Evento: %s ===", event_name)

        sessions = download_sessions(year, event, categories, token, out_dir)
        download_results(year, event, sessions, token, out_dir)

    # 5. Standings por categoría (opciones para ahorrar llamadas)
    if not args.skip_standings:
        download_standings(year, categories, token, out_dir)

    # 6. Calendario
    if not args.skip_calendar:
        download_calendar(token, out_dir)

    log.info("Descarga completada → %s", out_dir)
    return 0


if __name__ == "__main__":
    sys.exit(main())
