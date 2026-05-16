#!/usr/bin/env python3
"""
============================================================================
MotoGP -> Snowflake : script de ingesta vía Stage
----------------------------------------------------------------------------
Lee los CSVs de los directorios `motogp_data_{year_start}_{year_end}/` que se
encuentren bajo --root-dir, normaliza los datos por entidad y los carga a
Snowflake usando un stage interno nombrado (PUT → COPY INTO).

Flujo de carga:
    1. Recolectar y normalizar DataFrames por entidad (igual que antes)
    2. Serializar a CSV comprimido (gzip) en un directorio temporal local
    3. PUT del archivo al stage interno @{SCHEMA}.MOTOGP_STAGE/{entity}/
    4. COPY INTO la tabla destino desde el stage
    5. (Opcional) Purgar el stage tras la carga

Entidades cargadas:
    seasons, categories, events, sessions, results, standings, calendar, files

Uso:
    python ingest_motogp.py --root-dir /ruta/al/dataset

Opciones:
    --dry-run               : descubre archivos y muestra resumen, sin tocar Snowflake
    --apply-ddl             : ejecuta snowflake_ddl.sql antes de cargar
    --truncate              : TRUNCATE de cada tabla antes de insertar
    --only seasons,events   : entidades específicas (lista separada por comas)
    --purge-stage           : elimina los archivos del stage tras cargar con éxito
    --keep-tmp              : no borrar los CSV temporales locales al finalizar

Variables de entorno requeridas:
    SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PASSWORD,
    SNOWFLAKE_ROLE (opcional), SNOWFLAKE_WAREHOUSE,
    SNOWFLAKE_DATABASE, SNOWFLAKE_SCHEMA
============================================================================
"""

from __future__ import annotations

import argparse
import csv as csv_module
import gzip
import json
import logging
import os
import re
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import List

import pandas as pd

# --- Snowflake ---------------------------------------------------------------
try:
    import snowflake.connector
except ImportError:
    snowflake = None  # se valida antes de conectar si no es --dry-run

# --- dotenv (opcional) -------------------------------------------------------
try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass


# ----------------------------------------------------------------------------
# Configuración
# ----------------------------------------------------------------------------
DEFAULT_DATABASE  = os.getenv("SNOWFLAKE_DATABASE", "FAIL")
DEFAULT_SCHEMA    = os.getenv("SNOWFLAKE_SCHEMA",   "FAIL")
DEFAULT_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE", "FAIL")

# Stage y file format ya existentes en DEV_MOTOGP_BRONZE_DB.RAW
STAGE_NAME       = "MOTOGP_STAGE"
FILE_FORMAT_NAME = "CSV_FF"

# Mapeo entidad -> nombre de tabla en Snowflake
TABLE_MAP = {
    "seasons":    "SEASONS",
    "categories": "CATEGORIES",
    "events":     "EVENTS",
    "sessions":   "SESSIONS",
    "results":    "RESULTS",
    "standings":  "STANDINGS",
    "calendar":   "CALENDAR",
    "files":      "FILES",
}

# Renombrado global de columnas que colisionan con palabras reservadas de Snowflake
RESERVED_RENAMES = {
    "current":     "is_current",   # SEASONS  (CURRENT es reservada)
    "time":        "time_text",    # RESULTS  (TIME es reservada)
    "isnewrecord": "is_new_record",
}

# Coerción de tipos por entidad (se aplica al DataFrame antes de serializar a CSV)
TYPE_COERCIONS = {
    "seasons": {
        "year": "Int64", "is_current": "Int64", "_year": "Int64",
    },
    "categories": {
        "legacy_id": "Int64", "year": "Int64", "_year": "Int64",
    },
    "events": {
        "event_circuit_information_menu_position": "Int64",
        "event_podiums_menu_position": "Int64",
        "event_pole_positions_menu_position": "Int64",
        "event_nations_statistics_menu_position": "Int64",
        "event_riders_all_time_menu_position": "Int64",
        "circuit_legacy_id": "Int64",
        "test": "boolean",
        "year": "Int64",
        "season_current": "Int64",
        "_year": "Int64",
        "date_start": "date",
        "date_end": "date",
    },
    "sessions": {
        "number": "Int64",
        "category_legacy_id": "Int64",
        "year": "Int64",
        "circuit_legacy_id": "Int64",
        "_year": "Int64",
        "is_sprint": "boolean",
        "date": "datetime",
    },
    "results": {
        "position": "Int64",
        "rider_legacy_id": "Int64",
        "year": "Int64",
        "session_number": "Int64",
        "_year": "Int64",
        "is_new_record": "boolean",
        "rider_in_grid": "boolean",
        "published": "boolean",
        "birth_date": "date",
    },
    "standings": {
        "year": "Int64",
        "points": "Int64",
        "position": "Int64",
        "_year": "Int64",
    },
    "calendar": {
        "season": "Int64",
        "calendar_index": "Int64",
        "test": "boolean",
        "has_timing": "boolean",
        "is_upcoming": "boolean",
        "start_date": "date",
        "end_date": "date",
        "last_session_end_time": "datetime",
    },
    "files": {
        "event_category_files_entry_menu_position": "Int64",
        "year": "Int64",
        "_year": "Int64",
    },
}


# ----------------------------------------------------------------------------
# Utilidades genéricas
# ----------------------------------------------------------------------------
def setup_logging(level: str = "INFO") -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s | %(levelname)-7s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def sanitize_column(name: str) -> str:
    """Convierte un nombre de columna a snake_case válido para Snowflake.
    Preserva el underscore inicial de las columnas de metadatos (_year, _event_id…)."""
    s = name.strip()
    leading = s.startswith("_")
    s = re.sub(r"[^0-9A-Za-z_]+", "_", s)
    s = re.sub(r"_+", "_", s).strip("_")
    if s and s[0].isdigit():
        s = f"c_{s}"
    s = s.lower() or "col"
    return ("_" + s) if leading else s


def sanitize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [sanitize_column(c) for c in df.columns]
    df.rename(columns=RESERVED_RENAMES, inplace=True)
    return df


def find_dataset_dirs(root: Path, year: int | None = None) -> List[Path]:
    pattern = re.compile(r"^motogp_data_(\d{4})_\d{4}$")
    dirs = []
    for p in root.iterdir():
        m = pattern.match(p.name)
        if p.is_dir() and m:
            if year is None or int(m.group(1)) == year:
                dirs.append(p)
    return sorted(dirs)


def list_csvs(folder: Path) -> List[Path]:
    return sorted(folder.glob("*.csv")) if folder.is_dir() else []


def add_metadata(df: pd.DataFrame, source_file: Path, source_dataset: str) -> pd.DataFrame:
    df["_source_file"]    = source_file.name
    df["_source_dataset"] = source_dataset
    df["_ingested_at"]    = datetime.now(timezone.utc).replace(tzinfo=None)
    return df


def coerce_types(df: pd.DataFrame, entity: str) -> pd.DataFrame:
    """Aplica conversiones de tipo según TYPE_COERCIONS[entity].
    Errores de parsing se convierten en NULL (errors='coerce')."""
    spec = TYPE_COERCIONS.get(entity, {})
    if not spec:
        return df
    for col, target in spec.items():
        if col not in df.columns:
            continue
        if target == "Int64":
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
        elif target == "Float64":
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Float64")
        elif target == "boolean":
            df[col] = (
                df[col].astype("string").str.strip().str.lower()
                .map({"true": True, "1": True, "yes": True,
                      "false": False, "0": False, "no": False})
                .astype("boolean")
            )
        elif target == "datetime":
            df[col] = pd.to_datetime(df[col], errors="coerce", utc=True)
        elif target == "date":
            df[col] = pd.to_datetime(df[col], errors="coerce", utc=True).dt.date
    return df


# ----------------------------------------------------------------------------
# Esquema completo de sessions (las CSVs minimalistas se rellenan con NULLs)
# ----------------------------------------------------------------------------
SESSIONS_FULL_COLUMNS = [
    "id", "date", "number", "track_condition", "air_condition",
    "humidity_condition", "ground_condition", "weather_condition",
    "circuit_name", "classification_url", "classification_menu_position",
    "analysis_url", "analysis_menu_position", "average_speed_url",
    "average_speed_menu_position", "fast_lap_sequence_url",
    "fast_lap_sequence_menu_position", "lap_chart_url", "lap_chart_menu_position",
    "analysis_by_lap_url", "analysis_by_lap_menu_position", "fast_lap_rider_url",
    "fast_lap_rider_menu_position", "grid_url", "grid_menu_position",
    "session_url", "session_menu_position", "world_standing_url",
    "world_standing_menu_position", "best_partial_time_url",
    "best_partial_time_menu_position", "maximum_speed_url",
    "maximum_speed_menu_position", "combined_practice_url",
    "combined_practice_menu_position", "combined_classification_url",
    "combined_classification_menu_position", "type", "category_id",
    "category_legacy_id", "category_name", "event_id", "event_name",
    "event_sponsored_name", "year", "circuit_id", "circuit_legacy_id",
    "circuit_place", "circuit_nation", "country_iso", "country_name",
    "country_region_iso", "event_short_name", "status",
    "_year", "_event_id", "_category_id",
]

CALENDAR_ENTRY_FIELDS = [
    "id", "shortname", "name", "hashtag", "circuit", "country_code",
    "country", "start_date", "end_date", "local_tz_offset", "test",
    "has_timing", "friendly_name", "dates", "last_session_end_time",
]


# ----------------------------------------------------------------------------
# Lectores específicos por entidad
# ----------------------------------------------------------------------------
def read_simple_csv(path: Path) -> pd.DataFrame:
    return pd.read_csv(path, dtype=str, keep_default_na=False,
                       na_values=["", "NULL", "null"])


def read_sessions_csv(path: Path) -> pd.DataFrame:
    rows: list[dict] = []
    with path.open("r", encoding="utf-8", newline="") as fh:
        reader = csv_module.reader(fh)
        try:
            header = next(reader)
        except StopIteration:
            return pd.DataFrame(columns=SESSIONS_FULL_COLUMNS)

        header_short = (header == ["result", "_year", "_event_id", "_category_id"])

        for row in reader:
            if not row:
                continue
            if len(row) == 4 and row[0] == "NORACE":
                rows.append({
                    "status": "NORACE",
                    "_year": row[1],
                    "_event_id": row[2],
                    "_category_id": row[3],
                })
            elif header_short and len(row) > 4:
                d = dict(zip(SESSIONS_FULL_COLUMNS, row))
                rows.append(d)
            else:
                d = dict(zip([sanitize_column(h) for h in header], row))
                rows.append(d)

    df = pd.DataFrame(rows)
    for col in SESSIONS_FULL_COLUMNS:
        if col not in df.columns:
            df[col] = None
    return df[SESSIONS_FULL_COLUMNS]


def read_calendar_csv(path: Path, is_upcoming: bool) -> pd.DataFrame:
    df_wide = pd.read_csv(path, dtype=str, keep_default_na=False,
                          na_values=["", "NULL", "null"])
    if df_wide.empty:
        return pd.DataFrame(columns=["season", "calendar_index",
                                     *CALENDAR_ENTRY_FIELDS,
                                     "key_session_times", "is_upcoming"])

    rows: list[dict] = []
    cols = df_wide.columns.tolist()
    indices = sorted({
        int(m.group(1)) for c in cols
        if (m := re.match(r"^calendar_(\d+)_", c))
    })

    for _, row in df_wide.iterrows():
        season = row.get("season")
        for i in indices:
            prefix = f"calendar_{i}_"
            entry: dict = {"season": season, "calendar_index": i,
                           "is_upcoming": is_upcoming}
            for fld in CALENDAR_ENTRY_FIELDS:
                entry[fld] = row.get(f"{prefix}{fld}")

            key_sessions = []
            j = 0
            while True:
                sn = row.get(f"{prefix}key_session_times_{j}_session_shortname")
                nm = row.get(f"{prefix}key_session_times_{j}_session_name")
                dt = row.get(f"{prefix}key_session_times_{j}_start_datetime_utc")
                if sn is None and nm is None and dt is None:
                    break
                key_sessions.append({"session_shortname": sn,
                                     "session_name": nm,
                                     "start_datetime_utc": dt})
                j += 1
            entry["key_session_times"] = json.dumps(key_sessions) if key_sessions else None

            if all(entry.get(f) in (None, "") for f in CALENDAR_ENTRY_FIELDS):
                continue
            rows.append(entry)

    return pd.DataFrame(rows)


# ----------------------------------------------------------------------------
# Discovery + concatenación por entidad
# ----------------------------------------------------------------------------
def collect_entity(entity: str, dataset_dirs: List[Path]) -> pd.DataFrame:
    log = logging.getLogger(entity)
    frames: list[pd.DataFrame] = []

    for ds_dir in dataset_dirs:
        ds_name = ds_dir.name

        if entity == "seasons":
            f = ds_dir / "seasons.csv"
            if f.exists():
                df = read_simple_csv(f)
                df = sanitize_columns(df)
                df = add_metadata(df, f, ds_name)
                frames.append(df)
                log.info("  %-22s  + %5d filas", f.name, len(df))

        elif entity == "calendar":
            for sub in ("calendar_full.csv", "calendar_upcoming.csv"):
                f = ds_dir / "calendar" / sub
                if f.exists():
                    df = read_calendar_csv(f, is_upcoming=("upcoming" in sub))
                    df = sanitize_columns(df)
                    df = add_metadata(df, f, ds_name)
                    frames.append(df)
                    log.info("  %-22s  + %5d entradas", f.name, len(df))

        elif entity in ("categories", "events", "results", "standings", "files"):
            folder = ds_dir / entity
            for f in list_csvs(folder):
                try:
                    df = read_simple_csv(f)
                except pd.errors.EmptyDataError:
                    continue
                if df.empty:
                    continue
                df = sanitize_columns(df)
                df = add_metadata(df, f, ds_name)
                frames.append(df)
                log.info("  %-40s + %5d filas", f.name, len(df))

        elif entity == "sessions":
            for folder_name in ("sessions", "sessions_sprint"):
                folder = ds_dir / folder_name
                for f in list_csvs(folder):
                    df = read_sessions_csv(f)
                    if df.empty:
                        continue
                    df = sanitize_columns(df)
                    df["is_sprint"] = (folder_name == "sessions_sprint")
                    df = add_metadata(df, f, ds_name)
                    frames.append(df)

    if not frames:
        log.warning("  Ninguna fila descubierta")
        return pd.DataFrame()

    df_all = pd.concat(frames, ignore_index=True, sort=False)
    df_all = coerce_types(df_all, entity)
    log.info("  TOTAL: %d filas, %d columnas", len(df_all), df_all.shape[1])
    return df_all


# ----------------------------------------------------------------------------
# Snowflake: conexión
# ----------------------------------------------------------------------------
def get_snowflake_conn(args: argparse.Namespace):
    if snowflake is None:
        raise RuntimeError(
            "snowflake-connector-python no está instalado. "
            "Ejecuta: pip install snowflake-connector-python"
        )
    missing = [v for v in ("SNOWFLAKE_ACCOUNT", "SNOWFLAKE_USER", "SNOWFLAKE_PASSWORD")
               if not os.getenv(v)]
    if missing:
        raise RuntimeError(f"Faltan variables de entorno: {missing}")

    return snowflake.connector.connect(
        account   = os.environ["SNOWFLAKE_ACCOUNT"],
        user      = os.environ["SNOWFLAKE_USER"],
        password  = os.environ["SNOWFLAKE_PASSWORD"],
        role      = os.getenv("SNOWFLAKE_ROLE"),
        warehouse = args.warehouse,
        database  = args.database,
        schema    = args.schema,
        client_session_keep_alive = True,
    )


# ----------------------------------------------------------------------------
# Snowflake: DDL auxiliar
# ----------------------------------------------------------------------------
def execute_sql_file(conn, sql_path: Path) -> None:
    log = logging.getLogger("ddl")
    log.info("Aplicando DDL desde %s", sql_path)
    statements = [s.strip() for s in sql_path.read_text(encoding="utf-8").split(";")
                  if s.strip() and not s.strip().startswith("--")]
    cur = conn.cursor()
    try:
        for stmt in statements:
            log.debug("EXEC: %s", stmt[:120])
            cur.execute(stmt)
        log.info("DDL aplicado: %d statements", len(statements))
    finally:
        cur.close()


def verify_stage(conn, schema: str) -> None:
    """
    Verifica que el stage MOTOGP_STAGE y el file format CSV_FF
    existen en el schema. Falla rápido si no están disponibles
    para no llegar al PUT y obtener un error críptico.
    """
    log = logging.getLogger("stage")
    cur = conn.cursor()
    try:
        cur.execute(f"SHOW STAGES LIKE '{STAGE_NAME}' IN SCHEMA {schema}")
        if not cur.fetchone():
            raise RuntimeError(
                f"Stage '{STAGE_NAME}' no encontrado en {schema}. "
                "Créalo en Snowflake antes de ejecutar el script."
            )
        log.info("Stage @%s verificado", STAGE_NAME)

        cur.execute(f"SHOW FILE FORMATS LIKE '{FILE_FORMAT_NAME}' IN SCHEMA {schema}")
        if not cur.fetchone():
            raise RuntimeError(
                f"File format '{FILE_FORMAT_NAME}' no encontrado en {schema}. "
                "Créalo en Snowflake antes de ejecutar el script."
            )
        log.info("File format %s verificado", FILE_FORMAT_NAME)
    finally:
        cur.close()


# ----------------------------------------------------------------------------
# Snowflake: serialización a CSV comprimido
# ----------------------------------------------------------------------------
def df_to_csv_gz(df: pd.DataFrame, path: Path) -> None:
    """
    Serializa el DataFrame a un CSV comprimido con gzip listo para PUT.

    Decisiones de formato (alineadas con FILE FORMAT):
    - Fechas/timestamps como ISO 8601: Snowflake las parsea con DATE_FORMAT
      y TIMESTAMP_FORMAT definidos en el FILE FORMAT.
    - Booleanos como TRUE/FALSE en mayúsculas: Snowflake los reconoce.
    - NA nullable de pandas (pd.NA) se escribe como cadena vacía → NULL_IF
      la mapea a NULL en Snowflake.
    - El CSV lleva header (SKIP_HEADER = 1 en el FILE FORMAT).
    - FIELD_OPTIONALLY_ENCLOSED_BY='"' maneja campos con comas/saltos de línea.
    """
    # Convertir columnas de fecha/timestamp a string ISO para serialización limpia
    df_out = df.copy()
    for col in df_out.columns:
        dtype = df_out[col].dtype
        if hasattr(dtype, "tz") and dtype.tz is not None:
            # datetime con timezone → NTZ string (Snowflake TIMESTAMP_NTZ)
            df_out[col] = df_out[col].dt.strftime("%Y-%m-%d %H:%M:%S.%f")
        elif str(dtype) in ("datetime64[ns]", "datetime64[us]"):
            df_out[col] = df_out[col].dt.strftime("%Y-%m-%d %H:%M:%S.%f")
        elif str(dtype) == "object":
            # Columnas de date (Python date objects almacenados como object en pandas)
            try:
                sample = df_out[col].dropna().iloc[0]
                if hasattr(sample, "strftime") and not hasattr(sample, "hour"):
                    df_out[col] = df_out[col].apply(
                        lambda x: x.strftime("%Y-%m-%d") if pd.notna(x) and x is not None else None
                    )
            except (IndexError, AttributeError):
                pass
        elif str(dtype) == "boolean":
            # pd.BooleanDtype → True/False/pd.NA → "TRUE"/"FALSE"/""
            df_out[col] = df_out[col].map(
                {True: "TRUE", False: "FALSE", pd.NA: None}, na_action=None
            )

    with gzip.open(path, "wt", encoding="utf-8", newline="") as fh:
        df_out.to_csv(fh, index=False, quoting=csv_module.QUOTE_MINIMAL,
                      lineterminator="\n")


# ----------------------------------------------------------------------------
# Snowflake: PUT → COPY INTO
# ----------------------------------------------------------------------------
def put_file_to_stage(conn, local_path: Path, stage_prefix: str,
                      database: str, schema: str) -> None:
    """
    Sube un archivo local al stage interno de Snowflake.

    PUT file:///{local_path} @{database}.{schema}.{STAGE_NAME}/{stage_prefix}/
        AUTO_COMPRESS = FALSE  (ya comprimimos con gzip)
        OVERWRITE     = TRUE   (idempotente: re-ejecuciones sobrescriben)
        SOURCE_COMPRESSION = GZIP
    """
    log = logging.getLogger("stage")
    # En Windows los paths llevan backslash; Snowflake PUT requiere forward slash
    local_uri = local_path.as_posix()
    stmt = (
        f"PUT 'file://{local_uri}' "
        f"@{database}.{schema}.{STAGE_NAME}/{stage_prefix}/ "
        f"AUTO_COMPRESS = FALSE "
        f"OVERWRITE = TRUE "
        f"SOURCE_COMPRESSION = GZIP"
    )
    log.debug("PUT: %s", stmt)
    cur = conn.cursor()
    try:
        cur.execute(stmt)
        # El resultado del PUT incluye: source, target, source_size, target_size, status
        rows = cur.fetchall()
        for r in rows:
            log.info("  PUT   %-40s  →  %-50s  [%s]", r[0], r[1], r[6] if len(r) > 6 else "?")
    finally:
        cur.close()


def copy_into_table(conn, database: str, schema: str, table: str,
                    stage_prefix: str, purge: bool) -> int:
    """
    Ejecuta COPY INTO {table} desde el prefijo del stage.

    ON_ERROR = CONTINUE : filas con error no abortan la carga; se registran
    en VALIDATE_COPY y en SNOWFLAKE.ACCOUNT_USAGE.COPY_HISTORY.
    PURGE = TRUE/FALSE  : si True, los archivos del stage se eliminan
    tras la carga exitosa, liberando almacenamiento del stage.

    Devuelve el número de filas cargadas.
    """
    log = logging.getLogger("copy")
    purge_clause = "PURGE = TRUE" if purge else "PURGE = FALSE"

    stmt = f"""
        COPY INTO {database}.{schema}.{table}
        FROM @{database}.{schema}.{STAGE_NAME}/{stage_prefix}/
        FILE_FORMAT = (
            TYPE                         = CSV
            PARSE_HEADER                 = TRUE
            FIELD_OPTIONALLY_ENCLOSED_BY = '"'
            NULL_IF                      = ('', 'NULL', 'null', 'NaN', 'nan', '<NA>')
            EMPTY_FIELD_AS_NULL          = TRUE
            ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
            REPLACE_INVALID_CHARACTERS   = TRUE
            COMPRESSION                  = AUTO
        )
        MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
        {purge_clause}
        ON_ERROR = CONTINUE
    """
    log.debug("COPY INTO:\n%s", stmt.strip())
    cur = conn.cursor()
    total_rows = 0
    try:
        cur.execute(stmt)
        results = cur.fetchall()
        for r in results:
            # Columnas: file, status, rows_parsed, rows_loaded, error_limit,
            #           errors_seen, first_error, first_error_line, ...
            file_name    = r[0] if len(r) > 0 else "?"
            status       = r[1] if len(r) > 1 else "?"
            rows_parsed  = r[2] if len(r) > 2 else 0
            rows_loaded  = r[3] if len(r) > 3 else 0
            errors_seen  = r[5] if len(r) > 5 else 0
            first_error  = r[6] if len(r) > 6 else ""
            total_rows  += (rows_loaded or 0)
            log.info(
                "  COPY  %-50s  status=%-12s  parsed=%6s  loaded=%6s  errors=%s",
                file_name, status, rows_parsed, rows_loaded, errors_seen,
            )
            if errors_seen and first_error:
                log.warning("  Primer error en %s: %s", file_name, first_error)
    finally:
        cur.close()
    return total_rows


def truncate_table(conn, database: str, schema: str, table: str) -> None:
    cur = conn.cursor()
    try:
        cur.execute(f"TRUNCATE TABLE IF EXISTS {database}.{schema}.{table}")
    finally:
        cur.close()


def purge_stage_prefix(conn, database: str, schema: str, stage_prefix: str) -> None:
    """Elimina todos los archivos del stage bajo el prefijo dado."""
    log = logging.getLogger("stage")
    cur = conn.cursor()
    try:
        cur.execute(f"REMOVE @{database}.{schema}.{STAGE_NAME}/{stage_prefix}/")
        log.info("Stage prefix %s.%s.%s/%s/ purgado",
                 database, schema, STAGE_NAME, stage_prefix)
    finally:
        cur.close()


# ----------------------------------------------------------------------------
# Main
# ----------------------------------------------------------------------------
def main() -> int:
    parser = argparse.ArgumentParser(
        description="Ingesta MotoGP → Snowflake vía stage interno (PUT + COPY INTO)",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--root-dir", type=Path,
                        default=Path(__file__).resolve().parent.parent,
                        help="Directorio raíz con las carpetas motogp_data_*")
    parser.add_argument("--ddl-file", type=Path,
                        default=Path(__file__).resolve().parent / "snowflake_ddl.sql",
                        help="Ruta al archivo DDL")
    parser.add_argument("--apply-ddl", action="store_true",
                        help="Ejecutar el DDL antes de cargar")
    parser.add_argument("--truncate", action="store_true",
                        help="TRUNCATE de cada tabla antes de la carga")
    parser.add_argument("--year", type=int, default=None,
                        help="Filtrar solo carpetas del año indicado (ej: 2026)")
    parser.add_argument("--only", type=str, default="",
                        help=f"Entidades a cargar (coma-separadas): {list(TABLE_MAP)}")
    parser.add_argument("--dry-run", action="store_true",
                        help="Solo descubrir y mostrar resumen, sin tocar Snowflake")
    parser.add_argument("--purge-stage", action="store_true",
                        help="Eliminar archivos del stage tras cargar con éxito")
    parser.add_argument("--keep-tmp", action="store_true",
                        help="No borrar los CSVs temporales locales al finalizar")
    parser.add_argument("--database",  default=DEFAULT_DATABASE)
    parser.add_argument("--schema",    default=DEFAULT_SCHEMA)
    parser.add_argument("--warehouse", default=DEFAULT_WAREHOUSE)
    parser.add_argument("--log-level", default="INFO",
                        choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    args = parser.parse_args()

    setup_logging(args.log_level)
    log = logging.getLogger("main")

    root = args.root_dir.resolve()
    if not root.is_dir():
        log.error("--root-dir no existe o no es directorio: %s", root)
        return 2

    dataset_dirs = find_dataset_dirs(root, year=args.year)
    if not dataset_dirs:
        log.error("No se encontraron carpetas motogp_data_YYYY_YYYY en %s", root)
        return 2

    log.info("Datasets descubiertos: %s", [d.name for d in dataset_dirs])

    only = {e.strip().lower() for e in args.only.split(",") if e.strip()}
    entities = [e for e in TABLE_MAP if not only or e in only]
    log.info("Entidades a procesar: %s", entities)

    # ----- Recolección por entidad ------------------------------------------
    dataframes: dict[str, pd.DataFrame] = {}
    for entity in entities:
        log.info("=== Recolectando '%s' ===", entity)
        df = collect_entity(entity, dataset_dirs)
        if not df.empty:
            dataframes[entity] = df

    if args.dry_run:
        log.info("--- DRY RUN: resumen ---")
        for entity, df in dataframes.items():
            log.info("  %-12s -> %s : %d filas, %d columnas",
                     entity, TABLE_MAP[entity], len(df), df.shape[1])
        return 0

    # ----- Conexión a Snowflake ---------------------------------------------
    log.info("Conectando a Snowflake...")
    conn = get_snowflake_conn(args)

    tmp_dir = Path(tempfile.mkdtemp(prefix="motogp_stage_"))
    log.info("Directorio temporal local: %s", tmp_dir)

    try:
        if args.apply_ddl:
            execute_sql_file(conn, args.ddl_file)

        # Verificar que el stage y file format existen (ya creados en Snowflake)
        verify_stage(conn, f"{args.database}.{args.schema}")

        cur = conn.cursor()
        cur.execute(f"USE WAREHOUSE {args.warehouse}")
        cur.execute(f"USE DATABASE  {args.database}")
        cur.execute(f"USE SCHEMA    {args.schema}")
        cur.close()

        run_ts    = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        total_rows = 0

        for entity, df in dataframes.items():
            table         = TABLE_MAP[entity]
            stage_prefix  = f"{entity}/{run_ts}"  # e.g. results/20260514_123456

            log.info("=== %s → %s.%s.%s ===",
                     entity, args.database, args.schema, table)

            # 1. Serializar DataFrame a CSV.gz en directorio temporal local
            csv_name  = f"{entity}_{run_ts}.csv.gz"
            csv_path  = tmp_dir / csv_name
            log.info("  Serializando %d filas a %s...", len(df), csv_name)
            df_to_csv_gz(df, csv_path)
            log.info("  Archivo CSV.gz: %.1f MB",
                     csv_path.stat().st_size / 1024 / 1024)

            # 2. (Opcional) TRUNCATE antes de insertar
            if args.truncate:
                truncate_table(conn, args.database, args.schema, table)
                log.info("  TRUNCATE aplicado a %s", table)

            # 3. PUT al stage (el timestamp en el prefijo hace idempotentes las re-ejecuciones)
            #    @DEV_MOTOGP_BRONZE_DB.RAW.MOTOGP_STAGE/results/20260514_123456/
            put_file_to_stage(conn, csv_path, stage_prefix,
                              args.database, args.schema)

            # 4. COPY INTO desde el stage a la tabla Bronze
            nrows = copy_into_table(
                conn, args.database, args.schema, table,
                stage_prefix, purge=args.purge_stage,
            )
            if args.purge_stage:
                purge_stage_prefix(conn, args.database, args.schema, stage_prefix)
            log.info("  Filas cargadas en %s: %d", table, nrows)
            total_rows += nrows

        log.info("Carga completa: %d filas totales en %d entidades",
                 total_rows, len(dataframes))

    finally:
        conn.close()
        if not args.keep_tmp:
            import shutil
            shutil.rmtree(tmp_dir, ignore_errors=True)
            log.info("Directorio temporal local eliminado")
        else:
            log.info("Directorio temporal conservado en: %s", tmp_dir)

    return 0


if __name__ == "__main__":
    sys.exit(main())