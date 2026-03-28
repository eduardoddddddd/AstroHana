"""
01_migrate.py — Migración KeplerDB → SAP HANA Cloud
=====================================================
Lee las 8.502 cartas natales de kepler.db (SQLite),
calcula posiciones planetarias con pyswisseph,
y las carga en HANA Cloud tabla CARTAS_NATALES.

Uso:
    python scripts/01_migrate.py --create-tables   # solo crear DDL
    python scripts/01_migrate.py --migrate         # migrar datos
    python scripts/01_migrate.py --all             # ambos
    python scripts/01_migrate.py --check           # verificar conteo
"""

import argparse
import sqlite3
import os
import sys
from datetime import datetime

import pandas as pd
import swisseph as swe
from hdbcli import dbapi
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), '..', '.env'))

# ── Configuración ──────────────────────────────────────────────
HANA_HOST = os.getenv("HANA_HOST")
HANA_PORT = int(os.getenv("HANA_PORT", 443))
HANA_USER = os.getenv("HANA_USER")
HANA_PASS = os.getenv("HANA_PASS")
KEPLER_DB = os.getenv("KEPLER_DB", r"C:\Users\Edu\Documents\ClaudeWork\KeplerDB\kepler.db")
EPHE_PATH = os.getenv("EPHE_PATH", r"C:\swisseph\ephe")
BATCH_SIZE = 200

# ── Constantes astrológicas ────────────────────────────────────
PLANETAS = {
    "SOL":      swe.SUN,
    "LUNA":     swe.MOON,
    "MERCURIO": swe.MERCURY,
    "VENUS":    swe.VENUS,
    "MARTE":    swe.MARS,
    "JUPITER":  swe.JUPITER,
    "SATURNO":  swe.SATURN,
    "URANO":    swe.URANUS,
    "NEPTUNO":  swe.NEPTUNE,
    "PLUTON":   swe.PLUTO,
}

SIGNOS = ["Aries","Tauro","Geminis","Cancer","Leo","Virgo",
          "Libra","Escorpio","Sagitario","Capricornio","Acuario","Piscis"]


def conectar_hana():
    return dbapi.connect(
        address=HANA_HOST, port=HANA_PORT,
        user=HANA_USER, password=HANA_PASS,
        encrypt=True, sslValidateCertificate=False
    )


def grado_a_signo_casa(grado: float) -> tuple[int, int]:
    """Convierte grado absoluto 0-360 a (num_signo 1-12, grado_en_signo)."""
    signo = int(grado / 30) + 1
    return signo, grado % 30


def calcular_posiciones(row) -> dict | None:
    """
    Dada una fila de la tabla 'cartas', calcula posiciones planetarias
    y ángulos con pyswisseph. Devuelve dict o None si hay error.
    """
    try:
        # Construir UT: hora local - corrección GMT
        # gmt en kepler.db es el offset a restar (e.g. gmt=-1 → UT = hora + 1)
        hora_decimal = row["hora"] + row["min"] / 60.0
        ut = hora_decimal - row["gmt"]  # pyswisseph quiere UT

        jd = swe.julday(int(row["anio"]), int(row["mes"]), int(row["dia"]), ut)

        resultado = {}

        # Planetas
        for nombre, planet_id in PLANETAS.items():
            pos, _ = swe.calc_ut(jd, planet_id, swe.FLG_SWIEPH)
            grado_abs = pos[0] % 360
            signo_num, _ = grado_a_signo_casa(grado_abs)
            resultado[f"{nombre}_GR"] = round(grado_abs, 4)
            resultado[f"{nombre}_SIGNO"] = signo_num

        # Casas Placidus + ASC + MC
        lat = float(row["lat"]) if row["lat"] else 40.0
        lon = float(row["lon"]) if row["lon"] else -3.0
        casas, ascmc = swe.houses(jd, lat, lon, b'P')  # Placidus

        asc = ascmc[0] % 360
        mc  = ascmc[1] % 360
        resultado["ASC_GR"]    = round(asc, 4)
        resultado["ASC_SIGNO"] = grado_a_signo_casa(asc)[0]
        resultado["MC_GR"]     = round(mc, 4)
        resultado["MC_SIGNO"]  = grado_a_signo_casa(mc)[0]

        # Casa de cada planeta (en qué casa cae)
        limites = list(casas) + [casas[0] + 360]
        for nombre in PLANETAS:
            grado = resultado[f"{nombre}_GR"]
            for i in range(12):
                c1 = limites[i] % 360
                c2 = limites[i+1] % 360
                if c2 < c1:  # cruce 0°
                    if grado >= c1 or grado < c2:
                        resultado[f"{nombre}_CASA"] = i + 1
                        break
                else:
                    if c1 <= grado < c2:
                        resultado[f"{nombre}_CASA"] = i + 1
                        break
            else:
                resultado[f"{nombre}_CASA"] = 1  # fallback

        return resultado

    except Exception as e:
        return None  # carta con datos incompletos, se salta


def crear_tablas(conn_hana):
    """Crea las tablas en HANA Cloud."""
    cursor = conn_hana.cursor()

    ddl_cartas = """
CREATE COLUMN TABLE DBADMIN.CARTAS_NATALES (
    ID            INTEGER NOT NULL PRIMARY KEY,
    NOMBRE        NVARCHAR(200),
    LUGAR         NVARCHAR(200),
    ANIO          SMALLINT,
    MES           TINYINT,
    DIA           TINYINT,
    HORA          TINYINT,
    MIN_          TINYINT,
    GMT           DOUBLE,
    LAT           DOUBLE,
    LON           DOUBLE,
    TAGS          NVARCHAR(50),
    DESCRIPCION   NVARCHAR(500),
    SOL_GR        DOUBLE,   SOL_SIGNO    TINYINT,  SOL_CASA     TINYINT,
    LUNA_GR       DOUBLE,   LUNA_SIGNO   TINYINT,  LUNA_CASA    TINYINT,
    MERCURIO_GR   DOUBLE,   MERCURIO_SIGNO TINYINT, MERCURIO_CASA TINYINT,
    VENUS_GR      DOUBLE,   VENUS_SIGNO  TINYINT,  VENUS_CASA   TINYINT,
    MARTE_GR      DOUBLE,   MARTE_SIGNO  TINYINT,  MARTE_CASA   TINYINT,
    JUPITER_GR    DOUBLE,   JUPITER_SIGNO TINYINT, JUPITER_CASA TINYINT,
    SATURNO_GR    DOUBLE,   SATURNO_SIGNO TINYINT, SATURNO_CASA TINYINT,
    URANO_GR      DOUBLE,   URANO_SIGNO  TINYINT,  URANO_CASA   TINYINT,
    NEPTUNO_GR    DOUBLE,   NEPTUNO_SIGNO TINYINT, NEPTUNO_CASA TINYINT,
    PLUTON_GR     DOUBLE,   PLUTON_SIGNO TINYINT,  PLUTON_CASA  TINYINT,
    ASC_GR        DOUBLE,   ASC_SIGNO    TINYINT,
    MC_GR         DOUBLE,   MC_SIGNO     TINYINT,
    CLUSTER_ID    INTEGER,
    PC1           DOUBLE,
    PC2           DOUBLE,
    DIST_TO_CENTER DOUBLE,
    CALC_ERROR    TINYINT DEFAULT 0
)"""
    cursor.execute(ddl_cartas)
    conn_hana.commit()
    print("✓ Tabla CARTAS_NATALES creada")

    ddl_textos = """
CREATE COLUMN TABLE DBADMIN.KEPLER_TEXTOS (
    ID            INTEGER NOT NULL PRIMARY KEY,
    FICHERO       NVARCHAR(100),
    INDICE        INTEGER,
    CABECERA      NVARCHAR(300),
    TEXTO         NCLOB,
    PLANETA1      NVARCHAR(50),
    PLANETA2      NVARCHAR(50),
    SIGNO         NVARCHAR(50),
    CASA          TINYINT,
    ASPECTO       NVARCHAR(50),
    CODIGO_PAREJA NVARCHAR(50),
    VALENCIA      NVARCHAR(50)
)"""
    cursor.execute(ddl_textos)
    conn_hana.commit()
    print("✓ Tabla KEPLER_TEXTOS creada")
    cursor.close()


def migrar_textos(conn_sqlite, conn_hana):
    """Migra los 743 textos interpretativos."""
    df = pd.read_sql("SELECT * FROM interpretaciones", conn_sqlite)
    cursor = conn_hana.cursor()
    sql = """INSERT INTO DBADMIN.KEPLER_TEXTOS
        (ID,FICHERO,INDICE,CABECERA,TEXTO,PLANETA1,PLANETA2,SIGNO,CASA,ASPECTO,CODIGO_PAREJA,VALENCIA)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?)"""
    rows = []
    for _, r in df.iterrows():
        rows.append((
            int(r["id"]), r["fichero"], r.get("indice"),
            r["cabecera"], r["texto"],
            r.get("planeta1"), r.get("planeta2"), r.get("signo"),
            int(r["casa"]) if pd.notna(r.get("casa")) else None,
            r.get("aspecto"), r.get("codigo_pareja"), r.get("valencia")
        ))
    cursor.executemany(sql, rows)
    conn_hana.commit()
    print(f"✓ {len(rows)} textos Kepler migrados")
    cursor.close()


def migrar_cartas(conn_sqlite, conn_hana):
    """Migra las 8.502 cartas natales calculando posiciones con swisseph."""
    swe.set_ephe_path(EPHE_PATH)
    df = pd.read_sql("SELECT * FROM cartas", conn_sqlite)
    total = len(df)
    print(f"  Procesando {total} cartas en lotes de {BATCH_SIZE}...")

    cursor = conn_hana.cursor()
    cols = ["ID","NOMBRE","LUGAR","ANIO","MES","DIA","HORA","MIN_","GMT","LAT","LON",
            "TAGS","DESCRIPCION",
            "SOL_GR","SOL_SIGNO","SOL_CASA",
            "LUNA_GR","LUNA_SIGNO","LUNA_CASA",
            "MERCURIO_GR","MERCURIO_SIGNO","MERCURIO_CASA",
            "VENUS_GR","VENUS_SIGNO","VENUS_CASA",
            "MARTE_GR","MARTE_SIGNO","MARTE_CASA",
            "JUPITER_GR","JUPITER_SIGNO","JUPITER_CASA",
            "SATURNO_GR","SATURNO_SIGNO","SATURNO_CASA",
            "URANO_GR","URANO_SIGNO","URANO_CASA",
            "NEPTUNO_GR","NEPTUNO_SIGNO","NEPTUNO_CASA",
            "PLUTON_GR","PLUTON_SIGNO","PLUTON_CASA",
            "ASC_GR","ASC_SIGNO","MC_GR","MC_SIGNO","CALC_ERROR"]
    placeholders = ",".join(["?"] * len(cols))
    sql = f"INSERT INTO DBADMIN.CARTAS_NATALES ({','.join(cols)}) VALUES ({placeholders})"

    ok = 0; err = 0; batch = []
    for idx, row in df.iterrows():
        pos = calcular_posiciones(row)
        if pos:
            batch.append((
                int(row["id"]), row["nombre"], row["lugar"],
                row["anio"], row["mes"], row["dia"],
                row["hora"], row["min"], row["gmt"],
                row["lat"], row["lon"], row["tags"], row["descripcion"],
                pos["SOL_GR"],     pos["SOL_SIGNO"],     pos.get("SOL_CASA",1),
                pos["LUNA_GR"],    pos["LUNA_SIGNO"],    pos.get("LUNA_CASA",1),
                pos["MERCURIO_GR"],pos["MERCURIO_SIGNO"],pos.get("MERCURIO_CASA",1),
                pos["VENUS_GR"],   pos["VENUS_SIGNO"],   pos.get("VENUS_CASA",1),
                pos["MARTE_GR"],   pos["MARTE_SIGNO"],   pos.get("MARTE_CASA",1),
                pos["JUPITER_GR"], pos["JUPITER_SIGNO"], pos.get("JUPITER_CASA",1),
                pos["SATURNO_GR"], pos["SATURNO_SIGNO"], pos.get("SATURNO_CASA",1),
                pos["URANO_GR"],   pos["URANO_SIGNO"],   pos.get("URANO_CASA",1),
                pos["NEPTUNO_GR"], pos["NEPTUNO_SIGNO"], pos.get("NEPTUNO_CASA",1),
                pos["PLUTON_GR"],  pos["PLUTON_SIGNO"],  pos.get("PLUTON_CASA",1),
                pos["ASC_GR"], pos["ASC_SIGNO"],
                pos["MC_GR"],  pos["MC_SIGNO"], 0
            ))
            ok += 1
        else:
            err += 1

        if len(batch) >= BATCH_SIZE:
            cursor.executemany(sql, batch)
            conn_hana.commit()
            batch = []
            pct = (idx+1)*100//total
            print(f"  [{pct:3d}%] {ok} OK, {err} errores", end="\r")

    if batch:
        cursor.executemany(sql, batch)
        conn_hana.commit()

    print(f"\n✓ Migración completa: {ok} cartas cargadas, {err} con error de datos")
    cursor.close()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--create-tables", action="store_true")
    parser.add_argument("--migrate",       action="store_true")
    parser.add_argument("--all",           action="store_true")
    parser.add_argument("--check",         action="store_true")
    args = parser.parse_args()

    if not any(vars(args).values()):
        parser.print_help(); sys.exit(0)

    print(f"Conectando a HANA Cloud ({HANA_HOST})...")
    conn_hana = conectar_hana()
    print("✓ HANA conectado")

    if args.create_tables or args.all:
        print("\n── Creando tablas ──")
        crear_tablas(conn_hana)

    if args.migrate or args.all:
        print("\n── Migrando textos Kepler ──")
        conn_sqlite = sqlite3.connect(KEPLER_DB)
        migrar_textos(conn_sqlite, conn_hana)
        print("\n── Migrando cartas natales ──")
        migrar_cartas(conn_sqlite, conn_hana)
        conn_sqlite.close()

    if args.check:
        cursor = conn_hana.cursor()
        cursor.execute("SELECT COUNT(*) FROM DBADMIN.CARTAS_NATALES")
        n = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM DBADMIN.KEPLER_TEXTOS")
        t = cursor.fetchone()[0]
        cursor.close()
        print(f"\nCARTAS_NATALES: {n} filas")
        print(f"KEPLER_TEXTOS:  {t} filas")

    conn_hana.close()
    print("\nHecho.")

if __name__ == "__main__":
    main()
