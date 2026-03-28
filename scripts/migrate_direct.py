"""
migrate_direct.py — Migración directa sin pasar por 01_migrate.py
Resuelve el problema de NaN en columnas string/int para hdbcli.
"""
import sqlite3, os, math
import pandas as pd
import swisseph as swe
from hdbcli import dbapi
from dotenv import load_dotenv

load_dotenv(r"C:\Users\Edu\AstroHana\.env")

HANA_HOST = os.getenv("HANA_HOST")
HANA_PORT = int(os.getenv("HANA_PORT", 443))
HANA_USER = os.getenv("HANA_USER")
HANA_PASS = os.getenv("HANA_PASS")
KEPLER_DB = os.getenv("KEPLER_DB")
EPHE_PATH = os.getenv("EPHE_PATH", r"C:\swisseph\ephe")
BATCH     = 200

def n(v):
    """Convierte NaN/None de pandas a None de Python."""
    if v is None: return None
    try:
        if math.isnan(float(v)): return None
    except (TypeError, ValueError): pass
    return v

def ni(v):
    """Igual pero fuerza int o None."""
    v2 = n(v)
    return int(v2) if v2 is not None else None

def conectar():
    return dbapi.connect(address=HANA_HOST, port=HANA_PORT,
        user=HANA_USER, password=HANA_PASS,
        encrypt=True, sslValidateCertificate=False)

PLANETAS_SW = {
    "SOL": swe.SUN, "LUNA": swe.MOON, "MERCURIO": swe.MERCURY,
    "VENUS": swe.VENUS, "MARTE": swe.MARS, "JUPITER": swe.JUPITER,
    "SATURNO": swe.SATURN, "URANO": swe.URANUS,
    "NEPTUNO": swe.NEPTUNE, "PLUTON": swe.PLUTO,
}
PLANET_LIST = list(PLANETAS_SW.keys())

def calcular(row):
    try:
        hora_dec = float(row["hora"]) + float(row["min"]) / 60.0
        gmt = float(row["gmt"]) if n(row["gmt"]) is not None else 0.0
        ut  = hora_dec - gmt
        jd  = swe.julday(int(row["anio"]), int(row["mes"]), int(row["dia"]), ut)
        res = {}
        for nombre, pid in PLANETAS_SW.items():
            pos, _ = swe.calc_ut(jd, pid, swe.FLG_SWIEPH)
            gr = pos[0] % 360
            res[f"{nombre}_GR"]    = round(gr, 4)
            res[f"{nombre}_SIGNO"] = int(gr / 30) + 1
        lat = float(row["lat"]) if n(row["lat"]) is not None else 40.0
        lon = float(row["lon"]) if n(row["lon"]) is not None else -3.0
        casas, ascmc = swe.houses(jd, lat, lon, b'P')
        asc = ascmc[0] % 360; mc = ascmc[1] % 360
        res["ASC_GR"] = round(asc, 4); res["ASC_SIGNO"] = int(asc/30)+1
        res["MC_GR"]  = round(mc,  4); res["MC_SIGNO"]  = int(mc/30)+1
        lims = list(casas)
        for nombre in PLANET_LIST:
            gr = res[f"{nombre}_GR"]
            casa = 1
            for i in range(12):
                c1 = lims[i] % 360
                c2 = lims[(i+1) % 12] % 360
                if c2 < c1:
                    if gr >= c1 or gr < c2: casa = i+1; break
                else:
                    if c1 <= gr < c2: casa = i+1; break
            res[f"{nombre}_CASA"] = casa
        return res
    except Exception as e:
        return None

def migrar_textos(conn_s, conn_h):
    df = pd.read_sql("SELECT * FROM interpretaciones", conn_s)
    cur = conn_h.cursor()
    sql = """INSERT INTO DBADMIN.KEPLER_TEXTOS
        (ID,FICHERO,INDICE,CABECERA,TEXTO,PLANETA1,PLANETA2,SIGNO,CASA,ASPECTO,CODIGO_PAREJA,VALENCIA)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?)"""
    rows = []
    for _, r in df.iterrows():
        rows.append((
            int(r["id"]), str(r["fichero"]), ni(r.get("indice")),
            n(r["cabecera"]), n(r["texto"]),
            n(r.get("planeta1")), n(r.get("planeta2")), n(r.get("signo")),
            ni(r.get("casa")),
            n(r.get("aspecto")), n(r.get("codigo_pareja")), n(r.get("valencia"))
        ))
    cur.executemany(sql, rows)
    conn_h.commit()
    print(f"✓ {len(rows)} textos Kepler migrados")
    cur.close()

def migrar_cartas(conn_s, conn_h):
    swe.set_ephe_path(EPHE_PATH)
    df = pd.read_sql("SELECT * FROM cartas", conn_s)
    total = len(df)
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
    ph  = ",".join(["?"]*len(cols))
    sql = f"INSERT INTO DBADMIN.CARTAS_NATALES ({','.join(cols)}) VALUES ({ph})"
    cur = conn_h.cursor()
    ok = 0; err = 0; batch = []
    for idx, row in df.iterrows():
        pos = calcular(row)
        if pos:
            batch.append((
                int(row["id"]),
                n(row["nombre"]), n(row["lugar"]),
                ni(row["anio"]), ni(row["mes"]), ni(row["dia"]),
                ni(row["hora"]), ni(row["min"]), n(row["gmt"]),
                n(row["lat"]),   n(row["lon"]),
                n(row["tags"]),  n(row["descripcion"]),
                pos["SOL_GR"],      pos["SOL_SIGNO"],      pos.get("SOL_CASA",1),
                pos["LUNA_GR"],     pos["LUNA_SIGNO"],     pos.get("LUNA_CASA",1),
                pos["MERCURIO_GR"], pos["MERCURIO_SIGNO"], pos.get("MERCURIO_CASA",1),
                pos["VENUS_GR"],    pos["VENUS_SIGNO"],    pos.get("VENUS_CASA",1),
                pos["MARTE_GR"],    pos["MARTE_SIGNO"],    pos.get("MARTE_CASA",1),
                pos["JUPITER_GR"],  pos["JUPITER_SIGNO"],  pos.get("JUPITER_CASA",1),
                pos["SATURNO_GR"],  pos["SATURNO_SIGNO"],  pos.get("SATURNO_CASA",1),
                pos["URANO_GR"],    pos["URANO_SIGNO"],    pos.get("URANO_CASA",1),
                pos["NEPTUNO_GR"],  pos["NEPTUNO_SIGNO"],  pos.get("NEPTUNO_CASA",1),
                pos["PLUTON_GR"],   pos["PLUTON_SIGNO"],   pos.get("PLUTON_CASA",1),
                pos["ASC_GR"],      pos["ASC_SIGNO"],
                pos["MC_GR"],       pos["MC_SIGNO"], 0
            ))
            ok += 1
        else:
            err += 1
        if len(batch) >= BATCH:
            cur.executemany(sql, batch); conn_h.commit(); batch = []
            pct = (idx+1)*100//total
            print(f"  [{pct:3d}%] {ok:5d} cartas OK, {err} errores", end="\r")
    if batch:
        cur.executemany(sql, batch); conn_h.commit()
    print(f"\n✓ {ok} cartas migradas, {err} con error de datos")
    cur.close()

if __name__ == "__main__":
    print("Conectando a HANA..."); conn_h = conectar(); print("✓ OK")
    conn_s = sqlite3.connect(KEPLER_DB)
    print("\n── Textos Kepler ──"); migrar_textos(conn_s, conn_h)
    print("\n── Cartas natales (calcula posiciones con pyswisseph) ──")
    migrar_cartas(conn_s, conn_h)
    conn_s.close(); conn_h.close()
    print("\nHecho.")
