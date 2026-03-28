"""run_kmeans.py — ejecuta PAL K-Means sobre CARTAS_NATALES"""
import time
from hdbcli import dbapi

conn = dbapi.connect(
    address="20178d0a-d4af-4825-bba6-11a2aa151d20.hna1.prod-us10.hanacloud.ondemand.com",
    port=443, user="DBADMIN", password="Edu01edu.",
    encrypt=True, sslValidateCertificate=False)
cur = conn.cursor()
print("Conectado OK")

# ── Limpiar tablas de resultado previas ───────────────────────
for t in ["PAL_KMEANS_RESULT","PAL_KMEANS_CENTROIDS","PAL_KM_PARAMS"]:
    try:
        cur.execute(f"DROP TABLE DBADMIN.{t}")
        conn.commit()
    except:
        pass

# ── Tabla de parametros PAL (persistente, no temporal) ────────
cur.execute("""CREATE COLUMN TABLE DBADMIN.PAL_KM_PARAMS (
    NAME NVARCHAR(256), INTVAL INTEGER, DBLVAL DOUBLE, STRVAL NVARCHAR(1000))""")
params = [
    ("N_CLUSTERS",     12,   None,  None),
    ("MAX_ITER",       300,  None,  None),
    ("INIT",           None, None,  "KMEANS++"),
    ("DISTANCE_LEVEL", 2,    None,  None),
    ("NORMALIZATION",  1,    None,  None),
    ("EXIT_THRESHOLD", None, 1e-6,  None),
]
cur.executemany("INSERT INTO DBADMIN.PAL_KM_PARAMS VALUES (?,?,?,?)", params)
conn.commit()
print(f"Params insertados: {len(params)}")

# ── Tablas de output ──────────────────────────────────────────
cur.execute("""CREATE COLUMN TABLE DBADMIN.PAL_KMEANS_RESULT (
    ID INTEGER, CLUSTER_ID INTEGER, DISTANCE DOUBLE)""")
cur.execute("""CREATE COLUMN TABLE DBADMIN.PAL_KMEANS_CENTROIDS (
    CLUSTER_ID INTEGER, ATTR_NAME NVARCHAR(256), CENTROID_VAL DOUBLE)""")
conn.commit()
print("Tablas output creadas")

# ── Intentar con OVERLOAD_2_1 (3 tablas: data, params, result) ─
print("\nIntentando KMEANS__OVERLOAD_2_1 (solo assignments)...")
try:
    t0 = time.time()
    cur.execute("""CALL _SYS_AFL.PAL_KMEANS__OVERLOAD_2_1(
        DBADMIN.PAL_KMEANS_INPUT,
        DBADMIN.PAL_KM_PARAMS,
        DBADMIN.PAL_KMEANS_RESULT)""")
    conn.commit()
    print(f"  OK en {time.time()-t0:.1f}s - overload_2_1")
    overload = "2_1"
except Exception as e:
    print(f"  FAIL overload_2_1: {e}")
    overload = None

# ── Si falla, intentar con KMEANS base (4 tablas) ─────────────
if not overload:
    print("\nIntentando KMEANS base (4 tablas)...")
    try:
        t0 = time.time()
        cur.execute("""CALL _SYS_AFL.PAL_KMEANS(
            DBADMIN.PAL_KMEANS_INPUT,
            DBADMIN.PAL_KM_PARAMS,
            DBADMIN.PAL_KMEANS_RESULT,
            DBADMIN.PAL_KMEANS_CENTROIDS)""")
        conn.commit()
        print(f"  OK en {time.time()-t0:.1f}s - base")
        overload = "base"
    except Exception as e:
        print(f"  FAIL base: {e}")

# ── Si aun falla, intentar OVERLOAD_2_4 (6 tablas) ────────────
if not overload:
    print("\nIntentando KMEANS__OVERLOAD_2_4 (6 tablas)...")
    try:
        cur.execute("""CREATE COLUMN TABLE DBADMIN.PAL_KM_CENTERSTATS (
            CLUSTER_ID INTEGER, ATTR_NAME NVARCHAR(256),
            MEAN DOUBLE, VARIANCE DOUBLE)""")
        cur.execute("""CREATE COLUMN TABLE DBADMIN.PAL_KM_STATS (
            STAT_NAME NVARCHAR(256), STAT_VALUE NVARCHAR(1000))""")
        conn.commit()
        t0 = time.time()
        cur.execute("""CALL _SYS_AFL.PAL_KMEANS__OVERLOAD_2_4(
            DBADMIN.PAL_KMEANS_INPUT,
            DBADMIN.PAL_KM_PARAMS,
            DBADMIN.PAL_KMEANS_RESULT,
            DBADMIN.PAL_KMEANS_CENTROIDS,
            DBADMIN.PAL_KM_CENTERSTATS,
            DBADMIN.PAL_KM_STATS)""")
        conn.commit()
        print(f"  OK en {time.time()-t0:.1f}s - overload_2_4")
        overload = "2_4"
    except Exception as e:
        print(f"  FAIL overload_2_4: {e}")

print(f"\nResultado: overload={overload}")

# ── Si K-Means OK, escribir resultados en CARTAS_NATALES ──────
if overload:
    cur.execute("SELECT COUNT(*) FROM DBADMIN.PAL_KMEANS_RESULT")
    n = cur.fetchone()[0]
    print(f"\nPAL_KMEANS_RESULT: {n} filas")

    cur.execute("""UPDATE DBADMIN.CARTAS_NATALES
        SET CLUSTER_ID = R.CLUSTER_ID,
            DIST_TO_CENTER = R.DISTANCE
        FROM DBADMIN.PAL_KMEANS_RESULT R
        WHERE DBADMIN.CARTAS_NATALES.ID = R.ID""")
    conn.commit()
    print("CLUSTER_ID y DIST_TO_CENTER escritos en CARTAS_NATALES")

    # Distribucion por cluster
    cur.execute("""SELECT CLUSTER_ID, COUNT(*) AS N
        FROM DBADMIN.PAL_KMEANS_RESULT
        GROUP BY CLUSTER_ID ORDER BY CLUSTER_ID""")
    print("\nDistribucion de clusters:")
    for r in cur.fetchall():
        bar = "█" * (r[1]//30)
        print(f"  Cluster {r[0]:2d}: {r[1]:4d} cartas  {bar}")
else:
    print("\nERROR: ningún overload funcionó.")

cur.close()
conn.close()
print("\nHecho.")
