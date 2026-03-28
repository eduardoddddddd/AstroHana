"""
02_pal_kmeans.py — PAL K-Means sobre CARTAS_NATALES
====================================================
Ejecuta K-Means dentro de HANA Cloud (sin mover datos).
Guarda CLUSTER_ID y DIST_TO_CENTER en CARTAS_NATALES.
"""
import os
from hdbcli import dbapi
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), '..', '.env'))

HANA_HOST = os.getenv("HANA_HOST")
HANA_PORT = int(os.getenv("HANA_PORT", 443))
HANA_USER = os.getenv("HANA_USER")
HANA_PASS = os.getenv("HANA_PASS")
N_CLUSTERS = 12  # uno por cada signo — ajustable

def conectar():
    return dbapi.connect(address=HANA_HOST, port=HANA_PORT,
        user=HANA_USER, password=HANA_PASS,
        encrypt=True, sslValidateCertificate=False)

def run_kmeans(conn, n_clusters=N_CLUSTERS):
    cur = conn.cursor()

    # 1. Tabla de input PAL (formato long: ID, ATTR_NAME, ATTR_VAL)
    print("  Preparando tabla de features...")
    cur.execute("DROP TABLE DBADMIN.PAL_KMEANS_INPUT IF EXISTS")
    cur.execute("""
CREATE COLUMN TABLE DBADMIN.PAL_KMEANS_INPUT AS (
    SELECT ID, 'SOL'      AS ATTR_NAME, SOL_GR      AS ATTR_VAL FROM DBADMIN.CARTAS_NATALES WHERE CALC_ERROR=0
    UNION ALL SELECT ID, 'LUNA',     LUNA_GR     FROM DBADMIN.CARTAS_NATALES WHERE CALC_ERROR=0
    UNION ALL SELECT ID, 'MERCURIO', MERCURIO_GR FROM DBADMIN.CARTAS_NATALES WHERE CALC_ERROR=0
    UNION ALL SELECT ID, 'VENUS',    VENUS_GR    FROM DBADMIN.CARTAS_NATALES WHERE CALC_ERROR=0
    UNION ALL SELECT ID, 'MARTE',    MARTE_GR    FROM DBADMIN.CARTAS_NATALES WHERE CALC_ERROR=0
    UNION ALL SELECT ID, 'JUPITER',  JUPITER_GR  FROM DBADMIN.CARTAS_NATALES WHERE CALC_ERROR=0
    UNION ALL SELECT ID, 'SATURNO',  SATURNO_GR  FROM DBADMIN.CARTAS_NATALES WHERE CALC_ERROR=0
    UNION ALL SELECT ID, 'URANO',    URANO_GR    FROM DBADMIN.CARTAS_NATALES WHERE CALC_ERROR=0
    UNION ALL SELECT ID, 'NEPTUNO',  NEPTUNO_GR  FROM DBADMIN.CARTAS_NATALES WHERE CALC_ERROR=0
    UNION ALL SELECT ID, 'PLUTON',   PLUTON_GR   FROM DBADMIN.CARTAS_NATALES WHERE CALC_ERROR=0
    UNION ALL SELECT ID, 'ASC',      ASC_GR      FROM DBADMIN.CARTAS_NATALES WHERE CALC_ERROR=0
    UNION ALL SELECT ID, 'MC',       MC_GR       FROM DBADMIN.CARTAS_NATALES WHERE CALC_ERROR=0
)""")
    conn.commit()
    print(f"  Features cargadas. Lanzando PAL K-Means (k={n_clusters})...")

    # 2. Parámetros
    cur.execute("DROP TABLE DBADMIN.PAL_KMEANS_PARAMS IF EXISTS")
    cur.execute("""CREATE LOCAL TEMPORARY TABLE #PAL_KMEANS_PARAMS
        (NAME NVARCHAR(256), INTVAL INTEGER, DBLVAL DOUBLE, STRVAL NVARCHAR(1000))""")
    params = [
        ("THREAD_RATIO",   None, 0.5,  None),
        ("N_CLUSTERS",     n_clusters, None, None),
        ("MAX_ITER",       300, None, None),
        ("INIT",           None, None, "KMEANS++"),
        ("DISTANCE_LEVEL", 2,   None, None),   # 2 = Euclidea
        ("NORMALIZATION",  1,   None, None),   # Z-score
        ("EXIT_THRESHOLD", None, 1e-6, None),
    ]
    cur.executemany("INSERT INTO #PAL_KMEANS_PARAMS VALUES (?,?,?,?)", params)

    # 3. Tablas de output
    for t in ["PAL_KMEANS_RESULT","PAL_KMEANS_CENTROIDS","PAL_KMEANS_STATS"]:
        cur.execute(f"DROP TABLE DBADMIN.{t} IF EXISTS")
    cur.execute("CREATE COLUMN TABLE DBADMIN.PAL_KMEANS_RESULT (ID INTEGER, CLUSTER_ID INTEGER, DISTANCE DOUBLE)")
    cur.execute("CREATE COLUMN TABLE DBADMIN.PAL_KMEANS_CENTROIDS (CLUSTER_ID INTEGER, ATTR_NAME NVARCHAR(256), CENTROID_VAL DOUBLE)")
    cur.execute("CREATE COLUMN TABLE DBADMIN.PAL_KMEANS_STATS (STAT_NAME NVARCHAR(256), STAT_VALUE NVARCHAR(1000))")
    conn.commit()

    # 4. EJECUTAR PAL
    cur.execute("""CALL _SYS_AFL.PAL_KMEANS(
        DBADMIN.PAL_KMEANS_INPUT,
        #PAL_KMEANS_PARAMS,
        DBADMIN.PAL_KMEANS_RESULT,
        DBADMIN.PAL_KMEANS_CENTROIDS,
        DBADMIN.PAL_KMEANS_STATS)""")
    conn.commit()
    print("  PAL K-Means completado.")

    # 5. Escribir resultados en tabla principal
    cur.execute("""UPDATE DBADMIN.CARTAS_NATALES C
        SET CLUSTER_ID = R.CLUSTER_ID, DIST_TO_CENTER = R.DISTANCE
        FROM DBADMIN.PAL_KMEANS_RESULT R WHERE C.ID = R.ID""")
    conn.commit()

    cur.execute("SELECT CLUSTER_ID, COUNT(*) AS N FROM DBADMIN.PAL_KMEANS_RESULT GROUP BY CLUSTER_ID ORDER BY CLUSTER_ID")
    rows = cur.fetchall()
    print(f"\n  Distribución de clusters:")
    for cluster_id, n in rows:
        print(f"    Cluster {cluster_id:2d}: {n:4d} cartas")

    cur.close()
    print(f"\n✓ K-Means completado. {n_clusters} clusters asignados a CARTAS_NATALES.")

if __name__ == "__main__":
    conn = conectar()
    run_kmeans(conn)
    conn.close()
