"""
run_kmeans_v2.py — PAL K-Means via bloque DO BEGIN (HANA Cloud 4.0)
====================================================================
En HANA Cloud el PAL se invoca dentro de bloques SQLScript anónimos.
El CALL directo desde hdbcli falla por binding de parámetros.
"""
import time
from hdbcli import dbapi

conn = dbapi.connect(
    address="20178d0a-d4af-4825-bba6-11a2aa151d20.hna1.prod-us10.hanacloud.ondemand.com",
    port=443, user="DBADMIN", password="Edu01edu.",
    encrypt=True, sslValidateCertificate=False)
cur = conn.cursor()
print("Conectado OK")

# ── Limpiar tablas de output previas ─────────────────────────
for t in ["PAL_KMEANS_RESULT","PAL_KMEANS_CENTROIDS","PAL_KM_PARAMS"]:
    try: cur.execute(f"DROP TABLE DBADMIN.{t}"); conn.commit()
    except: pass

# ── Tabla de parametros persistente ──────────────────────────
cur.execute("""CREATE COLUMN TABLE DBADMIN.PAL_KM_PARAMS (
    NAME NVARCHAR(256), INTVAL INTEGER, DBLVAL DOUBLE, STRVAL NVARCHAR(1000))""")
cur.executemany("INSERT INTO DBADMIN.PAL_KM_PARAMS VALUES (?,?,?,?)", [
    ("N_CLUSTERS",     12,   None,  None),
    ("MAX_ITER",       300,  None,  None),
    ("INIT",           None, None,  "KMEANS++"),
    ("DISTANCE_LEVEL", 2,    None,  None),
    ("NORMALIZATION",  1,    None,  None),
    ("EXIT_THRESHOLD", None, 1e-6,  None),
])
conn.commit()
print("Params OK")

# ── Tablas de output ──────────────────────────────────────────
cur.execute("""CREATE COLUMN TABLE DBADMIN.PAL_KMEANS_RESULT (
    ID INTEGER, CLUSTER_ID INTEGER, DISTANCE DOUBLE)""")
cur.execute("""CREATE COLUMN TABLE DBADMIN.PAL_KMEANS_CENTROIDS (
    CLUSTER_ID INTEGER, ATTR_NAME NVARCHAR(256), CENTROID_VAL DOUBLE)""")
conn.commit()
print("Tablas output creadas")

# ── Bloque DO BEGIN: llamada PAL dentro de SQLScript ─────────
do_block = """
DO BEGIN
    -- Cargar parametros en variable tabla local
    DECLARE lt_params TABLE (
        NAME NVARCHAR(256), INTVAL INTEGER, DBLVAL DOUBLE, STRVAL NVARCHAR(1000));
    lt_params = SELECT NAME, INTVAL, DBLVAL, STRVAL FROM DBADMIN.PAL_KM_PARAMS;

    -- Resultado: asignacion de cluster por carta
    DECLARE lt_result TABLE (ID INTEGER, CLUSTER_ID INTEGER, DISTANCE DOUBLE);

    -- Resultado: centroides
    DECLARE lt_centers TABLE (
        CLUSTER_ID INTEGER, ATTR_NAME NVARCHAR(256), CENTROID_VAL DOUBLE);

    -- EJECUTAR K-Means PAL
    CALL _SYS_AFL.PAL_KMEANS(
        DBADMIN.PAL_KMEANS_INPUT,
        :lt_params,
        lt_result,
        lt_centers
    );

    -- Volcar resultados a tablas persistentes
    INSERT INTO DBADMIN.PAL_KMEANS_RESULT   SELECT * FROM :lt_result;
    INSERT INTO DBADMIN.PAL_KMEANS_CENTROIDS SELECT * FROM :lt_centers;
END;
"""

print("\nEjecutando PAL K-Means via DO BEGIN...")
t0 = time.time()
try:
    cur.execute(do_block)
    conn.commit()
    elapsed = time.time() - t0
    print(f"K-Means completado en {elapsed:.1f}s")
    success = True
except Exception as e:
    print(f"FAIL DO BEGIN: {e}")
    success = False

# ── Si DO BEGIN falla, intentar via ACCELERATEDKMEANS ─────────
if not success:
    print("\nIntentando con PAL_ACCELERATEDKMEANS...")
    do_block2 = """
DO BEGIN
    DECLARE lt_params TABLE (
        NAME NVARCHAR(256), INTVAL INTEGER, DBLVAL DOUBLE, STRVAL NVARCHAR(1000));
    lt_params = SELECT NAME, INTVAL, DBLVAL, STRVAL FROM DBADMIN.PAL_KM_PARAMS;
    DECLARE lt_result  TABLE (ID INTEGER, CLUSTER_ID INTEGER, DISTANCE DOUBLE);
    DECLARE lt_centers TABLE (
        CLUSTER_ID INTEGER, ATTR_NAME NVARCHAR(256), CENTROID_VAL DOUBLE);
    CALL _SYS_AFL.PAL_ACCELERATEDKMEANS(
        DBADMIN.PAL_KMEANS_INPUT,
        :lt_params,
        lt_result,
        lt_centers
    );
    INSERT INTO DBADMIN.PAL_KMEANS_RESULT    SELECT * FROM :lt_result;
    INSERT INTO DBADMIN.PAL_KMEANS_CENTROIDS SELECT * FROM :lt_centers;
END;
"""
    try:
        t0 = time.time()
        cur.execute(do_block2)
        conn.commit()
        print(f"ACCELERATEDKMEANS OK en {time.time()-t0:.1f}s")
        success = True
    except Exception as e:
        print(f"FAIL ACCELERATEDKMEANS: {e}")

# ── Resultados ─────────────────────────────────────────────────
if success:
    cur.execute("SELECT COUNT(*) FROM DBADMIN.PAL_KMEANS_RESULT")
    n = cur.fetchone()[0]
    print(f"\nPAL_KMEANS_RESULT: {n} asignaciones")

    # Escribir CLUSTER_ID en la tabla principal
    cur.execute("""UPDATE DBADMIN.CARTAS_NATALES
        SET CLUSTER_ID = R.CLUSTER_ID,
            DIST_TO_CENTER = R.DISTANCE
        FROM DBADMIN.PAL_KMEANS_RESULT R
        WHERE DBADMIN.CARTAS_NATALES.ID = R.ID""")
    conn.commit()
    print("CLUSTER_ID y DIST_TO_CENTER guardados en CARTAS_NATALES")

    # Distribucion por cluster
    cur.execute("""SELECT CLUSTER_ID, COUNT(*) AS N
        FROM DBADMIN.PAL_KMEANS_RESULT
        GROUP BY CLUSTER_ID ORDER BY CLUSTER_ID""")
    rows = cur.fetchall()
    print(f"\nDistribucion ({len(rows)} clusters):")
    for cid, n in rows:
        bar = "█" * (n // 30)
        print(f"  Cluster {cid:2d}: {n:5d}  {bar}")
else:
    print("\nTodos los intentos fallaron. Ver errores arriba.")

cur.close()
conn.close()
print("\nHecho.")
