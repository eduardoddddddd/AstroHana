"""
run_kmeans_v2.py — PAL K-Means via bloque DO BEGIN (HANA Cloud 4.0)
"""
import os
import time

from dotenv import load_dotenv
from hdbcli import dbapi

load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))

HOST = os.getenv("HANA_HOST")
PORT = int(os.getenv("HANA_PORT", 443))
USER = os.getenv("HANA_USER")
PASS = os.getenv("HANA_PASS")


def require_env(name: str, value: str | None) -> str:
    if value:
        return value
    raise RuntimeError(f"Falta la variable de entorno requerida: {name}")


def main():
    host = require_env("HANA_HOST", HOST)
    user = require_env("HANA_USER", USER)
    password = require_env("HANA_PASS", PASS)

    conn = dbapi.connect(
        address=host,
        port=PORT,
        user=user,
        password=password,
        encrypt=True,
        sslValidateCertificate=False,
    )
    cur = conn.cursor()
    print("Conectado OK")

    for table_name in ["PAL_KMEANS_RESULT", "PAL_KMEANS_CENTROIDS", "PAL_KM_PARAMS"]:
        try:
            cur.execute(f"DROP TABLE DBADMIN.{table_name}")
            conn.commit()
        except Exception:
            pass

    cur.execute(
        """CREATE COLUMN TABLE DBADMIN.PAL_KM_PARAMS (
        NAME NVARCHAR(256), INTVAL INTEGER, DBLVAL DOUBLE, STRVAL NVARCHAR(1000))"""
    )
    cur.executemany(
        "INSERT INTO DBADMIN.PAL_KM_PARAMS VALUES (?,?,?,?)",
        [
            ("N_CLUSTERS", 12, None, None),
            ("MAX_ITER", 300, None, None),
            ("INIT", None, None, "KMEANS++"),
            ("DISTANCE_LEVEL", 2, None, None),
            ("NORMALIZATION", 1, None, None),
            ("EXIT_THRESHOLD", None, 1e-6, None),
        ],
    )
    conn.commit()
    print("Params OK")

    cur.execute(
        """CREATE COLUMN TABLE DBADMIN.PAL_KMEANS_RESULT (
        ID INTEGER, CLUSTER_ID INTEGER, DISTANCE DOUBLE)"""
    )
    cur.execute(
        """CREATE COLUMN TABLE DBADMIN.PAL_KMEANS_CENTROIDS (
        CLUSTER_ID INTEGER, ATTR_NAME NVARCHAR(256), CENTROID_VAL DOUBLE)"""
    )
    conn.commit()
    print("Tablas output creadas")

    do_block = """
DO BEGIN
    DECLARE lt_params TABLE (
        NAME NVARCHAR(256), INTVAL INTEGER, DBLVAL DOUBLE, STRVAL NVARCHAR(1000));
    lt_params = SELECT NAME, INTVAL, DBLVAL, STRVAL FROM DBADMIN.PAL_KM_PARAMS;

    DECLARE lt_result TABLE (ID INTEGER, CLUSTER_ID INTEGER, DISTANCE DOUBLE);

    DECLARE lt_centers TABLE (
        CLUSTER_ID INTEGER, ATTR_NAME NVARCHAR(256), CENTROID_VAL DOUBLE);

    CALL _SYS_AFL.PAL_KMEANS(
        DBADMIN.PAL_KMEANS_INPUT,
        :lt_params,
        lt_result,
        lt_centers
    );

    INSERT INTO DBADMIN.PAL_KMEANS_RESULT SELECT * FROM :lt_result;
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
    except Exception as exc:
        print(f"FAIL DO BEGIN: {exc}")
        success = False

    if not success:
        print("\nIntentando con PAL_ACCELERATEDKMEANS...")
        do_block2 = """
DO BEGIN
    DECLARE lt_params TABLE (
        NAME NVARCHAR(256), INTVAL INTEGER, DBLVAL DOUBLE, STRVAL NVARCHAR(1000));
    lt_params = SELECT NAME, INTVAL, DBLVAL, STRVAL FROM DBADMIN.PAL_KM_PARAMS;
    DECLARE lt_result TABLE (ID INTEGER, CLUSTER_ID INTEGER, DISTANCE DOUBLE);
    DECLARE lt_centers TABLE (
        CLUSTER_ID INTEGER, ATTR_NAME NVARCHAR(256), CENTROID_VAL DOUBLE);
    CALL _SYS_AFL.PAL_ACCELERATEDKMEANS(
        DBADMIN.PAL_KMEANS_INPUT,
        :lt_params,
        lt_result,
        lt_centers
    );
    INSERT INTO DBADMIN.PAL_KMEANS_RESULT SELECT * FROM :lt_result;
    INSERT INTO DBADMIN.PAL_KMEANS_CENTROIDS SELECT * FROM :lt_centers;
END;
"""
        try:
            t0 = time.time()
            cur.execute(do_block2)
            conn.commit()
            print(f"ACCELERATEDKMEANS OK en {time.time() - t0:.1f}s")
            success = True
        except Exception as exc:
            print(f"FAIL ACCELERATEDKMEANS: {exc}")

    if success:
        cur.execute("SELECT COUNT(*) FROM DBADMIN.PAL_KMEANS_RESULT")
        count = cur.fetchone()[0]
        print(f"\nPAL_KMEANS_RESULT: {count} asignaciones")

        cur.execute(
            """UPDATE DBADMIN.CARTAS_NATALES
            SET CLUSTER_ID = R.CLUSTER_ID,
                DIST_TO_CENTER = R.DISTANCE
            FROM DBADMIN.PAL_KMEANS_RESULT R
            WHERE DBADMIN.CARTAS_NATALES.ID = R.ID"""
        )
        conn.commit()
        print("CLUSTER_ID y DIST_TO_CENTER guardados en CARTAS_NATALES")

        cur.execute(
            """SELECT CLUSTER_ID, COUNT(*) AS N
            FROM DBADMIN.PAL_KMEANS_RESULT
            GROUP BY CLUSTER_ID ORDER BY CLUSTER_ID"""
        )
        rows = cur.fetchall()
        print(f"\nDistribucion ({len(rows)} clusters):")
        for cluster_id, row_count in rows:
            bar = "#" * (row_count // 30)
            print(f"  Cluster {cluster_id:2d}: {row_count:5d}  {bar}")
    else:
        print("\nTodos los intentos fallaron. Ver errores arriba.")

    cur.close()
    conn.close()
    print("\nHecho.")


if __name__ == "__main__":
    main()
