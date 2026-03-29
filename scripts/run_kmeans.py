"""run_kmeans.py — ejecuta PAL K-Means sobre CARTAS_NATALES."""
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
    params = [
        ("N_CLUSTERS", 12, None, None),
        ("MAX_ITER", 300, None, None),
        ("INIT", None, None, "KMEANS++"),
        ("DISTANCE_LEVEL", 2, None, None),
        ("NORMALIZATION", 1, None, None),
        ("EXIT_THRESHOLD", None, 1e-6, None),
    ]
    cur.executemany("INSERT INTO DBADMIN.PAL_KM_PARAMS VALUES (?,?,?,?)", params)
    conn.commit()
    print(f"Params insertados: {len(params)}")

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

    print("\nIntentando KMEANS__OVERLOAD_2_1 (solo assignments)...")
    overload = None
    try:
        t0 = time.time()
        cur.execute(
            """CALL _SYS_AFL.PAL_KMEANS__OVERLOAD_2_1(
            DBADMIN.PAL_KMEANS_INPUT,
            DBADMIN.PAL_KM_PARAMS,
            DBADMIN.PAL_KMEANS_RESULT)"""
        )
        conn.commit()
        print(f"  OK en {time.time() - t0:.1f}s - overload_2_1")
        overload = "2_1"
    except Exception as exc:
        print(f"  FAIL overload_2_1: {exc}")

    if not overload:
        print("\nIntentando KMEANS base (4 tablas)...")
        try:
            t0 = time.time()
            cur.execute(
                """CALL _SYS_AFL.PAL_KMEANS(
                DBADMIN.PAL_KMEANS_INPUT,
                DBADMIN.PAL_KM_PARAMS,
                DBADMIN.PAL_KMEANS_RESULT,
                DBADMIN.PAL_KMEANS_CENTROIDS)"""
            )
            conn.commit()
            print(f"  OK en {time.time() - t0:.1f}s - base")
            overload = "base"
        except Exception as exc:
            print(f"  FAIL base: {exc}")

    if not overload:
        print("\nIntentando KMEANS__OVERLOAD_2_4 (6 tablas)...")
        try:
            cur.execute(
                """CREATE COLUMN TABLE DBADMIN.PAL_KM_CENTERSTATS (
                CLUSTER_ID INTEGER, ATTR_NAME NVARCHAR(256),
                MEAN DOUBLE, VARIANCE DOUBLE)"""
            )
            cur.execute(
                """CREATE COLUMN TABLE DBADMIN.PAL_KM_STATS (
                STAT_NAME NVARCHAR(256), STAT_VALUE NVARCHAR(1000))"""
            )
            conn.commit()
            t0 = time.time()
            cur.execute(
                """CALL _SYS_AFL.PAL_KMEANS__OVERLOAD_2_4(
                DBADMIN.PAL_KMEANS_INPUT,
                DBADMIN.PAL_KM_PARAMS,
                DBADMIN.PAL_KMEANS_RESULT,
                DBADMIN.PAL_KMEANS_CENTROIDS,
                DBADMIN.PAL_KM_CENTERSTATS,
                DBADMIN.PAL_KM_STATS)"""
            )
            conn.commit()
            print(f"  OK en {time.time() - t0:.1f}s - overload_2_4")
            overload = "2_4"
        except Exception as exc:
            print(f"  FAIL overload_2_4: {exc}")

    print(f"\nResultado: overload={overload}")

    if overload:
        cur.execute("SELECT COUNT(*) FROM DBADMIN.PAL_KMEANS_RESULT")
        count = cur.fetchone()[0]
        print(f"\nPAL_KMEANS_RESULT: {count} filas")

        cur.execute(
            """UPDATE DBADMIN.CARTAS_NATALES
            SET CLUSTER_ID = R.CLUSTER_ID,
                DIST_TO_CENTER = R.DISTANCE
            FROM DBADMIN.PAL_KMEANS_RESULT R
            WHERE DBADMIN.CARTAS_NATALES.ID = R.ID"""
        )
        conn.commit()
        print("CLUSTER_ID y DIST_TO_CENTER escritos en CARTAS_NATALES")

        cur.execute(
            """SELECT CLUSTER_ID, COUNT(*) AS N
            FROM DBADMIN.PAL_KMEANS_RESULT
            GROUP BY CLUSTER_ID ORDER BY CLUSTER_ID"""
        )
        print("\nDistribucion de clusters:")
        for cluster_id, row_count in cur.fetchall():
            bar = "#" * (row_count // 30)
            print(f"  Cluster {cluster_id:2d}: {row_count:4d} cartas  {bar}")
    else:
        print("\nERROR: ningun overload funciono.")

    cur.close()
    conn.close()
    print("\nHecho.")


if __name__ == "__main__":
    main()
