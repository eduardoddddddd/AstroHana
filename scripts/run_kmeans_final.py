"""
run_kmeans_final.py — K-Means PAL via hana-ml 2.28 (parametros verificados)
init='patent', normalization='min_max', distance_level='euclidean'
"""
import os
import time

from dotenv import load_dotenv
from hana_ml import dataframe as hd
from hana_ml.algorithms.pal.clustering import KMeans
from hdbcli import dbapi

load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))

HOST = os.getenv("HANA_HOST")
PORT = int(os.getenv("HANA_PORT", 443))
USER = os.getenv("HANA_USER")
PASS = os.getenv("HANA_PASS")

FEATURES = [
    "SOL_GR", "LUNA_GR", "MERCURIO_GR", "VENUS_GR", "MARTE_GR", "JUPITER_GR",
    "SATURNO_GR", "URANO_GR", "NEPTUNO_GR", "PLUTON_GR", "ASC_GR", "MC_GR",
]


def require_env(name: str, value: str | None) -> str:
    if value:
        return value
    raise RuntimeError(f"Falta la variable de entorno requerida: {name}")


def main():
    host = require_env("HANA_HOST", HOST)
    user = require_env("HANA_USER", USER)
    password = require_env("HANA_PASS", PASS)

    print("Conectando...")
    conn = hd.ConnectionContext(
        address=host,
        port=PORT,
        user=user,
        password=password,
        encrypt=True,
        sslValidateCertificate=False,
    )
    print(f"OK - HANA {conn.hana_version()}")

    hdf = hd.DataFrame(conn, "SELECT * FROM DBADMIN.CARTAS_NATALES WHERE CALC_ERROR=0")
    hdf_feat = hdf.select(["ID"] + FEATURES)
    print(f"Dataset: {hdf_feat.count()} filas x {len(FEATURES)} features")

    print("\nLanzando K-Means PAL (k=12, patent/kmeans++, min_max, euclidean)...")
    t0 = time.time()
    km = KMeans(
        n_clusters=12,
        init="patent",
        max_iter=300,
        distance_level="euclidean",
        normalization="min_max",
        tol=1e-6,
        thread_ratio=0.5,
    )
    km.fit(hdf_feat, key="ID")
    elapsed = time.time() - t0
    print(f"K-Means completado en {elapsed:.1f}s")

    labels_hdf = km.labels_
    centers_hdf = km.cluster_centers_
    print(f"\nLabels columnas  : {labels_hdf.columns}")
    print(f"Centroides columnas: {centers_hdf.columns}")
    print("\nPrimeras 5 asignaciones:")
    print(labels_hdf.head(5).collect().to_string(index=False))

    print("\nGuardando resultados en HANA...")
    raw = dbapi.connect(
        address=host,
        port=PORT,
        user=user,
        password=password,
        encrypt=True,
        sslValidateCertificate=False,
    )
    cur = raw.cursor()

    for table_name in ["PAL_KMEANS_RESULT", "PAL_KMEANS_CENTROIDS"]:
        try:
            cur.execute(f"DROP TABLE DBADMIN.{table_name}")
            raw.commit()
        except Exception:
            pass

    ldf = labels_hdf.collect()
    ldf.columns = [column.upper() for column in ldf.columns]
    print(f"  Labels cols reales: {list(ldf.columns)}")

    id_col = next(column for column in ldf.columns if column in ("ID", "KEY"))
    cid_col = next(column for column in ldf.columns if "CLUSTER" in column)
    dist_col = next((column for column in ldf.columns if "DIST" in column), None)
    sil_col = next((column for column in ldf.columns if "SIL" in column), None)

    cur.execute(
        """CREATE COLUMN TABLE DBADMIN.PAL_KMEANS_RESULT
        (ID INTEGER, CLUSTER_ID INTEGER, DISTANCE DOUBLE, SILHOUETTE DOUBLE)"""
    )
    rows = []
    for _, row in ldf.iterrows():
        rows.append(
            (
                int(row[id_col]),
                int(row[cid_col]),
                float(row[dist_col]) if dist_col and dist_col in row.index else 0.0,
                float(row[sil_col]) if sil_col and sil_col in row.index else 0.0,
            )
        )
    cur.executemany("INSERT INTO DBADMIN.PAL_KMEANS_RESULT VALUES (?,?,?,?)", rows)
    raw.commit()
    print(f"  {len(rows)} asignaciones guardadas en PAL_KMEANS_RESULT")

    cdf = centers_hdf.collect()
    cdf.columns = [column.upper() for column in cdf.columns]
    print(f"  Centroides cols reales: {list(cdf.columns)}")

    ccid_col = next(column for column in cdf.columns if "CLUSTER" in column)
    cur.execute(
        """CREATE COLUMN TABLE DBADMIN.PAL_KMEANS_CENTROIDS
        (CLUSTER_ID INTEGER, ATTR_NAME NVARCHAR(50), CENTROID_VAL DOUBLE)"""
    )
    crows = []
    for _, row in cdf.iterrows():
        cid = int(row[ccid_col])
        for feature in FEATURES:
            feature_upper = feature.upper()
            if feature_upper in cdf.columns:
                crows.append((cid, feature, float(row[feature_upper])))
    if crows:
        cur.executemany("INSERT INTO DBADMIN.PAL_KMEANS_CENTROIDS VALUES (?,?,?)", crows)
        raw.commit()
        print(f"  {len(crows)} valores de centroides guardados")

    cur.execute(
        """UPDATE DBADMIN.CARTAS_NATALES C
        SET CLUSTER_ID = R.CLUSTER_ID, DIST_TO_CENTER = R.DISTANCE
        FROM DBADMIN.PAL_KMEANS_RESULT R WHERE C.ID = R.ID"""
    )
    raw.commit()
    print("  CLUSTER_ID + DIST_TO_CENTER escritos en CARTAS_NATALES")

    cur.execute(
        """SELECT CLUSTER_ID, COUNT(*) AS N
        FROM DBADMIN.PAL_KMEANS_RESULT GROUP BY CLUSTER_ID ORDER BY CLUSTER_ID"""
    )
    print("\n==== Distribucion de clusters ====")
    total = 0
    for cid, count in cur.fetchall():
        bar = "#" * (count // 30)
        print(f"  Cluster {cid:2d}: {count:5d}  {bar}")
        total += count
    print(f"  TOTAL: {total}")

    cur.close()
    raw.close()
    conn.close()
    print("\nK-Means PAL completado y persistido en HANA.")


if __name__ == "__main__":
    main()
