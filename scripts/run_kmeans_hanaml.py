"""
run_kmeans_hanaml.py — PAL K-Means via hana-ml (API oficial SAP)
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

    print("Conectando via hana-ml...")
    conn = hd.ConnectionContext(
        address=host,
        port=PORT,
        user=user,
        password=password,
        encrypt=True,
        sslValidateCertificate=False,
    )
    print(f"Conectado OK - HANA {conn.hana_version()}")

    print("\nCargando CARTAS_NATALES como HANA DataFrame...")
    hdf = hd.DataFrame(conn, "SELECT * FROM DBADMIN.CARTAS_NATALES WHERE CALC_ERROR=0")
    print(f"  {hdf.count()} filas, {len(hdf.columns)} columnas")

    hdf_feat = hdf.select(["ID"] + FEATURES)

    print("\nLanzando K-Means PAL (k=12, kmeans++, z-score)...")
    t0 = time.time()
    km = KMeans(
        n_clusters=12,
        init="patent",
        max_iter=300,
        distance_level="euclidean",
        normalization="z-score",
        tol=1e-6,
        thread_ratio=0.5,
    )
    km.fit(hdf_feat, key="ID")
    print(f"K-Means completado en {time.time() - t0:.1f}s")

    print("\n-- Labels (primeras 5 filas) --")
    labels_hdf = km.labels_
    print(labels_hdf.head(5).collect().to_string())

    print("\n-- Centroides --")
    centers_df = km.cluster_centers_.collect()
    centers_df.columns = [column.upper() for column in centers_df.columns]
    print(centers_df.to_string())

    print("\nGuardando en HANA...")
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

    labels_df = labels_hdf.collect()
    labels_df.columns = [column.upper() for column in labels_df.columns]
    print(f"  Labels cols: {list(labels_df.columns)}")

    id_col = "ID"
    cid_col = next(column for column in labels_df.columns if "CLUSTER" in column)
    dist_col = next((column for column in labels_df.columns if "DIST" in column), None)

    cur.execute(
        "CREATE COLUMN TABLE DBADMIN.PAL_KMEANS_RESULT (ID INTEGER, CLUSTER_ID INTEGER, DISTANCE DOUBLE)"
    )
    rows = [
        (
            int(row[id_col]),
            int(row[cid_col]),
            float(row[dist_col]) if dist_col else 0.0,
        )
        for _, row in labels_df.iterrows()
    ]
    cur.executemany("INSERT INTO DBADMIN.PAL_KMEANS_RESULT VALUES (?,?,?)", rows)
    raw.commit()
    print(f"  {len(rows)} asignaciones guardadas")

    cid_col_centers = next(column for column in centers_df.columns if "CLUSTER" in column)
    center_rows = []
    for _, row in centers_df.iterrows():
        cid = int(row[cid_col_centers])
        for feature in FEATURES:
            feature_upper = feature.upper()
            if feature_upper in centers_df.columns:
                center_rows.append((cid, feature, float(row[feature_upper])))

    cur.execute(
        "CREATE COLUMN TABLE DBADMIN.PAL_KMEANS_CENTROIDS (CLUSTER_ID INTEGER, ATTR_NAME NVARCHAR(50), CENTROID_VAL DOUBLE)"
    )
    cur.executemany("INSERT INTO DBADMIN.PAL_KMEANS_CENTROIDS VALUES (?,?,?)", center_rows)
    raw.commit()
    print(f"  {len(center_rows)} valores de centroides guardados")

    cur.execute(
        """UPDATE DBADMIN.CARTAS_NATALES C
        SET CLUSTER_ID = R.CLUSTER_ID, DIST_TO_CENTER = R.DISTANCE
        FROM DBADMIN.PAL_KMEANS_RESULT R
        WHERE C.ID = R.ID"""
    )
    raw.commit()
    print("  CLUSTER_ID escrito en CARTAS_NATALES")

    cur.execute(
        """SELECT CLUSTER_ID, COUNT(*) AS N
        FROM DBADMIN.PAL_KMEANS_RESULT GROUP BY CLUSTER_ID ORDER BY CLUSTER_ID"""
    )
    print("\nDistribucion de clusters:")
    for cid, count in cur.fetchall():
        bar = "#" * (count // 35)
        print(f"  Cluster {cid:2d}: {count:5d}  {bar}")

    cur.close()
    raw.close()
    conn.close()
    print("\nTodo completado.")


if __name__ == "__main__":
    main()
