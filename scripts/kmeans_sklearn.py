"""
kmeans_sklearn.py — K-Means con scikit-learn, resultados a HANA
Lee las posiciones planetarias de HANA, clusteriza localmente
y sube CLUSTER_ID a HANA.
"""
import os
import time

import numpy as np
import pandas as pd
from dotenv import load_dotenv
from hdbcli import dbapi
from sklearn.cluster import KMeans
from sklearn.preprocessing import MinMaxScaler

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

    print("Conectando a HANA...")
    conn = dbapi.connect(
        address=host,
        port=PORT,
        user=user,
        password=password,
        encrypt=True,
        sslValidateCertificate=False,
    )
    cur = conn.cursor()

    print("Descargando posiciones planetarias...")
    t0 = time.time()
    cur.execute(f"SELECT ID, {','.join(FEATURES)} FROM DBADMIN.CARTAS_NATALES WHERE CALC_ERROR=0")
    rows = cur.fetchall()
    df = pd.DataFrame(rows, columns=["ID"] + FEATURES)
    print(f"  {len(df)} cartas descargadas en {time.time() - t0:.1f}s")

    print("\nEjecutando K-Means (k=12, init=k-means++, normalizacion min-max)...")
    t0 = time.time()
    scaler = MinMaxScaler()
    x_values = scaler.fit_transform(df[FEATURES].values)

    km = KMeans(
        n_clusters=12,
        init="k-means++",
        n_init=10,
        max_iter=300,
        tol=1e-6,
        random_state=42,
    )
    km.fit(x_values)
    elapsed = time.time() - t0
    print(f"K-Means completado en {elapsed:.1f}s")

    df["CLUSTER_ID"] = km.labels_
    centers = km.cluster_centers_
    dists = np.linalg.norm(x_values - centers[km.labels_], axis=1)
    df["DISTANCE"] = dists

    print("\nSubiendo resultados a HANA...")
    batch = [
        (int(row["CLUSTER_ID"]), float(row["DISTANCE"]), int(row["ID"]))
        for _, row in df.iterrows()
    ]
    cur.executemany(
        """UPDATE DBADMIN.CARTAS_NATALES
        SET CLUSTER_ID=?, DIST_TO_CENTER=? WHERE ID=?""",
        batch,
    )
    conn.commit()
    print(f"  {len(batch)} registros actualizados en CARTAS_NATALES")

    for table_name in ["PAL_KMEANS_RESULT", "PAL_KMEANS_CENTROIDS"]:
        try:
            cur.execute(f"DROP TABLE DBADMIN.{table_name}")
            conn.commit()
        except Exception:
            pass

    cur.execute(
        """CREATE COLUMN TABLE DBADMIN.PAL_KMEANS_RESULT (
        ID INTEGER, CLUSTER_ID INTEGER, DISTANCE DOUBLE)"""
    )
    res_rows = [
        (int(row["ID"]), int(row["CLUSTER_ID"]), float(row["DISTANCE"]))
        for _, row in df.iterrows()
    ]
    cur.executemany("INSERT INTO DBADMIN.PAL_KMEANS_RESULT VALUES (?,?,?)", res_rows)

    cur.execute(
        """CREATE COLUMN TABLE DBADMIN.PAL_KMEANS_CENTROIDS (
        CLUSTER_ID INTEGER, ATTR_NAME NVARCHAR(50), CENTROID_VAL DOUBLE)"""
    )
    centers_real = scaler.inverse_transform(centers)
    cen_rows = []
    for cid, row in enumerate(centers_real):
        for feature, value in zip(FEATURES, row):
            cen_rows.append((cid, feature, float(value)))
    cur.executemany("INSERT INTO DBADMIN.PAL_KMEANS_CENTROIDS VALUES (?,?,?)", cen_rows)
    conn.commit()
    print(f"  {len(cen_rows)} valores de centroides guardados")

    cur.execute(
        """SELECT CLUSTER_ID, COUNT(*) AS N FROM DBADMIN.PAL_KMEANS_RESULT
        GROUP BY CLUSTER_ID ORDER BY CLUSTER_ID"""
    )
    print("\n==== Distribucion de clusters ====")
    for cid, count in cur.fetchall():
        print(f"  Cluster {cid:2d}: {count:5d}  {'#' * (count // 30)}")

    cur.close()
    conn.close()
    print("\nK-Means completado. CLUSTER_ID en HANA listo para queries.")


if __name__ == "__main__":
    main()
