"""
run_kmeans_hanaml.py — PAL K-Means via hana-ml (API oficial SAP)
================================================================
hana-ml abstrae toda la complejidad de firmas PAL.
Funciona directamente con DBADMIN sin stored procedures.
"""
import time
from hana_ml import dataframe as hd
from hana_ml.algorithms.pal.clustering import KMeans
from hdbcli import dbapi

HOST = "20178d0a-d4af-4825-bba6-11a2aa151d20.hna1.prod-us10.hanacloud.ondemand.com"
PORT = 443
USER = "DBADMIN"
PASS = "Edu01edu."

FEATURES = [
    'SOL_GR','LUNA_GR','MERCURIO_GR','VENUS_GR','MARTE_GR','JUPITER_GR',
    'SATURNO_GR','URANO_GR','NEPTUNO_GR','PLUTON_GR','ASC_GR','MC_GR'
]

print("Conectando via hana-ml...")
conn = hd.ConnectionContext(
    address=HOST, port=PORT, user=USER, password=PASS,
    encrypt=True, sslValidateCertificate=False)
print(f"Conectado OK — HANA {conn.hana_version()}")

print("\nCargando CARTAS_NATALES como HANA DataFrame...")
hdf = hd.DataFrame(conn, 'SELECT * FROM DBADMIN.CARTAS_NATALES WHERE CALC_ERROR=0')
print(f"  {hdf.count()} filas, {len(hdf.columns)} columnas")

hdf_feat = hdf.select(['ID'] + FEATURES)

print("\nLanzando K-Means PAL (k=12, kmeans++, z-score)...")
t0 = time.time()
km = KMeans(
    n_clusters=12,
    init='patent',
    max_iter=300,
    distance_level='euclidean',
    normalization='z-score',
    tol=1e-6,
    thread_ratio=0.5
)
km.fit(hdf_feat, key='ID')
print(f"K-Means completado en {time.time()-t0:.1f}s")

print("\n── Labels (primeras 5 filas) ──")
labels_hdf = km.labels_
print(labels_hdf.head(5).collect().to_string())

print("\n── Centroides ──")
centers_df = km.cluster_centers_.collect()
centers_df.columns = [c.upper() for c in centers_df.columns]
print(centers_df.to_string())

# ── Guardar resultados en HANA ────────────────────────────────
print("\nGuardando en HANA...")
raw = dbapi.connect(address=HOST, port=PORT, user=USER, password=PASS,
                    encrypt=True, sslValidateCertificate=False)
cur = raw.cursor()

for t in ["PAL_KMEANS_RESULT", "PAL_KMEANS_CENTROIDS"]:
    try: cur.execute(f"DROP TABLE DBADMIN.{t}"); raw.commit()
    except: pass

# Labels → tabla resultado
labels_df = labels_hdf.collect()
labels_df.columns = [c.upper() for c in labels_df.columns]
print(f"  Labels cols: {list(labels_df.columns)}")

id_col  = 'ID'
cid_col = [c for c in labels_df.columns if 'CLUSTER' in c][0]
dist_col = next((c for c in labels_df.columns if 'DIST' in c or 'DIST' in c.upper()), None)

cur.execute("CREATE COLUMN TABLE DBADMIN.PAL_KMEANS_RESULT (ID INTEGER, CLUSTER_ID INTEGER, DISTANCE DOUBLE)")
rows = [(int(r[id_col]), int(r[cid_col]),
         float(r[dist_col]) if dist_col else 0.0) for _, r in labels_df.iterrows()]
cur.executemany("INSERT INTO DBADMIN.PAL_KMEANS_RESULT VALUES (?,?,?)", rows)
raw.commit()
print(f"  {len(rows)} asignaciones guardadas")

# Centroides → tabla long
cid_c = [c for c in centers_df.columns if 'CLUSTER' in c][0]
center_rows = []
for _, r in centers_df.iterrows():
    cid = int(r[cid_c])
    for feat in FEATURES:
        fu = feat.upper()
        if fu in centers_df.columns:
            center_rows.append((cid, feat, float(r[fu])))

cur.execute("CREATE COLUMN TABLE DBADMIN.PAL_KMEANS_CENTROIDS (CLUSTER_ID INTEGER, ATTR_NAME NVARCHAR(50), CENTROID_VAL DOUBLE)")
cur.executemany("INSERT INTO DBADMIN.PAL_KMEANS_CENTROIDS VALUES (?,?,?)", center_rows)
raw.commit()
print(f"  {len(center_rows)} valores de centroides guardados")

# ── Actualizar CARTAS_NATALES con CLUSTER_ID ──────────────────
cur.execute("""UPDATE DBADMIN.CARTAS_NATALES C
    SET CLUSTER_ID = R.CLUSTER_ID, DIST_TO_CENTER = R.DISTANCE
    FROM DBADMIN.PAL_KMEANS_RESULT R
    WHERE C.ID = R.ID""")
raw.commit()
print("  CLUSTER_ID escrito en CARTAS_NATALES ✓")

# ── Distribucion por cluster ───────────────────────────────────
cur.execute("""SELECT CLUSTER_ID, COUNT(*) AS N
    FROM DBADMIN.PAL_KMEANS_RESULT GROUP BY CLUSTER_ID ORDER BY CLUSTER_ID""")
print("\nDistribucion de clusters:")
for cid, n in cur.fetchall():
    bar = "█" * (n // 35)
    print(f"  Cluster {cid:2d}: {n:5d}  {bar}")

cur.close(); raw.close(); conn.close()
print("\n✓ Todo completado.")
