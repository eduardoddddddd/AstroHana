"""
run_kmeans_final.py — K-Means PAL via hana-ml 2.28 (parámetros verificados)
init='patent', normalization='min_max', distance_level=2 (euclidean)
"""
import time
from hana_ml import dataframe as hd
from hana_ml.algorithms.pal.clustering import KMeans
from hdbcli import dbapi

HOST = "20178d0a-d4af-4825-bba6-11a2aa151d20.hna1.prod-us10.hanacloud.ondemand.com"
PORT = 443; USER = "DBADMIN"; PASS = "Edu01edu."

FEATURES = ['SOL_GR','LUNA_GR','MERCURIO_GR','VENUS_GR','MARTE_GR','JUPITER_GR',
            'SATURNO_GR','URANO_GR','NEPTUNO_GR','PLUTON_GR','ASC_GR','MC_GR']

print("Conectando...")
conn = hd.ConnectionContext(address=HOST, port=PORT, user=USER, password=PASS,
                            encrypt=True, sslValidateCertificate=False)
print(f"OK — HANA {conn.hana_version()}")

hdf = hd.DataFrame(conn, 'SELECT * FROM DBADMIN.CARTAS_NATALES WHERE CALC_ERROR=0')
hdf_feat = hdf.select(['ID'] + FEATURES)
print(f"Dataset: {hdf_feat.count()} filas x {len(FEATURES)} features")

print("\nLanzando K-Means PAL (k=12, patent/kmeans++, min_max, euclidean)...")
t0 = time.time()
km = KMeans(
    n_clusters=12,
    init='patent',        # kmeans++ en hana-ml 2.28
    max_iter=300,
    distance_level='euclidean',     # 2 = euclidean (sin map, entero directo)
    normalization='min_max',
    tol=1e-6,
    thread_ratio=0.5
)
km.fit(hdf_feat, key='ID')
elapsed = time.time() - t0
print(f"K-Means completado en {elapsed:.1f}s")

# Inspeccionar columnas de salida
labels_hdf  = km.labels_
centers_hdf = km.cluster_centers_
print(f"\nLabels columnas  : {labels_hdf.columns}")
print(f"Centroides columnas: {centers_hdf.columns}")
print("\nPrimeras 5 asignaciones:")
print(labels_hdf.head(5).collect().to_string(index=False))

# ── Volcar a HANA via hdbcli raw ──────────────────────────────
print("\nGuardando resultados en HANA...")
raw = dbapi.connect(address=HOST, port=PORT, user=USER, password=PASS,
                    encrypt=True, sslValidateCertificate=False)
cur = raw.cursor()

for t in ["PAL_KMEANS_RESULT","PAL_KMEANS_CENTROIDS"]:
    try: cur.execute(f"DROP TABLE DBADMIN.{t}"); raw.commit()
    except: pass

# Labels
ldf = labels_hdf.collect()
ldf.columns = [c.upper() for c in ldf.columns]
print(f"  Labels cols reales: {list(ldf.columns)}")

# Detectar nombres de columnas automáticamente
id_col  = next(c for c in ldf.columns if c in ('ID','KEY'))
cid_col = next(c for c in ldf.columns if 'CLUSTER' in c)
dist_col= next((c for c in ldf.columns if 'DIST' in c), None)
sil_col = next((c for c in ldf.columns if 'SIL' in c), None)

cur.execute("""CREATE COLUMN TABLE DBADMIN.PAL_KMEANS_RESULT
    (ID INTEGER, CLUSTER_ID INTEGER, DISTANCE DOUBLE, SILHOUETTE DOUBLE)""")
rows = []
for _, r in ldf.iterrows():
    rows.append((
        int(r[id_col]), int(r[cid_col]),
        float(r[dist_col]) if dist_col and dist_col in r.index else 0.0,
        float(r[sil_col])  if sil_col  and sil_col  in r.index else 0.0
    ))
cur.executemany("INSERT INTO DBADMIN.PAL_KMEANS_RESULT VALUES (?,?,?,?)", rows)
raw.commit()
print(f"  {len(rows)} asignaciones guardadas en PAL_KMEANS_RESULT")

# Centroides → formato long
cdf = centers_hdf.collect()
cdf.columns = [c.upper() for c in cdf.columns]
print(f"  Centroides cols reales: {list(cdf.columns)}")

ccid_col = next(c for c in cdf.columns if 'CLUSTER' in c)
cur.execute("""CREATE COLUMN TABLE DBADMIN.PAL_KMEANS_CENTROIDS
    (CLUSTER_ID INTEGER, ATTR_NAME NVARCHAR(50), CENTROID_VAL DOUBLE)""")
crows = []
for _, r in cdf.iterrows():
    cid = int(r[ccid_col])
    for feat in FEATURES:
        fu = feat.upper()
        if fu in cdf.columns:
            crows.append((cid, feat, float(r[fu])))
if crows:
    cur.executemany("INSERT INTO DBADMIN.PAL_KMEANS_CENTROIDS VALUES (?,?,?)", crows)
    raw.commit()
    print(f"  {len(crows)} valores de centroides guardados")

# ── Actualizar CARTAS_NATALES ─────────────────────────────────
cur.execute("""UPDATE DBADMIN.CARTAS_NATALES C
    SET CLUSTER_ID = R.CLUSTER_ID, DIST_TO_CENTER = R.DISTANCE
    FROM DBADMIN.PAL_KMEANS_RESULT R WHERE C.ID = R.ID""")
raw.commit()
print("  CLUSTER_ID + DIST_TO_CENTER escritos en CARTAS_NATALES ✓")

# ── Distribución ──────────────────────────────────────────────
cur.execute("""SELECT CLUSTER_ID, COUNT(*) AS N
    FROM DBADMIN.PAL_KMEANS_RESULT GROUP BY CLUSTER_ID ORDER BY CLUSTER_ID""")
print("\n════ Distribución de clusters ════")
total = 0
for cid, n in cur.fetchall():
    bar = "█" * (n // 30)
    print(f"  Cluster {cid:2d}: {n:5d}  {bar}")
    total += n
print(f"  TOTAL: {total}")

cur.close(); raw.close(); conn.close()
print("\n✓ K-Means PAL completado y persistido en HANA.")
