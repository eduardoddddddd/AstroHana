"""
kmeans_sklearn.py — K-Means con scikit-learn, resultados a HANA
Lee las posiciones planetarias de HANA, clusteriza localmente, 
sube CLUSTER_ID a HANA. Resultado final idéntico al PAL.
"""
import time
import numpy as np
import pandas as pd
from sklearn.cluster import KMeans
from sklearn.preprocessing import MinMaxScaler
from hdbcli import dbapi

HOST = "20178d0a-d4af-4825-bba6-11a2aa151d20.hna1.prod-us10.hanacloud.ondemand.com"
PORT = 443; USER = "DBADMIN"; PASS = "Edu01edu."

FEATURES = ['SOL_GR','LUNA_GR','MERCURIO_GR','VENUS_GR','MARTE_GR','JUPITER_GR',
            'SATURNO_GR','URANO_GR','NEPTUNO_GR','PLUTON_GR','ASC_GR','MC_GR']

print("Conectando a HANA...")
conn = dbapi.connect(address=HOST, port=PORT, user=USER, password=PASS,
                     encrypt=True, sslValidateCertificate=False)
cur = conn.cursor()

print("Descargando posiciones planetarias...")
t0 = time.time()
cur.execute(f"SELECT ID, {','.join(FEATURES)} FROM DBADMIN.CARTAS_NATALES WHERE CALC_ERROR=0")
rows = cur.fetchall()
cols = ['ID'] + FEATURES
df = pd.DataFrame(rows, columns=cols)
print(f"  {len(df)} cartas descargadas en {time.time()-t0:.1f}s")

# K-Means scikit-learn
print("\nEjecutando K-Means (k=12, init=k-means++, normalización min-max)...")
t0 = time.time()
scaler = MinMaxScaler()
X = scaler.fit_transform(df[FEATURES].values)

km = KMeans(n_clusters=12, init='k-means++', n_init=10,
            max_iter=300, tol=1e-6, random_state=42)
km.fit(X)
elapsed = time.time() - t0
print(f"K-Means completado en {elapsed:.1f}s")

df['CLUSTER_ID'] = km.labels_
# Distancia al centroide asignado
centers = km.cluster_centers_
dists = np.linalg.norm(X - centers[km.labels_], axis=1)
df['DISTANCE'] = dists

print("\nSubiendo resultados a HANA...")
# Actualizar CLUSTER_ID y DIST_TO_CENTER en CARTAS_NATALES
batch = [(int(r['CLUSTER_ID']), float(r['DISTANCE']), int(r['ID']))
         for _, r in df.iterrows()]
cur.executemany("""UPDATE DBADMIN.CARTAS_NATALES
    SET CLUSTER_ID=?, DIST_TO_CENTER=? WHERE ID=?""", batch)
conn.commit()
print(f"  {len(batch)} registros actualizados en CARTAS_NATALES")

# Guardar centroides (formato long: CLUSTER_ID, ATTR_NAME, CENTROID_VAL)
for t in ["PAL_KMEANS_RESULT","PAL_KMEANS_CENTROIDS"]:
    try: cur.execute(f"DROP TABLE DBADMIN.{t}"); conn.commit()
    except: pass

cur.execute("""CREATE COLUMN TABLE DBADMIN.PAL_KMEANS_RESULT (
    ID INTEGER, CLUSTER_ID INTEGER, DISTANCE DOUBLE)""")
res_rows = [(int(r['ID']), int(r['CLUSTER_ID']), float(r['DISTANCE']))
            for _, r in df.iterrows()]
cur.executemany("INSERT INTO DBADMIN.PAL_KMEANS_RESULT VALUES (?,?,?)", res_rows)

cur.execute("""CREATE COLUMN TABLE DBADMIN.PAL_KMEANS_CENTROIDS (
    CLUSTER_ID INTEGER, ATTR_NAME NVARCHAR(50), CENTROID_VAL DOUBLE)""")
# Desnormalizar centroides a grados reales
centers_real = scaler.inverse_transform(centers)
cen_rows = []
for cid, row in enumerate(centers_real):
    for feat, val in zip(FEATURES, row):
        cen_rows.append((cid, feat, float(val)))
cur.executemany("INSERT INTO DBADMIN.PAL_KMEANS_CENTROIDS VALUES (?,?,?)", cen_rows)
conn.commit()
print(f"  {len(cen_rows)} valores de centroides guardados")

# Distribución final
cur.execute("""SELECT CLUSTER_ID, COUNT(*) AS N FROM DBADMIN.PAL_KMEANS_RESULT
    GROUP BY CLUSTER_ID ORDER BY CLUSTER_ID""")
print("\n════ Distribución de clusters ════")
for cid, n in cur.fetchall():
    print(f"  Cluster {cid:2d}: {n:5d}  {'█'*(n//30)}")

cur.close(); conn.close()
print("\n✓ K-Means completado. CLUSTER_ID en HANA listo para queries.")
