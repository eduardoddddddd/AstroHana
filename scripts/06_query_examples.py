"""
06_query_examples.py — Queries analíticas sobre CARTAS_NATALES
==============================================================
Demos de lo que puedes preguntar una vez migrado el dataset.
"""
import os
from hdbcli import dbapi
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), '..', '.env'))

def conectar():
    return dbapi.connect(
        address=os.getenv("HANA_HOST"), port=int(os.getenv("HANA_PORT",443)),
        user=os.getenv("HANA_USER"), password=os.getenv("HANA_PASS"),
        encrypt=True, sslValidateCertificate=False)

# ── Mi carta natal: Sol Libra (signo 7), Luna Tauro (signo 2), ASC Géminis (signo 3) ──
MI_SOL_GR   = 198.63   # 18°38' Libra en grados absolutos
MI_LUNA_GR  = 42.5     # Luna Tauro
MI_ASC_GR   = 77.2     # ASC Géminis

def q1_famosos_similares(cur):
    """Los 20 famosos más cercanos a mi carta en espacio 3D Sol/Luna/ASC."""
    print("\n══ 1. Famosos más similares a mi carta natal ══")
    cur.execute("""
SELECT TOP 20
    NOMBRE, DESCRIPCION, ANIO,
    ROUND(SOL_GR,1) AS SOL, ROUND(LUNA_GR,1) AS LUNA, ROUND(ASC_GR,1) AS ASC_,
    ROUND(SQRT(
        POWER(SOL_GR   - ?, 2) +
        POWER(LUNA_GR  - ?, 2) +
        POWER(ASC_GR   - ?, 2)
    ), 2) AS DISTANCIA_3D
FROM DBADMIN.CARTAS_NATALES
WHERE CALC_ERROR = 0
ORDER BY DISTANCIA_3D ASC
""", (MI_SOL_GR, MI_LUNA_GR, MI_ASC_GR))
    for r in cur.fetchall():
        print(f"  {r[0]:30s} | {str(r[1])[:25]:25s} | {r[2]} | Dist={r[6]:.1f}")

def q2_frecuencias_sol_luna(cur):
    """Combinaciones Sol/Luna más raras (estadística pura)."""
    print("\n══ 2. Combinaciones Sol+Luna más raras en 8.502 cartas ══")
    SIGNOS = {1:"Ari",2:"Tau",3:"Gem",4:"Can",5:"Leo",6:"Vir",
              7:"Lib",8:"Esc",9:"Sag",10:"Cap",11:"Acu",12:"Pis"}
    cur.execute("""
SELECT SOL_SIGNO, LUNA_SIGNO, COUNT(*) AS N
FROM DBADMIN.CARTAS_NATALES WHERE CALC_ERROR=0
GROUP BY SOL_SIGNO, LUNA_SIGNO
ORDER BY N ASC LIMIT 15
""")
    for r in cur.fetchall():
        s = SIGNOS.get(r[0],"?"); l = SIGNOS.get(r[1],"?")
        print(f"  Sol {s} + Luna {l}: {r[2]:4d} cartas")

def q3_clusters_por_categoria(cur):
    """¿Qué cluster domina en médicos vs artistas?"""
    print("\n══ 3. Distribución de clusters por categoría ══")
    cur.execute("""
SELECT
    CASE
        WHEN DESCRIPCION LIKE '%medic%' OR DESCRIPCION LIKE '%doctor%' THEN 'Médico'
        WHEN DESCRIPCION LIKE '%artis%' OR DESCRIPCION LIKE '%actor%'  THEN 'Artista'
        WHEN DESCRIPCION LIKE '%polit%' OR DESCRIPCION LIKE '%presid%' THEN 'Político'
        WHEN DESCRIPCION LIKE '%sport%' OR DESCRIPCION LIKE '%futbol%' THEN 'Deportista'
        ELSE 'Otros'
    END AS CATEGORIA,
    CLUSTER_ID,
    COUNT(*) AS N
FROM DBADMIN.CARTAS_NATALES
WHERE CLUSTER_ID IS NOT NULL AND CALC_ERROR=0
GROUP BY CATEGORIA, CLUSTER_ID
ORDER BY CATEGORIA, N DESC
LIMIT 30
""")
    cat_actual = None
    for r in cur.fetchall():
        if r[0] != cat_actual:
            cat_actual = r[0]
            print(f"\n  [{cat_actual}]")
        print(f"    Cluster {r[1]:2d}: {r[2]:4d} cartas")

def q4_mi_cluster_vecinos(cur):
    """Cuando exista CLUSTER_ID: mis vecinos de cluster."""
    print("\n══ 4. Cartas en el mismo cluster que yo (Sol Libra) ══")
    cur.execute("""
SELECT TOP 1 CLUSTER_ID FROM DBADMIN.PAL_KMEANS_CENTROIDS
WHERE ATTR_NAME = 'SOL'
ORDER BY ABS(CENTROID_VAL - ?) ASC
""", (MI_SOL_GR,))
    row = cur.fetchone()
    if not row:
        print("  (K-Means aún no ejecutado — corre 02_pal_kmeans.py primero)")
        return
    mi_cluster = row[0]
    print(f"  Mi cluster estimado: {mi_cluster}")
    cur.execute("""
SELECT TOP 15 NOMBRE, DESCRIPCION, ANIO, ROUND(DIST_TO_CENTER,2) AS DIST
FROM DBADMIN.CARTAS_NATALES
WHERE CLUSTER_ID = ? AND CALC_ERROR=0
ORDER BY DIST_TO_CENTER ASC
""", (mi_cluster,))
    for r in cur.fetchall():
        print(f"  {r[0]:30s} | {str(r[1])[:30]:30s} | {r[2]} | Dist={r[3]}")

def q5_estadisticas_basicas(cur):
    """Estadísticas básicas del dataset."""
    print("\n══ 5. Estadísticas del dataset ══")
    cur.execute("SELECT COUNT(*), MIN(ANIO), MAX(ANIO) FROM DBADMIN.CARTAS_NATALES WHERE CALC_ERROR=0")
    r = cur.fetchone()
    print(f"  Total cartas calculadas: {r[0]}")
    print(f"  Rango años: {r[1]} – {r[2]}")
    cur.execute("""
SELECT SOL_SIGNO, COUNT(*) AS N FROM DBADMIN.CARTAS_NATALES
WHERE CALC_ERROR=0 GROUP BY SOL_SIGNO ORDER BY SOL_SIGNO
""")
    SIGNOS = ["","Ari","Tau","Gem","Can","Leo","Vir","Lib","Esc","Sag","Cap","Acu","Pis"]
    print("  Distribución Solar:")
    for r in cur.fetchall():
        bar = "█" * (r[1]//30)
        print(f"    {SIGNOS[r[0]]:3s}: {r[1]:4d} {bar}")

if __name__ == "__main__":
    conn = conectar()
    cur = conn.cursor()
    q5_estadisticas_basicas(cur)
    q2_frecuencias_sol_luna(cur)
    q1_famosos_similares(cur)
    q3_clusters_por_categoria(cur)
    q4_mi_cluster_vecinos(cur)
    cur.close(); conn.close()
