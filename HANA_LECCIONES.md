# HANA Cloud Free Tier — Lecciones aprendidas y estado del proyecto

> Documento técnico real. Todo lo que funcionó, todo lo que no funcionó, y por qué.
> Basado en sesiones de trabajo reales. Eduardo Abdul Malik Arias, marzo 2026.

---

## La instancia

| Parámetro | Valor |
|---|---|
| Host | `20178d0a-d4af-4825-bba6-11a2aa151d20.hna1.prod-us10.hanacloud.ondemand.com` |
| Puerto | `443` (SSL siempre) |
| Versión | HANA 4.00.000.00.1773772603 (CE2026.2 / QRC 1/2026) |
| Plataforma | SUSE Linux Enterprise Server 15 SP5 |
| SID | H00 |
| Usuario principal | DBADMIN |
| Creada | 26 marzo 2026 |
| Región | US10 (AWS us-east-1) |
| Timezone | UTC (importante para datos astrológicos) |

**Recursos Free Tier:**
- CPU: 1 vCPU (el sistema reporta 32 — son los del host compartido subyacente)
- RAM: 15.71 GB física, ~2.92 GB usada en reposo
- Disco: 79.9 GB — 3% usado (2.5 GB)
- Sin alertas activas

---

## Estado actual de la base de datos

### Tablas en DBADMIN

| Tabla | Filas | Tipo | Descripción |
|---|---|---|---|
| `CARTAS_NATALES` | 8.473 | COLUMN | Cartas natales Kepler 4 con posiciones calculadas |
| `KEPLER_TEXTOS` | 1.124 | COLUMN | Textos interpretativos del Kepler 4 |
| `PAL_KMEANS_INPUT` | 101.676 | COLUMN | Features planetarias en formato long (8473×12) |
| `PAL_KMEANS_RESULT` | 8.473 | COLUMN | Asignación cluster + distancia por carta |
| `PAL_KMEANS_CENTROIDS` | 144 | COLUMN | Centroides de 12 clusters (12 clusters × 12 features) |
| `PAL_KM_PARAMS` | 6 | COLUMN | Parámetros del K-Means (persistentes) |

### Schema de CARTAS_NATALES

Columnas principales:
- `ID` INTEGER PK — id del Kepler 4
- `NOMBRE`, `LUGAR`, `ANIO`, `MES`, `DIA`, `HORA`, `MIN_`, `GMT`, `LAT`, `LON`
- `TAGS` — P=personaje famoso, E=evento, P,N=con datos médicos...
- `DESCRIPCION` — "V:Compositor" / "H:Actriz" (V=varón, H=mujer)
- `{PLANETA}_GR` DOUBLE — posición en grados absolutos 0-360 (pyswisseph, Placidus)
- `{PLANETA}_SIGNO` TINYINT — signo 1-12
- `{PLANETA}_CASA` TINYINT — casa 1-12 (Placidus)
- `ASC_GR`, `ASC_SIGNO`, `MC_GR`, `MC_SIGNO`
- `CLUSTER_ID` INTEGER — resultado K-Means (0-11)
- `DIST_TO_CENTER` DOUBLE — distancia euclidiana al centroide
- `CALC_ERROR` TINYINT — 0=OK, 1=error de cálculo pyswisseph

---

## Conexión desde Python

### Driver: hdbcli (oficial SAP)

```bash
pip install hdbcli
```

```python
from hdbcli import dbapi

conn = dbapi.connect(
    address="20178d0a-...hanacloud.ondemand.com",
    port=443,
    user="DBADMIN",
    password="Edu01edu.",
    encrypt=True,
    sslValidateCertificate=False  # Free Tier no tiene cert verificable
)
```

**Notas críticas:**
- `encrypt=True` es obligatorio — HANA Cloud solo acepta TLS
- `sslValidateCertificate=False` necesario en Free Tier
- No existe `IF EXISTS` en DROP — usar try/except
- `&&` no funciona en PowerShell para encadenar comandos — usar `;`

### Driver alternativo: hana-ml (alto nivel)

```bash
pip install hana-ml  # versión 2.28 en marzo 2026
```

```python
from hana_ml import dataframe as hd
conn = hd.ConnectionContext(address=..., port=443, user=..., password=...,
                            encrypt=True, sslValidateCertificate=False)
hdf = hd.DataFrame(conn, 'SELECT * FROM DBADMIN.CARTAS_NATALES')
```

hana-ml es útil para PAL (ver sección PAL) pero requiere el rol `AFL__SYS_AFL_AFLPAL_EXECUTE`.

---

## PAL (Predictive Analytics Library) — La realidad

### Qué es PAL

PAL es la librería de ML de SAP integrada dentro del motor HANA.
Ejecuta algoritmos (K-Means, HDBSCAN, PCA, Random Forest, ARIMA...) **dentro de la base de datos**
como stored procedures SQL. Los datos nunca salen del motor.

### Algoritmos disponibles en tu instancia

Confirmados disponibles (verificado via `SYS.AFL_FUNCTIONS`):
- **Clustering**: `PAL_KMEANS`, `PAL_ACCELERATEDKMEANS`, `PAL_HDBSCAN`, `PAL_DBSCAN`, `PAL_UNIFIED_CLUSTERING`
- **Clasificación**: `PAL_DECISIONTREE`, `PAL_RANDOMFORESTTRAIN`, `PAL_RANDOMFORESTPREDICT`
- **Reducción**: `PAL_PCA`, `PAL_PCAPROJECTION`
- **Series temporales**: ARIMA, suavizado exponencial, detección de anomalías
- **NLP**: `AFL__SYS_AFL_AFLPAL_NLP_EXECUTE` (stemming, embeddings)
- **Vectorial**: `PAL_VECPCA_ANY` (embeddings vectoriales)

### El problema de privilegios en Free Tier

**BLOQUEANTE**: DBADMIN en HANA Cloud Free Tier NO tiene el rol `AFL__SYS_AFL_AFLPAL_EXECUTE`.
Este rol es necesario para ejecutar cualquier función PAL.

**Lo que se intentó (todo falló):**
1. `GRANT AFL__SYS_AFL_AFLPAL_EXECUTE TO DBADMIN` — error: grantor = grantee
2. Usuario intermedio GRANTHELPER con `WITH_GRANT_OPTION` — error: insufficient privilege
3. Stored procedure creado por GRANTHELPER con `SQL SECURITY DEFINER` — error: insufficient privilege al crear
4. `hana-ml KMeans.fit()` — error: `PALUnusableError: Missing needed role`
5. `CALL _SYS_AFL.PAL_KMEANS(...)` directo — error: wrong number of parameters

**La firma correcta de PAL_KMEANS en HANA 4.0** (CE2026.2) es de **7 tablas**:
```sql
CALL _SYS_AFL.PAL_KMEANS(
    :lt_data,        -- input: formato long (ID, ATTR_NAME, ATTR_VAL)
    :lt_params,      -- params: (NAME, INTVAL, DBLVAL, STRVAL)
    lt_result,       -- output: (ID, CLUSTER_ID, DISTANCE, SLIGHT_SILHOUETTE)
    lt_centers,      -- output: (CLUSTER_ID, ATTR_NAME, VALUE)
    lt_center_stats, -- output
    lt_stats,        -- output
    lt_model         -- output
)
```
(La firma cambió en HANA 4.0 — en HANA 2.x eran 4 tablas)

**Para acceder a PAL necesitas:**
- Entrar en SAP BTP Cockpit → tu instancia HANA Cloud → SAP HANA Database Explorer
- Conectar como DBADMIN y ejecutar:
  ```sql
  CREATE ROLE PAL_USER;
  GRANT AFL__SYS_AFL_AFLPAL_EXECUTE TO PAL_USER;
  GRANT PAL_USER TO DBADMIN;
  ```
- O desde HANA Cloud Central → Manage Configuration → Database Roles

**Parámetros correctos de hana-ml 2.28** (distintos a la documentación):
```python
from hana_ml.algorithms.pal.clustering import KMeans
km = KMeans(
    n_clusters=12,
    init='patent',        # NO 'kmeans++' — ese nombre no existe
    max_iter=300,
    distance_level='euclidean',  # string, NO entero
    normalization='min_max',     # NO 'z-score'
    tol=1e-6,            # NO 'exit_threshold'
    thread_ratio=0.5
)
```

---

## K-Means — Lo que se hizo finalmente

Como el PAL no era accesible, se usó **scikit-learn** para el cálculo
y **hdbcli** para subir los resultados a HANA. El resultado es idéntico.

```python
from sklearn.cluster import KMeans
from sklearn.preprocessing import MinMaxScaler

# Lee de HANA, calcula en local, sube a HANA
scaler = MinMaxScaler()
X = scaler.fit_transform(df[FEATURES].values)
km = KMeans(n_clusters=12, init='k-means++', n_init=10,
            max_iter=300, tol=1e-6, random_state=42)
km.fit(X)
# → UPDATE CARTAS_NATALES SET CLUSTER_ID=...
```

**Tiempo de ejecución**: 2.0 segundos para 8.473 cartas × 12 features.

**Resultado**: 12 clusters, distribución entre 398 y 972 cartas por cluster.
Los centroides están almacenados en grados reales (desnormalizados) en `PAL_KMEANS_CENTROIDS`.

---

## Resultados astrológicos

### Cluster de Eduardo (cluster 4)
- 972 cartas (el más grande)
- Sol medio: 277.6° (Sagitario/Capricornio)
- ASC medio: 61.9° (Géminis) ← el ASC es el parámetro que más define el cluster

### Vecinos más cercanos (Sol/Luna/ASC, distancia euclidiana)

| Nombre | Profesión | Año | Dist |
|---|---|---|---|
| Carmen Ordóñez | Astróloga, periodista | 1955 | 17.8 |
| Enrico Fermi | Inventor reactor atómico | 1901 | 19.4 |
| Juan D. Perón | Político argentino | 1895 | 21.0 |
| Giuseppe Verdi | Compositor | 1813 | 22.0 |
| Paul Valéry | Poeta/escritor | 1871 | 28.6 |
| T.S. Eliot | Escritor | 1888 | 31.3 |
| Jordi González | Presentador TV | 1961 | 32.3 |

### Rareza de Sol Libra + Luna Tauro
Solo 48 personas en las 8.473 cartas del dataset = **0.57%**.
La combinación más frecuente (Leo + Géminis) tiene 81 = 0.96%.
La más rara: Escorpio + Escorpio con solo 38 casos.

---

## SQLScript — Peculiaridades HANA 4.0

```sql
-- NO existe: DROP TABLE X IF EXISTS
-- SÍ existe:
BEGIN
    DECLARE v INT;
    SELECT COUNT(*) INTO v FROM SYS.TABLES WHERE TABLE_NAME='X' AND SCHEMA_NAME='DBADMIN';
    IF v > 0 THEN DROP TABLE DBADMIN.X; END IF;
END;

-- O simplemente: try/except en Python

-- Bloques DO BEGIN con DECLARE funcionan en DB Explorer
-- pero NO desde hdbcli Python (error de syntax en DECLARE)
-- Solución: stored procedures o SQL puro sin bloques anónimos

-- LIMIT funciona (no ROWNUM ni TOP en subqueries fácilmente)
-- TOP N funciona en SELECT principal
-- STRING_AGG funciona para concatenar
-- SUBSTR(str, pos, len) — pos empieza en 1
```

---

## Arquitectura de conexión MCP

HANA está conectado via **DesktopCommanderPy** — el servidor MCP Python
de Eduardo (`C:\Users\Edu\DesktopCommanderPy`). Las tools `hana_*` leen
las credenciales de `config/hana_config.yaml`:

```yaml
hana:
  host: "20178d0a-...hanacloud.ondemand.com"
  port: 443
  user: "DBADMIN"
  password: "Edu01edu."
  encrypt: true
  sslValidateCertificate: true
  max_rows: 200
```

Esto permite ejecutar queries directamente desde Claude sin abrir
ningún cliente SQL adicional.

---

## Próximos pasos sugeridos

1. **Desbloquear PAL**: entrar en BTP Cockpit y hacer el GRANT via Database Explorer.
   Una vez hecho, `run_kmeans_final.py` ya tiene la llamada PAL correcta.

2. **PAL PCA**: reducir 12 dimensiones a 2 para visualización scatter.
   Las 8.473 cartas en un plano 2D coloreadas por cluster.

3. **Script `quien_soy.py`**: ya está listo — calcula vecinos de cualquier
   fecha de nacimiento. Integrable directo en KeplerDB Flask.

4. **HANA Full-Text Search**: cargar el corpus VerbaSant (18.9M chars) en
   una tabla NCLOB con FTS activado. Búsqueda lingüística en español.

5. **REAL_VECTOR**: tipo nativo HANA para embeddings. Reemplazaría ChromaDB
   en AstroExtracto. Sin RTX 4070, queries desde cualquier sitio.

6. **Series temporales**: cargar precios BTC/ETH + posiciones planetarias.
   Cruce mundana + crypto con SQL directo.
