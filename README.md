# 🔭 AstroHana

> KeplerDB + SAP HANA Cloud: búsqueda vectorial astrológica sobre 8.473 cartas natales.

![Status](https://img.shields.io/badge/Status-Funcionando-brightgreen)
![Python](https://img.shields.io/badge/Python-3.12-blue)
![HANA](https://img.shields.io/badge/SAP%20HANA%20Cloud-4.0%20Free%20Tier-0FAAFF)
![scikit-learn](https://img.shields.io/badge/ML-scikit--learn-orange)

---

## 🎯 Qué es este proyecto

**AstroHana** migra las 8.502 cartas natales del [KeplerDB](https://github.com/eduardoddddddd/KeplerDB)
a SAP HANA Cloud y las convierte en un sistema de búsqueda vectorial astrológica.

Dado cualquier nacimiento (fecha, hora, lugar), el sistema:
1. Calcula las posiciones planetarias exactas con **pyswisseph**
2. Construye un vector de 12 dimensiones (grados de cada planeta)
3. Busca en HANA los vecinos más cercanos entre 8.473 cartas históricas
4. Devuelve los personajes con la carta natal más similar

Todo esto en menos de 2 segundos.

---

## 🏗️ Arquitectura real del sistema

```
fecha / hora / lugar de nacimiento
            │
            ▼
      pyswisseph (Python local)
      calcula posiciones planetarias
            │
            │  Sol=198.6°, Luna=42.5°, ASC=77.2°...
            │  → vector de 12 números
            ▼
      SAP HANA Cloud (búsqueda)
      SELECT... ORDER BY distancia euclidiana
      compara el vector contra 8.473 vectores almacenados
            │
            ▼
      Lista de personajes históricos similares
      (Fermi, Verdi, Eliot...)
```

**pyswisseph siempre calcula.** HANA no sabe astrología —
lo que almacena son resultados ya calculados de 8.473 cartas,
cada una convertida a 12 números (grados absolutos 0-360 de cada planeta).

---

## 📊 Estado actual de la base de datos

| Tabla | Filas | Contenido |
|---|---|---|
| `CARTAS_NATALES` | 8.473 | Cartas con 12 posiciones planetarias calculadas |
| `KEPLER_TEXTOS` | 1.124 | Textos interpretativos del Kepler 4 |
| `PAL_KMEANS_INPUT` | 101.676 | Features planetarias formato long (8473×12) |
| `PAL_KMEANS_RESULT` | 8.473 | Asignación cluster + distancia por carta |
| `PAL_KMEANS_CENTROIDS` | 144 | Centroides de 12 clusters |

29 cartas con datos de fecha/hora inválidos (dataset original del Kepler 4, años 90).

---

## ✅ Qué funciona

### Búsqueda de vecinos astrológicos
El núcleo del proyecto. Distancia euclidiana en espacio de 8 ó 12 dimensiones.
```bash
py -X utf8 scripts/vecinos.py 1976 10 11 20 33 1.0 40.4 -3.7
```
Devuelve los N famosos más similares entre 8.473 cartas en ~1 segundo.

### Posiciones planetarias verificadas
pyswisseph calcula todos los planetas + ASC + MC con sistema Placidus.
Verificado contra valores conocidos: Sol Libra 18°36', Luna Tauro 27°41',
ASC Géminis 00°32' para la carta de referencia (11/10/1976, 20:33h, Madrid, GMT+1).

### K-Means clustering (scikit-learn)
12 clusters asignados sobre las 8.473 cartas.
**Nota importante sobre los clusters:** agrupan principalmente por época de
nacimiento, no por arquetipo natal. Saturno, Urano y Neptuno se mueven muy
despacio (Neptuno tarda 165 años en dar una vuelta), así que dos personas
nacidas en 1925 comparten esos planetas casi idénticos. El K-Means detecta
eso más que el "arquetipo natal". Los clusters son útiles para exploración
pero no representan tipos astrológicos puros.

### Textos Kepler 4 en HANA
Los 1.124 textos interpretativos enlazados por planeta/signo/casa.
Query directa: "dame los textos de Sol Libra Casa 6 y Luna Tauro Casa 12".

**Regla importante descubierta:** el Kepler 4 guarda signo y casa juntos.
"Sol en Libra **o** Casa 7" es una sola entrada. Para buscar
"Sol en Casa 6" hay que pedir "Sol en Virgo" (Casa 6 = signo 6 = Virgo).

### Conexión MCP desde Claude
HANA está conectado via DesktopCommanderPy. Las tools `hana_*` permiten
ejecutar queries directamente desde Claude sin abrir ningún cliente SQL.

---

## ❌ Qué no funciona: PAL en Free Tier

### El problema
PAL (Predictive Analytics Library) es el motor de ML nativo de HANA.
Ejecuta K-Means, HDBSCAN, PCA, Random Forest y otros algoritmos
**dentro del motor de base de datos**, sin mover datos al cliente.

Para usar PAL, DBADMIN necesita el rol `AFL__SYS_AFL_AFLPAL_EXECUTE`.
**En HANA Cloud Free Tier este rol no se puede asignar desde SQL.**

### Todo lo que se intentó (y falló)
```sql
-- Intento 1: auto-grant (imposible, grantor = grantee)
GRANT AFL__SYS_AFL_AFLPAL_EXECUTE TO DBADMIN
-- Error: feature not supported: grantor and grantee are identical

-- Intento 2: usuario intermediario GRANTHELPER
-- Se creó con AFL__SYS_AFL_AFLPAL_EXECUTE_WITH_GRANT_OPTION
-- Error: insufficient privilege al intentar grantar a DBADMIN

-- Intento 3: stored procedure con SQL SECURITY DEFINER
-- El procedure hereda los privilegios del creador
-- Error: insufficient privilege al crear el procedure
```

También se probó `hana-ml` (librería oficial Python de SAP):
```python
from hana_ml.algorithms.pal.clustering import KMeans
km = KMeans(n_clusters=12, ...)
km.fit(hdf)
# Error: PALUnusableError: Missing needed role AFL__SYS_AFL_AFLPAL_EXECUTE
```

### Por qué ocurre
En HANA Cloud Free Tier, SAP gestiona la instancia en modo "managed service".
Los roles AFL (Application Function Library) solo se pueden asignar desde:
- La consola SAP BTP Cockpit → SAP HANA Database Explorer
- O desde el usuario SYSTEM (cuya password no es accesible en Free Tier)

DBADMIN tiene `ROLE ADMIN` pero no puede grantarse roles AFL a sí mismo.
Es una restricción de seguridad del modelo managed cloud de SAP.

### La solución aplicada
Se usó **scikit-learn** en Python local para el cálculo del K-Means,
y **hdbcli** para subir los resultados a HANA. El resultado final
(CLUSTER_ID en cada carta) es idéntico al que habría producido PAL.

```python
from sklearn.cluster import KMeans
from sklearn.preprocessing import MinMaxScaler

# Lee de HANA, calcula en local (2 segundos), sube a HANA
km = KMeans(n_clusters=12, init='k-means++', n_init=10, random_state=42)
km.fit(scaler.fit_transform(df[FEATURES].values))
# → UPDATE CARTAS_NATALES SET CLUSTER_ID=...
```

### Cómo desbloquearlo en una versión real (no Free Tier)
En una instancia HANA Cloud de pago o trial completo, desde el
SAP HANA Database Explorer conectado como DBADMIN:
```sql
CREATE ROLE PAL_USER;
GRANT AFL__SYS_AFL_AFLPAL_EXECUTE TO PAL_USER;
GRANT PAL_USER TO DBADMIN;
```
Con eso desbloqueado, `run_kmeans_final.py` ya tiene el código correcto
esperando, con la firma exacta verificada para HANA 4.0 CE2026.2 (7 tablas).

---

## 🧠 HANA como base de datos vectorial: la explicación conceptual

### Qué hemos construido realmente

HANA actúa aquí como **tres cosas a la vez**:

1. **Base de datos relacional clásica** — almacena nombre, fecha, lugar,
   signos, casas. Consultas SQL normales.

2. **Base de datos vectorial** — cada carta natal es un vector de 12 números.
   La búsqueda de vecinos es búsqueda por similitud en ese espacio vectorial.

3. **Motor de ML** — K-Means agrupa esos vectores en clusters.

### HANA vs Chroma/Pinecone: ¿sucedáneo o equivalente?

La respuesta depende del tipo de datos:

**Para vectores numéricos directos (nuestro caso): equivalente real.**

Los grados planetarios son números físicos con significado matemático
intrínseco. La distancia euclidiana entre dos cartas es la medida correcta
para decir "estas cartas se parecen". Chroma o Pinecone harían exactamente
lo mismo, solo que con un índice más rápido para millones de vectores.
Con 8.473 cartas la diferencia de velocidad es imperceptible.

```
Nuestro pipeline:
fecha/hora → pyswisseph → [198.6°, 42.5°, 77.2°...] → HANA → vecinos
                           vector de números físicos directos
                           (no necesita LLM, ya tiene significado)
```

**Para búsqueda semántica en texto: sí sería sucedáneo sin LLM.**

Si quisieras buscar "fragmentos que hablen de Saturno como maestro interior"
en el corpus VerbaSant, el pipeline sería diferente:

```
texto → LLM de embeddings → [0.23, -0.81, 0.44...] → HANA/Chroma → resultado
         (ej: text-embedding-3)   vector de 1536 números
         entiende que "lección dolorosa" y  que codifica SIGNIFICADO
         "karma" son próximos a               semántico
         "maestro interior"
```

Aquí **la clave no es el motor de búsqueda** (HANA, Chroma y Pinecone
hacen lo mismo) — la clave es que el vector viene de un LLM que entiende
el lenguaje. Sin LLM que genere los embeddings, cualquier bbdd vectorial
es un sucedáneo para búsqueda semántica.

**Con LLM generando los embeddings y HANA almacenándolos con `REAL_VECTOR`,
HANA funciona exactamente igual que Chroma.** No es sucedáneo.

### Tabla resumen

| | AstroHana (lo actual) | VerbaSant futuro en HANA |
|---|---|---|
| Tipo de vector | Grados planetarios (números físicos) | Embeddings de LLM |
| Necesita LLM | No | Sí (para generar embeddings) |
| Métrica | Distancia euclidiana | Similitud coseno |
| Motor en HANA | SQL con POWER() + SQRT() | REAL_VECTOR + L2DISTANCE() |
| Equivalente a Chroma | Sí, para estos datos | Sí, con LLM en el pipeline |
| Sucedáneo | No | No (si se implementa bien) |

### El siguiente nivel natural

Cargar el corpus VerbaSant (18.9M chars de transcripciones YouTube) convertido
a embeddings con un modelo (e5-large, OpenAI text-embedding-3...) en una tabla
HANA con tipo `REAL_VECTOR`. Pregunta en lenguaje natural → HANA busca por
similitud semántica → devuelve el fragmento de vídeo relevante.
Eso es RAG clásico con HANA como backend: recupera HANA, interpreta el LLM.
Reemplazaría ChromaDB local y el RTX 4070 para las búsquedas de AstroExtracto.

---

## 🗂️ Estructura del repositorio

```
AstroHana/
├── README.md
├── HANA_LECCIONES.md         ← detalles técnicos completos, errores y soluciones
├── .env.example              ← plantilla de credenciales
├── requirements.txt
│
├── scripts/
│   ├── vecinos.py            ← ★ SCRIPT PRINCIPAL: busca vecinos de cualquier carta
│   ├── migrate_direct.py     ← migración SQLite → HANA (con fix de NaN)
│   ├── kmeans_sklearn.py     ← K-Means scikit-learn → resultados a HANA
│   ├── run_kmeans_final.py   ← K-Means via PAL (requiere rol AFL, bloqueado en Free Tier)
│   └── quien_soy.py          ← versión anterior de vecinos.py (deprecated)
│
└── sql/
    ├── 01_create_tables.sql  ← DDL completo reproducible
    └── 04_queries.sql        ← 11 queries analíticas documentadas
```

---

## ⚡ Uso rápido

### Instalar dependencias
```bash
pip install hdbcli pyswisseph pandas python-dotenv scikit-learn
```

### Configurar credenciales
```bash
cp .env.example .env
# Editar .env con tus credenciales HANA
```

### Buscar tus vecinos astrológicos

Modo interactivo (te pregunta los datos):
```bash
py -X utf8 scripts/vecinos.py
```

Modo directo (año mes dia hora min gmt lat lon):
```bash
py -X utf8 scripts/vecinos.py 1976 10 11 20 33 1.0 40.4 -3.7
```

Con más vecinos y todos los planetas:
```bash
py -X utf8 scripts/vecinos.py 1976 10 11 20 33 1.0 40.4 -3.7 --top 20 --todos
```

---

## 🔗 Proyectos relacionados

- [KeplerDB](https://github.com/eduardoddddddd/KeplerDB) — app Flask con interpretaciones Kepler 4
- [AstroExtracto](https://github.com/eduardoddddddd/AstroExtracto) — RAG sobre corpus YouTube (ChromaDB local)
- [DesktopCommanderPy](https://github.com/eduardoddddddd/DesktopCommanderPy) — MCP server Python con tools HANA

---

## ⚙️ Detalles técnicos de conexión

```python
from hdbcli import dbapi
conn = dbapi.connect(
    address="20178d0a-...hanacloud.ondemand.com",
    port=443,
    user="DBADMIN",
    password="...",
    encrypt=True,              # obligatorio en HANA Cloud
    sslValidateCertificate=False  # necesario en Free Tier
)
```

**Notas importantes:**
- La instancia Free Tier se apaga sola — hay que arrancarla manualmente desde BTP Cockpit
- Timezone de la instancia: UTC. Los datos astrológicos requieren conversión GMT antes de insertar
- `IF EXISTS` no existe en HANA 4.0 — usar try/except en Python
- `&&` no funciona en PowerShell para encadenar comandos — usar `;`
- `DISTINCT TOP N` no es sintaxis válida en HANA — usar `TOP N` sin DISTINCT

---

*Eduardo Abdul Malik Arias · Órgiva, Granada · Marzo 2026*
