# 🔭 AstroHana

> KeplerDB + SAP HANA Cloud: Machine Learning astrológico sobre 8.502 cartas natales.

![Status](https://img.shields.io/badge/Status-En%20desarrollo-orange)
![Python](https://img.shields.io/badge/Python-3.12-blue)
![HANA](https://img.shields.io/badge/SAP%20HANA%20Cloud-4.0%20Free%20Tier-0FAAFF)
![PAL](https://img.shields.io/badge/PAL-KMeans%20%7C%20HDBSCAN%20%7C%20PCA%20%7C%20RandomForest-purple)

---

## 🎯 Visión

**AstroHana** es la capa analítica de [KeplerDB](https://github.com/eduardoddddddd/KeplerDB).

KeplerDB resucita el motor interpretativo del **Kepler 4** (software astrológico DOS de los 90)
como aplicación web moderna con Flask + pyswisseph + SQLite.

AstroHana va un paso más allá: migra las **8.502 cartas natales** que venían incluidas en
el Kepler 4 a **SAP HANA Cloud** y aplica su librería **PAL (Predictive Analytics Library)**
para hacer lo que el Kepler nunca pudo: *machine learning sobre el cielo*.

---

## 🧠 ¿Qué hace que esto sea especial?

El dataset del Kepler 4 incluye cartas de:
- Famosos (artistas, políticos, deportistas, científicos)
- Expedientes médicos anonimizados (con diagnósticos)
- Estudios estadísticos históricos (Gauquelin et al.)
- Datos de referencia astronómica

**8.502 fechas, horas y lugares de nacimiento** → pyswisseph calcula las posiciones
planetarias exactas → HANA Cloud almacena los vectores → PAL ejecuta el ML **dentro
del motor de base de datos**, sin mover datos.

---

## 🏗️ Arquitectura

```
KeplerDB (SQLite local)          SAP HANA Cloud Free Tier
  ├── cartas (8502 filas)   →    CARTAS_NATALES
  │   fecha/hora/lugar      →      + 12 posiciones planetarias (°)
  │                         →      + signos y casas (1-12)
  │                         →      + CLUSTER_ID, PC1, PC2 (post-PAL)
  └── interpretaciones      →    KEPLER_TEXTOS
      (743 bloques Kepler4)         (para lookup desde app)

pyswisseph (cálculo)
  └── 01_migrate.py calcula posiciones en tiempo real durante la migración

PAL (dentro de HANA, puro SQL)
  ├── K-Means (12 clusters)   → arquetipos planetarios
  ├── HDBSCAN                 → clusters naturales sin k fijo
  ├── PCA (12D → 2D)          → visualización en plano
  └── Random Forest           → predicción de categoría profesional
```

---

## 🗂️ Estructura del repositorio

```
AstroHana/
├── README.md
├── .env.example              ← variables de entorno (credenciales HANA)
├── requirements.txt
│
├── scripts/
│   ├── 01_migrate.py         ← migración SQLite → HANA (calcula posiciones con swisseph)
│   ├── 02_pal_kmeans.py      ← ejecuta K-Means PAL y guarda resultados
│   ├── 03_pal_pca.py         ← PCA 12D → 2D para visualización
│   ├── 04_pal_hdbscan.py     ← clustering sin k predefinido
│   ├── 05_pal_rforest.py     ← Random Forest: predecir categoría
│   └── 06_query_examples.py  ← queries de demostración
│
├── sql/
│   ├── 01_create_tables.sql  ← DDL completo
│   ├── 02_kmeans_pal.sql     ← llamada PAL K-Means
│   ├── 03_pca_pal.sql        ← llamada PAL PCA
│   └── 04_queries.sql        ← queries analíticas
│
└── notebooks/
    └── analisis_clusters.ipynb  ← visualización resultados
```

---

## ⚡ Setup rápido

### 1. Dependencias
```bash
pip install hdbcli pyswisseph pandas python-dotenv
```

### 2. Credenciales HANA
Copia `.env.example` a `.env` y rellena:
```
HANA_HOST=xxxx.hanacloud.ondemand.com
HANA_PORT=443
HANA_USER=DBADMIN
HANA_PASS=tu_password
```

### 3. Ejecutar en orden
```bash
# Crear tablas en HANA
python scripts/01_migrate.py --create-tables

# Migrar cartas natales (calcula posiciones con pyswisseph)
python scripts/01_migrate.py --migrate

# Ejecutar PAL K-Means
python scripts/02_pal_kmeans.py

# Ver resultados
python scripts/06_query_examples.py
```

---

## 🔬 Análisis disponibles

### "¿Qué famosos tienen una carta similar a la mía?"
K-Means + búsqueda por distancia euclidiana al centroide más cercano.

### "¿Cuál es la combinación Sol/Luna más rara en las 8.502 cartas?"
Query estadística directa con GROUP BY + HAVING sobre signos.

### "¿Qué cluster domina en médicos vs artistas?"
Cross de CLUSTER_ID con el campo `tags`/`descripcion` del Kepler.

### "¿Dónde cae mi carta en el mapa PCA?"
PCA reduce las 12 dimensiones planetarias a 2 ejes.
Se puede pintar en matplotlib con los 8.502 puntos de fondo.

### "Predecir categoría profesional desde posiciones natales"
Random Forest entrenado sobre las cartas con categoría conocida.
*(Exploración estadística, no predictiva en sentido astrológico)*

---

## 📊 Dataset: Kepler 4 Natal Charts

| Campo | Descripción |
|---|---|
| `nombre` | Nombre del personaje |
| `lugar` | Ciudad de nacimiento |
| `anio/mes/dia` | Fecha de nacimiento |
| `hora/min` | Hora local |
| `gmt` | Corrección GMT (zona horaria) |
| `lat/lon` | Coordenadas geográficas |
| `tags` | Categoría: P=personaje, E=evento... |
| `descripcion` | Profesión/descripción breve |

Posiciones planetarias calculadas con **pyswisseph** (Swiss Ephemeris).
Casas calculadas con sistema **Placidus**.

---

## 🔗 Proyectos relacionados

- [KeplerDB](https://github.com/eduardoddddddd/KeplerDB) — app web Flask + interpretaciones
- [AstroExtracto](https://github.com/eduardoddddddd/AstroExtracto) — RAG sobre corpus YouTube
- [DesktopCommanderPy](https://github.com/eduardoddddddd/DesktopCommanderPy) — MCP server Python

---

## ⚠️ Notas técnicas

- **Free Tier HANA Cloud**: 15 GB RAM, 1 vCPU, 80 GB disco. Suficiente para 8.502 cartas.
- **PAL**: La Predictive Analytics Library corre *dentro* del motor HANA. No se mueven datos al cliente.
- **Zona horaria**: pyswisseph requiere tiempo universal (UT). La migración aplica la corrección GMT de cada carta.
- **Sistema de casas**: Placidus para todas las cartas natales.

---

*Eduardo Abdul Malik Arias · Órgiva, Granada · 2026*
