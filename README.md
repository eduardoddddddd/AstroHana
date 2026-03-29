# AstroHana

> KeplerDB + SAP HANA Cloud: cartas natales, clustering y exploracion analitica sobre 8.473 registros historicos.

![Status](https://img.shields.io/badge/Status-Funcionando-brightgreen)
![Python](https://img.shields.io/badge/Python-3.12-blue)
![HANA](https://img.shields.io/badge/SAP%20HANA%20Cloud-Free%20Tier-0FAAFF)
![Gradio](https://img.shields.io/badge/UI-Gradio-orange)

---

## Que es AstroHana

**AstroHana** toma el dataset historico de [KeplerDB](https://github.com/eduardoddddddd/KeplerDB), recalcula cada carta natal con `pyswisseph`, lo carga en **SAP HANA Cloud** y lo explota como:

1. base de datos relacional para cartas y textos interpretativos,
2. base analitica para queries astrologicas sobre miles de registros,
3. base vectorial numerica para similitud entre cartas,
4. soporte de clustering con resultados persistidos en HANA,
5. miniapp web para explorar todo esto desde una capa visual.

El proyecto ya no es solo una demo de "vecinos". Ahora expone un pequeño entorno de trabajo para:

- calcular una carta natal a partir de fecha, hora y coordenadas,
- encontrar perfiles historicos cercanos,
- inspeccionar clusters guardados en HANA,
- explorar categorias y busquedas por texto dentro del dataset,
- recuperar interpretaciones Kepler desde la tabla `KEPLER_TEXTOS`.

---

## Que hace el sistema

Pipeline real:

```text
fecha / hora / lugar
        |
        v
pyswisseph
calcula posiciones planetarias, ASC y MC
        |
        v
vector numerico de carta
(8 o 12 factores)
        |
        v
SAP HANA Cloud
- vecinos por similitud
- queries SQL analiticas
- clustering persistido
- textos Kepler
        |
        v
CLI + miniapp Gradio
```

### Idea central

Aqui no hay embeddings generados por LLM para la parte astrologica principal. Cada carta es un vector de **grados planetarios fisicos**. Eso hace que HANA funcione como backend de similitud numerica real, no como sucedaneo de vector DB.

### Mejora importante aplicada

La similitud ya no usa diferencia lineal de angulos. Ahora emplea **distancia angular circular**, para que `359°` y `1°` se consideren cercanos y no opuestos por culpa del salto `0/360`.

---

## Estado actual de la base de datos

| Tabla | Filas aproximadas | Contenido |
|---|---:|---|
| `CARTAS_NATALES` | 8.473 | Cartas con posiciones planetarias y metadatos |
| `KEPLER_TEXTOS` | 1.124 | Textos interpretativos del Kepler 4 |
| `PAL_KMEANS_INPUT` | 101.676 | Features en formato long (`8473 x 12`) |
| `PAL_KMEANS_RESULT` | 8.473 | Cluster asignado y distancia al centro |
| `PAL_KMEANS_CENTROIDS` | 144 | Centroides de 12 clusters |

Hay 29 cartas del dataset original con datos temporales incompletos o invalidos que quedan fuera del calculo correcto.

---

## Que funciona hoy

### 1. Calculo de cartas natales

- `pyswisseph` calcula Sol, Luna, planetas, ASC y MC.
- Sistema de casas: **Placidus**.
- Conversión de hora local a UT aplicada antes del calculo.
- Manejo del desbordamiento de dia por efecto del GMT.

### 2. Vecinos astrologicos

- similitud sobre 8 factores o 12 factores,
- query directa contra HANA,
- ranking de cartas cercanas,
- visualizacion en CLI y en miniapp.

### 3. Clustering

- `scikit-learn` ejecuta `KMeans` localmente,
- los resultados se guardan en HANA (`CLUSTER_ID`, `DIST_TO_CENTER`),
- la miniapp permite explorar overview y miembros por cluster.

### 4. Textos Kepler

- almacenamiento de interpretaciones en `KEPLER_TEXTOS`,
- recuperacion por planeta, signo y casa,
- uso directo desde la miniapp para la carta calculada.

### 5. Exploracion del dataset

- busquedas por categoria,
- busquedas libres por palabra clave en nombre y descripcion,
- queries SQL analiticas sobre el corpus historico.

---

## Miniapp web

La miniapp esta implementada en [app.py](./app.py) con **Gradio** y reutiliza la logica Python + HANA del proyecto.

### Pestañas actuales

#### `Carta viva`

- formulario de nacimiento,
- calculo de carta natal,
- vecinos mas cercanos,
- resumen del mejor match,
- recuperacion de textos Kepler asociados a la carta.

#### `Atlas de clusters`

- overview del K-Means persistido en HANA,
- numero de cartas por cluster,
- cohesion media,
- inspeccion de miembros cercanos al centroide.

#### `Explorador de dataset`

- filtros rapidos por categoria (`Musica`, `Politica`, `Ciencia`, `Arte`, `Deporte`),
- busqueda libre por texto en nombre o descripcion,
- lectura rapida de perfiles dentro del corpus.

#### `Kepler lab`

- consulta directa a `KEPLER_TEXTOS`,
- seleccion de planeta, signo y casa,
- inspeccion de interpretaciones sin recalcular carta.

### Arranque

```bash
py -X utf8 app.py
```

Normalmente quedara disponible en:

```text
http://127.0.0.1:7860
```

---

## Estructura del repositorio

```text
AstroHana/
|-- app.py
|-- README.md
|-- HANA_LECCIONES.md
|-- requirements.txt
|-- .env.example
|-- scripts/
|   |-- __init__.py
|   |-- 01_migrate.py
|   |-- 02_pal_kmeans.py
|   |-- 06_query_examples.py
|   |-- fix_roles.py
|   |-- kmeans_sklearn.py
|   |-- migrate_direct.py
|   |-- quien_soy.py
|   |-- run_kmeans.py
|   |-- run_kmeans_final.py
|   |-- run_kmeans_hanaml.py
|   |-- run_kmeans_v2.py
|   `-- vecinos.py
`-- sql/
    |-- 01_create_tables.sql
    `-- 04_queries.sql
```

### Archivos mas importantes

- [app.py](./app.py): miniapp web de presentacion y exploracion.
- [scripts/vecinos.py](./scripts/vecinos.py): logica principal de calculo + vecinos.
- [scripts/quien_soy.py](./scripts/quien_soy.py): version simplificada del flujo de consulta.
- [scripts/kmeans_sklearn.py](./scripts/kmeans_sklearn.py): clustering local con persistencia en HANA.
- [sql/04_queries.sql](./sql/04_queries.sql): queries analiticas de referencia.
- [HANA_LECCIONES.md](./HANA_LECCIONES.md): notas tecnicas, errores y aprendizajes.

---

## Instalacion

### Requisitos

- Python 3.12
- acceso a SAP HANA Cloud
- dataset fuente `kepler.db`
- efemerides Swiss Ephemeris disponibles en disco

### Dependencias

```bash
pip install -r requirements.txt
```

Actualmente el proyecto declara:

- `hdbcli`
- `pyswisseph`
- `pandas`
- `python-dotenv`
- `matplotlib`
- `numpy`
- `scikit-learn`
- `hana-ml`
- `gradio`

---

## Configuracion

Copia la plantilla:

```bash
copy .env.example .env
```

Contenido esperado en `.env`:

```env
HANA_HOST=tu_instancia.hanacloud.ondemand.com
HANA_PORT=443
HANA_USER=DBADMIN
HANA_PASS=tu_password
HANA_GRANTHELPER_USER=GRANTHELPER
HANA_GRANTHELPER_PASS=tu_password_granthelper
KEPLER_DB=C:\ruta\al\kepler.db
EPHE_PATH=C:\swisseph\ephe
```

### Seguridad

Las credenciales ya no se guardan en duro dentro de los scripts principales. Toda la configuracion sensible se carga desde variables de entorno.

Si el repo tuvo claves expuestas en iteraciones anteriores, conviene **rotarlas** en HANA aunque el codigo actual ya este saneado.

---

## Uso desde linea de comandos

### Calcular vecinos de forma interactiva

```bash
py -X utf8 scripts/vecinos.py
```

### Calcular vecinos pasando argumentos

```bash
py -X utf8 scripts/vecinos.py 1976 10 11 20 33 1.0 40.4 -3.7
```

### Usar 12 factores y pedir mas resultados

```bash
py -X utf8 scripts/vecinos.py 1976 10 11 20 33 1.0 40.4 -3.7 --top 20 --todos
```

### Ejecutar clustering local y guardar en HANA

```bash
py -X utf8 scripts/kmeans_sklearn.py
```

### Lanzar queries analiticas de ejemplo

```bash
py -X utf8 scripts/06_query_examples.py
```

---

## Migracion y generacion de datos

### Crear tablas y migrar desde SQLite

```bash
py -X utf8 scripts/01_migrate.py --all
```

Opciones utiles:

```bash
py -X utf8 scripts/01_migrate.py --create-tables
py -X utf8 scripts/01_migrate.py --migrate
py -X utf8 scripts/01_migrate.py --check
```

### Alternativa de migracion

```bash
py -X utf8 scripts/migrate_direct.py
```

Esta ruta se uso como variante practica cuando hubo que resolver ciertos problemas de datos o insercion.

---

## Clustering y PAL

### Camino real que funciona hoy

En Free Tier, la opcion fiable es:

1. leer features desde HANA,
2. ejecutar `KMeans` con `scikit-learn`,
3. persistir resultados en HANA.

### Scripts relacionados

- [scripts/kmeans_sklearn.py](./scripts/kmeans_sklearn.py): camino funcional principal.
- [scripts/run_kmeans_final.py](./scripts/run_kmeans_final.py): intento con `hana-ml` preparado para entornos con PAL disponible.
- [scripts/run_kmeans_hanaml.py](./scripts/run_kmeans_hanaml.py): variante PAL via `hana-ml`.
- [scripts/run_kmeans.py](./scripts/run_kmeans.py): llamadas PAL por overload.
- [scripts/run_kmeans_v2.py](./scripts/run_kmeans_v2.py): pruebas de invocacion via `DO BEGIN`.
- [scripts/fix_roles.py](./scripts/fix_roles.py): utilidades relacionadas con permisos/roles PAL.

### Limitacion conocida del Free Tier

En **SAP HANA Cloud Free Tier**, PAL queda bloqueado por privilegios AFL que no siempre se pueden conceder desde SQL con `DBADMIN`.

Consecuencia:

- el motor de clustering nativo de HANA no queda utilizable en este entorno,
- pero el proyecto sigue funcionando porque el clustering se ejecuta fuera y se persiste dentro de HANA.

---

## Queries HANA que merecen la pena

El valor del proyecto no esta solo en "vecinos". Estas son las capas realmente interesantes:

### Analitica de clusters

- distribucion de cartas por cluster,
- cohesion media,
- perfiles centrales,
- cluster dominante por categoria.

### Exploracion del corpus

- busqueda por categoria profesional,
- filtrado por palabra clave en descripcion,
- busqueda de cartas con combinaciones raras Sol/Luna.

### Interpretacion textual

- acceso directo a `KEPLER_TEXTOS`,
- combinacion signo/casa/planeta,
- lectura de textos interpretativos asociados a una carta calculada.

### Query SQL de referencia

Revisa [sql/04_queries.sql](./sql/04_queries.sql) para ejemplos listos sobre:

- vecinos,
- clusters,
- combinaciones solares/lunares,
- textos Kepler,
- estadisticas del dataset.

---

## HANA como backend vectorial

### En este proyecto

HANA trabaja aqui como motor de similitud numerica sobre vectores de grados astrologicos.

- vector actual: grados planetarios,
- metrica: distancia en espacio numerico,
- uso: similitud entre cartas.

### Siguiente nivel natural

El siguiente salto es combinar esto con vectores semanticos reales:

- cargar corpus textual en HANA,
- generar embeddings,
- guardarlos como `REAL_VECTOR`,
- consultar por similitud semantica.

Eso permitiria convertir AstroHana en puente entre:

- astrologia estructurada,
- corpus textual interpretativo,
- RAG semantico,
- y HANA como backend unico.

---

## Verificacion realizada en esta iteracion

Durante esta fase se verifico:

- compilacion sintactica de `app.py` y scripts principales con `py_compile`,
- importacion correcta de la miniapp,
- consultas reales contra HANA para:
  - `run_lookup`,
  - overview de clusters,
  - miembros por cluster,
  - categorias del dataset,
  - textos Kepler.

Tambien se resolvio un error real de parametros SQL en la app al aplicar la distancia angular circular.

---

## Limitaciones conocidas

- la calidad del clustering esta muy influida por planetas generacionales y epoca de nacimiento,
- algunos registros del dataset tienen descripciones ruidosas o repetidas,
- PAL sigue bloqueado por restricciones del Free Tier,
- la miniapp es funcional pero no pretende ser una app final de usuario masivo,
- no hay aun capa de autenticacion ni despliegue productivo.

---

## Roadmap sugerido

### Corto plazo

- sintetizador narrativo de la carta desde la miniapp,
- mejor filtro y deduplicacion de vecinos,
- mejoras visuales en tablas y resaltes,
- exportacion de resultados.

### Medio plazo

- perfilado automatico por cluster,
- busqueda combinada carta + textos,
- dashboards mas fuertes sobre epocas, signos y categorias.

### Largo plazo

- embeddings semanticos en HANA para corpus textual,
- RAG astrologico / interpretativo,
- app desplegada con persistencia de consultas o perfiles de usuario.

---

## Proyectos relacionados

- [KeplerDB](https://github.com/eduardoddddddd/KeplerDB): app con interpretaciones Kepler 4.
- [AstroExtracto](https://github.com/eduardoddddddd/AstroExtracto): RAG sobre corpus YouTube con backend vectorial local.
- [DesktopCommanderPy](https://github.com/eduardoddddddd/DesktopCommanderPy): MCP server Python con tools HANA.

---

## Notas tecnicas de conexion

Ejemplo base:

```python
from hdbcli import dbapi

conn = dbapi.connect(
    address="tu_instancia.hanacloud.ondemand.com",
    port=443,
    user="DBADMIN",
    password="...",
    encrypt=True,
    sslValidateCertificate=False,
)
```

Notas practicas:

- Free Tier suele dormirse y hay que reactivar la instancia desde BTP Cockpit.
- La instancia trabaja en UTC y eso importa mucho para astrologia.
- HANA 4.0 no soporta algunos patrones comodisimos de otros motores, asi que parte del manejo DDL se resuelve con `try/except`.
- En PowerShell, `&&` no es el mejor aliado; usa `;` para encadenar.

---

## Autor

Eduardo Abdul Malik Arias  
Orgiva, Granada  
Marzo 2026
