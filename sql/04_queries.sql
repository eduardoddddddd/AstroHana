-- =============================================================
-- AstroHana — Queries analíticas sobre CARTAS_NATALES
-- =============================================================
-- Ejecutar desde cualquier cliente SQL (DBeaver, HANA DB Explorer,
-- hdbcli Python, DesktopCommanderPy MCP).
-- Schema: DBADMIN. Todas las tablas son COLUMN STORE (in-memory).
-- =============================================================

-- ── 1. TUS VECINOS MÁS CERCANOS (Sol/Luna/ASC) ───────────────
-- Distancia euclidiana 3D. Cambia los tres valores por tu carta.
-- Eduardo: Sol=198.63 (Libra 18°38'), Luna=42.5 (Tauro), ASC=77.2 (Géminis)
SELECT DISTINCT TOP 25
    NOMBRE,
    SUBSTR(DESCRIPCION, 3, 50)          AS PERFIL,
    ANIO,
    CLUSTER_ID,
    ROUND(SOL_GR,  1)                   AS SOL,
    ROUND(LUNA_GR, 1)                   AS LUNA,
    ROUND(ASC_GR,  1)                   AS ASC_,
    ROUND(SQRT(
        POWER(SOL_GR   - 198.63, 2) +
        POWER(LUNA_GR  - 42.5,   2) +
        POWER(ASC_GR   - 77.2,   2)
    ), 1)                               AS DIST_3D
FROM DBADMIN.CARTAS_NATALES
WHERE CALC_ERROR = 0
  AND LENGTH(NOMBRE) > 4
  AND NOMBRE NOT LIKE '%#%'
ORDER BY DIST_3D ASC;

-- ── 2. VECINOS EN LOS 12 PLANETAS (distancia completa) ────────
-- Más preciso: usa los 12 vectores planetarios normalizados
SELECT DISTINCT TOP 20
    NOMBRE,
    SUBSTR(DESCRIPCION, 3, 50)          AS PERFIL,
    ANIO,
    CLUSTER_ID,
    ROUND(DIST_TO_CENTER, 3)            AS DIST_CLUSTER,
    ROUND(SQRT(
        POWER(SOL_GR      - 198.63, 2) +
        POWER(LUNA_GR     - 42.5,   2) +
        POWER(MERCURIO_GR - 210.0,  2) +
        POWER(VENUS_GR    - 225.0,  2) +
        POWER(MARTE_GR    - 290.0,  2) +
        POWER(JUPITER_GR  - 65.0,   2) +
        POWER(SATURNO_GR  - 128.0,  2) +
        POWER(ASC_GR      - 77.2,   2)
    ), 1)                               AS DIST_8P
FROM DBADMIN.CARTAS_NATALES
WHERE CALC_ERROR = 0
  AND LENGTH(NOMBRE) > 4
  AND NOMBRE NOT LIKE '%#%'
ORDER BY DIST_8P ASC;

-- ── 3. FAMOSOS DE MI CLUSTER ──────────────────────────────────
-- El cluster 4 es el de Eduardo (Sol ~Escorpio/Sagitario medio, ASC ~Géminis)
SELECT TOP 30
    NOMBRE,
    SUBSTR(DESCRIPCION, 3, 60)          AS PERFIL,
    ANIO,
    ROUND(DIST_TO_CENTER, 3)            AS DIST_CENTRO
FROM DBADMIN.CARTAS_NATALES
WHERE CLUSTER_ID = 4
  AND CALC_ERROR = 0
  AND LENGTH(NOMBRE) > 4
  AND NOMBRE NOT LIKE '%#%'
ORDER BY DIST_TO_CENTER ASC;

-- ── 4. DISTRIBUCIÓN POR CLUSTER ───────────────────────────────
SELECT
    CLUSTER_ID,
    COUNT(*)                                        AS N_CARTAS,
    ROUND(AVG(SOL_GR),  1)                          AS SOL_MEDIO_GR,
    ROUND(AVG(LUNA_GR), 1)                          AS LUNA_MEDIA_GR,
    ROUND(AVG(ASC_GR),  1)                          AS ASC_MEDIO_GR,
    ROUND(AVG(DIST_TO_CENTER), 3)                   AS COHESION_MEDIA
FROM DBADMIN.CARTAS_NATALES
WHERE CALC_ERROR = 0
GROUP BY CLUSTER_ID
ORDER BY CLUSTER_ID;

-- ── 5. SIGNO SOLAR MÁS FRECUENTE POR CLUSTER ──────────────────
SELECT
    CLUSTER_ID,
    CASE SOL_SIGNO
        WHEN 1 THEN 'Aries'       WHEN 2 THEN 'Tauro'    WHEN 3 THEN 'Géminis'
        WHEN 4 THEN 'Cáncer'      WHEN 5 THEN 'Leo'      WHEN 6 THEN 'Virgo'
        WHEN 7 THEN 'Libra'       WHEN 8 THEN 'Escorpio' WHEN 9 THEN 'Sagitario'
        WHEN 10 THEN 'Capricornio' WHEN 11 THEN 'Acuario' WHEN 12 THEN 'Piscis'
    END                                             AS SIGNO_SOLAR,
    COUNT(*)                                        AS N
FROM DBADMIN.CARTAS_NATALES
WHERE CALC_ERROR = 0
GROUP BY CLUSTER_ID, SOL_SIGNO
ORDER BY CLUSTER_ID, N DESC;

-- ── 6. COMBINACIONES SOL+LUNA — LAS MÁS RARAS ────────────────
SELECT
    CASE SOL_SIGNO  WHEN 1 THEN 'Aries' WHEN 2 THEN 'Tauro' WHEN 3 THEN 'Géminis'
        WHEN 4 THEN 'Cáncer' WHEN 5 THEN 'Leo' WHEN 6 THEN 'Virgo'
        WHEN 7 THEN 'Libra' WHEN 8 THEN 'Escorpio' WHEN 9 THEN 'Sagitario'
        WHEN 10 THEN 'Capricornio' WHEN 11 THEN 'Acuario' WHEN 12 THEN 'Piscis'
    END                                             AS SOL,
    CASE LUNA_SIGNO WHEN 1 THEN 'Aries' WHEN 2 THEN 'Tauro' WHEN 3 THEN 'Géminis'
        WHEN 4 THEN 'Cáncer' WHEN 5 THEN 'Leo' WHEN 6 THEN 'Virgo'
        WHEN 7 THEN 'Libra' WHEN 8 THEN 'Escorpio' WHEN 9 THEN 'Sagitario'
        WHEN 10 THEN 'Capricornio' WHEN 11 THEN 'Acuario' WHEN 12 THEN 'Piscis'
    END                                             AS LUNA,
    COUNT(*)                                        AS N,
    ROUND(COUNT(*) * 100.0 / 8473, 2)              AS PCT
FROM DBADMIN.CARTAS_NATALES
WHERE CALC_ERROR = 0
GROUP BY SOL_SIGNO, LUNA_SIGNO
ORDER BY N ASC
LIMIT 20;

-- ── 7. TEXTOS KEPLER PARA MI CARTA NATAL ──────────────────────
-- Recupera las interpretaciones del Kepler 4 para posiciones concretas
SELECT
    FICHERO,
    CABECERA,
    SUBSTR(TEXTO, 1, 300)               AS TEXTO_INICIO
FROM DBADMIN.KEPLER_TEXTOS
WHERE (PLANETA1 = 'Sol'  AND SIGNO = 'Libra')
   OR (PLANETA1 = 'Luna' AND CASA  = 12)
   OR (PLANETA1 = 'Saturno' AND SIGNO = 'Leo' AND CASA = 4)
   OR (PLANETA1 = 'Ascendente' AND SIGNO = 'Geminis')
ORDER BY FICHERO, CASA;

-- ── 8. BÚSQUEDA DE FAMOSOS POR CATEGORÍA ──────────────────────
-- 'V:' = varón, 'H:' = mujer (convención Kepler)
SELECT DISTINCT TOP 20
    NOMBRE,
    SUBSTR(DESCRIPCION, 3, 60)          AS PERFIL,
    ANIO,
    ROUND(SOL_GR,  1)                   AS SOL,
    ROUND(LUNA_GR, 1)                   AS LUNA,
    CLUSTER_ID
FROM DBADMIN.CARTAS_NATALES
WHERE UPPER(DESCRIPCION) LIKE '%MUSIC%'
   OR UPPER(DESCRIPCION) LIKE '%COMPOS%'
   OR UPPER(DESCRIPCION) LIKE '%MUSICIAN%'
ORDER BY ANIO DESC;

-- ── 9. QUERY GENÉRICA: vecinos de CUALQUIER carta ─────────────
-- Sustituye los valores de las variables con pyswisseph
-- :SOL_GR, :LUNA_GR, :MERC_GR, :VENUS_GR, :MARTE_GR, :ASC_GR
SELECT DISTINCT TOP 15
    NOMBRE,
    SUBSTR(DESCRIPCION, 3, 50)          AS PERFIL,
    ANIO,
    ROUND(SQRT(
        POWER(SOL_GR      - :SOL_GR,   2) +
        POWER(LUNA_GR     - :LUNA_GR,  2) +
        POWER(MERCURIO_GR - :MERC_GR,  2) +
        POWER(VENUS_GR    - :VENUS_GR, 2) +
        POWER(MARTE_GR    - :MARTE_GR, 2) +
        POWER(ASC_GR      - :ASC_GR,   2)
    ), 1)                               AS DISTANCIA
FROM DBADMIN.CARTAS_NATALES
WHERE CALC_ERROR = 0
  AND LENGTH(NOMBRE) > 4
  AND NOMBRE NOT LIKE '%#%'
ORDER BY DISTANCIA ASC;

-- ── 10. ESTADÍSTICAS GENERALES DEL DATASET ────────────────────
SELECT
    COUNT(*)                            AS TOTAL_CARTAS,
    SUM(CASE WHEN CALC_ERROR=0 THEN 1 ELSE 0 END)  AS CALCULADAS_OK,
    SUM(CASE WHEN CLUSTER_ID IS NOT NULL THEN 1 ELSE 0 END) AS CON_CLUSTER,
    MIN(ANIO)                           AS ANIO_MIN,
    MAX(ANIO)                           AS ANIO_MAX,
    ROUND(AVG(CAST(ANIO AS DOUBLE)),0)  AS ANIO_MEDIO
FROM DBADMIN.CARTAS_NATALES;

-- ── 11. TEXTOS KEPLER PARA CARTA NATAL (QUERY CORRECTA) ───────
-- El Kepler 4 guarda signo y casa juntos en una entrada:
--   "Sol en Libra O Casa 7" — signo Libra = entrada casa 7
--   "Sol en Virgo O Casa 6" — para buscar textos de casa 6 hay que pedir Virgo
--   "Luna en Piscis O Casa 12" — para casa 12 hay que pedir Piscis
-- PLANETAS.REV contiene textos de Revolución Solar, no natales — excluir.
--
-- Eduardo: Sol Libra Casa 6 / Luna Tauro Casa 12 / ASC Géminis / Saturno Leo Casa 4

SELECT
    FICHERO,
    CABECERA,
    TEXTO
FROM DBADMIN.KEPLER_TEXTOS
WHERE
    -- Sol en Libra (signo) — texto de signo Solar
    (PLANETA1 = 'Sol'  AND SIGNO = 'Libra'  AND FICHERO != 'PLANETAS.REV')
    OR
    -- Sol en Casa 6 — el Kepler lo guarda como "Virgo o Casa 6"
    (PLANETA1 = 'Sol'  AND CASA = 6         AND FICHERO != 'PLANETAS.REV')
    OR
    -- Luna en Tauro (signo)
    (PLANETA1 = 'Luna' AND SIGNO = 'Tauro'  AND FICHERO != 'PLANETAS.REV')
    OR
    -- Luna en Casa 12 — el Kepler lo guarda como "Piscis o Casa 12"
    (PLANETA1 = 'Luna' AND CASA = 12        AND FICHERO != 'PLANETAS.REV')
ORDER BY PLANETA1 DESC, FICHERO, CASA;

-- REGLA GENERAL para cualquier planeta/casa:
-- Casa N → buscar el signo cuyo número ordinal es N
--   Casa 1=Aries, 2=Tauro, 3=Géminis, 4=Cáncer, 5=Leo, 6=Virgo,
--   7=Libra, 8=Escorpio, 9=Sagitario, 10=Capricornio, 11=Acuario, 12=Piscis
-- Y FICHERO != 'PLANETAS.REV' para excluir Revolución Solar

-- QUERY PARAMETRIZABLE para cualquier carta natal:
-- Sustituye los pares (signo, casa_equivalente) según la tabla anterior
SELECT FICHERO, CABECERA, TEXTO
FROM DBADMIN.KEPLER_TEXTOS
WHERE FICHERO NOT IN ('PLANETAS.REV', 'ASPECTOS.ASC')
  AND PLANETA1 = :planeta   -- 'Sol', 'Luna', 'Mercurio'...
  AND (SIGNO = :signo OR CASA = :casa_natural)
ORDER BY FICHERO;
