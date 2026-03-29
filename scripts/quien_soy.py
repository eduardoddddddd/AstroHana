"""
quien_soy.py — Dado cualquier nacimiento, devuelve sus vecinos en HANA.
"""
import os
import sys

import swisseph as swe
from dotenv import load_dotenv
from hdbcli import dbapi

load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))

HOST = os.getenv("HANA_HOST")
PORT = int(os.getenv("HANA_PORT", 443))
USER = os.getenv("HANA_USER")
PASS = os.getenv("HANA_PASS")
EPHE = os.getenv("EPHE_PATH", r"C:\swisseph\ephe")

SIGNOS = [
    "", "Aries", "Tauro", "Geminis", "Cancer", "Leo", "Virgo",
    "Libra", "Escorpio", "Sagitario", "Capricornio", "Acuario", "Piscis",
]

PLANETAS = {
    "SOL": swe.SUN,
    "LUNA": swe.MOON,
    "MERCURIO": swe.MERCURY,
    "VENUS": swe.VENUS,
    "MARTE": swe.MARS,
    "JUPITER": swe.JUPITER,
    "SATURNO": swe.SATURN,
    "URANO": swe.URANUS,
    "NEPTUNO": swe.NEPTUNE,
    "PLUTON": swe.PLUTO,
}

FEATURES_8 = [
    "SOL_GR", "LUNA_GR", "MERCURIO_GR", "VENUS_GR",
    "MARTE_GR", "JUPITER_GR", "SATURNO_GR", "ASC_GR",
]


def require_env(name: str, value: str | None) -> str:
    if value:
        return value
    raise RuntimeError(f"Falta la variable de entorno requerida: {name}")


def ajustar_ut(dia: int, ut: float) -> tuple[int, float]:
    dia_ut = dia
    hora_ut = ut
    if hora_ut < 0:
        dia_ut -= 1
        hora_ut += 24
    if hora_ut >= 24:
        dia_ut += 1
        hora_ut -= 24
    return dia_ut, hora_ut


def calcular_carta(anio, mes, dia, hora, minuto, gmt, lat, lon):
    swe.set_ephe_path(EPHE)
    ut = (hora + minuto / 60.0) - gmt
    dia_ut, hora_ut = ajustar_ut(dia, ut)
    jd = swe.julday(anio, mes, dia_ut, hora_ut)

    pos = {}
    for nombre, pid in PLANETAS.items():
        planet_pos, _ = swe.calc_ut(jd, pid, swe.FLG_SWIEPH)
        pos[nombre] = round(planet_pos[0] % 360, 4)

    _, ascmc = swe.houses(jd, lat, lon, b"P")
    pos["ASC"] = round(ascmc[0] % 360, 4)
    pos["MC"] = round(ascmc[1] % 360, 4)
    return pos


def signo(grados):
    return SIGNOS[int(grados / 30) + 1]


def angular_distance_sql(column_name: str) -> str:
    delta = f"ABS({column_name} - ?)"
    return f"(CASE WHEN {delta} > 180 THEN 360 - {delta} ELSE {delta} END)"


def buscar_vecinos(pos, top=15):
    host = require_env("HANA_HOST", HOST)
    user = require_env("HANA_USER", USER)
    password = require_env("HANA_PASS", PASS)

    feature_values = {
        "SOL_GR": pos["SOL"],
        "LUNA_GR": pos["LUNA"],
        "MERCURIO_GR": pos["MERCURIO"],
        "VENUS_GR": pos["VENUS"],
        "MARTE_GR": pos["MARTE"],
        "JUPITER_GR": pos["JUPITER"],
        "SATURNO_GR": pos["SATURNO"],
        "ASC_GR": pos["ASC"],
    }
    distance_terms = [
        f"POWER({angular_distance_sql(feature)}, 2)" for feature in FEATURES_8
    ]
    sql = f"""
SELECT DISTINCT TOP {top}
    NOMBRE,
    SUBSTR(DESCRIPCION, 3, 55) AS PERFIL,
    ANIO, CLUSTER_ID,
    ROUND(SQRT(
        {' + '.join(distance_terms)}
    ), 1) AS DIST
FROM DBADMIN.CARTAS_NATALES
WHERE CALC_ERROR = 0 AND LENGTH(NOMBRE) > 4 AND NOMBRE NOT LIKE '%#%'
ORDER BY DIST ASC
"""
    params = []
    for feature in FEATURES_8:
        params.extend([feature_values[feature], feature_values[feature], feature_values[feature]])

    conn = dbapi.connect(
        address=host,
        port=PORT,
        user=user,
        password=password,
        encrypt=True,
        sslValidateCertificate=False,
    )
    cur = conn.cursor()
    cur.execute(sql, params)
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows


if __name__ == "__main__":
    if len(sys.argv) == 9:
        anio, mes, dia, hora, minuto = [int(x) for x in sys.argv[1:6]]
        gmt, lat, lon = [float(x) for x in sys.argv[6:9]]
    else:
        anio, mes, dia, hora, minuto = 1976, 10, 11, 20, 33
        gmt, lat, lon = 1.0, 40.4, -3.7

    print(f"\nCalculando carta natal: {dia}/{mes}/{anio} {hora}:{minuto:02d}h (GMT{gmt:+.1f})")
    pos = calcular_carta(anio, mes, dia, hora, minuto, gmt, lat, lon)

    print("\nPosiciones planetarias:")
    for nombre, grados in pos.items():
        print(f"  {nombre:10s}: {grados:6.2f}°  ({signo(grados)})")

    print("\nBuscando los 15 famosos mas similares en HANA (8.473 cartas)...")
    vecinos = buscar_vecinos(pos)

    print(f"\n{'NOMBRE':<30} {'PERFIL':<40} {'AÑO':>4}  {'DIST':>6}")
    print("-" * 85)
    for nombre, perfil, anio_v, cluster, dist in vecinos:
        print(f"{str(nombre):<30} {str(perfil):<40} {anio_v:>4}  {dist:>6.1f}")
