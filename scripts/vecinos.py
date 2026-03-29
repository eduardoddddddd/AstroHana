"""
vecinos.py — Vecinos astrológicos en HANA.
"""
import argparse
import os
import textwrap

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
    "Aries", "Tauro", "Geminis", "Cancer", "Leo", "Virgo",
    "Libra", "Escorpio", "Sagitario", "Capricornio", "Acuario", "Piscis",
]
CASAS_NOM = ["", "I", "II", "III", "IV", "V", "VI", "VII", "VIII", "IX", "X", "XI", "XII"]

PLANETAS_SW = {
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
FEATURES_12 = [
    "SOL_GR", "LUNA_GR", "MERCURIO_GR", "VENUS_GR", "MARTE_GR",
    "JUPITER_GR", "SATURNO_GR", "URANO_GR", "NEPTUNO_GR",
    "PLUTON_GR", "ASC_GR", "MC_GR",
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
    for nombre, pid in PLANETAS_SW.items():
        planet_pos, _ = swe.calc_ut(jd, pid, swe.FLG_SWIEPH)
        pos[nombre] = round(planet_pos[0] % 360, 4)

    casas, ascmc = swe.houses(jd, lat, lon, b"P")
    pos["ASC"] = round(ascmc[0] % 360, 4)
    pos["MC"] = round(ascmc[1] % 360, 4)
    pos["_jd"] = jd
    pos["_casas"] = casas
    return pos


def signo(grados):
    return SIGNOS[int(grados / 30)]


def grado_en_signo(grados):
    g = grados % 30
    return f"{int(g)}°{int((g % 1) * 60):02d}'"


def casa_de(grados, casas):
    lims = list(casas)
    for idx in range(12):
        c1 = lims[idx] % 360
        c2 = lims[(idx + 1) % 12] % 360
        if c2 < c1:
            if grados >= c1 or grados < c2:
                return idx + 1
        else:
            if c1 <= grados < c2:
                return idx + 1
    return 1


def imprimir_carta(pos):
    planetas_orden = [
        "SOL", "LUNA", "MERCURIO", "VENUS", "MARTE",
        "JUPITER", "SATURNO", "URANO", "NEPTUNO", "PLUTON",
    ]
    casas = pos.get("_casas", [0] * 12)
    print()
    print("  Planeta      Signo         Grados    Casa")
    print("  " + "-" * 48)
    for planeta in planetas_orden:
        grados = pos[planeta]
        casa = casa_de(grados, casas)
        print(f"  {planeta:10s}   {signo(grados):12s}  {grado_en_signo(grados):8s}  {CASAS_NOM[casa]}")
    print(f"  {'ASC':10s}   {signo(pos['ASC']):12s}  {grado_en_signo(pos['ASC']):8s}")
    print(f"  {'MC':10s}   {signo(pos['MC']):12s}  {grado_en_signo(pos['MC']):8s}")
    print()


def angular_distance_sql(column_name: str) -> str:
    delta = f"ABS({column_name} - ?)"
    return f"(CASE WHEN {delta} > 180 THEN 360 - {delta} ELSE {delta} END)"


def conectar():
    host = require_env("HANA_HOST", HOST)
    user = require_env("HANA_USER", USER)
    password = require_env("HANA_PASS", PASS)
    return dbapi.connect(
        address=host,
        port=PORT,
        user=user,
        password=password,
        encrypt=True,
        sslValidateCertificate=False,
    )


def buscar_vecinos(pos, top=10, features=FEATURES_8):
    mapa = {
        "SOL_GR": pos["SOL"],
        "LUNA_GR": pos["LUNA"],
        "MERCURIO_GR": pos["MERCURIO"],
        "VENUS_GR": pos["VENUS"],
        "MARTE_GR": pos["MARTE"],
        "JUPITER_GR": pos["JUPITER"],
        "SATURNO_GR": pos["SATURNO"],
        "URANO_GR": pos["URANO"],
        "NEPTUNO_GR": pos["NEPTUNO"],
        "PLUTON_GR": pos["PLUTON"],
        "ASC_GR": pos["ASC"],
        "MC_GR": pos["MC"],
    }
    distance_terms = [f"POWER({angular_distance_sql(col)}, 2)" for col in features]
    sql = f"""
SELECT TOP {top}
    NOMBRE,
    SUBSTR(DESCRIPCION, 3, 60) AS PERFIL,
    ANIO,
    CLUSTER_ID,
    ROUND(SOL_GR, 1) AS SOL,
    ROUND(LUNA_GR, 1) AS LUNA,
    ROUND(ASC_GR, 1) AS ASC_,
    ROUND(SQRT({' + '.join(distance_terms)}), 2) AS DISTANCIA
FROM DBADMIN.CARTAS_NATALES
WHERE CALC_ERROR = 0
  AND LENGTH(NOMBRE) > 4
  AND NOMBRE NOT LIKE '%#%'
  AND NOMBRE NOT LIKE '%ESQUIZO%'
  AND NOMBRE NOT LIKE '%MURDE%'
ORDER BY DISTANCIA ASC
"""
    params = []
    for col in features:
        params.extend([mapa[col], mapa[col], mapa[col]])

    conn = conectar()
    cur = conn.cursor()
    cur.execute(sql, params)
    cols = [desc[0] for desc in cur.description]
    rows = [dict(zip(cols, row)) for row in cur.fetchall()]
    cur.close()
    conn.close()
    return rows


def imprimir_vecinos(vecinos, features):
    n_features = len(features)
    print(f"  {'#':>2}  {'Nombre':<28}  {'Perfil':<35}  {'Año':>4}  {f'Dist({n_features}p)':>10}")
    print("  " + "-" * 88)
    for idx, vecino in enumerate(vecinos, 1):
        nombre = str(vecino["NOMBRE"])[:27]
        perfil = str(vecino["PERFIL"] or "")[:34]
        anio = vecino["ANIO"] or "?"
        dist = vecino["DISTANCIA"]
        print(f"  {idx:>2}  {nombre:<28}  {perfil:<35}  {str(anio):>4}  {dist:>10.2f}")
    print()


def pedir_dato(texto, tipo=float, opciones=None):
    while True:
        try:
            val = tipo(input(f"  {texto}: ").strip())
            if opciones and val not in opciones:
                print(f"  Valor debe ser uno de: {opciones}")
                continue
            return val
        except ValueError:
            print("  Valor invalido, intenta de nuevo.")


def modo_interactivo():
    print("\n" + "=" * 60)
    print("  AstroHana - Vecinos natales en HANA")
    print("  8.473 cartas del Kepler 4 | pyswisseph | Placidus")
    print("=" * 60)
    print("\n  Introduce los datos de nacimiento:\n")

    anio = pedir_dato("Año  (ej: 1976)", int)
    mes = pedir_dato("Mes  (1-12)", int)
    dia = pedir_dato("Dia  (1-31)", int)
    hora = pedir_dato("Hora local (0-23)", int)
    minuto = pedir_dato("Minutos (0-59)", int)
    gmt = pedir_dato("GMT offset (ej: 1 para CET, -5 para EST)", float)
    lat = pedir_dato("Latitud  (ej: 40.4 para Madrid)", float)
    lon = pedir_dato("Longitud (ej: -3.7 para Madrid)", float)

    print("\n  ¿Cuantos vecinos? (por defecto 10)")
    try:
        top = int(input("  N: ").strip() or "10")
    except ValueError:
        top = 10

    print("\n  Modo distancia:")
    print("    1 -> 8 planetas personales (Sol, Luna, Merc, Venus, Marte, Jup, Sat, ASC)")
    print("    2 -> 12 planetas completos (incluye Urano, Neptuno, Pluton, MC)")
    try:
        modo = int(input("  Opcion [1]: ").strip() or "1")
    except ValueError:
        modo = 1

    features = FEATURES_12 if modo == 2 else FEATURES_8
    return anio, mes, dia, hora, minuto, gmt, lat, lon, top, features


def main():
    parser = argparse.ArgumentParser(
        description="Vecinos astrologicos en HANA",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=textwrap.dedent(
            """
            Ejemplos:
              py -X utf8 scripts/vecinos.py
              py -X utf8 scripts/vecinos.py 1976 10 11 20 33 1.0 40.4 -3.7
              py -X utf8 scripts/vecinos.py 1926 6 1 17 0 0 48.85 2.35 --top 15
              py -X utf8 scripts/vecinos.py 1926 6 1 17 0 0 48.85 2.35 --todos
            """
        ),
    )
    parser.add_argument("args", nargs="*", help="año mes dia hora min gmt lat lon (8 valores)")
    parser.add_argument("--top", type=int, default=10, help="Numero de vecinos (default: 10)")
    parser.add_argument("--todos", action="store_true", help="Usar 12 planetas en lugar de 8")
    cli = parser.parse_args()

    features = FEATURES_12 if cli.todos else FEATURES_8

    if len(cli.args) == 8:
        anio, mes, dia, hora, minuto = [int(x) for x in cli.args[:5]]
        gmt, lat, lon = [float(x) for x in cli.args[5:]]
        top = cli.top
    else:
        anio, mes, dia, hora, minuto, gmt, lat, lon, top, features = modo_interactivo()

    lon_label = "E" if lon >= 0 else "W"
    print(
        f"\n  Calculando carta natal: {dia:02d}/{mes:02d}/{anio}  "
        f"{hora:02d}:{minuto:02d}h  GMT{gmt:+.1f}  "
        f"({lat:.2f}°N, {abs(lon):.2f}°{lon_label})"
    )
    pos = calcular_carta(anio, mes, dia, hora, minuto, gmt, lat, lon)
    imprimir_carta(pos)

    n_features = len(features)
    print(
        f"  Buscando {top} vecinos mas cercanos en HANA "
        f"({n_features} planetas, distancia circular)...\n"
    )
    vecinos = buscar_vecinos(pos, top=top, features=features)

    if not vecinos:
        print("  Sin resultados. Verifica la conexion a HANA.")
        return

    imprimir_vecinos(vecinos, features)

    primero = vecinos[0]
    print(f"  Vecino mas cercano: {primero['NOMBRE']} ({primero['ANIO']})")
    print(f"    Sol {signo(primero['SOL'])} · Luna {signo(primero['LUNA'])} · ASC {signo(primero['ASC_'])}")
    print(f"    Distancia: {primero['DISTANCIA']:.2f}°  |  Cluster: {primero['CLUSTER_ID']}")
    print()


if __name__ == "__main__":
    main()
