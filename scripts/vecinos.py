"""
vecinos.py — Vecinos astrológicos en HANA
==========================================
Dado cualquier nacimiento, calcula posiciones con pyswisseph
y devuelve los N famosos más similares entre 8.473 cartas natales.

MODOS DE USO:
  Interactivo:
    py -X utf8 scripts/vecinos.py

  Argumentos directos:
    py -X utf8 scripts/vecinos.py 1976 10 11 20 33 1.0 40.4 -3.7
    (año mes dia hora min gmt lat lon)

  Con nombre de ciudad (requiere geopy):
    py -X utf8 scripts/vecinos.py 1976 10 11 20 33 --ciudad Madrid

DISTANCIA:
  Por defecto usa 8 planetas personales (Sol, Luna, Merc, Venus,
  Marte, Jupiter, Saturno, ASC). Usa --todos para los 12.
"""

import sys, os, argparse, textwrap
from dotenv import load_dotenv
import swisseph as swe
from hdbcli import dbapi

load_dotenv(os.path.join(os.path.dirname(__file__), '..', '.env'))

HOST = os.getenv("HANA_HOST")
PORT = int(os.getenv("HANA_PORT", 443))
USER = os.getenv("HANA_USER")
PASS = os.getenv("HANA_PASS")
EPHE = os.getenv("EPHE_PATH", r"C:\swisseph\ephe")

SIGNOS = ["Aries","Tauro","Géminis","Cáncer","Leo","Virgo",
          "Libra","Escorpio","Sagitario","Capricornio","Acuario","Piscis"]
CASAS_NOM = ["","I","II","III","IV","V","VI","VII","VIII","IX","X","XI","XII"]

PLANETAS_SW = {
    "SOL": swe.SUN, "LUNA": swe.MOON, "MERCURIO": swe.MERCURY,
    "VENUS": swe.VENUS, "MARTE": swe.MARS, "JUPITER": swe.JUPITER,
    "SATURNO": swe.SATURN, "URANO": swe.URANUS,
    "NEPTUNO": swe.NEPTUNE, "PLUTON": swe.PLUTO,
}

# Planetas personales (excluyen los generacionales Urano/Neptuno/Plutón)
FEATURES_8  = ["SOL_GR","LUNA_GR","MERCURIO_GR","VENUS_GR",
                "MARTE_GR","JUPITER_GR","SATURNO_GR","ASC_GR"]
FEATURES_12 = ["SOL_GR","LUNA_GR","MERCURIO_GR","VENUS_GR","MARTE_GR",
                "JUPITER_GR","SATURNO_GR","URANO_GR","NEPTUNO_GR",
                "PLUTON_GR","ASC_GR","MC_GR"]

# ── Cálculo pyswisseph ────────────────────────────────────────

def calcular_carta(anio, mes, dia, hora, minuto, gmt, lat, lon):
    """Calcula posiciones planetarias. Devuelve dict con grados absolutos."""
    swe.set_ephe_path(EPHE)
    ut = (hora + minuto / 60.0) - gmt
    # Ajuste de día si UT desborda
    dia_ut, hora_ut = dia, ut
    if hora_ut < 0:   dia_ut -= 1; hora_ut += 24
    if hora_ut >= 24: dia_ut += 1; hora_ut -= 24
    jd = swe.julday(anio, mes, dia_ut, hora_ut)

    pos = {}
    for nombre, pid in PLANETAS_SW.items():
        p, _ = swe.calc_ut(jd, pid, swe.FLG_SWIEPH)
        pos[nombre] = round(p[0] % 360, 4)

    casas, ascmc = swe.houses(jd, lat, lon, b'P')   # Placidus
    pos["ASC"] = round(ascmc[0] % 360, 4)
    pos["MC"]  = round(ascmc[1] % 360, 4)
    pos["_jd"]    = jd
    pos["_casas"] = casas
    return pos


def signo(grados):
    return SIGNOS[int(grados / 30)]

def grado_en_signo(grados):
    g = grados % 30
    return f"{int(g)}°{int((g%1)*60):02d}'"

def casa_de(grados, casas):
    """Devuelve número de casa (1-12) para unos grados dados."""
    lims = list(casas)
    for i in range(12):
        c1 = lims[i] % 360
        c2 = lims[(i+1) % 12] % 360
        if c2 < c1:
            if grados >= c1 or grados < c2: return i + 1
        else:
            if c1 <= grados < c2: return i + 1
    return 1


def imprimir_carta(pos):
    """Muestra la carta calculada en formato legible."""
    planetas_orden = ["SOL","LUNA","MERCURIO","VENUS","MARTE",
                      "JUPITER","SATURNO","URANO","NEPTUNO","PLUTON"]
    casas = pos.get("_casas", [0]*12)
    print()
    print("  Planeta      Signo         Grados    Casa")
    print("  " + "─"*48)
    for p in planetas_orden:
        g = pos[p]
        c = casa_de(g, casas)
        print(f"  {p:10s}   {signo(g):12s}  {grado_en_signo(g):8s}  {CASAS_NOM[c]}")
    print(f"  {'ASC':10s}   {signo(pos['ASC']):12s}  {grado_en_signo(pos['ASC']):8s}")
    print(f"  {'MC':10s}   {signo(pos['MC']):12s}  {grado_en_signo(pos['MC']):8s}")
    print()

# ── Consulta HANA ─────────────────────────────────────────────

def conectar():
    return dbapi.connect(address=HOST, port=PORT, user=USER, password=PASS,
                         encrypt=True, sslValidateCertificate=False)


def buscar_vecinos(pos, top=10, features=FEATURES_8):
    """
    Busca los N vecinos más cercanos en HANA.
    Distancia euclidiana en el espacio de features indicado.
    Devuelve lista de dicts.
    """
    # Mapeo feature → valor en pos
    mapa = {
        "SOL_GR": pos["SOL"], "LUNA_GR": pos["LUNA"],
        "MERCURIO_GR": pos["MERCURIO"], "VENUS_GR": pos["VENUS"],
        "MARTE_GR": pos["MARTE"], "JUPITER_GR": pos["JUPITER"],
        "SATURNO_GR": pos["SATURNO"], "URANO_GR": pos["URANO"],
        "NEPTUNO_GR": pos["NEPTUNO"], "PLUTON_GR": pos["PLUTON"],
        "ASC_GR": pos["ASC"], "MC_GR": pos["MC"],
    }
    terminos = " + ".join(
        f"POWER({col} - {mapa[col]}, 2)" for col in features
    )
    sql = f"""
SELECT TOP {top}
    NOMBRE,
    SUBSTR(DESCRIPCION, 3, 60)      AS PERFIL,
    ANIO,
    CLUSTER_ID,
    ROUND(SOL_GR,  1)               AS SOL,
    ROUND(LUNA_GR, 1)               AS LUNA,
    ROUND(ASC_GR,  1)               AS ASC_,
    ROUND(SQRT({terminos}), 2)      AS DISTANCIA
FROM DBADMIN.CARTAS_NATALES
WHERE CALC_ERROR = 0
  AND LENGTH(NOMBRE) > 4
  AND NOMBRE NOT LIKE '%#%'
  AND NOMBRE NOT LIKE '%ESQUIZO%'
  AND NOMBRE NOT LIKE '%MURDE%'
ORDER BY DISTANCIA ASC
"""
    conn = conectar()
    cur  = conn.cursor()
    cur.execute(sql)
    cols = [d[0] for d in cur.description]
    rows = [dict(zip(cols, r)) for r in cur.fetchall()]
    cur.close(); conn.close()
    return rows


def imprimir_vecinos(vecinos, features):
    """Muestra la tabla de vecinos."""
    nf = len(features)
    print(f"  {'#':>2}  {'Nombre':<28}  {'Perfil':<35}  {'Año':>4}  {f'Dist({nf}p)':>10}")
    print("  " + "─"*88)
    for i, v in enumerate(vecinos, 1):
        nombre = str(v["NOMBRE"])[:27]
        perfil = str(v["PERFIL"] or "")[:34]
        anio   = v["ANIO"] or "?"
        dist   = v["DISTANCIA"]
        print(f"  {i:>2}  {nombre:<28}  {perfil:<35}  {str(anio):>4}  {dist:>10.2f}")
    print()

# ── Modo interactivo ──────────────────────────────────────────

def pedir_dato(texto, tipo=float, opciones=None):
    while True:
        try:
            val = tipo(input(f"  {texto}: ").strip())
            if opciones and val not in opciones:
                print(f"  Valor debe ser uno de: {opciones}")
                continue
            return val
        except ValueError:
            print("  Valor inválido, intenta de nuevo.")


def modo_interactivo():
    print("\n" + "═"*60)
    print("  AstroHana — Vecinos natales en HANA")
    print("  8.473 cartas del Kepler 4 | pyswisseph | Placidus")
    print("═"*60)
    print("\n  Introduce los datos de nacimiento:\n")

    anio   = pedir_dato("Año  (ej: 1976)", int)
    mes    = pedir_dato("Mes  (1-12)",      int)
    dia    = pedir_dato("Día  (1-31)",      int)
    hora   = pedir_dato("Hora local (0-23)",int)
    minuto = pedir_dato("Minutos (0-59)",   int)
    gmt    = pedir_dato("GMT offset (ej: 1 para CET, -5 para EST)", float)
    lat    = pedir_dato("Latitud  (ej: 40.4 para Madrid)",          float)
    lon    = pedir_dato("Longitud (ej: -3.7 para Madrid)",          float)

    print("\n  ¿Cuántos vecinos? (por defecto 10)")
    try:
        top = int(input("  N: ").strip() or "10")
    except ValueError:
        top = 10

    print("\n  Modo distancia:")
    print("    1 → 8 planetas personales (Sol, Luna, Merc, Venus, Marte, Jup, Sat, ASC)")
    print("    2 → 12 planetas completos (incluye Urano, Neptuno, Plutón, MC)")
    try:
        modo = int(input("  Opción [1]: ").strip() or "1")
    except ValueError:
        modo = 1

    features = FEATURES_12 if modo == 2 else FEATURES_8
    return anio, mes, dia, hora, minuto, gmt, lat, lon, top, features


# ── Main ──────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Vecinos astrológicos en HANA",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=textwrap.dedent("""
          Ejemplos:
            py -X utf8 scripts/vecinos.py
            py -X utf8 scripts/vecinos.py 1976 10 11 20 33 1.0 40.4 -3.7
            py -X utf8 scripts/vecinos.py 1926 6 1 17 0 0 48.85 2.35 --top 15
            py -X utf8 scripts/vecinos.py 1926 6 1 17 0 0 48.85 2.35 --todos
        """))
    parser.add_argument("args", nargs="*",
        help="año mes dia hora min gmt lat lon (8 valores)")
    parser.add_argument("--top",   type=int, default=10,
        help="Número de vecinos (default: 10)")
    parser.add_argument("--todos", action="store_true",
        help="Usar 12 planetas en lugar de 8")
    cli = parser.parse_args()

    features = FEATURES_12 if cli.todos else FEATURES_8

    if len(cli.args) == 8:
        anio, mes, dia, hora, minuto = [int(x)   for x in cli.args[:5]]
        gmt, lat, lon               = [float(x) for x in cli.args[5:]]
        top = cli.top
    else:
        anio, mes, dia, hora, minuto, gmt, lat, lon, top, features = modo_interactivo()

    # ── Calcular carta ────────────────────────────────────────
    print(f"\n  Calculando carta natal: {dia:02d}/{mes:02d}/{anio}  "
          f"{hora:02d}:{minuto:02d}h  GMT{gmt:+.1f}  "
          f"({lat:.2f}°N, {lon:.2f}°E)")
    pos = calcular_carta(anio, mes, dia, hora, minuto, gmt, lat, lon)
    imprimir_carta(pos)

    # ── Buscar vecinos ────────────────────────────────────────
    nf = len(features)
    print(f"  Buscando {top} vecinos más cercanos en HANA "
          f"({nf} planetas, distancia euclidiana)...\n")
    vecinos = buscar_vecinos(pos, top=top, features=features)

    if not vecinos:
        print("  Sin resultados. Verifica la conexión a HANA.")
        return

    imprimir_vecinos(vecinos, features)

    # ── Resumen del primero ───────────────────────────────────
    v1 = vecinos[0]
    print(f"  Vecino más cercano: {v1['NOMBRE']} ({v1['ANIO']})")
    print(f"    Sol {signo(v1['SOL'])} · Luna {signo(v1['LUNA'])} · ASC {signo(v1['ASC_'])}")
    print(f"    Distancia: {v1['DISTANCIA']:.2f}°  |  Cluster: {v1['CLUSTER_ID']}")
    print()


if __name__ == "__main__":
    main()
