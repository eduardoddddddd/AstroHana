"""
quien_soy.py — Dado cualquier nacimiento, devuelve sus vecinos en HANA
=======================================================================
Uso:
    py -X utf8 scripts/quien_soy.py 1976 10 11 20 33 1 40.4 -3.7
    (año mes dia hora min gmt lat lon)

    o sin argumentos para usar la carta de Eduardo por defecto.
"""
import sys, os
from dotenv import load_dotenv
import swisseph as swe
from hdbcli import dbapi

load_dotenv(os.path.join(os.path.dirname(__file__), '..', '.env'))

HOST = os.getenv("HANA_HOST")
PORT = int(os.getenv("HANA_PORT", 443))
USER = os.getenv("HANA_USER")
PASS = os.getenv("HANA_PASS")
EPHE = os.getenv("EPHE_PATH", r"C:\swisseph\ephe")

SIGNOS = ["","Aries","Tauro","Géminis","Cáncer","Leo","Virgo",
          "Libra","Escorpio","Sagitario","Capricornio","Acuario","Piscis"]

PLANETAS = {
    "SOL": swe.SUN, "LUNA": swe.MOON, "MERCURIO": swe.MERCURY,
    "VENUS": swe.VENUS, "MARTE": swe.MARS, "JUPITER": swe.JUPITER,
    "SATURNO": swe.SATURN, "URANO": swe.URANUS,
    "NEPTUNO": swe.NEPTUNE, "PLUTON": swe.PLUTO,
}

def calcular_carta(anio, mes, dia, hora, minuto, gmt, lat, lon):
    swe.set_ephe_path(EPHE)
    ut = (hora + minuto / 60.0) - gmt
    jd = swe.julday(anio, mes, dia, ut)
    pos = {}
    for nombre, pid in PLANETAS.items():
        p, _ = swe.calc_ut(jd, pid, swe.FLG_SWIEPH)
        pos[nombre] = round(p[0] % 360, 4)
    casas, ascmc = swe.houses(jd, lat, lon, b'P')
    pos["ASC"] = round(ascmc[0] % 360, 4)
    pos["MC"]  = round(ascmc[1] % 360, 4)
    return pos

def signo(grados):
    return SIGNOS[int(grados / 30) + 1]

def buscar_vecinos(pos, top=15):
    conn = dbapi.connect(address=HOST, port=PORT, user=USER, password=PASS,
                         encrypt=True, sslValidateCertificate=False)
    cur = conn.cursor()
    cur.execute(f"""
SELECT DISTINCT TOP {top}
    NOMBRE,
    SUBSTR(DESCRIPCION, 3, 55) AS PERFIL,
    ANIO, CLUSTER_ID,
    ROUND(SQRT(
        POWER(SOL_GR      - {pos['SOL']},      2) +
        POWER(LUNA_GR     - {pos['LUNA']},     2) +
        POWER(MERCURIO_GR - {pos['MERCURIO']}, 2) +
        POWER(VENUS_GR    - {pos['VENUS']},    2) +
        POWER(MARTE_GR    - {pos['MARTE']},    2) +
        POWER(JUPITER_GR  - {pos['JUPITER']},  2) +
        POWER(SATURNO_GR  - {pos['SATURNO']},  2) +
        POWER(ASC_GR      - {pos['ASC']},      2)
    ), 1) AS DIST
FROM DBADMIN.CARTAS_NATALES
WHERE CALC_ERROR = 0 AND LENGTH(NOMBRE) > 4 AND NOMBRE NOT LIKE '%#%'
ORDER BY DIST ASC
""")
    rows = cur.fetchall()
    cur.close(); conn.close()
    return rows

if __name__ == "__main__":
    if len(sys.argv) == 9:
        anio, mes, dia, hora, minuto = [int(x) for x in sys.argv[1:6]]
        gmt, lat, lon = [float(x) for x in sys.argv[6:9]]
    else:
        # Carta de Eduardo por defecto
        anio, mes, dia, hora, minuto = 1976, 10, 11, 20, 33
        gmt, lat, lon = 1.0, 40.4, -3.7

    print(f"\nCalculando carta natal: {dia}/{mes}/{anio} {hora}:{minuto:02d}h (GMT{gmt:+.1f})")
    pos = calcular_carta(anio, mes, dia, hora, minuto, gmt, lat, lon)

    print(f"\nPosiciones planetarias:")
    for nombre, grados in pos.items():
        print(f"  {nombre:10s}: {grados:6.2f}°  ({signo(grados)})")

    print(f"\nBuscando los 15 famosos más similares en HANA (8.473 cartas)...")
    vecinos = buscar_vecinos(pos)

    print(f"\n{'NOMBRE':<30} {'PERFIL':<40} {'AÑO':>4}  {'DIST':>6}")
    print("─" * 85)
    for nombre, perfil, anio_v, cluster, dist in vecinos:
        print(f"{str(nombre):<30} {str(perfil):<40} {anio_v:>4}  {dist:>6.1f}")
