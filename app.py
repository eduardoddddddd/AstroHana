import os

import gradio as gr
import pandas as pd
from dotenv import load_dotenv
from hdbcli import dbapi

from scripts.vecinos import (
    FEATURES_8,
    FEATURES_12,
    calcular_carta,
    buscar_vecinos,
    casa_de,
    grado_en_signo,
    signo,
)

load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))

HANA_HOST = os.getenv("HANA_HOST")
HANA_PORT = int(os.getenv("HANA_PORT", 443))
HANA_USER = os.getenv("HANA_USER")
HANA_PASS = os.getenv("HANA_PASS")

APP_CSS = """
@import url('https://fonts.googleapis.com/css2?family=Space+Grotesk:wght@400;500;700&family=IBM+Plex+Mono:wght@400;500&display=swap');

:root {
  --bg: #f4efe3;
  --panel: rgba(255, 251, 243, 0.86);
  --panel-strong: rgba(255, 248, 235, 0.97);
  --ink: #1f2430;
  --muted: #5b6472;
  --accent: #c45d2d;
  --accent-deep: #6e2f16;
  --line: rgba(107, 63, 35, 0.16);
  --shadow: 0 20px 45px rgba(72, 43, 27, 0.12);
}

body, .gradio-container {
  background:
    radial-gradient(circle at top left, rgba(196, 93, 45, 0.18), transparent 30%),
    radial-gradient(circle at top right, rgba(215, 176, 83, 0.18), transparent 28%),
    linear-gradient(180deg, #fbf7ef 0%, #f1eadc 100%);
  color: var(--ink);
  font-family: 'Space Grotesk', sans-serif;
}

.gradio-container {
  max-width: 1280px !important;
}

#hero {
  background: linear-gradient(135deg, rgba(255,255,255,0.72), rgba(255,248,235,0.92));
  border: 1px solid var(--line);
  border-radius: 28px;
  padding: 28px 32px;
  box-shadow: var(--shadow);
  backdrop-filter: blur(12px);
}

#hero h1 {
  margin: 0 0 10px 0;
  font-size: 2.6rem;
  line-height: 1;
  letter-spacing: -0.04em;
}

#hero p {
  margin: 0;
  color: var(--muted);
  font-size: 1.02rem;
}

.panel-card {
  background: var(--panel);
  border: 1px solid var(--line);
  border-radius: 24px;
  box-shadow: var(--shadow);
}

.panel-card .block {
  background: transparent !important;
}

.astro-note {
  color: var(--muted);
  font-size: 0.94rem;
}

.astro-kpi-grid {
  display: grid;
  grid-template-columns: repeat(3, minmax(0, 1fr));
  gap: 12px;
}

.astro-kpi {
  background: var(--panel-strong);
  border: 1px solid var(--line);
  border-radius: 18px;
  padding: 16px 18px;
}

.astro-kpi h3 {
  margin: 0 0 6px 0;
  font-size: 0.85rem;
  text-transform: uppercase;
  letter-spacing: 0.08em;
  color: var(--muted);
}

.astro-kpi p {
  margin: 0;
  font-size: 1.35rem;
  color: var(--accent-deep);
  font-weight: 700;
}

.gr-button-primary {
  background: linear-gradient(135deg, #c45d2d, #8b3d1e) !important;
  border: none !important;
}

.gr-button-secondary {
  border-color: rgba(110, 47, 22, 0.24) !important;
}
"""

PLANETA_LABELS = {
    "SOL": "Sol",
    "LUNA": "Luna",
    "MERCURIO": "Mercurio",
    "VENUS": "Venus",
    "MARTE": "Marte",
    "JUPITER": "Jupiter",
    "SATURNO": "Saturno",
    "URANO": "Urano",
    "NEPTUNO": "Neptuno",
    "PLUTON": "Pluton",
    "ASC": "Ascendente",
}

SIGN_TO_HOUSE = {
    "Aries": 1,
    "Tauro": 2,
    "Geminis": 3,
    "Cancer": 4,
    "Leo": 5,
    "Virgo": 6,
    "Libra": 7,
    "Escorpio": 8,
    "Sagitario": 9,
    "Capricornio": 10,
    "Acuario": 11,
    "Piscis": 12,
}

SEARCH_CATEGORIES = {
    "Música": ["MUSIC", "COMPOS", "MUSICIAN", "SINGER"],
    "Política": ["POLIT", "PRESID", "KING", "QUEEN"],
    "Ciencia": ["PHYSIC", "SCIENT", "CHEMIS", "MATHEM"],
    "Arte": ["ARTIS", "ACTOR", "PAINTER", "WRITER"],
    "Deporte": ["SPORT", "FUTBOL", "ATHLET", "PLAYER"],
}


def require_env(name: str, value: str | None) -> str:
    if value:
        return value
    raise RuntimeError(f"Falta la variable de entorno requerida: {name}")


def connect_hana():
    return dbapi.connect(
        address=require_env("HANA_HOST", HANA_HOST),
        port=HANA_PORT,
        user=require_env("HANA_USER", HANA_USER),
        password=require_env("HANA_PASS", HANA_PASS),
        encrypt=True,
        sslValidateCertificate=False,
    )


def fetch_dataframe(sql: str, params=None) -> pd.DataFrame:
    conn = connect_hana()
    cur = conn.cursor()
    cur.execute(sql, params or [])
    columns = [desc[0] for desc in cur.description]
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return pd.DataFrame(rows, columns=columns)


def chart_summary_markdown(pos: dict) -> str:
    casas = pos["_casas"]
    planetas = [
        "SOL", "LUNA", "MERCURIO", "VENUS", "MARTE",
        "JUPITER", "SATURNO", "URANO", "NEPTUNO", "PLUTON",
    ]
    lines = [
        "### Carta calculada",
        "",
        "| Factor | Signo | Grado | Casa |",
        "|---|---|---:|---:|",
    ]
    for planeta in planetas:
        grados = pos[planeta]
        lines.append(
            f"| {PLANETA_LABELS[planeta]} | {signo(grados)} | {grado_en_signo(grados)} | {casa_de(grados, casas)} |"
        )
    lines.append(f"| ASC | {signo(pos['ASC'])} | {grado_en_signo(pos['ASC'])} | - |")
    lines.append(f"| MC | {signo(pos['MC'])} | {grado_en_signo(pos['MC'])} | - |")
    return "\n".join(lines)


def top_match_markdown(vecinos_df: pd.DataFrame, feature_mode: str) -> str:
    if vecinos_df.empty:
        return "### Sin resultados\nNo se encontraron vecinos para esta carta."

    top = vecinos_df.iloc[0]
    return "\n".join(
        [
            "### Match principal",
            "",
            f"**{top['NOMBRE']}** ({int(top['ANIO']) if pd.notna(top['ANIO']) else 's/f'})",
            "",
            f"- Perfil: {top['PERFIL'] or 'sin descripción'}",
            f"- Distancia: **{top['DISTANCIA']:.2f}** usando {feature_mode}",
            f"- Cluster: **{int(top['CLUSTER_ID']) if pd.notna(top['CLUSTER_ID']) else '-'}**",
            f"- Sol/Luna/ASC: **{signo(float(top['SOL']))} / {signo(float(top['LUNA']))} / {signo(float(top['ASC_']))}**",
        ]
    )


def stats_html(vecinos_df: pd.DataFrame) -> str:
    if vecinos_df.empty:
        return "<div class='astro-kpi'><h3>Estado</h3><p>Sin resultados</p></div>"

    mean_distance = vecinos_df["DISTANCIA"].mean()
    return f"""
    <div class="astro-kpi-grid">
      <div class="astro-kpi">
        <h3>Vecinos encontrados</h3>
        <p>{len(vecinos_df)}</p>
      </div>
      <div class="astro-kpi">
        <h3>Mejor distancia</h3>
        <p>{vecinos_df.iloc[0]['DISTANCIA']:.2f}°</p>
      </div>
      <div class="astro-kpi">
        <h3>Media Top-N</h3>
        <p>{mean_distance:.2f}°</p>
      </div>
    </div>
    """


def normalize_neighbors(vecinos: pd.DataFrame) -> pd.DataFrame:
    if vecinos.empty:
        return vecinos

    vecinos = vecinos.drop_duplicates(subset=["NOMBRE", "ANIO", "DISTANCIA"]).copy()
    vecinos = vecinos.sort_values(["DISTANCIA", "ANIO"], ascending=[True, True])
    return vecinos.reset_index(drop=True)


def fetch_kepler_texts_for_chart(pos: dict) -> pd.DataFrame:
    casas = pos["_casas"]
    mappings = [
        ("Sol", signo(pos["SOL"]), casa_de(pos["SOL"], casas)),
        ("Luna", signo(pos["LUNA"]), casa_de(pos["LUNA"], casas)),
        ("Saturno", signo(pos["SATURNO"]), casa_de(pos["SATURNO"], casas)),
        ("Ascendente", signo(pos["ASC"]), SIGN_TO_HOUSE[signo(pos["ASC"])]),
    ]

    clauses = []
    params = []
    for planeta, sign_name, house_number in mappings:
        clauses.append("(PLANETA1 = ? AND (SIGNO = ? OR CASA = ?))")
        params.extend([planeta, sign_name, house_number])

    sql = f"""
SELECT TOP 24
    PLANETA1,
    FICHERO,
    CABECERA,
    SUBSTR(TEXTO, 1, 500) AS EXTRACTO
FROM DBADMIN.KEPLER_TEXTOS
WHERE FICHERO NOT IN ('PLANETAS.REV', 'ASPECTOS.ASC')
  AND ({' OR '.join(clauses)})
ORDER BY PLANETA1, FICHERO, CABECERA
"""
    return fetch_dataframe(sql, params)


def format_kepler_markdown(df: pd.DataFrame) -> str:
    if df.empty:
        return "### Textos Kepler\nNo se encontraron interpretaciones para esta carta."

    blocks = ["### Textos Kepler para esta carta", ""]
    for _, row in df.head(8).iterrows():
        blocks.append(f"**{row['PLANETA1']}** · `{row['FICHERO']}`")
        blocks.append(f"{row['CABECERA']}")
        blocks.append("")
        blocks.append(str(row["EXTRACTO"]).strip())
        blocks.append("")
    return "\n".join(blocks)


def run_lookup(year, month, day, hour, minute, gmt, lat, lon, top_n, use_all_planets):
    try:
        pos = calcular_carta(
            int(year),
            int(month),
            int(day),
            int(hour),
            int(minute),
            float(gmt),
            float(lat),
            float(lon),
        )
        features = FEATURES_12 if use_all_planets else FEATURES_8
        vecinos = buscar_vecinos(pos, top=int(top_n), features=features)
        vecinos_df = normalize_neighbors(pd.DataFrame(vecinos))
        if not vecinos_df.empty:
            vecinos_df = vecinos_df[["NOMBRE", "PERFIL", "ANIO", "CLUSTER_ID", "SOL", "LUNA", "ASC_", "DISTANCIA"]]

        feature_mode = "12 factores" if use_all_planets else "8 factores"
        chart_md = chart_summary_markdown(pos)
        top_md = top_match_markdown(vecinos_df, feature_mode)
        stats = stats_html(vecinos_df)
        kepler_df = fetch_kepler_texts_for_chart(pos)
        kepler_md = format_kepler_markdown(kepler_df)
        if not vecinos_df.empty:
            neighbors_view = vecinos_df.rename(
                columns={
                    "NOMBRE": "Nombre",
                    "PERFIL": "Perfil",
                    "ANIO": "Año",
                    "CLUSTER_ID": "Cluster",
                    "SOL": "Sol",
                    "LUNA": "Luna",
                    "ASC_": "Asc",
                    "DISTANCIA": "Distancia",
                }
            )
        else:
            neighbors_view = pd.DataFrame(columns=["Nombre", "Perfil", "Año", "Cluster", "Sol", "Luna", "Asc", "Distancia"])

        if not kepler_df.empty:
            kepler_view = kepler_df.rename(
                columns={
                    "PLANETA1": "Planeta",
                    "FICHERO": "Fichero",
                    "CABECERA": "Cabecera",
                    "EXTRACTO": "Texto",
                }
            )
        else:
            kepler_view = pd.DataFrame(columns=["Planeta", "Fichero", "Cabecera", "Texto"])

        status = (
            f"Consulta completada con {feature_mode}. "
            f"HANA devolvió vecinos y textos interpretativos de la carta calculada."
        )
        return chart_md, neighbors_view, top_md, stats, kepler_md, kepler_view, status
    except Exception as exc:
        empty_neighbors = pd.DataFrame(columns=["Nombre", "Perfil", "Año", "Cluster", "Sol", "Luna", "Asc", "Distancia"])
        empty_kepler = pd.DataFrame(columns=["Planeta", "Fichero", "Cabecera", "Texto"])
        return (
            "### Error\nNo se pudo calcular la carta.",
            empty_neighbors,
            f"### Error\n\n`{exc}`",
            "<div class='astro-kpi'><h3>Estado</h3><p>Error</p></div>",
            f"### Error\n\n`{exc}`",
            empty_kepler,
            f"Falló la consulta: {exc}",
        )


def load_cluster_overview():
    sql = """
SELECT
    CLUSTER_ID,
    COUNT(*) AS N_CARTAS,
    ROUND(AVG(SOL_GR), 1) AS SOL_MEDIO,
    ROUND(AVG(LUNA_GR), 1) AS LUNA_MEDIA,
    ROUND(AVG(ASC_GR), 1) AS ASC_MEDIO,
    ROUND(AVG(DIST_TO_CENTER), 3) AS COHESION
FROM DBADMIN.CARTAS_NATALES
WHERE CALC_ERROR = 0 AND CLUSTER_ID IS NOT NULL
GROUP BY CLUSTER_ID
ORDER BY CLUSTER_ID
"""
    df = fetch_dataframe(sql)
    if df.empty:
        return "<div class='astro-kpi'><h3>Clusters</h3><p>Sin datos</p></div>", df

    html = f"""
    <div class="astro-kpi-grid">
      <div class="astro-kpi">
        <h3>Clusters detectados</h3>
        <p>{len(df)}</p>
      </div>
      <div class="astro-kpi">
        <h3>Total cartas</h3>
        <p>{int(df['N_CARTAS'].sum())}</p>
      </div>
      <div class="astro-kpi">
        <h3>Cohesión media</h3>
        <p>{df['COHESION'].mean():.2f}</p>
      </div>
    </div>
    """
    return html, df.rename(
        columns={
            "CLUSTER_ID": "Cluster",
            "N_CARTAS": "Cartas",
            "SOL_MEDIO": "Sol medio",
            "LUNA_MEDIA": "Luna media",
            "ASC_MEDIO": "Asc medio",
            "COHESION": "Cohesión",
        }
    )


def load_cluster_members(cluster_id):
    try:
        sql = """
SELECT TOP 40
    NOMBRE,
    SUBSTR(DESCRIPCION, 3, 60) AS PERFIL,
    ANIO,
    ROUND(DIST_TO_CENTER, 3) AS DIST_CENTRO
FROM DBADMIN.CARTAS_NATALES
WHERE CLUSTER_ID = ? AND CALC_ERROR = 0
  AND LENGTH(NOMBRE) > 4
  AND NOMBRE NOT LIKE '%#%'
ORDER BY DIST_TO_CENTER ASC
"""
        df = fetch_dataframe(sql, [int(cluster_id)])
        if df.empty:
            return "### Cluster sin cartas\nNo hay registros para ese cluster.", df

        intro = "\n".join(
            [
                f"### Cluster {int(cluster_id)}",
                "",
                f"- Cartas mostradas: **{len(df)}**",
                f"- Distancia media al centro: **{df['DIST_CENTRO'].mean():.3f}**",
                f"- Primer perfil: **{df.iloc[0]['NOMBRE']}**",
            ]
        )
        return intro, df.rename(
            columns={
                "NOMBRE": "Nombre",
                "PERFIL": "Perfil",
                "ANIO": "Año",
                "DIST_CENTRO": "Distancia al centro",
            }
        )
    except Exception as exc:
        return f"### Error\n\n`{exc}`", pd.DataFrame(columns=["Nombre", "Perfil", "Año", "Distancia al centro"])


def search_category(category_name):
    try:
        tokens = SEARCH_CATEGORIES.get(category_name, [])
        if not tokens:
            return pd.DataFrame(columns=["Nombre", "Perfil", "Año", "Cluster", "Sol", "Luna"])
        where_clause = " OR ".join(["UPPER(DESCRIPCION) LIKE ?"] * len(tokens))
        params = [f"%{token}%" for token in tokens]
        sql = f"""
SELECT TOP 50
    NOMBRE,
    SUBSTR(DESCRIPCION, 3, 70) AS PERFIL,
    ANIO,
    CLUSTER_ID,
    ROUND(SOL_GR, 1) AS SOL,
    ROUND(LUNA_GR, 1) AS LUNA
FROM DBADMIN.CARTAS_NATALES
WHERE CALC_ERROR = 0
  AND ({where_clause})
ORDER BY ANIO DESC
"""
        df = fetch_dataframe(sql, params)
        return df.rename(
            columns={
                "NOMBRE": "Nombre",
                "PERFIL": "Perfil",
                "ANIO": "Año",
                "CLUSTER_ID": "Cluster",
                "SOL": "Sol",
                "LUNA": "Luna",
            }
        )
    except Exception:
        return pd.DataFrame(columns=["Nombre", "Perfil", "Año", "Cluster", "Sol", "Luna"])


def search_people(keyword):
    try:
        sql = """
SELECT TOP 60
    NOMBRE,
    SUBSTR(DESCRIPCION, 3, 80) AS PERFIL,
    ANIO,
    CLUSTER_ID,
    ROUND(SOL_GR, 1) AS SOL,
    ROUND(LUNA_GR, 1) AS LUNA,
    ROUND(ASC_GR, 1) AS ASC_
FROM DBADMIN.CARTAS_NATALES
WHERE CALC_ERROR = 0
  AND (
    UPPER(NOMBRE) LIKE ?
    OR UPPER(DESCRIPCION) LIKE ?
  )
ORDER BY ANIO DESC
"""
        token = f"%{str(keyword).upper()}%"
        df = fetch_dataframe(sql, [token, token])
        return df.rename(
            columns={
                "NOMBRE": "Nombre",
                "PERFIL": "Perfil",
                "ANIO": "Año",
                "CLUSTER_ID": "Cluster",
                "SOL": "Sol",
                "LUNA": "Luna",
                "ASC_": "Asc",
            }
        )
    except Exception:
        return pd.DataFrame(columns=["Nombre", "Perfil", "Año", "Cluster", "Sol", "Luna", "Asc"])


def search_kepler_texts(planet, sign_name, house_number):
    try:
        sql = """
SELECT TOP 40
    FICHERO,
    CABECERA,
    SUBSTR(TEXTO, 1, 700) AS TEXTO
FROM DBADMIN.KEPLER_TEXTOS
WHERE FICHERO NOT IN ('PLANETAS.REV', 'ASPECTOS.ASC')
  AND PLANETA1 = ?
  AND (SIGNO = ? OR CASA = ?)
ORDER BY FICHERO, CABECERA
"""
        df = fetch_dataframe(sql, [planet, sign_name, int(house_number)])
        summary = (
            f"### {planet} · {sign_name} / casa {house_number}\n\n"
            f"Resultados encontrados: **{len(df)}**"
        )
        return summary, df.rename(columns={"FICHERO": "Fichero", "CABECERA": "Cabecera", "TEXTO": "Texto"})
    except Exception as exc:
        return f"### Error\n\n`{exc}`", pd.DataFrame(columns=["Fichero", "Cabecera", "Texto"])


with gr.Blocks(title="AstroHana") as demo:
    gr.HTML(
        """
        <section id="hero">
          <h1>AstroHana</h1>
          <p>
            Consola visual para explotar SAP HANA con cartas natales, clustering, textos Kepler
            y exploración del dataset histórico.
          </p>
        </section>
        """
    )

    with gr.Tabs():
        with gr.Tab("Carta viva"):
            with gr.Row(equal_height=False):
                with gr.Column(scale=4, elem_classes="panel-card"):
                    gr.Markdown("## Datos de nacimiento")
                    gr.Markdown(
                        "Aquí sí hay más chicha: carta calculada, vecinos, y textos Kepler sobre tu combinación natal.",
                        elem_classes="astro-note",
                    )
                    with gr.Row():
                        year = gr.Number(value=1976, precision=0, label="Año")
                        month = gr.Number(value=10, precision=0, label="Mes")
                        day = gr.Number(value=11, precision=0, label="Día")
                    with gr.Row():
                        hour = gr.Number(value=20, precision=0, label="Hora")
                        minute = gr.Number(value=33, precision=0, label="Minuto")
                        gmt = gr.Number(value=1.0, precision=1, label="GMT")
                    with gr.Row():
                        lat = gr.Number(value=40.40, precision=4, label="Latitud")
                        lon = gr.Number(value=-3.70, precision=4, label="Longitud")
                    with gr.Row():
                        top_n = gr.Slider(minimum=5, maximum=30, step=1, value=12, label="Número de vecinos")
                        use_all_planets = gr.Checkbox(
                            value=False,
                            label="Usar 12 factores",
                            info="Incluye Urano, Neptuno, Plutón y MC además de los 8 factores base.",
                        )
                    with gr.Row():
                        run_btn = gr.Button("Activar carta", variant="primary")
                        clear_btn = gr.Button("Limpiar", variant="secondary")

                with gr.Column(scale=3):
                    stats = gr.HTML(value=stats_html(pd.DataFrame()))
                    status = gr.Markdown("Esperando una consulta.", elem_classes="astro-note")

            with gr.Row(equal_height=False):
                with gr.Column(scale=5, elem_classes="panel-card"):
                    chart_md = gr.Markdown("### Carta calculada\nAún no hay datos.")
                with gr.Column(scale=4, elem_classes="panel-card"):
                    top_md = gr.Markdown("### Match principal\nTodavía no has ejecutado ninguna búsqueda.")

            with gr.Row(equal_height=False):
                with gr.Column(scale=6):
                    neighbors_df = gr.Dataframe(
                        headers=["Nombre", "Perfil", "Año", "Cluster", "Sol", "Luna", "Asc", "Distancia"],
                        datatype=["str", "str", "number", "number", "number", "number", "number", "number"],
                        row_count=12,
                        column_count=(8, "fixed"),
                        wrap=True,
                        label="Vecinos encontrados",
                    )
                with gr.Column(scale=4):
                    kepler_md = gr.Markdown("### Textos Kepler\nTodavía no hay consulta.")

            kepler_df = gr.Dataframe(
                headers=["Planeta", "Fichero", "Cabecera", "Texto"],
                datatype=["str", "str", "str", "str"],
                row_count=8,
                column_count=(4, "fixed"),
                wrap=True,
                label="Interpretaciones recuperadas desde HANA",
            )

            run_btn.click(
                fn=run_lookup,
                inputs=[year, month, day, hour, minute, gmt, lat, lon, top_n, use_all_planets],
                outputs=[chart_md, neighbors_df, top_md, stats, kepler_md, kepler_df, status],
            )
            clear_btn.click(
                fn=lambda: (
                    "### Carta calculada\nAún no hay datos.",
                    pd.DataFrame(columns=["Nombre", "Perfil", "Año", "Cluster", "Sol", "Luna", "Asc", "Distancia"]),
                    "### Match principal\nTodavía no has ejecutado ninguna búsqueda.",
                    stats_html(pd.DataFrame()),
                    "### Textos Kepler\nTodavía no hay consulta.",
                    pd.DataFrame(columns=["Planeta", "Fichero", "Cabecera", "Texto"]),
                    "Esperando una consulta.",
                ),
                outputs=[chart_md, neighbors_df, top_md, stats, kepler_md, kepler_df, status],
            )

            gr.Examples(
                examples=[
                    [1976, 10, 11, 20, 33, 1.0, 40.40, -3.70, 12, False],
                    [1926, 6, 1, 17, 0, 0.0, 48.85, 2.35, 10, True],
                    [1984, 11, 22, 6, 15, -3.0, -34.60, -58.38, 15, False],
                ],
                inputs=[year, month, day, hour, minute, gmt, lat, lon, top_n, use_all_planets],
                label="Ejemplos rápidos",
            )

        with gr.Tab("Atlas de clusters"):
            gr.Markdown("## Clusters en HANA")
            gr.Markdown(
                "Vista analítica del K-Means guardado en HANA: volumen, cohesión y exploración por cluster.",
                elem_classes="astro-note",
            )
            with gr.Row():
                cluster_refresh = gr.Button("Recargar overview", variant="primary")
                cluster_id = gr.Number(value=4, precision=0, label="Cluster a explorar")
                cluster_members_btn = gr.Button("Ver miembros", variant="secondary")

            cluster_kpis = gr.HTML("<div class='astro-kpi'><h3>Clusters</h3><p>Sin cargar</p></div>")
            cluster_overview_df = gr.Dataframe(
                headers=["Cluster", "Cartas", "Sol medio", "Luna media", "Asc medio", "Cohesión"],
                datatype=["number", "number", "number", "number", "number", "number"],
                row_count=12,
                label="Overview de clusters",
            )
            cluster_intro = gr.Markdown("### Cluster\nSelecciona uno para ver cartas cercanas al centro.")
            cluster_members_df = gr.Dataframe(
                headers=["Nombre", "Perfil", "Año", "Distancia al centro"],
                datatype=["str", "str", "number", "number"],
                row_count=12,
                label="Miembros del cluster",
            )

            cluster_refresh.click(fn=load_cluster_overview, outputs=[cluster_kpis, cluster_overview_df])
            cluster_members_btn.click(fn=load_cluster_members, inputs=[cluster_id], outputs=[cluster_intro, cluster_members_df])

        with gr.Tab("Explorador de dataset"):
            gr.Markdown("## Buscar dentro del dataset")
            with gr.Row():
                category = gr.Dropdown(
                    choices=list(SEARCH_CATEGORIES.keys()),
                    value="Música",
                    label="Categoría rápida",
                )
                category_btn = gr.Button("Explorar categoría", variant="primary")
            category_df = gr.Dataframe(
                headers=["Nombre", "Perfil", "Año", "Cluster", "Sol", "Luna"],
                datatype=["str", "str", "number", "number", "number", "number"],
                row_count=12,
                label="Resultados por categoría",
            )
            with gr.Row():
                keyword = gr.Textbox(label="Palabra clave libre", placeholder="Einstein, actor, doctor, Chile...")
                keyword_btn = gr.Button("Buscar en HANA", variant="secondary")
            keyword_df = gr.Dataframe(
                headers=["Nombre", "Perfil", "Año", "Cluster", "Sol", "Luna", "Asc"],
                datatype=["str", "str", "number", "number", "number", "number", "number"],
                row_count=12,
                label="Búsqueda libre",
            )
            category_btn.click(fn=search_category, inputs=[category], outputs=[category_df])
            keyword_btn.click(fn=search_people, inputs=[keyword], outputs=[keyword_df])

        with gr.Tab("Kepler lab"):
            gr.Markdown("## Textos Kepler por planeta/signo/casa")
            gr.Markdown(
                "Acceso directo a la tabla `KEPLER_TEXTOS` para inspeccionar interpretaciones desde HANA.",
                elem_classes="astro-note",
            )
            with gr.Row():
                kepler_planet = gr.Dropdown(
                    choices=["Sol", "Luna", "Mercurio", "Venus", "Marte", "Jupiter", "Saturno", "Ascendente"],
                    value="Sol",
                    label="Planeta",
                )
                kepler_sign = gr.Dropdown(
                    choices=list(SIGN_TO_HOUSE.keys()),
                    value="Libra",
                    label="Signo",
                )
                kepler_house = gr.Slider(minimum=1, maximum=12, step=1, value=7, label="Casa natural")
                kepler_btn = gr.Button("Consultar textos", variant="primary")
            kepler_search_summary = gr.Markdown("### Consulta Kepler\nSelecciona planeta, signo y casa.")
            kepler_search_df = gr.Dataframe(
                headers=["Fichero", "Cabecera", "Texto"],
                datatype=["str", "str", "str"],
                row_count=10,
                label="Resultados en KEPLER_TEXTOS",
                wrap=True,
            )
            kepler_btn.click(
                fn=search_kepler_texts,
                inputs=[kepler_planet, kepler_sign, kepler_house],
                outputs=[kepler_search_summary, kepler_search_df],
            )


if __name__ == "__main__":
    demo.launch(css=APP_CSS)
