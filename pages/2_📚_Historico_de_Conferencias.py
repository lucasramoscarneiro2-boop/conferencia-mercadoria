import streamlit as st
import pandas as pd
import psycopg2
import io
from datetime import datetime

# ==========================================================
# CONFIG STREAMLIT
# ==========================================================
st.set_page_config(
    page_title="Histórico de Conferências",
    layout="wide",
    page_icon="📚"
)

st.title("📚 Histórico de Conferências de Viagens")

st.markdown("""
Aqui você consegue consultar todas as conferências já **salvas no Supabase**:

- Filtrar por **loja**, **viagem** e período de data
- Ver o resumo de cada conferência
- Abrir os **itens detalhados** de uma conferência específica
- Baixar o **Excel** dessa conferência
""")

# ==========================================================
# CONEXÃO COM SUPABASE (POSTGRES)
# ==========================================================
cfg = st.secrets["postgres"]

def get_conn():
    return psycopg2.connect(
        host=cfg["host"],
        port=cfg["port"],
        database=cfg["database"],
        user=cfg["user"],
        password=cfg["password"],
        sslmode=cfg.get("sslmode", "require")
    )

# ==========================================================
# CARREGAR CONFERÊNCIAS DO BANCO
# ==========================================================
@st.cache_data(ttl=60)
def carregar_conferencias():
    conn = get_conn()
    try:
        query = """
            select
                id,
                viagem,
                loja,
                data_viagem,
                data_hora_conferencia,
                criado_em
            from public.conferencias_viagem
            order by data_hora_conferencia desc
        """
        df = pd.read_sql_query(query, conn)
    finally:
        conn.close()
    return df

@st.cache_data(ttl=60)
def carregar_itens_conferencia(conferencia_id: int):
    conn = get_conn()
    try:
        query = """
            select
                codigo,
                descricao,
                qtd_prevista,
                qtd_contada,
                diferenca,
                status
            from public.conferencias_viagem_itens
            where conferencia_id = %s
            order by descricao
        """
        df = pd.read_sql_query(query, conn, params=(conferencia_id,))
    finally:
        conn.close()
    return df

df_conf = carregar_conferencias()

# Garantir que datas estão no tipo correto
if "data_viagem" in df_conf.columns:
    df_conf["data_viagem"] = pd.to_datetime(df_conf["data_viagem"], errors="coerce")

if "data_hora_conferencia" in df_conf.columns:
    df_conf["data_hora_conferencia"] = pd.to_datetime(df_conf["data_hora_conferencia"], errors="coerce")


if df_conf.empty:
    st.info("Nenhuma conferência encontrada ainda. Volte para a página principal, faça uma conferência e salve no Supabase.")
    st.stop()

# ==========================================================
# FILTROS (SIDEBAR)
# ==========================================================
st.sidebar.header("Filtros")

# Loja
lojas = sorted(df_conf["loja"].dropna().unique().tolist())
loja_sel = st.sidebar.selectbox("Filtrar por loja", options=["Todas"] + lojas)

# Filtra por loja
if loja_sel != "Todas":
    df_filtrado_loja = df_conf[df_conf["loja"] == loja_sel]
else:
    df_filtrado_loja = df_conf.copy()

# Viagem
viagens = sorted(df_filtrado_loja["viagem"].dropna().unique().tolist())
viagem_sel = st.sidebar.selectbox("Filtrar por viagem", options=["Todas"] + viagens)

# Período (data da conferência)
data_min = df_filtrado_loja["data_hora_conferencia"].min().date()
data_max = df_filtrado_loja["data_hora_conferencia"].max().date()

data_ini, data_fim = st.sidebar.date_input(
    "Período da conferência",
    value=(data_min, data_max),
    min_value=data_min,
    max_value=data_max
)

df_filtro = df_conf.copy()

if loja_sel != "Todas":
    df_filtro = df_filtro[df_filtro["loja"] == loja_sel]

if viagem_sel != "Todas":
    df_filtro = df_filtro[df_filtro["viagem"] == viagem_sel]

df_filtro = df_filtro[
    (df_filtro["data_hora_conferencia"].dt.date >= data_ini) &
    (df_filtro["data_hora_conferencia"].dt.date <= data_fim)
]

if df_filtro.empty:
    st.warning("Nenhuma conferência encontrada com os filtros selecionados.")
    st.stop()

# ==========================================================
# LISTA DE CONFERÊNCIAS
# ==========================================================
st.subheader("📋 Lista de conferências encontradas")

df_mostrar = df_filtro.copy()
df_mostrar["data_viagem_fmt"] = df_mostrar["data_viagem"].dt.strftime("%d/%m/%Y").fillna("")
df_mostrar["data_conf_fmt"] = df_mostrar["data_hora_conferencia"].dt.strftime("%d/%m/%Y %H:%M")

df_mostrar = df_mostrar[[
    "id",
    "viagem",
    "loja",
    "data_viagem_fmt",
    "data_conf_fmt"
]].rename(columns={
    "id": "ID",
    "viagem": "Viagem",
    "loja": "Loja",
    "data_viagem_fmt": "Data da viagem",
    "data_conf_fmt": "Data/hora da conferência"
})

st.dataframe(df_mostrar, use_container_width=True)

# Escolher conferência específica
st.markdown("### 🔎 Selecionar uma conferência para ver detalhes")

opcoes = []
for _, row in df_filtro.sort_values("data_hora_conferencia", ascending=False).iterrows():
    label = f"#{row['id']} | Viagem {row['viagem']} | Loja {row['loja']} | {row['data_hora_conferencia'].strftime('%d/%m/%Y %H:%M')}"
    opcoes.append((label, int(row["id"]), row["viagem"], row["loja"]))

labels = [o[0] for o in opcoes]
ids = [o[1] for o in opcoes]
viagens_sel = [o[2] for o in opcoes]
lojas_sel = [o[3] for o in opcoes]

escolha = st.selectbox("Conferência:", options=labels)
idx = labels.index(escolha)
conferencia_id = ids[idx]
viagem_escolhida = viagens_sel[idx]
loja_escolhida = lojas_sel[idx]

st.info(f"Conferência selecionada: **ID {conferencia_id} | Viagem {viagem_escolhida} | Loja {loja_escolhida}**")

# ==========================================================
# ITENS DA CONFERÊNCIA SELECIONADA
# ==========================================================
st.markdown("### 📦 Itens da conferência selecionada")

df_itens = carregar_itens_conferencia(conferencia_id)

if df_itens.empty:
    st.warning("Nenhum item encontrado para esta conferência.")
    st.stop()

# Resumo por status
col1, col2, col3 = st.columns(3)
df_ok       = df_itens[df_itens["status"] == "OK"]
df_faltando = df_itens[df_itens["status"].str.startswith("FALTANDO")]
df_sobrando = df_itens[df_itens["status"].str.startswith("SOBRANDO")]

with col1:
    st.metric("Itens OK", len(df_ok))
with col2:
    st.metric("Itens FALTANDO", len(df_faltando))
with col3:
    st.metric("Itens SOBRANDO", len(df_sobrando))

with st.expander("📄 Ver todos os itens da conferência", expanded=True):
    st.dataframe(
        df_itens[[
            "codigo",
            "descricao",
            "qtd_prevista",
            "qtd_contada",
            "diferenca",
            "status"
        ]],
        use_container_width=True
    )

with st.expander("🔍 Ver somente itens FALTANDO"):
    st.dataframe(
        df_faltando[[
            "codigo",
            "descricao",
            "qtd_prevista",
            "qtd_contada",
            "diferenca",
            "status"
        ]],
        use_container_width=True
    )

with st.expander("🔍 Ver somente itens SOBRANDO"):
    st.dataframe(
        df_sobrando[[
            "codigo",
            "descricao",
            "qtd_prevista",
            "qtd_contada",
            "diferenca",
            "status"
        ]],
        use_container_width=True
    )

# ==========================================================
#  DOWNLOAD EM EXCEL DA CONFERÊNCIA
# ==========================================================
st.markdown("### 📥 Exportar esta conferência para Excel")

def gerar_excel_conferencia(df: pd.DataFrame) -> bytes:
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="Conferencia")
    return output.getvalue()

# Excel com todos os itens
excel_todos = gerar_excel_conferencia(df_itens)

st.download_button(
    label="⬇️ Baixar Excel com **todos os itens** desta conferência",
    data=excel_todos,
    file_name=f"conferencia_viagem_{viagem_escolhida}_loja_{loja_escolhida}_ID{conferencia_id}.xlsx",
    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
)

# Excel só com faltando + sobrando (sem OK)
df_fs = pd.concat([df_faltando, df_sobrando], ignore_index=True)
if not df_fs.empty:
    excel_fs = gerar_excel_conferencia(df_fs)
    st.download_button(
        label="⬇️ Baixar Excel somente com **FALTANDO + SOBRANDO**",
        data=excel_fs,
        file_name=f"conferencia_FS_viagem_{viagem_escolhida}_loja_{loja_escolhida}_ID{conferencia_id}.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )
