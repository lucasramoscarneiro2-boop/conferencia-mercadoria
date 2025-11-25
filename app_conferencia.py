import streamlit as st
import pandas as pd
import io
from datetime import datetime
import psycopg2
from psycopg2.extras import execute_values

# ==========================================================
# CONFIG STREAMLIT
# ==========================================================
st.set_page_config(
    page_title="Conferência de Mercadorias",
    layout="wide",
    page_icon="📦"
)

st.title("📦 Sistema de Conferência de Mercadorias")

st.markdown("""
1. Anexe a **planilha de conferência** (igual a usada na loja).  
2. O conferente digita ou escaneia o **código SAP** (futuramente EAN) e informa a **quantidade conferida**.  
3. O sistema soma as contagens por item e gera um **relatório de OK / Faltando / Sobrando**.  
4. No final, clique em **“Salvar conferência desta viagem no Supabase”** para gravar o histórico.
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
# FUNÇÃO PARA LER A PLANILHA NO FORMATO REAL DA LOJA
# ==========================================================
def carregar_planilha_nf(uploaded_file):
    """
    Lê a planilha no formato da loja:
    Roll | Guia | CodSap | Depto | Descrição | Qtde | Qtde Real | Nota Fiscal | Vl Unitário | Vl Total

    Também captura:
    - Viagem: xxxx
    - Loja: Lxxx
    - Data: dd-mm-aaaa
    """
    # 1ª leitura sem header só para achar metadados e a linha do cabeçalho
    df_raw = pd.read_excel(uploaded_file, header=None)

    # ------------------------------
    # Metadados: Viagem / Loja / Data (normalmente na primeira linha)
    # ------------------------------
    meta_viagem = None
    meta_loja = None
    meta_data = None

    first_row = df_raw.iloc[0].astype(str)
    for value in first_row:
        if "Viagem" in value:
            partes = value.split(":")
            if len(partes) > 1:
                meta_viagem = partes[1].strip()
        if "Loja" in value:
            partes = value.split(":")
            if len(partes) > 1:
                meta_loja = partes[1].strip()
        if "Data" in value:
            partes = value.split(":")
            if len(partes) > 1:
                meta_data = partes[1].strip()

    # ------------------------------
    # Procura a linha de cabeçalho (onde aparece "CodSap")
    # ------------------------------
    header_row_candidates = df_raw.index[
        df_raw.apply(
            lambda row: row.astype(str).str.contains("CodSap", case=False, na=False).any(),
            axis=1
        )
    ]

    if len(header_row_candidates) == 0:
        raise ValueError("Não encontrei a linha de cabeçalho com 'CodSap' na planilha.")

    header_row = int(header_row_candidates[0])

    # Volta o ponteiro do arquivo para o início para ler de novo
    uploaded_file.seek(0)

    # Lê de novo, agora usando essa linha como cabeçalho
    df = pd.read_excel(uploaded_file, header=header_row)

    # Normaliza nomes de colunas (tira espaços, deixa uniforme)
    df.columns = [str(c).strip() for c in df.columns]

    # Descobre as colunas importantes pelo nome
    def achar_coluna(busca, excluir=None):
        busca = busca.upper()
        for c in df.columns:
            nome = c.upper()
            if busca in nome and (not excluir or excluir.upper() not in nome):
                return c
        return None

    col_cod_sap = achar_coluna("CODSAP")
    col_desc    = achar_coluna("DESCRI")
    # Pega "Qtde" que NÃO seja "Qtde Real"
    col_qtd     = achar_coluna("QTDE", excluir="REAL")

    if not col_cod_sap or not col_desc or not col_qtd:
        raise ValueError(
            f"Não consegui identificar colunas CodSap/Descrição/Qtde. "
            f"Encontrei: CodSap={col_cod_sap}, Descrição={col_desc}, Qtde={col_qtd}"
        )

    df_nf = pd.DataFrame({
        "codigo": df[col_cod_sap].astype(str).str.strip(),   # por enquanto é o CodSap
        "descricao": df[col_desc].astype(str).str.strip(),
        "qtd_prevista": pd.to_numeric(df[col_qtd], errors="coerce").fillna(0).astype(int)
    })

    # Remove linhas totalmente vazias de código/descrição
    df_nf = df_nf[(df_nf["codigo"] != "") & (df_nf["descricao"] != "")]
    df_nf = df_nf.reset_index(drop=True)

    # Retorna também os metadados
    return df_nf, meta_viagem, meta_loja, meta_data

# ==========================================================
# ESTADO DA APLICAÇÃO
# ==========================================================
if "df_nf" not in st.session_state:
    st.session_state.df_nf = None
if "df_conferencia" not in st.session_state:
    st.session_state.df_conferencia = None
if "meta_viagem" not in st.session_state:
    st.session_state.meta_viagem = None
if "meta_loja" not in st.session_state:
    st.session_state.meta_loja = None
if "meta_data" not in st.session_state:
    st.session_state.meta_data = None

# ==========================================================
# 1. UPLOAD DA PLANILHA
# ==========================================================
arquivo = st.file_uploader(
    "📎 Anexe a planilha de conferência (Excel da loja)",
    type=["xlsx", "xls"]
)

if arquivo is not None and st.session_state.df_nf is None:
    try:
        df_nf, meta_viagem, meta_loja, meta_data = carregar_planilha_nf(arquivo)
    except Exception as e:
        st.error(f"Erro ao ler a planilha: {e}")
        st.stop()

    st.session_state.df_nf = df_nf
    st.session_state.meta_viagem = meta_viagem
    st.session_state.meta_loja = meta_loja
    st.session_state.meta_data = meta_data

    # DataFrame de conferência começa com qtd_contada = 0
    df_conf = df_nf.copy()
    df_conf["qtd_contada"] = 0
    st.session_state.df_conferencia = df_conf

if st.session_state.df_nf is None:
    st.info("👆 Anexe a planilha da loja para iniciar a conferência.")
    st.stop()

df_nf = st.session_state.df_nf
df_conf = st.session_state.df_conferencia

# ==========================================================
# CABEÇALHO DA VIAGEM / LOJA / DATA
# ==========================================================
viagem = st.session_state.meta_viagem or "N/D"
loja   = st.session_state.meta_loja or "N/D"
data_v_str = st.session_state.meta_data or "N/D"

st.markdown(
    f"**Viagem:** `{viagem}` &nbsp;&nbsp;|&nbsp;&nbsp; "
    f"**Loja:** `{loja}` &nbsp;&nbsp;|&nbsp;&nbsp; "
    f"**Data:** `{data_v_str}`"
)

with st.expander("👁️ Visualizar itens da NF (base para conferência)", expanded=False):
    st.dataframe(df_nf, use_container_width=True)

st.markdown("---")

# ==========================================================
# 2. ÁREA DE CONTAGEM
# ==========================================================
st.subheader("🧾 Lançar contagem dos produtos")

col1, col2, col3 = st.columns([2, 1, 1])

with col1:
    codigo_digitado = st.text_input(
        "Código (SAP por enquanto, futuramente EAN)",
        placeholder="Aponte o leitor no código ou digite",
        key="input_codigo"
    )

with col2:
    qtd_lida = st.number_input(
        "Quantidade conferida",
        min_value=1,
        step=1,
        value=1,
        key="input_qtd"
    )

with col3:
    confirmar = st.button("➕ Adicionar à contagem")

if confirmar and codigo_digitado.strip() != "":
    codigo = codigo_digitado.strip()

    # Procura o código na base
    mask = df_conf["codigo"] == codigo
    if mask.any():
        idx = df_conf[mask].index[0]
        st.session_state.df_conferencia.loc[idx, "qtd_contada"] += int(qtd_lida)
        produto = df_conf.loc[idx, "descricao"]
        st.success(f"Contagem adicionada para: {produto}")
    else:
        # Não estava na planilha → sobra
        nova_linha = pd.DataFrame([{
            "codigo": codigo,
            "descricao": "NÃO CADASTRADO NA PLANILHA",
            "qtd_prevista": 0,
            "qtd_contada": int(qtd_lida)
        }])
        st.session_state.df_conferencia = pd.concat(
            [st.session_state.df_conferencia, nova_linha],
            ignore_index=True
        )
        st.warning("Código não estava na planilha. Incluído como item SOBRANDO (qtd_prevista = 0).")

    # Limpa para próxima leitura
    st.session_state.input_codigo = ""
    st.session_state.input_qtd = 1

    df_conf = st.session_state.df_conferencia

# ==========================================================
# 3. PARCIAL E STATUS
# ==========================================================
st.markdown("### 🧮 Parcial da conferência")

df_parcial = df_conf.copy()
df_parcial["diferenca"] = df_parcial["qtd_contada"] - df_parcial["qtd_prevista"]

def classificar_status(row):
    if row["qtd_prevista"] == 0 and row["qtd_contada"] > 0:
        return "SOBRANDO (não estava na planilha)"
    if row["diferenca"] == 0:
        return "OK"
    elif row["diferenca"] > 0:
        return "SOBRANDO"
    else:
        return "FALTANDO"

df_parcial["status"] = df_parcial.apply(classificar_status, axis=1)

st.dataframe(
    df_parcial[[
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
# 4. RESUMO
# ==========================================================
st.markdown("---")
st.subheader("📊 Resumo da conferência")

col_ok, col_faltando, col_sobrando = st.columns(3)

df_ok       = df_parcial[df_parcial["status"] == "OK"]
df_faltando = df_parcial[df_parcial["status"].str.startswith("FALTANDO")]
df_sobrando = df_parcial[df_parcial["status"].str.startswith("SOBRANDO")]

with col_ok:
    st.metric("Itens OK", len(df_ok))

with col_faltando:
    st.metric("Itens FALTANDO", len(df_faltando))

with col_sobrando:
    st.metric("Itens SOBRANDO", len(df_sobrando))

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
# 5. DOWNLOAD DO RELATÓRIO EM EXCEL (OPCIONAL)
# ==========================================================
st.markdown("### 📥 Exportar relatório atual (Excel)")

def gerar_excel_relatorio(df_resultado: pd.DataFrame) -> bytes:
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df_resultado.to_excel(writer, index=False, sheet_name="Conferencia")
    return output.getvalue()

arquivo_excel = gerar_excel_relatorio(df_parcial)

st.download_button(
    label="⬇️ Baixar relatório em Excel (esta conferência)",
    data=arquivo_excel,
    file_name="relatorio_conferencia.xlsx",
    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
)

# ==========================================================
# 6. SALVAR CONFERÊNCIA NO SUPABASE
# ==========================================================
st.markdown("### 💾 Salvar conferência desta viagem no Supabase")

def parse_data_viagem(data_str: str):
    """
    Converte '25-11-2025' ou '25/11/2025' em datetime.date.
    Se não conseguir, retorna None.
    """
    if not data_str or data_str == "N/D":
        return None
    for fmt in ("%d-%m-%Y", "%d/%m/%Y"):
        try:
            return datetime.strptime(data_str, fmt).date()
        except ValueError:
            continue
    return None

def salvar_conferencia_supabase(df_resultado: pd.DataFrame, viagem: str, loja: str, data_viagem_str: str):
    data_viagem = parse_data_viagem(data_viagem_str)

    conn = get_conn()
    try:
        with conn:
            with conn.cursor() as cur:
                # 1) Insere cabeçalho da conferência
                cur.execute("""
                    insert into public.conferencias_viagem (
                        viagem, loja, data_viagem, arquivo_origem
                    ) values (%s, %s, %s, %s)
                    returning id;
                """, (viagem, loja, data_viagem, None))
                conferencia_id = cur.fetchone()[0]

                # 2) Insere itens da conferência
                rows = []
                for _, row in df_resultado.iterrows():
                    rows.append((
                        conferencia_id,
                        str(row["codigo"]),
                        str(row["descricao"]),
                        int(row["qtd_prevista"]),
                        int(row["qtd_contada"]),
                        int(row["diferenca"]),
                        str(row["status"]),
                    ))

                execute_values(cur, """
                    insert into public.conferencias_viagem_itens (
                        conferencia_id, codigo, descricao,
                        qtd_prevista, qtd_contada, diferenca, status
                    ) values %s
                """, rows)

        return conferencia_id
    finally:
        conn.close()

if st.button("💾 Salvar conferência desta viagem no Supabase"):
    try:
        conf_id = salvar_conferencia_supabase(df_parcial, viagem, loja, data_v_str)
        st.success(f"Conferência salva no Supabase com id = {conf_id}")
    except Exception as e:
        st.error(f"Erro ao salvar no Supabase: {e}")
