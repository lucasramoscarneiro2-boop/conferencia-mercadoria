import streamlit as st
import pandas as pd
import io
from datetime import datetime
import psycopg2
from psycopg2.extras import execute_values
import streamlit.components.v1 as components  # <-- ADICIONADO: para usar HTML/JS (câmera)

# ==========================================================
# CONFIG STREAMLIT
# ==========================================================
st.set_page_config(
    page_title="Conferência de Mercadorias",
    layout="wide",
    page_icon="📦"
)

st.title("📦 Sistema de Conferência de Mercadorias")

# ==========================================================
# ALERTA DE SAIR SEM SALVAR
# ==========================================================

# Controla se a conferência já foi salva
if "conferencia_salva" not in st.session_state:
    st.session_state.conferencia_salva = False

# Script JS — alerta ao tentar sair
alerta_js = """
<script>
window.addEventListener("beforeunload", function (e) {
    // Só alerta se ainda não foi salvo
    if (window.conferencia_salva !== true) {
        e.preventDefault();
        e.returnValue = '';
        return '';
    }
});
</script>
"""

# Injeta variável JS responsável
st.markdown(f"""
<script>
window.conferencia_salva = {str(st.session_state.conferencia_salva).lower()};
</script>
""", unsafe_allow_html=True)

# Injeta o alerta na página
st.markdown(alerta_js, unsafe_allow_html=True)

# ==========================================================
# JS PARA BEEP DO LEITOR (ÁUDIO DE CONFIRMAÇÃO)
# ==========================================================
st.markdown("""
<script>
window.playBeep = function() {
    try {
        const ctx = new (window.AudioContext || window.webkitAudioContext)();
        const oscillator = ctx.createOscillator();
        const gainNode = ctx.createGain();
        oscillator.type = "sine";
        oscillator.frequency.setValueAtTime(900, ctx.currentTime);
        gainNode.gain.setValueAtTime(0.1, ctx.currentTime);  // volume baixo
        oscillator.connect(gainNode);
        gainNode.connect(ctx.destination);
        oscillator.start();
        oscillator.stop(ctx.currentTime + 0.08);  // beep curto
    } catch (e) {
        console.log("AudioContext error: ", e);
    }
};
</script>
""", unsafe_allow_html=True)

st.markdown("""
1. Anexe a **planilha de conferência** (igual a usada na loja).  
2. O conferente digita ou escaneia o **código SAP** (futuramente EAN) e informa a **quantidade conferida**.  
3. O sistema soma as contagens por item e gera um **relatório de OK / Faltando / Sobrando**.  
4. No final, clique em **“Salvar conferência desta viagem”** para gravar o histórico.
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
# FUNÇÕES AUXILIARES DE BANCO (AUTO-SAVE POR CONTAGEM)
# ==========================================================
def obter_ou_criar_conferencia(viagem: str, loja: str, data_viagem_str: str):
    """
    Busca a conferência mais recente para (viagem, loja, data_viagem).
    Se não existir, cria um novo registro e retorna o id.
    Usado para auto-salvar a cada contagem.
    """
    data_viagem = parse_data_viagem(data_viagem_str)
    conn = get_conn()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute("""
                    select id
                    from public.conferencias_viagem
                    where viagem = %s
                      and loja = %s
                      and (data_viagem is not distinct from %s)
                    order by id desc
                    limit 1;
                """, (viagem, loja, data_viagem))
                row = cur.fetchone()
                if row:
                    return row[0]

                # Não existe → cria
                cur.execute("""
                    insert into public.conferencias_viagem (
                        viagem, loja, data_viagem, arquivo_origem
                    ) values (%s, %s, %s, %s)
                    returning id;
                """, (viagem, loja, data_viagem, None))
                conferencia_id = cur.fetchone()[0]
                return conferencia_id
    finally:
        conn.close()

def carregar_itens_conferencia(conferencia_id: int) -> pd.DataFrame:
    """
    Carrega itens já registrados no banco para essa conferência (auto-salvos).
    """
    conn = get_conn()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute("""
                    select codigo, descricao, qtd_prevista, qtd_contada
                    from public.conferencias_viagem_itens
                    where conferencia_id = %s;
                """, (conferencia_id,))
                rows = cur.fetchall()
                if not rows:
                    return pd.DataFrame(columns=["codigo", "descricao", "qtd_prevista", "qtd_contada"])
                return pd.DataFrame(rows, columns=["codigo", "descricao", "qtd_prevista", "qtd_contada"])
    finally:
        conn.close()

def calcular_status_db(qtd_prevista: int, qtd_contada: int) -> str:
    diferenca = qtd_contada - qtd_prevista
    if qtd_prevista == 0 and qtd_contada > 0:
        return "SOBRANDO (não estava na planilha)"
    if diferenca == 0:
        return "OK"
    elif diferenca > 0:
        return "SOBRANDO"
    else:
        return "FALTANDO"

def registrar_contagem_db(conferencia_id: int, codigo: str, descricao: str,
                          qtd_prevista: int, qtd_add: int):
    """
    Atualiza ou insere o item da conferência no banco, somando a quantidade contada.
    Chamada a cada contagem lançada (auto-save).
    """
    conn = get_conn()
    try:
        with conn:
            with conn.cursor() as cur:
                # Verifica se já existe registro desse código
                cur.execute("""
                    select id, qtd_contada
                    from public.conferencias_viagem_itens
                    where conferencia_id = %s
                      and codigo = %s;
                """, (conferencia_id, codigo))
                row = cur.fetchone()
                if row:
                    item_id, qtd_atual = row
                    nova_qtd = int((qtd_atual or 0) + qtd_add)
                    diferenca = nova_qtd - int(qtd_prevista)
                    status = calcular_status_db(int(qtd_prevista), nova_qtd)
                    cur.execute("""
                        update public.conferencias_viagem_itens
                        set qtd_contada = %s,
                            qtd_prevista = %s,
                            diferenca = %s,
                            status = %s
                        where id = %s;
                    """, (nova_qtd, int(qtd_prevista), diferenca, status, item_id))
                else:
                    nova_qtd = int(qtd_add)
                    diferenca = nova_qtd - int(qtd_prevista)
                    status = calcular_status_db(int(qtd_prevista), nova_qtd)
                    cur.execute("""
                        insert into public.conferencias_viagem_itens (
                            conferencia_id, codigo, descricao,
                            qtd_prevista, qtd_contada, diferenca, status
                        ) values (%s, %s, %s, %s, %s, %s, %s);
                    """, (conferencia_id, codigo, descricao,
                          int(qtd_prevista), nova_qtd, diferenca, status))
    finally:
        conn.close()

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
        "codigo_original": df[col_cod_sap].astype(str).str.strip(),
        "codigo": df[col_cod_sap].astype(str).str.lstrip("0").str.strip(),  # <- sem zeros à esquerda
        "descricao": df[col_desc].astype(str).str.strip(),
        "qtd_prevista": pd.to_numeric(df[col_qtd], errors="coerce").fillna(0).astype(int)
    })

    # ==========================================================
    # LIMPEZA ROBUSTA: REMOVER LINHAS INVÁLIDAS DA PLANILHA
    # ==========================================================

    # Remove NaN em código e descrição
    df_nf = df_nf.dropna(subset=["codigo", "descricao"])

    # Remove cabeçalho repetido no meio da planilha
    df_nf = df_nf[~df_nf["codigo_original"].str.upper().str.contains("CODSAP", na=False)]
    df_nf = df_nf[~df_nf["descricao"].str.upper().str.contains("DESCRI", na=False)]

    # Remove linhas sem código (após remover zeros)
    df_nf = df_nf[df_nf["codigo"].str.strip() != ""]

    # Remove linhas onde descrição é literalmente 'nan'
    df_nf = df_nf[df_nf["descricao"].str.lower() != "nan"]

    # Remove linhas onde código é 'nan'
    df_nf = df_nf[df_nf["codigo"].str.lower() != "nan"]

    # Remove linhas em que não há quantidade prevista (linha lixo típica)
    df_nf = df_nf[df_nf["qtd_prevista"].notna()]

    # Remove linhas completamente vazias com qtd_prevista = 0 e descricao = nan
    df_nf = df_nf[~((df_nf["qtd_prevista"] == 0) & (df_nf["descricao"].str.lower() == "nan"))]

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
if "camera_ativa" not in st.session_state:
    st.session_state.camera_ativa = False  # <-- estado do leitor de câmera
if "conferencia_id" not in st.session_state:
    st.session_state.conferencia_id = None  # <-- id da conferência no banco para autosave

# ==========================================================
# 1. UPLOAD DA PLANILHA
# ==========================================================
arquivo = st.file_uploader(
    "📎 Anexe a planilha de conferência (Excel da loja)",
    type=["xlsx", "xls"]
)

if arquivo is not None and st.session_state.df_nf is None:

    # Nova planilha → nova conferência → volta a exigir salvar
    st.session_state.conferencia_salva = False
    st.markdown("""
    <script>
    window.conferencia_salva = false;
    </script>
    """, unsafe_allow_html=True)

    try:
        df_nf, meta_viagem, meta_loja, meta_data = carregar_planilha_nf(arquivo)
    except Exception as e:
        st.error(f"Erro ao ler a planilha: {e}")
        st.stop()

    st.session_state.df_nf = df_nf
    st.session_state.meta_viagem = meta_viagem or "N/D"
    st.session_state.meta_loja = meta_loja or "N/D"
    st.session_state.meta_data = meta_data or "N/D"

    # ======================================================
    # AQUI: CRIA OU REAPROVEITA CONFERÊNCIA NO BANCO (AUTO-SAVE)
    # ======================================================
    conferencia_id = obter_ou_criar_conferencia(
        st.session_state.meta_viagem,
        st.session_state.meta_loja,
        st.session_state.meta_data
    )
    st.session_state.conferencia_id = conferencia_id

    # Carrega itens já existentes no banco para essa conferência (se houver)
    df_itens_db = carregar_itens_conferencia(conferencia_id)

    # DataFrame base da NF
    df_conf = df_nf.copy()

    if df_itens_db.empty:
        # Nenhuma contagem anterior → começa em zero
        df_conf["qtd_contada"] = 0
    else:
        # Merge NF com o que já foi contado
        merge = df_nf.merge(
            df_itens_db[["codigo", "qtd_contada"]],
            on="codigo",
            how="left"
        )
        df_conf["qtd_contada"] = merge["qtd_contada"].fillna(0).astype(int)

        # Itens que existem no banco mas não estão na NF → itens sobrando
        codigos_nf = set(df_nf["codigo"])
        df_extra = df_itens_db[~df_itens_db["codigo"].isin(codigos_nf)].copy()
        if not df_extra.empty:
            df_extra["qtd_prevista"] = 0
            df_conf = pd.concat(
                [df_conf, df_extra[["codigo", "descricao", "qtd_prevista", "qtd_contada"]]],
                ignore_index=True
            )

    if "codigo_original" not in df_conf.columns:
        df_conf["codigo_original"] = df_conf["codigo"]

    st.session_state.df_conferencia = df_conf

if st.session_state.df_nf is None:
    st.info("👆 Anexe a planilha da loja para iniciar a conferência.")
    st.stop()

df_nf = st.session_state.df_nf
df_conf = st.session_state.df_conferencia
conferencia_id = st.session_state.conferencia_id

# ==========================================================
# CABEÇALHO DA VIAGEM / LOJA / DATA
# ==========================================================
viagem = st.session_state.meta_viagem or "N/D"
loja   = st.session_state.meta_loja or "N/D"
data_v_str = st.session_state.meta_data or "N/D"

st.markdown(
    f"**Viagem:** `{viagem}` &nbsp;&nbsp;|&nbsp;&nbsp; "
    f"**Loja:** `{loja}` &nbsp;&nbsp;|&nbsp;&nbsp; "
    f"**Data:** `{data_v_str}` &nbsp;&nbsp;|&nbsp;&nbsp; "
    f"**ID Conferência (auto-save):** `{conferencia_id}`"
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


# ==========================================================
# 2.1 LEITOR POR CÂMERA (OPCIONAL, VIA QUAGGAJS)
# ==========================================================
st.markdown("### 📸 Leitura por câmera (opcional)")

cam_col1, cam_col2 = st.columns([1, 3])
with cam_col1:
    if st.button("📸 Ativar câmera para leitura"):
        st.session_state.camera_ativa = True

if st.session_state.camera_ativa:
    components.html(
        """
<div style="border:1px solid #ccc; border-radius: 10px; padding:8px; margin-bottom:8px;">
  <div id="interactive" class="viewport" style="width:100%; text-align:center;">
    <video autoplay="true" preload="auto" playsinline="true" style="width:100%; border-radius:10px;"></video>
  </div>
  <p style="font-size: 13px; color: #555; margin-top:4px;">
    Aponte a câmera para o código de barras (EAN). Mantenha o código bem centralizado e com boa iluminação.
  </p>
  <p style="font-size: 13px; color: #333; margin-top:4px;">
    Último código lido: <strong id="last-code">Nenhum ainda</strong>
  </p>
</div>

<script src="https://unpkg.com/quagga/dist/quagga.min.js"></script>
<script>
(function() {
    if (window._quaggaStarted) {
        console.log("Quagga já iniciado.");
        return;
    }
    window._quaggaStarted = true;

    function setCodigoNoInput(code) {
        try {
            const inputs = window.parent.document.querySelectorAll('input');
            for (const inp of inputs) {
                const aria = inp.getAttribute('aria-label');
                if (aria === 'Código (SAP por enquanto, futuramente EAN)') {
                    inp.value = code;
                    inp.dispatchEvent(new Event('input', { bubbles: true }));
                    if (window.parent.playBeep) {
                        window.parent.playBeep();
                    }
                    break;
                }
            }
        } catch (e) {
            console.log("Erro ao setar código no input:", e);
        }
    }

    function updateLastCode(code) {
        try {
            const span = document.getElementById('last-code');
            if (span) {
                span.textContent = code || 'Nenhum ainda';
            }
        } catch (e) {
            console.log("Erro ao atualizar last-code:", e);
        }
    }

    function startScanner() {
        const target = document.querySelector('#interactive');
        if (!target) {
            console.log("Container de câmera não encontrado.");
            return;
        }

        Quagga.init({
            inputStream: {
                name: "Live",
                type: "LiveStream",
                target: target,
                constraints: {
                    facingMode: "environment",
                    width: { min: 640 },
                    height: { min: 480 },
                    aspectRatio: { min: 1, max: 2 }
                }
            },
            locator: {
                patchSize: "medium",
                halfSample: true
            },
            numOfWorkers: (navigator.hardwareConcurrency || 4),
            frequency: 10,
            decoder: {
                readers: [
                    "ean_reader",
                    "ean_8_reader"
                ]
            },
            locate: true
        }, function(err) {
            if (err) {
                console.log("Erro ao iniciar Quagga:", err);
                updateLastCode("Erro ao iniciar câmera");
                return;
            }
            Quagga.start();
            console.log("Quagga iniciado.");
            updateLastCode("Aguardando leitura...");
        });

        let lastCode = "";
        Quagga.onDetected(function(data) {
            if (!data || !data.codeResult || !data.codeResult.code) return;
            const code = data.codeResult.code;
            if (!code) return;

            console.log("Código detectado bruto:", code);

            if (code === lastCode) return;
            lastCode = code;

            updateLastCode(code);
            setCodigoNoInput(code);

            setTimeout(function() { lastCode = ""; }, 1500);
        });
    }

    if (document.readyState === "complete" || document.readyState === "interactive") {
        startScanner();
    } else {
        document.addEventListener("DOMContentLoaded", startScanner);
    }
})();
</script>
        """,
        height=380
    )

# ==========================================================
# 2.x PROCESSAMENTO DA CONTAGEM (MANUAL OU CÂMERA) + AUTO-SAVE
# ==========================================================
if confirmar and codigo_digitado.strip() != "":

    codigo_digitado_norm = codigo_digitado.strip().lstrip("0")

    mask = df_conf["codigo"] == codigo_digitado_norm
    if mask.any():
        idx = df_conf[mask].index[0]
        # Atualiza em memória
        st.session_state.df_conferencia.loc[idx, "qtd_contada"] += int(qtd_lida)
        produto = df_conf.loc[idx, "descricao"]
        qtd_prevista = int(df_conf.loc[idx, "qtd_prevista"])

        # AUTO-SAVE NO BANCO
        if conferencia_id is not None:
            registrar_contagem_db(
                conferencia_id=conferencia_id,
                codigo=codigo_digitado_norm,
                descricao=produto,
                qtd_prevista=qtd_prevista,
                qtd_add=int(qtd_lida)
            )

        st.success(f"Contagem adicionada para: {produto}")
    else:
        # Não estava na planilha → sobra
        nova_linha = pd.DataFrame([{
            "codigo_original": codigo_digitado.strip(),
            "codigo": codigo_digitado_norm,
            "descricao": "NÃO CADASTRADO NA PLANILHA",
            "qtd_prevista": 0,
            "qtd_contada": int(qtd_lida)
        }])

        st.session_state.df_conferencia = pd.concat(
            [st.session_state.df_conferencia, nova_linha],
            ignore_index=True
        )

        # AUTO-SAVE NO BANCO PARA ITEM SOBRANDO
        if conferencia_id is not None:
            registrar_contagem_db(
                conferencia_id=conferencia_id,
                codigo=codigo_digitado_norm,
                descricao="NÃO CADASTRADO NA PLANILHA",
                qtd_prevista=0,
                qtd_add=int(qtd_lida)
            )

        st.warning("Código não estava na planilha. Incluído como item SOBRANDO (qtd_prevista = 0).")

    # Atualiza referência local
    df_conf = st.session_state.df_conferencia

    # 🔊 Beep de confirmação da leitura (caso venha do leitor físico/manual)
    st.markdown("<script>window.playBeep && window.playBeep();</script>", unsafe_allow_html=True)

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
# 3.1 BARRA DE PROGRESSO DA CONFERÊNCIA
# ==========================================================
st.markdown("### 📈 Progresso da conferência")

total_previsto = int(df_parcial["qtd_prevista"].sum())
total_contado = int(df_parcial["qtd_contada"].sum())

if total_previsto > 0:
    progresso = min(total_contado / total_previsto, 1.0)
else:
    progresso = 0.0

itens_totais = len(df_parcial)
itens_com_contagem = int((df_parcial["qtd_contada"] > 0).sum())

st.progress(
    progresso,
    text=f"Progresso por quantidade: {total_contado}/{total_previsto} unidades conferidas"
)

st.caption(
    f"Itens com alguma contagem: {itens_com_contagem} de {itens_totais} itens da NF "
    f"({(itens_com_contagem / itens_totais * 100 if itens_totais else 0):.1f}%)."
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
# 6. SALVAR CONFERÊNCIA NO SUPABASE (SNAPSHOT FINAL)
# ==========================================================
st.markdown("### 💾 Salvar conferência desta viagem")

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
    # Mantido do seu código: essa função gera um SNAPSHOT completo,
    # inserindo um novo registro de conferência e todos os itens.
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

if st.button("💾 Salvar conferência desta viagem"):
    try:
        conf_id = salvar_conferencia_supabase(df_parcial, viagem, loja, data_v_str)
        st.success(f"Conferência salva no Supabase com id = {conf_id}")

        st.session_state.conferencia_salva = True

        # Atualiza JS para não alertar mais
        st.markdown("""
            <script>
            window.conferencia_salva = true;
            </script>
        """, unsafe_allow_html=True)

    except Exception as e:
        st.error(f"Erro ao salvar no Supabase: {e}")
