import streamlit as st
import json
import base64
import re
import _snowflake
from snowflake.snowpark.context import get_active_session

session = get_active_session()

API_ENDPOINT = "/api/v2/cortex/agent:run"
API_TIMEOUT = 120000

SEMANTIC_VIEW = "POWERUTILITY.PUBLIC.A2A_ENERGY_SEMANTIC_VIEW"
CORTEX_SEARCH = "POWERUTILITY.PUBLIC.A2A_CONTRATTO_SEARCH"

SYSTEM_PROMPT = """Rispondi sempre in italiano. Quando l'utente chiede un grafico o una visualizzazione, 
dopo aver ottenuto i dati dalla Semantic View, genera un grafico chiaro e professionale.
Usa titoli, etichette e legenda in italiano."""

st.set_page_config(page_title="A2A Energy Assistant", page_icon="⚡", layout="wide")

st.markdown("""
<style>
    .stApp { background-color: #FFFFFF; }
    .main .block-container { max-width: 900px; padding-top: 2rem; }
    h1 { color: #E30613 !important; font-weight: 700 !important; }
    .user-msg {
        background: linear-gradient(135deg, #E8F4FD, #F0F7FF);
        border-left: 4px solid #00B4D8;
        padding: 12px 16px; border-radius: 8px; margin: 8px 0; color: #1A1A2E;
    }
    .assistant-msg {
        background: linear-gradient(135deg, #FFF0F0, #FFF5F5);
        border-left: 4px solid #E30613;
        padding: 12px 16px; border-radius: 8px; margin: 8px 0; color: #1A1A2E;
    }
</style>
""", unsafe_allow_html=True)

if "messages" not in st.session_state:
    st.session_state.messages = []
if "pending_query" not in st.session_state:
    st.session_state.pending_query = None


def call_cortex_agent(query, history):
    messages = []
    for msg in history:
        messages.append({"role": msg["role"], "content": [{"type": "text", "text": msg["content"]}]})
    messages.append({"role": "user", "content": [{"type": "text", "text": query}]})

    payload = {
        "models": {"orchestration": "auto"},
        "messages": messages,
        "tools": [
            {"tool_spec": {"type": "cortex_analyst_text_to_sql", "name": "Analisi_Dati"}},
            {"tool_spec": {"type": "cortex_search", "name": "Ricerca_Contratti"}}
        ],
        "tool_resources": {
            "Analisi_Dati": {"semantic_view": SEMANTIC_VIEW, "execution_environment": {"type": "warehouse", "warehouse": "COMPUTE_WH"}},
            "Ricerca_Contratti": {"name": CORTEX_SEARCH, "max_results": 5, "id_column": "NOME_FILE", "title_column": "SEZIONE"}
        },
        "instructions": {"response": SYSTEM_PROMPT}
    }

    try:
        resp = _snowflake.send_snow_api_request("POST", API_ENDPOINT, {}, {"stream": True}, payload, None, API_TIMEOUT)
        if resp["status"] != 200:
            return None, f"Errore API: {resp['status']}"
        return json.loads(resp["content"]), None
    except Exception as e:
        return None, str(e)


STEP_ICONS = {
    "thinking": "🧠",
    "planning": "📋",
    "tool_call": "🔧",
    "sql_generation": "💾",
    "sql_execution": "▶️",
    "search": "🔍",
    "chart": "📊",
    "response": "💬",
    "error": "❌",
}


def process_response(response):
    text, sql, citations, chart_spec = "", "", [], ""
    steps = []
    if not response:
        return text, sql, citations, chart_spec, steps

    try:
        for event in response:
            event_type = event.get("event", "unknown")
            data = event.get("data", {})

            if event_type == "response.thinking.delta":
                thinking_text = data.get("text", "")
                if thinking_text and not any(s["type"] == "thinking" for s in steps):
                    steps.append({"type": "thinking", "label": "Ragionamento", "detail": thinking_text[:200]})

            elif event_type == "response.status":
                status = data.get("status", "")
                if status:
                    steps.append({"type": "planning", "label": status.replace("_", " ").title()})

            elif event_type == "response.tool_use":
                tool_name = data.get("name", data.get("tool_name", "strumento"))
                label_map = {
                    "Analisi_Dati": "Analisi dati (Semantic View)",
                    "Ricerca_Contratti": "Ricerca documenti (Cortex Search)",
                    "data_to_chart": "Generazione grafico",
                }
                label = label_map.get(tool_name, f"Uso strumento: {tool_name}")
                step_type = "search" if "Ricerca" in tool_name or "search" in tool_name.lower() else "tool_call"
                steps.append({"type": step_type, "label": label})

            elif event_type == "response.text.delta":
                text += data.get("text", "")
                if not any(s["type"] == "response" for s in steps):
                    steps.append({"type": "response", "label": "Generazione risposta"})

            elif event_type == "response.text":
                if not text:
                    text = data.get("text", "")

            elif event_type == "response.tool_result":
                for c in data.get("content", []):
                    if c.get("type") == "json":
                        jd = c.get("json", {})
                        if jd.get("sql"):
                            sql = jd.get("sql", "") or sql
                            steps.append({"type": "sql_generation", "label": "Query SQL generata"})
                        analyst_text = jd.get("text", "")
                        if analyst_text and analyst_text not in text:
                            text += analyst_text
                        if "charts" in jd:
                            charts = jd.get("charts", [])
                            if charts and isinstance(charts, list) and len(charts) > 0:
                                chart_spec = charts[0] if isinstance(charts[0], str) else json.dumps(charts[0])
                                steps.append({"type": "chart", "label": "Grafico generato"})

            elif event_type == "response.tool_result.analyst.delta":
                delta = data.get("delta", data)
                if delta.get("sql"):
                    sql = delta.get("sql", "") or sql
                if delta.get("text"):
                    text += delta.get("text", "")

            elif event_type == "response.chart":
                chart_spec = data.get("chart_spec", "") or chart_spec
                if not any(s["type"] == "chart" for s in steps):
                    steps.append({"type": "chart", "label": "Grafico nativo generato"})

            elif event_type == "response":
                for item in data.get("content", []):
                    itype = item.get("type")
                    if itype == "text":
                        final_text = item.get("text", "")
                        if final_text and not text:
                            text = final_text
                    elif itype == "chart":
                        ch = item.get("chart", {})
                        chart_spec = ch.get("chart_spec", "") or chart_spec
                    elif itype == "tool_result":
                        tr = item.get("tool_result", {})
                        for c in tr.get("content", []):
                            if c.get("type") == "json":
                                jd = c.get("json", {})
                                sql = jd.get("sql", "") or sql
                                if "charts" in jd:
                                    charts = jd.get("charts", [])
                                    if charts and isinstance(charts, list) and len(charts) > 0:
                                        chart_spec = charts[0] if isinstance(charts[0], str) else json.dumps(charts[0])

            elif event_type == "error":
                err_msg = data.get("message", str(data))
                text += f"\nErrore: {err_msg}"
                steps.append({"type": "error", "label": f"Errore: {err_msg[:100]}"})

            elif event_type == "message.delta":
                delta_data = data.get("delta", {})
                for item in delta_data.get("content", []):
                    ctype = item.get("type")
                    if ctype == "text":
                        text += item.get("text", "")
                    elif ctype == "tool_results":
                        tr = item.get("tool_results", {})
                        for r in tr.get("content", []):
                            if r.get("type") == "json":
                                jd = r.get("json", {})
                                sql = jd.get("sql", "") or sql
                                text += jd.get("text", "")

    except Exception as e:
        text += f"\n[Errore: {e}]"

    if sql and not any(s["type"] == "sql_execution" for s in steps):
        steps.append({"type": "sql_execution", "label": "Esecuzione query SQL"})

    return text, sql, citations, chart_spec, steps


def execute_python_chart(code, timeout=120):
    try:
        escaped = code.replace("'", "''")
        result_raw = session.sql(f"SELECT POWERUTILITY.PUBLIC.EXECUTE_PYTHON('{escaped}', {timeout})").collect()[0][0]
        return json.loads(result_raw)
    except Exception as e:
        return {"success": False, "error": str(e)}


def try_render_base64_image(output):
    stripped = output.strip()
    if len(stripped) > 500:
        try:
            img_bytes = base64.b64decode(stripped)
            if img_bytes[:4] == b'\x89PNG' or img_bytes[:2] == b'\xff\xd8':
                return img_bytes
        except Exception:
            pass
    return None


def generate_chart_from_sql(sql_query, user_request):
    try:
        df = session.sql(sql_query.replace(";", "")).to_pandas()
        if df.empty:
            return None, None
        import numpy as np
        data_json = df.head(50).to_dict(orient="records")
        safe_title = user_request[:80].replace("'", "")
        chart_code = f"""
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
import base64, io, json
sns.set_theme(style='whitegrid', font_scale=1.1)
plt.rcParams['figure.facecolor'] = '#FFFFFF'
plt.rcParams['axes.facecolor'] = '#F8F9FA'
plt.rcParams['text.color'] = '#1A1A2E'
plt.rcParams['axes.labelcolor'] = '#1A1A2E'
plt.rcParams['xtick.color'] = '#333333'
plt.rcParams['ytick.color'] = '#333333'
data = json.loads('''{json.dumps(data_json)}''')
import pandas as pd
df = pd.DataFrame(data)
numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()
text_cols = [c for c in df.columns if c not in numeric_cols]
fig, ax = plt.subplots(figsize=(12, 6))
if len(text_cols) >= 1 and len(numeric_cols) >= 2:
    x = np.arange(len(df))
    n_bars = min(len(numeric_cols), 4)
    width = 0.8 / n_bars
    colors = ['#E30613', '#00B4D8', '#00C49A', '#FFB800']
    for i, col in enumerate(numeric_cols[:n_bars]):
        ax.bar(x + i*width - (n_bars-1)*width/2, df[col], width, label=col.replace('_',' ').title(), color=colors[i], alpha=0.85)
    ax.set_xticks(x)
    ax.set_xticklabels(df[text_cols[0]].astype(str).tolist(), rotation=45, ha='right')
    ax.legend(facecolor='#FFFFFF', edgecolor='#CCCCCC', labelcolor='#1A1A2E')
elif len(text_cols) >= 1 and len(numeric_cols) == 1:
    colors = sns.color_palette('husl', n_colors=len(df))
    bars = ax.barh(df[text_cols[0]].astype(str), df[numeric_cols[0]], color=colors)
    ax.set_xlabel(numeric_cols[0].replace('_',' ').title())
    for bar in bars:
        w = bar.get_width()
        ax.text(w + w*0.01, bar.get_y() + bar.get_height()/2, f'{{w:,.0f}}', va='center', fontsize=9, color='#333333')
elif len(numeric_cols) >= 2:
    ax.plot(df[numeric_cols[0]], df[numeric_cols[1]], 'o-', color='#00B4D8', linewidth=2, markersize=6)
    ax.set_xlabel(numeric_cols[0].replace('_',' ').title())
    ax.set_ylabel(numeric_cols[1].replace('_',' ').title())
ax.set_title('{safe_title}', fontsize=14, fontweight='bold', color='#E30613', pad=15)
ax.grid(axis='y', alpha=0.3, color='#CCCCCC')
for spine in ax.spines.values():
    spine.set_color('#CCCCCC')
plt.tight_layout()
buf = io.BytesIO()
fig.savefig(buf, format='png', dpi=150, bbox_inches='tight', facecolor='#FFFFFF')
buf.seek(0)
print(base64.b64encode(buf.read()).decode('utf-8'))
plt.close(fig)
"""
        result = execute_python_chart(chart_code, 120)
        if result.get("success"):
            img = try_render_base64_image(result.get("output", ""))
            return img, df
        return None, df
    except Exception as e:
        return None, None


def process_query(prompt):
    st.session_state.messages.append({"role": "user", "content": prompt})
    history = [{"role": m["role"], "content": m["content"]} for m in st.session_state.messages[:-1]]

    status_placeholder = st.empty()
    status_placeholder.markdown("⏳ **Invio richiesta all'agente...**")

    response, error = call_cortex_agent(prompt, history)
    if error:
        status_placeholder.empty()
        st.session_state.messages.append({"role": "assistant", "content": f"Errore: {error}"})
        return

    status_placeholder.markdown("🧠 **Elaborazione risposta...**")
    text, sql, citations, chart_spec, steps = process_response(response)
    msg_data = {"role": "assistant", "content": text, "steps": steps}
    chart_img, df = None, None

    if chart_spec:
        msg_data["chart_spec"] = chart_spec

    if sql:
        msg_data["sql"] = sql
        is_chart = any(w in prompt.lower() for w in ["grafico", "chart", "visualizza", "mostra", "plot", "istogramma", "torta", "barre"])
        if is_chart:
            status_placeholder.markdown("📊 **Generazione grafico...**")
            chart_img, df = generate_chart_from_sql(sql, prompt)
        if not is_chart or (chart_img is None and not chart_spec):
            try:
                df = session.sql(sql.replace(";", "")).to_pandas()
            except Exception:
                pass

    if chart_img:
        msg_data["image"] = chart_img
    if df is not None:
        msg_data["dataframe"] = df
    clean_text = re.sub(r'!\[.*?\]\(data:image/png;base64,.*?\)', '[Grafico visualizzato sopra]', text)
    msg_data["content"] = clean_text
    status_placeholder.empty()
    st.session_state.messages.append(msg_data)


def render_steps(steps):
    if not steps:
        return
    with st.expander("Fasi dell'agente", expanded=False):
        for i, step in enumerate(steps):
            icon = STEP_ICONS.get(step["type"], "▪️")
            label = step.get("label", step["type"])
            detail = step.get("detail", "")
            st.markdown(f"{icon} **{label}**" + (f"  \n_{detail}_" if detail else ""))


def render_message(msg):
    role = msg["role"]
    css_class = "user-msg" if role == "user" else "assistant-msg"
    icon = "Tu" if role == "user" else "Assistente"
    st.markdown(f'<div class="{css_class}"><b>{icon}</b></div>', unsafe_allow_html=True)
    if msg.get("steps"):
        render_steps(msg["steps"])
    if msg.get("chart_spec"):
        try:
            spec = json.loads(msg["chart_spec"]) if isinstance(msg["chart_spec"], str) else msg["chart_spec"]
            st.vega_lite_chart(spec, use_container_width=True)
        except Exception:
            pass
        if msg.get("image"):
            img_data = msg["image"]
            download_bytes = None
            if isinstance(img_data, bytes):
                download_bytes = img_data
            elif isinstance(img_data, str):
                try:
                    download_bytes = base64.b64decode(img_data)
                except Exception:
                    pass
            if download_bytes:
                st.download_button("Scarica grafico (PNG)", data=download_bytes, file_name="grafico.png", mime="image/png")
    elif msg.get("image"):
        img_data = msg["image"]
        try:
            st.image(img_data, use_container_width=True)
        except TypeError:
            st.image(img_data, use_column_width=True)
        download_bytes = None
        if isinstance(img_data, bytes):
            download_bytes = img_data
        elif isinstance(img_data, str):
            try:
                download_bytes = base64.b64decode(img_data)
            except Exception:
                pass
        if download_bytes:
            st.download_button("Scarica grafico (PNG)", data=download_bytes, file_name="grafico.png", mime="image/png")
    if msg.get("dataframe") is not None:
        with st.expander("Dati", expanded=False):
            st.dataframe(msg["dataframe"])
    if msg.get("sql"):
        with st.expander("SQL generato", expanded=False):
            st.code(msg["sql"], language="sql")
    content = msg.get("content", "")
    if content:
        st.markdown(content)


SUGGESTIONS = {
    "Clienti per provincia": "Mostrami un grafico con la suddivisione dei clienti per tipologia e provincia",
    "Consumi per citta": "Quali sono le 5 citta con il consumo totale di energia piu alto? Mostra un grafico",
    "Costi per segmento": "Qual e il costo medio in bolletta per ogni segmento cliente? Visualizza in un grafico",
    "Produzione impianti": "Mostra la produzione totale e il margine per tipo di impianto in un grafico",
}

st.markdown("# ⚡ A2A Energy Assistant")
st.caption("Assistente AI per analisi dati energetici con grafici avanzati")

if not st.session_state.messages and not st.session_state.pending_query:
    st.markdown("**Prova a chiedere:**")
    cols = st.columns(2)
    for i, key in enumerate(list(SUGGESTIONS.keys())):
        with cols[i % 2]:
            if st.button(key, key=f"sug_{i}", use_container_width=True):
                st.session_state.pending_query = SUGGESTIONS[key]

if st.session_state.pending_query:
    query = st.session_state.pending_query
    st.session_state.pending_query = None
    process_query(query)

for msg in st.session_state.messages:
    render_message(msg)

st.markdown("---")
with st.form("chat_form", clear_on_submit=True):
    user_input = st.text_input("Chiedi qualcosa sui dati energetici A2A...", key="user_input", label_visibility="collapsed")
    submitted = st.form_submit_button("Invia")

if submitted and user_input and user_input.strip():
    process_query(user_input.strip())
    for msg in st.session_state.messages[-2:]:
        render_message(msg)

with st.sidebar:
    st.markdown("### ⚡ A2A Energia")
    st.caption("Assistente AI con grafici avanzati")
    st.markdown("---")
    if st.button("Nuova conversazione", use_container_width=True):
        st.session_state.messages = []
        st.session_state.pending_query = None
    st.markdown("---")
    st.markdown("**Strumenti disponibili:**")
    st.markdown("- Semantic View (dati)")
    st.markdown("- Cortex Search (documenti)")
    st.markdown("- Grafici automatici")
