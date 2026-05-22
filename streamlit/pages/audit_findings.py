# pages/audit_findings.py
# Shows LLM quality auditor findings for coach review

import streamlit as st
import pandas as pd
from databricks import sql as dbsql

st.set_page_config(
    page_title="Tennis Platform — Audit Findings",
    page_icon="🎾",
    layout="wide"
)

st.title("🔍 Quality Audit Findings")
st.markdown(
    "Semantic inconsistencies detected by the LLM quality auditor. "
    "Review and confirm or dismiss each finding."
)


@st.cache_resource
def get_connection():
    return dbsql.connect(
        server_hostname = st.secrets["DATABRICKS_HOST"],
        http_path       = st.secrets["DATABRICKS_HTTP_PATH"],
        access_token    = st.secrets["DATABRICKS_TOKEN"]
    )


def run_query(sql: str) -> pd.DataFrame:
    conn   = get_connection()
    cursor = conn.cursor()
    cursor.execute(sql)
    cols = [d[0] for d in cursor.description]
    return pd.DataFrame(cursor.fetchall(), columns=cols)


def run_update(sql: str):
    conn   = get_connection()
    cursor = conn.cursor()
    cursor.execute(sql)


# ── LOAD FINDINGS ─────────────────────────────────────────────────
@st.cache_data(ttl=30)
def load_findings(status_filter: str) -> pd.DataFrame:
    where = "" if status_filter == "All" else f"WHERE coach_status = '{status_filter}'"
    try:
        return run_query(f"""
            SELECT finding_id, session_id, session_type, player_id,
                   session_date, inconsistency_type, severity,
                   description, note_excerpt, stat_evidence,
                   coach_status, audited_at
            FROM tennis_dev.gold.llm_quality_findings
            {where}
            ORDER BY
                CASE severity WHEN 'high' THEN 1
                              WHEN 'medium' THEN 2
                              ELSE 3 END,
                audited_at DESC
        """)
    except Exception as e:
        st.error(f"Error loading findings: {e}")
        return pd.DataFrame()


# ── FILTERS ───────────────────────────────────────────────────────
col1, col2 = st.columns(2)
with col1:
    status_filter = st.selectbox(
        "Status", ["pending", "confirmed", "dismissed", "All"]
    )
with col2:
    severity_filter = st.selectbox(
        "Severity", ["All", "high", "medium", "low"]
    )

findings_df = load_findings(status_filter)

if severity_filter != "All":
    findings_df = findings_df[findings_df["severity"] == severity_filter]

# ── SUMMARY METRICS ───────────────────────────────────────────────
m1, m2, m3, m4 = st.columns(4)
m1.metric("Total Findings",   len(findings_df))
m2.metric("High Severity",
          len(findings_df[findings_df["severity"] == "high"]) if not findings_df.empty else 0)
m3.metric("Pending Review",
          len(findings_df[findings_df["coach_status"] == "pending"]) if not findings_df.empty else 0)
m4.metric("Confirmed Issues",
          len(findings_df[findings_df["coach_status"] == "confirmed"]) if not findings_df.empty else 0)

st.markdown("---")

# ── FINDINGS LIST ─────────────────────────────────────────────────
if findings_df.empty:
    st.info("No findings match the current filter.")
    st.stop()

for _, row in findings_df.iterrows():
    severity_icon = "🔴" if row["severity"] == "high" \
        else "🟡" if row["severity"] == "medium" else "🟢"

    with st.expander(
        f"{severity_icon} {row['session_id']} — "
        f"{row['inconsistency_type']} ({row['coach_status']})"
    ):
        col_a, col_b = st.columns(2)
        with col_a:
            st.markdown(f"**Player:** {row['player_id']}")
            st.markdown(f"**Date:** {row['session_date']}")
            st.markdown(f"**Type:** {row['session_type']}")
        with col_b:
            st.markdown(f"**Severity:** {severity_icon} {row['severity']}")
            st.markdown(f"**Status:** {row['coach_status']}")

        st.markdown(f"**Finding:** {row['description']}")
        st.markdown(f"**Note says:** *\"{row['note_excerpt']}\"*")
        st.markdown(f"**Data shows:** {row['stat_evidence']}")

        btn_col1, btn_col2 = st.columns(2)
        with btn_col1:
            if st.button("✅ Confirm issue", key=f"confirm_{row['finding_id']}"):
                run_update(f"""
                    UPDATE tennis_dev.gold.llm_quality_findings
                    SET coach_status = 'confirmed'
                    WHERE finding_id = '{row['finding_id']}'
                """)
                st.cache_data.clear()
                st.rerun()
        with btn_col2:
            if st.button("❌ Dismiss", key=f"dismiss_{row['finding_id']}"):
                run_update(f"""
                    UPDATE tennis_dev.gold.llm_quality_findings
                    SET coach_status = 'dismissed'
                    WHERE finding_id = '{row['finding_id']}'
                """)
                st.cache_data.clear()
                st.rerun()
                