# hitl_review.py
# Human-in-the-Loop review app for low-confidence AI extractions
# Runs on Streamlit Community Cloud
# Connects to Databricks via databricks-sdk

import streamlit as st
import pandas as pd
from databricks import sql as dbsql
import os

# Add to the sidebar section
st.sidebar.markdown("---")
st.sidebar.markdown("**Quick Links**")
st.sidebar.markdown("[🔍 Audit Findings](./audit_findings)")
st.sidebar.markdown("[💬 Ask Genie](https://dbc-66b56d97-276e.cloud.databricks.com/genie)")

# ── PAGE CONFIG ───────────────────────────────────────────────────
st.set_page_config(
    page_title="Tennis Platform — HITL Review",
    page_icon="🎾",
    layout="wide"
)

# ── CONNECTION ────────────────────────────────────────────────────
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
    rows = cursor.fetchall()
    return pd.DataFrame(rows, columns=cols)


def run_update(sql: str):
    conn   = get_connection()
    cursor = conn.cursor()
    cursor.execute(sql)


# ── SIDEBAR ───────────────────────────────────────────────────────
st.sidebar.title("🎾 Tennis Platform")
st.sidebar.markdown("**HITL Review Dashboard**")

confidence_threshold = st.sidebar.slider(
    "Confidence threshold",
    min_value=0.0,
    max_value=1.0,
    value=0.8,
    step=0.05,
    help="Show extractions below this confidence score"
)

session_type_filter = st.sidebar.selectbox(
    "Session type",
    options=["All", "match", "training"]
)

st.sidebar.markdown("---")
st.sidebar.markdown("**Stats**")

# ── MAIN ──────────────────────────────────────────────────────────
st.title("🎾 Extraction Review")
st.markdown(
    "Review low-confidence AI extractions. "
    "Correct any errors and approve to save to the eval set."
)

# ── LOAD LOW CONFIDENCE RECORDS ───────────────────────────────────
@st.cache_data(ttl=30)
def load_pending_reviews(threshold: float, stype: str) -> pd.DataFrame:
    type_filter = "" if stype == "All" else f"AND session_type = '{stype}'"

    match_query = f"""
        SELECT
            match_id        AS session_id,
            player_id,
            match_date      AS session_date,
            session_time,
            'match'         AS session_type,
            winners,
            unforced_errors,
            first_serves_in,
            first_serves_total,
            break_points_won,
            break_points_total,
            raw_note_text,
            extraction_confidence,
            source_file
        FROM tennis_dev.bronze.raw_match_extractions
        WHERE extraction_confidence < {threshold}
        AND match_id NOT IN (
            SELECT session_id FROM tennis_dev.gold.extraction_eval_set
        )
        {type_filter if stype == "match" or stype == "All" else "AND 1=0"}
    """

    training_query = f"""
        SELECT
            session_id,
            player_id,
            session_date,
            session_time,
            'training'      AS session_type,
            NULL            AS winners,
            NULL            AS unforced_errors,
            NULL            AS first_serves_in,
            NULL            AS first_serves_total,
            NULL            AS break_points_won,
            NULL            AS break_points_total,
            raw_note_text,
            extraction_confidence,
            source_file
        FROM tennis_dev.bronze.raw_training_sessions
        WHERE extraction_confidence < {threshold}
        AND session_id NOT IN (
            SELECT session_id FROM tennis_dev.gold.extraction_eval_set
        )
        {type_filter if stype == "training" or stype == "All" else "AND 1=0"}
    """

    try:
        match_df    = run_query(match_query)
        training_df = run_query(training_query)
        combined    = pd.concat([match_df, training_df], ignore_index=True)
        return combined.sort_values("extraction_confidence")
    except Exception as e:
        st.error(f"Error loading data: {e}")
        return pd.DataFrame()


@st.cache_data(ttl=30)
def load_eval_set_count() -> int:
    try:
        df = run_query(
            "SELECT COUNT(*) as cnt FROM tennis_dev.gold.extraction_eval_set"
        )
        return int(df["cnt"].iloc[0])
    except Exception:
        return 0


# ── STATS ROW ─────────────────────────────────────────────────────
col1, col2, col3 = st.columns(3)

pending_df  = load_pending_reviews(confidence_threshold, session_type_filter)
eval_count  = load_eval_set_count()

col1.metric("Pending Reviews",  len(pending_df))
col2.metric("Eval Set Size",    eval_count)
col3.metric("Confidence Threshold", f"{confidence_threshold:.0%}")

st.markdown("---")

# ── REVIEW INTERFACE ──────────────────────────────────────────────
if pending_df.empty:
    st.success(
        f"✅ No extractions below {confidence_threshold:.0%} confidence. "
        "All good!"
    )
    st.stop()

st.subheader(f"Pending Reviews ({len(pending_df)})")

# Session selector
selected_session = st.selectbox(
    "Select session to review",
    options=pending_df["session_id"].tolist(),
    format_func=lambda x: (
        f"{x}  —  confidence: "
        f"{pending_df[pending_df['session_id']==x]['extraction_confidence'].values[0]:.2f}"
    )
)

if selected_session:
    row = pending_df[pending_df["session_id"] == selected_session].iloc[0]

    st.markdown(f"### Reviewing: `{selected_session}`")

    info_col, conf_col = st.columns([3, 1])
    with info_col:
        st.markdown(f"**Player:** {row['player_id']}")
        st.markdown(f"**Date:** {row['session_date']}")
        st.markdown(f"**Type:** {row['session_type']}")
    with conf_col:
        conf = float(row["extraction_confidence"])
        color = "🔴" if conf < 0.6 else "🟡" if conf < 0.8 else "🟢"
        st.markdown(f"**Confidence:** {color} {conf:.2f}")

    st.markdown("---")

    # ── MATCH FIELDS ──────────────────────────────────────────────
    if row["session_type"] == "match":
        st.subheader("Match Statistics")

        m_col1, m_col2, m_col3 = st.columns(3)
        with m_col1:
            winners = st.number_input(
                "Winners",
                value=int(row["winners"]) if pd.notna(row["winners"]) else 0,
                min_value=0
            )
            unforced = st.number_input(
                "Unforced Errors",
                value=int(row["unforced_errors"]) if pd.notna(row["unforced_errors"]) else 0,
                min_value=0
            )
        with m_col2:
            serves_in = st.number_input(
                "1st Serves In",
                value=int(row["first_serves_in"]) if pd.notna(row["first_serves_in"]) else 0,
                min_value=0
            )
            serves_total = st.number_input(
                "1st Serves Total",
                value=int(row["first_serves_total"]) if pd.notna(row["first_serves_total"]) else 0,
                min_value=0
            )
        with m_col3:
            bp_won = st.number_input(
                "Break Points Won",
                value=int(row["break_points_won"]) if pd.notna(row["break_points_won"]) else 0,
                min_value=0
            )
            bp_total = st.number_input(
                "Break Points Total",
                value=int(row["break_points_total"]) if pd.notna(row["break_points_total"]) else 0,
                min_value=0
            )

    # ── COACHING NOTE ──────────────────────────────────────────────
    st.subheader("Coaching Note")
    note_text = st.text_area(
        "Raw note text (edit if extraction was wrong)",
        value=str(row["raw_note_text"]) if pd.notna(row["raw_note_text"]) else "",
        height=150
    )

    technique = st.text_input("Technique", value="")
    issue     = st.text_input("Issue (leave blank if none)", value="")
    rec       = st.text_input("Recommendation (leave blank if none)", value="")
    sentiment = st.selectbox(
        "Sentiment",
        options=["improving", "needs_work", "neutral"],
        index=2
    )

    st.markdown("---")

    # ── APPROVE BUTTON ────────────────────────────────────────────
    approve_col, skip_col = st.columns([1, 4])

    with approve_col:
        if st.button("✅ Approve & Save to Eval Set", type="primary"):
            try:
                # Escape single quotes
                safe_note      = note_text.replace("'", "''")
                safe_technique = technique.replace("'", "''")
                safe_issue     = issue.replace("'", "''") if issue else "NULL"
                safe_rec       = rec.replace("'", "''") if rec else "NULL"

                issue_val = f"'{safe_issue}'" if issue else "NULL"
                rec_val   = f"'{safe_rec}'"   if rec   else "NULL"

                insert_sql = f"""
                    INSERT INTO tennis_dev.gold.extraction_eval_set
                    VALUES (
                        '{selected_session}',
                        '{row["session_type"]}',
                        '{row["player_id"]}',
                        '{row["session_date"]}',
                        '{safe_note}',
                        '{safe_technique}',
                        {issue_val},
                        {rec_val},
                        '{sentiment}',
                        current_timestamp()
                    )
                """
                run_update(insert_sql)

                st.success(f"✅ Saved to eval set!")
                st.cache_data.clear()
                st.rerun()

            except Exception as e:
                st.error(f"Error saving: {e}")

    with skip_col:
        if st.button("⏭ Skip for now"):
            st.info("Skipped. Select another session above.")