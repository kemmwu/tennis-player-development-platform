# coach_app.py
# Streamlit coach dashboard
# Select a student → view development score + trend + training history
# + LLM-generated next session recommendation

import streamlit as st
import pandas as pd
import requests
from databricks import sql as dbsql

st.set_page_config(
    page_title="Tennis Coach Dashboard",
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
    return pd.DataFrame(cursor.fetchall(), columns=cols)


# ── SIDEBAR ───────────────────────────────────────────────────────
st.sidebar.title("🎾 Tennis Coach")
st.sidebar.markdown("---")

@st.cache_data(ttl=60)
def load_players() -> list:
    df = run_query("""
        SELECT DISTINCT player_id
        FROM tennis_dev.silver_gold.fct_match_performance
        ORDER BY player_id
    """)
    return df["player_id"].tolist()

players     = load_players()
selected    = st.sidebar.selectbox("Select student", players)

st.sidebar.markdown("---")
st.sidebar.markdown("**Quick Links**")
st.sidebar.markdown("[📊 Parent Dashboard](#)")
st.sidebar.markdown("[🔍 HITL Review](./hitl_review)")
st.sidebar.markdown(
    "[💬 Ask Genie]"
    "(https://dbc-66b56d97-276e.cloud.databricks.com/genie)"
)

# ── MAIN ──────────────────────────────────────────────────────────
st.title(f"🎾 {selected.title()}'s Development Dashboard")

# ── LOAD DATA ─────────────────────────────────────────────────────
@st.cache_data(ttl=60)
def load_player_data(player_id: str) -> dict:
    matches = run_query(f"""
        SELECT
            match_date,
            score,
            player_won,
            first_serve_pct,
            second_serve_pct,
            break_point_conversion,
            winners,
            unforced_errors,
            return_points_won_pct,
            rallies_9plus_won_pct,
            has_note,
            extraction_confidence
        FROM tennis_dev.silver_gold.fct_match_performance
        WHERE player_id = '{player_id}'
        ORDER BY match_date DESC
        LIMIT 20
    """)

    training = run_query(f"""
        SELECT
            session_date,
            sessions_on_day,
            avg_shots_in,
            avg_forehand_cross_court_in,
            avg_backhand_cross_court_in,
            avg_serve_speed_ad,
            avg_serve_speed_deuce,
            max_longest_rally,
            avg_rallies_above_5_shots
        FROM tennis_dev.silver_gold.fct_training_sessions
        WHERE player_id = '{player_id}'
        ORDER BY session_date DESC
        LIMIT 10
    """)

    scores = run_query(f"""
        SELECT
            score_week,
            development_score,
            win_rate_score,
            opponent_strength_score,
            technique_score,
            break_point_score,
            matches_played,
            matches_won,
            sessions_completed,
            score_trend
        FROM tennis_dev.silver_gold.mart_coach_weekly_digest
        WHERE player_id = '{player_id}'
        ORDER BY week_start DESC
        LIMIT 12
    """)

    notes = run_query(f"""
        SELECT
            match_id,
            match_date,
            raw_note_text
        FROM tennis_dev.silver_gold.fct_match_performance
        WHERE player_id = '{player_id}'
          AND has_note = true
        ORDER BY match_date DESC
        LIMIT 5
    """)

    return {
        "matches":  matches,
        "training": training,
        "scores":   scores,
        "notes":    notes
    }


data = load_player_data(selected)

# ── TOP METRICS ───────────────────────────────────────────────────
scores_df  = data["scores"]
matches_df = data["matches"]

if not scores_df.empty:
    latest = scores_df.iloc[0]
    trend  = latest.get("score_trend", "stable")
    trend_icon = "📈" if trend == "improving" \
        else "📉" if trend == "declining" else "➡️"

    m1, m2, m3, m4 = st.columns(4)
    m1.metric(
        "Development Score",
        f"{latest['development_score']:.1f}",
        f"{trend_icon} {trend}"
    )
    m2.metric(
        "Matches This Week",
        int(latest["matches_played"]) if pd.notna(latest["matches_played"]) else 0
    )
    m3.metric(
        "Sessions This Week",
        int(latest["sessions_completed"]) if pd.notna(latest["sessions_completed"]) else 0
    )

    if not matches_df.empty:
        wins  = matches_df["player_won"].sum()
        total = len(matches_df)
        m4.metric("Recent Win Rate", f"{wins/total*100:.0f}%", f"{wins}/{total}")

st.markdown("---")

# ── DEVELOPMENT SCORE TREND ───────────────────────────────────────
if not scores_df.empty:
    st.subheader("📈 Development Score Trend")
    st.line_chart(
        scores_df.set_index("score_week")[["development_score"]]
    )

    with st.expander("Score components breakdown"):
        st.dataframe(
            scores_df[[
                "score_week", "development_score",
                "win_rate_score", "opponent_strength_score",
                "technique_score", "break_point_score"
            ]],
            use_container_width=True
        )

st.markdown("---")

# ── RECENT MATCHES ────────────────────────────────────────────────
st.subheader("🎾 Recent Matches")

if not matches_df.empty:
    display_df = matches_df.copy()
    display_df["result"]          = display_df["player_won"].map(
        {True: "✅ Win", False: "❌ Loss", None: "—"}
    )
    display_df["first_serve_%"]   = (
        display_df["first_serve_pct"] * 100
    ).round(1).astype(str) + "%"
    display_df["bp_conversion"]   = (
        display_df["break_point_conversion"] * 100
    ).round(1).astype(str) + "%"

    st.dataframe(
        display_df[[
            "match_date", "result", "score",
            "first_serve_%", "bp_conversion",
            "winners", "unforced_errors"
        ]],
        use_container_width=True
    )
else:
    st.info("No match data available yet.")

st.markdown("---")

# ── TRAINING HISTORY ──────────────────────────────────────────────
st.subheader("🏋️ Recent Training Sessions")

training_df = data["training"]
if not training_df.empty:
    display_train = training_df.copy()
    display_train["shots_in_%"] = (
        display_train["avg_shots_in"] * 100
    ).round(1).astype(str) + "%"
    display_train["fh_accuracy"] = (
        display_train["avg_forehand_cross_court_in"] * 100
    ).round(1).astype(str) + "%"
    display_train["bh_accuracy"] = (
        display_train["avg_backhand_cross_court_in"] * 100
    ).round(1).astype(str) + "%"

    st.dataframe(
        display_train[[
            "session_date", "sessions_on_day",
            "shots_in_%", "fh_accuracy", "bh_accuracy",
            "max_longest_rally"
        ]],
        use_container_width=True
    )
else:
    st.info("No training data available yet.")

st.markdown("---")

# ── COACHING NOTES ────────────────────────────────────────────────
st.subheader("📝 Recent Coaching Notes")

notes_df = data["notes"]
if not notes_df.empty:
    for _, row in notes_df.iterrows():
        with st.expander(f"{row['match_date']} — {row['match_id']}"):
            st.markdown(row["raw_note_text"])
else:
    st.info("No coaching notes available yet.")

st.markdown("---")

# ── LLM NEXT SESSION RECOMMENDATION ──────────────────────────────
st.subheader("🤖 Next Session Recommendation")
st.markdown(
    "AI-generated recommendation based on recent performance data."
)

if st.button("Generate recommendation", type="primary"):
    with st.spinner("Analysing recent performance..."):
        try:
            # Build context from recent data
            match_summary = ""
            if not matches_df.empty:
                recent = matches_df.head(3)
                wins   = recent["player_won"].sum()
                match_summary = (
                    f"Last 3 matches: {wins} wins, "
                    f"{3-wins} losses. "
                    f"Avg first serve: "
                    f"{recent['first_serve_pct'].mean()*100:.0f}%. "
                    f"Avg winners: {recent['winners'].mean():.0f}. "
                    f"Avg unforced errors: "
                    f"{recent['unforced_errors'].mean():.0f}."
                )

            training_summary = ""
            if not training_df.empty:
                recent_t = training_df.head(2)
                training_summary = (
                    f"Last 2 sessions: "
                    f"avg shot accuracy "
                    f"{recent_t['avg_shots_in'].mean()*100:.0f}%. "
                    f"Forehand cross-court: "
                    f"{recent_t['avg_forehand_cross_court_in'].mean()*100:.0f}%. "
                    f"Backhand cross-court: "
                    f"{recent_t['avg_backhand_cross_court_in'].mean()*100:.0f}%."
                )

            notes_summary = ""
            if not notes_df.empty:
                notes_summary = (
                    "Recent coaching notes: "
                    + " | ".join(
                        notes_df["raw_note_text"].head(2).tolist()
                    )
                )

            prompt = f"""You are an expert tennis coach assistant.
Based on the following recent performance data for player {selected},
generate a specific, actionable next training session recommendation.

{match_summary}
{training_summary}
{notes_summary}

Provide:
1. One key focus area for the next session (1 sentence)
2. Two specific drills to run (2-3 sentences each)
3. One mental/tactical point to work on (1 sentence)

Keep it practical and specific to the data shown."""

            response = requests.post(
                "https://api.anthropic.com/v1/messages",
                headers={
                    "x-api-key":         st.secrets["ANTHROPIC_API_KEY"],
                    "anthropic-version": "2023-06-01",
                    "content-type":      "application/json"
                },
                json={
                    "model":    "claude-sonnet-4-6",
                    "max_tokens": 500,
                    "messages": [
                        {"role": "user", "content": prompt}
                    ]
                },
                timeout=30
            )
            response.raise_for_status()
            recommendation = response.json()["content"][0]["text"]
            st.markdown(recommendation)

        except Exception as e:
            st.error(f"Error generating recommendation: {e}")
