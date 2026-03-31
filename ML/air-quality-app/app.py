import streamlit as st
import pandas as pd
import requests
import json
import streamlit.components.v1 as components
from datetime import datetime, date

# -------------------------------------------------------
# Page Config
# -------------------------------------------------------
st.set_page_config(
    page_title="Air Quality PM2.5 Predictor",
    page_icon="🌫️",
    layout="wide"
)

# -------------------------------------------------------
# Custom CSS
# -------------------------------------------------------
st.markdown("""
    <style>
    @import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;600&family=IBM+Plex+Sans:wght@300;400;600&display=swap');

    html, body, [class*="css"] {
        font-family: 'IBM Plex Sans', sans-serif;
    }
    .main-header {
        font-family: 'IBM Plex Mono', monospace;
        font-size: 2.2rem;
        font-weight: 600;
        color: #0f4c81;
        text-align: center;
        letter-spacing: -0.5px;
        margin-bottom: 0.3rem;
    }
    .sub-header {
        font-size: 1rem;
        color: #6b7280;
        text-align: center;
        margin-bottom: 2rem;
        font-weight: 300;
    }
    .prediction-box {
        padding: 1.5rem 2rem;
        border-radius: 8px;
        text-align: center;
        font-family: 'IBM Plex Mono', monospace;
        font-size: 1.3rem;
        font-weight: 600;
        margin: 1.5rem 0;
    }
    .risk-high   { background:#fff1f2; color:#be123c; border:2px solid #be123c; }
    .risk-medium { background:#fffbeb; color:#b45309; border:2px solid #b45309; }
    .risk-low    { background:#f0fdf4; color:#15803d; border:2px solid #15803d; }
    .stButton > button {
        width: 100%;
        background-color: #0f4c81;
        color: white;
        font-family: 'IBM Plex Mono', monospace;
        font-size: 1rem;
        padding: 0.8rem;
        border-radius: 6px;
        border: none;
        letter-spacing: 0.5px;
    }
    .stButton > button:hover { background-color: #1e3a5f; }
    .footer-note {
        text-align: center;
        color: #9ca3af;
        font-size: 0.8rem;
        padding: 1rem;
    }
    </style>
""", unsafe_allow_html=True)

# -------------------------------------------------------
# Config
# -------------------------------------------------------
ENDPOINT_URL     = "https://dbc-0a95f537-e7bd.cloud.databricks.com/serving-endpoints/air_quality_xgb/invocations"
DASHBOARD_URL    = "https://dbc-0a95f537-e7bd.cloud.databricks.com/embed/dashboardsv3/01f11b6cc1181328ab529289a2c28104?o=3217507003837667"
DATABRICKS_TOKEN = st.secrets.get("DATABRICKS_TOKEN", "")
MODEL_VERSION    = "champion"

def aqi_category(pm25):
    if pm25 <= 12:    return "🟢 Good",                    "#15803d"
    if pm25 <= 35.4:  return "🟡 Moderate",                 "#b45309"
    if pm25 <= 55.4:  return "🟠 Unhealthy (Sensitive)",    "#c2410c"
    if pm25 <= 150.4: return "🔴 Unhealthy",                "#be123c"
    if pm25 <= 250.4: return "🟣 Very Unhealthy",           "#7c3aed"
    return                   "⚫ Hazardous",                "#1f2937"

# -------------------------------------------------------
# Prediction function
# -------------------------------------------------------
def predict_pm25(input_df):
    if not DATABRICKS_TOKEN:
        st.error("⚠️ DATABRICKS_TOKEN not found in app secrets.")
        return None

    headers = {
        "Authorization": f"Bearer {DATABRICKS_TOKEN}",
        "Content-Type": "application/json"
    }
    payload = {"dataframe_split": input_df.to_dict(orient="split")}

    try:
        response = requests.post(
            ENDPOINT_URL,
            headers=headers,
            data=json.dumps(payload),
            timeout=30
        )
        if response.status_code != 200:
            st.error(f"Endpoint error {response.status_code}: {response.text}")
            return None

        result      = response.json()
        predictions = result.get("predictions", [])
        if not predictions:
            st.error("No predictions returned from endpoint.")
            return None

        pred_value = predictions[0]
        pm25_pred  = float(pred_value) if not isinstance(pred_value, list) else float(pred_value[0])
        return {"pm25_predicted": pm25_pred, "raw_response": result}

    except requests.exceptions.Timeout:
        st.error("⏱️ Request timed out.")
    except requests.exceptions.RequestException as e:
        st.error(f"❌ Request failed: {e}")
    except Exception as e:
        st.error(f"❌ Unexpected error: {e}")
        st.exception(e)
    return None

# -------------------------------------------------------
# Header
# -------------------------------------------------------
st.markdown('<p class="main-header">🌫️ AIR QUALITY PM2.5 PREDICTOR</p>', unsafe_allow_html=True)
st.markdown('<p class="sub-header">XGBoost-powered PM2.5 forecasting · Databricks Model Serving</p>', unsafe_allow_html=True)

# -------------------------------------------------------
# Sidebar
# -------------------------------------------------------
with st.sidebar:
    st.markdown("### ℹ️ About")
    st.info("Predicts PM2.5 concentration (µg/m³) using temporal signals, location context, and historical air quality statistics.")

    st.markdown("---")
    st.markdown("### 🔧 Model")
    st.text(f"Endpoint : air_quality_xgb")
    st.text(f"Version  : {MODEL_VERSION}")
    st.success("✅ Token loaded" if DATABRICKS_TOKEN else "❌ Token missing")

    st.markdown("---")
    st.markdown("### 📊 PM2.5 AQI Scale")
    st.markdown("""
| µg/m³ | Category |
|---|---|
| 0 – 12 | 🟢 Good |
| 12 – 35.4 | 🟡 Moderate |
| 35.4 – 55.4 | 🟠 Unhealthy* |
| 55.4 – 150.4 | 🔴 Unhealthy |
| 150.4 – 250.4 | 🟣 Very Unhealthy |
| 250.4+ | ⚫ Hazardous |
    """)

# -------------------------------------------------------
# Tabs
# -------------------------------------------------------
tab1, tab2 = st.tabs(["🔮 Predict PM2.5", "📊 Dashboard"])

# ===========================================================
# TAB 1 — Predictor
# ===========================================================
with tab1:

    # ── Section 1: Date ──────────────────────────────────
    st.markdown("#### 📅 Forecast Date")
    selected_date = st.date_input("Date", value=date.today(), label_visibility="collapsed")

    # Auto-derive all temporal features
    year        = float(selected_date.year)
    month       = float(selected_date.month)
    day_of_week = float(selected_date.weekday())          # 0=Mon … 6=Sun
    quarter     = float((selected_date.month - 1) // 3 + 1)
    is_weekend  = float(1 if selected_date.weekday() >= 5 else 0)
    is_winter   = float(1 if selected_date.month in [12, 1, 2]  else 0)
    is_spring   = float(1 if selected_date.month in [3, 4, 5]   else 0)
    is_summer   = float(1 if selected_date.month in [6, 7, 8]   else 0)
    is_fall     = float(1 if selected_date.month in [9, 10, 11] else 0)

    season = "Winter" if is_winter else "Spring" if is_spring else "Summer" if is_summer else "Fall"
    st.caption(
        f"Auto-derived → year: {int(year)}  |  month: {int(month)}  |  "
        f"Q{int(quarter)}  |  day_of_week: {int(day_of_week)}  |  "
        f"weekend: {'Yes' if is_weekend else 'No'}  |  season: {season}"
    )

    st.markdown("---")

    # ── Section 2: Location & Infrastructure ─────────────
    st.markdown("#### 📍 Location & Monitoring Infrastructure")
    lcol1, lcol2, lcol3, lcol4 = st.columns(4)

    with lcol1:
        lat = st.number_input("Latitude",  min_value=-90.0,  max_value=90.0,   value=6.5244,  step=0.0001, format="%.4f")
    with lcol2:
        lon = st.number_input("Longitude", min_value=-180.0, max_value=180.0,  value=3.3792,  step=0.0001, format="%.4f")
    with lcol3:
        population = st.number_input("Population", min_value=0, max_value=50_000_000, value=15_000_000, step=100_000)
    with lcol4:
        n_stations = st.number_input("Monitoring Stations", min_value=1, max_value=500, value=5)

    # Auto-derive station density features
    population_per_station   = float(population) / float(n_stations)
    monitoring_density_score = float(n_stations) / (float(population) / 1_000_000) if population > 0 else 0.0

    st.caption(
        f"Auto-derived → population_per_station: {population_per_station:,.0f}  |  "
        f"monitoring_density_score: {monitoring_density_score:.4f}"
    )

    st.markdown("---")

    # ── Section 3: PM2.5 Historical Stats ────────────────
    st.markdown("#### 🌫️ PM2.5 Historical Statistics")
    hcol1, hcol2, hcol3 = st.columns(3)

    with hcol1:
        st.markdown("**Lag Values**")
        pm25_lag_1d  = st.number_input("Lag 1 Day (µg/m³)",   min_value=0.0, max_value=500.0, value=20.0, step=0.5)
        pm25_lag_7d  = st.number_input("Lag 7 Days (µg/m³)",  min_value=0.0, max_value=500.0, value=19.5, step=0.5)
        pm25_lag_14d = st.number_input("Lag 14 Days (µg/m³)", min_value=0.0, max_value=500.0, value=18.0, step=0.5)
        pm25_lag_30d = st.number_input("Lag 30 Days (µg/m³)", min_value=0.0, max_value=500.0, value=17.0, step=0.5)

    with hcol2:
        st.markdown("**Rolling Averages**")
        pm25_7d_avg        = st.number_input("7-Day Avg (µg/m³)",   min_value=0.0, max_value=500.0, value=20.0, step=0.5)
        pm25_30d_avg       = st.number_input("30-Day Avg (µg/m³)",  min_value=0.0, max_value=500.0, value=18.5, step=0.5)
        pm25_90d_avg       = st.number_input("90-Day Avg (µg/m³)",  min_value=0.0, max_value=500.0, value=17.0, step=0.5)
        pm25_7d_volatility = st.number_input(
            "7-Day Volatility (std)", min_value=0.0, max_value=100.0, value=2.5, step=0.1,
            help="Std deviation of PM2.5 over last 7 days")

    with hcol3:
        st.markdown("**Range & Trend**")
        pm25_30d_min      = st.number_input("30-Day Min (µg/m³)", min_value=0.0, max_value=500.0, value=10.0, step=0.5)
        pm25_30d_max      = st.number_input("30-Day Max (µg/m³)", min_value=0.0, max_value=500.0, value=35.0, step=0.5)
        pm25_trend        = st.number_input(
            "Trend (µg/m³/day)", min_value=-20.0, max_value=20.0, value=0.1, step=0.01,
            help="Positive = worsening, Negative = improving")
        pm25_recent_ratio = st.number_input(
            "Recent Ratio (7d / 30d avg)", min_value=0.0, max_value=10.0, value=1.08, step=0.01,
            help="Values >1 indicate recent worsening relative to the month average")

    # Auto-derive range
    pm25_30d_range = pm25_30d_max - pm25_30d_min
    st.caption(f"Auto-derived → pm25_30d_range: {pm25_30d_range:.1f} µg/m³")

    st.markdown("---")

    # ── Section 4: WHO Exceedance + Optional Pollutants ──
    st.markdown("#### 🚨 WHO Exceedance Flags & Optional Pollutants")
    fcol1, fcol2, fcol3, fcol4 = st.columns(4)

    with fcol1:
        was_exceeding_who_7d = float(st.selectbox(
            "Exceeding WHO last 7 days?", options=[0, 1],
            format_func=lambda x: "Yes" if x else "No",
            help="PM2.5 above WHO 24h guideline (15 µg/m³) in last 7 days"))
    with fcol2:
        was_exceeding_who_30d = float(st.selectbox(
            "Exceeding WHO last 30 days?", options=[0, 1],
            format_func=lambda x: "Yes" if x else "No",
            help="PM2.5 above WHO annual guideline (5 µg/m³) in last 30 days"))
    with fcol3:
        pm10_30d_avg = st.number_input(
            "PM10 30-Day Avg — optional (µg/m³)", min_value=0.0, max_value=1000.0, value=35.0, step=1.0)
    with fcol4:
        no2_30d_avg = st.number_input(
            "NO₂ 30-Day Avg — optional (µg/m³)", min_value=0.0, max_value=300.0,  value=25.0, step=1.0)

    st.markdown("---")

    # ── Predict ──────────────────────────────────────────
    if st.button("🔮 Predict PM2.5 Concentration", type="primary"):
        with st.spinner("Running model inference..."):

            input_data = pd.DataFrame({
                # Temporal (all auto-derived from date picker)
                "year":                    [year],
                "month":                   [month],
                "day_of_week":             [day_of_week],
                "quarter":                 [quarter],
                "is_weekend":              [is_weekend],
                "is_winter":               [is_winter],
                "is_spring":               [is_spring],
                "is_summer":               [is_summer],
                "is_fall":                 [is_fall],
                # Location / infrastructure
                "population":              [float(population)],
                "n_stations":              [float(n_stations)],
                "population_per_station":  [population_per_station],
                "monitoring_density_score":[monitoring_density_score],
                "lat":                     [float(lat)],
                "lon":                     [float(lon)],
                # PM2.5 rolling averages
                "pm25_7d_avg":             [pm25_7d_avg],
                "pm25_30d_avg":            [pm25_30d_avg],
                "pm25_90d_avg":            [pm25_90d_avg],
                # Optional co-pollutants
                "pm10_30d_avg":            [pm10_30d_avg],
                "no2_30d_avg":             [no2_30d_avg],
                # Volatility & lags
                "pm25_7d_volatility":      [pm25_7d_volatility],
                "pm25_lag_1d":             [pm25_lag_1d],
                "pm25_lag_7d":             [pm25_lag_7d],
                "pm25_lag_14d":            [pm25_lag_14d],
                "pm25_lag_30d":            [pm25_lag_30d],
                # Range stats
                "pm25_30d_min":            [pm25_30d_min],
                "pm25_30d_max":            [pm25_30d_max],
                "pm25_30d_range":          [pm25_30d_range],
                "pm25_trend":              [pm25_trend],
                "pm25_recent_ratio":       [pm25_recent_ratio],
                # WHO exceedance
                "was_exceeding_who_7d":    [was_exceeding_who_7d],
                "was_exceeding_who_30d":   [was_exceeding_who_30d],
            })

            result = predict_pm25(input_data)

        if result:
            pm25_val        = result["pm25_predicted"]
            category, color = aqi_category(pm25_val)

            st.markdown("## 📊 Prediction Results")

            m1, m2, m3, m4 = st.columns(4)
            m1.metric("Predicted PM2.5",  f"{pm25_val:.2f} µg/m³")
            m2.metric("AQI Category",      category)
            m3.metric("30d Baseline",      f"{pm25_30d_avg:.1f} µg/m³")
            delta_val = pm25_val - pm25_30d_avg
            m4.metric("vs. 30d Baseline",  f"{delta_val:+.2f} µg/m³", delta_color="inverse")

            # Colour gauge bar
            bar_pct = min(pm25_val / 300 * 100, 100)
            st.markdown(f"""
            <div style="position:relative; height:44px; background:#f3f4f6;
                        border-radius:6px; overflow:hidden; margin:1rem 0;">
                <div style="background:{color}; width:{bar_pct:.1f}%; height:100%;
                            display:flex; align-items:center; justify-content:center;
                            color:white; font-family:'IBM Plex Mono',monospace;
                            font-weight:600; font-size:16px; min-width:120px;">
                    {pm25_val:.2f} µg/m³
                </div>
            </div>
            <div style="text-align:center; color:#6b7280; font-size:0.8rem; margin-bottom:1rem;">
                Scale: 0 (Clean) → 300+ (Hazardous)
            </div>
            """, unsafe_allow_html=True)

            risk_css = ("risk-high" if pm25_val > 55.4 else
                        "risk-medium" if pm25_val > 12 else "risk-low")
            st.markdown(
                f'<div class="prediction-box {risk_css}">'
                f'{category} &nbsp;·&nbsp; {pm25_val:.2f} µg/m³</div>',
                unsafe_allow_html=True
            )

            # Recommendations
            st.markdown("### 💡 Recommended Actions")
            if pm25_val > 150.4:
                st.error("""
**🔴 Hazardous / Very Unhealthy — Immediate action required:**
- Issue public health advisory; avoid all outdoor activities
- Activate industrial emission controls
- Run air purifiers indoors on maximum setting
- Alert vulnerable populations: elderly, children, respiratory patients
- Consider school and outdoor event closures
                """)
            elif pm25_val > 35.4:
                st.warning("""
**🟠 Unhealthy — Protective measures advised:**
- Sensitive groups should limit prolonged outdoor exertion
- Recommend N95 masks for necessary outdoor activities
- Monitor trend closely; escalate if worsening
- Increase ventilation in indoor public spaces
                """)
            else:
                st.success("""
**🟢 Good / Moderate — Standard monitoring:**
- Continue routine air quality monitoring
- No immediate public health action required
- Watch for upward trends as early warning
                """)

            with st.expander("🔍 View Input Payload"):
                st.dataframe(
                    input_data.T.rename(columns={0: "value"}),
                    use_container_width=True
                )

            with st.expander("🤖 Model Info"):
                st.markdown(f"""
- **Endpoint:** air_quality_xgb
- **Version:** {MODEL_VERSION}
- **Raw prediction:** {pm25_val:.4f} µg/m³
- **Timestamp:** {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
                """)


# ===========================================================
# TAB 2 — Dashboard
# ===========================================================
with tab2:
    st.markdown("### 📊 Air Quality Analytics Dashboard")
    # st.caption("Live Databricks dashboard — may require workspace authentication.")

    components.html(
        f"""
        <iframe
            src="{DASHBOARD_URL}"
            width="100%"
            height="960"
            frameborder="0"
            style="border-radius:8px; border:1px solid #e5e7eb;">
        </iframe>
        """,
        height=960,
    )

# -------------------------------------------------------
# Footer
# -------------------------------------------------------
st.markdown("---")
st.markdown(f"""
<div class="footer-note">
    🌍 For environmental monitoring and decision support purposes only ·
    Model: {MODEL_VERSION} · {datetime.now().strftime("%Y-%m-%d %H:%M")}
</div>
""", unsafe_allow_html=True)