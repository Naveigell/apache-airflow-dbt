import os

import duckdb
from dash import Dash, dcc, html, Input, Output
import plotly.express as px
import dash_bootstrap_components as dbc
from dotenv import load_dotenv

load_dotenv()

con = duckdb.connect(os.environ['DATABASE_NAME'])

date_range = con.execute("""
    SELECT MIN(DATE(pickup_datetime)) AS min_date,
           MAX(DATE(pickup_datetime)) AS max_date
    FROM fct_trips
""").fetchone()

min_date, max_date = date_range

zones = con.execute("""
    SELECT DISTINCT dz.zone
    FROM dim_zones dz
    JOIN fct_trips ft ON ft.pickup_location_id = dz.location_id
    ORDER BY dz.zone
""").df()['zone'].tolist()

con.close()

app = Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])
app.title = "NYC Taxi Dashboard"

app.layout = html.Div([
    html.H1("NYC Taxi Trip Dashboard", className="text-center"),
    html.Div([
        html.Div([
            html.Label("Select Date Range", htmlFor="date-range"),
            html.Div([
                dcc.DatePickerRange(
                    id="date-range",
                    start_date=min_date,
                    end_date=max_date,
                    min_date_allowed=min_date,
                    max_date_allowed=max_date,
                    display_format="YYYY-MM-DD",
                    className="form-group w-100",
                    style={"width": "100%"}
                ),
            ], className="form-group w-100"),
        ], className="col-6"),

        html.Div([
            html.Label("Select Zone", htmlFor="zone-filter"),
            dcc.Dropdown(
                id="zone-filter",
                options=[{"label": z, "value": z} for z in zones],
                multi=True,
                placeholder="All Zones"
            )
        ], className="col-6"),
    ], className="row mt-5"),

    html.Hr(),

    # KPI Cards
    html.Div(id="kpi-cards", className="kpi-container"),

    html.Hr(),

    # Graphs
    html.Div([
        html.Div(className="row", children=[
            html.Div([
                dcc.Graph(id="trend-chart", className="card"),
            ], className="col-12 my-3"),
            html.Div([
                dcc.Graph(id="heatmap-chart", className="card"),
            ], className="col-6 my-3"),
            html.Div([
                dcc.Graph(id="payment-chart", className="card")
            ], className="col-6 my-3"),
            html.Div([
                dcc.Graph(id="top-zones-chart", className="card"),
            ], className="col-12 my-3"),
        ]),
    ])
], className="container my-5")

@app.callback(
    [
        Output("kpi-cards", "children"),
        Output("trend-chart", "figure"),
        Output("top-zones-chart", "figure"),
        Output("heatmap-chart", "figure"),
        Output("payment-chart", "figure")
    ],
    [
        Input("date-range", "start_date"),
        Input("date-range", "end_date"),
        Input("zone-filter", "value")
    ]
)
def update_dashboard(start_date, end_date, selected_zones):
    con = duckdb.connect(os.environ['DATABASE_NAME'])

    zone_filter = ""
    if selected_zones:
        zone_list = "', '".join(selected_zones)
        zone_filter = f"AND dz.zone IN ('{zone_list}')"

    df_kpi = con.execute(f"""
        SELECT
            COUNT(*) AS total_trips,
            SUM(total_amount) AS total_revenue,
            ROUND(AVG(total_amount), 2) AS avg_fare
        FROM fct_trips ft
        JOIN dim_zones dz ON ft.pickup_location_id = dz.location_id
        WHERE DATE(ft.pickup_datetime) BETWEEN '{start_date}' AND '{end_date}'
        {zone_filter}
    """).df()

    df_trend = con.execute(f"""
        SELECT
            DATE(ft.pickup_datetime) AS trip_date,
            COUNT(*) AS total_trips,
            SUM(ft.total_amount) AS total_revenue
        FROM fct_trips ft
        JOIN dim_zones dz ON ft.pickup_location_id = dz.location_id
        WHERE DATE(ft.pickup_datetime) BETWEEN '{start_date}' AND '{end_date}'
        {zone_filter}
        GROUP BY trip_date
        ORDER BY trip_date
    """).df()

    df_top_zones = con.execute(f"""
        SELECT
            dz.zone AS pickup_zone,
            SUM(ft.total_amount) AS total_revenue,
            COUNT(*) AS total_trips
        FROM fct_trips ft
        JOIN dim_zones dz ON ft.pickup_location_id = dz.location_id
        WHERE DATE(ft.pickup_datetime) BETWEEN '{start_date}' AND '{end_date}'
        {zone_filter}
        GROUP BY dz.zone
        ORDER BY total_revenue DESC
        LIMIT 10
    """).df()

    df_heatmap = con.execute(f"""
        SELECT
            dz.zone AS pickup_zone,
            EXTRACT(HOUR FROM ft.pickup_datetime) AS pickup_hour,
            COUNT(*) AS total_trips
        FROM fct_trips ft
        JOIN dim_zones dz ON ft.pickup_location_id = dz.location_id
        WHERE DATE(ft.pickup_datetime) BETWEEN '{start_date}' AND '{end_date}'
        {zone_filter}
        GROUP BY dz.zone, pickup_hour
        ORDER BY dz.zone, pickup_hour
    """).df()

    df_payment = con.execute(f"""
        SELECT
            CASE ft.payment_type
                WHEN 1 THEN 'Credit Card'
                WHEN 2 THEN 'Cash'
                WHEN 3 THEN 'No Charge'
                WHEN 4 THEN 'Dispute'
                WHEN 5 THEN 'Unknown'
                ELSE 'Other'
            END AS payment_method,
            COUNT(*) AS total_trips
        FROM fct_trips ft
        JOIN dim_zones dz ON ft.pickup_location_id = dz.location_id
        WHERE DATE(ft.pickup_datetime) BETWEEN '{start_date}' AND '{end_date}'
        {zone_filter}
        GROUP BY payment_method
        ORDER BY total_trips DESC
    """).df()

    con.close()

    kpi_cards = html.Div([
        html.Div([
            html.Div([
                html.H4("Total Trips"),
                html.H2(f"{df_kpi['total_trips'][0]:,}")
            ], className="card p-4")
        ], className="col-4 p-2"),
        html.Div([
            html.Div([
                html.H4("Total Revenue"),
                html.H2(f"${df_kpi['total_revenue'][0]:,.2f}")
            ], className="card p-4")
        ], className="col-4 p-2"),
        html.Div([
            html.Div([
                html.H4("Average Fare"),
                html.H2(f"${df_kpi['avg_fare'][0]:,.2f}")
            ], className="card p-4")
        ], className="col-4 p-2"),
    ], className="row")

    fig_trend = px.line(
        df_trend,
        x="trip_date",
        y="total_trips",
        title="Daily Trip Trend"
    )

    fig_trend.update_layout(
        xaxis_title="Trip Date",
        yaxis_title="Total Trips"
    )

    fig_top_zones = px.bar(
        df_top_zones,
        x="pickup_zone",
        y="total_revenue",
        title="Top 10 Zones by Revenue",
        text_auto=True
    )

    fig_top_zones.update_layout(
        xaxis_title="Pickup Zone",
        yaxis_title="Total Revenue"
    )

    fig_heatmap = px.density_heatmap(
        df_heatmap,
        x="pickup_hour",
        y="pickup_zone",
        z="total_trips",
        color_continuous_scale="YlOrRd",
        title="Demand Heatmap by Hour and Zone"
    )

    fig_heatmap.update_layout(
        xaxis_title="Pickup Hour",
        yaxis_title="Pickup Zone",
    )

    fig_payment = px.pie(
        df_payment,
        names="payment_method",
        values="total_trips",
        title="Payment Method Breakdown",
        hole=0.4
    )

    fig_payment.update_layout(
        xaxis_title="Payment Method",
        yaxis_title="Total Trips"
    )

    return kpi_cards, fig_trend, fig_top_zones, fig_heatmap, fig_payment


if __name__ == "__main__":
    app.run(debug=True)
