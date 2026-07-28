from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any

import pandas as pd
import streamlit as st


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATABASE_FILE = PROJECT_ROOT / "database" / "bus_analytics.db"


st.set_page_config(
    page_title="Bus Timetable Analytics",
    page_icon="🚌",
    layout="wide",
)


@st.cache_data(ttl=300, show_spinner=False)
def query_dataframe(
    query: str,
    params: tuple[Any, ...] = (),
) -> pd.DataFrame:
    """Run a read-only parameterised query and return a DataFrame."""

    connection = sqlite3.connect(DATABASE_FILE)

    try:
        connection.execute("PRAGMA query_only = ON")

        return pd.read_sql_query(
            query,
            connection,
            params=params,
        )

    finally:
        connection.close()


@st.cache_data(ttl=300, show_spinner=False)
def query_scalar(
    query: str,
    params: tuple[Any, ...] = (),
) -> Any:
    """Run a read-only parameterised query and return one value."""

    connection = sqlite3.connect(DATABASE_FILE)

    try:
        connection.execute("PRAGMA query_only = ON")

        row = connection.execute(
            query,
            params,
        ).fetchone()

        return row[0] if row is not None else None

    finally:
        connection.close()


@st.cache_data(ttl=300, show_spinner=False)
def load_route_options() -> list[str]:
    """Load all service-line references for dashboard filters."""

    routes = query_dataframe(
        """
        SELECT DISTINCT line_ref
        FROM segments
        ORDER BY line_ref
        """
    )

    return routes["line_ref"].astype(str).tolist()


def format_minutes(value: float | int | None) -> str:
    """Format a minute value for dashboard metrics."""

    if value is None:
        return "N/A"

    return f"{float(value):.3f} min"


def show_overview() -> None:
    """Render the project-level dashboard overview."""

    overview = query_dataframe(
        """
        SELECT
            (SELECT COUNT(*) FROM segments) AS segment_records,
            (SELECT COUNT(DISTINCT line_ref) FROM segments)
                AS service_lines,
            (SELECT COUNT(*) FROM stops) AS stop_records,
            (SELECT COUNT(*) FROM predictions)
                AS prediction_records,
            (SELECT COUNT(*) FROM predictions
                WHERE is_model_anomaly = 1)
                AS anomaly_records
        """
    ).iloc[0]

    best_model = query_dataframe(
        """
        SELECT
            model_name,
            rmse,
            mae,
            r2,
            training_seconds
        FROM model_results
        ORDER BY rank_by_rmse
        LIMIT 1
        """
    ).iloc[0]

    st.subheader("System overview")

    first_row = st.columns(5)
    first_row[0].metric(
        "Processed segments",
        f"{int(overview['segment_records']):,}",
    )
    first_row[1].metric(
        "Service lines",
        f"{int(overview['service_lines']):,}",
    )
    first_row[2].metric(
        "NaPTAN stops",
        f"{int(overview['stop_records']):,}",
    )
    first_row[3].metric(
        "Test predictions",
        f"{int(overview['prediction_records']):,}",
    )
    first_row[4].metric(
        "Model anomalies",
        f"{int(overview['anomaly_records']):,}",
    )

    st.subheader("Best regression model")

    model_columns = st.columns(5)
    model_columns[0].metric(
        "Model",
        str(best_model["model_name"]),
    )
    model_columns[1].metric(
        "RMSE",
        format_minutes(best_model["rmse"]),
    )
    model_columns[2].metric(
        "MAE",
        format_minutes(best_model["mae"]),
    )
    model_columns[3].metric(
        "R²",
        f"{float(best_model['r2']):.3f}",
    )
    model_columns[4].metric(
        "Training time",
        f"{float(best_model['training_seconds']):.2f} s",
    )

    left_chart, right_chart = st.columns(2)

    time_summary = query_dataframe(
        """
        SELECT
            time_of_day,
            COUNT(*) AS record_count,
            ROUND(AVG(runtime_minutes), 4)
                AS average_runtime_minutes
        FROM segments
        GROUP BY time_of_day
        ORDER BY
            CASE time_of_day
                WHEN 'Morning' THEN 1
                WHEN 'Afternoon' THEN 2
                WHEN 'Evening' THEN 3
                ELSE 4
            END
        """
    )

    with left_chart:
        st.markdown("#### Average scheduled runtime by time of day")
        st.bar_chart(
            time_summary.set_index("time_of_day")[
                "average_runtime_minutes"
            ]
        )
        st.dataframe(
            time_summary,
            width="stretch",
            hide_index=True,
        )

    top_routes = query_dataframe(
        """
        SELECT
            line_ref,
            COUNT(*) AS segment_records,
            ROUND(AVG(runtime_minutes), 4)
                AS average_runtime_minutes
        FROM segments
        GROUP BY line_ref
        ORDER BY segment_records DESC
        LIMIT 10
        """
    )

    with right_chart:
        st.markdown("#### Ten largest service lines")
        st.bar_chart(
            top_routes.set_index("line_ref")[
                "segment_records"
            ]
        )
        st.dataframe(
            top_routes,
            width="stretch",
            hide_index=True,
        )

    st.info(
        "The dashboard analyses scheduled stop-to-stop runtimes. "
        "It does not claim to measure live traffic delay or actual vehicle lateness."
    )


def show_route_explorer(routes: list[str]) -> None:
    """Render route-level summaries and stop geography."""

    st.subheader("Route explorer")

    selected_route = st.selectbox(
        "Choose a service line",
        options=routes,
        key="route_explorer_route",
    )

    route_summary = query_dataframe(
        """
        SELECT
            COUNT(*) AS segment_records,
            COUNT(DISTINCT segment_key) AS unique_segments,
            ROUND(AVG(runtime_minutes), 4)
                AS average_runtime_minutes,
            ROUND(AVG(distance_km), 4)
                AS average_distance_km,
            MAX(runtime_minutes) AS maximum_runtime_minutes,
            SUM(is_iqr_high_duration)
                AS high_duration_records
        FROM segments
        WHERE line_ref = ?
        """,
        (selected_route,),
    ).iloc[0]

    metric_columns = st.columns(6)
    metric_columns[0].metric(
        "Records",
        f"{int(route_summary['segment_records']):,}",
    )
    metric_columns[1].metric(
        "Unique segments",
        f"{int(route_summary['unique_segments']):,}",
    )
    metric_columns[2].metric(
        "Average runtime",
        format_minutes(route_summary["average_runtime_minutes"]),
    )
    metric_columns[3].metric(
        "Average distance",
        f"{float(route_summary['average_distance_km']):.3f} km",
    )
    metric_columns[4].metric(
        "Maximum runtime",
        format_minutes(route_summary["maximum_runtime_minutes"]),
    )
    metric_columns[5].metric(
        "IQR flags",
        f"{int(route_summary['high_duration_records']):,}",
    )

    hourly_summary = query_dataframe(
        """
        SELECT
            departure_hour,
            COUNT(*) AS record_count,
            ROUND(AVG(runtime_minutes), 4)
                AS average_runtime_minutes
        FROM segments
        WHERE line_ref = ?
        GROUP BY departure_hour
        ORDER BY departure_hour
        """,
        (selected_route,),
    )

    chart_column, map_column = st.columns(2)

    with chart_column:
        st.markdown("#### Runtime pattern by departure hour")
        st.line_chart(
            hourly_summary.set_index("departure_hour")[
                "average_runtime_minutes"
            ]
        )
        st.dataframe(
            hourly_summary,
            width="stretch",
            hide_index=True,
        )

    route_stops = query_dataframe(
        """
        SELECT DISTINCT
            s.stop_ref,
            s.stop_name,
            s.latitude,
            s.longitude
        FROM stops AS s
        INNER JOIN (
            SELECT from_stop_ref AS stop_ref
            FROM segments
            WHERE line_ref = ?

            UNION

            SELECT to_stop_ref AS stop_ref
            FROM segments
            WHERE line_ref = ?
        ) AS route_stop_refs
            ON s.stop_ref = route_stop_refs.stop_ref
        ORDER BY s.stop_name
        """,
        (selected_route, selected_route),
    )

    with map_column:
        st.markdown("#### Stops used by the selected service line")
        st.map(
            route_stops,
            latitude="latitude",
            longitude="longitude",
            height=450,
        )
        st.caption(
            f"Mapped stops: {len(route_stops):,}"
        )

    longest_segments = query_dataframe(
        """
        SELECT
            seg.from_stop_ref,
            origin.stop_name AS origin_stop,
            seg.to_stop_ref,
            destination.stop_name AS destination_stop,
            ROUND(seg.distance_km, 4) AS distance_km,
            seg.runtime_minutes,
            seg.departure_time,
            seg.time_of_day
        FROM segments AS seg
        INNER JOIN stops AS origin
            ON seg.from_stop_ref = origin.stop_ref
        INNER JOIN stops AS destination
            ON seg.to_stop_ref = destination.stop_ref
        WHERE seg.line_ref = ?
        ORDER BY
            seg.runtime_minutes DESC,
            seg.distance_km DESC
        LIMIT 25
        """,
        (selected_route,),
    )

    st.markdown("#### Longest scheduled segment records")
    st.dataframe(
        longest_segments,
        width="stretch",
        hide_index=True,
    )


def show_anomaly_explorer(routes: list[str]) -> None:
    """Render interactive model-anomaly filters and results."""

    st.subheader("Residual-based anomaly explorer")

    filter_columns = st.columns(4)

    selected_route = filter_columns[0].selectbox(
        "Service line",
        options=["All routes"] + routes,
        key="anomaly_route",
    )

    selected_direction = filter_columns[1].selectbox(
        "Anomaly direction",
        options=[
            "All directions",
            "Longer than expected",
            "Shorter than expected",
        ],
    )

    minimum_error = filter_columns[2].slider(
        "Minimum absolute error (minutes)",
        min_value=0.5,
        max_value=10.0,
        value=1.0,
        step=0.1,
    )

    row_limit = filter_columns[3].selectbox(
        "Maximum displayed records",
        options=[25, 50, 100, 250],
        index=1,
    )

    route_parameter = (
        None
        if selected_route == "All routes"
        else selected_route
    )

    direction_parameter = (
        None
        if selected_direction == "All directions"
        else selected_direction
    )

    summary = query_dataframe(
        """
        SELECT
            COUNT(*) AS anomaly_records,
            ROUND(AVG(absolute_error_minutes), 4)
                AS average_absolute_error,
            ROUND(MAX(absolute_error_minutes), 4)
                AS maximum_absolute_error,
            ROUND(AVG(actual_runtime_minutes), 4)
                AS average_actual_runtime,
            ROUND(AVG(predicted_runtime_minutes), 4)
                AS average_predicted_runtime
        FROM predictions
        WHERE is_model_anomaly = 1
          AND absolute_error_minutes >= ?
          AND (? IS NULL OR line_ref = ?)
          AND (? IS NULL OR anomaly_direction = ?)
        """,
        (
            minimum_error,
            route_parameter,
            route_parameter,
            direction_parameter,
            direction_parameter,
        ),
    ).iloc[0]

    summary_columns = st.columns(5)
    summary_columns[0].metric(
        "Matching anomalies",
        f"{int(summary['anomaly_records']):,}",
    )
    summary_columns[1].metric(
        "Mean absolute error",
        format_minutes(summary["average_absolute_error"]),
    )
    summary_columns[2].metric(
        "Maximum error",
        format_minutes(summary["maximum_absolute_error"]),
    )
    summary_columns[3].metric(
        "Mean actual runtime",
        format_minutes(summary["average_actual_runtime"]),
    )
    summary_columns[4].metric(
        "Mean prediction",
        format_minutes(summary["average_predicted_runtime"]),
    )

    anomalies = query_dataframe(
        """
        SELECT
            p.line_ref,
            origin.stop_name AS origin_stop,
            destination.stop_name AS destination_stop,
            ROUND(p.distance_km, 4) AS distance_km,
            ROUND(p.actual_runtime_minutes, 4)
                AS actual_runtime_minutes,
            ROUND(p.predicted_runtime_minutes, 4)
                AS predicted_runtime_minutes,
            ROUND(p.residual_minutes, 4)
                AS residual_minutes,
            ROUND(p.absolute_error_minutes, 4)
                AS absolute_error_minutes,
            p.anomaly_direction,
            p.departure_time,
            origin.latitude,
            origin.longitude
        FROM predictions AS p
        INNER JOIN stops AS origin
            ON p.from_stop_ref = origin.stop_ref
        INNER JOIN stops AS destination
            ON p.to_stop_ref = destination.stop_ref
        WHERE p.is_model_anomaly = 1
          AND p.absolute_error_minutes >= ?
          AND (? IS NULL OR p.line_ref = ?)
          AND (? IS NULL OR p.anomaly_direction = ?)
        ORDER BY p.absolute_error_minutes DESC
        LIMIT ?
        """,
        (
            minimum_error,
            route_parameter,
            route_parameter,
            direction_parameter,
            direction_parameter,
            int(row_limit),
        ),
    )

    if anomalies.empty:
        st.warning("No anomaly records match the selected filters.")
        return

    table_column, map_column = st.columns([3, 2])

    with table_column:
        st.markdown("#### Highest-error records")
        st.dataframe(
            anomalies.drop(
                columns=["latitude", "longitude"]
            ),
            width="stretch",
            hide_index=True,
        )

        st.download_button(
            label="Download filtered anomalies as CSV",
            data=anomalies.drop(
                columns=["latitude", "longitude"]
            ).to_csv(index=False).encode("utf-8"),
            file_name="filtered_model_anomalies.csv",
            mime="text/csv",
        )

    with map_column:
        st.markdown("#### Origin locations of displayed anomalies")
        st.map(
            anomalies,
            latitude="latitude",
            longitude="longitude",
            height=500,
        )

    st.caption(
        "Anomalies are scheduled runtimes whose prediction error exceeds "
        "the training-derived threshold. They are review flags, not confirmed service failures."
    )


def show_model_and_security(routes: list[str]) -> None:
    """Render model comparison and a parameterised-query demonstration."""

    st.subheader("Model evaluation")

    model_results = query_dataframe(
        """
        SELECT
            rank_by_rmse,
            model_name,
            rmse,
            mae,
            r2,
            training_seconds,
            evaluation_seconds,
            configuration
        FROM model_results
        ORDER BY rank_by_rmse
        """
    )

    st.dataframe(
        model_results,
        width="stretch",
        hide_index=True,
    )

    metric_chart_column, r2_chart_column = st.columns(2)

    with metric_chart_column:
        st.markdown("#### RMSE by model")
        st.bar_chart(
            model_results.set_index("model_name")["rmse"]
        )

    with r2_chart_column:
        st.markdown("#### R² by model")
        st.bar_chart(
            model_results.set_index("model_name")["r2"]
        )

    st.divider()
    st.subheader("Secure parameterised route lookup")

    st.write(
        "The value entered below is supplied separately from the SQL statement. "
        "Characters that resemble SQL remain ordinary text."
    )

    example_route = routes[0] if routes else ""

    lookup_value = st.text_input(
        "Exact service-line reference",
        value=example_route,
    )

    if st.button("Run secure lookup"):
        lookup_result = query_dataframe(
            """
            SELECT
                COUNT(*) AS matching_records,
                ROUND(AVG(runtime_minutes), 4)
                    AS average_runtime_minutes
            FROM segments
            WHERE line_ref = ?
            """,
            (lookup_value,),
        ).iloc[0]

        lookup_columns = st.columns(2)
        lookup_columns[0].metric(
            "Matching records",
            f"{int(lookup_result['matching_records']):,}",
        )
        lookup_columns[1].metric(
            "Average runtime",
            format_minutes(
                lookup_result["average_runtime_minutes"]
            ),
        )

        if lookup_value.strip() == "' OR 1=1 --":
            st.success(
                "The SQL-injection-style text returned no unintended records "
                "because the query used a parameter placeholder."
            )

    st.code(
        "SELECT COUNT(*) FROM segments WHERE line_ref = ?;",
        language="sql",
    )


def main() -> None:
    """Run the Streamlit analytics dashboard."""

    st.title("🚌 Bus Timetable Analytics Dashboard")
    st.caption(
        "Stagecoach South East timetable consistency and scheduled-runtime analysis"
    )

    if not DATABASE_FILE.exists():
        st.error(
            "The SQLite database was not found. Run "
            "`python .\\src\\build_database.py` before starting the dashboard."
        )
        st.stop()

    routes = load_route_options()

    if not routes:
        st.error("No service lines were found in the database.")
        st.stop()

    overview_tab, route_tab, anomaly_tab, model_tab = st.tabs(
        [
            "Overview",
            "Route Explorer",
            "Anomaly Explorer",
            "Model & Security",
        ]
    )

    with overview_tab:
        show_overview()

    with route_tab:
        show_route_explorer(routes)

    with anomaly_tab:
        show_anomaly_explorer(routes)

    with model_tab:
        show_model_and_security(routes)


if __name__ == "__main__":
    main()