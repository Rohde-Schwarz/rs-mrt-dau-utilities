import altair as alt
import polars as pl

from .dev import delay_get_segment, delay_get_start_stop_segment, delay_parse_log


def extract_delay_from_log(log_file: str) -> dict[str, pl.DataFrame]:
    """
    Extract delay information from the centralservice.log file.
    This file is located in the following directory:
    /DATAUNIT/rohde-schwarz/log/DAU/centralservice.log
    The function parses the log file, extracts relevant data, and returns a dictionary containing Polars DataFrames.
    Each keys is a combination of the segments found (start-stop pairs) and the measurements found in one segment.
    - "1_1": first segment (start-stop) and first meas_id
    - "1_2": first segment (start-stop) and second meas_id
    - "2_1": second segment (start-stop) and first meas_id
    - ...
    """
    # Parse the log file to extract delay information
    parsed_data = delay_parse_log(log_file)

    # Get start and stop segments from the command DataFrame
    results_per_segment = delay_get_start_stop_segment(
        parsed_data["command"], parsed_data["hash"]
    )

    # Get segments of data based on the extracted start and stop times
    result_per_hash = delay_get_segment(results_per_segment)

    return result_per_hash


def plot_all(results_one_segment: pl.DataFrame) -> alt.RepeatChart:
    """
    Plot all the delays found for this measurement.
    """
    items = results_one_segment.columns
    filtered_items = [item for item in items if item.startswith("delay")]

    chart = (
        alt.Chart(results_one_segment)
        .mark_point()
        .encode(
            # x='min_time:T',
            # alt.X(items[1], title='time'),
            alt.X(alt.repeat("column"), type="temporal", title="time"),
            alt.Y(alt.repeat("row"), type="quantitative"),
            # y=r'ip\.throughput_interval_bps_dst_src:Q',
            # color='flow_id',
        )
        .properties(width=1100, height=300)
        .repeat(row=filtered_items, column=[items[1]])
        .interactive()
    )
    return chart
