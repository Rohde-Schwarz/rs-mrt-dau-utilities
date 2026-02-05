import polars as pl
import pytest
import tempfile
import datetime
import json
import gzip
import base64
from polars.testing import assert_frame_equal

# Import the function to be tested
from rs_mrt_dau_utilities.delay_meas.dev import (
    delay_get_segment,
    delay_get_start_stop_segment,
    delay_parse_log,
)


def test_delay_get_start_stop_segment_basic():
    # Create a sample command DataFrame
    command_data = {
        "timestamp": [1, 2, 3, 4, 5, 6],
        "command": ["Start", "Stop", "Start", "Stop", "Start", "Stop"],
    }
    command_df = pl.DataFrame(command_data)

    # Create a sample hash DataFrame
    hash_data = {
        "timestamp": [1, 2, 3, 4, 5, 6],
        "value": ["a", "b", "c", "d", "e", "f"],
    }
    hash_df = pl.DataFrame(hash_data)

    # Call the function
    result = delay_get_start_stop_segment(command_df, hash_df)

    # Define expected results
    expected_results = [
        pl.DataFrame({"timestamp": [1, 2], "value": ["a", "b"]}),
        pl.DataFrame({"timestamp": [3, 4], "value": ["c", "d"]}),
        pl.DataFrame({"timestamp": [5, 6], "value": ["e", "f"]}),
    ]

    # Assert the results
    assert len(result) == len(expected_results)
    for res, exp in zip(result, expected_results):
        assert_frame_equal(res, exp)
        # assert res.frame_equal(exp)


def test_delay_get_start_stop_segment_no_start():
    # Create a sample command DataFrame with no "Start" command
    command_data = {
        "timestamp": [1, 2, 3],
        "command": ["Stop", "Stop", "Stop"],
    }
    command_df = pl.DataFrame(command_data)

    # Create a sample hash DataFrame
    hash_data = {
        "timestamp": [1, 2, 3],
        "value": ["a", "b", "c"],
    }
    hash_df = pl.DataFrame(hash_data)

    # Call the function
    result = delay_get_start_stop_segment(command_df, hash_df)

    # Assert the result is empty
    assert result == []


def test_delay_get_start_stop_segment_no_stop():
    # Create a sample command DataFrame with no "Stop" command
    command_data = {
        "timestamp": [1, 2, 3],
        "command": ["Start", "Start", "Start"],
    }
    command_df = pl.DataFrame(command_data)

    # Create a sample hash DataFrame
    hash_data = {
        "timestamp": [1, 2, 3],
        "value": ["a", "b", "c"],
    }
    hash_df = pl.DataFrame(hash_data)

    # Call the function
    result = delay_get_start_stop_segment(command_df, hash_df)

    # Assert the result is empty
    assert result == []


def test_delay_get_start_stop_segment_mixed_commands():
    # Create a sample command DataFrame with mixed commands
    command_data = {
        "timestamp": [1, 2, 3, 4, 5],
        "command": ["Start", "Stop", "Stop", "Start", "Stop"],
    }
    command_df = pl.DataFrame(command_data)

    # Create a sample hash DataFrame
    hash_data = {
        "timestamp": [1, 2, 3, 4, 5],
        "value": ["a", "b", "c", "d", "e"],
    }
    hash_df = pl.DataFrame(hash_data)

    # Call the function
    result = delay_get_start_stop_segment(command_df, hash_df)

    # Define expected results
    expected_results = [
        pl.DataFrame({"timestamp": [1, 2], "value": ["a", "b"]}),
        pl.DataFrame({"timestamp": [4, 5], "value": ["d", "e"]}),
    ]

    # Assert the results
    assert len(result) == len(expected_results)
    for res, exp in zip(result, expected_results):
        assert_frame_equal(res, exp)
        # assert res.frame_equal(exp)


def test_delay_get_segment_basic():
    # Create a sample result_per_segment input
    result_per_segment = [
        pl.DataFrame(
            {
                "timestamp": [
                    1748433393,
                    1748433394,
                    1748433395,
                    1748433396,
                ],
                "hash": [1, 1, 2, 2],
                "origin": ["Ims", "Upc", "Ims", "Upc"],
                "meas_id": [None, 1, None, 1],
            }
        ),
        pl.DataFrame(
            {
                "timestamp": [
                    1748433397,
                    1748433398,
                    1748433399,
                    1748433400,
                ],
                "hash": [3, 3, 4, 4],
                "origin": ["Upc", "Ims", "Upc", "Ims"],
                "meas_id": [2, None, 2, None],
            }
        ),
    ]

    result_per_segment[0] = result_per_segment[0].with_columns(
        timestamp=pl.from_epoch("timestamp", time_unit="s").dt.replace_time_zone("UTC")
    )
    result_per_segment[1] = result_per_segment[1].with_columns(
        timestamp=pl.from_epoch("timestamp", time_unit="s").dt.replace_time_zone("UTC")
    )

    # Call the function
    result = delay_get_segment(result_per_segment)

    # Define expected results
    expected_results = {
        "1_1": pl.DataFrame(
            {
                "hash": [1, 2],
                "Ims_1": [1748433393, 1748433395],
                "Upc_1": [1748433394, 1748433396],
                "delay_global_us": [1000000, 1000000],
            }
        ),
        "2_2": pl.DataFrame(
            {
                "hash": [3, 4],
                "Upc_1": [1748433397, 1748433399],
                "Ims_1": [1748433398, 1748433400],
                "delay_global_us": [1000000, 1000000],
            }
        ),
    }
    expected_results["1_1"] = expected_results["1_1"].with_columns(
        Ims_1=pl.from_epoch("Ims_1", time_unit="s").dt.replace_time_zone("UTC"),
        Upc_1=pl.from_epoch("Upc_1", time_unit="s").dt.replace_time_zone("UTC"),
    )
    expected_results["2_2"] = expected_results["2_2"].with_columns(
        Ims_1=pl.from_epoch("Ims_1", time_unit="s").dt.replace_time_zone("UTC"),
        Upc_1=pl.from_epoch("Upc_1", time_unit="s").dt.replace_time_zone("UTC"),
    )
    # Assert the results
    assert set(result.keys()) == set(expected_results.keys())
    for key in result:
        assert_frame_equal(result[key], expected_results[key])
        # assert result[key].frame_equal(expected_results[key])


def test_delay_get_segment_all_paths():
    # Create a sample result_per_segment input
    result_per_segment = [
        pl.DataFrame(
            {
                "timestamp": [
                    1748433393,
                    1748433394,
                    1748433395,
                    1748433396,
                ],
                "hash": [1, 1, 2, 2],
                "origin": ["Ims", "Upc", "Ims", "Upc"],
                "meas_id": [None, 1, None, 1],
            }
        )
    ]
    result_per_segment[0] = result_per_segment[0].with_columns(
        timestamp=pl.from_epoch("timestamp", time_unit="s").dt.replace_time_zone("UTC")
    )

    # Call the function with all_paths=True
    result = delay_get_segment(result_per_segment, all_paths=True)

    # Define expected results
    expected_results = {
        "1_1": pl.DataFrame(
            {
                "hash": [1, 2],
                "Ims_1": [1748433393, 1748433395],
                "Upc_1": [1748433394, 1748433396],
                "delay_global_us": [1000000, 1000000],
                "delay-Ims_1->Upc_1_us": [1000000, 1000000],
            }
        )
    }
    expected_results["1_1"] = expected_results["1_1"].with_columns(
        Ims_1=pl.from_epoch("Ims_1", time_unit="s").dt.replace_time_zone("UTC"),
        Upc_1=pl.from_epoch("Upc_1", time_unit="s").dt.replace_time_zone("UTC"),
    )

    # Assert the results
    assert set(result.keys()) == set(expected_results.keys())
    for key in result:
        assert_frame_equal(result[key], expected_results[key])
        # assert result[key].frame_equal(expected_results[key])


def test_delay_get_segment_no_upc():
    # Create a sample result_per_segment input with no "Upc" in origin
    result_per_segment = [
        pl.DataFrame(
            {
                "timestamp": [
                    1748433393,
                    1748433394,
                    1748433395,
                    1748433396,
                ],
                "hash": [1, 2, 3, 4],
                "origin": ["Other", "Other", "Other", "Other"],
                "meas_id": [None, None, None, None],
            }
        )
    ]
    result_per_segment[0] = result_per_segment[0].with_columns(
        timestamp=pl.from_epoch("timestamp", time_unit="s").dt.replace_time_zone("UTC")
    )

    # Call the function
    result = delay_get_segment(result_per_segment)

    # Assert the result is empty
    assert result == {}


def create_sample_log_file(content: str) -> str:
    """Helper function to create a temporary log file with the given content."""
    temp_file = tempfile.NamedTemporaryFile(delete=False)
    temp_file.write(content.encode("utf-8"))
    temp_file.close()
    return temp_file.name


def test_delay_parse_log_basic():
    # Create a sample log content
    timestamp1 = datetime.datetime.fromtimestamp(1633072798, tz=datetime.timezone.utc).isoformat()
    timestamp2 = datetime.datetime.fromtimestamp(1633072800, tz=datetime.timezone.utc).isoformat()
    timestamp3 = datetime.datetime.fromtimestamp(1633072805, tz=datetime.timezone.utc).isoformat()
    meas_data = json.dumps(
        {
            "hash": 123456789,
            "meas": [
                {
                    "timestamp": {"secs": 1633072800, "nanos": 123456789},
                    "meas_id": "meas1",
                    "origin": "Upc",
                },
                {
                    "timestamp": {"secs": 1633072801, "nanos": 987654321},
                    "meas_id": "meas2",
                    "origin": "Upc",
                },
            ],
        }
    )
    encoded_data = base64.b64encode(gzip.compress(meas_data.encode("utf-8"))).decode(
        "utf-8"
    )
    log_content = (
        f"{timestamp1}  INFO centralservice::delay_meas_core: Start msg from FSW received\n"
        f"{timestamp2}  INFO centralservice::delay_meas_core: mime=application/json, data={encoded_data}\n"
        f"{timestamp3}  INFO centralservice::delay_meas_core: Stop msg from FSW received\n"
    )

    # Create a temporary log file
    log_file = create_sample_log_file(log_content)

    # Call the function
    result = delay_parse_log(log_file)

    # Define expected results
    expected_hash_df = (
        pl.DataFrame(
            {
                "timestamp": [1633072800123456789, 1633072801987654321],
                "meas_id": ["meas1", "meas2"],
                "origin": ["Upc", "Upc"],
                "hash": [123456789, 123456789],
            }
        )
        .with_columns(
            timestamp=pl.from_epoch("timestamp", time_unit="ns").dt.replace_time_zone(
                "UTC"
            )
        )
        .cast({"hash": pl.UInt64})
    )

    expected_command_df = pl.DataFrame(
        {
            "timestamp": [
                datetime.datetime.fromisoformat(timestamp1),
                datetime.datetime.fromisoformat(timestamp3),
            ],
            "command": ["Start", "Stop"],
        }
    )

    # Assert the results
    assert_frame_equal(result["command"], expected_command_df)
    assert_frame_equal(result["hash"], expected_hash_df)


# def test_delay_parse_log_no_hash():
    # Create a sample log content with no hash data
    # timestamp = datetime.datetime.now().isoformat()
    # log_content = f"{timestamp} INFO centralservice::delay_meas_core: Start msg from FSW received\n"
 
    # Create a temporary log file
    # log_file = create_sample_log_file(log_content)

    # Call the function
    # result = delay_parse_log(log_file)

    # Define expected results
    # expected_hash_df = pl.DataFrame(
    #    {"timestamp": [datetime.datetime.fromisoformat(timestamp)], "meas_id": [], "origin": [], "hash": []}
    # )

    # expected_command_df = pl.DataFrame(
    #    {
    #        "timestamp": [datetime.datetime.fromisoformat(timestamp)],
    #        "command": ["Start"],
    #    }
    # )

    # Assert the results
    # assert expected_hash_df.is_empty()
    # assert_frame_equal(result["hash"], expected_hash_df)
    # assert_frame_equal(result["command"], expected_command_df)


def test_delay_parse_log_no_command():
    # Create a sample log content with no command data
    timestamp = datetime.datetime.now().isoformat()
    meas_data = json.dumps(
        {
            "hash": 123456789,
            "meas": [
                {
                    "timestamp": {"secs": 1633072800, "nanos": 123456789},
                    "meas_id": "meas1",
                    "origin": ["Upc"],
                }
            ],
        }
    )
    encoded_data = base64.b64encode(gzip.compress(meas_data.encode("utf-8"))).decode(
        "utf-8"
    )
    log_content = f"{timestamp} INFO centralservice::delay_meas_core: mime=application/json, data={encoded_data}\n"

    # Create a temporary log file
    log_file = create_sample_log_file(log_content)

    # Call the function
    result = delay_parse_log(log_file)

    # Define expected results
    expected_hash_df = (
        pl.DataFrame(
            {
                "timestamp": [1633072800123456789],
                "meas_id": ["meas1"],
                "origin": [["Upc"]],
                "hash": [123456789],
            }
        )
        .with_columns(
            timestamp=pl.from_epoch("timestamp", time_unit="ns").dt.replace_time_zone(
                "UTC"
            )
        )
        .cast({"hash": pl.UInt64})
    )

    expected_command_df = pl.DataFrame({"timestamp": [], "command": []})
 
    # Assert the results
    assert expected_command_df.is_empty()
    assert_frame_equal(result["hash"], expected_hash_df)


# Run the tests
if __name__ == "__main__":
    pytest.main()
