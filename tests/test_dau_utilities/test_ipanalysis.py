import base64
import gzip

# import re
# import datetime
import json
import logging

import pytest

# from polars.testing import assert_frame_equal
# import fast_json_normalize
# Import the function to be tested
from rs_mrt_dau_utilities.ip_analysis.ip_analysis import (
    ipanalysis_init_dataframes,
    ipanalysis_parse_json_result,
    ipanalysis_parse_scpi_result,
    ipanalysis_parse_scpi_schema_result,
    ipanalysis_update_dataframes,
)


# Helper function to create a base64-encoded gzip string
def create_gzip_base64_string(json_messages):
    json_block = "\n".join(json_messages)
    compressed_data = gzip.compress(json_block.encode("utf-8"))
    return base64.b64encode(compressed_data).decode("utf-8")


def test_valid_scpi_result():
    json_messages = ['{"key": "value"}', '{"key2": "value2"}']
    encoded_json_block = create_gzip_base64_string(json_messages)
    scpi_result = (
        f'2023-10-01 12:00:00",#{len(encoded_json_block)}{encoded_json_block}'
        f'2023-10-01 12:01:00",#{len(encoded_json_block)}{encoded_json_block}'
    )

    expected_result = [
        {
            "time": "2023-10-01 12:00:00",
            "json_messages": [{"key": "value"}, {"key2": "value2"}],
        },
        {
            "time": "2023-10-01 12:01:00",
            "json_messages": [{"key": "value"}, {"key2": "value2"}],
        },
    ]

    result = ipanalysis_parse_scpi_result(scpi_result)

    assert result == expected_result


def test_empty_scpi_result():
    scpi_result = ""

    result = ipanalysis_parse_scpi_result(scpi_result)

    assert result == []  # Should return an empty list


def test_no_json_messages():
    scpi_result = (
        '2023-10-01 12:00:00",#123\n'
        "\n"  # No JSON message
        '2023-10-01 12:01:00",#456\n'
        "\n"  # No JSON message
    )

    expected_result = [
        {"time": "2023-10-01 12:00:00", "json_messages": []},
        {"time": "2023-10-01 12:01:00", "json_messages": []},
    ]

    result = ipanalysis_parse_scpi_result(scpi_result)

    assert result == expected_result


def test_malformed_scpi_result():
    json_messages = [
        '{"key": "value"',
        '{"key2": "value2"}',
    ]  # First message is malformed
    encoded_json_block = create_gzip_base64_string(json_messages)
    scpi_result = f'2023-10-01 12:00:00",#{len(encoded_json_block)}{encoded_json_block}'

    expected_result = [
        {
            "time": "2023-10-01 12:00:00",
            "json_messages": [{"key2": "value2"}],
        }  # Only the second message is valid
    ]

    result = ipanalysis_parse_scpi_result(scpi_result)

    assert result == expected_result


def test_valid_json_messages():
    time = "2023-10-01T12:00:00Z"
    json_messages = [
        json.dumps({"key1": "value1"}),
        json.dumps({"key2": "value2"}),
    ]
    encoded_json_block = base64.b64encode(
        gzip.compress("\n".join(json_messages).encode("utf-8"))
    ).decode("utf-8")

    result = ipanalysis_parse_json_result(time, encoded_json_block)

    assert result["time"] == time
    assert len(result["json_messages"]) == 2
    assert result["json_messages"][0] == {"key1": "value1"}
    assert result["json_messages"][1] == {"key2": "value2"}


def test_schema_not_found():
    schema_result = "This string does not contain a schema."

    result = ipanalysis_parse_scpi_schema_result(schema_result)

    assert result is None  # Should return None when schema is not found


def test_invalid_json_schema():
    schema_result = (
        '{"$schema": "http://json-schema.org/draft-07/schema#",'
        '"type": "object",'
        '"properties": {'
        '"key1": {"type": "string"},'
        '"key2": {"type": "integer",}  # Trailing comma will cause JSONDecodeError'
        "}}"
    )

    result = ipanalysis_parse_scpi_schema_result(schema_result)

    assert result is None  # Should return None due to JSON decoding error


def test_empty_string():
    schema_result = ""

    result = ipanalysis_parse_scpi_schema_result(schema_result)

    assert result is None  # Should return None for an empty string


def test_unexpected_error():
    # To simulate an unexpected error, we can modify the function to raise an exception
    # For this test, we will mock the function to raise an exception
    from unittest.mock import patch

    with patch(
        "rs_mrt_dau_utilities.ip_analysis.ip_analysis.json.loads",
        side_effect=Exception("Unexpected error"),
    ):
        schema_result = '{"$schema": "http://json-schema.org/draft-07/schema#"}'

        result = ipanalysis_parse_scpi_schema_result(schema_result)

        assert result is None  # Should return None due to unexpected error


def test_empty_json_block():
    time = "2023-10-01T12:00:00Z"
    encoded_json_block = base64.b64encode(gzip.compress("".encode("utf-8"))).decode(
        "utf-8"
    )

    result = ipanalysis_parse_json_result(time, encoded_json_block)

    assert result["time"] == time
    assert result["json_messages"] == []  # Should return an empty list


def test_mixed_valid_and_invalid_json():
    time = "2023-10-01T12:00:00Z"
    json_messages = [
        json.dumps({"key1": "value1"}),
        "invalid_json_message",  # Invalid JSON
        json.dumps({"key2": "value2"}),
    ]
    encoded_json_block = base64.b64encode(
        gzip.compress("\n".join(json_messages).encode("utf-8"))
    ).decode("utf-8")

    result = ipanalysis_parse_json_result(time, encoded_json_block)

    assert result["time"] == time
    assert len(result["json_messages"]) == 2  # Two valid messages should be parsed
    assert result["json_messages"][0] == {"key1": "value1"}
    assert result["json_messages"][1] == {"key2": "value2"}


# Configure logging to capture log messages during tests
logging.basicConfig(level=logging.DEBUG)


def test_valid_schema():
    schema_result = (
        '{"$schema": "http://json-schema.org/draft-07/schema#",'
        '"type": "object",'
        '"properties": {'
        '"key1": {"type": "string"},'
        '"key2": {"type": "integer"}'
        "}}"
    )

    expected_schema = {
        "$schema": "http://json-schema.org/draft-07/schema#",
        "type": "object",
        "properties": {"key1": {"type": "string"}, "key2": {"type": "integer"}},
    }

    result = ipanalysis_parse_scpi_schema_result(schema_result)

    assert result == expected_schema


def test_ipanalysis_init_dataframes():
    # Call the function to test
    result = ipanalysis_init_dataframes()

    # Check that the result is a dictionary
    assert isinstance(result, dict)

    # Check that the dictionary has the correct keys
    expected_keys = [
        "flow_started",
        "report",
        "upd_classification",
        "upd_network",
        "upd_fqdn",
        "upd_ssl",
        "flow_closed",
    ]
    assert set(result.keys()) == set(expected_keys)

    # Check that each DataFrame is empty
    for key in expected_keys:
        assert result[key].is_empty()


def test_ipanalysis_update_dataframes_flow_started():
    # Initialize the DataFrames using the ipanalysis_init_dataframes function
    list_of_dfs = ipanalysis_init_dataframes()

    message_flow_started = {
        "FLOW_STARTED": {
            "time": {"secs": 1633072801, "nanos": 123456789},
            "flow_id": "1",
            "source": {
                "ip": "fe80::1440:6386:5713:932c",
                "flags": {
                    "V6": {
                        "is_loopback": "false",
                        "is_unspecified": "false",
                        "is_multicast": "false",
                    }
                },
                "geo": "null",
                "port": 0,
            },
            "destination": {
                "ip": "ff02::16",
                "flags": {
                    "V6": {
                        "is_loopback": "false",
                        "is_unspecified": "false",
                        "is_multicast": "true",
                    }
                },
                "geo": "null",
                "port": 0,
            },
        }
    }

    # Update the DataFrames with the FLOW_STARTED message
    list_of_dfs = ipanalysis_update_dataframes(list_of_dfs, message_flow_started)

    assert list_of_dfs["flow_started"].height == 1
    assert list_of_dfs["flow_started"].width == 14

    # add a second flow_started message (updating the time and flow_id and adding a geo field)
    message_flow_started["FLOW_STARTED"]["time"] = {
        "secs": 1633072802,
        "nanos": 987654321,
    }
    message_flow_started["FLOW_STARTED"]["flow_id"] = "2"
    message_flow_started["FLOW_STARTED"]["source"]["geo"] = {"country": "US"}
    list_of_dfs = ipanalysis_update_dataframes(list_of_dfs, message_flow_started)

    assert list_of_dfs["flow_started"].height == 2
    assert list_of_dfs["flow_started"].width == 15


# def test_report_message(setup_dataframes):
#    message = {
#        "REPORT": {
#            "time": {"secs": 1633072800, "nanos": 123456789},
#            "flows_stat": [
#                {"flow_id": "1", "stat": "active"},
#            #    {"flow_id": "2", "stat": "inactive"},
#            ],
#        }
#    }
#    updated_dfs = ipanalysis_update_dataframes(setup_dataframes, message)

# Check if the report DataFrame is updated correctly
# expected_times = [1633072800123456789, 1633072800123456789]
# expected_flow_ids = ["1", "2"]
# expected_stats = ["active", "inactive"]
#    expected_times = [1633072800123456789]
#    expected_flow_ids = ["1"]
#    expected_stats = ["active"]

#    report_df = updated_dfs["report"]
# assert report_df.shape[0] == 2  # Check number of rows
#    assert report_df.shape[0] == 1  # Check number of rows
# assert report_df["time"].to_list() == expected_times  # Check times
#    assert report_df["flow_id"].to_list() == expected_flow_ids  # Check flow_ids
#    assert report_df["stat"].to_list() == expected_stats  # Check stats
#
# def test_flow_started_message(setup_dataframes):
#    message = {
#        "FLOW_STARTED": {
#            "time": {"secs": 1633072800, "nanos": 987654321},
#            "time": 1,
#            "flow_id": "3",
#        }
#    }
#    updated_dfs = ipanalysis_update_dataframes(setup_dataframes, message)

# Check if the flow_started DataFrame is updated correctly
#    expected_time = 1633072800987654321
#    expected_time = 1
#    expected_flow_id = "3"

#    flow_started_df = updated_dfs["flow_started"]

#    assert flow_started_df.shape[0] == 1  # Check number of rows
#    assert flow_started_df["time"][0] == expected_time  # Check time
#    assert flow_started_df["flow_id"][0] == expected_flow_id  # Check flow_id

# Additional tests for other message types can be added here...

if __name__ == "__main__":
    pytest.main()
