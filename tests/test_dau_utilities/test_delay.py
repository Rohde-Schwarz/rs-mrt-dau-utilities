import base64
import datetime
import gzip
import json
import tempfile

from rs_mrt_dau_utilities.delay_meas.delay_meas import extract_delay_from_log


def create_sample_log_file(content: str) -> str:
    """Helper function to create a temporary log file with the given content."""
    temp_file = tempfile.NamedTemporaryFile(delete=False)
    temp_file.write(content.encode("utf-8"))
    temp_file.close()
    return temp_file.name


def test_delay_parse_log_basic():
    # Create a sample log content
    timestamp1 = datetime.datetime.fromtimestamp(
        1633072798, tz=datetime.timezone.utc
    ).isoformat()
    timestamp2 = datetime.datetime.fromtimestamp(
        1633072800, tz=datetime.timezone.utc
    ).isoformat()
    timestamp3 = datetime.datetime.fromtimestamp(
        1633072805, tz=datetime.timezone.utc
    ).isoformat()
    meas_data = json.dumps(
        {
            "hash": 123456789,
            "meas": [
                {
                    "timestamp": {"secs": 1633072800, "nanos": 123456789},
                    "meas_id": "1",
                    "origin": "Upc",
                },
                {
                    "timestamp": {"secs": 1633072802, "nanos": 123456789},
                    # "meas_id": "1",
                    "origin": "Ims",
                },
                {
                    "timestamp": {"secs": 1633072801, "nanos": 123456789},
                    "meas_id": "2",
                    "origin": "Upc",
                },
                {
                    "timestamp": {"secs": 1633072803, "nanos": 123456789},
                    # "meas_id": "2",
                    "origin": "Ims",
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
    result = extract_delay_from_log(log_file)

    assert result.keys() == {"1_1", "1_2"}
    assert result["1_1"].shape == (1, 5)
    assert result["1_2"].shape == (1, 5)
    assert result["1_1"].columns == [
        "hash",
        "Upc_1",
        "Ims_1",
        "Ims_2",
        "delay_global_us",
    ]
    assert result["1_2"].columns == [
        "hash",
        "Upc_1",
        "Ims_1",
        "Ims_2",
        "delay_global_us",
    ]
