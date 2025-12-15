import base64
import gzip
import json
import logging
import re

import fast_json_normalize
import polars as pl

# parse a SCPI result obtained with FETCh:DATA:MEASurement:IPANalysis:RESult?
# return a list of pattern: ['time', json_messages']
# example of use: Print the parsed sequences
# parsed_sequences = parse_scpi_result(scpi_result)
# for sequence in parsed_sequences:
#    print(f"Time: {sequence['time']}")
#    for message in sequence['json_messages']:
#        print(f"message: {message}")
#        #print(json.dumps(message, indent=2))
#    print()


def ipanalysis_parse_scpi_result(scpi_result: str) -> list[dict]:
    """
    Processes a given SCPI result string by splitting it into sequences based on a time pattern and SCPI block.

    Args:
        scpi_result (str): A string containing the SCPI result data.

    Returns:
        list: A list of dictionaries, each containing a time and a list of parsed JSON messages.
    """
    # Split the input into sequences based on the pattern: time, SCPI block
    sequences = re.split(r'(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})",', scpi_result)[1:]

    # Initialize a list to store the parsed sequences
    parsed_sequences = []

    # print(f"split len: {len(sequences)}")

    # Iterate over the sequences in pairs (time, SCPI block)
    for i in range(0, len(sequences), 2):
        time = sequences[i]
        scpi_block_with_len = sequences[i + 1]

        # remove the length of the block data
        scpi_block = re.split(r"#(\d+)", scpi_block_with_len)[2]

        # print(f"time_json_messages: {time_json_messages}")
        time_json_messages = ipanalysis_parse_json_result(time, scpi_block)

        # Store the time and parsed JSON messages in the result list
        parsed_sequences.append(time_json_messages)

    return parsed_sequences


def ipanalysis_parse_json_result(time: str, encoded_json_block: str) -> dict:
    """
    Processes a base64 block:
    - obtain the binary gzip string
    - decompress the gzip string
    - process the JSON message string by splitting it into individual JSON messages and parsing each message.

    Args:
        time (str): A string representing the time associated with the JSON messages.
        encoded_json_block (str): A base64 block who is a gzip string containing the JSON messages, separated by newline characters.

    Returns:
        dict: A dictionary containing the time and a list of parsed JSON messages.
    """
    decoded_scpi_block = base64.b64decode(encoded_json_block)
    decompressed_data = gzip.decompress(decoded_scpi_block)
    # Split the SCPI block into individual JSON messages
    json_block = decompressed_data.decode("utf-8")

    json_messages = json_block.strip().split("\n")
    # Parse each JSON message
    parsed_json_messages = []
    for message in json_messages:
        try:
            parsed_json_messages.append(json.loads(message))
        except json.JSONDecodeError:
            print(f"\njson.JSONDecodeError: {message}")
            continue

    # Store the time and parsed JSON messages in the result
    return {"time": time, "json_messages": parsed_json_messages}


def ipanalysis_parse_scpi_schema_result(schema_result: str) -> dict | None:
    """
    Parses the SCPI schema result string and extracts the JSON schema.

    Args:
        schema_result (str): A string containing the SCPI schema result.

    Returns:
        dict: A dictionary representing the parsed JSON schema, or None if the schema is not found or if there is an error in parsing.
    """
    try:
        # Find the index of '{"$schema"'
        start_index = schema_result.find('{"$schema"')
        if start_index != -1:
            # Create a new string starting from '{"$schema"'
            json_schema_str = schema_result[start_index:].strip()
            json_schema = json.loads(json_schema_str)
            return json_schema
        else:
            logging.warning(
                'The keyword {"$schema"} was not found in the input string.'
            )
            return None
    except json.JSONDecodeError as e:
        logging.error(f"Error decoding JSON schema: {e}")
        return None
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")
        return None


def ipanalysis_init_dataframes() -> dict[str, pl.DataFrame]:
    """
    Initializes and returns a dictionary of empty Polars DataFrames for IP analysis.

    Returns:
        dict: A dictionary containing empty DataFrames for various categories.
    """
    return {
        "flow_started": pl.DataFrame(),
        "report": pl.DataFrame(),
        "upd_classification": pl.DataFrame(),
        "upd_network": pl.DataFrame(),
        "upd_fqdn": pl.DataFrame(),
        "flow_closed": pl.DataFrame(),
    }


def ipanalysis_update_dataframes(
    list_of_dfs: dict[str, pl.DataFrame], message: dict
) -> dict[str, pl.DataFrame]:
    """
    Updates the dictionary of Polars DataFrames based on the contents of a given message.

    Args:
        list_of_dfs (dict): A dictionary containing Polars DataFrames for various categories (ipanalysis_init_dataframes may be used to get the initial values).
        message (dict): A dictionary containing the message data to be processed.

    Returns:
        dict: The updated dictionary of Polars DataFrames.
    """
    data = message
    msgs = []
    # default
    key = "report"
    # test for a REPORT
    if "REPORT" in data:
        for i in data["REPORT"]["flows_stat"]:
            i["time"] = (
                data["REPORT"]["time"]["secs"] * 1000000000
                + data["REPORT"]["time"]["nanos"]
            )
            msgs.append(i)
    # test for a FLOW_STARTED
    elif "FLOW_STARTED" in data:
        msgs = [data["FLOW_STARTED"]]
        key = "flow_started"
    # test for a CLASSIFICATION
    elif "UPDATE_CLASSIFICATION" in data:
        msgs = [data["UPDATE_CLASSIFICATION"]]
        key = "upd_classification"
    # test for a NETWORK
    elif "UPDATE_NETWORK" in data:
        msgs = [data["UPDATE_NETWORK"]]
        key = "upd_network"
    # test for a FQDN
    elif "UPDATE_FQDN" in data:
        msgs = [data["UPDATE_FQDN"]]
        key = "upd_fqdn"
    # test for a FLOW_CLOSED
    elif "FLOW_CLOSED" in data:
        msgs = [data["FLOW_CLOSED"]]
        key = "flow_closed"

    # normalize the data
    for i in msgs:
        # test if 'time' key has not been replaced
        if isinstance(i["time"], dict):
            i["time"] = i["time"]["secs"] * 1000000000 + i["time"]["nanos"]
        msg_df = fast_json_normalize.fast_json_normalize(
            i,
            separator="_",
            to_pandas=False,
            order_to_pandas=False,
        )
        # convert the time to datetime with the correct timezone
        msg_df = pl.DataFrame(msg_df).with_columns(
            time=pl.from_epoch("time", time_unit="ns").dt.replace_time_zone("UTC")
        )
        list_of_dfs[key] = pl.concat([list_of_dfs[key], msg_df], how="diagonal")

    return list_of_dfs
