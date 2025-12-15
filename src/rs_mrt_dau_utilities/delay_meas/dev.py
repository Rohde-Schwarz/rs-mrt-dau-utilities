import base64
import datetime
import gzip
import json
import re

import polars as pl


def delay_parse_log(log_file: str) -> dict[str, pl.DataFrame]:
    """
    Parse the centralservice.log file and return a dictionary containing 2 dataframes:
    - hash: DataFrame containing the hash data
    - command: DataFrame containing the command data
    """
    with open(log_file, "r") as f:
        lines = f.readlines()

    fl: dict[str, list] = {"hash": [], "command": []}
    for line in lines:
        # Extract the relevant information from the log line
        match_hash = re.search(
            r"(.*) INFO centralservice::delay_meas_core: mime=.*, data=(.*)", line
        )
        if match_hash:
            timestamp = match_hash.group(1)
            encoded = match_hash.group(2)
            decoded = base64.b64decode(encoded)
            decompress = gzip.decompress(decoded)
            for line in decompress.decode("utf-8").splitlines():
                data = json.loads(line)
                for i in data["meas"]:
                    # Add the two timestamps together
                    i["timestamp"] = (
                        i["timestamp"]["secs"] * 1000000000 + i["timestamp"]["nanos"]
                    )
                    i["hash"] = data["hash"]
                    fl["hash"].append(i)
        match_cmd = re.search(
            r"(.*)  INFO centralservice::delay_meas_core: (.*) msg from FSW received",
            line,
        )
        if match_cmd:
            timestamp = match_cmd.group(1)
            cmd = match_cmd.group(2)
            # config = match_cmd.group(3)
            json_dict = {
                "timestamp": datetime.datetime.fromisoformat(timestamp),
                "command": cmd,
            }

            fl["command"].append(json_dict)
    # logging.d(fl["command"])

    # print("hash:", fl["hash"])
    # Create a DataFrame for the hash data and cast the 'hash' column to UInt64
    df = pl.DataFrame(fl["hash"], infer_schema_length=None).cast({"hash": pl.UInt64})

    # convert the timestamp to datetime with the correct timezone
    df = df.with_columns(
        timestamp=pl.from_epoch("timestamp", time_unit="ns").dt.replace_time_zone("UTC")
    )

    return {"hash": df, "command": pl.DataFrame(fl["command"])}


def delay_get_start_stop_segment(
    command_df: pl.DataFrame, hash_df: pl.DataFrame
) -> list[pl.DataFrame]:
    """ """
    # search for the segments start - stop
    start_time = None
    result = []

    # Iterate through the DataFrame rows
    for row in command_df.iter_rows(named=True):
        if row["command"] == "Start":
            start_time = row["timestamp"]
        elif row["command"] == "Stop" and start_time is not None:
            # Append the pair of start and stop times to the result list
            result.append({"Start": start_time, "Stop": row["timestamp"]})
            # Reset start_time to None after pairing
            start_time = None

    # Create a new DataFrame from the result list
    paired_df = pl.DataFrame(result)
    # print("paired_df:", paired_df)
    results_per_segment = []
    # Iterate through the DataFrame rows
    for row in paired_df.iter_rows(named=True):
        start_time = row["Start"]
        stop_time = row["Stop"]
        # Filter the hash DataFrame for the current segment
        filtered_hash_df = hash_df.filter(
            pl.col("timestamp").is_between(start_time, stop_time)
        )
        # Append the filtered DataFrame to the result list
        results_per_segment.append(filtered_hash_df)

    return results_per_segment


def delay_get_segment(
    result_per_segment: list[pl.DataFrame], all_paths: bool = False
) -> dict[str, pl.DataFrame]:
    """ """
    result_per_hash = {}
    segment = 1  # first segment (start-stop) begins with 1
    # group the hash together
    for m in result_per_segment:
        # Group by 'hash'
        # only keep the groups that have 'Upc' in the 'origin' column
        # filtered_groups=m.sort("timestamp").group_by('hash').all().filter(pl.col('origin').list.contains('Upc')) #.head(10)

        # print(filtered_groups)
        # getting the number of unique meas_id
        list_of_meas = pl.Series(
            m.select(pl.col("meas_id").drop_nulls().unique())
        ).to_list()
        # print("list of meas: ", list_of_meas)

        for meas in list_of_meas:
            aaa = (
                m.lazy()
                .filter(pl.col("meas_id").is_in([meas]) | pl.col("meas_id").is_null())
                .sort("timestamp")
                .group_by("hash")
                .all()
                .filter(pl.col("origin").list.contains("Upc"))
                .explode(
                    "timestamp", "origin", "meas_id"
                )  # .group_by('hash','origin').all()
                .sort("timestamp", "hash", "origin")
                .with_columns(idx=pl.col("hash").rank("ordinal").over("hash", "origin"))
                .collect()
            )
            eee3 = aaa.pivot(index="hash", on=["origin", "idx"], values="timestamp")
            iii = (
                eee3.select(pl.all().exclude("hash"))
                .rename(lambda cn: cn[2:-1].replace('",', "_"))
                .insert_column(0, eee3["hash"])
            )
            # # remove the group who are not meas or n/a
            # fil_only_one_meas = m.filter(pl.col("meas_id").is_in([meas]) | pl.col("meas_id").is_null())
            # #print("fil_only_one_meas:", fil_only_one_meas)
            # # add a number at the end of every origin, sorted by timestamp
            # ddd = fil_only_one_meas.sort("timestamp").group_by('hash').all().filter(pl.col('origin').list.contains('Upc'))
            # eee = ddd.explode("timestamp","origin","meas_id") #.group_by('hash','origin').all()
            # #print(eee)
            # eee2 = eee.sort('timestamp','hash','origin').with_columns(
            #     idx = pl.col('hash').rank("ordinal").over('hash', 'origin')
            #     #pl.int_range(pl.len()).over(pl.col('origin'))
            # )
            # #print(eee2)
            # eee3 = eee2.pivot(index="hash", on=["origin", "idx"], values="timestamp")
            # # rename the columns
            # iii = eee3.select(pl.all().exclude("hash")).rename(
            #     lambda cn: cn[2:-1].replace("\",", "_")
            # ).insert_column(0, eee3["hash"])
            # print(iii)
            column_present = iii.columns
            column_present = [x for x in column_present if x != "hash"]
            # print(column_present)
            iii = iii.with_columns(
                delay_global_us=(
                    pl.col(column_present[-1]) - pl.col(column_present[0])
                ).dt.total_microseconds()
            )

            if all_paths:
                last_column = ""
                for i in column_present:
                    if last_column == "":
                        last_column = i
                    else:
                        iii = iii.with_columns(
                            (pl.col(i) - pl.col(last_column))
                            .alias("delay-" + last_column + "->" + i + "_us")
                            .dt.total_microseconds()
                        )
                        last_column = i
            result_per_hash[str(segment) + "_" + str(meas)] = iii
        segment += 1
    return result_per_hash
