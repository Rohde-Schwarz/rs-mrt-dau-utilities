.. _Ipanalysis:

Ipanalysis module Guide
=======================

Introduction
------------
The `ip_analysis` module provides utilities to parse and analyze IP analysis measurement results from Rohde & Schwarz Data Application Unit (DAU) instruments.
It helps convert SCPI results into structured Polars dataframes for easier analysis and visualization.

The `Polars <https://pola.rs>`_ library is used for efficient data manipulation and analysis.

This consist in two steps:

- Parsing the SCPI Results (FETCH command)
- Creating and updating Polars Dataframes from the parsed results

Parsing the SCPI Results
------------------------
Parsing the SCPI Results is a complex task because the results consist of a succession of:

- string containing the UTC timestamp of the measurement
- blob of base64 block
  
this base64 block is a gzip compressed succession of JSON messages.

.. code-block:: python

    from RsInstrument import *
    import rs_mrt_dau_utilities.ip_analysis as ipana

    ip_analysis_res=cmx.query('FETCh:DATA:MEASurement:IPANalysis:RESult?')
    parsed_sequences = ipana.ipanalysis_parse_scpi_result(ip_analysis_res)

The parsed_sequences is a list of sequences, each sequence being a dictionary with the following structure:

.. code-block:: python

    [
        {
            'time': '2024-06-01T12:00:00Z',
            'json_messages': [
                { ... },  # First JSON message as a dictionary
                { ... },  # Second JSON message as a dictionary
                ...
            ]
        },
        ...
    ]


Creating and updating Polars Dataframes
---------------------------------------
With the parsed results, we can create and update Polars Dataframes.

.. code-block:: python

    list_of_dfs = ipana.ipanalysis_init_dataframes()

    for sequence in parsed_sequences:
       for message in sequence['json_messages']:
             ipana.ipanalysis_update_dataframes(list_of_dfs, message)

    print(list_of_dfs)

we iterates over each sequence and each JSON message in the sequence to update the dataframes accordingly.
The resulting list_of_dfs contains multiple Polars dataframes with structured data from the IP analysis measurements.
Each dataframe corresponds to a different message produced by the DAU during the IP analysis measurement.

The init function creates empty dataframes for each message type, and the update function populates these dataframes with data extracted from the JSON messages.

Note: The update function does not check for duplicate entries. If the same JSON message is processed multiple times, the corresponding data will be duplicated in the dataframe.
Storing and checking the time field in the sequence can help avoid processing duplicates.

.. toctree::
   :maxdepth: 2
