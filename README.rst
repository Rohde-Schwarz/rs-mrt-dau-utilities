=====================
 rs-mrt-dau-utilities
=====================

.. image:: https://img.shields.io/pypi/v/rs-mrt-dau-utilities.svg
   :target: https://pypi.org/project/rs-mrt-dau-utilities/

.. image:: https://readthedocs.org/projects/sphinx/badge/?version=master
   :target: https://rs-mrt-dau-utilities.readthedocs.io/

.. image:: https://img.shields.io/pypi/l/rs-mrt-dau-utilities.svg
   :target: https://pypi.python.org/pypi/rs-mrt-dau-utilities/

.. image:: https://img.shields.io/pypi/dm/rs-mrt-dau-utilities.svg
   :target: https://pypi.python.org/pypi/rs-mrt-dau-utilities/

rs-mrt-dau-utilities package provides two convenient modules for Rohde & Schwarz Data Application Unit (DAU):

* `ip_analysis` module for creating Polars dataframes from the SCPI results.
* `delay` module for creating Polars dataframes from the centralservice.log file.

ip_analysis code:

.. code-block:: python

   from RsInstrument import *
   import rs_mrt_dau_utilities.ip_analysis as ipana

   cmx = RsInstrument('TCPIP::10.102.20.55::hislip0')
   iden = cmx.query("*IDN?")
   print(iden)
   ip_analysis_res=cmx.query('FETCh:DATA:MEASurement:IPANalysis:RESult?')
   parsed_sequences = ipana.ipanalysis_parse_scpi_result(ip_analysis_res)

   list_of_dfs = ipana.ipanalysis_init_dataframes()

   for sequence in parsed_sequences:
      for message in sequence['json_messages']:
            ipana.ipanalysis_update_dataframes(list_of_dfs, message)

   print(list_of_dfs)

delay code:

.. code-block:: python

   import rs_mrt_dau_utilities.delay as delay

   log_file_path = 'centralservice.log'
   delay_df = delay.extract_delay_from_log(log_file_path)

   print(delay_df)

Installation
------------
You can install the package via pip:

.. code-block:: bash

   pip install rs-mrt-dau-utilities

Check out the full documentation on `ReadTheDocs <https://rs-mrt-dau-utilities.readthedocs.io/>`_.

Our public `Rohde&Schwarz Github repository <https://github.com/Rohde-Schwarz/Examples/tree/main/Misc/Python/rs-mrt-dau-utilities>`_ hosts many examples using this library.
If you're looking for examples with specific instruments, check out the ones for
`Oscilloscopes <https://github.com/Rohde-Schwarz/Examples/tree/main/Oscilloscopes/Python/RsInstrument>`_,
`Powersensors <https://github.com/Rohde-Schwarz/Examples/tree/main/Powersensors/Python/RsInstrument>`_,
`Powersupplies <https://github.com/Rohde-Schwarz/Examples/tree/main/Powersupplies/Python/RsInstrument>`_,
`Spectrum Analyzers <https://github.com/Rohde-Schwarz/Examples/tree/main/SpectrumAnalyzers/Python/RsInstrument>`_,
`Vector Network Analyzers <https://github.com/Rohde-Schwarz/Examples/tree/main/VectorNetworkAnalyzers/Python/RsInstrument>`_.


Version history:
----------------

    Version 0.2.0 (14.11.2025)
        - initial release.
