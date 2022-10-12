# nielsenIQ_Test
This task has been completed by Zhouyu Guo

This repository contains a script.py which processes the yellow taxi trips data and writes the metrics asked.

The dataset used in this test were the two latest datasets available in their <a href="doc:introduction" target="_blank">page</a> (May and June 2022). Download, place under the root directory and rename them to yellow_tripdata_YYYY-MM-DD.parquet (did this to simulate different chunks).

The script must be run with an argument. Run it with -h or no arguments to get info.

3 python libraries are requiered, they are under requirements.txt: pandas, numpy and pyarrow.

All 3 metrics are stored following the name convention described:
* YYYYMMDD_yellow_taxi_avg.json for the average price per mile traveled.
* YYYYMMDD_yellow_taxi_distrib.json for the payment distribution.
* YYYYMMDD_yellow_taxi_custom.json for the custom indicator.