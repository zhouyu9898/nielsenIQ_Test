import os
import sys
import pandas as pd
import numpy as np
import json
from argparse import ArgumentParser


def avg_price_per_mile(data_df):
    """Returns average price per mile and number of rows of a given dataframe. It must contain 'total_amount' and 'trip_distance' columns"""
    price_per_mile_ser = data_df['total_amount'] / data_df['trip_distance']
    price_per_mile_ser.replace([np.inf, -np.inf], np.nan, inplace=True)
    price_per_mile_ser.dropna(inplace=True)

    return price_per_mile_ser.mean(), price_per_mile_ser.count()


def payment_distribution(data_df):
    """Returns the payment distribution of a given dataframe. It must contain 'payment_type' column"""
    distribution_ser = data_df.groupby('payment_type').count()[data_df.columns[0]]
    distribution_ser.index = distribution_ser.index.astype('str')

    return distribution_ser


def custom_indicator(data_df):
    """Returns a custom indicator ((tip_amount + extra)/trip_distance) of a given dataframe. It must contain 'tip_amount',  'extra' and 'trip_distance' columns"""
    custom_indicator_ser = (data_df['tip_amount'] + data_df['extra'])/data_df['trip_distance']
    custom_indicator_ser.replace([np.inf, -np.inf], np.nan, inplace=True)
    custom_indicator_ser.dropna(inplace=True)

    return custom_indicator_ser


if __name__ == "__main__":
    #################### READ AND PARSE ARGS ####################
    parser = ArgumentParser()
    parser.add_argument("-d", "--date", dest="date", help="yellow tripdata file date", metavar="YYYYMMDD")
    if len(sys.argv) < 2:
        parser.print_usage()
        sys.exit(1)
    args = parser.parse_args()

    year, month, day = (args.date[:4], args.date[4:6], args.date[6:])
    ymd = year+month+day

    #################### OPEN YELLOW TRIPDATA FILE ####################
    try:
        yellow_tripdata_filename = 'yellow_tripdata_{year}-{month}-{day}.parquet'.format(
            year=year, month=month, day=day
        )
        data_df = pd.read_parquet(yellow_tripdata_filename)
    except Exception as e: 
        print(e)
        parser.print_usage()
        sys.exit(1)

    #################### READ PREVIOUS JSON FILES IF ANY ####################
    curr_path = os.path.dirname(os.path.realpath(__file__))
    prev_avg = prev_count = prev_distribution = prev_custom_filename = None
    for f_name in os.listdir(curr_path):
        if f_name.endswith('avg.json'):
            with open(f_name) as f:
                d = json.load(f)
                prev_avg, prev_count = d['avg'], d['count']
            os.remove(f.name)

        if f_name.endswith('distrib.json'):
            with open(f_name) as f:
                d = json.load(f)
                prev_distribution = pd.Series(d)
            os.remove(f.name)
        
        if f_name.endswith('custom.json'):
            prev_custom_filename = f_name

    #################### UPDATE WITH NEW CHUNK ####################
    curr_avg, curr_count = avg_price_per_mile(data_df)
    if prev_avg and prev_count:
        new_count = prev_count + curr_count
        new_avg = (curr_avg * curr_count + prev_avg * prev_count) / new_count
    else:
        new_avg = curr_avg
        new_count = curr_count

    curr_distribution = payment_distribution(data_df)
    if isinstance(prev_distribution, pd.Series):
        new_distribution = prev_distribution.add(curr_distribution)
    else:
        new_distribution = curr_distribution

    curr_custom = custom_indicator(data_df)

    #################### WRITE NEW UPDATED JSON DATA ####################
    with open(
        '{}_yellow_taxi_avg.json'.format(ymd),
        'w', encoding='utf-8'
    ) as f:
        json.dump({'avg':new_avg, 'count':int(new_count)}, f, ensure_ascii=False, indent=4)

    with open(
        '{}_yellow_taxi_distrib.json'.format(ymd),
        'w', encoding='utf-8'
    ) as f:
        json.dump(new_distribution.to_dict(), f, ensure_ascii=False, indent=4)

    if prev_custom_filename:
        with open(
            prev_custom_filename,
            'a', encoding='utf-8'
        ) as f:
            json.dump({ymd: curr_custom.to_dict()}, f, ensure_ascii=False, indent=4)
        os.rename(prev_custom_filename, '{}_yellow_taxi_custom.json'.format(ymd))
    else:
        with open(
            '{}_yellow_taxi_custom.json'.format(ymd),
            'w', encoding='utf-8'
        ) as f:
            json.dump({ymd: curr_custom.to_dict()}, f, ensure_ascii=False, indent=4)


    sys.exit(0)