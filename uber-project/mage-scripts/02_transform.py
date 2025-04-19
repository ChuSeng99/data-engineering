import pandas as pd
if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(df, *args, **kwargs):
    """
    Template code for a transformer block.

    Add more parameters to this function if this block has multiple parent blocks.
    There should be one parameter for each output variable from each parent block.

    Args:
        data: The output from the upstream parent block
        args: The output from any additional upstream blocks (if applicable)

    Returns:
        Anything (e.g. data frame, dictionary, array, int, str, etc.)
    """
    # Specify your transformation logic here
    df = (
        df
        .astype({
            'tpep_pickup_datetime': 'datetime64[s]',
            'tpep_dropoff_datetime': 'datetime64[s]',
        })
        .drop_duplicates()
        .reset_index(drop=True)
        .assign(trip_id=df.index)
    )

    datetime_dim = (
        df[['tpep_pickup_datetime', 'tpep_dropoff_datetime']]
        .reset_index(drop=True)
        .assign(
            pick_hour=df['tpep_pickup_datetime'].dt.hour,
            pick_day=df['tpep_pickup_datetime'].dt.day,
            pick_month=df['tpep_pickup_datetime'].dt.month,
            pick_year=df['tpep_pickup_datetime'].dt.year,
            pick_weekday=df['tpep_pickup_datetime'].dt.weekday,

            drop_hour=df['tpep_dropoff_datetime'].dt.hour,
            drop_day=df['tpep_dropoff_datetime'].dt.day,
            drop_month=df['tpep_dropoff_datetime'].dt.month,
            drop_year=df['tpep_dropoff_datetime'].dt.year,
            drop_weekday=df['tpep_dropoff_datetime'].dt.weekday,

            datetime_id=df.index
        )
        .loc[:, ['datetime_id', 'tpep_pickup_datetime', 'pick_hour', 'pick_day', 'pick_month', 'pick_year', 'pick_weekday', 'tpep_dropoff_datetime', 'drop_hour', 'drop_day', 'drop_month', 'drop_year', 'drop_weekday']]
    )

    passenger_count_dim = (
        df[['passenger_count']]
        .reset_index(drop=True)
        .assign(passenger_count_id=df.index)
        .loc[:, ['passenger_count_id', 'passenger_count']]
    )

    trip_distance_dim = (
        df[['trip_distance']]
        .reset_index(drop=True)
        .assign(trip_distance_id=df.index)
        .loc[:, ['trip_distance_id', 'trip_distance']]
    )

    rate_code_dim = (
        df[['RatecodeID']]
        .reset_index(drop=True)
        .assign(
            rate_code_id=df.index,
            rate_code_name=df['RatecodeID'].map({
                1:"Standard rate",
                2:"JFK",
                3:"Newark",
                4:"Nassau or Westchester",
                5:"Negotiated fare",
                6:"Group ride"
            })
        )
        .loc[:, ['rate_code_id', 'RatecodeID', 'rate_code_name']]
    )

    payment_type_dim = (
        df[['payment_type']]
        .reset_index(drop=True)
        .assign(
            payment_type_id=df.index,
            payment_type_name=df['payment_type'].map({
                1:"Credit card",
                2:"Cash",
                3:"No charge",
                4:"Dispute",
                5:"Unknown",
                6:"Voided trip"
            })
        )
        .loc[:, ['payment_type_id', 'payment_type', 'payment_type_name']]
    )

    pickup_location_dim = (
        df[['pickup_longitude', 'pickup_latitude']]
        .reset_index(drop=True)
        .assign(pickup_location_id=df.index)
        .loc[:, ['pickup_location_id', 'pickup_longitude', 'pickup_latitude']]
    )

    dropoff_location_dim = (
        df[['dropoff_longitude', 'dropoff_latitude']]
        .reset_index(drop=True)
        .assign(dropoff_location_id=df.index)
        .loc[:, ['dropoff_location_id', 'dropoff_longitude', 'dropoff_latitude']]
    )

    fact_table = (df
        .merge(passenger_count_dim, left_on='trip_id', right_on='passenger_count_id')
        .merge(trip_distance_dim, left_on='trip_id', right_on='trip_distance_id')
        .merge(rate_code_dim, left_on='trip_id', right_on='rate_code_id') 
        .merge(pickup_location_dim, left_on='trip_id', right_on='pickup_location_id') 
        .merge(dropoff_location_dim, left_on='trip_id', right_on='dropoff_location_id')
        .merge(datetime_dim, left_on='trip_id', right_on='datetime_id') 
        .merge(payment_type_dim, left_on='trip_id', right_on='payment_type_id') 
        .loc[:, ['trip_id','VendorID', 'datetime_id', 'passenger_count_id', 'trip_distance_id', 'rate_code_id', 'store_and_fwd_flag', 'pickup_location_id', 'dropoff_location_id', 'payment_type_id', 'fare_amount', 'extra', 'mta_tax', 'tip_amount', 'tolls_amount','improvement_surcharge', 'total_amount']]
    )


    return {
        "datetime_dim": datetime_dim,
        "passenger_count_dim": passenger_count_dim,
        "trip_distance_dim": trip_distance_dim,
        "rate_code_dim": rate_code_dim,
        "pickup_location_dim": pickup_location_dim,
        "dropoff_location_dim": dropoff_location_dim,
        "payment_type_dim": payment_type_dim,
        "fact_table": fact_table,
    }



@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'