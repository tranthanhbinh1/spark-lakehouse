import pandera.pyspark as pa
import pyspark.sql.types as T


class GreenTripsPanderaSchema(pa.DataFrameModel):
    vendor_id: T.LongType() = pa.Field(nullable=True, isin=[1, 2])
    pickup_ts: T.TimestampType() = pa.Field(nullable=False)
    dropoff_ts: T.TimestampType() = pa.Field(nullable=False)

    passenger_count: T.LongType() = pa.Field(nullable=False, ge=1, le=6)
    trip_distance: T.DoubleType() = pa.Field(nullable=False, ge=0)

    rate_code_id: T.LongType() = pa.Field(nullable=True)
    store_and_fwd_flag: T.StringType() = pa.Field(nullable=True, isin=["Y", "N"])
    pickup_location_id: T.LongType() = pa.Field(nullable=False, ge=1)
    dropoff_location_id: T.LongType() = pa.Field(nullable=False, ge=1)
    payment_type: T.LongType() = pa.Field(nullable=True)

    fare_amount: T.FloatType() = pa.Field(nullable=True)
    extra: T.FloatType() = pa.Field(nullable=True)
    mta_tax: T.FloatType() = pa.Field(nullable=True)
    tip_amount: T.FloatType() = pa.Field(nullable=True, ge=0)
    tolls_amount: T.FloatType() = pa.Field(nullable=True, ge=0)
    ehail_fee: T.DoubleType() = pa.Field(nullable=True)
    improvement_surcharge: T.FloatType() = pa.Field(nullable=True)
    total_amount: T.FloatType() = pa.Field(nullable=True)
    congestion_surcharge: T.DoubleType() = pa.Field(nullable=True)
    trip_type: T.IntegerType() = pa.Field(nullable=True)

    trip_duration_min: T.LongType() = pa.Field(nullable=False, gt=0)
    year: T.IntegerType() = pa.Field(nullable=False, ge=2014)
    month: T.IntegerType() = pa.Field(nullable=False, ge=1, le=12)

    is_valid_trip: T.BooleanType() = pa.Field(nullable=False)
    has_tip: T.BooleanType() = pa.Field(nullable=False)
    tip_ratio: T.DoubleType() = pa.Field(nullable=True, ge=0)
