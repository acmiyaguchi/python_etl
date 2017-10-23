import logging
import click

from pyspark.sql import SparkSession, functions as F

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def extract(spark, bucket, prefix):
    path = "s3://{}/{}".format(bucket, prefix)
    logger.info("Extracting data from {}".format(path))
    return spark.read.parquet(path)


def transform(df, date, sample_id):
    """Count the number of pings from each channel."""
    aggregate = (
        df
        .where(F.col("submission_date_s3") == date)
        .where(F.col("sample_id") == sample_id)
        .groupBy("normalized_channel")
        .agg(F.count("*").alias("pings"))
    )

    return (
        aggregate
        .select(
            F.col("normalized_channel").alias("channel"),
            "pings"
        )
    )


def load(df, bucket, prefix, date):
    path = (
        "s3://{}/{}/submission_date={}"
        .format(bucket, prefix, date)
    )
    logger.info("Loading data into {}".format(path))

    df.write.parquet(path, mode="overwrite")


@click.command()
@click.option('--date', required=True)
@click.option('--sample-id', required=True)
@click.option('--bucket', required=True)
@click.option('--prefix', default='python_example/v1')
@click.option('--input-bucket', default='telemetry-parquet')
@click.option('--input-prefix', default='main_summary/v4')
def main(date, sample_id, bucket, prefix, input_bucket, input_prefix):

    spark = SparkSession.builder.getOrCreate()

    logger.info(
        "Starting Python Example for {} using sample-id {}"
        .format(date, sample_id)
    )

    main_summary = extract(spark, input_bucket, input_prefix)
    pings_per_channel = transform(main_summary, date, sample_id)
    load(pings_per_channel, bucket, prefix, date)
