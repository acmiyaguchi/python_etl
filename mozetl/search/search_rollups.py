"""
main_summary to Vertica Search Rollups

This job is added by Bug 1364530 and was originally located at
[1]. Search rollups are computed and then ingested by Vertica.

[1] https://gist.github.com/SamPenrose/856aa21191ef9f0de18c94220cd311a8
"""

import logging

import arrow
import click
from pyspark.sql import SparkSession, functions as F

from mozetl import utils
from mozetl.constants import SEARCH_SOURCE_WHITELIST

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def explode_searches(dataframe):
    """Expand the search_count list for each ping"""
    columns = [
        "profile",
        "country",
        "locale",
        "distribution_id",
        "default_provider",
    ]

    exploded_search_columns = [
        F.col("search_counts.engine").alias("search_provider"),
        F.col("search_counts.source").alias("search_source"),
        F.col("search_counts.count").alias("search_count")
    ]

    null_search_columns = [
        F.lit("NO_SEARCHES").alias("search_provider"),
        F.lit(0).alias("search_count"),
    ]

    # explode search counts and filter records outside of source whitelist
    exploded_searches = (
        dataframe
        .withColumn("search_counts", F.explode("search_counts"))
        .select(columns + exploded_search_columns)
        .where("search_count > -1 or search_count is null")
        .where(F.col("search_source").isNull() |
               F.col("search_source").isin(SEARCH_SOURCE_WHITELIST))
        .drop("search_source")
    )

    # `explode` loses null values, preserve these rows
    null_searches = (
        dataframe
        .where("search_counts is null")
        .select(columns + null_search_columns)
    )

    # return their union
    return exploded_searches.union(null_searches)


def rollup_searches(dataframe, attributes, mode):
    """ Gather all permutations of columns specified by the attributes arg:
    1) How many searches fall into each permutation? -> search_count
    2) How many unique profiles fall into each bucket -> profile_count
    3) What share of total profiles does this bucket represent? -> profile_share

    The distinction between profile_count and profile_share is
    necessary because on a given submission_date a single user of a
    profile may switch default search engine (or locale or geo), or a
    profile may be shared by multiple users on a day (causing all the
    other fields to vary if the users are on widely distributed
    machines).

    :dataframe DataFrame: slightly processed data
    :attributes list[str]: columns to group over
    :mode str: `daily` or `monthly`, used to remove `profile_shares`
    """
    metrics = {
        "search_count": F.sum("search_count").alias("search_count"),
        "profile": F.countDistinct("profile").alias("profile_count"),
    }
    if mode == "daily":
        metrics["profile_share"] = F.sum("profile_share").alias("profile_share")

    rollup = (
        dataframe
        .select(attributes + metrics.keys())
        .where("search_count > -1 OR search_count is null")
        .groupBy(attributes)
        .agg(*metrics.values())
    )
    return rollup


def transform(main_summary, mode):
    """ Group over attributes, and count the number of searches

    :main_summary DataFrame: source table
    :mode str: `daily` or `monthly`
    """

    # take a subset of the original dataframe
    columns = [
        F.col("client_id").alias("profile"),
        "country",
        "locale",
        "distribution_id",
        F.col("default_search_engine").alias("default_provider"),
        "search_counts",
    ]

    # attributes of the final search rollup
    attributes = [
        "country",
        "search_provider",
        "default_provider",
        "locale",
        "distribution_id",
    ]

    defaults = {
        "country": "XX",
        "search_provider": "NO_SEARCHES",
        "default_provider": "NO_DEFAULT",
        "locale": "xx",
        "distribution_id": "MOZILLA",
        "search_count": 0,
    }

    dataframe = main_summary.select(columns)
    exploded = explode_searches(dataframe).na.fill(defaults)

    processed = exploded.withColumn("profile_shares", F.lit(1.0))
    search_rollup = rollup_searches(processed, attributes, mode)

    return search_rollup


def format_spark_path(bucket, prefix):
    return "s3://{}/{}".format(bucket, prefix)


def get_date_range(ds_start, period):
    """ Return the start and end date given the start date and period. """
    DS_NODASH = "YYYYMMDD"

    date_start = arrow.get(ds_start, DS_NODASH)
    if period == "monthly":
        date_start = date_start.replace(day=1)
        date_end = date_start.replace(months=+1)
    else:
        date_end = date_start.replace(days=+1)

    ds_start = date_start.format(DS_NODASH)
    ds_end = date_end.format(DS_NODASH)

    return ds_start, ds_end


def extract(spark, path, ds_start, ds_end):
    """Extract the source dataframe from the spark compatible path.

    spark: SparkSession
    path: path to parquet files in s3
    ds_start: inclusive date
    """

    return (
        spark.read
        .option("mergeSchema", "true")
        .parquet(path)
        .where((F.col("submission_date_s3") >= ds_start) &
               (F.col("submission_date_s3") < ds_end))
    )


def save(dataframe, bucket, prefix, mode, version, start_ds):
    """Write dataframe to an s3 location and generate a manifest

    :dataframe DataFrame: rollup data
    :bucket str: s3 bucket
    :prefix str: s3 prefix
    :mode str: either `daily` or `monthly`
    :version int: version of the rollup
    :start_ds str: yyyymmdd
    """

    # format the save location of the data
    start_date = arrow.get(start_ds, "YYYYMMDD")

    # select the relevant fields
    select_expr = [
        F.lit(start_date.format("YYYY-MM-DD")),
        "search_provider",
        "search_count",
        "country",
        "locale",
        "distribution_id",
        "default_provider",
        "profile_count",
        "profile_share",    # only for daily
        F.lit(start_date.replace(days=+1).format("YYYY-MM-DD")),
    ]

    # replace mode specific items, like rollup_date
    if mode == "monthly":
        select_expr[0] = F.lit(start_date.format("YYYY-MM"))

        # NOTE: beware of calling remove when there are Column elements in the
        # array because boolean operations are overloaded for dataframes.
        shares_index = map(str, select_expr).index("profile_share")
        del select_expr[shares_index]

    key = (
        "{}/{}/processed-{}.csv"
        .format(prefix, mode, start_date.format("YYYY-MM-DD"))
    )

    # persist the dataframe to disk
    logging.info("Writing dataframe to {}/{}".format(bucket, key))
    utils.write_csv_to_s3(dataframe.select(select_expr), bucket, key, header=False)


@click.command()
@click.option('--start_date', required=True)
@click.option('--mode', type=click.Choice(['daily', 'monthly']), required=True)
@click.option('--bucket', required=True)
@click.option('--prefix', required=True)
@click.option('--input_bucket',
              default='telemetry-parquet',
              help='Bucket of the input dataset')
@click.option('--input_prefix',
              default='main_summary/v4',
              help='Prefix of the input dataset')
def main(start_date, mode, bucket, prefix, input_bucket, input_prefix):
    spark = (
        SparkSession
        .builder
        .appName("search_rollups")
        .getOrCreate()
    )

    version = 2
    source_path = format_spark_path(input_bucket, input_prefix)

    logging.info(
        "Extracting main_summary from {}"
        "starting {} over a {} period..."
        .format(source_path, start_date, mode)
    )
    ds_start, ds_end = get_date_range(start_date, mode)
    main_summary = extract(spark, source_path, ds_start, ds_end)

    logging.info("Running the search rollup...")
    rollup = transform(main_summary, mode)

    logging.info("Saving rollup to disk...")
    save(rollup, bucket, prefix, mode, version, ds_start)


if __name__ == '__main__':
    main()
