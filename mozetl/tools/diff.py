"""
Find the difference two datasets by joining data against a key.

schema for a potential

dataset_left:
    table_name:
        original
    path:
        # spark compatible location
        s3://net-mozaws-prod-us-west-2-pipeline-analysis/amiyaguchi/test.csv
    is_emr: False # s3:// vs s3a://
    input_type:
        (csv | parquet)
    prepare:
        SELECT * FROM original
dataset_right:
    ...
join_key:
    [list of column names]
join_mode:
    left, right, inner, outer
"""
import logging

from pyspark.sql import functions as F

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DataFrameComparator:
    def __init__(self, spark, df1, df2):
        self.spark = spark
        self.src_df1 = df1
        self.src_df2 = df2

        self.prep_df1 = self.src_df1
        self.prep_df2 = self.src_df2

    @classmethod
    def from_config(cls, spark, config):
        """Load data from a configuration dictionary."""
        raise NotImplementedError()

    @classmethod
    def from_yaml(cls, spark, filename):
        """Load configuration for the comparator from a yaml file."""
        raise NotImplementedError()

    def set_table_names(self, table1, table2):
        """Register the dataframes with names"""
        self.src_df1.registerTempTable(table1)
        self.src_df2.registerTempTable(table2)

    def prepare(self, expr1, expr2, cache=False):
        """Prepare the datasets for comparison by applying SQL operations."""
        self.prep_df1 = self.spark.sql(expr1)
        self.prep_df2 = self.spark.sql(expr2)

    def compare(self, relative=False, join_on=None, join_mode="outer"):
        """Return a dataframe that summarizes the difference. This function
        only accepts flat data type -- dataframes containing nested data
        should apply transformations to flatten the resulting comparisons.

        Both datasets should have a common set of shared keys. This
        comparison works best for rollups and other aggregate queries where
        the set of keys are approximately the same size.

        For all metric fields (Integer, Long, Double), the summary table will
        contain the arithmetic difference between the fields. For all other
        fields, the resulting field will be in (1, 0, NULL). A value of 1
        means there is a difference, whereas a value of 0 means the two items
        are the same. If the value is not in both datasets, then the value
        will be NULL.

        """
        # assert that the dataframes are compatible
        df1 = self.prep_df1
        df2 = self.prep_df2

        count1 = df1.count()

        col_diff = set(df1.columns) - set(df2.columns)
        if col_diff:
            logging.error("Wrong number of columns: {}"
                          .format("".join(col_diff)))
            return None

        # the set of columns has been shown to be the same
        columns = df1.columns

        logging.info("Generating a list of all numeric fields")

        # try casting all fields to decimal
        select_expr = [F.col(col).cast("decimal") for col in columns]
        non_null_counts = (
            df1
                .select(select_expr)
                .describe()
                .where(F.col("summary") == "count")
                .first()
        )

        # Threshold to be considered numeric is a majority of the rows. If
        # the column has more than half of its rows null and is numeric, the
        # option of using a default value for numeric types should be
        # strongly considered.
        threshold = 0.50

        # keep track of all numeric fields
        numeric_columns = []
        for col in columns:
            if int(non_null_counts[col]) > count1 * threshold:
                numeric_columns.append(col)

        # find suitable join on keys if not provided anything
        if not join_on:
            join_on = set(columns) - set(numeric_columns)
            logging.info("No join keys specified, using candidates: {}"
                         .format(", ".join(join_on)))

        # build select expression for generating diffs
        select_expr = {col: F.col(col) for col in join_on}

        # annotate comparison metrics and generate the comparison clauses
        for col in columns:
            if col in join_on:
                continue

            lhs = "lhs_{}".format(col)
            rhs = "rhs_{}".format(col)

            # rename fields to avoid namespace collision
            df1 = df1.withColumnRenamed(col, lhs)
            df2 = df2.withColumnRenamed(col, rhs)

            # generate the select statement
            if col in numeric_columns:
                expr = F.col(lhs).cast("decimal") - F.col(rhs).cast("decimal")
                if relative:
                    expr = expr / F.col(lhs).cast("decimal")
            else:
                expr = F.col(lhs) != F.col(rhs)
            select_expr[col] = expr.alias(col)

        # join the datasets
        joined_df = df1.join(df2, join_on, join_mode)

        return (
            joined_df
                .select(select_expr)  # run comparison clauses
                .select(columns)  # maintain relative
        )
