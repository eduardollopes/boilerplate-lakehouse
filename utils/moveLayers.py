from pyspark.sql import functions as F
from pyspark.sql.window import Window
import datetime

class MoveLayer:
    def __init__(self, spark):
        self.spark = spark

    def generate_hash(self, df, columns):
        concat_cols = F.concat_ws("||", *[df[col] for col in columns])
        return F.sha2(concat_cols, 384)

    def add_control_columns(self, df, scd_type, data_cols):
        current_date = F.lit(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

        if scd_type == "type_2":
            df = df.withColumn('key_bronze', self.generate_hash(df, data_cols))
            df = df.withColumn('data_create_row', current_date).withColumn('data_update_row', current_date)
        elif scd_type == "type_4":
            df = df.withColumn('data_update_row', current_date)

        return df

    def load_delta_table(self, delta_table_path):
        return self.spark.read.format("delta").load(delta_table_path)

    def perform_merge(self, delta_table, df, merge_condition, data_cols):
        delta_table.alias("delta") \
            .merge(
                df.alias("df"), 
                merge_condition
            ) \
            .whenMatchedUpdate(set={
                **{col: f"df.{col}" for col in data_cols}
            }) \
            .whenNotMatchedInsert(values={
                **{col: f"df.{col}" for col in data_cols}
            }) \
            .execute()

    def write_scd(self, df, delta_table_path, scd_type, primary_key=None):
        control_cols = ['key_bronze', 'data_create_row', 'data_update_row']
        data_cols = [col for col in df.columns if col not in control_cols]

        df = self.add_control_columns(df, scd_type, data_cols)

        delta_table = self.load_delta_table(delta_table_path)

        if scd_type == "type_2":
            merge_condition = "df.key_bronze = delta.key_bronze"
        elif scd_type == "type_4" and primary_key:
            merge_condition = f"df.{primary_key} = delta.{primary_key}"

            window_spec = Window.partitionBy(primary_key).orderBy(F.desc('data_update_row'))
            df = df.withColumn("row_number", F.row_number().over(window_spec)).filter(F.col("row_number") == 1)

        self.perform_merge(delta_table, df, merge_condition, data_cols)
