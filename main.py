from pyspark.sql import SparkSession
from MoveLayer import MoveLayer

SPARK = SparkSession.builder.appName("Lakehouse Pipeline").getOrCreate()
MOVE_LAYER = MoveLayer(SPARK)

def main(spark, move_layer):
    raw_data_path = "boilerplate-lakehouse/data/raw"
    bronze_table_path = "boilerplate-lakehouse/data/bronze"
    silver_table_path = "boilerplate-lakehouse/data/silver"

    raw_df = spark.read.format("parquet").load(raw_data_path)

    print("Processing layer on bronze layer ...")
    move_layer.write_scd(
        raw_df, 
        bronze_table_path, 
        scd_type="type_2"
    )

    print("Processing layer on silver layer ...")
    move_layer.write_scd(
        raw_df, 
        silver_table_path, 
        scd_type="type_4", 
        primary_key="id"
    ) 

    spark.stop()

if __name__ == "__main__":
    main(spark=SPARK, move_layer=MOVE_LAYER)
