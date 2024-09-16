from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Cria uma sessão Spark
spark = SparkSession.builder \
    .appName("Spark Parquet Example") \
    .getOrCreate()

# Cria um DataFrame simples
data = [
    (1,"Alice", 34, "Engineer"),
    (2,"Bob", 45, "Data Scientist"),
    (3,"Catherine", 29, "Analyst")
]

columns = ["id", "name", "age", "occupation"]

df = spark.createDataFrame(data, columns)

# Mostra o DataFrame
df.show()

# Especifica o caminho para salvar o arquivo Parquet
output_path = "./boilerplate-lakehouse/data/raw"

# Escreve o DataFrame no formato Parquet
df.write.parquet(output_path, mode='overwrite')

# Encerra a sessão Spark
spark.stop()
