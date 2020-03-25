from boto3 import s3
from docutils.nodes import paragraph
from pyspark.sql import SparkSession

if __name__ == "__main__":
    sc = SparkSession.Builder().appName("Teste").getOrCreate()
    parquetFile = sc.read.parquet("/home/diego/Documentos/parquet.parquet")
    parquetFile.printSchema()

    view = parquetFile.createOrReplaceTempView("teste")

    sc.sql("select * from teste").show()


