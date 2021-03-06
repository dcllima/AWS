# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession, SQLContext
import pandas as pd
from pyspark.sql.types import StringType, StructField, StructType
from pyspark.sql.functions import to_date, substring
from pyspark.sql import functions as F

if __name__ == "__main__":
    # client = boto3.client('s3')  # low-level functional API

    # resource = boto3.resource('s3')  # high-level object-oriented API
    # my_bucket = resource.Bucket('ve-datalake-stage-vg-dev')  # subsitute this for your s3 bucket name.

    '''
    obj = client.get_object(Bucket='ve-datalake-stage-vg-dev',
                            Key='gene/contratos/comercial/tmp_aditivos/dt_ref=2020-03-16/Aditivos.csv')
    
    fileData = obj['Body'].read()
    '''
    sc = SparkSession.Builder().appName("Teste").enableHiveSupport().getOrCreate()
    # sc.conf.set("spark.driver.memory", "4g")
    print(sc.conf.get("spark.sql.shuffle.partitions"))
    print(sc.conf.get("spark.driver.memory"))
    spark = SQLContext(sc)

    aditivosSchema = StructType([
        StructField("Id", StringType(), True),
        StructField("IdContrato", StringType(), True),
        StructField("IdClausulasInfoGerais", StringType(), True),
        StructField("IdClausulasPoliticasDatacao", StringType(), True),
        StructField("IdClausulasParadas", StringType(), True),
        StructField("Principal", StringType(), True),
        StructField("Descricao", StringType(), True),
        StructField("InicioValidade", StringType(), True),
        StructField("FimValidade", StringType(), True),
        StructField("InicioVigenciaContrato", StringType(), True),
        StructField("FimVigenciaContrato", StringType(), True),
        StructField("DataAssinatura", StringType(), True),
        StructField("IdArmazenadorCamposSI", StringType(), True),
        StructField("IdOpcoesFaturamento", StringType(), True)])

    csv = pd.read_csv(
        "s3a://ve-datalake-stage-vg-dev/gene/contratos/comercial/tmp_aditivos/dt_ref=2020-03-18/Aditivos.csv")

    df = spark.createDataFrame(csv, schema=aditivosSchema)

    df = df.withColumn("teste", df.Id)
    df = df.withColumn("data", to_date(substring(df.InicioVigenciaContrato, 1, 10)))
    df.printSchema()
    data = df.agg({"data": "max"}).collect()[0]
    print(data["max(data)"])
    # df.select("InicioVigenciaContrato", "data").filter(df.data == data["max(data)"]).show()
    out = df.filter(df.data == data["max(data)"])
    # df.agg(F.max(df.data)).show()

    out.write.mode("append").saveAsTable("aditivo")

    spark.sql("select * from aditivo").write.parquet("/home/diego/Documentos/Projeto/spark/aditivo/",
                                                     mode="append")

    spark.read.parquet("/home/diego/Documentos/Projeto/spark/aditivo/").show()

    sc.stop()
