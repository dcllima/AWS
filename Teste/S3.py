import boto3
from pyspark.sql import SparkSession, SQLContext
import pandas as pd
from pyspark.sql.types import StringType, StructField, StructType

if __name__ == "__main__":
    client = boto3.client('s3')  # low-level functional API

    # resource = boto3.resource('s3')  # high-level object-oriented API
    # my_bucket = resource.Bucket('ve-datalake-stage-vg-dev')  # subsitute this for your s3 bucket name.

    '''
    obj = client.get_object(Bucket='ve-datalake-stage-vg-dev',
                            Key='gene/contratos/comercial/tmp_aditivos/dt_ref=2020-03-16/Aditivos.csv')
    
    fileData = obj['Body'].read()
    '''
    sc = SparkSession.Builder().appName("Teste").getOrCreate()
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
        "s3://ve-datalake-stage-vg-dev/gene/contratos/comercial/tmp_aditivos/dt_ref=2020-03-18/Aditivos.csv")

    df = spark.createDataFrame(csv, schema=aditivosSchema)
    df.printSchema()
    df.where(df.)

    sc.stop()
