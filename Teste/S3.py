import boto3
from pyspark.sql import SparkSession,SQLContext
import pandas as pd

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
    csv = pd.read_csv(
        "s3://ve-datalake-stage-vg-dev/gene/contratos/comercial/tmp_aditivos/dt_ref=2020-03-18/Aditivos.csv")

    #df = sc.createDataFrame(csv,schema=)

    sc.stop()
