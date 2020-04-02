# -*- coding: utf-8 -*-
from abc import abstractproperty

from pyspark import SparkContext
from pyspark.sql import SparkSession, SQLContext
import pandas as pd
from pyspark.sql.types import StringType, StructField, StructType
from pyspark.sql.functions import to_date, substring
from pyspark.sql import functions as F

# Variáveis
tpCaracContrPartesContratuais = '(11,20,24,32,43)'  # fcQtdPartesContratuais tipo CaracteristicasContrato

tpCaracContrSerieReajuste = '6'  # fcSerieReajuste        tipo CaracteristicasContrato

idCategIsentosPisConfim = '131'  # vwIsentosPisConfim     ID   Categorias

tipoPeriodoVigenciaContrCont = '1'  # vwContratosCont        TIPOPERIODOVIGENCIA   Contratos

dadospropostaContrCont = 0  # vwContratosCont        DADOSPROPOSTA    ContratosAprovaveis

dadosPropostaContrProp = 1  # vwContratosPropostas        DADOSPROPOSTA    Contratos

tipoFcNaturezaStep1 = '(1,2)'  # fcNaturezaStep1 tipo PartesContratuais

idTipoFcPrecoComissaoPreco = '(8)'  # fcPrecoComissaoPreco IdTipo CaracteristicasContrato

idTipoFcPrecoComissaoCursor = '6'  # fcPrecoComissaoCursor IDTIPO CaracteristicasContrato

idTipoCampovw09NumNotaFiscal = '(5,24)'  # vw09NumNotaFiscal IdTipoCampo CamposSI

mesReferenciaVw10GeneFaturas = "'01/01/2017'"  # vw10GeneFaturas MesReferencia EnergiaFaturas

FlagsVw10GeneFaturas = '32'  # vw10GeneFaturas Flags EnergiaFaturas

idTipoVwEnergiaContratadaStep1 = '2'  # vwEnergiaContratadaStep1 IdTipo CaracteristicasContrato

idTipoVwEnergiaContratadaStep2Aux = '(11,20, 32)'  # vwEnergiaContratadaStep2Aux IdTipo CaracteristicasContrato

iIdTipoVwEnergiaEntregue = '(5)'  # vwEnergiaEntregue IdTipo CaracteristicasContrato

idTipoVwBalancoServicosStep1 = '(8)'  # vwBalancoServicosStep1 IdTipo CaracteristicasContrato

if __name__ == "__main__":
    sc = SparkSession.Builder().appName("BalancoTeste").enableHiveSupport().getOrCreate()
    # sc._jsc.hadoopConfiguration().set("fs.s3a.access.key", "AKIAUXQ6QZCNKOOVPIW2")
    # sc._jsc.hadoopConfiguration().set("fs.s3a.secret.key", "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")
    # sc._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    # sc.conf.set("spark.driver.memory", "15g")
    print(sc.conf.get("spark.sql.shuffle.partitions"))
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
    # s3://ve-datalake-stage-vg-dev/gene/contratos/comercial/tmp_aditivos/dt_ref=2020-03-17/Aditivos.csv
    a = spark.read.csv(
        "s3://ve-datalake-stage-vg-dev/gene/contratos/comercial/tmp_aditivos/dt_ref=2020-03-17/Aditivos.csv",
        schema=aditivosSchema)
    data = a.agg({"data": "max"}).collect()[0]
    print(data["max(data)"])
