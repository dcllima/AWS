tmpEnergiaContratadaSazoFinalStep1 = spark.sql("SELECT current_date() AS DTA_REFERENCIA, \
											       CodigoReferencia AS Contrato_Operacao , \
											       CLASSIFICACAO AS Tipo , \
											       CASE \
											           WHEN NATUREZA = 'Venda' THEN COMPRADOR \
											           ELSE VENDEDOR \
											       END AS Contraparte , \
											       Cnpj, \
											       NATUREZA AS Natureza, \
											       MWmRateado AS MWm , \
											       INICIO_MES AS Periodo, \
											       A.ANO, \
											       A.MES, \
											       PERFIL_ENERGIA AS Energia, \
											       SUBMERCADO AS Submercado , \
											       ValorPreco AS Preco, \
											       'X', \
											       NVL(UF.Sigla, CASE SUBMERCADO WHEN 'Sul' THEN 'SC' WHEN 'Norte' THEN 'AM' WHEN 'Nordeste' THEN 'BA' WHEN 'Sudeste/Centro-oeste' THEN 'SP' END) AS UF , \
											       DTA_OPERACAO, \
											       FLEX_MAX, \
											       FLEX_MIN, \
											       EXTENSAO,\
											       A.IDADITIVO, \
											       A.SERIE1 as SERIE, \
									   			   A.DIABASE, \
									   			   A.RECORRENTE, \
									   			   A.FLAG_ISENCAO \
											FROM UF_EMPRESA UF \
											RIGHT JOIN TMP_ENERGIA_CONTRATADA_SAZO A ON UF.IdEmpresa = A.IdEmpresa \
											LEFT JOIN Empresas emp ON (A.IdEmpresa = emp.Id) \
											WHERE NATUREZA IN ('Venda', \
											                   'Compra') \
											  AND CLASSIFICACAO <> 'Servicos'")
											  
tmpEnergiaContratadaSazoFinalStep2 = tmpEnergiaContratadaSazoFinalStep1

tmpEnergiaContratadaSazoFinalStep2.createOrReplaceTempView("TMP_ENERGIA_CONTRATADA_SAZO_FINAL_STEP1")

#tmpEnergiaContratadaSazoFinalStep2.repartition("ANO")
#tmpEnergiaEntregue.repartition("ANO")

tmpEnergiaContratadaSazoFinalStep3 = spark.sql("SELECT A.DTA_REFERENCIA, \
                                                       A.Contrato_Operacao, \
                                                       A.Tipo, \
                                                       A.Contraparte, \
                                                       A.Cnpj, \
                                                       A.Natureza, \
                                                       A.MWm, \
                                                       A.Periodo, \
                                                       A.Periodo as DtRef, \
                                                       A.ANO, \
                                                       A.MES, \
                                                       A.Energia, \
                                                       A.Submercado, \
                                                       A.Preco, \
                                                       A.X, \
                                                       A.UF, \
                                                       A.DTA_OPERACAO, \
                                                       A.FLEX_MAX, \
                                                       A.FLEX_MIN, \
                                                       A.EXTENSAO, \
											       	   A.SERIE, \
									   			   	   A.DIABASE, \
									   			   	   A.RECORRENTE, \
									   			   	   A.FLAG_ISENCAO \
                                                FROM TMP_ENERGIA_CONTRATADA_SAZO_FINAL_STEP1 AS A \
                                                LEFT JOIN (SELECT IDADITIVO,ANO,MES FROM TMP_ENERGIA_ENTREGUE_FINAL WHERE NATUREZA IN ('Venda','Compra')) AS B ON (A.IDADITIVO = B.IDADITIVO \
                                                AND A.ANO = B.ANO \
                                                AND A.MES = B.MES) WHERE B.IDADITIVO IS NULL")

tmpEnergiaContratadaSazoFinalStep3 = fc_Qtd_Horas_Mes(tmpEnergiaContratadaSazoFinalStep3,tmpEnergiaContratadaSazoFinalStep3.DtRef)

tmpEnergiaContratadaSazoFinalStep4 = tmpEnergiaContratadaSazoFinalStep3.withColumnRenamed("Qtd_horas","MESHORA")

tmpEnergiaContratadaSazoFinalStep4.createOrReplaceTempView("TMP_ENERGIA_CONTRATADA_SAZO_FINAL_STEP4")

tmpEnergiaContratadaSazoFinal = spark.sql("SELECT DTA_REFERENCIA, \
												  Contrato_Operacao, \
												  Tipo, \
												  Contraparte, \
												  Cnpj, \
												  Natureza, \
												  MESHORA, \
												  MWm, \
												  MWm * MESHORA as MWH, \
												  Periodo, \
												  ANO, \
												  MES, \
												  Energia, \
												  Submercado, \
												  Preco, \
												  X as Join, \
												  UF, \
												  DTA_OPERACAO, \
												  FLEX_MAX, \
												  FLEX_MIN, \
												  EXTENSAO, \
											      SERIE, \
									   			  DIABASE, \
									   			  RECORRENTE, \
									   			  FLAG_ISENCAO, \
									   			  NULL AS NumNotaFiscal, \
									   			  NULL AS NumDocumento, \
									   			  NULL AS PRECO_BASE \
												  FROM TMP_ENERGIA_CONTRATADA_SAZO_FINAL_STEP4")

tmpEnergiaContratadaSazoFinal = tmpEnergiaContratadaSazoFinal.withColumn("IdSubmercado",
													when(tmpEnergiaContratadaSazoFinal.Submercado == lit('Sudeste/Centro-oeste'), lit(1))
		   										   .when(tmpEnergiaContratadaSazoFinal.Submercado == lit('Sul'), lit(2))
           										   .when(tmpEnergiaContratadaSazoFinal.Submercado == lit('Norte'), lit(3))
           										   .when(tmpEnergiaContratadaSazoFinal.Submercado == lit('Nordeste'), lit(8))
           										   .otherwise(lit(1)))

tmpEnergiaContratadaSazoFinal.createOrReplaceTempView("TMP_ENERGIA_CONTRATADA_SAZO_FINAL")