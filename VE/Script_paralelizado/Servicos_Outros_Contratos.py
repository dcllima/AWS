tmpBalancoServicosfinalStep1 = spark.sql("SELECT current_date() AS DTA_REFERENCIA , \
									        CODIGOREFERENCIA as Contrato_Operacao, \
									        CLASSIFICACAO as Tipo, \
									        CASE \
									            WHEN COMPRADOR LIKE '%VOTENER%' THEN VENDEDOR \
									            ELSE COMPRADOR \
									        END AS CONTRAPARTE, \
									        '00000000000000' AS CNPJ, \
									        NATUREZA, \
									        horas as MESHORA, \
									        MWM, \
									        MWH, \
									        INICIO_MES AS PERIODO, \
									        YEAR(INICIO_MES) AS ANO, \
									        MONTH(INICIO_MES) AS MES, \
									        'N/A' AS ENERGIA, \
									        'N/A' AS SUBMERCADO, \
									        PRECO, \
									        'X' as Join, \
									        NULL AS UF, \
									        DataAcordoComercial AS DTA_OPERACAO, \
									        NULL AS FLEX_MAX, \
									        NULL AS FLEX_MIN, \
									        EXTENSAO, \
									        NULL AS SERIE, \
									        NULL AS DIABASE, \
									        NULL AS RECORRENTE, \
									        NULL AS FLAG_ISENCAO, \
									   		NULL AS NumNotaFiscal, \
									   		NULL AS NumDocumento, \
									   		NULL AS PRECO_BASE \
									 FROM VW_GENE_BALANCO_SERVICOS_FINAL ")

tmpOutrosContratosStep1 = spark.sql("SELECT current_date() AS DTA_REFERENCIA, \
									        CONTRATO, \
									        'Serviços' AS TIPO, \
									        NULL AS CONTRAPARTE, \
									                NULL AS CNPJ, \
									                        CASE \
									                            WHEN VALOR >= 0 THEN 'Venda' \
									                            ELSE 'Compra' \
									                        END AS NATUREZA, \
									                        1 AS VALOR, \
									                        1 AS MMH, \
									                        PERIODO, \
									                        PERIODO as DtRef, \
									                        YEAR(PERIODO) AS ANO, \
									                        MONTH(PERIODO) AS MES, \
									                        NULL AS ENERGIA, \
									                        NULL AS SUBMERCADO, \
									                        ABS(VALOR) AS PRECO, \
									                        'X', \
									                        NULL AS UF, \
									                        NULL AS DTA_OPERACAO, \
									                        NULL AS FLEX_MAX, \
									                        NULL AS FLEX_MIN, \
									                        NULL AS EXTENSAO, \
									                        NULL AS SERIE, \
									                        NULL AS DIABASE, \
									                        NULL AS RECORRENTE, \
									                        NULL AS FLAG_ISENCAO, \
									   			   			NULL AS NumNotaFiscal, \
									   			   			NULL AS NumDocumento, \
									   			  			NULL AS PRECO_BASE \
									 FROM OutrosContratos")

tmpOutrosContratosStep2 = fc_Qtd_Horas_Mes(tmpOutrosContratosStep1,tmpOutrosContratosStep1.DtRef)

tmpOutrosContratosStep2 = tmpOutrosContratosStep2.withColumnRenamed("Qtd_horas","MESHORA")

tmpOutrosContratos = tmpOutrosContratosStep2.select("DTA_REFERENCIA","CONTRATO","TIPO","CONTRAPARTE","CNPJ","NATUREZA","MESHORA","VALOR","MMH","PERIODO","ANO","MES","ENERGIA","SUBMERCADO","PRECO","X","UF","DTA_OPERACAO","FLEX_MAX","FLEX_MIN","EXTENSAO","SERIE","DIABASE","RECORRENTE","FLAG_ISENCAO","NumNotaFiscal","NumDocumento","PRECO_BASE")

tmpLiquidacaoStep1 = spark.sql("SELECT current_date() AS DTA_REFERENCIA, \
						          'CTO-LIQUIDACAO', \
						          'Liquidação' AS TIPO, \
						          NULL AS CONTRAPARTE, \
						                  NULL AS CNPJ, \
						                          'Outros' AS NATUREZA, \
						                          1 AS VALOR, \
						                          1 AS MMH, \
						                           TO_DATE(concat(ANOMES,'01'), 'yyyyMMdd') AS PERIODO, \
						                           TO_DATE(concat(ANOMES,'01'), 'yyyyMMdd') AS DtRef, \
						                          LEFT(ANOMES,4) AS ANO, \
						                          RIGHT(ANOMES,2) AS MES, \
						                          NULL AS ENERGIA, \
						                          NULL AS SUBMERCADO, \
						                          LIQUIDACAO AS PRECO, \
						                          'X', \
						                          NULL AS UF, \
						                          NULL AS DTA_OPERACAO, \
						                          NULL AS FLEX_MAX, \
						                          NULL AS FLEX_MIN, \
						                          NULL AS EXTENSAO, \
						                          NULL AS SERIE, \
						                          NULL AS DIABASE, \
						                          NULL AS RECORRENTE, \
						                          NULL AS FLAG_ISENCAO, \
									   			  NULL AS NumNotaFiscal, \
									   			  NULL AS NumDocumento, \
									   			  NULL AS PRECO_BASE \
						   FROM Liquidacao")

tmpLiquidacaoStep2 = fc_Qtd_Horas_Mes(tmpLiquidacaoStep1,tmpLiquidacaoStep1.DtRef)

tmpLiquidacaoStep2 = tmpLiquidacaoStep2.withColumnRenamed("Qtd_horas","MESHORA")

tmpLiquidacao = tmpLiquidacaoStep2.select("DTA_REFERENCIA","CTO-LIQUIDACAO","TIPO","CONTRAPARTE","CNPJ","NATUREZA","VALOR","MESHORA","MMH","PERIODO", "ANO","MES","ENERGIA","SUBMERCADO","PRECO","X","UF","DTA_OPERACAO","FLEX_MAX","FLEX_MIN","EXTENSAO","SERIE","DIABASE","RECORRENTE","FLAG_ISENCAO","NumNotaFiscal","NumDocumento","PRECO_BASE")

tmpModulacaoStep1 = spark.sql("SELECT current_date() AS DTA_REFERENCIA, \
						         'CTO-MODULACAO-CEMIG', \
						         'Modulação' AS TIPO, \
						         NULL AS CONTRAPARTE, \
						                 NULL AS CNPJ, \
						                         'Outros' AS NATUREZA, \
						                         1 AS VALOR, \
						                         1 AS MMH, \
						                         TO_DATE(concat(ANOMES,'01'), 'yyyyMMdd') AS PERIODO, \
						                         TO_DATE(concat(ANOMES,'01'), 'yyyyMMdd') AS DtRef, \
						                         LEFT(ANOMES,4) AS ANO, \
						                         RIGHT(ANOMES,2) AS MES, \
						                         NULL AS ENERGIA, \
						                         NULL AS SUBMERCADO, \
						                         MODULACAO_CEMIG AS PRECO, \
						                         'X', \
						                         NULL AS UF, \
						                         NULL AS DTA_OPERACAO, \
						                         NULL AS FLEX_MAX, \
						                         NULL AS FLEX_MIN, \
						                         NULL AS EXTENSAO, \
						                         NULL AS SERIE, \
						                         NULL AS DIABASE, \
						                         NULL AS RECORRENTE, \
						                         NULL AS FLAG_ISENCAO, \
									   			 NULL AS NumNotaFiscal, \
									   			 NULL AS NumDocumento, \
									   			 NULL AS PRECO_BASE \
						  FROM Liquidacao \
						  WHERE MODULACAO_CEMIG IS NOT NULL")

tmpModulacaoStep2 = fc_Qtd_Horas_Mes(tmpModulacaoStep1,tmpModulacaoStep1.DtRef)

tmpModulacaoStep2 = tmpModulacaoStep2.withColumnRenamed("Qtd_horas","MESHORA")

tmpModulacao = tmpModulacaoStep2.select("DTA_REFERENCIA","CTO-MODULACAO-CEMIG","TIPO","CONTRAPARTE","CNPJ","NATUREZA","MESHORA","VALOR","MMH","PERIODO","ANO","MES","ENERGIA","SUBMERCADO","PRECO","X","UF","DTA_OPERACAO","FLEX_MAX","FLEX_MIN","EXTENSAO","SERIE","DIABASE","RECORRENTE","FLAG_ISENCAO","NumNotaFiscal","NumDocumento","PRECO_BASE")

tmpEnergiaEntregueFinal.coalesce(50)
#tmpEnergiaContratadaSazoFinal.coalesce(50)
tmpBalancoServicosfinalStep1.coalesce(50)
tmpOutrosContratos.coalesce(50)
tmpLiquidacao.coalesce(50)
tmpModulacao.coalesce(50)


resultUnion = tmpBalancoServicosfinalStep1.union(tmpOutrosContratos.union(tmpLiquidacao.union(tmpModulacao)))


resultUnion = resultUnion.withColumn("IdSubmercado",
													when(resultUnion.SUBMERCADO == lit('Sudeste/Centro-oeste'), lit(1))
		   										   .when(resultUnion.SUBMERCADO == lit('Sul'), lit(2))
           										   .when(resultUnion.SUBMERCADO == lit('Norte'), lit(3))
           										   .when(resultUnion.SUBMERCADO == lit('Nordeste'), lit(8))
           										   .otherwise(lit(1)))
#começa aqui a merda
resultUnion.createOrReplaceTempView("TMP_RESULTADO_UNION")

TbFinalStep1 = spark.sql("SELECT  TAB1.DTA_REFERENCIA, TAB1.Contrato_Operacao, TAB1.Tipo, TAB1.Contraparte, TAB1.Cnpj, TAB1.Natureza, TAB1.MesHora,  \
                        TAB1.MWm, TAB1.MWh, \
                        ANO+RIGHT('0' + MES,2) AS Ano_Mes, \
                        TAB1.Periodo, TAB1.Energia, TAB1.Submercado, TAB1.Preco, TAB1.Join, TAB1.UF, TAB1.DTA_OPERACAO, TAB1.FLEX_MAX, TAB1.FLEX_MIN,   \
                        TAB1.Periodo,	 \
                        	TAB1.EXTENSAO,   \
                        	ROW_NUMBER() OVER(ORDER BY TAB1.Periodo, TAB1.Contrato_Operacao ASC) AS RowId, \
                        	B.valor, \
                        	SERIE, \
                        	DIABASE, \
                        	RECORRENTE, \
                        	FLAG_ISENCAO, \
                        	NumNotaFiscal, \
                        	NumDocumento, \
							PRECO_BASE \
                        FROM TMP_RESULTADO_UNION as TAB1 \
                        LEFT JOIN PLDMedio AS B ON SUBSTR(B.data,0,10) = TAB1.Periodo AND B.IdSubmercado = TAB1.IdSubmercado where TAB1.Periodo is not null and TAB1.Periodo <>  'PERIODO' ")

TbFinalStep2 = TbFinalStep1.withColumn("FontePreco",fc_Fonte_Preco_UDF("Periodo","DTA_REFERENCIA"))

TbFinalStep2.createOrReplaceTempView("TAB_FINAL_STEP2")
#Termina aqui a merda
TbFinalStep3 = spark.sql("SELECT A.DTA_REFERENCIA, \
		                         A.Contrato_Operacao, \
		                         A.Tipo, \
		                         A.Contraparte, \
		                         A.Cnpj, \
		                         A.Natureza, \
		                         A.MesHora, \
		                         A.MWm, \
		                         A.MWh, \
		                         A.Ano_Mes, \
		                         A.Periodo, \
		                         A.Energia, \
		                         A.Submercado, \
		                         A.Preco, \
		                         A.JOIN, \
		                         A.UF, \
		                         A.DTA_OPERACAO, \
		                         A.FLEX_MAX, \
		                         A.FLEX_MIN, \
		                         A.Periodo, \
		                         A.EXTENSAO, \
		                         A.RowId, \
		                         A.valor, \
		                         A.FontePreco, \
		                         A.SERIE, \
                        		 A.DIABASE, \
                        		 A.RECORRENTE, \
                        		 A.FLAG_ISENCAO, \
                        		 A.NumNotaFiscal, \
                        		 A.NumDocumento, \
                        		 A.PRECO_BASE, \
		                         SUBSTR(B.DTA_REF,0,10) AS DTA_REF, \
		                         B.C_M0, \
		                         B.C_M1, \
		                         B.C_M2, \
		                         B.C_M3, \
		                         B.C_A0, \
		                         B.C_A1, \
		                         B.C_A2, \
		                         B.C_A3, \
		                         B.C_A4, \
		                         B.I_M0, \
		                         B.I_M1, \
		                         B.I_M2, \
		                         B.I_M3, \
		                         B.I_A0, \
		                         B.I_A1, \
		                         B.I_A2, \
		                         B.I_A3, \
		                         B.I_A4, \
		                         1 AS DCIDE \
		                  FROM TAB_FINAL_STEP2 AS A \
		                  LEFT JOIN DCIDE AS B ON ((CASE WHEN A.valor IS NULL \
		                                       OR A.FontePreco = 'DCIDE'THEN ADD_MONTHS(A.DTA_REFERENCIA,-1) ELSE A.DTA_REFERENCIA END) BETWEEN TO_DATE(SUBSTR(B.DTA_INICIO_REF,0,10),'yyyy-MM-dd') AND TO_DATE(SUBSTR(B.DTA_FIM_REF,0,10),'yyyy-MM-dd') \
		                  					   AND B.SUBMERCADO = A.Submercado)")

TbFinalStep3.createOrReplaceTempView("TAB_FINAL_STEP3")

TbFinalStep4 = spark.sql("SELECT A.DTA_REFERENCIA, \
		                         A.Contrato_Operacao, \
		                         A.Tipo, \
		                         A.Contraparte, \
		                         A.Cnpj, \
		                         A.Natureza, \
		                         A.MesHora, \
		                         A.MWm, \
		                         A.MWh, \
		                         A.Ano_Mes, \
		                         A.Periodo, \
		                         A.Energia, \
		                         A.Submercado, \
		                         A.Preco, \
		                         A.JOIN, \
		                         A.UF, \
		                         A.DTA_OPERACAO, \
		                         A.FLEX_MAX, \
		                         A.FLEX_MIN, \
		                         A.Periodo, \
		                         A.EXTENSAO, \
		                         A.RowId, \
		                         A.valor, \
		                         A.FontePreco, \
		                         A.SERIE, \
                        		 A.DIABASE, \
                        		 A.RECORRENTE, \
                        		 A.FLAG_ISENCAO, \
                        		 A.NumNotaFiscal, \
                        		 A.NumDocumento, \
                        		 A.PRECO_BASE, \
		                         A.DTA_REF,  \
		                         A.C_M0,  \
		                         A.C_M1,  \
		                         A.C_M2,  \
		                         A.C_M3,  \
		                         A.C_A0,  \
		                         A.C_A1,  \
		                         A.C_A2,  \
		                         A.C_A3,  \
		                         A.C_A4, \
		                         A.I_M0,  \
		                         A.I_M1,  \
		                         A.I_M2,  \
		                         A.I_M3,  \
		                         A.I_A0,  \
		                         A.I_A1,  \
		                         A.I_A2,  \
		                         A.I_A3,  \
		                         A.I_A4, \
		                         B.MAX_DATA \
		                  FROM TAB_FINAL_STEP3 AS A \
		                  LEFT JOIN (SELECT MAX(TO_DATE(SUBSTR(DTA_FIM_REF,0,10), 'yyyy-MM-dd')) AS MAX_DATA,1, 1 AS DCIDE FROM DCIDE) AS B ON (A.DCIDE = B.DCIDE)")

TbFinalStep4.createOrReplaceTempView("TAB_FINAL_STEP4")

TbFinalStep5 = spark.sql("SELECT A.DTA_REFERENCIA, \
		                         A.Contrato_Operacao, \
		                         A.Tipo, \
		                         A.Contraparte, \
		                         A.Cnpj, \
		                         A.Natureza, \
		                         A.MesHora, \
		                         A.MWm, \
		                         A.MWh, \
		                         A.Ano_Mes, \
		                         A.Periodo, \
		                         A.Energia, \
		                         A.Submercado, \
		                         A.Preco, \
		                         A.JOIN, \
		                         A.UF, \
		                         A.DTA_OPERACAO, \
		                         A.FLEX_MAX, \
		                         A.FLEX_MIN, \
		                         A.Periodo, \
		                         A.EXTENSAO, \
		                         A.RowId, \
		                         A.valor, \
		                         A.FontePreco, \
		                         A.SERIE, \
                        		 A.DIABASE, \
                        		 A.RECORRENTE, \
                        		 A.FLAG_ISENCAO, \
                        		 A.NumNotaFiscal, \
                        		 A.NumDocumento, \
                        		 A.PRECO_BASE, \
		                         A.MAX_DATA , \
		                         NVL(A.DTA_REF,B.DTA_REF) AS DTA_REF, \
		                         NVL(A.C_M0,B.C_M0) AS C_M0, \
		                         NVL(A.C_M1,B.C_M1) AS C_M1, \
		                         NVL(A.C_M2,B.C_M2) AS C_M2, \
		                         NVL(A.C_M3,B.C_M3) AS C_M3, \
		                         NVL(A.C_A0,B.C_A0) AS C_A0, \
		                         NVL(A.C_A1,B.C_A1) AS C_A1, \
		                         NVL(A.C_A2,B.C_A2) AS C_A2, \
		                         NVL(A.C_A3,B.C_A3) AS C_A3, \
		                         NVL(A.C_A4,B.C_A4) AS C_A4, \
		                         NVL(A.I_M0,B.I_M0) AS I_M0, \
		                         NVL(A.I_M1,B.I_M1) AS I_M1, \
		                         NVL(A.I_M2,B.I_M2) AS I_M2, \
		                         NVL(A.I_M3,B.I_M3) AS I_M3, \
		                         NVL(A.I_A0,B.I_A0) AS I_A0, \
		                         NVL(A.I_A1,B.I_A1) AS I_A1, \
		                         NVL(A.I_A2,B.I_A2) AS I_A2, \
		                         NVL(A.I_A3,B.I_A3) AS I_A3, \
		                         NVL(A.I_A4,B.I_A4) AS I_A4  \
		                  FROM TAB_FINAL_STEP4 AS A \
		                  LEFT JOIN DCIDE AS B ON (A.DTA_REF IS NULL AND TO_DATE(SUBSTR(B.DTA_FIM_REF,0,10), 'yyyy-MM-dd') = TO_DATE(SUBSTR(A.MAX_DATA,0,10), 'yyyy-MM-dd') AND B.SUBMERCADO = A.Submercado)")

TbFinalStep6 = fc_Pld_Submercado(TbFinalStep5,TbFinalStep5.Energia,TbFinalStep5.DTA_REF, TbFinalStep5.Periodo,TbFinalStep5.C_M0,TbFinalStep5.C_M1,TbFinalStep5.C_M2,TbFinalStep5.C_M3,TbFinalStep5.C_A0,TbFinalStep5.C_A1,TbFinalStep5.C_A2,TbFinalStep5.C_A3,TbFinalStep5.C_A4,TbFinalStep5.I_M0,TbFinalStep5.I_M1,TbFinalStep5.I_M2,TbFinalStep5.I_M3,TbFinalStep5.I_A0,TbFinalStep5.I_A1,TbFinalStep5.I_A2,TbFinalStep5.I_A3,TbFinalStep5.I_A4)

TbFinalStep7 = TbFinalStep6.coalesce(50)

TbFinalStep7.createOrReplaceTempView("TAB_FINAL_STEP7")

TbFinalStep8Ini = spark.sql("SELECT A.MAX_DATA, \
									A.Periodo, \
									MAX(DT_INI.DataVigencia) AS DT_INI_DataVigencia \
								FROM TAB_FINAL_STEP7 AS A \
								LEFT JOIN VW_SERIE_IPCA AS DT_INI ON TO_DATE(SUBSTR(DT_INI.DataVigencia,0,10), 'yyyy-MM-dd') < TO_DATE(SUBSTR(A.MAX_DATA ,0,10), 'yyyy-MM-dd') \
								GROUP BY A.MAX_DATA, \
										 A.Periodo")

TbFinalStep8Ini.createOrReplaceTempView("TAB_FINAL_STEP8_INI")

TbFinalStep8Fim = spark.sql("SELECT A.MAX_DATA, \
									A.Periodo, \
									MAX(DT_FIM.DataVigencia) AS DT_FIM_DataVigencia \
								FROM TAB_FINAL_STEP7 AS A \
								LEFT JOIN VW_SERIE_IPCA AS DT_FIM ON TO_DATE(SUBSTR(DT_FIM.DataVigencia,0,10), 'yyyy-MM-dd') < A.Periodo \
								GROUP BY A.MAX_DATA, \
										 A.Periodo")

TbFinalStep8Fim.createOrReplaceTempView("TAB_FINAL_STEP8_FIM")

# Deu merda aqui Java Heap space
TbFinalStep8 = spark.sql("SELECT A.DTA_REFERENCIA, \
						         A.Contrato_Operacao, \
						         A.Tipo, \
						         A.Contraparte, \
						         A.Cnpj, \
						         A.Natureza, \
						         A.MesHora, \
						         A.MWm, \
						         A.MWh, \
						         A.Ano_Mes, \
						         A.Periodo, \
						         A.Energia, \
						         A.Submercado, \
						         A.Preco, \
						         A.JOIN, \
						         A.UF, \
						         A.DTA_OPERACAO, \
						         A.FLEX_MAX, \
						         A.FLEX_MIN, \
						         A.Periodo, \
						         A.EXTENSAO, \
						         A.RowId, \
						         A.valor, \
						         A.FontePreco, \
		                         A.SERIE, \
                        		 A.DIABASE, \
                        		 A.RECORRENTE, \
                        		 A.FLAG_ISENCAO, \
                        		 A.NumNotaFiscal, \
                        		 A.NumDocumento, \
                        		 A.PRECO_BASE, \
						         A.DTA_REF, \
						         A.C_M0, \
						         A.C_M1, \
						         A.C_M2, \
						         A.C_M3, \
						         A.C_A0, \
						         A.C_A1, \
						         A.C_A2, \
						         A.C_A3, \
						         A.C_A4, \
						         A.I_M0, \
						         A.I_M1, \
						         A.I_M2, \
						         A.I_M3, \
						         A.I_A0, \
						         A.I_A1, \
						         A.I_A2, \
						         A.I_A3, \
						         A.I_A4, \
						         A.CURVA_PLD, \
						         A.MAX_DATA, \
						         DT_INI.DT_INI_DataVigencia, \
						         DT_FIM.DT_FIM_DataVigencia \
						  FROM TAB_FINAL_STEP7 AS A \
						  LEFT JOIN TAB_FINAL_STEP8_INI AS DT_INI ON TO_DATE(SUBSTR(DT_INI.MAX_DATA ,0,10), 'yyyy-MM-dd') = TO_DATE(SUBSTR(A.MAX_DATA ,0,10), 'yyyy-MM-dd') AND A.Periodo = DT_INI.Periodo \
						  LEFT JOIN TAB_FINAL_STEP8_FIM AS DT_FIM ON A.Periodo = DT_FIM.Periodo AND TO_DATE(SUBSTR(DT_FIM.MAX_DATA ,0,10), 'yyyy-MM-dd') = TO_DATE(SUBSTR(A.MAX_DATA ,0,10), 'yyyy-MM-dd') ")

# Step TbFinalStep8 verificar pois count está demorando

TbFinalStep8.createOrReplaceTempView("TAB_FINAL_STEP8")
# Deu merda aqui

TbFinalStep9 = spark.sql("SELECT \
								 current_timestamp() as DTA_REFERENCIA, \
						         A.Contrato_Operacao, \
						         A.Tipo, \
						         A.Contraparte, \
						         A.Cnpj, \
						         A.Natureza, \
						         int(A.MesHora) as MesHora, \
						         double(A.MWm) as MWm, \
						         double(A.MWh) as MWh, \
						         string(A.Ano_Mes), \
						         TO_DATE(SUBSTR(A.Periodo ,0,10), 'yyyy-MM-dd') as Periodo, \
						         A.Energia, \
						         A.Submercado, \
						         double(A.Preco) as Preco, \
						         A.JOIN, \
						         A.UF, \
						         TO_DATE(SUBSTR(A.DTA_OPERACAO ,0,10), 'yyyy-MM-dd') as DTA_OPERACAO, \
						         double(A.FLEX_MAX) as FLEX_MAX, \
						         double(A.FLEX_MIN) as FLEX_MIN, \
						         A.CURVA_PLD, \
						         CASE WHEN YEAR(TO_DATE(SUBSTR(A.Periodo ,0,10), 'yyyy-MM-dd')) <= YEAR(TO_DATE(SUBSTR(A.MAX_DATA ,0,10), 'yyyy-MM-dd')) THEN A.CURVA_PLD ELSE A.CURVA_PLD * (FIM.VALOR/INI.VALOR) END AS CURVA_DCIDE, \
						         A.EXTENSAO, \
						         A.RowId, \
		                         REPLACE(REPLACE(STRING(A.SERIE),']'),'[') AS SERIE, \
                        		 A.DIABASE, \
                        		 A.RECORRENTE, \
                        		 double(A.PRECO_BASE) as PRECO_BASE, \
                        		 A.NumNotaFiscal, \
                        		 A.NumDocumento, \
                        		 A.FLAG_ISENCAO AS PIS_COFINS\
						  FROM TAB_FINAL_STEP8 AS A \
						  LEFT JOIN \
						    (SELECT VALOR, \
						            DataVigencia \
						     FROM VW_SERIE_IPCA) AS INI ON INI.DataVigencia = A.DT_INI_DataVigencia \
						  LEFT JOIN \
						    (SELECT VALOR, \
						            DataVigencia \
						     FROM VW_SERIE_IPCA) AS FIM ON FIM.DataVigencia = A.DT_FIM_DataVigencia")