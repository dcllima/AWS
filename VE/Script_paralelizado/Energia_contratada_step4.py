TbFinalSazoStep1 = spark.sql("SELECT  TAB1.DTA_REFERENCIA, TAB1.Contrato_Operacao, TAB1.Tipo, TAB1.Contraparte, TAB1.Cnpj, TAB1.Natureza, TAB1.MesHora,  \
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
                        FROM TMP_ENERGIA_CONTRATADA_SAZO_FINAL as TAB1 \
                        LEFT JOIN PLDMedio AS B ON SUBSTR(B.data,0,10) = TAB1.Periodo AND B.IdSubmercado = TAB1.IdSubmercado where TAB1.Periodo is not null and TAB1.Periodo <>  'PERIODO' ")

TbFinalSazoStep2 = TbFinalSazoStep1.withColumn("FontePreco",fc_Fonte_Preco_UDF("Periodo","DTA_REFERENCIA"))

TbFinalSazoStep2.createOrReplaceTempView("TAB_FINAL_STEP_SAZO_2")

TbFinalSazoStep3 = spark.sql("SELECT A.DTA_REFERENCIA, \
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
		                  FROM TAB_FINAL_STEP_SAZO_2 AS A \
		                  LEFT JOIN DCIDE AS B ON ((CASE WHEN A.valor IS NULL \
		                                       OR A.FontePreco = 'DCIDE'THEN ADD_MONTHS(A.DTA_REFERENCIA,-1) ELSE A.DTA_REFERENCIA END) BETWEEN TO_DATE(SUBSTR(B.DTA_INICIO_REF,0,10),'yyyy-MM-dd') AND TO_DATE(SUBSTR(B.DTA_FIM_REF,0,10),'yyyy-MM-dd') \
		                  					   AND B.SUBMERCADO = A.Submercado)")

TbFinalSazoStep3.createOrReplaceTempView("TAB_FINAL_STEP_SAZO_3")

TbFinalSazoStep4 = spark.sql("SELECT A.DTA_REFERENCIA, \
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
		                  FROM TAB_FINAL_STEP_SAZO_3 AS A \
		                  LEFT JOIN (SELECT MAX(DTA_FIM_REF) AS MAX_DATA, 1 AS DCIDE FROM DCIDE) AS B ON (A.DCIDE = B.DCIDE)")

TbFinalSazoStep4.createOrReplaceTempView("TAB_FINAL_STEP_SAZO_4")

TbFinalSazoStep5 = spark.sql("SELECT A.DTA_REFERENCIA, \
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
		                  FROM TAB_FINAL_STEP_SAZO_4 AS A \
		                  LEFT JOIN DCIDE AS B ON (A.DTA_REF IS NULL AND B.DTA_FIM_REF = A.MAX_DATA AND B.SUBMERCADO = A.Submercado)")

TbFinalSazoStep6 = TbFinalSazoStep5.filter("DTA_REF is not null")

TbFinalSazoStep7 = fc_Pld_Submercado(TbFinalSazoStep6,TbFinalSazoStep6.Energia,TbFinalSazoStep6.DTA_REF, TbFinalSazoStep6.Periodo,TbFinalSazoStep6.C_M0,TbFinalSazoStep6.C_M1,TbFinalSazoStep6.C_M2,TbFinalSazoStep6.C_M3,TbFinalSazoStep6.C_A0,TbFinalSazoStep6.C_A1,TbFinalSazoStep6.C_A2,TbFinalSazoStep6.C_A3,TbFinalSazoStep6.C_A4,TbFinalSazoStep6.I_M0,TbFinalSazoStep6.I_M1,TbFinalSazoStep6.I_M2,TbFinalSazoStep6.I_M3,TbFinalSazoStep6.I_A0,TbFinalSazoStep6.I_A1,TbFinalSazoStep6.I_A2,TbFinalSazoStep6.I_A3,TbFinalSazoStep6.I_A4)

TbFinalSazoStep8 = TbFinalSazoStep7.coalesce(50)

TbFinalSazoStep8.createOrReplaceTempView("TAB_FINAL_STEP_SAZO_8")

TbFinalSazoStep9Ini = spark.sql("SELECT A.MAX_DATA, \
									A.Periodo, \
									MAX(DT_INI.DataVigencia) AS DT_INI_DataVigencia \
								FROM TAB_FINAL_STEP_SAZO_8 AS A \
								LEFT JOIN VW_SERIE_IPCA AS DT_INI ON TO_DATE(SUBSTR(DT_INI.DataVigencia,0,10), 'yyyy-MM-dd') < TO_DATE(SUBSTR(A.MAX_DATA ,0,10), 'yyyy-MM-dd') \
								GROUP BY A.MAX_DATA, \
										 A.Periodo")

TbFinalSazoStep9Ini.createOrReplaceTempView("TAB_FINAL_STEP_SAZO_9_INI")

TbFinalSazoStep9Fim = spark.sql("SELECT A.MAX_DATA, \
									A.Periodo, \
									MAX(DT_FIM.DataVigencia) AS DT_FIM_DataVigencia \
								FROM TAB_FINAL_STEP_SAZO_8 AS A \
								LEFT JOIN VW_SERIE_IPCA AS DT_FIM ON TO_DATE(SUBSTR(DT_FIM.DataVigencia,0,10), 'yyyy-MM-dd') < A.Periodo \
								GROUP BY A.MAX_DATA, \
										 A.Periodo")

TbFinalSazoStep9Fim.createOrReplaceTempView("TAB_FINAL_STEP_SAZO_9_FIM")

TbFinalSazoStep9 = spark.sql("SELECT A.DTA_REFERENCIA, \
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
						  FROM TAB_FINAL_STEP_SAZO_8 AS A \
						  LEFT JOIN TAB_FINAL_STEP_SAZO_9_INI AS DT_INI ON TO_DATE(SUBSTR(DT_INI.MAX_DATA ,0,10), 'yyyy-MM-dd') = TO_DATE(SUBSTR(A.MAX_DATA ,0,10), 'yyyy-MM-dd') AND DT_INI.Periodo = A.Periodo \
						  LEFT JOIN TAB_FINAL_STEP_SAZO_9_FIM AS DT_FIM ON DT_FIM.Periodo = A.Periodo AND TO_DATE(SUBSTR(DT_FIM.MAX_DATA ,0,10), 'yyyy-MM-dd') = TO_DATE(SUBSTR(A.MAX_DATA ,0,10), 'yyyy-MM-dd')")

TbFinalSazoStep9.createOrReplaceTempView("TAB_FINAL_STEP_SAZO_9")

TbFinalSazoStep10 = spark.sql("SELECT \
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
						  FROM TAB_FINAL_STEP_SAZO_9 AS A \
						  LEFT JOIN \
						    (SELECT VALOR, \
						            DataVigencia \
						     FROM VW_SERIE_IPCA) AS INI ON INI.DataVigencia = A.DT_INI_DataVigencia \
						  LEFT JOIN \
						    (SELECT VALOR, \
						            DataVigencia \
						     FROM VW_SERIE_IPCA) AS FIM ON FIM.DataVigencia = A.DT_FIM_DataVigencia")