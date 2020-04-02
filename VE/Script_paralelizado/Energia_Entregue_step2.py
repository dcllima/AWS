tmpEnergiaEntregueStep1 = spark.sql("SELECT CG.CodigoReferencia, \
							     	        CG.CLASSIFICACAO AS CLASSIFICACAO, \
							     	        EMPD.NomeFantasia AS COMPRADOR, \
							     	        EMPO.NomeFantasia AS VENDEDOR, \
							     	        CG.NATUREZA  AS NATUREZA, \
							     	        CG.ID_ADITIVO AS IDADITIVO, \
							     	        CG.ID_CONTRATO AS IDCONTRATO, \
							     	        EE.Agente AS IdAgente, \
							     	        EE.IdEmpresaOrigem AS IdEmpresaOrigem, \
							     	        EE.IdEmpresaDestino  AS IdEmpresaDestino, \
							     	        EE.IdAgenteOrigem AS IdAgenteOrigem, \
							     	        EE.IdAgenteDestino AS IdAgenteDestino, \
							     	        EE.IdPerfil AS IdPerfil, \
							     	        EE.IdSubmercado AS IdSubmercado, \
							     	        EE.InicioPeriodo  AS InicioIntervalo, \
							     	        EE.FimPeriodo AS FimIntervalo, \
							     	        EE.ANO AS ANO, \
							     	        EE.MES AS MES, \
							     	        EE.MWm AS MWm, \
							     	        1  AS QTD_PARTES, \
							     	        EE.MWm AS MWmRateado, \
							     	        EE.MWh AS MWh, \
							     	        EE.MWh AS MWhRateado, \
							     	        EE.InicioPeriodo AS INICIO_MES, \
							     	        EE.FimPeriodo AS FIM_MES, \
							     	        EE.InicioPeriodo AS pInicio, \
							     	        EE.FimPeriodo AS pFim, \
							     	        EE.PERFIL_ENERGIA AS PERFIL_ENERGIA, \
							     	        EE.SUBMERCADO  AS SUBMERCADO, \
							     	        EE.ValorPreco AS ValorPreco, \
							     	        CG.DTA_OPERACAO AS DTA_OPERACAO, \
							     	        NULL AS FLEX_MAX, \
							     	        NULL AS FLEX_MIN, \
							     	        EXTENSAO AS EXTENSAO, \
							     	        CG.SERIE, \
									 		CG.DIABASE, \
									 		CG.RECORRENTE, \
											CG.FLAG_ISENCAO, \
											EE.NumNotaFiscal, \
											EE.NumDocumento, \
											EE.CONT_EOL \
							     	 FROM TMP_CONTRATOS_GENE AS CG \
							     	 INNER JOIN VW_GENE_ENERGIA_ENTREGUE AS EE ON (CG.ID_ADITIVO = EE.IDADITIVO) \
							     	 LEFT JOIN VW_GENE_FLEXIBILIDADE AS F ON (CG.ID_ADITIVO = F.IDADITIVO \
							     	                                       AND EE.InicioPeriodo BETWEEN F.INICIOINTERVALO AND F.FIMINTERVALO) \
							     	 LEFT OUTER JOIN Empresas AS EMPO ON (EE.IdEmpresaOrigem = EMPO.Id) \
							     	 LEFT OUTER JOIN Empresas AS EMPD ON (EE.IdEmpresaDestino = EMPD.Id) \
							     	 WHERE CG.FIMVIGENCIACONTRATO >= EE.INICIOPERIODO")

tmpEnergiaEntregueStep1 = fc_Qtd_Horas_Periodo(tmpEnergiaEntregueStep1,tmpEnergiaEntregueStep1.pInicio,tmpEnergiaEntregueStep1.pFim)

tmpEnergiaEntregueStep2 = tmpEnergiaEntregueStep1.withColumnRenamed("Qtd_horas","HORAS_PERIODO")

tmpEnergiaEntregueStep2.createOrReplaceTempView("TMP_ENERGIA_ENTREGUE_STEP2")

tmpEnergiaEntregueStep3 = spark.sql("SELECT A.CodigoReferencia, \
									        A.CLASSIFICACAO, \
									        A.COMPRADOR, \
									        A.VENDEDOR, \
									        A.NATUREZA, \
									        A.IDADITIVO, \
									        A.IDCONTRATO, \
									        A.IdAgente, \
									        A.IdEmpresaOrigem, \
									        A.IdEmpresaDestino, \
									        A.IdAgenteOrigem, \
									        A.IdAgenteDestino, \
									        A.IdPerfil, \
									        A.IdSubmercado, \
									        A.InicioIntervalo, \
									        A.FimIntervalo, \
									        A.ANO, \
									        A.MES, \
									        A.MWm, \
									        A.QTD_PARTES, \
									        A.MWmRateado, \
									        A.MWh, \
									        A.MWhRateado, \
									        A.INICIO_MES, \
									        A.FIM_MES, \
									        A.PERFIL_ENERGIA, \
									        A.SUBMERCADO, \
									        A.ValorPreco, \
									        A.DTA_OPERACAO, \
									        A.FLEX_MAX, \
									        A.FLEX_MIN, \
									        A.EXTENSAO, \
							     	        A.SERIE, \
									 		A.DIABASE, \
									 		A.RECORRENTE, \
											A.FLAG_ISENCAO, \
									        A.HORAS_PERIODO, \
											A.NumNotaFiscal, \
											A.NumDocumento, \
											A.CONT_EOL, \
									        NVL(B.PRECO, B.CoeficientePLD * NVL(GP.VALOR,GPM.VALOR) + B.PrecoBase) AS PRECO, \
									        TO_DATE(substr(B.DataInicioVigencia,0,10), 'yyyy-MM-dd') AS DataInicioVigencia, \
									        TO_DATE(substr(B.DataFimVigencia,0,10), 'yyyy-MM-dd') AS DataFimVigencia \
									 FROM TMP_ENERGIA_ENTREGUE_STEP2 AS A \
									 LEFT JOIN FC_GET_PRECO AS B ON A.IDADITIVO = B.ID_CONTRATO \
									 AND A.INICIO_MES BETWEEN B.DataInicioVigencia AND B.DataFimVigencia \
									 LEFT JOIN FC_GET_PLD_MAX AS GPM ON GPM.IdSubmercado = A.IdSubmercado \
									 LEFT JOIN FC_GET_PLD AS GP ON GP.IdSubmercado = A.IdSubmercado \
									 AND A.InicioIntervalo = SUBSTR(GP.DATA,0,10)")

tmpEnergiaEntregueStep3.createOrReplaceTempView("TMP_ENERGIA_ENTREGUE_STEP3")

tmpEnergiaEntregueStep4 = spark.sql("SELECT DISTINCT NVL(TO_DATE(substr(AA.InicioValidade,0,10), 'yyyy-MM-dd'),TO_DATE(substr(B.DataInicioVigencia,0,10), 'yyyy-MM-dd')) AS INICIOVALIDADE, \
									                 NVL(LEAD(TO_DATE(substr(AA.InicioValidade,0,10), 'yyyy-MM-dd')) OVER ( \
									                                                                                       ORDER BY TO_DATE(substr(AA.InicioValidade,0,10), 'yyyy-MM-dd')),ADD_MONTHS(B.DataFimVigencia,1)) AS FIMVALIDADE, \
									                 AA.Id, \
									                 AA.DiaBase, \
									                 AA.DiaAutoAjustado, \
									                 AA.MesAutoAjustado, \
									                 AA.Peso, \
									                 AA.AtrasoReajuste, \
									                 B.CodigoReferencia, \
									                 B.CLASSIFICACAO, \
									                 B.COMPRADOR, \
									                 B.VENDEDOR, \
									                 B.NATUREZA, \
									                 B.IDADITIVO, \
									                 B.IDCONTRATO, \
									                 B.IdAgente, \
									                 B.IdEmpresaOrigem, \
									                 B.IdEmpresaDestino, \
									                 B.IdAgenteOrigem, \
									                 B.IdAgenteDestino, \
									                 B.IdPerfil, \
									                 B.IdSubmercado, \
									                 B.InicioIntervalo, \
									                 B.FimIntervalo, \
									                 B.ANO, \
									                 B.MES, \
									                 B.MWm, \
									                 B.QTD_PARTES, \
									                 B.MWmRateado, \
									                 B.MWh, \
									                 B.MWhRateado, \
									                 B.INICIO_MES, \
									                 B.FIM_MES, \
									                 B.PERFIL_ENERGIA, \
									                 B.SUBMERCADO, \
									                 B.ValorPreco, \
									                 B.DTA_OPERACAO, \
									                 B.FLEX_MAX, \
									                 B.FLEX_MIN, \
									                 B.EXTENSAO, \
							     	        		 B.SERIE, \
									 				 B.RECORRENTE, \
													 B.FLAG_ISENCAO, \
									                 B.HORAS_PERIODO, \
									                 B.PRECO, \
									                 B.DataInicioVigencia, \
									                 B.DataFimVigencia, \
													 B.NumNotaFiscal, \
													 B.NumDocumento, \
													 B.CONT_EOL \
									 FROM contratos AS C \
									 LEFT JOIN Aditivos AS A ON (C.ID = A.IdContrato) \
									 LEFT JOIN TMP_ENERGIA_ENTREGUE_STEP3 AS B ON A.ID = B.IDADITIVO \
									 LEFT OUTER JOIN Ajustes_Aditivo AS AA ON A.Id = AA.IdAditivo \
									 WHERE AA.IDTIPO = 6 \
									   AND B.InicioIntervalo >= NVL(TO_DATE(substr(AA.InicioValidade,0,10), 'yyyy-MM-dd'),B.DataInicioVigencia) \
									   AND B.InicioIntervalo < NVL(TO_DATE(substr(AA.InicioValidade,0,10), 'yyyy-MM-dd'),ADD_MONTHS(B.DataFimVigencia,1))")

tmpEnergiaEntregueStep4.createOrReplaceTempView("TMP_ENERGIA_ENTREGUE_STEP4")

tmpEnergiaEntregueStep5Ini = spark.sql("SELECT A.DiaBase, \
										       A.InicioIntervalo, \
										       MAX(DT_INI.DataVigencia) AS DT_INI_DataVigencia \
										FROM TMP_ENERGIA_ENTREGUE_STEP4 AS A \
										LEFT JOIN VW_SERIE_IPCA AS DT_INI ON TO_DATE(SUBSTR(DT_INI.DataVigencia,0,10), 'yyyy-MM-dd') < TO_DATE(SUBSTR(A.DiaBase,0,10), 'yyyy-MM-dd') \
										GROUP BY A.DiaBase, \
										         A.InicioIntervalo")

tmpEnergiaEntregueStep5Ini.createOrReplaceTempView("TMP_ENERGIA_ENTREGUE_STEP5_INI")


tmpEnergiaEntregueStep5Fim = spark.sql("SELECT A.DiaBase, \
										       A.InicioIntervalo, \
										       MAX(DT_FIM.DataVigencia) AS DT_FIM_DataVigencia \
										FROM TMP_ENERGIA_ENTREGUE_STEP4 AS A \
										LEFT JOIN VW_SERIE_IPCA AS DT_FIM ON TO_DATE(SUBSTR(DT_FIM.DataVigencia,0,10), 'yyyy-MM-dd') < TO_DATE(SUBSTR(A.InicioIntervalo,0,10), 'yyyy-MM-dd') \
										GROUP BY A.DiaBase, \
										         A.InicioIntervalo")

tmpEnergiaEntregueStep5Fim.createOrReplaceTempView("TMP_ENERGIA_ENTREGUE_STEP5_FIM")

tmpEnergiaEntregueStep5 = spark.sql("SELECT A.INICIOVALIDADE, \
									        A.FIMVALIDADE, \
									        A.Id, \
									        A.DiaBase, \
									        A.DiaAutoAjustado, \
									        A.MesAutoAjustado, \
									        A.Peso, \
									        A.AtrasoReajuste, \
									        A.CodigoReferencia, \
									        A.CLASSIFICACAO, \
									        A.COMPRADOR, \
									        A.VENDEDOR, \
									        A.NATUREZA, \
									        A.IDADITIVO, \
									        A.IDCONTRATO, \
									        A.IdAgente, \
									        A.IdEmpresaOrigem, \
									        A.IdEmpresaDestino, \
									        A.IdAgenteOrigem, \
									        A.IdAgenteDestino, \
									        A.IdPerfil, \
									        A.IdSubmercado, \
									        A.InicioIntervalo, \
									        A.FimIntervalo, \
									        A.ANO, \
									        A.MES, \
									        A.MWm, \
									        A.QTD_PARTES, \
									        A.MWmRateado, \
									        A.MWh, \
									        A.MWhRateado, \
									        A.INICIO_MES, \
									        A.FIM_MES, \
									        A.PERFIL_ENERGIA, \
									        A.SUBMERCADO, \
									        A.ValorPreco, \
									        A.DTA_OPERACAO, \
									        A.FLEX_MAX, \
									        A.FLEX_MIN, \
									        A.EXTENSAO, \
							     	        A.SERIE, \
									 		A.RECORRENTE, \
											A.FLAG_ISENCAO, \
									        A.HORAS_PERIODO, \
									        A.PRECO, \
									        A.DataInicioVigencia, \
									        A.DataFimVigencia, \
											A.NumNotaFiscal, \
											A.NumDocumento, \
											A.CONT_EOL, \
									        DT_INI.DT_INI_DataVigencia, \
									        DT_FIM.DT_FIM_DataVigencia \
									 FROM TMP_ENERGIA_ENTREGUE_STEP4 AS A \
									 LEFT JOIN TMP_ENERGIA_ENTREGUE_STEP5_INI AS DT_INI ON TO_DATE(SUBSTR(DT_INI.DiaBase,0,10), 'yyyy-MM-dd') = TO_DATE(SUBSTR(A.DiaBase,0,10), 'yyyy-MM-dd') and TO_DATE(SUBSTR(DT_INI.InicioIntervalo,0,10), 'yyyy-MM-dd') = TO_DATE(SUBSTR(A.InicioIntervalo,0,10), 'yyyy-MM-dd')\
									 LEFT JOIN TMP_ENERGIA_ENTREGUE_STEP5_FIM AS DT_FIM ON TO_DATE(SUBSTR(DT_FIM.InicioIntervalo,0,10), 'yyyy-MM-dd') = TO_DATE(SUBSTR(A.InicioIntervalo,0,10), 'yyyy-MM-dd') and TO_DATE(SUBSTR(DT_INI.DiaBase,0,10), 'yyyy-MM-dd') = TO_DATE(SUBSTR(A.DiaBase,0,10), 'yyyy-MM-dd') ")

tmpEnergiaEntregueStep5.createOrReplaceTempView("TMP_ENERGIA_ENTREGUE_STEP5")

tmpEnergiaEntregueStep6 = spark.sql("SELECT A.INICIOVALIDADE, \
									        A.FIMVALIDADE, \
									        A.Id, \
									        SUBSTR(A.DiaBase,0,10) AS DiaBase, \
									        A.DiaAutoAjustado, \
									        A.MesAutoAjustado, \
									        SUBSTR(A.Peso,0,1) AS Peso, \
									        SUBSTR(A.AtrasoReajuste,0,10) AS AtrasoReajuste, \
									        A.CodigoReferencia, \
									        A.CLASSIFICACAO, \
									        A.COMPRADOR, \
									        A.VENDEDOR, \
									        A.NATUREZA, \
									        A.IDADITIVO, \
									        A.IDCONTRATO, \
									        A.IdAgente, \
									        A.IdEmpresaOrigem, \
									        A.IdEmpresaDestino, \
									        A.IdAgenteOrigem, \
									        A.IdAgenteDestino, \
									        A.IdPerfil, \
									        A.IdSubmercado, \
									        A.InicioIntervalo, \
									        A.InicioIntervalo as DtRef, \
									        A.FimIntervalo, \
									        A.ANO, \
									        A.MES, \
									        A.MWm, \
									        A.QTD_PARTES, \
									        A.MWmRateado, \
									        A.MWh, \
									        A.MWhRateado, \
									        A.INICIO_MES, \
									        A.FIM_MES, \
									        A.PERFIL_ENERGIA, \
									        A.SUBMERCADO, \
									        A.ValorPreco, \
									        A.DTA_OPERACAO, \
									        A.FLEX_MAX, \
									        A.FLEX_MIN, \
									        A.EXTENSAO, \
							     	        A.SERIE as SERIE1, \
									 		A.RECORRENTE, \
											A.FLAG_ISENCAO, \
									        A.HORAS_PERIODO, \
									        A.PRECO, \
									        A.DataInicioVigencia, \
									        A.DataFimVigencia, \
											A.NumNotaFiscal, \
											A.NumDocumento, \
											A.CONT_EOL, \
									        A.DT_INI_DataVigencia, \
									        A.DT_FIM_DataVigencia, \
									        INI.VALOR AS IniValor, \
									        FIM.VALOR AS FimValor \
									 FROM TMP_ENERGIA_ENTREGUE_STEP5 AS A \
									 LEFT JOIN \
									   (SELECT VALOR, \
									           DataVigencia \
									    FROM VW_SERIE_IPCA) AS INI ON INI.DataVigencia = A.DT_INI_DataVigencia \
									 LEFT JOIN \
									   (SELECT VALOR, \
									           DataVigencia \
									    FROM VW_SERIE_IPCA) AS FIM ON FIM.DataVigencia = A.DT_FIM_DataVigencia")

tmpEnergiaEntregueStep7 = fc_Preco(tmpEnergiaEntregueStep6,tmpEnergiaEntregueStep6.DtRef,tmpEnergiaEntregueStep6.AtrasoReajuste,tmpEnergiaEntregueStep6.MesAutoAjustado,tmpEnergiaEntregueStep6.DiaAutoAjustado,tmpEnergiaEntregueStep6.DiaBase,tmpEnergiaEntregueStep6.PRECO,tmpEnergiaEntregueStep6.Peso,tmpEnergiaEntregueStep6.Id,tmpEnergiaEntregueStep6.IniValor,tmpEnergiaEntregueStep6.FimValor)

tmpEnergiaEntregueStep7 = tmpEnergiaEntregueStep7.withColumnRenamed("precoIntermediario","ValorPrecoFinal")

tmpEnergiaEntregueStep7.createOrReplaceTempView("TMP_ENERGIA_ENTREGUE_STEP7")

tmpEnergiaEntregueStep8 = spark.sql("SELECT A.INICIOVALIDADE, \
								       A.FIMVALIDADE, \
								       A.Id, \
								       A.DiaBase, \
								       A.DiaAutoAjustado, \
								       A.MesAutoAjustado, \
								       A.Peso, \
								       A.AtrasoReajuste, \
								       A.CodigoReferencia, \
								       A.CLASSIFICACAO, \
								       A.COMPRADOR, \
								       A.VENDEDOR, \
								       A.NATUREZA, \
								       A.IDADITIVO, \
								       A.IDCONTRATO, \
								       A.IdAgente, \
								       A.IdEmpresaOrigem, \
								       A.IdEmpresaDestino, \
								       A.IdAgenteOrigem, \
								       A.IdAgenteDestino, \
								       A.IdPerfil, \
								       A.IdSubmercado, \
								       A.InicioIntervalo, \
								       A.FimIntervalo, \
								       A.ANO, \
								       A.MES, \
								       A.MWm, \
								       A.QTD_PARTES, \
								       A.MWmRateado, \
								       A.MWh, \
								       A.MWhRateado, \
								       A.INICIO_MES, \
								       A.FIM_MES, \
								       A.PERFIL_ENERGIA, \
								       A.SUBMERCADO, \
								       NVL(A.ValorPreco, A.ValorPrecoFinal) AS ValorPreco, \
								       A.DTA_OPERACAO, \
								       A.FLEX_MAX, \
								       A.FLEX_MIN, \
								       A.EXTENSAO, \
							     	   A.SERIE1 as SERIE, \
									   A.RECORRENTE, \
									   A.FLAG_ISENCAO, \
								       A.HORAS_PERIODO, \
								       A.PRECO, \
								       A.DataInicioVigencia, \
								       A.DataFimVigencia, \
									   A.NumNotaFiscal, \
									   A.NumDocumento, \
								       A.DT_INI_DataVigencia, \
								       A.DT_FIM_DataVigencia, \
								       A.IniValor, \
								       A.FimValor, \
								       CASE \
										WHEN A.IdEmpresaDestino = 3 THEN A.IdEmpresaOrigem \
										ELSE A.IdEmpresaDestino END AS IdEmpresa \
								FROM TMP_ENERGIA_ENTREGUE_STEP7 AS A \
								where A.CONT_EOL = 'N'")

tmpEnergiaEntregueStep9 = spark.sql("SELECT A.INICIOVALIDADE, \
								       A.FIMVALIDADE, \
								       A.Id, \
								       A.DiaBase, \
								       A.DiaAutoAjustado, \
								       A.MesAutoAjustado, \
								       A.Peso, \
								       A.AtrasoReajuste, \
								       A.CodigoReferencia, \
								       A.CLASSIFICACAO, \
								       A.COMPRADOR, \
								       A.VENDEDOR, \
								       A.NATUREZA, \
								       A.IDADITIVO, \
								       A.IDCONTRATO, \
								       A.IdAgente, \
								       A.IdEmpresaOrigem, \
								       A.IdEmpresaDestino, \
								       A.IdAgenteOrigem, \
								       A.IdAgenteDestino, \
								       A.IdPerfil, \
								       A.IdSubmercado, \
								       A.InicioIntervalo, \
								       A.FimIntervalo, \
								       A.ANO, \
								       A.MES, \
								       A.MWm, \
								       A.QTD_PARTES, \
								       A.MWmRateado, \
								       A.MWh, \
								       A.MWhRateado, \
								       A.INICIO_MES, \
								       A.FIM_MES, \
								       A.PERFIL_ENERGIA, \
								       A.SUBMERCADO, \
								       NVL(A.ValorPreco, A.ValorPrecoFinal) AS ValorPreco, \
								       A.DTA_OPERACAO, \
								       A.FLEX_MAX, \
								       A.FLEX_MIN, \
								       A.EXTENSAO, \
							     	   A.SERIE1 as SERIE, \
									   A.RECORRENTE, \
									   A.FLAG_ISENCAO, \
								       A.HORAS_PERIODO, \
								       A.PRECO, \
								       A.DataInicioVigencia, \
								       A.DataFimVigencia, \
									   MAX(A.NumNotaFiscal) AS NumNotaFiscal, \
									   MAX(A.NumDocumento) AS NumDocumento, \
								       A.DT_INI_DataVigencia, \
								       A.DT_FIM_DataVigencia, \
								       A.IniValor, \
								       A.FimValor, \
								       CASE \
										WHEN A.IdEmpresaDestino = 3 THEN A.IdEmpresaOrigem \
										ELSE A.IdEmpresaDestino END AS IdEmpresa \
								FROM TMP_ENERGIA_ENTREGUE_STEP7 AS A \
								where A.CONT_EOL = 'S' \
								GROUP BY  A.INICIOVALIDADE, \
								       A.FIMVALIDADE, \
								       A.Id, \
								       A.DiaBase, \
								       A.DiaAutoAjustado, \
								       A.MesAutoAjustado, \
								       A.Peso, \
								       A.AtrasoReajuste, \
								       A.CodigoReferencia, \
								       A.CLASSIFICACAO, \
								       A.COMPRADOR, \
								       A.VENDEDOR, \
								       A.NATUREZA, \
								       A.IDADITIVO, \
								       A.IDCONTRATO, \
								       A.IdAgente, \
								       A.IdEmpresaOrigem, \
								       A.IdEmpresaDestino, \
								       A.IdAgenteOrigem, \
								       A.IdAgenteDestino, \
								       A.IdPerfil, \
								       A.IdSubmercado, \
								       A.InicioIntervalo, \
								       A.FimIntervalo, \
								       A.ANO, \
								       A.MES, \
								       A.MWm, \
								       A.QTD_PARTES, \
								       A.MWmRateado, \
								       A.MWh, \
								       A.MWhRateado, \
								       A.INICIO_MES, \
								       A.FIM_MES, \
								       A.PERFIL_ENERGIA, \
								       A.SUBMERCADO, \
								       NVL(A.ValorPreco, A.ValorPrecoFinal), \
								       A.DTA_OPERACAO, \
								       A.FLEX_MAX, \
								       A.FLEX_MIN, \
								       A.EXTENSAO, \
							     	   A.SERIE1, \
									   A.RECORRENTE, \
									   A.FLAG_ISENCAO, \
								       A.HORAS_PERIODO, \
								       A.PRECO, \
								       A.DataInicioVigencia, \
								       A.DataFimVigencia, \
								       A.DT_INI_DataVigencia, \
								       A.DT_FIM_DataVigencia, \
								       A.IniValor, \
								       A.FimValor, \
								       CASE \
										WHEN A.IdEmpresaDestino = 3 THEN A.IdEmpresaOrigem \
										ELSE A.IdEmpresaDestino END")

tmpEnergiaEntregue = tmpEnergiaEntregueStep9.union(tmpEnergiaEntregueStep8)

tmpEnergiaEntregue.createOrReplaceTempView("TMP_ENERGIA_ENTREGUE")

vwBalancoServicosfinal = spark.sql("SELECT S2.ID_ADITIVO, \
									       S2.CODIGOREFERENCIA, \
									       S2.INICIO_MES, \
									       S2.EXTENSAO, \
									       S2.CLASSIFICACAO, \
									       S2.COMPRADOR, \
									       S2.VENDEDOR, \
									       S2.NATUREZA, \
									       CASE \
									           WHEN S2.DESCRICAO = 'MWméd' THEN S2.VALOR \
									           ELSE CASE \
									                    WHEN S2.DESCRICAO = 'MWh' THEN S2.VALOR/S2.horas \
									                    ELSE 1 \
									                END \
									       END AS MWM, \
									       CASE \
									           WHEN S2.DESCRICAO = 'MWméd' THEN S2.VALOR * S2.horas \
									           ELSE CASE \
									                    WHEN S2.DESCRICAO = 'MWh' THEN S2.VALOR \
									                    ELSE 1 \
									                END \
									       END AS MWH, \
									       S2.DESCRICAO, \
									       S2.PRECO, \
									       CASE \
									           WHEN S2.DESCRICAO = 'MWméd' THEN S2.valor * S2.preco * S2.horas \
									           ELSE S2.valor * S2.preco \
									       END AS total , \
									       S2.horas, \
									       S2.DataAcordoComercial \
									FROM VW_GENE_BALANCO_SERVICOS_STEP2 S2 \
									LEFT JOIN VW_GENE_BALANCO_SERVICOS_STEP3 S3 ON (S2.ID_ADITIVO = S3.ID_ADITIVO \
									                                                AND S2.INICIO_MES = S3.DTA_INICIO) \
									LEFT JOIN \
									  (SELECT E1.IDADITIVO, \
									          E1.ANO, \
									          E1.MES, \
									          E1.INICIO_MES \
									   FROM TMP_ENERGIA_ENTREGUE E1 \
									   WHERE E1.NATUREZA IN ('Venda', \
									                         'Compra')) B ON (s2.ID_ADITIVO = B.IDADITIVO \
									                                          AND S2.INICIO_MES = B.INICIO_MES) \
									WHERE S3.ID_ADITIVO IS NULL \
									  AND B.IDADITIVO IS NULL ")

vwBalancoServicosfinal.createOrReplaceTempView("VW_GENE_BALANCO_SERVICOS_FINAL")

tmpEnergiaEntregueFinal = spark.sql("SELECT current_date() AS DTA_REFERENCIA, \
									        CodigoReferencia AS Contrato_Operacao , \
									        CLASSIFICACAO AS Tipo , \
									        CASE \
									            WHEN NATUREZA = 'Venda' THEN COMPRADOR ELSE VENDEDOR \
									        END AS Contraparte , \
									        Cnpj, \
									        NATUREZA AS Natureza, \
									        HORAS_PERIODO AS MesHora,MWmRateado AS MWm , \
									        MWmRateado * HORAS_PERIODO AS MWh , \
									        INICIO_MES AS Periodo,ANO, MES,PERFIL_ENERGIA AS Energia,SUBMERCADO AS Submercado , \
									        ValorPreco AS Preco,'x' AS Join , \
									        NVL(UF.Sigla, CASE SUBMERCADO WHEN 'Sul' THEN 'SC' WHEN 'Norte' THEN 'AM' WHEN 'Nordeste' THEN 'BA' WHEN 'Sudeste/Centro-oeste' THEN 'SP' END) AS UF , \
									        DTA_OPERACAO, \
									        FLEX_MAX, \
									        FLEX_MIN, \
									        EXTENSAO, \
									        E.SERIE, \
									   		E.DIABASE, \
									   		E.RECORRENTE, \
									   		E.FLAG_ISENCAO, \
									   		E.NumNotaFiscal, \
									   		E.NumDocumento, \
									   		NVL(P.PRECO, P.CoeficientePLD * NVL(GP.VALOR,GPM.VALOR) + P.PrecoBase) AS PRECO_BASE,\
									 		E.IDADITIVO \
									 FROM UF_EMPRESA UF \
									 RIGHT JOIN TMP_ENERGIA_ENTREGUE E ON (E.IdEmpresa= UF.IdEmpresa) \
									 LEFT JOIN Empresas emp ON (E.IdEmpresa = emp.Id) \
									 LEFT JOIN FC_GET_PRECO P ON E.IDADITIVO = P.ID_CONTRATO AND E.INICIO_MES BETWEEN P.DataInicioVigencia AND P.DataFimVigencia \
									 LEFT JOIN FC_GET_PLD_MAX AS GPM ON GPM.IdSubmercado = E.IdSubmercado \
									 LEFT JOIN FC_GET_PLD AS GP ON GP.IdSubmercado = E.IdSubmercado \
									 WHERE NATUREZA IN ('Venda', \
									                    'Compra')")

tmpEnergiaEntregueFinal = tmpEnergiaEntregueFinal.withColumn("IdSubmercado",
													when(tmpEnergiaEntregueFinal.Submercado == lit('Sudeste/Centro-oeste'), lit(1))
		   										   .when(tmpEnergiaEntregueFinal.Submercado == lit('Sul'), lit(2))
           										   .when(tmpEnergiaEntregueFinal.Submercado == lit('Norte'), lit(3))
           										   .when(tmpEnergiaEntregueFinal.Submercado == lit('Nordeste'), lit(8))
           										   .otherwise(lit(1)))

tmpEnergiaEntregueFinal.createOrReplaceTempView("TMP_ENERGIA_ENTREGUE_FINAL")