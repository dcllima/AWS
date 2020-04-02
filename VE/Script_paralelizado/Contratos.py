#cria VW_GENE_CONTRATOS_NEW
vwContratosCont = spark.sql("SELECT A.Id AS ID_ADITIVO, \
       								C.Id AS ID_CONTRATO, \
       								C.CodigoReferencia, \
       								NVL(A.InicioValidade, A.InicioVigenciaContrato) AS INICIOVIGENCIACONTRATO, \
       								NVL(EA.DataDesativacao, NVL(A.FimValidade, A.FimVigenciaContrato)) AS FIMVIGENCIACONTRATO, \
       								CASE \
           								WHEN C.TIPOPERIODOVIGENCIA = 1 THEN 'Curto Prazo' \
           								ELSE 'Longo Prazo' \
       								END AS EXTENSAO, \
       								CT.Nome AS CLASSIFICACAO, \
       								C.CodigoReferencia, \
       								NVL(C.DataAcordoComercial, C.DataCriacao) AS DataAcordoComercial, \
       								IPC.NOME AS FLAG_ISENCAO \
								FROM CategoriasContratos AS CTC \
								INNER JOIN Categorias AS CT ON CTC.IdCategoria = CT.Id \
								INNER JOIN Contratos AS C ON C.Id = CTC.IdContrato \
								LEFT OUTER JOIN Aditivos AS A ON C.Id = A.IdContrato \
								LEFT OUTER JOIN AUX_JOIN_ENTIDADES_ATIVAVEIS AS EA ON C.Id = EA.IdEntidade \
								LEFT JOIN VW_ISENTOS_PIS_COFINS IPC ON (A.ID = IPC.ID) \
								WHERE (CT.Grupo = 'Balanço') \
  								AND C.DADOSPROPOSTA = 'false'")

vwContratosPropostas = spark.sql("SELECT A.Id AS ID_ADITIVO, \
       									C.Id AS ID_CONTRATO, \
       									C.CodigoReferencia, \
       									SUBSTR(NVL(A.InicioValidade, A.InicioVigenciaContrato),0,10) AS INICIOVIGENCIACONTRATO, \
       									SUBSTR(NVL(EA.DataDesativacao, NVL(A.FimValidade, A.FimVigenciaContrato)),0,10) AS FIMVIGENCIACONTRATO, \
       									CASE \
           									WHEN C.TIPOPERIODOVIGENCIA = 1 THEN 'Curto Prazo' \
           									ELSE 'Longo Prazo' \
       									END AS EXTENSAO, \
       									CT.Nome AS CLASSIFICACAO, \
       									C.CodigoReferencia, \
       									SUBSTR(NVL(C.DataAcordoComercial, C.DataCriacao),0,10) AS DataAcordoComercial, \
       									IPC.NOME AS FLAG_ISENCAO \
									FROM CategoriasContratos AS CTC \
									INNER JOIN Categorias AS CT ON CTC.IdCategoria = CT.Id \
									INNER JOIN Contratos AS C ON C.Id = CTC.IdContrato \
									LEFT OUTER JOIN Aditivos AS A ON C.Id = A.IdContrato \
									LEFT OUTER JOIN AUX_JOIN_ENTIDADES_ATIVAVEIS AS EA ON C.Id = EA.IdEntidade \
									LEFT OUTER JOIN ContratosAprovaveis CA ON (C.IDCONTRATOAPROVAVEL = CA.IDENTIDADEAPROVAVEL) \
									LEFT OUTER JOIN EntidadesAprovaveis EAP ON (CA.IDENTIDADEAPROVAVEL = EAP.Id) \
									LEFT JOIN VW_ISENTOS_PIS_COFINS IPC ON (A.ID = IPC.ID) \
									WHERE (CT.Grupo = 'Balanço') \
  									AND C.DADOSPROPOSTA = 'true' \
  									AND C.ID NOT IN \
    									(SELECT NVL(IDPROPOSTAORIGINAL,0) \
     									FROM Contratos \
     									WHERE DADOSPROPOSTA = 'false') \
  									AND DATAAPROVACAOREPROVACAO IS NULL")

vwContratosStep1 = vwContratosCont.unionAll(vwContratosPropostas)

vwContratosStep1.createOrReplaceTempView("VW_GENE_CONTRATOS_NEW_STEP1")

vwContratos = spark.sql("SELECT ID_ADITIVO, \
							       ID_CONTRATO, \
							       A.CodigoReferencia, \
							       INICIOVIGENCIACONTRATO, \
							       FIMVIGENCIACONTRATO, \
							       EXTENSAO, \
							       CLASSIFICACAO, \
							       D.NOMEFANTASIA AS COMPRADOR, \
							       E.NOMEFANTASIA AS VENDEDOR, \
							       B.Natureza, \
							       DataAcordoComercial, \
							       C.descricao AS SERIE, \
							       G.DIABASE, \
							       F.RECORRENTE, \
							       FLAG_ISENCAO \
							FROM VW_GENE_CONTRATOS_NEW_STEP1 A \
							LEFT JOIN FC_GET_NATUREZA B ON A.ID_ADITIVO = B.IDADITIVO \
							LEFT JOIN FC_GET_SERIE_REAJUSTE C ON A.ID_ADITIVO = C.ID \
							LEFT JOIN FC_GET_COMPRADOR_VENDEDOR D ON A.CodigoReferencia = D.CodigoReferencia AND (CASE WHEN B.Natureza = 'Serviços' THEN 'CS' ELSE 'C' END) = D.TIPO\
							LEFT JOIN FC_GET_COMPRADOR_VENDEDOR E ON A.CodigoReferencia = E.CodigoReferencia AND (CASE WHEN B.Natureza = 'Serviços' THEN 'VS' ELSE 'V' END) = E.TIPO \
							LEFT JOIN FC_GET_RECORRENCIA_REAJUSTE F ON A.ID_ADITIVO = F.ID\
							LEFT JOIN FC_GET_DIABASE_REAJUSTE G ON A.ID_ADITIVO = G.ID")

vwContratos = vwContratos.withColumn('COMPRADOR', concat_ws(',', 'COMPRADOR')) \
	.withColumn('VENDEDOR', concat_ws(',', 'VENDEDOR'))

vwContratos.createOrReplaceTempView("VW_GENE_CONTRATOS_NEW")


vwBalancoServicosStep1 = spark.sql("SELECT ID_ADITIVO, \
									       ID_CONTRATO, \
									       CODIGOREFERENCIA, \
									       NVL(INICIOINTERVALO, INICIOVIGENCIACONTRATO) AS DTA_INICIO, \
									       NVL(FIMINTERVALO, FIMVIGENCIACONTRATO) AS DTA_FIM, \
									       EXTENSAO, \
									       CLASSIFICACAO, \
									       COMPRADOR, \
									       VENDEDOR, \
									       NATUREZA, \
									       NVL(VALOR,1) AS VALOR, \
									       NVL(DESCRICAO,'R$') AS DESCRICAO, \
									       CASE \
									           WHEN NATUREZA = 'Serviços' THEN CC8.ID \
									           ELSE EN.IDCARACTERISTICA \
									       END AS IDCARACTERISTICA, \
									       cg.DataAcordoComercial, \
									       CG.SERIE, \
										   CG.DIABASE, \
										   CG.RECORRENTE, \
										   CG.FLAG_ISENCAO \
									FROM VW_GENE_CONTRATOS_NEW CG \
									INNER JOIN \
									  (SELECT CC.IdAditivo, \
									          DEF.InicioIntervalo, \
									          DEF.FimIntervalo, \
									          DEF.Valor, \
									          UE.Descricao, \
									          CC.ID AS IDCARACTERISTICA \
									   FROM CaracteristicasContrato CC \
									   INNER JOIN DadosEnergiaFixa AS DEF ON cc.Id = DEF.IdCaracteristicaEnergia \
									   INNER JOIN UnidadesEnergia AS UE ON CASE \
									                                                               WHEN DEF.UnidadeValor = 3 THEN 2 \
									                                                               ELSE DEF.UnidadeValor \
									                                                           END = UE.Id \
									   WHERE CC.IdTipo IN (2)) AS EN ON (CG.ID_ADITIVO = EN.IdAditivo) \
									LEFT JOIN \
									  (SELECT IDADITIVO, \
									          ID \
									   FROM CaracteristicasContrato \
									   WHERE IdTipo IN (8)) AS CC8 ON (CG.ID_ADITIVO = CC8.IDADITIVO) \
									WHERE NATUREZA IN ('Serviços', \
									                   'Compra') \
									  AND CLASSIFICACAO = 'Serviços' \
									UNION ALL \
									SELECT ID_ADITIVO, \
									       ID_CONTRATO, \
									       CodigoReferencia, \
									       NVL(CASE WHEN INICIOINTERVALO < INICIOVIGENCIACONTRATO THEN INICIOVIGENCIACONTRATO ELSE INICIOINTERVALO END, INICIOVIGENCIACONTRATO) AS DTA_INICIO, \
									       NVL(FIMINTERVALO, FIMVIGENCIACONTRATO) AS DTA_FIM, \
									       EXTENSAO, \
									       CLASSIFICACAO, \
									       COMPRADOR, \
									       VENDEDOR, \
									       NATUREZA, \
									       1 AS VALOR, \
									       'R$' AS DESCRICAO, \
									       COM.ID AS IDCARACTERISTICA, \
									       cg.DataAcordoComercial, \
									       CG.SERIE, \
										   CG.DIABASE, \
										   CG.RECORRENTE, \
										   CG.FLAG_ISENCAO \
									FROM VW_GENE_CONTRATOS_NEW CG \
									INNER JOIN \
									  (SELECT CC.IdAditivo, \
									          DPC.InicioIntervalo, \
									          DPC.FimIntervalo, \
									          CC.Id \
									   FROM CaracteristicasContrato CC \
									   INNER JOIN CaracteristicasComissao AS CM ON (CC.Id = CM.IdCaracteristica) \
									   INNER JOIN DadosPrecoComissao AS DPC ON (CM.IdCaracteristica = DPC.IdCaracteristicaPreco) \
									   WHERE IdTipo IN (8) \
									     AND ParcelaFixaEnergia = 0) AS COM \
									 ON (CG.ID_ADITIVO = COM.IdAditivo \
									     AND NVL(COM.InicioIntervalo,CG.INICIOVIGENCIACONTRATO) BETWEEN CG.INICIOVIGENCIACONTRATO AND CG.FIMVIGENCIACONTRATO) \
									WHERE NATUREZA IN ('Serviços', \
									                   'Compra') \
									  AND CLASSIFICACAO = 'Serviços' ")

vwBalancoServicosStep1.createOrReplaceTempView("VW_GENE_BALANCO_SERVICOS_STEP1")

vwBalancoServicosStep2Aux = spark.sql("SELECT int(ID_ADITIVO), \
										       ID_CONTRATO, \
										       CODIGOREFERENCIA, \
										       IDCARACTERISTICA, \
										       SUBSTR(DTA_INICIO,0,10) AS DTA_INICIO, \
										       SUBSTR(DTA_FIM,0,10) AS DTA_FIM, \
										       CEIL(MONTHS_BETWEEN(DTA_FIM,DTA_INICIO)) AS Qtd_Meses, \
										       EXTENSAO, \
										       CLASSIFICACAO, \
										       COMPRADOR, \
										       VENDEDOR, \
										       CASE \
										           WHEN COMPRADOR LIKE '%VOTENER%' THEN 'Compra' \
										           ELSE 'Venda' \
										       END AS NATUREZA, \
										       VALOR, \
										       DESCRICAO, \
										       DataAcordoComercial, \
										       SERIE, \
										   	   DIABASE, \
										   	   RECORRENTE, \
										   	   FLAG_ISENCAO \
										FROM VW_GENE_BALANCO_SERVICOS_STEP1 S1")

vwBalancoServicosStep2Rang = vwBalancoServicosStep2Aux.withColumn("records", fc_Range_Dat_UDF(vwBalancoServicosStep2Aux.DTA_INICIO,vwBalancoServicosStep2Aux.Qtd_Meses) ) \
	.withColumn("DtRef", explode("records")) \
	.drop("records")

vwBalancoServicosStep2Rang = fc_Qtd_Horas_Mes(vwBalancoServicosStep2Rang,vwBalancoServicosStep2Rang.DtRef)

vwBalancoServicosStep2Rang = vwBalancoServicosStep2Rang.withColumnRenamed("Qtd_horas","horas")

vwBalancoServicosStep2Data = vwBalancoServicosStep2Rang.withColumn("INICIO_MES",vwBalancoServicosStep2Rang.DtRef) \
	.withColumn("FIM_MES",last_day(vwBalancoServicosStep2Rang.DtRef))

vwBalancoServicosStep2Data.createOrReplaceTempView("VW_GENE_BALANCO_SERVICOS_STEP2_DATA")

vwBalancoServicosStep2PrecoComStep1 = spark.sql("SELECT A.ID_ADITIVO, \
											       A.ID_CONTRATO, \
											       A.CODIGOREFERENCIA, \
											       A.IDCARACTERISTICA, \
											       A.DTA_INICIO, \
											       A.DTA_FIM, \
											       A.Qtd_Meses, \
											       A.EXTENSAO, \
											       A.CLASSIFICACAO, \
											       A.COMPRADOR, \
											       A.VENDEDOR, \
											       A.NATUREZA, \
											       A.VALOR, \
											       A.DESCRICAO, \
											       A.DataAcordoComercial, \
											       A.SERIE, \
										   	   	   A.DIABASE, \
										   	   	   A.RECORRENTE, \
										   	   	   A.FLAG_ISENCAO, \
											       A.DtRef, \
											       A.INICIO_MES, \
											       A.FIM_MES, \
											       A.horas, \
											       CASE \
											           WHEN NVL(A.DESCRICAO, 'R$') = 'R$' THEN B.ParcelaFixa \
											           ELSE B.ParcelaFixaEnergia \
											       END AS PRECO \
											FROM VW_GENE_BALANCO_SERVICOS_STEP2_DATA AS A \
											JOIN FC_GET_PRECO_COMISSAO_PRECO AS B \
											    ON int(A.ID_ADITIVO) = B.ID \
											        AND A.IDCARACTERISTICA = B.Id_CCENER \
											        AND A.DtRef BETWEEN NVL(CASE WHEN B.InicioIntervalo < B.InicioVigenciaContrato THEN B.InicioVigenciaContrato ELSE B.InicioIntervalo END, B.InicioVigenciaContrato) \
											                        AND NVL(B.FimIntervalo, B.FimVigenciaContrato)")

vwBalancoServicosStep2PrecoComStep1.createOrReplaceTempView("VW_GENE_BALANCO_SERVICOS_STEP2_PRECO_COMISSAO_STEP1")

vwBalancoServicosStep2PrecoComStep2 = spark.sql("SELECT A.ID_ADITIVO, \
											      	 A.ID_CONTRATO, \
											      	 A.CODIGOREFERENCIA, \
											      	 A.IDCARACTERISTICA, \
											      	 A.DTA_INICIO, \
											      	 A.DTA_FIM, \
											      	 A.Qtd_Meses, \
											      	 A.EXTENSAO, \
											      	 A.CLASSIFICACAO, \
											      	 A.COMPRADOR, \
											      	 A.VENDEDOR, \
											      	 A.NATUREZA, \
											      	 A.VALOR, \
											      	 A.DESCRICAO, \
											      	 A.DataAcordoComercial, \
											       	 A.SERIE, \
										   	   	     A.RECORRENTE, \
										   	   	   	 A.FLAG_ISENCAO, \
											      	 A.DtRef, \
											      	 A.INICIO_MES, \
											      	 A.FIM_MES, \
											      	 A.horas, \
													 A.PRECO, \
													 B.Id, \
													 SUBSTR(B.DiaBase,0,10) as DiaBase, \
													 CASE WHEN LENGTH(B.DiaAutoAjustado) = 1 THEN '0' || B.DiaAutoAjustado ELSE CAST(B.DiaAutoAjustado AS STRING) END AS DiaAutoAjustado, \
													 CASE WHEN LENGTH(B.MesAutoAjustado) = 1 THEN '0' || B.MesAutoAjustado ELSE CAST(B.MesAutoAjustado AS STRING) END AS MesAutoAjustado, \
													 B.Peso, \
													 SUBSTR(B.AtrasoReajuste,0,10) as AtrasoReajuste \
									FROM VW_GENE_BALANCO_SERVICOS_STEP2_PRECO_COMISSAO_STEP1 as A\
									JOIN FC_GET_PRECO_COMISSAO_CURSOR B \
									ON A.ID_ADITIVO = B.ID_ADITIVO AND \
									B.IDCARACTERISTICAPAI = A.IDCARACTERISTICA AND \
									TO_DATE(A.DtRef, 'yyyy-MM-dd') >= TO_DATE(NVL(INICIOVALIDADE,substr(B.InicioVigenciaContrato,0,10)), 'yyyy-MM-dd') AND \
									TO_DATE(A.DtRef, 'yyyy-MM-dd') <= TO_DATE(NVL(DT_REF_INI,FimVigenciaContrato), 'yyyy-MM-dd') ")

vwBalancoServicosStep2PrecoComStep2.createOrReplaceTempView("VW_GENE_BALANCO_SERVICOS_STEP2_PRECO_COMISSAO_STEP2")

vwBalancoServicosStep2PrecoComStep3Ini = spark.sql("SELECT A.DiaBase, \
										       A.DtRef, \
										       MAX(DT_INI.DataVigencia) AS DT_INI_DataVigencia \
										FROM VW_GENE_BALANCO_SERVICOS_STEP2_PRECO_COMISSAO_STEP2 AS A \
										LEFT JOIN VW_SERIE_IPCA AS DT_INI ON TO_DATE(SUBSTR(DT_INI.DataVigencia,0,10), 'yyyy-MM-dd') < TO_DATE(SUBSTR(A.DiaBase,0,10), 'yyyy-MM-dd') \
										GROUP BY A.DiaBase, \
										         A.DtRef")

vwBalancoServicosStep2PrecoComStep3Ini.createOrReplaceTempView("VW_GENE_BALANCO_SERVICOS_STEP2_PRECO_COMISSAO_STEP3_INI")

vwBalancoServicosStep2PrecoComStep3Fim = spark.sql("SELECT A.DiaBase, \
										       A.DtRef, \
										       MAX(DT_FIM.DataVigencia) AS DT_FIM_DataVigencia \
										FROM VW_GENE_BALANCO_SERVICOS_STEP2_PRECO_COMISSAO_STEP2 AS A \
										LEFT JOIN VW_SERIE_IPCA AS DT_FIM ON TO_DATE(SUBSTR(DT_FIM.DataVigencia,0,10), 'yyyy-MM-dd') < A.DtRef \
										GROUP BY A.DiaBase, \
										         A.DtRef")

vwBalancoServicosStep2PrecoComStep3Fim.createOrReplaceTempView("VW_GENE_BALANCO_SERVICOS_STEP2_PRECO_COMISSAO_STEP3_FIM")

vwBalancoServicosStep2PrecoComStep3 = spark.sql("SELECT A.ID_ADITIVO, \
                                                     A.ID_CONTRATO, \
                                                     A.CODIGOREFERENCIA, \
                                                     A.IDCARACTERISTICA, \
                                                     A.DTA_INICIO, \
                                                     A.DTA_FIM, \
                                                     A.Qtd_Meses, \
                                                     A.EXTENSAO, \
                                                     A.CLASSIFICACAO, \
                                                     A.COMPRADOR, \
                                                     A.VENDEDOR, \
                                                     A.NATUREZA, \
                                                     A.VALOR, \
                                                     A.DESCRICAO, \
                                                     A.DataAcordoComercial, \
											       	 A.SERIE, \
										   	   	     A.RECORRENTE, \
										   	   	   	 A.FLAG_ISENCAO, \
                                                     A.DtRef, \
                                                     A.INICIO_MES, \
                                                     A.FIM_MES, \
                                                     A.horas, \
                                                     A.PRECO, \
                                                     A.Id, \
                                                     A.DiaBase, \
                                                     A.DiaAutoAjustado, \
                                                     A.MesAutoAjustado, \
                                                     SUBSTR(A.Peso,0,1) AS Peso, \
                                                     A.AtrasoReajuste, \
                                                     DT_INI.DT_INI_DataVigencia, \
                                                     DT_FIM.DT_FIM_DataVigencia \
                                                     FROM VW_GENE_BALANCO_SERVICOS_STEP2_PRECO_COMISSAO_STEP2 AS A \
													 LEFT JOIN VW_GENE_BALANCO_SERVICOS_STEP2_PRECO_COMISSAO_STEP3_INI as DT_INI ON TO_DATE(SUBSTR(DT_INI.DiaBase,0,10), 'yyyy-MM-dd') = TO_DATE(SUBSTR(A.DiaBase,0,10), 'yyyy-MM-dd') AND DT_INI.DtRef =  A.DtRef\
													 LEFT JOIN VW_GENE_BALANCO_SERVICOS_STEP2_PRECO_COMISSAO_STEP3_FIM as DT_FIM ON DT_FIM.DtRef =  A.DtRef AND TO_DATE(SUBSTR(DT_FIM.DiaBase,0,10), 'yyyy-MM-dd') = TO_DATE(SUBSTR(A.DiaBase,0,10), 'yyyy-MM-dd')")

vwBalancoServicosStep2PrecoComStep3.createOrReplaceTempView("VW_GENE_BALANCO_SERVICOS_STEP2_PRECO_COMISSAO_STEP3")

vwBalancoServicosStep2PrecoComStep4 = spark.sql("SELECT A.ID_ADITIVO, \
                                                     A.ID_CONTRATO, \
                                                     A.CODIGOREFERENCIA, \
                                                     A.IDCARACTERISTICA, \
                                                     A.DTA_INICIO, \
                                                     A.DTA_FIM, \
                                                     A.Qtd_Meses, \
                                                     A.EXTENSAO, \
                                                     A.CLASSIFICACAO, \
                                                     A.COMPRADOR, \
                                                     A.VENDEDOR, \
                                                     A.NATUREZA, \
                                                     A.VALOR, \
                                                     A.DESCRICAO, \
                                                     A.DataAcordoComercial, \
											       	 A.SERIE, \
										   	   	     A.RECORRENTE, \
										   	   	   	 A.FLAG_ISENCAO, \
                                                     A.DtRef, \
                                                     A.INICIO_MES, \
                                                     A.FIM_MES, \
                                                     A.horas, \
                                                     A.PRECO, \
                                                     A.Id, \
                                                     A.DiaBase, \
                                                     A.DiaAutoAjustado, \
                                                     A.MesAutoAjustado, \
                                                     SUBSTR(A.Peso,0,1) AS Peso, \
                                                     A.AtrasoReajuste, \
                                                     A.DT_INI_DataVigencia, \
                                                     A.DT_FIM_DataVigencia, \
                                                     INI.VALOR AS IniValor, \
													 FIM.VALOR AS FimValor, \
													 1 AS IdSubmercado \
                                                     FROM VW_GENE_BALANCO_SERVICOS_STEP2_PRECO_COMISSAO_STEP3 AS A \
                                                     LEFT JOIN (SELECT VALOR, DataVigencia FROM VW_SERIE_IPCA) as INI \
                                                     ON INI.DataVigencia =  A.DT_INI_DataVigencia\
                                                     LEFT JOIN (SELECT VALOR, DataVigencia FROM VW_SERIE_IPCA) as FIM \
                                                     ON FIM.DataVigencia =  A.DT_FIM_DataVigencia")

vwBalancoServicosStep2PrecoComStep5 = fc_Preco(vwBalancoServicosStep2PrecoComStep4,vwBalancoServicosStep2PrecoComStep4.DtRef,vwBalancoServicosStep2PrecoComStep4.AtrasoReajuste,vwBalancoServicosStep2PrecoComStep4.MesAutoAjustado,vwBalancoServicosStep2PrecoComStep4.DiaAutoAjustado,vwBalancoServicosStep2PrecoComStep4.DiaBase,vwBalancoServicosStep2PrecoComStep4.PRECO,vwBalancoServicosStep2PrecoComStep4.Peso,vwBalancoServicosStep2PrecoComStep4.Id,vwBalancoServicosStep2PrecoComStep4.IniValor,vwBalancoServicosStep2PrecoComStep4.FimValor)

vwBalancoServicosStep2PrecoComStep5 = vwBalancoServicosStep2PrecoComStep5.withColumnRenamed("precoIntermediario","PrecoComFinal")

vwBalancoServicosStep2PrecoComStep5.createOrReplaceTempView("VW_GENE_BALANCO_SERVICOS_STEP2_PRECO_COMISSAO_STEP5")

vwBalancoServicosStep2PrecoStep1 = spark.sql("SELECT A.ID_ADITIVO, \
													 A.ID_CONTRATO, \
													 A.CODIGOREFERENCIA, \
													 A.IDCARACTERISTICA, \
													 A.DTA_INICIO, \
													 A.DTA_FIM, \
													 A.Qtd_Meses, \
													 A.EXTENSAO, \
													 A.CLASSIFICACAO, \
													 A.COMPRADOR, \
													 A.VENDEDOR, \
													 A.NATUREZA, \
													 A.VALOR, \
													 A.DESCRICAO, \
													 A.DataAcordoComercial, \
										   	   	   	 A.DIABASE, \
										   	   	     A.RECORRENTE, \
										   	   	   	 A.FLAG_ISENCAO, \
													 A.DtRef, \
													 A.INICIO_MES, \
													 A.FIM_MES, \
													 A.horas, \
													 A.PrecoComFinal, \
													 NVL(B.PRECO, B.CoeficientePLD * NVL(GP.VALOR,GPM.VALOR) + B.PrecoBase) as PRECO, \
													 B.DataInicioVigencia, \
													 B.DataFimVigencia \
												FROM VW_GENE_BALANCO_SERVICOS_STEP2_PRECO_COMISSAO_STEP5 AS A \
												LEFT JOIN FC_GET_PRECO AS B ON A.ID_ADITIVO = B.ID_CONTRATO \
												AND A.DtRef BETWEEN B.DataInicioVigencia AND B.DataFimVigencia \
												LEFT JOIN FC_GET_PLD_MAX AS GPM ON GPM.IdSubmercado = A.IdSubmercado \
												LEFT JOIN FC_GET_PLD AS GP ON GP.IdSubmercado = A.IdSubmercado \
												AND A.DtRef = SUBSTR(GP.DATA,0,10)")

vwBalancoServicosStep2PrecoStep1.createOrReplaceTempView("VW_GENE_BALANCO_SERVICOS_STEP2_PRECO_STEP1")

vwBalancoServicosStep2PrecoStep2 = spark.sql("SELECT DISTINCT NVL(TO_DATE(substr(AA.InicioValidade,0,10), 'yyyy-MM-dd'),TO_DATE(substr(B.DataInicioVigencia,0,10), 'yyyy-MM-dd')) AS INICIOVALIDADE, \
					                   NVL(LEAD(TO_DATE(substr(AA.InicioValidade,0,10), 'yyyy-MM-dd')) OVER ( \
					                                                        ORDER BY TO_DATE(substr(AA.InicioValidade,0,10), 'yyyy-MM-dd')),ADD_MONTHS(B.DataFimVigencia,1)) AS FIMVALIDADE, \
					                   AA.Id, \
					                   AA.DiaBase, \
					                   AA.DiaAutoAjustado, \
					                   AA.MesAutoAjustado, \
					                   AA.Peso, \
					                   B.ID_ADITIVO, \
									   B.ID_CONTRATO, \
									   B.CODIGOREFERENCIA, \
									   B.IDCARACTERISTICA, \
									   B.DTA_INICIO, \
									   B.DTA_FIM, \
									   B.Qtd_Meses, \
									   B.EXTENSAO, \
									   B.CLASSIFICACAO, \
									   B.COMPRADOR, \
									   B.VENDEDOR, \
									   B.NATUREZA, \
									   B.VALOR, \
									   B.DESCRICAO, \
									   B.DataAcordoComercial, \
									   B.RECORRENTE, \
									   B.FLAG_ISENCAO, \
									   B.DtRef, \
									   B.INICIO_MES, \
									   B.FIM_MES, \
									   B.horas, \
									   B.PrecoComFinal, \
									   B.PRECO, \
									   B.DataInicioVigencia, \
									   B.DataFimVigencia, \
					                   AA.AtrasoReajuste \
					   FROM contratos AS C \
					   LEFT JOIN Aditivos AS A ON (C.ID = A.IdContrato) \
					   LEFT JOIN VW_GENE_BALANCO_SERVICOS_STEP2_PRECO_STEP1 AS B ON A.ID = B.ID_ADITIVO \
					   LEFT OUTER JOIN Ajustes_Aditivo AS AA ON A.Id = AA.IdAditivo \
					   WHERE AA.IDTIPO = 6 \
					   AND B.DtRef >= NVL(TO_DATE(substr(AA.InicioValidade,0,10), 'yyyy-MM-dd'),B.DataInicioVigencia) \
					   AND B.DtRef < NVL(TO_DATE(substr(AA.InicioValidade,0,10), 'yyyy-MM-dd'),ADD_MONTHS(B.DataFimVigencia,1))")

vwBalancoServicosStep2PrecoStep2.createOrReplaceTempView("VW_GENE_BALANCO_SERVICOS_STEP2_PRECO_STEP2")

vwBalancoServicosStep2PrecoStep3Ini = spark.sql("SELECT A.DiaBase, \
										       A.DtRef, \
										       MAX(DT_INI.DataVigencia) AS DT_INI_DataVigencia \
										FROM VW_GENE_BALANCO_SERVICOS_STEP2_PRECO_STEP2 AS A \
										LEFT JOIN VW_SERIE_IPCA AS DT_INI ON TO_DATE(SUBSTR(DT_INI.DataVigencia,0,10), 'yyyy-MM-dd') < TO_DATE(SUBSTR(A.DiaBase,0,10), 'yyyy-MM-dd') \
										GROUP BY A.DiaBase, \
										         A.DtRef")

vwBalancoServicosStep2PrecoStep3Ini.createOrReplaceTempView("VW_GENE_BALANCO_SERVICOS_STEP2_PRECO_STEP3_INI")

vwBalancoServicosStep2PrecoStep3Fim = spark.sql("SELECT A.DiaBase, \
										       A.DtRef, \
										       MAX(DT_FIM.DataVigencia) AS DT_FIM_DataVigencia \
										FROM VW_GENE_BALANCO_SERVICOS_STEP2_PRECO_STEP2 AS A \
										LEFT JOIN VW_SERIE_IPCA AS DT_FIM ON TO_DATE(SUBSTR(DT_FIM.DataVigencia,0,10), 'yyyy-MM-dd') < A.DtRef \
										GROUP BY A.DiaBase, \
										         A.DtRef")

vwBalancoServicosStep2PrecoStep3Fim.createOrReplaceTempView("VW_GENE_BALANCO_SERVICOS_STEP2_PRECO_STEP3_FIM")

vwBalancoServicosStep2PrecoStep3 = spark.sql("SELECT A.INICIOVALIDADE, \
											         A.FIMVALIDADE, \
											         A.Id, \
											         A.DiaBase, \
											         A.DiaAutoAjustado, \
											         A.MesAutoAjustado, \
											         A.Peso, \
											         A.ID_ADITIVO, \
											         A.ID_CONTRATO, \
											         A.CODIGOREFERENCIA, \
											         A.IDCARACTERISTICA, \
											         A.DTA_INICIO, \
											         A.DTA_FIM, \
											         A.Qtd_Meses, \
											         A.EXTENSAO, \
											         A.CLASSIFICACAO, \
											         A.COMPRADOR, \
											         A.VENDEDOR, \
											         A.NATUREZA, \
											         A.VALOR, \
											         A.DESCRICAO, \
											         A.DataAcordoComercial, \
										   	   	     A.RECORRENTE, \
										   	   	   	 A.FLAG_ISENCAO, \
											         A.DtRef, \
											         A.INICIO_MES, \
											         A.FIM_MES, \
											         A.horas, \
											         A.PrecoComFinal, \
											         A.PRECO, \
											         A.DataInicioVigencia, \
											         A.DataFimVigencia, \
											         A.AtrasoReajuste, \
											         DT_INI.DT_INI_DataVigencia, \
											         DT_FIM.DT_FIM_DataVigencia \
											  FROM VW_GENE_BALANCO_SERVICOS_STEP2_PRECO_STEP2 AS A \
											  LEFT JOIN VW_GENE_BALANCO_SERVICOS_STEP2_PRECO_STEP3_INI AS DT_INI ON TO_DATE(SUBSTR(DT_INI.DiaBase,0,10), 'yyyy-MM-dd') = TO_DATE(SUBSTR(A.DiaBase,0,10), 'yyyy-MM-dd') AND DT_INI.DtRef = A.DtRef\
											  LEFT JOIN VW_GENE_BALANCO_SERVICOS_STEP2_PRECO_STEP3_FIM AS DT_FIM ON DT_FIM.DtRef = A.DtRef AND TO_DATE(SUBSTR(DT_FIM.DiaBase,0,10), 'yyyy-MM-dd') = TO_DATE(SUBSTR(A.DiaBase,0,10), 'yyyy-MM-dd')")

vwBalancoServicosStep2PrecoStep3.createOrReplaceTempView("VW_GENE_BALANCO_SERVICOS_STEP2_PRECO_STEP3")

vwBalancoServicosStep2PrecoStep4 = spark.sql("SELECT A.INICIOVALIDADE, \
											         A.FIMVALIDADE, \
											         A.Id, \
											         SUBSTR(A.DiaBase,0,10) as DiaBase, \
											         A.DiaAutoAjustado, \
											         A.MesAutoAjustado, \
											         SUBSTR(A.Peso,0,1) AS Peso, \
											         A.ID_ADITIVO, \
											         A.ID_CONTRATO, \
											         A.CODIGOREFERENCIA, \
											         A.IDCARACTERISTICA, \
											         A.DTA_INICIO, \
											         A.DTA_FIM, \
											         A.Qtd_Meses, \
											         A.EXTENSAO, \
											         A.CLASSIFICACAO, \
											         A.COMPRADOR, \
											         A.VENDEDOR, \
											         A.NATUREZA, \
											         A.VALOR, \
											         A.DESCRICAO, \
											         A.DataAcordoComercial, \
										   	   	     A.RECORRENTE, \
										   	   	   	 A.FLAG_ISENCAO, \
											         A.DtRef, \
											         A.INICIO_MES, \
											         A.FIM_MES, \
											         A.horas, \
											         A.PrecoComFinal, \
											         A.PRECO, \
											         A.DataInicioVigencia, \
											         A.DataFimVigencia, \
											         SUBSTR(A.AtrasoReajuste,0,10) as AtrasoReajuste, \
											         A.DT_INI_DataVigencia, \
											         A.DT_FIM_DataVigencia, \
											         INI.VALOR AS IniValor, \
											         FIM.VALOR AS FimValor \
											  FROM VW_GENE_BALANCO_SERVICOS_STEP2_PRECO_STEP3 AS A \
											  LEFT JOIN \
											    (SELECT VALOR, \
											            DataVigencia \
											     FROM VW_SERIE_IPCA) AS INI ON INI.DataVigencia = A.DT_INI_DataVigencia \
											  LEFT JOIN \
											    (SELECT VALOR, \
											            DataVigencia \
											     FROM VW_SERIE_IPCA) AS FIM ON FIM.DataVigencia = A.DT_FIM_DataVigencia")

vwBalancoServicosStep2 = fc_Preco(vwBalancoServicosStep2PrecoStep4,vwBalancoServicosStep2PrecoStep4.DtRef,vwBalancoServicosStep2PrecoStep4.AtrasoReajuste,vwBalancoServicosStep2PrecoStep4.MesAutoAjustado,vwBalancoServicosStep2PrecoStep4.DiaAutoAjustado,vwBalancoServicosStep2PrecoStep4.DiaBase,vwBalancoServicosStep2PrecoStep4.PRECO,vwBalancoServicosStep2PrecoStep4.Peso,vwBalancoServicosStep2PrecoStep4.Id,vwBalancoServicosStep2PrecoStep4.IniValor,vwBalancoServicosStep2PrecoStep4.FimValor)

vwBalancoServicosStep2 = vwBalancoServicosStep2.withColumnRenamed("precoIntermediario","PrecoFinal")

vwBalancoServicosStep2.createOrReplaceTempView("VW_GENE_BALANCO_SERVICOS_STEP2")

vwBalancoServicosStep3Aux = spark.sql("SELECT ID_ADITIVO, \
									       CODIGOREFERENCIA, \
									       SUBSTR(F.MesReferencia,0,10) AS DTA_INICIO, \
									       SUBSTR(F.MesReferencia,0,10) AS DtRef, \
									       EXTENSAO, \
									       CLASSIFICACAO, \
									       COMPRADOR, \
									       VENDEDOR, \
									       CASE \
									           WHEN COMPRADOR LIKE '%VOTENER%' THEN 'Compra' \
									           ELSE 'Venda' \
									       END AS NATUREZA, \
									       1 AS VALOR, \
									       'R$' AS DESCRICAO, \
									       PrecoMedio, \
									       F.Total, \
									       f.idempresa, \
									       CG.DataAcordoComercial, \
									       CG.SERIE, \
										   CG.DIABASE, \
										   CG.RECORRENTE, \
										   CG.FLAG_ISENCAO \
									FROM VW_GENE_CONTRATOS_NEW CG \
									LEFT JOIN Faturas F ON (CG.ID_ADITIVO = F.IdAditivo) \
									LEFT JOIN EnergiaFaturas EF ON (F.ID = EF.IdFatura) \
									LEFT JOIN EntidadesAprovaveis EA ON (F.ID = EA.Id) \
									WHERE NATUREZA IN ('Serviços', \
									                   'Compra') \
									  AND CLASSIFICACAO = 'Serviços' \
									  AND IDNATUREZA = 1")

vwBalancoServicosStep3 = fc_Qtd_Horas_Mes(vwBalancoServicosStep3Aux,vwBalancoServicosStep3Aux.DtRef)

vwBalancoServicosStep3 = vwBalancoServicosStep3.withColumnRenamed("Qtd_horas","horas")

vwBalancoServicosStep3.createOrReplaceTempView("VW_GENE_BALANCO_SERVICOS_STEP3")

tmpContratosGene = spark.sql("SELECT ID_ADITIVO, \
							         ID_CONTRATO, \
							         CodigoReferencia, \
							         INICIOVIGENCIACONTRATO , \
							         FIMVIGENCIACONTRATO, \
							         EXTENSAO, \
							         CLASSIFICACAO, \
							         COMPRADOR, \
							         VENDEDOR, \
							         NATUREZA, \
							         SERIE, \
									 DIABASE, \
									 RECORRENTE, \
									 CASE WHEN FLAG_ISENCAO IS NULL THEN 1 ELSE 0 END AS FLAG_ISENCAO, \
							         DataAcordoComercial AS DTA_OPERACAO \
							  FROM VW_GENE_CONTRATOS_NEW")

tmpContratosGene.createOrReplaceTempView("TMP_CONTRATOS_GENE")