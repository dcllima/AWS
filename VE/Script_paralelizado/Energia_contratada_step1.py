#cria VW_GENE_ENERGIA_CONTRATADA_STEP1
vwEnergiaContratadaStep1 = spark.sql("SELECT A.ID AS IDADITIVO, \
       										A.IDCONTRATO, \
       										C.CODIGOREFERENCIA, \
       										CASE \
           										WHEN NVL(A.INICIOVALIDADE,A.InicioVigenciaContrato) >= NVL(DEF.INICIOINTERVALO,NVL(A.INICIOVALIDADE,A.InicioVigenciaContrato)) THEN NVL(A.INICIOVALIDADE,A.InicioVigenciaContrato) \
           										ELSE NVL(DEF.INICIOINTERVALO,NVL(A.INICIOVALIDADE,A.InicioVigenciaContrato)) \
       										END AS INICIO_INTERVALO, \
       										CASE \
           										WHEN EA.DataDesativacao < CASE \
                                         										WHEN NVL(A.FIMVALIDADE, A.FIMVIGENCIACONTRATO) <= NVL(DEF.FIMINTERVALO,NVL(A.FIMVALIDADE, A.FIMVIGENCIACONTRATO)) THEN NVL(A.FIMVALIDADE, A.FIMVIGENCIACONTRATO) \
                                         										ELSE NVL(DEF.FIMINTERVALO,NVL(A.FIMVALIDADE, A.FIMVIGENCIACONTRATO)) \
                                     										END THEN EA.DataDesativacao \
           										ELSE CASE \
                    										WHEN NVL(A.FIMVALIDADE, A.FIMVIGENCIACONTRATO) <= NVL(DEF.FIMINTERVALO,NVL(A.FIMVALIDADE, A.FIMVIGENCIACONTRATO)) THEN NVL(A.FIMVALIDADE, A.FIMVIGENCIACONTRATO) \
                    										ELSE NVL(DEF.FIMINTERVALO,NVL(A.FIMVALIDADE, A.FIMVIGENCIACONTRATO)) \
                										END \
       										END AS FIM_INTERVALO, \
       										DEF.Valor, \
       										UE.Descricao, \
       										CC.ID AS IDCC, \
       										DEF.IdCaracteristicaEnergia \
										FROM CategoriasContratos AS CTC \
										LEFT JOIN Categorias AS CT ON CTC.IdCategoria = CT.Id \
										LEFT JOIN Contratos AS C ON C.Id = CTC.IdContrato \
										LEFT OUTER JOIN Aditivos AS A ON C.Id = A.IdContrato \
										LEFT OUTER JOIN AUX_JOIN_ENTIDADES_ATIVAVEIS AS EA ON A.IdContrato = EA.IdEntidade \
										LEFT OUTER JOIN CaracteristicasContrato AS CC ON A.Id = CC.IdAditivo \
										INNER JOIN DadosEnergiaFixa AS DEF ON CC.Id = DEF.IdCaracteristicaEnergia \
										INNER JOIN UnidadesEnergia AS UE ON CASE \
                                        										WHEN DEF.UnidadeValor = 3 THEN 2 \
                                        										ELSE DEF.UnidadeValor \
                                    										END = UE.Id \
										WHERE CT.Grupo = 'Balanço' \
  										AND CC.IdTipo = "+idTipoVwEnergiaContratadaStep1+" \
  										AND CASE \
          										WHEN NVL(A.INICIOVALIDADE,A.InicioVigenciaContrato) >= NVL(DEF.INICIOINTERVALO,NVL(A.INICIOVALIDADE,A.InicioVigenciaContrato)) THEN NVL(A.INICIOVALIDADE,A.InicioVigenciaContrato) \
          										ELSE NVL(DEF.INICIOINTERVALO,NVL(A.INICIOVALIDADE,A.InicioVigenciaContrato)) \
      										END <= CASE \
                 										WHEN EA.DataDesativacao < CASE \
                                               										WHEN NVL(A.FIMVALIDADE, A.FIMVIGENCIACONTRATO) <= NVL(DEF.FIMINTERVALO,NVL(A.FIMVALIDADE, A.FIMVIGENCIACONTRATO)) THEN NVL(A.FIMVALIDADE, A.FIMVIGENCIACONTRATO) \
                                               										ELSE NVL(DEF.FIMINTERVALO,NVL(A.FIMVALIDADE, A.FIMVIGENCIACONTRATO)) \
                                           										END THEN EA.DataDesativacao \
                 										ELSE CASE \
                          										WHEN NVL(A.FIMVALIDADE, A.FIMVIGENCIACONTRATO) <= NVL(DEF.FIMINTERVALO,NVL(A.FIMVALIDADE, A.FIMVIGENCIACONTRATO)) THEN NVL(A.FIMVALIDADE, A.FIMVIGENCIACONTRATO) \
                          										ELSE NVL(DEF.FIMINTERVALO,NVL(A.FIMVALIDADE, A.FIMVIGENCIACONTRATO)) \
                      										END \
             										END ")

vwEnergiaContratadaStep1.createOrReplaceTempView("VW_GENE_ENERGIA_CONTRATADA_STEP1")

# cria VW_GENE_ENERGIA_CONTRATADA_STEP2
vwEnergiaContratadaStep2Aux = spark.sql("SELECT S.IDADITIVO, \
       										S.IdContrato, \
       										CASE \
           										WHEN IdEmpresaDestino = 3 THEN NVL(CE.IdAgenteDestino, CE.IdAgenteOrigem) \
           										ELSE NVL(CE.IdAgenteOrigem, CE.IdAgenteDestino) \
       										END AS IdAgente, \
       										CE.IdEmpresaOrigem, \
       										CE.IdEmpresaDestino, \
       										CE.IdAgenteOrigem, \
       										CE.IdAgenteDestino, \
       										CE.IdPerfil, \
       										CE.IdSubmercado, \
       										SUBSTR(INICIO_INTERVALO,0,10) AS InicioIntervalo, \
       										SUBSTR(FIM_INTERVALO,0,10) AS FimIntervalo, \
       										CEIL(MONTHS_BETWEEN(FIM_INTERVALO,INICIO_INTERVALO)) AS Qtd_Meses, \
       										S.Descricao, \
       										S.Valor, \
       										SUBSTR(INICIO_INTERVALO,0,10) AS pInicio, \
											SUBSTR(FIM_INTERVALO,0,10) AS pFim, \
       										CC.IdCaracteristicaPai, \
       										CASE CE.IdPerfil \
           										WHEN 1 THEN 'Convencional' \
           										WHEN 2 THEN 'I5' \
           										WHEN 4 THEN 'I1' \
           										WHEN 32 THEN 'I0' \
           										WHEN 64 THEN 'CQ5' \
           										ELSE CASE NVL(AGorigem.IdTipoAgente, AGdestino.IdTipoAgente) \
                    										WHEN 1 THEN 'Convencional' \
                    										WHEN 2 THEN 'Convencional' \
                    										WHEN 4 THEN 'Indefinido' \
                    										WHEN 8 THEN 'I5' \
                    										WHEN 16 THEN 'I1' \
                    										WHEN 32 THEN 'I0' \
                    										WHEN 64 THEN 'I0' \
                    										WHEN 128 THEN 'CQ5' \
                    										ELSE 'Indefinido' \
                										END \
       										END AS perfil_Energia, \
       										SUB.Nome AS SUBMERCADO, \
       										S.IdCaracteristicaEnergia \
										FROM CaracteristicasContrato AS CC \
										LEFT OUTER JOIN CaracteristicasEnergiaEntregue AS CE ON CC.Id = CE.IdCaracteristica \
										LEFT OUTER JOIN Agentes AS AGorigem ON CE.IdAgenteOrigem = AGorigem.Id \
										LEFT OUTER JOIN Agentes AS AGdestino ON CE.IdAgenteDestino = AGdestino.Id \
										LEFT OUTER JOIN Submercados AS SUB ON CE.IdSubmercado = SUB.Id \
										LEFT JOIN VW_GENE_ENERGIA_CONTRATADA_STEP1 S ON (S.IDADITIVO = CC.IdAditivo \
                                                 										AND S.IdCC = CC.IdCaracteristicaPai) \
										WHERE (CC.IdTipo IN "+idTipoVwEnergiaContratadaStep2Aux+") AND \
                     							S.IDADITIVO IS NOT NULL")

vwEnergiaContratadaStep2Rang = vwEnergiaContratadaStep2Aux.withColumn("records", fc_Range_Dat_UDF(vwEnergiaContratadaStep2Aux.InicioIntervalo,vwEnergiaContratadaStep2Aux.Qtd_Meses) ) \
														.withColumn("DtRef", explode("records")) \
                										.drop("records")
vwEnergiaContratadaStep2Rang = fc_Qtd_Horas_Periodo(vwEnergiaContratadaStep2Rang,vwEnergiaContratadaStep2Rang.pInicio,vwEnergiaContratadaStep2Rang.pFim)
 
vwEnergiaContratadaStep2Rang = vwEnergiaContratadaStep2Rang.withColumnRenamed("Qtd_horas","QtdHorasPeriodoIniFim")

vwEnergiaContratadaStep2Rang = fc_Qtd_Horas_Mes(vwEnergiaContratadaStep2Rang,vwEnergiaContratadaStep2Rang.DtRef)

vwEnergiaContratadaStep2Rang = vwEnergiaContratadaStep2Rang.withColumnRenamed("Qtd_horas","QtdHorasMes")

vwEnergiaContratadaStep2Parts = vwEnergiaContratadaStep2Rang.withColumn("Ano",year( vwEnergiaContratadaStep2Rang.DtRef ) )\
                                  .withColumn("Mes",month(vwEnergiaContratadaStep2Rang.DtRef) ) \
                                  .withColumn("FimMes",last_day(vwEnergiaContratadaStep2Rang.DtRef))

vwEnergiaContratadaStep2Parts.createOrReplaceTempView("VW_GENE_ENERGIA_CONTRATADA_STEP2_PART")

vwEnergiaContratadaStep2 = spark.sql("SELECT A.IDADITIVO, \
										       A.IdContrato, \
										       A.IdAgente, \
										       A.IdEmpresaOrigem, \
										       A.IdEmpresaDestino, \
										       A.IdAgenteOrigem, \
										       A.IdAgenteDestino, \
										       A.IdPerfil, \
										       A.IdSubmercado, \
										       A.InicioIntervalo, \
										       A.FimIntervalo, \
										       A.Qtd_Meses, \
										       A.Descricao, \
										       A.Valor, \
										       A.IdCaracteristicaPai, \
										       A.perfil_Energia, \
										       A.SUBMERCADO, \
										       A.IdCaracteristicaEnergia, \
										       A.DtRef, \
										       A.QtdHorasPeriodoIniFim, \
										       A.QtdHorasMes, \
										       A.Ano, \
										       A.Mes, \
										       B.QtdPartes, \
										       CASE \
										           WHEN A.Descricao = 'MWméd' THEN A.Valor \
										           ELSE A.Valor / A.QtdHorasPeriodoIniFim \
										       END AS MWm, \
										       QtdPartes AS QTD_PARTES, \
										       CASE \
										           WHEN A.Descricao = 'MWméd' THEN A.Valor \
										           ELSE A.Valor / A.QtdHorasPeriodoIniFim \
										       END / QtdPartes AS MWmRateado, \
										             CASE \
										                 WHEN A.Descricao = 'MWh' THEN A.Valor / A.QtdHorasPeriodoIniFim * A.QtdHorasMes \
										                 ELSE A.Valor * A.QtdHorasMes \
										             END AS MWh, \
										             CASE \
										                 WHEN A.Descricao = 'MWh' THEN A.Valor / A.QtdHorasPeriodoIniFim * A.QtdHorasMes \
										                 ELSE A.Valor * A.QtdHorasMes \
										             END / QtdPartes AS MWhRateado, \
										                   A.DtRef AS INICIO_MES, \
										                   SUBSTR(FimMes,0,10) AS FIM_MES \
										FROM VW_GENE_ENERGIA_CONTRATADA_STEP2_PART AS A \
										LEFT JOIN FC_GET_QTD_PARTES_CONTRATUAIS AS B ON B.IdAditivo = A.IDADITIVO \
										AND B.IdCaracteristicaPai = A.IdCaracteristicaPai ")

vwEnergiaContratadaStep2.createOrReplaceTempView("VW_GENE_ENERGIA_CONTRATADA_STEP2")

vwEnergiaSazonalizadaAux = spark.sql("SELECT A.Id AS IDADITIVO, \
                         A.IdContrato, \
                         DIS.Id, \
                         DIS.IdTipoItem, \
                         SUBSTR(DIS.DataInicio,0,10) AS DataInicio, \
                         SUBSTR(DIS.DataFim,0,10) AS DataFim, \
                         SUBSTR(DIS.DataInicio,0,10) AS pInicio, \
                         SUBSTR(DIS.DataFim,0,10) AS pFim, \
                         DIS.Unidade, \
                         DIS.ValorLiq, \
                         DIS.ValorBruto, \
                         YEAR(DIS.DataInicio) AS ANO, \
                         MONTH(DIS.DataInicio) AS MES, \
                         CS.IdCaracteristicaEnergia \
                  FROM Aditivos AS A \
                  LEFT OUTER JOIN CaracteristicasContrato AS CCSAZO ON A.Id = CCSAZO.IdAditivo \
                  LEFT OUTER JOIN CaracteristicaSazonalizacao AS CS ON CCSAZO.Id = CS.IdCaracteristica \
                  LEFT OUTER JOIN BlocosSazonalizacao AS BS ON CS.IdBlocoSazonalizacao = BS.Id \
                  INNER JOIN DadosAnosSazonalizacao AS DAS ON BS.Id = DAS.IdBlocoSazonalizacao \
                  INNER JOIN DadosMesesSazonalizacao AS DMS ON DAS.Id = DMS.IdAno \
                  INNER JOIN DadosItemSazonalizacao AS DIS ON DMS.IdItem = DIS.Id")

vwEnergiaSazonalizadaAux = fc_Qtd_Horas_Periodo(vwEnergiaSazonalizadaAux,vwEnergiaSazonalizadaAux.pInicio,vwEnergiaSazonalizadaAux.pFim)

vwEnergiaSazonalizadaQtdHoras = vwEnergiaSazonalizadaAux.withColumnRenamed("Qtd_horas","QtdHorasPeriodoIniFim")

vwEnergiaSazonalizadaQtdHoras.createOrReplaceTempView("VW_GENE_ENERGIA_SAZONALIZADA_HORAS")

vwEnergiaSazonalizada = spark.sql("SELECT IDADITIVO, \
                         IdContrato, \
                         Id, \
                         IdTipoItem, \
                         DataInicio, \
                         DataFim, \
                         Unidade, \
                         ValorLiq, \
                         ValorBruto, \
                         ANO, \
                         MES, \
                         CASE \
                             WHEN Unidade = 2 THEN ValorLiq / QtdHorasPeriodoIniFim \
                             ELSE ValorLiq \
                         END AS MWm, \
                         CASE \
                             WHEN Unidade = 1 THEN ValorLiq * QtdHorasPeriodoIniFim \
                             ELSE ValorLiq \
                         END AS MWh, \
                         QtdHorasPeriodoIniFim, \
                         IdCaracteristicaEnergia \
                  FROM VW_GENE_ENERGIA_SAZONALIZADA_HORAS") #.repartition("ANO")

vwEnergiaSazonalizada.createOrReplaceTempView("VW_GENE_ENERGIA_SAZONALIZADA")

tmpEnergiaContratada = spark.sql("SELECT IDADITIVO, \
                         IDCONTRATO, \
                         IdAgente, \
                         IdEmpresaOrigem, \
                         IdEmpresaDestino , \
                         IdAgenteOrigem, \
                         IdAgenteDestino, \
                         IdPerfil, \
                         IdSubmercado, \
                         InicioIntervalo , \
                         FimIntervalo, \
                         ANO, \
                         MES, \
                         QtdHorasPeriodoIniFim AS HORAS_PERIODO, \
                         MWm, \
                         QTD_PARTES, \
                         MWmRateado , \
                         MWh, \
                         MWhRateado, \
                         INICIO_MES, \
                         FIM_MES, \
                         PERFIL_ENERGIA, \
                         SUBMERCADO, \
                         IdCaracteristicaEnergia \
                  FROM VW_GENE_ENERGIA_CONTRATADA_STEP2")

tmpEnergiaContratada.createOrReplaceTempView("TMP_ENERGIA_CONTRATADA")

