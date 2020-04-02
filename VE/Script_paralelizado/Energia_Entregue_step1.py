#cria VW_GENE_ENERGIA_ENTREGUE

vwEnergiaEntregueStep1 = spark.sql("SELECT A.Id AS IdAditivo, \
								       A.IdContrato, \
								       EEE.QuantidadeEntregueMWh, \
								       F.Preco,  \
								       idTipoPreco, \
								       PLD.PrecoBase, \
								       Round(EEE.ValorPreco,2) AS ValorPreco, \
								       PLD.CoeficientePLD, \
								       EEE.IdSubmercado AS EEE_IdSubmercado, \
								       YEAR(EEE.InicioPeriodo) AS ANO, \
								       MONTH(EEE.InicioPeriodo) AS MES, \
								       SUBSTR(EEE.InicioPeriodo,0,10) AS InicioPeriodo, \
								       SUBSTR(EEE.FimPeriodo,0,10) AS FimPeriodo, \
								       SUBSTR(EEE.InicioPeriodo,0,10) AS pInicio, \
								       SUBSTR(EEE.FimPeriodo,0,10) AS pFim, \
								       EEE.QuantidadeEntregueMWh AS MWh, \
								       NVL(EEE.IdAgenteOrigem, CEE.IdAgenteOrigem) AS IdAgenteOrigem, \
								       NVL(EEE.IdAgenteDestino, CEE.IdAgenteDestino) AS IdAgenteDestino, \
								       NVL(EEE.IdEmpresaOrigem, CEE.IdEmpresaOrigem) AS IdEmpresaOrigem, \
								       NVL(EEE.IdEmpresaDestino, CEE.IdEmpresaDestino) AS IdEmpresaDestino, \
								       A.FimValidade, \
								       NVL(AGorigem.IdTipoAgente, AGdestino.IdTipoAgente) AS AGENTE, \
								       NVL(AGorigem.Nome, AGdestino.Nome) AS Expr1, \
								       NVL(CASE NVL(EEE.PerfilOrigem, PE.Id) WHEN 2 THEN 8 WHEN 4 THEN 16 ELSE NVL(EEE.PerfilOrigem, PE.Id) END, NVL(AGorigem.IdTipoAgente, AGdestino.IdTipoAgente)) AS Expr2, \
								       CASE EEE.PerfilOrigem \
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
								       NVL(EEE.IdSubmercado,CEE.IdSubmercado) AS IdSubmercado, \
								       SUB.Nome AS SUBMERCADO, \
								       PE.Id AS IDPERFIL, \
								       F.NumNotaFiscal, \
								       F.NumDocumento \
								FROM Aditivos AS A \
								INNER JOIN CaracteristicasContrato AS CC ON A.Id = CC.IdAditivo \
								INNER JOIN CaracteristicasEnergiaEntregue AS CEE ON CC.Id = CEE.IdCaracteristica \
								INNER JOIN BlocosEnergiaEntregue AS BEE ON CEE.IdCaracteristica = BEE.IdCaracteristicaEnergiaEntr \
								INNER JOIN EntradasEnergiaEntregue AS EEE ON BEE.Id = EEE.IdBlocoEnergiaEntregue \
								AND NVL(A.InicioValidade, A.InicioVigenciaContrato) <= EEE.InicioPeriodo \
								AND NVL(A.FimValidade, A.FimVigenciaContrato) >= EEE.FimPeriodo \
								LEFT OUTER JOIN PerfilEnergia AS PE ON CEE.IdPerfil = PE.Id \
								LEFT OUTER JOIN Agentes AS AGorigem ON NVL(EEE.IdAgenteOrigem, CEE.IdAgenteOrigem) = AGorigem.Id \
								LEFT OUTER JOIN Agentes AS AGdestino ON NVL(EEE.IdAgenteDestino, CEE.IdAgenteDestino) = AGdestino.Id \
								LEFT OUTER JOIN Submercados AS SUB ON NVL(EEE.IdSubmercado,CEE.IdSubmercado) = SUB.Id \
								LEFT OUTER JOIN \
								  (SELECT IdAditivo, \
								          Id \
								   FROM CaracteristicasContrato AS CC1 \
								   WHERE (IdTipo IN "+iIdTipoVwEnergiaEntregue+")) AS CC2 ON A.Id = CC2.IdAditivo \
								LEFT OUTER JOIN CaracteristicasPreco AS CPR ON CC2.Id = CPR.IdCaracteristica \
								LEFT OUTER JOIN DadosPrecoFuncaoPLD AS PLD ON CPR.IdCaracteristica = PLD.IdCaracteristicaPreco \
								LEFT JOIN VW_10_GENE_FATURAS AS F ON (EEE.Id = F.IdEntradaEnergiaEntregue)")

vwEnergiaEntregueStep1 = fc_Qtd_Horas_Periodo(vwEnergiaEntregueStep1,vwEnergiaEntregueStep1.pInicio,vwEnergiaEntregueStep1.pFim)

vwEnergiaEntregueStep2 = vwEnergiaEntregueStep1.withColumnRenamed("Qtd_horas","QtdHorasPeriodoIniFim")

vwEnergiaEntregueStep2.createOrReplaceTempView("VW_GENE_ENERGIA_ENTREGUE_STEP2")

vwEnergiaEntregue = spark.sql("SELECT IdAditivo, \
								       IdContrato, \
								       QuantidadeEntregueMWh, \
								       Preco, \
								       idTipoPreco, \
								       PrecoBase, \
								       ValorPreco, \
								       CoeficientePLD, \
								       EEE_IdSubmercado, \
								       ANO, \
								       MES, \
								       InicioPeriodo, \
								       FimPeriodo, \
								       MWh, \
								       IdAgenteOrigem, \
								       IdAgenteDestino, \
								       IdEmpresaOrigem, \
								       IdEmpresaDestino, \
								       FimValidade, \
								       AGENTE, \
								       Expr1, \
								       Expr2, \
								       perfil_Energia, \
								       IdSubmercado, \
								       SUBMERCADO, \
								       IDPERFIL, \
								       NumNotaFiscal, \
								       NumDocumento, \
								       MWh / QtdHorasPeriodoIniFim AS MWm, \
								       CASE WHEN EOL.Nome IS NULL THEN 'N' ELSE 'S'END AS CONT_EOL \
								FROM VW_GENE_ENERGIA_ENTREGUE_STEP2 \
								LEFT JOIN VW_CONTATOS_EOL EOL ON EOL.ID = IdAditivo")

vwEnergiaEntregue.createOrReplaceTempView("VW_GENE_ENERGIA_ENTREGUE")