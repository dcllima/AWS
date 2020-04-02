tmpEnergiaContratadaSazoStep1 = spark.sql("SELECT CG.CodigoReferencia, \
                              CG.CLASSIFICACAO, \
                              CG.COMPRADOR, \
                              CG.VENDEDOR, \
                              CG.NATUREZA , \
                              EC.IDADITIVO, \
                              EC.IDCONTRATO, \
                              IdAgente, \
                              IdEmpresaOrigem, \
                              IdEmpresaDestino , \
                              IdAgenteOrigem, \
                              IdAgenteDestino, \
                              IdPerfil, \
                              IdSubmercado, \
                              EC.InicioIntervalo , \
                              EC.FimIntervalo, \
                              EC.ANO, \
                              EC.MES, \
                              HORAS_PERIODO, \
                              NVL(ES.MWm,EC.MWm) AS MWm, \
                              QTD_PARTES , \
                              (1 + NVL(valor_flex,0)) * CASE \
                                                               WHEN ES.MWm IS NULL THEN MWmRateado \
                                                               ELSE ES.MWm / QTD_PARTES \
                                                           END AS MWmRateado , \
                              NVL(ES.MWh,EC.MWh) AS MWh , \
                              (1 + NVL(valor_flex,0)) * CASE \
                                                               WHEN ES.MWh IS NULL THEN MWhRateado \
                                                               ELSE ES.MWh / QTD_PARTES \
                                                           END AS MWhRateado , \
                              INICIO_MES, \
                              FIM_MES, \
                              PERFIL_ENERGIA, \
                              SUBMERCADO, \
                              CG.DTA_OPERACAO, \
                              FLEX_MAX,FLEX_MIN,EXTENSAO, \
                              CG.SERIE, \
                          CG.DIABASE, \
                          CG.RECORRENTE, \
                          CG.FLAG_ISENCAO \
                       FROM TMP_ENERGIA_CONTRATADA EC \
                       LEFT JOIN VW_GENE_ENERGIA_SAZONALIZADA ES ON (EC.IDADITIVO = ES.IDADITIVO \
                                                                     AND EC.ANO = ES.ANO \
                                                                     AND EC.MES = ES.MES \
                                                                     AND EC.IdCaracteristicaEnergia = ES.IdCaracteristicaEnergia) \
                       INNER JOIN TMP_CONTRATOS_GENE CG ON (CG.ID_ADITIVO = EC.IDADITIVO) \
                       LEFT JOIN VW_GENE_FLEXIBILIDADE F ON (CG.ID_ADITIVO = F.IDADITIVO \
                                                             AND EC.INICIO_MES BETWEEN F.INICIOINTERVALO AND F.FIMINTERVALO) \
                       LEFT JOIN FLEX_COMPRA FC ON (CG.CodigoReferencia = FC.COD_CONTRATO \
                                                    AND EC.INICIO_MES = fc.DTA_REFERENCIA)")

tmpEnergiaContratadaSazoStep1.createOrReplaceTempView("TMP_ENERGIA_CONTRATADA_SAZO_STEP1")

tmpEnergiaContratadaSazoStep2 = spark.sql("SELECT A.CodigoReferencia, \
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
                           A.HORAS_PERIODO, \
                           A.MWm, \
                           A.QTD_PARTES, \
                           A.MWmRateado, \
                           A.MWh, \
                           A.MWhRateado, \
                           A.INICIO_MES, \
                           A.FIM_MES, \
                           A.PERFIL_ENERGIA, \
                           A.SUBMERCADO, \
                           A.DTA_OPERACAO, \
                           A.FLEX_MAX, \
                           A.FLEX_MIN, \
                           A.EXTENSAO, \
                         A.SERIE, \
                       A.DIABASE, \
                       A.RECORRENTE, \
                       A.FLAG_ISENCAO, \
                           NVL(B.PRECO, B.CoeficientePLD * NVL(GP.VALOR,GPM.VALOR) + B.PrecoBase) as PRECO, \
                           TO_DATE(substr(B.DataInicioVigencia,0,10), 'yyyy-MM-dd') as DataInicioVigencia, \
                       TO_DATE(substr(B.DataFimVigencia,0,10), 'yyyy-MM-dd') as DataFimVigencia \
                    FROM TMP_ENERGIA_CONTRATADA_SAZO_STEP1 AS A \
                    LEFT JOIN FC_GET_PRECO AS B ON A.IDADITIVO = B.ID_CONTRATO \
                    AND A.INICIO_MES BETWEEN B.DataInicioVigencia AND B.DataFimVigencia \
                    LEFT JOIN FC_GET_PLD_MAX AS GPM ON GPM.IdSubmercado = A.IdSubmercado \
                                            LEFT JOIN FC_GET_PLD AS GP ON GP.IdSubmercado = A.IdSubmercado \
                                            AND A.INICIO_MES = SUBSTR(GP.DATA,0,10)")

tmpEnergiaContratadaSazoStep2.createOrReplaceTempView("TMP_ENERGIA_CONTRATADA_SAZO_STEP2")

tmpEnergiaContratadaSazoStep3 = spark.sql(" SELECT DISTINCT NVL(TO_DATE(substr(AA.InicioValidade,0,10), 'yyyy-MM-dd'),TO_DATE(substr(B.DataInicioVigencia,0,10), 'yyyy-MM-dd')) AS INICIOVALIDADE, \
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
                           B.HORAS_PERIODO, \
                           B.MWm, \
                           B.QTD_PARTES, \
                           B.MWmRateado, \
                           B.MWh, \
                           B.MWhRateado, \
                           B.INICIO_MES, \
                           B.FIM_MES, \
                           B.PERFIL_ENERGIA, \
                           B.SUBMERCADO, \
                           B.DTA_OPERACAO, \
                           B.FLEX_MAX, \
                           B.FLEX_MIN, \
                           B.EXTENSAO, \
                           B.SERIE, \
                       B.RECORRENTE, \
                       B.FLAG_ISENCAO, \
                           B.PRECO, \
                           B.DataInicioVigencia, \
                       B.DataFimVigencia \
             FROM contratos AS C \
             LEFT JOIN Aditivos AS A ON (C.ID = A.IdContrato) \
             LEFT JOIN TMP_ENERGIA_CONTRATADA_SAZO_STEP2 AS B ON A.ID = B.IDADITIVO \
             LEFT OUTER JOIN Ajustes_Aditivo AS AA ON A.Id = AA.IdAditivo \
             WHERE AA.IDTIPO = 6 \
             AND B.INICIO_MES >= NVL(TO_DATE(substr(AA.InicioValidade,0,10), 'yyyy-MM-dd'),B.DataInicioVigencia) AND B.INICIO_MES < NVL(TO_DATE(substr(AA.InicioValidade,0,10), 'yyyy-MM-dd'),ADD_MONTHS(B.DataFimVigencia,1))")

tmpEnergiaContratadaSazoStep3.createOrReplaceTempView("TMP_ENERGIA_CONTRATADA_SAZO_STEP3")

tmpEnergiaContratadaSazoStep4Ini = spark.sql("SELECT A.DiaBase, \
                           A.INICIO_MES, \
                           MAX(DT_INI.DataVigencia) AS DT_INI_DataVigencia \
                    FROM TMP_ENERGIA_CONTRATADA_SAZO_STEP3 AS A \
                    LEFT JOIN VW_SERIE_IPCA AS DT_INI ON TO_DATE(SUBSTR(DT_INI.DataVigencia,0,10), 'yyyy-MM-dd') < TO_DATE(SUBSTR(A.DiaBase,0,10), 'yyyy-MM-dd') \
                    GROUP BY A.DiaBase, \
                             A.INICIO_MES")

tmpEnergiaContratadaSazoStep4Ini.createOrReplaceTempView("TMP_ENERGIA_CONTRATADA_SAZO_STEP4_INI")

tmpEnergiaContratadaSazoStep4Fim = spark.sql("SELECT A.DiaBase, \
                           A.INICIO_MES, \
                           MAX(DT_FIM.DataVigencia) AS DT_FIM_DataVigencia \
                    FROM TMP_ENERGIA_CONTRATADA_SAZO_STEP3 AS A \
                    LEFT JOIN VW_SERIE_IPCA AS DT_FIM ON TO_DATE(SUBSTR(DT_FIM.DataVigencia,0,10), 'yyyy-MM-dd') < A.INICIO_MES \
                    GROUP BY A.DiaBase, \
                             A.INICIO_MES")

tmpEnergiaContratadaSazoStep4Fim.createOrReplaceTempView("TMP_ENERGIA_CONTRATADA_SAZO_STEP4_FIM")

tmpEnergiaContratadaSazoStep4 = spark.sql("SELECT A.INICIOVALIDADE, \
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
                          A.HORAS_PERIODO, \
                          A.MWm, \
                          A.QTD_PARTES, \
                          A.MWmRateado, \
                          A.MWh, \
                          A.MWhRateado, \
                          A.INICIO_MES, \
                          A.FIM_MES, \
                          A.PERFIL_ENERGIA, \
                          A.SUBMERCADO, \
                          A.DTA_OPERACAO, \
                          A.FLEX_MAX, \
                          A.FLEX_MIN, \
                          A.EXTENSAO, \
                          A.SERIE, \
                          A.RECORRENTE, \
                          A.FLAG_ISENCAO, \
                          A.PRECO, \
                          A.DataInicioVigencia, \
                          A.DataFimVigencia, \
                          DT_INI.DT_INI_DataVigencia, \
                            DT_FIM.DT_FIM_DataVigencia \
                      FROM TMP_ENERGIA_CONTRATADA_SAZO_STEP3 AS A \
                      LEFT JOIN TMP_ENERGIA_CONTRATADA_SAZO_STEP4_INI AS DT_INI ON TO_DATE(SUBSTR(DT_INI.DiaBase,0,10), 'yyyy-MM-dd') = TO_DATE(SUBSTR(A.DiaBase,0,10), 'yyyy-MM-dd') AND DT_INI.INICIO_MES = A.INICIO_MES \
                      LEFT JOIN TMP_ENERGIA_CONTRATADA_SAZO_STEP4_FIM AS DT_FIM ON DT_FIM.INICIO_MES = A.INICIO_MES AND TO_DATE(SUBSTR(DT_FIM.DiaBase,0,10), 'yyyy-MM-dd') = TO_DATE(SUBSTR(A.DiaBase,0,10), 'yyyy-MM-dd')")

tmpEnergiaContratadaSazoStep4.createOrReplaceTempView("TMP_ENERGIA_CONTRATADA_SAZO_STEP4")

tmpEnergiaContratadaSazoStep5 = spark.sql("SELECT A.INICIOVALIDADE, \
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
                          A.FimIntervalo, \
                          A.ANO, \
                          A.MES, \
                          A.HORAS_PERIODO, \
                          A.MWm, \
                          A.QTD_PARTES, \
                          A.MWmRateado, \
                          A.MWh, \
                          A.MWhRateado, \
                          A.INICIO_MES, \
                          A.INICIO_MES as DtRef, \
                          A.FIM_MES, \
                          A.PERFIL_ENERGIA, \
                          A.SUBMERCADO, \
                          A.DTA_OPERACAO, \
                          A.FLEX_MAX, \
                          A.FLEX_MIN, \
                          A.EXTENSAO, \
                          A.SERIE as SERIE1, \
                          A.RECORRENTE, \
                          A.FLAG_ISENCAO, \
                          A.PRECO, \
                          A.DataInicioVigencia, \
                          A.DataFimVigencia, \
                          A.DT_INI_DataVigencia, \
                          A.DT_FIM_DataVigencia, \
                          INI.VALOR AS IniValor, \
                            FIM.VALOR AS FimValor, \
                            CASE \
                          WHEN A.IdEmpresaDestino = 3 THEN A.IdEmpresaOrigem \
                          ELSE A.IdEmpresaDestino END as IdEmpresa \
                        FROM TMP_ENERGIA_CONTRATADA_SAZO_STEP4 AS A \
                        LEFT JOIN \
                          (SELECT VALOR, \
                                  DataVigencia \
                           FROM VW_SERIE_IPCA) AS INI ON INI.DataVigencia = A.DT_INI_DataVigencia \
                        LEFT JOIN \
                          (SELECT VALOR, \
                                  DataVigencia \
                           FROM VW_SERIE_IPCA) AS FIM ON FIM.DataVigencia = A.DT_FIM_DataVigencia \
                          WHERE SUBSTR(A.DiaBase,0,10) IS NOT NULL ")

tmpEnergiaContratadaSazoStep5.createOrReplaceTempView("TMP_ENERGIA_CONTRATADA_SAZO_STEP5")

tmpEnergiaContratadaSazoStep6 = fc_Preco(tmpEnergiaContratadaSazoStep5,tmpEnergiaContratadaSazoStep5.DtRef,tmpEnergiaContratadaSazoStep5.AtrasoReajuste,tmpEnergiaContratadaSazoStep5.MesAutoAjustado,tmpEnergiaContratadaSazoStep5.DiaAutoAjustado,tmpEnergiaContratadaSazoStep5.DiaBase,tmpEnergiaContratadaSazoStep5.PRECO,tmpEnergiaContratadaSazoStep5.Peso,tmpEnergiaContratadaSazoStep5.Id,tmpEnergiaContratadaSazoStep5.IniValor,tmpEnergiaContratadaSazoStep5.FimValor)

tmpEnergiaContratadaSazoStep6 = tmpEnergiaContratadaSazoStep6.withColumnRenamed("precoIntermediario","ValorPreco")

tmpEnergiaContratadaSazo = tmpEnergiaContratadaSazoStep6

tmpEnergiaContratadaSazo.createOrReplaceTempView("TMP_ENERGIA_CONTRATADA_SAZO")