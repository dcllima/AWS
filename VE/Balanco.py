from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructField, StructType, IntegerType, DateType, DoubleType, ArrayType

if __name__ == "__main__":

    spark = SparkSession.builder.master("spark://ec2-3-80-170-100.compute-1.amazonaws.com:7077").appName("BalancoEnergeticoPy").getOrCreate()

    spark.conf.set("spark.sql.crossJoin.enabled", 'true')
    spark.conf.set("spark.executor.memory", "5g")
    spark.conf.set("spark.driver.memory", "15g")
    spark.conf.set("spark.driver.memoryOverhead", "5g")
    spark.conf.set("spark.memory.offHeap.size", "20g")

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

    # declaracao do schema do data frame

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

    categoriasSchema = StructType([
        StructField("Id", StringType(), True),
        StructField("IdTipo", StringType(), True),
        StructField("IdEscopo", StringType(), True),
        StructField("Nome", StringType(), True),
        StructField("Grupo", StringType(), True),
        StructField("Cor", StringType(), True),
        StructField("IdGeneSharp", StringType(), True),
        StructField("Editavel", StringType(), True)])

    categoriasContratosSchema = StructType([
        StructField("IdCategoria", StringType(), True),
        StructField("IdContrato", StringType(), True)])

    contratosSchema = StructType([
        StructField("Id", StringType(), True),
        StructField("IdTipoCodigoReferencia", StringType(), True),
        StructField("CodigoReferencia", StringType(), True),
        StructField("LocalFisico", StringType(), True),
        StructField("Config", StringType(), True),
        StructField("DataCriacao", StringType(), True),
        StructField("IdUsuarioCriador", StringType(), True),
        StructField("DataUltimaModificacao", StringType(), True),
        StructField("IdUsuarioUltimaModificacao", StringType(), True),
        StructField("IdTipoAutomacao", StringType(), True),
        StructField("ConfigAutomacao", StringType(), True),
        StructField("IdArmazenadorAlertas", StringType(), True),
        StructField("IdArmazenadorPermissoes", StringType(), True),
        StructField("IdArmazenadorArquivos", StringType(), True),
        StructField("IdGestorPrimario", StringType(), True),
        # StructField("Observacoes", StringType(), True),
        StructField("IdModelo", StringType(), True),
        StructField("Referencia", StringType(), True),
        StructField("IdTipo", StringType(), True),
        StructField("IdClausulasGarantias", StringType(), True),
        StructField("IdArmazenadorStatus", StringType(), True),
        StructField("DataAcordoComercial", StringType(), True),
        StructField("IdEmpresaMercante", StringType(), True),
        StructField("TipoPeriodoVigencia", StringType(), True),
        StructField("DadosProposta", StringType(), True),
        StructField("IdPropostaOriginal", StringType(), True),
        StructField("IdDadosBancarios", StringType(), True),
        StructField("IdContratoAprovavel", StringType(), True),
        StructField("StatusIncorporacao", StringType(), True),
        StructField("IdUsuarioIncorporacao", StringType(), True),
        StructField("DataIncorporacao", StringType(), True),
        StructField("IdRiscoJuridico", StringType(), True),
        StructField("IdRiscoRegulatorio", StringType(), True),
        StructField("PreAprovacaoRegulatoria", StringType(), True),
        StructField("PreAprovacaoJuridica", StringType(), True),
        StructField("PrefixoAutomatico", StringType(), True),
        StructField("IdDadosIntegrEntFat", StringType(), True),
        StructField("IdArmEventosServidor", StringType(), True),
        StructField("IdArmazenadorCamposSI", StringType(), True),
        StructField("IdArmazenadorNotas", StringType(), True),
        StructField("IdArmMensagensIntegracao", StringType(), True),
        StructField("QtdAditivos", StringType(), True)])

    contratosAprovaveisSchema = StructType([StructField("IdEntidadeAprovavel", StringType(), True)])

    entidadesAprovaveisSchema = StructType([
        StructField("Id", StringType(), True),
        StructField("IdProcesso", StringType(), True),
        StructField("DataValidade", StringType(), True),
        StructField("IdTipo", StringType(), True),
        StructField("DataAprovacaoReprovacao", StringType(), True)])

    entidadesAtivaveisSchema = StructType([
        StructField("IdEntidade", StringType(), True),
        StructField("IdTipoEntidade", StringType(), True),
        StructField("DataDesativacao", StringType(), True),
        StructField("DataUltimaModificacao", StringType(), True),
        StructField("IdUsuarioModificador", StringType(), True)])

    dadosEnergiaFixaSchema = StructType([
        StructField("Id", StringType(), True),
        StructField("IdCaracteristicaEnergia", StringType(), True),
        StructField("InicioIntervalo", StringType(), True),
        StructField("FimIntervalo", StringType(), True),
        StructField("IdModelo", StringType(), True),
        StructField("IdPatamar", StringType(), True),
        StructField("UnidadeValor", StringType(), True),
        StructField("Valor", StringType(), True),
        StructField("Dados", StringType(), True)])

    unidadesEnergiaSchema = StructType([
        StructField("Id", StringType(), True),
        StructField("Descricao", StringType(), True)])

    caracteristicasContratoSchema = StructType([
        StructField("Id", StringType(), True),
        StructField("IdAditivo", StringType(), True),
        StructField("IdCaracteristicaPai", StringType(), True),
        StructField("IdTipo", StringType(), True),
        StructField("IdClausula", StringType(), True),
        StructField("Agrupamento", StringType(), True),
        StructField("IdCaracteristicaOriginal", StringType(), True)])

    agentesSchema = StructType([
        StructField("Id", StringType(), True),
        StructField("IdTipoAgente", StringType(), True),
        StructField("IdStatusRegistroCCEE", StringType(), True),
        StructField("IdAtributosEntidade", StringType(), True),
        StructField("Nome", StringType(), True),
        StructField("CodigoCCEE", StringType(), True),
        StructField("Notas", StringType(), True),
        StructField("CodigoONS", StringType(), True)])

    caracteristicasEnergiaEntregueSchema = StructType([
        StructField("IdCaracteristica", StringType(), True),
        StructField("IdEmpresaOrigem", StringType(), True),
        StructField("EntregaApenasComercial", StringType(), True),
        StructField("IdPerfil", StringType(), True),
        StructField("IdEmpresaDestino", StringType(), True),
        StructField("IdSubmercado", StringType(), True),
        StructField("Descricao", StringType(), True),
        StructField("IdAgenteOrigem", StringType(), True),
        StructField("IdAgenteDestino", StringType(), True),
        StructField("DescontoContratado", StringType(), True),
        StructField("Sufixo", StringType(), True),
        StructField("PerfilEntregaFlexivel", StringType(), True),
        StructField("NaoInferirAutomaticamente", StringType(), True),
        StructField("IdEnergiaDesconto", StringType(), True),
        StructField("OpcaoFaturamento", StringType(), True),
        StructField("IdContratoCCEE", StringType(), True),
        StructField("IdEmpresaEmitenteFatura", StringType(), True),
        StructField("InferirContratoCCEE", StringType(), True),
        StructField("IdConceitoFaturavel", StringType(), True),
        StructField("CFOP", StringType(), True),
        StructField("IdTipoModulacao", StringType(), True),
        StructField("IdAtivoModulacao", StringType(), True)])

    submercadosSchema = StructType([
        StructField("Id", StringType(), True),
        StructField("Acronimo", StringType(), True),
        StructField("Nome", StringType(), True)])

    caracteristicaSazonalizacaoSchema = StructType([
        StructField("IdCaracteristica", StringType(), True),
        StructField("IdCaracteristicaEnergia", StringType(), True),
        StructField("IdBlocoSazonalizacao", StringType(), True),
        StructField("DiaLimite", StringType(), True),
        StructField("MesLimite", StringType(), True)])

    blocosSazonalizacaoSchema = StructType([
        StructField("Id", StringType(), True),
        StructField("IdBlocoFlexibilidade", StringType(), True),
        StructField("LigadoA", StringType(), True)])

    dadosAnosSazonalizacaoSchema = StructType([
        StructField("Id", StringType(), True),
        StructField("IdBlocoSazonalizacao", StringType(), True),
        StructField("IdModelo", StringType(), True),
        StructField("IdPatamar", StringType(), True),
        StructField("Ano", StringType(), True),
        StructField("IdEstadoIntegracaoCCEE", StringType(), True),
        StructField("AlertasEstadoIntegracaoCCEE", StringType(), True)])

    dadosItemSazonalizacaoSchema = StructType([
        StructField("Id", StringType(), True),
        StructField("IdTipoItem", StringType(), True),
        StructField("DataInicio", StringType(), True),
        StructField("DataFim", StringType(), True),
        StructField("Unidade", StringType(), True),
        StructField("ValorLiq", StringType(), True),
        StructField("ValorBruto", StringType(), True)])

    dadosMesesSazonalizacaoSchema = StructType([
        StructField("IdItem", StringType(), True),
        StructField("IdAno", StringType(), True),
        StructField("Inferido", StringType(), True)])

    caracteristicasAjusteSchema = StructType([
        StructField("IdCaracteristica", StringType(), True),
        StructField("InicioValidade", StringType(), True),
        StructField("DiaAutoAjustado", StringType(), True),
        StructField("MesAutoAjustado", StringType(), True),
        StructField("AtrasoReajuste", StringType(), True)])

    metodologiasAjusteSchema = StructType([
        StructField("IdCaracteristicaAjuste", StringType(), True),
        StructField("IdSerie", StringType(), True),
        StructField("DiaBase", StringType(), True),
        StructField("DiaBaseSerie", StringType(), True),
        StructField("Peso", StringType(), True)])

    seriesSchema = StructType([
        StructField("Id", StringType(), True),
        StructField("IdNatureza", StringType(), True),
        StructField("IdTipo", StringType(), True),
        StructField("Descricao", StringType(), True),
        StructField("PalavraChave", StringType(), True),
        StructField("Grupo", StringType(), True),
        StructField("Padrao", StringType(), True),
        StructField("IdEmpresaReferencia", StringType(), True)])

    entradasEEEnergiaFaturasSchema = StructType([
        StructField("IdEntradaEnergiaEntregue", StringType(), True),
        StructField("IdEnergiaFatura", StringType(), True),
        StructField("IdTipoVinculo", StringType(), True),
        StructField("EhFaturaDeCobranca", StringType(), True),
        StructField("EstaAtivo", StringType(), True)])

    energiaFaturasSchema = StructType([
        StructField("Id", StringType(), True),
        StructField("IdFatura", StringType(), True),
        StructField("MesReferencia", StringType(), True),
        StructField("DescricaoEnergia", StringType(), True),
        StructField("ChaveEnergia", StringType(), True),
        StructField("Energia", StringType(), True),
        StructField("DescricaoPreco", StringType(), True),
        StructField("ChavePreco", StringType(), True),
        StructField("Preco", StringType(), True),
        StructField("DescricaoTotal", StringType(), True),
        StructField("ChaveTotal", StringType(), True),
        StructField("Total", StringType(), True),
        StructField("IdArmCamposDinamicos", StringType(), True),
        StructField("IdTipoEnergiaFatura", StringType(), True),
        StructField("CodigoONS", StringType(), True),
        StructField("IdArmazenadorCamposSI", StringType(), True),
        StructField("ModoArredondamento", StringType(), True)])

    faturasSchema = StructType([
        StructField("Id", StringType(), True),
        StructField("IdContrato", StringType(), True),
        StructField("IdAditivo", StringType(), True),
        StructField("IdEmpresa", StringType(), True),
        StructField("IdEmpresaEmpreendimento", StringType(), True),
        StructField("IdEmpresaEmitente", StringType(), True),
        StructField("IdEmpresaFavorecida", StringType(), True),
        StructField("MesReferencia", StringType(), True),
        StructField("IdNatureza", StringType(), True),
        StructField("IdRegraContratual", StringType(), True),
        StructField("NomePlanilha", StringType(), True),
        StructField("Flags", StringType(), True),
        StructField("IdUsuarioCriador", StringType(), True),
        StructField("DataCalculo", StringType(), True),
        StructField("DataApresentacao", StringType(), True),
        StructField("DataApresentacaoIdeal", StringType(), True),
        StructField("DataVencimento", StringType(), True),
        StructField("DataVencimentoIdeal", StringType(), True),
        StructField("DataCriacao", StringType(), True),
        StructField("IdUsuarioModificador", StringType(), True),
        StructField("DataUltimaModificacao", StringType(), True),
        StructField("IdTipoAutomacao", StringType(), True),
        StructField("ConsiderarEReg", StringType(), True),
        StructField("BCICMS", StringType(), True),
        StructField("TaxaICMS", StringType(), True),
        StructField("BCICMS_ST", StringType(), True),
        StructField("TaxaICMS_ST", StringType(), True),
        StructField("PrecoMedio", StringType(), True),
        StructField("AjustesComercManual", StringType(), True),
        StructField("AjustesFinancManual", StringType(), True),
        StructField("AjustesComercExAnte", StringType(), True),
        StructField("AjustesFinancExAnte", StringType(), True),
        StructField("TotalICMS", StringType(), True),
        StructField("TotalICMS_ST", StringType(), True),
        StructField("PercRedBCICMS_ST", StringType(), True),
        StructField("MargemAgregada_ST", StringType(), True),
        StructField("ModoDeCalcBC_ST", StringType(), True),
        StructField("TotalJuros", StringType(), True),
        StructField("Total", StringType(), True),
        StructField("TotalSemICMS", StringType(), True),
        StructField("TotalEnergiaFat", StringType(), True),
        StructField("CustoEnergia", StringType(), True),
        StructField("ComissaoTotal", StringType(), True),
        StructField("RecalculoAssociado", StringType(), True),
        StructField("Comentarios", StringType(), True),
        StructField("IdCaracteristicaDatacao", StringType(), True),
        StructField("CodigoSAP", StringType(), True),
        StructField("IntegrNumDocVenda", StringType(), True),
        StructField("IdArmCamposDinamicos", StringType(), True),
        StructField("IdFaturaAprovavel", StringType(), True),
        StructField("IdNaturezaCCEAR", StringType(), True),
        StructField("IdConfiabilidade", StringType(), True),
        StructField("FaturaLigadaA", StringType(), True),
        StructField("IdMoeda", StringType(), True),
        StructField("IdUnidadeQuantidade", StringType(), True),
        StructField("MontantePonta", StringType(), True),
        StructField("MontanteForaPonta", StringType(), True),
        StructField("IdContratoLigadoA", StringType(), True),
        StructField("IdTipoGeracao", StringType(), True),
        StructField("IntegrNumDocFaturamento", StringType(), True),
        StructField("IntegrStatus", StringType(), True),
        StructField("IdTipoDebito", StringType(), True),
        StructField("IdArmEventosServidor", StringType(), True),
        StructField("NumeroAVD", StringType(), True),
        StructField("NumeroAVC", StringType(), True),
        StructField("IdEncargoSetorial", StringType(), True),
        StructField("IdAcronimo", StringType(), True),
        StructField("IdNormaANEEL", StringType(), True),
        StructField("IdFonteNativa", StringType(), True),
        StructField("IdTipo", StringType(), True),
        StructField("IdArmazenadorCamposSI", StringType(), True),
        StructField("IdEmpresaEnergiaEntregue", StringType(), True),
        StructField("IdArmazenadorLog", StringType(), True),
        StructField("IdArmazenadorArquivos", StringType(), True),
        StructField("IdArmMensagensIntegracao", StringType(), True),
        StructField("DataCompetencia", StringType(), True),
        StructField("PrecoMedioComAjustes", StringType(), True),
        StructField("ProximoVencimentoParaAtrasar", StringType(), True),
        StructField("IdEntradaCUSDFaturavel", StringType(), True),
        StructField("CodigoCFOP", StringType(), True),
        StructField("Fluxo", StringType(), True),
        StructField("IdFaturaPai", StringType(), True),
        StructField("Juros", StringType(), True),
        StructField("Multa", StringType(), True),
        StructField("CorrecaoMonetaria", StringType(), True),
        StructField("ICMSDiferido", StringType(), True),
        StructField("PercICMSDiferido", StringType(), True),
        StructField("MesReferenciaCalculo", StringType(), True),
        StructField("TaxaJuros", StringType(), True),
        StructField("TaxaMulta", StringType(), True),
        StructField("IdSerie", StringType(), True),
        StructField("TipoJuros", StringType(), True),
        StructField("DiasJurosComposto", StringType(), True),
        StructField("ModoArredondamento", StringType(), True),
        StructField("StatusDispensaEncargos", StringType(), True),
        StructField("TaxaPisCofins", StringType(), True),
        StructField("PercentualFCP", StringType(), True),
        StructField("PercentualFCP_ST", StringType(), True),
        StructField("TotalFCP", StringType(), True),
        StructField("TotalFCP_ST", StringType(), True),
        StructField("BCFCP", StringType(), True),
        StructField("BCFCP_ST", StringType(), True),
        StructField("TotalDiasAtraso", StringType(), True),
        StructField("IdAtivo", StringType(), True)])

    PLDMedioSchema = StructType([
        StructField("Data", StringType(), True),
        StructField("IdSubmercado", StringType(), True),
        StructField("IdSerie", StringType(), True),
        StructField("Valor", StringType(), True)])

    camposReaisSISchema = StructType([
        StructField("Id", StringType(), True),
        StructField("IdArmazenadorCamposSI", StringType(), True)])

    camposSISchema = StructType([
        StructField("Id", StringType(), True),
        StructField("IdSistemaIntegrado", StringType(), True),
        StructField("IdTipoNatureza", StringType(), True),
        StructField("IdTipoCampo", StringType(), True),
        StructField("Valor", StringType(), True)])

    caracteristicasPrecoSchema = StructType([
        StructField("IdCaracteristica", StringType(), True),
        StructField("IdParteContratual", StringType(), True),
        StructField("IdVariabilidade", StringType(), True),
        StructField("Descricao", StringType(), True),
        StructField("Sufixo", StringType(), True),
        StructField("DivisaoIntervaloConsiderada", StringType(), True)])

    blocosEnergiaEntregueSchema = StructType([
        StructField("Id", StringType(), True),
        StructField("IdCaracteristicaEnergiaEntr", StringType(), True),
        StructField("InicioPeriodo", StringType(), True),
        StructField("FimPeriodo", StringType(), True),
        StructField("Inferido", StringType(), True)])

    dadosPrecoFuncaoPLDSchema = StructType([
        StructField("Id", StringType(), True),
        StructField("IdCaracteristicaPreco", StringType(), True),
        StructField("InicioIntervalo", StringType(), True),
        StructField("FimIntervalo", StringType(), True),
        StructField("IdModelo", StringType(), True),
        StructField("IdPatamar", StringType(), True),
        StructField("CoeficientePLD", StringType(), True),
        StructField("PrecoBase", StringType(), True),
        StructField("Piso", StringType(), True),
        StructField("Teto", StringType(), True),
        StructField("Dados", StringType(), True)])

    perfilEnergiaSchema = StructType([
        StructField("Id", StringType(), True),
        StructField("Nome", StringType(), True)])

    entradasEnergiaEntregueSchema = StructType([
        StructField("Id", StringType(), True),
        StructField("IdBlocoEnergiaEntregue", StringType(), True),
        StructField("IdEmpresaOrigem", StringType(), True),
        StructField("IdEmpresaDestino", StringType(), True),
        StructField("IdAgenteOrigem", StringType(), True),
        StructField("IdAgenteDestino", StringType(), True),
        StructField("IdTipoEntrega", StringType(), True),
        StructField("IdSubmercado", StringType(), True),
        StructField("IdTipoPreco", StringType(), True),
        StructField("InicioPeriodo", StringType(), True),
        StructField("FimPeriodo", StringType(), True),
        StructField("IdModelo", StringType(), True),
        StructField("IdPatamar", StringType(), True),
        StructField("QuantidadeEntregueMWh", StringType(), True),
        StructField("QuantidadeFaturadaMWh", StringType(), True),
        StructField("DescontoContratado", StringType(), True),
        StructField("DescontoRealizado", StringType(), True),
        StructField("ValorPreco", StringType(), True),
        StructField("CoeficientePLD", StringType(), True),
        StructField("PerfilOrigem", StringType(), True),
        StructField("StatusModulacao", StringType(), True),
        StructField("DataModulacao", StringType(), True),
        StructField("IdRegraModulacao", StringType(), True),
        StructField("IdContratoCCEEModulacao", StringType(), True),
        StructField("IndiceOrdem", StringType(), True),
        StructField("IdEntEnergEntregueAprovavel", StringType(), True),
        StructField("IdEmpresaEmitenteFatura", StringType(), True),
        StructField("IdCaracteristicaPreco", StringType(), True),
        StructField("IdAtributosEntidade", StringType(), True),
        StructField("IdArmazenadorInfoFaturamento", StringType(), True),
        StructField("IdContratoCCEEVinculado", StringType(), True),
        StructField("IdTipoModulacao", StringType(), True),
        StructField("IdAtivoModulacao", StringType(), True)])

    partesContratuaisSchema = StructType([
        StructField("IdAditivo", StringType(), True),
        StructField("IdEmpresa", StringType(), True),
        StructField("IdTipo", StringType(), True),
        StructField("IdTipoContratacao", StringType(), True),
        StructField("IdRepositorioEnergiaCarga", StringType(), True),
        StructField("PercPerdasCarga", StringType(), True),
        StructField("IdOpcoesFaturamento", StringType(), True),
        StructField("IdDadosBancarios", StringType(), True)])

    empresasSchema = StructType([
        StructField("Id", StringType(), True),
        StructField("IdTipoEmpresa", StringType(), True),
        StructField("IdTipoHierarquia", StringType(), True),
        StructField("IdPerfilEnergia", StringType(), True),
        StructField("IdRamoAtividade", StringType(), True),
        StructField("IdEmpresaSuperior", StringType(), True),
        StructField("IdAtributosEntidade", StringType(), True),
        StructField("IdArmazenadorArquivos", StringType(), True),
        StructField("IdEstadoICMS", StringType(), True),
        StructField("RazaoSocial", StringType(), True),
        StructField("NomeFantasia", StringType(), True),
        StructField("CodigoCCEE", StringType(), True),
        StructField("CNPJ", StringType(), True),
        StructField("Site", StringType(), True),
        StructField("Notas", StringType(), True),
        StructField("CodigoCNAE", StringType(), True),
        StructField("TipoContribuicaoICMS", StringType(), True),
        StructField("EmpresaReferencia", StringType(), True),
        StructField("CodigoSAP", StringType(), True),
        StructField("ConfiaAnaliseCred", StringType(), True),
        StructField("ImpostoCumulativo", StringType(), True),
        StructField("CodigoFornecedorIntegracao", StringType(), True),
        StructField("CodigoClienteIntegracao", StringType(), True),
        StructField("IdArmCamposDinamicos", StringType(), True),
        StructField("ConfigIntegracao", StringType(), True),
        StructField("IdArmazenadorArquivosGlobais", StringType(), True),
        StructField("NaturezaPotencial", StringType(), True),
        StructField("IdContainerAnaCred", StringType(), True),
        StructField("CodigoFilial", StringType(), True),
        StructField("ContaContabilClienteIntegr", StringType(), True),
        StructField("ContaContabilFornecedorIntegr", StringType(), True),
        StructField("IntegrStatus", StringType(), True),
        StructField("IdArmEventosServidor", StringType(), True),
        StructField("IdGeneSharp", StringType(), True),
        StructField("CodigoEnderecoIntegracao", StringType(), True),
        StructField("IdArmazenadorCamposSI", StringType(), True),
        StructField("IdArmMensagensIntegracao", StringType(), True),
        StructField("TipoContribuicaoMunicipal", StringType(), True),
        StructField("Governamental", StringType(), True),
        StructField("IdEntidadeAprovavel", StringType(), True),
        StructField("ParticipaArvoreEmpRef", StringType(), True),
        StructField("IdOpcoesFaturamento", StringType(), True),
        StructField("EhPermitidoLogin", StringType(), True)])

    caracteristicasComissaoSchema = StructType([
        StructField("IdCaracteristica", StringType(), True),
        StructField("IdCaracPrecoReferenciada", StringType(), True),
        StructField("IdEmpresaCorretora", StringType(), True),
        StructField("IdEmpresaContratante", StringType(), True),
        StructField("GerarFaturamento", StringType(), True),
        StructField("Desconto", StringType(), True)])

    dadosPrecoComissaoSchema = StructType([
        StructField("Id", StringType(), True),
        StructField("IdCaracteristicaPreco", StringType(), True),
        StructField("InicioIntervalo", StringType(), True),
        StructField("FimIntervalo", StringType(), True),
        StructField("IdModelo", StringType(), True),
        StructField("IdPatamar", StringType(), True),
        StructField("ParcelaFixa", StringType(), True),
        StructField("Percentual", StringType(), True),
        StructField("Dados", StringType(), True),
        StructField("ParcelaFixaEnergia", StringType(), True)])

    ipcaSchema = StructType([
        StructField("DataVigencia", StringType(), True),
        StructField("IdSerie", StringType(), True),
        StructField("Valor", StringType(), True)])

    ipcaProjetadoSchema = StructType([
        StructField("DataVigencia", StringType(), True),
        StructField("IdSerie", StringType(), True),
        StructField("Valor", StringType(), True)])

    caracteristicaPrecoFixoSchema = StructType([
        StructField("IdCaracteristicaPreco", StringType(), True)])
    dadosPrecoFixoSchema = StructType([
        StructField("Id", StringType(), True),
        StructField("IdCaracteristicaPreco", StringType(), True),
        StructField("InicioIntervalo", StringType(), True),
        StructField("FimIntervalo", StringType(), True),
        StructField("IdModelo", StringType(), True),
        StructField("IdPatamar", StringType(), True),
        StructField("Preco", StringType(), True),
        StructField("Dados", StringType(), True)])

    caracteristicasFlexibilidadeSchema = StructType([
        StructField("IdCaracteristica", StringType(), True),
        StructField("IdParteContratual", StringType(), True),
        StructField("IdVariabilidade", StringType(), True),
        StructField("Descricao", StringType(), True),
        StructField("Sufixo", StringType(), True),
        StructField("Periodicidade", StringType(), True),
        StructField("DiaLimite", StringType(), True)])

    dadosFlexibilidadesSchema = StructType([
        StructField("Id", StringType(), True),
        StructField("IdCaracteristicaFlexibilidade", StringType(), True),
        StructField("InicioIntervalo", StringType(), True),
        StructField("FimIntervalo", StringType(), True),
        StructField("IdModelo", StringType(), True),
        StructField("IdPatamar", StringType(), True),
        StructField("FatorMaximo", StringType(), True),
        StructField("FatorMinimo", StringType(), True),
        StructField("Dados", StringType(), True)])

    flexCompraSchema = StructType([
        StructField("COD_CONTRATO", StringType(), True),
        StructField("DTA_REFERENCIA", StringType(), True),
        StructField("VALOR_FLEX", StringType(), True)])

    enderecoEempresasSchema = StructType([
        StructField("IdEndereco", StringType(), True),
        StructField("IdEmpresa", StringType(), True),
        StructField("Tipo", StringType(), True),
        StructField("Descricao", StringType(), True),
        StructField("UsarParaFaturamento", StringType(), True)])

    enderecosSchema = StructType([
        StructField("Id", StringType(), True),
        StructField("EnderecoPrincipal", StringType(), True),
        StructField("IdEstado", StringType(), True),
        StructField("IdPais", StringType(), True),
        StructField("CodigoIBGEMunicipio", StringType(), True),
        StructField("IdModoEndereco", StringType(), True),
        StructField("Complemento", StringType(), True),
        StructField("Bairro", StringType(), True),
        StructField("CEP", StringType(), True),
        StructField("EnderecoLivre", StringType(), True),
        StructField("Numero", StringType(), True),
        StructField("Telefone", StringType(), True)])

    municipiosSchema = StructType([
        StructField("CodigoIBGE", StringType(), True),
        StructField("CodigoIBGEEstado", StringType(), True),
        StructField("Nome", StringType(), True)])

    estadosSchema = StructType([
        StructField("Id", StringType(), True),
        StructField("CodigoIBGE", StringType(), True),
        StructField("Nome", StringType(), True),
        StructField("Sigla", StringType(), True),
        StructField("IdSubmercado", StringType(), True),
        StructField("IdPais", StringType(), True)])

    outrosContratosSchema = StructType([
        StructField("PERIODO", StringType(), True),
        StructField("CONTRATO", StringType(), True),
        StructField("VALOR", StringType(), True)])

    liquidacaoSchema = StructType([
        StructField("ANOMES", StringType(), True),
        StructField("LIQUIDACAO", StringType(), True),
        StructField("MODULACAO_CEMIG", StringType(), True)])

    dcideSchema = StructType([StructField("ID", StringType(), True),
                              StructField("DTA_REF", StringType(), True),
                              StructField("SEMANA", StringType(), True),
                              StructField("DTA_INICIO_REF", StringType(), True),
                              StructField("DTA_FIM_REF", StringType(), True),
                              StructField("C_SPREAD", StringType(), True),
                              StructField("C_M0", StringType(), True),
                              StructField("C_M1", StringType(), True),
                              StructField("C_M2", StringType(), True),
                              StructField("C_M3", StringType(), True),
                              StructField("C_A0", StringType(), True),
                              StructField("C_A1", StringType(), True),
                              StructField("C_A2", StringType(), True),
                              StructField("C_A3", StringType(), True),
                              StructField("C_A4", StringType(), True),
                              StructField("I_SWAP", StringType(), True),
                              StructField("I_M0", StringType(), True),
                              StructField("I_M1", StringType(), True),
                              StructField("I_M2", StringType(), True),
                              StructField("I_M3", StringType(), True),
                              StructField("I_A0", StringType(), True),
                              StructField("I_A1", StringType(), True),
                              StructField("I_A2", StringType(), True),
                              StructField("I_A3", StringType(), True),
                              StructField("I_A4", StringType(), True),
                              StructField("VE_C_A0", StringType(), True),
                              StructField("VE_C_A1", StringType(), True),
                              StructField("VE_C_A2", StringType(), True),
                              StructField("VE_C_A3", StringType(), True),
                              StructField("VE_C_A4", StringType(), True),
                              StructField("VE_I_A0", StringType(), True),
                              StructField("VE_I_A1", StringType(), True),
                              StructField("VE_I_A2", StringType(), True),
                              StructField("VE_I_A3", StringType(), True),
                              StructField("VE_I_A4", StringType(), True),
                              StructField("SUBMERCADO", StringType(), True)])

    # ler Arquivo
    a = spark.read.csv('s3://ve-datalake-stage-vg-dev/gene/contratos/comercial/tmp_aditivos/',
                       schema=aditivosSchema).coalesce(20)