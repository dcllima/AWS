from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructField, StructType, IntegerType, DateType, DoubleType, ArrayType
import datetime
from dateutil.relativedelta import relativedelta
from pyspark.sql.functions import udf, collect_list, explode, concat_ws, to_date, lit, when, month, year, dayofmonth, \
    concat, round, isnull, coalesce, abs, datediff, add_months, date_sub, trunc, max, last_day
from pyspark.sql.types import IntegerType


spark = SparkSession.builder.appName("BalancoEnergeticoPy").getOrCreate()

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
ct = spark.read.csv('s3://ve-datalake-stage-vg-dev/gene/contratos/comercial/tmp_categorias/',
                    schema=categoriasSchema)
ctc = spark.read.csv('s3://ve-datalake-stage-vg-dev/gene/contratos/comercial/tmp_categoriasContratos/',
                     schema=categoriasContratosSchema).coalesce(20)
c = spark.read.csv('s3://ve-datalake-stage-vg-dev/gene/contratos/comercial/tmp_contratos/', schema=contratosSchema,
                   multiLine=True, escape="\n").coalesce(20)
ea = spark.read.csv('s3://ve-datalake-stage-vg-dev/gene/contratos/comercial/tmp_entidadesAtivaveis/',
                    schema=entidadesAtivaveisSchema).coalesce(20)
ca = spark.read.csv('s3://ve-datalake-stage-vg-dev/gene/contratos/comercial/tmp_contratosAprovaveis/',
                    schema=contratosAprovaveisSchema).coalesce(20)
eap = spark.read.csv('s3://ve-datalake-stage-vg-dev/gene/contratos/comercial/tmp_entidadesAprovaveis/',
                     schema=entidadesAprovaveisSchema).coalesce(20)
denf = spark.read.csv('s3://ve-datalake-stage-vg-dev/gene/contratos/comercial/tmp_dadosEnergiaFixa/',
                      schema=dadosEnergiaFixaSchema).coalesce(20)
ue = spark.read.csv('s3://ve-datalake-stage-vg-dev/gene/contratos/comercial/tmp_unidadesEnergia/',
                    schema=unidadesEnergiaSchema)
cc = spark.read.csv('s3://ve-datalake-stage-vg-dev/gene/contratos/comercial/tmp_caracteristicasContrato/',
                    schema=caracteristicasContratoSchema).coalesce(20)
ag = spark.read.csv('s3://ve-datalake-stage-vg-dev/gene/contratos/comercial/tmp_agentes/', schema=agentesSchema)
ce = spark.read.csv('s3://ve-datalake-stage-vg-dev/gene/contratos/comercial/tmp_caracteristicasEnergiaEntregue/',
                    schema=caracteristicasEnergiaEntregueSchema).coalesce(20)
sub = spark.read.csv('s3://ve-datalake-stage-vg-dev/gene/contratos/comercial/tmp_submercados/',
                     schema=submercadosSchema)
cs = spark.read.csv('s3://ve-datalake-stage-vg-dev/gene/contratos/comercial/tmp_caracteristicaSazonalizacao/',
                    schema=caracteristicaSazonalizacaoSchema)
bs = spark.read.csv('s3://ve-datalake-stage-vg-dev/gene/contratos/comercial/tmp_blocosSazonalizacao/',
                    schema=blocosSazonalizacaoSchema).coalesce(20)
das = spark.read.csv('s3://ve-datalake-stage-vg-dev/gene/contratos/comercial/tmp_dadosAnosSazonalizacao/',
                     schema=dadosAnosSazonalizacaoSchema)
dis = spark.read.csv('s3://ve-datalake-stage-vg-dev/gene/contratos/comercial/tmp_dadosItemSazonalizacao/',
                     schema=dadosItemSazonalizacaoSchema).coalesce(20)
dms = spark.read.csv('s3://ve-datalake-stage-vg-dev/gene/contratos/comercial/tmp_dadosMesesSazonalizacao/',
                     schema=dadosMesesSazonalizacaoSchema).coalesce(20)
caj = spark.read.csv('s3://ve-datalake-stage-vg-dev/gene/contratos/comercial/tmp_caracteristicasAjuste/',
                     schema=caracteristicasAjusteSchema)
ma = spark.read.csv('s3://ve-datalake-stage-vg-dev/gene/contratos/comercial/tmp_metodologiasAjuste/',
                    schema=metodologiasAjusteSchema)
s = spark.read.csv('s3://ve-datalake-stage-vg-dev/gene/contratos/comercial/tmp_series/', schema=seriesSchema)
eeef = spark.read.csv('s3://ve-datalake-stage-vg-dev/gene/contratos/comercial/tmp_entradasEEEnergiaFaturas/',
                      schema=entradasEEEnergiaFaturasSchema).coalesce(20)
ef = spark.read.csv('s3://ve-datalake-stage-vg-dev/gene/contratos/comercial/tmp_energiaFaturas/',
                    schema=energiaFaturasSchema).coalesce(20)
f = spark.read.csv('s3://ve-datalake-stage-vg-dev/gene/contratos/comercial/tmp_faturas/',
                   schema=faturasSchema).coalesce(20)
pldm = spark.read.csv('s3://ve-datalake-stage-vg-dev/gene/contratos/comercial/tmp_pLDMedio/', schema=PLDMedioSchema)
pldm1 = spark.read.csv('s3://ve-datalake-stage-vg-dev/gene/contratos/comercial/tmp_pLDMedio/',
                       schema=PLDMedioSchema)
si = spark.read.csv('s3://ve-datalake-stage-vg-dev/gene/contratos/comercial/tmp_camposReaisSI/',
                    schema=camposReaisSISchema).coalesce(20)
csi = spark.read.csv('s3://ve-datalake-stage-vg-dev/gene/contratos/comercial/tmp_camposSI/',
                     schema=camposSISchema).coalesce(20)
cpr = spark.read.csv('s3://ve-datalake-stage-vg-dev/gene/contratos/comercial/tmp_caracteristicasPreco/',
                     schema=caracteristicasPrecoSchema).coalesce(20)
bee = spark.read.csv('s3://ve-datalake-stage-vg-dev/gene/contratos/comercial/tmp_blocosEnergiaEntregue/',
                     schema=blocosEnergiaEntregueSchema).coalesce(20)
pld = spark.read.csv('s3://ve-datalake-stage-vg-dev/gene/contratos/comercial/tmp_dadosPrecoFuncaoPLD/',
                     schema=dadosPrecoFuncaoPLDSchema)
pe = spark.read.csv('s3://ve-datalake-stage-vg-dev/gene/contratos/comercial/tmp_perfilEnergia/',
                    schema=perfilEnergiaSchema)
eee = spark.read.csv('s3://ve-datalake-stage-vg-dev/gene/contratos/comercial/tmp_entradasEnergiaEntregue/',
                     schema=entradasEnergiaEntregueSchema).coalesce(20)
pc = spark.read.csv('s3://ve-datalake-stage-vg-dev/gene/contratos/comercial/tmp_partesContratuais/',
                    schema=partesContratuaisSchema).coalesce(20)
e = spark.read.csv('s3://ve-datalake-stage-vg-dev/gene/contratos/comercial/tmp_empresas/', schema=empresasSchema)
cm = spark.read.csv('s3://ve-datalake-stage-vg-dev/gene/contratos/comercial/tmp_caracteristicasComissao/',
                    schema=caracteristicasComissaoSchema)
dcp = spark.read.csv('s3://ve-datalake-stage-vg-dev/gene/contratos/comercial/tmp_dadosPrecoComissao/',
                     schema=dadosPrecoComissaoSchema)
i = spark.read.csv('s3://ve-datalake-stage-vg-dev/gene/contratos/comercial/tmp_ipca/', schema=ipcaSchema)
ip = spark.read.csv('s3://ve-datalake-stage-vg-dev/gene/contratos/comercial/tmp_ipcaProjetado/',
                    schema=ipcaProjetadoSchema)
cprf = spark.read.csv('s3://ve-datalake-stage-vg-dev/gene/contratos/comercial/tmp_caracteristicaPrecoFixo/',
                      schema=caracteristicaPrecoFixoSchema)
dpf = spark.read.csv('s3://ve-datalake-stage-vg-dev/gene/contratos/comercial/tmp_dadosPrecoFixo/',
                     schema=dadosPrecoFixoSchema).coalesce(20)
cf = spark.read.csv('s3://ve-datalake-stage-vg-dev/gene/contratos/comercial/tmp_caracteristicasFlexibilidade/',
                    schema=caracteristicasFlexibilidadeSchema)
df = spark.read.csv('s3://ve-datalake-stage-vg-dev/gene/contratos/comercial/tmp_dadosFlexibilidades/',
                    schema=dadosFlexibilidadesSchema)
fc = spark.read.csv('s3://ve-datalake-stage-vg-dev/gene/contratos/comercial//tmp_flexCompra/',
                    schema=flexCompraSchema)
ee = spark.read.csv('s3://ve-datalake-stage-vg-dev/gene/contratos/comercial/tmp_enderecosEmpresas/',
                    schema=enderecoEempresasSchema)
ed = spark.read.csv('s3://ve-datalake-stage-vg-dev/gene/contratos/comercial/tmp_enderecos/', schema=enderecosSchema)
m = spark.read.csv('s3://ve-datalake-stage-vg-dev/gene/contratos/comercial/tmp_municipios/',
                   schema=municipiosSchema)
es = spark.read.csv('s3://ve-datalake-stage-vg-dev/gene/contratos/comercial/tmp_estados/', schema=estadosSchema)
oc = spark.read.csv('s3://ve-datalake-stage-vg-dev/gene/contratos/comercial/tmp_outrosContratos/',
                    schema=outrosContratosSchema)
l = spark.read.csv('s3://ve-datalake-stage-vg-dev/gene/contratos/comercial/tmp_liquidacao/',
                   schema=liquidacaoSchema)
d = spark.read.csv('s3://ve-datalake-stage-vg-dev/gene/contratos/comercial/tmp_dcide/', schema=dcideSchema)

# cria TempView

a.createOrReplaceTempView("Aditivos")
a = spark.sql("select * from Aditivos where dt_ref = (select max(dt_ref) from Aditivos)")
a.createOrReplaceTempView("Aditivos")
ct.createOrReplaceTempView("Categorias")
ct = spark.sql("select * from Categorias where dt_ref = (select max(dt_ref) from Categorias)")
ct.createOrReplaceTempView("Categorias")
ctc.createOrReplaceTempView("CategoriasContratos")
ctc = spark.sql("select * from CategoriasContratos where dt_ref = (select max(dt_ref) from CategoriasContratos)")
ctc.createOrReplaceTempView("CategoriasContratos")
c.createOrReplaceTempView("Contratos")
c = spark.sql("select * from Contratos where dt_ref = (select max(dt_ref) from Contratos)")
c.createOrReplaceTempView("Contratos")
ea.createOrReplaceTempView("EntidadesAtivaveis")
ea = spark.sql("select * from EntidadesAtivaveis where dt_ref = (select max(dt_ref) from EntidadesAtivaveis)")
ea.createOrReplaceTempView("EntidadesAtivaveis")
ca.createOrReplaceTempView("ContratosAprovaveis")
ca = spark.sql("select * from ContratosAprovaveis where dt_ref = (select max(dt_ref) from ContratosAprovaveis)")
ca.createOrReplaceTempView("ContratosAprovaveis")
eap.createOrReplaceTempView("EntidadesAprovaveis")
eap = spark.sql("select * from EntidadesAprovaveis where dt_ref = (select max(dt_ref) from EntidadesAprovaveis)")
eap.createOrReplaceTempView("EntidadesAprovaveis")
denf.createOrReplaceTempView("DadosEnergiaFixa")
denf = spark.sql("select * from DadosEnergiaFixa where dt_ref = (select max(dt_ref) from DadosEnergiaFixa)")
denf.createOrReplaceTempView("DadosEnergiaFixa")
ue.createOrReplaceTempView("UnidadesEnergia")
ue = spark.sql("select * from UnidadesEnergia where dt_ref = (select max(dt_ref) from UnidadesEnergia)")
ue.createOrReplaceTempView("UnidadesEnergia")
cc.createOrReplaceTempView("CaracteristicasContrato")
cc = spark.sql(
    "select * from CaracteristicasContrato where dt_ref = (select max(dt_ref) from CaracteristicasContrato)")
cc.createOrReplaceTempView("CaracteristicasContrato")
ag.createOrReplaceTempView("Agentes")
ag = spark.sql("select * from Agentes where dt_ref = (select max(dt_ref) from Agentes)")
ag.createOrReplaceTempView("Agentes")
ce.createOrReplaceTempView("CaracteristicasEnergiaEntregue")
ce = spark.sql(
    "select * from CaracteristicasEnergiaEntregue where dt_ref = (select max(dt_ref) from CaracteristicasEnergiaEntregue)")
ce.createOrReplaceTempView("CaracteristicasEnergiaEntregue")
sub.createOrReplaceTempView("Submercados")
sub = spark.sql("select * from Submercados where dt_ref = (select max(dt_ref) from Submercados)")
sub.createOrReplaceTempView("Submercados")
cs.createOrReplaceTempView("CaracteristicaSazonalizacao")
cs = spark.sql(
    "select * from CaracteristicaSazonalizacao where dt_ref = (select max(dt_ref) from CaracteristicaSazonalizacao)")
cs.createOrReplaceTempView("CaracteristicaSazonalizacao")
bs.createOrReplaceTempView("BlocosSazonalizacao")
bs = spark.sql("select * from BlocosSazonalizacao where dt_ref = (select max(dt_ref) from BlocosSazonalizacao)")
bs.createOrReplaceTempView("BlocosSazonalizacao")
das.createOrReplaceTempView("DadosAnosSazonalizacao")
das = spark.sql(
    "select * from DadosAnosSazonalizacao where dt_ref = (select max(dt_ref) from DadosAnosSazonalizacao)")
das.createOrReplaceTempView("DadosAnosSazonalizacao")
dis.createOrReplaceTempView("DadosItemSazonalizacao")
dis = spark.sql(
    "select * from DadosItemSazonalizacao where dt_ref = (select max(dt_ref) from DadosItemSazonalizacao)")
dis.createOrReplaceTempView("DadosItemSazonalizacao")
dms.createOrReplaceTempView("DadosMesesSazonalizacao")
dms = spark.sql(
    "select * from DadosMesesSazonalizacao where dt_ref = (select max(dt_ref) from DadosMesesSazonalizacao)")
dms.createOrReplaceTempView("DadosMesesSazonalizacao")
caj.createOrReplaceTempView("CaracteristicasAjuste")
caj = spark.sql(
    "select * from CaracteristicasAjuste where dt_ref = (select max(dt_ref) from CaracteristicasAjuste)")
caj.createOrReplaceTempView("CaracteristicasAjuste")
ma.createOrReplaceTempView("MetodologiasAjuste")
ma = spark.sql("select * from MetodologiasAjuste where dt_ref = (select max(dt_ref) from MetodologiasAjuste)")
ma.createOrReplaceTempView("MetodologiasAjuste")
s.createOrReplaceTempView("Series")
s = spark.sql("select * from Series where dt_ref = (select max(dt_ref) from Series)")
s.createOrReplaceTempView("Series")
eeef.createOrReplaceTempView("EntradasEEEnergiaFaturas")
eeef = spark.sql(
    "select * from EntradasEEEnergiaFaturas where dt_ref = (select max(dt_ref) from EntradasEEEnergiaFaturas)")
eeef.createOrReplaceTempView("EntradasEEEnergiaFaturas")
ef.createOrReplaceTempView("EnergiaFaturas")
ef = spark.sql("select * from EnergiaFaturas where dt_ref = (select max(dt_ref) from EnergiaFaturas)")
ef.createOrReplaceTempView("EnergiaFaturas")
f.createOrReplaceTempView("Faturas")
f = spark.sql("select * from Faturas where dt_ref = (select max(dt_ref) from Faturas)")
f.createOrReplaceTempView("Faturas")
pldm.createOrReplaceTempView("PLDMedio")
pldm = spark.sql("select * from PLDMedio where dt_ref = (select max(dt_ref) from PLDMedio)")
pldm.createOrReplaceTempView("PLDMedio")
pldm1.createOrReplaceTempView("PLDMedio1")
pldm1 = spark.sql("select * from PLDMedio1 where dt_ref = (select max(dt_ref) from PLDMedio1)")
pldm1.createOrReplaceTempView("PLDMedio1")
si.createOrReplaceTempView("CamposReaisSI")
si = spark.sql("select * from CamposReaisSI where dt_ref = (select max(dt_ref) from CamposReaisSI)")
si.createOrReplaceTempView("CamposReaisSI")
csi.createOrReplaceTempView("CamposSI")
csi = spark.sql("select * from CamposSI where dt_ref = (select max(dt_ref) from CamposSI)")
csi.createOrReplaceTempView("CamposSI")
cpr.createOrReplaceTempView("CaracteristicasPreco")
cpr = spark.sql("select * from CaracteristicasPreco where dt_ref = (select max(dt_ref) from CaracteristicasPreco)")
cpr.createOrReplaceTempView("CaracteristicasPreco")
bee.createOrReplaceTempView("BlocosEnergiaEntregue")
bee = spark.sql(
    "select * from BlocosEnergiaEntregue where dt_ref = (select max(dt_ref) from BlocosEnergiaEntregue)")
bee.createOrReplaceTempView("BlocosEnergiaEntregue")
pld.createOrReplaceTempView("DadosPrecoFuncaoPLD")
pld = spark.sql("select * from DadosPrecoFuncaoPLD where dt_ref = (select max(dt_ref) from DadosPrecoFuncaoPLD)")
pld.createOrReplaceTempView("DadosPrecoFuncaoPLD")
pe.createOrReplaceTempView("PerfilEnergia")
pe = spark.sql("select * from PerfilEnergia where dt_ref = (select max(dt_ref) from PerfilEnergia)")
pe.createOrReplaceTempView("PerfilEnergia")
eee.createOrReplaceTempView("EntradasEnergiaEntregue")
eee = spark.sql(
    "select * from EntradasEnergiaEntregue where dt_ref = (select max(dt_ref) from EntradasEnergiaEntregue)")
eee.createOrReplaceTempView("EntradasEnergiaEntregue")
pc.createOrReplaceTempView("PartesContratuais")
pc = spark.sql("select * from PartesContratuais where dt_ref = (select max(dt_ref) from PartesContratuais)")
pc.createOrReplaceTempView("PartesContratuais")
e.createOrReplaceTempView("Empresas")
e = spark.sql("select * from Empresas where dt_ref = (select max(dt_ref) from Empresas)")
e.createOrReplaceTempView("Empresas")
cm.createOrReplaceTempView("CaracteristicasComissao")
cm = spark.sql(
    "select * from CaracteristicasComissao where dt_ref = (select max(dt_ref) from CaracteristicasComissao)")
cm.createOrReplaceTempView("CaracteristicasComissao")
dcp.createOrReplaceTempView("DadosPrecoComissao")
dcp = spark.sql("select * from DadosPrecoComissao where dt_ref = (select max(dt_ref) from DadosPrecoComissao)")
dcp.createOrReplaceTempView("DadosPrecoComissao")
i.createOrReplaceTempView("IPCA")
i = spark.sql("select * from IPCA where dt_ref = (select max(dt_ref) from IPCA)")
i.createOrReplaceTempView("IPCA")
ip.createOrReplaceTempView("IPCA_PROJETADO")
ip = spark.sql("select * from IPCA_PROJETADO where dt_ref = (select max(dt_ref) from IPCA_PROJETADO)")
ip.createOrReplaceTempView("IPCA_PROJETADO")
cprf.createOrReplaceTempView("CaracteristicaPrecoFixo")
cprf = spark.sql(
    "select * from CaracteristicaPrecoFixo where dt_ref = (select max(dt_ref) from CaracteristicaPrecoFixo)")
cprf.createOrReplaceTempView("CaracteristicaPrecoFixo")
dpf.createOrReplaceTempView("DadosPrecoFixo")
dpf = spark.sql("select * from DadosPrecoFixo where dt_ref = (select max(dt_ref) from DadosPrecoFixo)")
dpf.createOrReplaceTempView("DadosPrecoFixo")
cf.createOrReplaceTempView("CaracteristicasFlexibilidade")
cf = spark.sql(
    "select * from CaracteristicasFlexibilidade where dt_ref = (select max(dt_ref) from CaracteristicasFlexibilidade)")
cf.createOrReplaceTempView("CaracteristicasFlexibilidade")
df.createOrReplaceTempView("DadosFlexibilidades")
df = spark.sql("select * from DadosFlexibilidades where dt_ref = (select max(dt_ref) from DadosFlexibilidades)")
df.createOrReplaceTempView("DadosFlexibilidades")
fc.createOrReplaceTempView("FLEX_COMPRA")
fc = spark.sql("select * from FLEX_COMPRA where dt_ref = (select max(dt_ref) from FLEX_COMPRA)")
fc.createOrReplaceTempView("FLEX_COMPRA")
ee.createOrReplaceTempView("EnderecosEmpresas")
ee = spark.sql("select * from EnderecosEmpresas where dt_ref = (select max(dt_ref) from EnderecosEmpresas)")
ee.createOrReplaceTempView("EnderecosEmpresas")
ed.createOrReplaceTempView("Enderecos")
ed = spark.sql("select * from Enderecos where dt_ref = (select max(dt_ref) from Enderecos)")
ed.createOrReplaceTempView("Enderecos")
m.createOrReplaceTempView("Municipios")
m = spark.sql("select * from Municipios where dt_ref = (select max(dt_ref) from Municipios)")
m.createOrReplaceTempView("Municipios")
es.createOrReplaceTempView("Estados")
es = spark.sql("select * from Estados where dt_ref = (select max(dt_ref) from Estados)")
es.createOrReplaceTempView("Estados")
oc.createOrReplaceTempView("OutrosContratos")
oc = spark.sql("select * from OutrosContratos where dt_ref = (select max(dt_ref) from OutrosContratos)")
oc.createOrReplaceTempView("OutrosContratos")
l.createOrReplaceTempView("Liquidacao")
l = spark.sql("select * from Liquidacao where dt_ref = (select max(dt_ref) from Liquidacao)")
l.createOrReplaceTempView("Liquidacao")
d.createOrReplaceTempView("DCIDE")
d = spark.sql("select * from DCIDE where dt_ref = (select max(dt_ref) from DCIDE)")
d.createOrReplaceTempView("DCIDE")


# cria funcoes UDF

def fc_Qtd_Horas_Periodo(df, dtInicio, dtFim):

    df = df.withColumn("dtInicio", to_date(df.pInicio)) \
        .withColumn("dtFim", to_date(df.pFim)) \
        .withColumn("Qtd_horas", lit(0))
    df = df.withColumn("Qtd_horas",
                       when((year(df.dtInicio) < lit(2018)) & (month(df.dtInicio) == lit(2)),
                            abs(datediff(df.dtFim, df.dtInicio) + lit(1)) * lit(24) + lit(1))
                       .when((year(df.dtInicio) < lit(2018)) & (month(df.dtInicio) == lit(10)),
                             abs(datediff(df.dtFim, df.dtInicio) + lit(1)) * lit(24) - lit(1))
                       .when((year(df.dtInicio) < lit(2019)) & (month(df.dtInicio) == lit(11)),
                             abs(datediff(df.dtFim, df.dtInicio) + lit(1)) * lit(24))
                       .when((year(df.dtInicio) < lit(2019)) & (month(df.dtInicio) == lit(2)),
                             abs(datediff(df.dtFim, df.dtInicio) + lit(1)) * lit(24) + lit(1))
                       .when((year(df.dtInicio) < lit(2020)) & (month(df.dtInicio) == lit(11)),
                             abs(datediff(df.dtFim, df.dtInicio) + lit(1)) * lit(24) - lit(1))
                       .when((year(df.dtInicio) < lit(2020)) & (month(df.dtInicio) == lit(2)),
                             abs(datediff(df.dtFim, df.dtInicio) + lit(1)) * lit(24))
                       .when((year(df.dtInicio) > lit(2020)) & (month(df.dtInicio) == lit(11)),
                             abs(datediff(df.dtFim, df.dtInicio) + lit(1)) * lit(24) - lit(1))
                       .when((year(df.dtInicio) > lit(2020)) & (month(df.dtInicio) == lit(2)),
                             abs(datediff(df.dtFim, df.dtInicio) + lit(1)) * lit(24) + lit(1))
                       .otherwise(abs(datediff(df.dtFim, df.dtInicio) + lit(1)) * lit(24)))
    df2 = df.drop(df.dtInicio) \
        .drop(df.dtFim)
    return df2


def fc_Fonte_Preco(pRef, pBalanco):

    dtRef = datetime.datetime.strptime(str(pRef), '%Y-%m-%d')
    dtBalanco = datetime.datetime.strptime(str(pBalanco), '%Y-%m-%d')

    if (dtRef.year - dtBalanco.year) < 0 or (
            (dtRef.year - dtBalanco.year) == 0 and (dtRef.month - dtBalanco.month) <= 0):
        retorno = 'DCIDE'
        return retorno
    else:
        inicio = dtRef + relativedelta(months=+1)
        fim = dtBalanco
        dias = 0

    while inicio <= fim:
        if inicio.weekday() != 1 or inicio.weekday() != 7:
            dias = dias + 1
        inicio = inicio + relativedelta(days=+1)

    if dias >= 8:
        retorno = 'PLD'
    else:
        retorno = 'DCIDE'
    return retorno


def fc_Qtd_Horas_Mes(df, Ref):

    df = df.withColumn("pRef", to_date(df.DtRef)) \
        .withColumn("nextMonth", add_months(to_date(df.DtRef), 1)) \
        .withColumn("firstDay", trunc(df.DtRef, "month")) \
        .withColumn("endDay", date_sub(add_months(trunc(df.DtRef, "month"), 1), 1)) \
        .withColumn("Qtd", lit(0)) \
        .withColumn("Qtd_horas", lit(0))
    df = df.withColumn("Qtd",
                       when((year(df.pRef) < lit(2018)) & (month(df.pRef) == lit(2)), lit(1))
                       .when((year(df.pRef) < lit(2018)) & (month(df.pRef) == lit(10)), lit(-1))
                       .when((year(df.pRef) < lit(2019)) & (month(df.pRef) == lit(11)), lit(0))
                       .when((year(df.pRef) < lit(2019)) & (month(df.pRef) == lit(2)), lit(1))
                       .when((year(df.pRef) < lit(2020)) & (month(df.pRef) == lit(11)), lit(-1))
                       .when((year(df.pRef) < lit(2020)) & (month(df.pRef) == lit(2)), lit(0))
                       .when((year(df.pRef) > lit(2020)) & (month(df.pRef) == lit(11)), lit(-1))
                       .when((year(df.pRef) > lit(2020)) & (month(df.pRef) == lit(2)), lit(1))
                       .otherwise(0))
    df = df.withColumn("Qtd_horas", abs(datediff(df.endDay, df.firstDay) + lit(1)) * lit(24) + df.Qtd)

    df2 = df.drop(df.pRef) \
        .drop(df.nextMonth) \
        .drop(df.firstDay) \
        .drop(df.endDay) \
        .drop(df.Qtd)
    return df2


def fc_Natureza(df, check, tipo):
    df = df.withColumn("pTipo", df.TIPO) \
        .withColumn("check", df.Count.cast(IntegerType()))
    df = df.withColumn("Natureza",
                       when((df.pTipo == lit("IGUAL")) & (df.check > lit(0)), "Serviços")
                       .when((df.pTipo == lit("IGUAL")) & (df.check <= lit(0)), "Terceiros")
                       .when((df.pTipo != lit("IGUAL")) & (df.check == lit(2)), "Venda")
                       .when((df.pTipo != lit("IGUAL")) & (df.check == lit(1)), "Compra")
                       .otherwise("Terceiros"))
    df2 = df.drop(df.pTipo) \
        .drop(df.check)
    return df2


def fc_Pld_Submercado(df, vPerfilEnergia, pRef, pBalanco, vconvM0, vconvM1, vconvM2, vconvM3, vconvA0, vconvA1,
                      vconvA2, vconvA3, vconvA4, viM0, viM1, viM2, viM3, viA0, viA1, viA2, viA3, viA4):

    df = df.withColumn("PerfilEnergia", df.Energia) \
        .withColumn("dtRef", to_date(df.DTA_REF)) \
        .withColumn("dtBalanco", to_date(df.Periodo)) \
        .withColumn("convM0", df.C_M0.cast("double")) \
        .withColumn("convM1", df.C_M1.cast("double")) \
        .withColumn("convM2", df.C_M2.cast("double")) \
        .withColumn("convM3", df.C_M3.cast("double")) \
        .withColumn("convA0", df.C_A0.cast("double")) \
        .withColumn("convA1", df.C_A1.cast("double")) \
        .withColumn("convA2", df.C_A2.cast("double")) \
        .withColumn("convA3", df.C_A3.cast("double")) \
        .withColumn("convA4", df.C_A4.cast("double")) \
        .withColumn("iM0", df.I_M0.cast("double")) \
        .withColumn("iM1", df.I_M1.cast("double")) \
        .withColumn("iM2", df.I_M2.cast("double")) \
        .withColumn("iM3", df.I_M3.cast("double")) \
        .withColumn("iA0", df.I_A0.cast("double")) \
        .withColumn("iA1", df.I_A1.cast("double")) \
        .withColumn("iA2", df.I_A2.cast("double")) \
        .withColumn("iA3", df.I_A3.cast("double")) \
        .withColumn("iA4", df.I_A4.cast("double")) \
        .withColumn("CURVA_PLD", lit(0.0))
    df = df.withColumn("iM0", \
                       when(df.PerfilEnergia == 'I1', df.convM0 + lit(150)) \
                       .otherwise(df.iM0))
    df = df.withColumn("iM1", \
                       when(df.PerfilEnergia == 'I1', df.convM1 + lit(150)) \
                       .otherwise(df.iM1))
    df = df.withColumn("iM2", \
                       when(df.PerfilEnergia == 'I1', df.convM2 + lit(150)) \
                       .otherwise(df.iM2))
    df = df.withColumn("iM3", \
                       when(df.PerfilEnergia == 'I1', df.convM3 + lit(150)) \
                       .otherwise(df.iM3))
    df = df.withColumn("iA0", \
                       when(df.PerfilEnergia == 'I1', df.convA0 + lit(150)) \
                       .otherwise(df.iA0))
    df = df.withColumn("iA1", \
                       when(df.PerfilEnergia == 'I1', df.convA1 + lit(150)) \
                       .otherwise(df.iA1))
    df = df.withColumn("iA2", \
                       when(df.PerfilEnergia == 'I1', df.convA2 + lit(150)) \
                       .otherwise(df.iA2))
    df = df.withColumn("iA3", \
                       when(df.PerfilEnergia == 'I1', df.convA3 + lit(150)) \
                       .otherwise(df.iA3))
    df = df.withColumn("iA4", \
                       when(df.PerfilEnergia == 'CQ5', df.convA4 + lit(15)) \
                       .otherwise(df.iA4))
    df = df.withColumn("iM0", \
                       when(df.PerfilEnergia == 'CQ5', df.convM0 + lit(15)) \
                       .otherwise(df.iM0))
    df = df.withColumn("iM1", \
                       when(df.PerfilEnergia == 'CQ5', df.convM1 + lit(15)) \
                       .otherwise(df.iM1))
    df = df.withColumn("iM2", \
                       when(df.PerfilEnergia == 'CQ5', df.convM2 + lit(15)) \
                       .otherwise(df.iM2))
    df = df.withColumn("iM3", \
                       when(df.PerfilEnergia == 'CQ5', df.convM3 + lit(15)) \
                       .otherwise(df.iM3))
    df = df.withColumn("iA0", \
                       when(df.PerfilEnergia == 'CQ5', df.convA0 + lit(15)) \
                       .otherwise(df.iA0))
    df = df.withColumn("iA1", \
                       when(df.PerfilEnergia == 'CQ5', df.convA1 + lit(15)) \
                       .otherwise(df.iA1))
    df = df.withColumn("iA2", \
                       when(df.PerfilEnergia == 'CQ5', df.convA2 + lit(15)) \
                       .otherwise(df.iA2))
    df = df.withColumn("iA3", \
                       when(df.PerfilEnergia == 'CQ5', df.convA3 + lit(15)) \
                       .otherwise(df.iA3))
    df = df.withColumn("iA4", \
                       when(df.PerfilEnergia == 'CQ5', df.convA4 + lit(15)) \
                       .otherwise(df.iA4))
    df = df.withColumn("CURVA_PLD",
                       when(((df.PerfilEnergia == lit("Convencional")) | (df.PerfilEnergia == lit("Indefinido")) | (
                               df.PerfilEnergia == lit("I0"))) & (
                                    ((year(df.dtRef) - year(df.dtBalanco)) * lit(12)) + (
                                    month(df.dtRef) - month(df.dtBalanco)) <= lit(0)), df.convM0)
                       .when(((df.PerfilEnergia == lit("Convencional")) | (
                               df.PerfilEnergia == lit("Indefinido")) | (df.PerfilEnergia == lit("I0"))) & (
                                     ((year(df.dtRef) - year(df.dtBalanco)) * lit(12)) + (
                                     month(df.dtRef) - month(df.dtBalanco)) == lit(1)), df.convM1)
                       .when(((df.PerfilEnergia == lit("Convencional")) | (
                               df.PerfilEnergia == lit("Indefinido")) | (df.PerfilEnergia == lit("I0"))) & (
                                     ((year(df.dtRef) - year(df.dtBalanco)) * lit(12)) + (
                                     month(df.dtRef) - month(df.dtBalanco)) == lit(2)), df.convM2)
                       .when(((df.PerfilEnergia == lit("Convencional")) | (
                               df.PerfilEnergia == lit("Indefinido")) | (df.PerfilEnergia == lit("I0"))) & (
                                     ((year(df.dtRef) - year(df.dtBalanco)) * lit(12)) + (
                                     month(df.dtRef) - month(df.dtBalanco)) == lit(3)), df.convM3)
                       .when(((df.PerfilEnergia == lit("Convencional")) | (
                               df.PerfilEnergia == lit("Indefinido")) | (df.PerfilEnergia == lit("I0"))) & (
                                     ((year(df.dtRef) - year(df.dtBalanco)) * lit(12)) + (
                                     month(df.dtRef) - month(df.dtBalanco)) > lit(3)) & (
                                     (year(df.dtRef) - year(df.dtBalanco)) == lit(0)), df.convA0)
                       .when(((df.PerfilEnergia == lit("Convencional")) | (
                               df.PerfilEnergia == lit("Indefinido")) | (df.PerfilEnergia == lit("I0"))) & (
                                     ((year(df.dtRef) - year(df.dtBalanco)) * lit(12)) + (
                                     month(df.dtRef) - month(df.dtBalanco)) > lit(3)) & (
                                     (year(df.dtRef) - year(df.dtBalanco)) == lit(1)) & (
                                     month(df.dtBalanco) < lit(9)), df.convA1)
                       .when(((df.PerfilEnergia == lit("Convencional")) | (
                               df.PerfilEnergia == lit("Indefinido")) | (df.PerfilEnergia == lit("I0"))) & (
                                     ((year(df.dtRef) - year(df.dtBalanco)) * lit(12)) + (
                                     month(df.dtRef) - month(df.dtBalanco)) > lit(3)) & (
                                     (year(df.dtRef) - year(df.dtBalanco)) == lit(1)) & (
                                     month(df.dtBalanco) >= lit(9)), df.convA0)
                       .when(((df.PerfilEnergia == lit("Convencional")) | (
                               df.PerfilEnergia == lit("Indefinido")) | (df.PerfilEnergia == lit("I0"))) & (
                                     ((year(df.dtRef) - year(df.dtBalanco)) * lit(12)) + (
                                     month(df.dtRef) - month(df.dtBalanco)) > lit(3)) & (
                                     (year(df.dtRef) - year(df.dtBalanco)) == lit(2)) & (
                                     month(df.dtBalanco) < lit(9)), df.convA2)
                       .when(((df.PerfilEnergia == lit("Convencional")) | (
                               df.PerfilEnergia == lit("Indefinido")) | (df.PerfilEnergia == lit("I0"))) & (
                                     ((year(df.dtRef) - year(df.dtBalanco)) * lit(12)) + (
                                     month(df.dtRef) - month(df.dtBalanco)) > lit(3)) & (
                                     (year(df.dtRef) - year(df.dtBalanco)) == lit(2)) & (
                                     month(df.dtBalanco) >= lit(9)), df.convA1)
                       .when(((df.PerfilEnergia == lit("Convencional")) | (
                               df.PerfilEnergia == lit("Indefinido")) | (df.PerfilEnergia == lit("I0"))) & (
                                     ((year(df.dtRef) - year(df.dtBalanco)) * lit(12)) + (
                                     month(df.dtRef) - month(df.dtBalanco)) > lit(3)) & (
                                     (year(df.dtRef) - year(df.dtBalanco)) == lit(3)) & (
                                     month(df.dtBalanco) < lit(9)), df.convA3)
                       .when(((df.PerfilEnergia == lit("Convencional")) | (
                               df.PerfilEnergia == lit("Indefinido")) | (df.PerfilEnergia == lit("I0"))) & (
                                     ((year(df.dtRef) - year(df.dtBalanco)) * lit(12)) + (
                                     month(df.dtRef) - month(df.dtBalanco)) > lit(3)) & (
                                     (year(df.dtRef) - year(df.dtBalanco)) == lit(3)) & (
                                     month(df.dtBalanco) >= lit(9)), df.convA2)
                       .when(((df.PerfilEnergia == lit("Convencional")) | (
                               df.PerfilEnergia == lit("Indefinido")) | (df.PerfilEnergia == lit("I0"))) & (
                                     ((year(df.dtRef) - year(df.dtBalanco)) * lit(12)) + (
                                     month(df.dtRef) - month(df.dtBalanco)) > lit(3)) & (
                                     (year(df.dtRef) - year(df.dtBalanco)) == lit(4)) & (
                                     month(df.dtBalanco) < lit(9)), df.convA4)
                       .when(((df.PerfilEnergia == lit("Convencional")) | (
                               df.PerfilEnergia == lit("Indefinido")) | (df.PerfilEnergia == lit("I0"))) & (
                                     ((year(df.dtRef) - year(df.dtBalanco)) * lit(12)) + (
                                     month(df.dtRef) - month(df.dtBalanco)) > lit(3)) & (
                                     (year(df.dtRef) - year(df.dtBalanco)) == lit(4)) & (
                                     month(df.dtBalanco) >= lit(9)), df.convA3)
                       .otherwise(df.convA4))
    df = df.withColumn("CURVA_PLD",
                       when((((year(df.dtRef) - year(df.dtBalanco)) * lit(12)) + (
                               month(df.dtRef) - month(df.dtBalanco)) <= lit(3)) & (
                                    ((year(df.dtRef) - year(df.dtBalanco)) * lit(12)) + (
                                    month(df.dtRef) - month(df.dtBalanco)) <= lit(0)), df.iM0)
                       .when((((year(df.dtRef) - year(df.dtBalanco)) * lit(12)) + (
                               month(df.dtRef) - month(df.dtBalanco)) <= lit(3)) & (
                                     ((year(df.dtRef) - year(df.dtBalanco)) * lit(12)) + (
                                     month(df.dtRef) - month(df.dtBalanco)) == lit(1)), df.iM1)
                       .when((((year(df.dtRef) - year(df.dtBalanco)) * lit(12)) + (
                               month(df.dtRef) - month(df.dtBalanco)) <= lit(3)) & (
                                     ((year(df.dtRef) - year(df.dtBalanco)) * lit(12)) + (
                                     month(df.dtRef) - month(df.dtBalanco)) == lit(2)), df.iM2)
                       .when((((year(df.dtRef) - year(df.dtBalanco)) * lit(12)) + (
                               month(df.dtRef) - month(df.dtBalanco)) <= lit(3)) & (
                                     ((year(df.dtRef) - year(df.dtBalanco)) * lit(12)) + (
                                     month(df.dtRef) - month(df.dtBalanco)) == lit(3)), df.iM3)
                       .when(((((year(df.dtRef) - year(df.dtBalanco)) * lit(12)) + (
                               month(df.dtRef) - month(df.dtBalanco)) > lit(3)) & (
                                      (year(df.dtRef) - year(df.dtBalanco)) == lit(1)) & (
                                      month(df.dtRef) < lit(9))), df.iA1)
                       .when(((((year(df.dtRef) - year(df.dtBalanco)) * lit(12)) + (
                               month(df.dtRef) - month(df.dtBalanco)) > lit(3)) & (
                                      (year(df.dtRef) - year(df.dtBalanco)) == lit(1)) & (
                                      month(df.dtRef) >= lit(9))), df.iA0)
                       .when(((((year(df.dtRef) - year(df.dtBalanco)) * lit(12)) + (
                               month(df.dtRef) - month(df.dtBalanco)) > lit(3)) & (
                                      (year(df.dtRef) - year(df.dtBalanco)) == lit(2)) & (
                                      month(df.dtRef) < lit(9))), df.iA2)
                       .when(((((year(df.dtRef) - year(df.dtBalanco)) * lit(12)) + (
                               month(df.dtRef) - month(df.dtBalanco)) > lit(3)) & (
                                      (year(df.dtRef) - year(df.dtBalanco)) == lit(2)) & (
                                      month(df.dtRef) >= lit(9))), df.iA1)
                       .when(((((year(df.dtRef) - year(df.dtBalanco)) * lit(12)) + (
                               month(df.dtRef) - month(df.dtBalanco)) > lit(3)) & (
                                      (year(df.dtRef) - year(df.dtBalanco)) == lit(3)) & (
                                      month(df.dtRef) < lit(9))), df.iA3)
                       .when(((((year(df.dtRef) - year(df.dtBalanco)) * lit(12)) + (
                               month(df.dtRef) - month(df.dtBalanco)) > lit(3)) & (
                                      (year(df.dtRef) - year(df.dtBalanco)) == lit(3)) & (
                                      month(df.dtRef) >= lit(9))), df.iA2)
                       .when(((((year(df.dtRef) - year(df.dtBalanco)) * lit(12)) + (
                               month(df.dtRef) - month(df.dtBalanco)) > lit(3)) & (
                                      (year(df.dtRef) - year(df.dtBalanco)) == lit(4)) & (
                                      month(df.dtRef) < lit(9))), df.iA4)
                       .when(((((year(df.dtRef) - year(df.dtBalanco)) * lit(12)) + (
                               month(df.dtRef) - month(df.dtBalanco)) > lit(3)) & (
                                      (year(df.dtRef) - year(df.dtBalanco)) == lit(4)) & (
                                      month(df.dtRef) >= lit(9))), df.iA3)
                       .when(((((year(df.dtRef) - year(df.dtBalanco)) * lit(12)) + (
                               month(df.dtRef) - month(df.dtBalanco)) > lit(3)) & (
                                      (year(df.dtRef) - year(df.dtBalanco)) > lit(4))), df.iA4)
                       .otherwise(df.CURVA_PLD))

    df2 = df.drop(df.PerfilEnergia) \
        .drop(df.dtRef) \
        .drop(df.dtBalanco) \
        .drop(df.convM0) \
        .drop(df.convM1) \
        .drop(df.convM2) \
        .drop(df.convM3) \
        .drop(df.convA0) \
        .drop(df.convA1) \
        .drop(df.convA2) \
        .drop(df.convA3) \
        .drop(df.convA4) \
        .drop(df.iM0) \
        .drop(df.iM1) \
        .drop(df.iM2) \
        .drop(df.iM3) \
        .drop(df.iA0) \
        .drop(df.iA1) \
        .drop(df.iA2) \
        .drop(df.iA3) \
        .drop(df.iA4)
    return df2


def fc_Preco(df, pDtRef, pDtAtrasoReajuste, pMesReajuste, pDiaReajuste, pDataBase, pPreco, pPeso, serie,
             valorIndiceInicio, valorIndiceFim):
    df = df.withColumn("pDtRef", to_date(df.DtRef)) \
        .withColumn("pDtAtrasoReajuste", to_date(df.AtrasoReajuste)) \
        .withColumn("pMesReajuste", df.MesAutoAjustado) \
        .withColumn("pDiaReajuste", df.DiaAutoAjustado) \
        .withColumn("pDataBase", to_date(df.DiaBase)) \
        .withColumn("pPreco", df.PRECO.cast("double")) \
        .withColumn("pPeso", df.Peso.cast("double")) \
        .withColumn("serie", df.Id.cast("integer")) \
        .withColumn("valorIndiceInicio", df.IniValor.cast("double")) \
        .withColumn("valorIndiceFim", df.FimValor.cast("double")) \
        .withColumn("precoIntermediario", lit(0.0))
    df = df.withColumn("pDtRef", \
                       when((df.pDtRef >= df.pDtAtrasoReajuste) & (month(df.pDtRef) == df.pMesReajuste) & (
                               dayofmonth(df.pDtRef) == df.pDiaReajuste), df.pDtRef) \
                       .when((df.pDtRef >= df.pDtAtrasoReajuste) & (month(df.pDtRef) < df.pMesReajuste), to_date(
                           concat(year(df.pDtRef) - 1, lit("-"), df.pMesReajuste, lit("-"), df.pDiaReajuste),
                           "yyyy-mm-dd")) \
                       .when((df.pDtRef >= df.pDtAtrasoReajuste) & (month(df.pDtRef) != df.pMesReajuste),
                             to_date(concat(year(df.pDtRef), lit("-"), df.pMesReajuste, lit("-"), df.pDiaReajuste),
                                     "yyyy-mm-dd")) \
                       .otherwise(df.pDtRef))
    df = df.withColumn("precoIntermediario",
                       when(year(df.pDtRef) <= year(df.pDataBase), df.pPreco * df.pPeso)
                       .when((df.serie == lit(6)) | (df.serie == lit(5)),
                             round(df.pPreco * (df.valorIndiceInicio / df.valorIndiceFim), 3) * df.pPeso)
                       .when((df.serie == lit(19)), df.pPreco)
                       .when((df.serie.isNull()) & (df.pDiaReajuste.isNull()),
                             df.pPreco * coalesce(df.pPeso, lit(1)))
                       .otherwise(df.precoIntermediario))
    df2 = df.drop(df.pDtRef) \
        .drop(df.pDtAtrasoReajuste) \
        .drop(df.pMesReajuste) \
        .drop(df.pDiaReajuste) \
        .drop(df.pDataBase) \
        .drop(df.pPreco) \
        .drop(df.pPeso) \
        .drop(df.serie) \
        .drop(df.valorIndiceInicio) \
        .drop(df.valorIndiceFim)
    return df2


# cria UDF

fc_Fonte_Preco_UDF = udf(lambda dtRef, dtBalanco: fc_Fonte_Preco(dtRef, dtBalanco), StringType())

# fc_Ano_Data_UDF = udf(lambda dtRef: [ datetime.datetime.strptime(str(dtRef), '%Y-%m-%d').year], StringType())

# fc_Mes_Data_UDF = udf(lambda dtRef: [ datetime.datetime.strptime(str(dtRef), '%Y-%m-%d').month], StringType())

# c_Fim_Mes_UDF = udf(lambda dtRef: [ str((datetime.datetime.strptime(str(dtRef), '%Y-%m-%d') + relativedelta(months=+1)) - relativedelta(days=+1))], StringType())

fc_Range_Dat_UDF = udf(
    lambda dtInicio, x: [datetime.datetime.strptime(dtInicio, '%Y-%m-%d') + relativedelta(months=int(y)) for y in
                         range(0, int(x))], ArrayType(DateType()))

# aux Join

auxJoinContratoAjuste = spark.sql("SELECT CC.IdAditivo, \
                                              CC.Id, \
                                              CC.IDCARACTERISTICAPAI, \
                                              CC.IDTIPO, \
                                              CA.IdCaracteristica, \
                                              CA.DiaAutoAjustado, \
                                              CA.MesAutoAjustado, \
                                              CA.InicioValidade, \
                                              CA.AtrasoReajuste, \
                                              MA.IDCARACTERISTICAAJUSTE, \
                                              MA.IDSERIE, \
                                              MA.DiaBase, \
                                              MA.Peso \
                              FROM MetodologiasAjuste AS MA \
                              RIGHT JOIN CaracteristicasContrato AS CC  ON (MA.IDCARACTERISTICAAJUSTE = CC.Id) \
                              RIGHT JOIN CaracteristicasAjuste AS CA ON (CA.IdCaracteristica = CC.Id)")

auxJoinContratoAjuste.createOrReplaceTempView("AUX_JOIN_CONTRATO_AJUSTE")

auxJoinEntidadesAtivaveis = spark.sql("SELECT IdEntidade, \
                                                  DataDesativacao \
                                            FROM EntidadesAtivaveis \
                                            WHERE DataDesativacao IS NOT NULL")

auxJoinEntidadesAtivaveis.createOrReplaceTempView("AUX_JOIN_ENTIDADES_ATIVAVEIS")

# cria Funcoes de Join

fcSerieReajusteAux = spark.sql("SELECT  CONCAT(CONCAT(CONCAT(S.Descricao,'('),CC.Peso),')') as descricao,A.ID  \
                                    FROM contratos AS C \
                                    LEFT JOIN Aditivos AS A ON (C.ID = A.IdContrato) \
                                    LEFT OUTER JOIN AUX_JOIN_CONTRATO_AJUSTE AS CC ON (A.Id = CC.IdAditivo) \
                                    LEFT JOIN SERIES S ON (CC.IDSERIE = S.ID) \
                                    WHERE CC.IDTIPO = 6 \
                                    GROUP BY S.Descricao,\
                                             CC.Peso,A.ID")

fcQtdPartesContratuais = spark.sql("SELECT  COUNT(*) as QtdPartes, \
                                                IdAditivo, \
                                                IdCaracteristicaPai  \
                                                FROM CaracteristicasContrato \
                                                WHERE IdTipo IN " + tpCaracContrPartesContratuais + " \
                                                  GROUP BY IdAditivo,IdCaracteristicaPai")

fcPrecoFatura = spark.sql("SELECT EF.PRECO, EEEF.IdEntradaEnergiaEntregue \
                                FROM Faturas F \
                                LEFT JOIN EnergiaFaturas EF ON (F.ID = EF.IDFATURA) \
                                LEFT JOIN EntradasEEEnergiaFaturas EEEF ON (EF.Id = EEEF.IdEnergiaFatura)")

fcPLD = spark.sql("SELECT VALOR, \
                               DATA, \
                               IdSubmercado \
                        FROM PLDMedio1 ")

fcCompradorVendedorStep1 = spark.sql("SELECT E.NOMEFANTASIA, \
                                                   PC.IDTIPO, \
                                                   C.CodigoReferencia, \
                                                   CASE \
                                                       WHEN PC.IDTIPO = 2 THEN 'V' \
                                                       WHEN PC.IDTIPO = 1 THEN 'C' \
                                                       WHEN PC.IDTIPO = 16 THEN 'VS' \
                                                       WHEN PC.IDTIPO = 8 THEN 'CS' \
                                                       ELSE NULL \
                                                   END AS TIPO \
                                            FROM PartesContratuais AS PC \
                                            INNER JOIN Aditivos AS A ON PC.IdAditivo = A.Id \
                                            INNER JOIN Contratos AS C ON A.IdContrato = C.Id \
                                            LEFT JOIN EMPRESAS E ON (PC.IDEMPRESA = E.ID)")

fcPldMaxStep1 = spark.sql("SELECT MAX(DATA) AS MAX_DATA\
                                    FROM PLDMedio")

fcPldMaxStep1.createOrReplaceTempView("FC_GET_PLD_MAX_STEP1")

fcPldMax = spark.sql("SELECT VALOR,A.IdSubmercado \
                            FROM PLDMedio AS A \
                            JOIN FC_GET_PLD_MAX_STEP1 AS B ON SUBSTR(A.DATA,0,10) =  SUBSTR(B.MAX_DATA,0,10)")

ufEmpresa = spark.sql("SELECT EE.IDEMPRESA, \
                                   UF.SIGLA \
                            FROM ENDERECOSEMPRESAS EE \
                            JOIN ENDERECOS ED ON (ED.ID = EE.IDENDERECO) \
                            JOIN MUNICIPIOS M ON (ED.CODIGOIBGEMUNICIPIO = M.CODIGOIBGE) \
                            JOIN ESTADOS UF ON (M.CODIGOIBGEESTADO = UF.CODIGOIBGE) ")

ufEmpresa.createOrReplaceTempView("UF_EMPRESA")

ajustesAditivo = spark.sql("SELECT CC.IdAditivo, \
                                       S.Id, \
                                       CC.DiaBase, \
                                       CC.Peso,  \
                                       NVL(CC.AtrasoReajuste,DiaBase) AS AtrasoReajuste, \
                                       CC.DiaAutoAjustado, \
                                       CC.MesAutoAjustado, \
                                       CC.InicioValidade, \
                                       CC.IDTIPO \
                                    FROM AUX_JOIN_CONTRATO_AJUSTE AS CC \
                                    LEFT JOIN SERIES S ON (CC.IDSERIE = S.ID)")

ajustesAditivo.createOrReplaceTempView("Ajustes_Aditivo")

fcSerieReajuste = (fcSerieReajusteAux
                   .groupby("ID")
                   .agg(collect_list("descricao")))

fcSerieReajuste = fcSerieReajuste.withColumnRenamed("collect_list(Descricao)", "Descricao")

fcCompradorVendedor = (fcCompradorVendedorStep1
                       .groupby("CodigoReferencia", "IDTIPO", "TIPO")
                       .agg(collect_list("NOMEFANTASIA")))

fcCompradorVendedor = fcCompradorVendedor.withColumnRenamed("collect_list(NOMEFANTASIA)", "NOMEFANTASIA")

# Funcoes complexas

# FC_GET_NATUREZA

fcNaturezaStep1 = spark.sql("SELECT COUNT(*) AS Count, PC.IDADITIVO \
                                    FROM PartesContratuais AS PC \
                                    WHERE PC.IDTIPO IN " + tipoFcNaturezaStep1 + " \
                                    GROUP BY PC.IDADITIVO")

fcNaturezaStep1.createOrReplaceTempView("FC_GET_NATUREZA_STEP1")

fcNaturezaStep2 = spark.sql("SELECT COUNT(*) AS Count,PC.IDADITIVO, 'IGUAL' AS TIPO \
                                    FROM PartesContratuais AS PC \
                                    WHERE PC.IDADITIVO NOT IN (SELECT IDADITIVO FROM FC_GET_NATUREZA_STEP1 WHERE Count = 0) \
                                      AND PC.IDEMPRESA = 3 \
                                      GROUP BY  PC.IDADITIVO \
                                    UNION ALL \
                                    SELECT PC.IDTIPO,PC.IDADITIVO, 'DIFERENTE' AS TIPO \
                                    FROM PartesContratuais AS PC \
                                    WHERE PC.IDADITIVO IN(SELECT IDADITIVO FROM FC_GET_NATUREZA_STEP1 WHERE Count <> 0) \
                                      AND PC.IDEMPRESA = 3 ")

fcNatureza = fc_Natureza(fcNaturezaStep2, fcNaturezaStep2.Count, fcNaturezaStep2.TIPO)

fcPrecoComissaoPreco = spark.sql("SELECT int(A.ID), \
                                               CCENER.Id as Id_CCENER, \
                                               DPC.InicioIntervalo, \
                                               A.InicioVigenciaContrato, \
                                               DPC.FimIntervalo, \
                                               A.FimVigenciaContrato, \
                                               NVL(ParcelaFixa,0) AS ParcelaFixa , \
                                               NVL(ParcelaFixaEnergia,0) AS ParcelaFixaEnergia, \
                                               NVL(CASE WHEN TO_DATE(SUBSTR(DPC.InicioIntervalo,0,10), 'yyyy-MM-dd') < TO_DATE(SUBSTR(A.InicioVigenciaContrato,0,10), 'yyyy-MM-dd') THEN TO_DATE(SUBSTR(A.InicioVigenciaContrato,0,10), 'yyyy-MM-dd') ELSE TO_DATE(SUBSTR(DPC.InicioIntervalo,0,10), 'yyyy-MM-dd') END, TO_DATE(SUBSTR(A.InicioVigenciaContrato,0,10), 'yyyy-MM-dd')) AS DT_INICIO, \
                                               NVL(TO_DATE(SUBSTR(DPC.FimIntervalo,0,10), 'yyyy-MM-dd'), TO_DATE(SUBSTR(A.FimVigenciaContrato,0,10), 'yyyy-MM-dd')) AS DT_FIM \
                                        FROM contratos AS C \
                                        LEFT JOIN Aditivos AS A ON (C.ID = A.IdContrato) \
                                        LEFT OUTER JOIN CaracteristicasContrato AS CCENER ON A.Id = CCENER.IdAditivo \
                                        LEFT JOIN CaracteristicasComissao AS CC ON (CCENER.Id = CC.IdCaracteristica) \
                                        LEFT JOIN DadosPrecoComissao AS DPC ON (CC.IdCaracteristica = DPC.IdCaracteristicaPreco) \
                                        WHERE (CCENER.IdTipo IN " + idTipoFcPrecoComissaoPreco + ")")

fcPreco = spark.sql("SELECT A.ID as ID_CONTRATO, \
                                PRECO, \
                                NVL(DPF.InicioIntervalo, A.InicioVigenciaContrato) as DataInicioVigencia , \
                                NVL(DPF.FimIntervalo, A.FimVigenciaContrato) as DataFimVigencia, \
                                PLD.CoeficientePLD, \
                                PLD.PrecoBase \
                            FROM   \
                                contratos AS C LEFT JOIN \
                                Aditivos AS A ON (C.ID = A.IdContrato) LEFT OUTER JOIN \
                                CaracteristicasContrato AS CCENER ON A.Id = CCENER.IdAditivo LEFT JOIN \
                                CaracteristicasPreco AS CPR ON (CCENER.Id = CPR.IdCaracteristica) LEFT JOIN \
                                CaracteristicaPrecoFixo AS CPRF ON (CPR.IdCaracteristica = CPRF.IdCaracteristicaPreco) LEFT JOIN \
                                DadosPrecoFixo AS DPF ON (CPRF.IdCaracteristicaPreco = DPF.IdCaracteristicaPreco)  LEFT JOIN \
                                DadosPrecoFuncaoPLD PLD ON (CPR.IDCARACTERISTICA = PLD.IDCARACTERISTICAPRECO) \
                            WHERE (CCENER.IdTipo IN (3,5)) AND (SUFIXO IS NULL OR CPR.DESCRICAO = 'Preço')")

fcPrecoComissaoCursor = spark.sql("SELECT DISTINCT S.Id, \
                                                        DiaBase, \
                                                        DiaAutoAjustado, \
                                                        MesAutoAjustado, \
                                                        Peso, \
                                                        NVL(AtrasoReajuste,DiaBase) AS AtrasoReajuste, \
                                                        int(A.ID) AS ID_ADITIVO, \
                                                        CC.IDCARACTERISTICAPAI, \
                                                        CC.INICIOVALIDADE, \
                                                        A.InicioVigenciaContrato, \
                                                        B.InicioValidade AS DT_REF_INI, \
                                                        TO_DATE(SUBSTR(A.FimVigenciaContrato,0,10), 'yyyy-MM-dd') AS FimVigenciaContrato \
                                        FROM contratos AS C \
                                        LEFT JOIN Aditivos AS A ON (C.ID = A.IdContrato) \
                                        LEFT OUTER JOIN AUX_JOIN_CONTRATO_AJUSTE AS CC ON A.Id = CC.IdAditivo \
                                        LEFT JOIN SERIES S ON (CC.IDSERIE = S.ID) \
                                        LEFT JOIN (SELECT MIN(CA1.InicioValidade) AS InicioValidade,CC1.IdCaracteristicaPai \
                                                    FROM CaracteristicasContrato AS CC1 \
                                                    LEFT JOIN CaracteristicasAjuste AS CA1 ON CA1.IdCaracteristica = CC1.Id \
                                                    LEFT JOIN MetodologiasAjuste AS MA1 ON CC1.ID = MA1.IDCARACTERISTICAAJUSTE \
                                                    GROUP BY CC1.IdCaracteristicaPai) AS B ON B.IdCaracteristicaPai = cc.IdCaracteristicaPai AND B.InicioValidade > NVL(CC.INICIOVALIDADE,A.InicioVigenciaContrato) \
                                        WHERE CC.IDTIPO =" + idTipoFcPrecoComissaoCursor)

fcRecorrenciaReajuste = spark.sql("SELECT CONCAT(CONCAT(CAST(DiaAutoAjustado AS STRING), '/') ,CAST(MesAutoAjustado AS STRING)) as RECORRENTE, A.ID \
                                        FROM  \
                                        contratos AS C LEFT JOIN\
                                        Aditivos AS A ON (C.ID = A.IdContrato) LEFT OUTER JOIN\
                                        CaracteristicasContrato AS CC ON A.Id = CC.IdAditivo LEFT JOIN\
                                        CaracteristicasAjuste AS CA ON (CA.IdCaracteristica = CC.Id) LEFT JOIN\
                                        MetodologiasAjuste AS MA ON (CC.ID = MA.IDCARACTERISTICAAJUSTE) LEFT JOIN\
                                        SERIES S ON (MA.IDSERIE = S.ID) \
                                        WHERE  CC.IDTIPO = 6 and MesAutoAjustado is not null \
                                        GROUP BY  DiaAutoAjustado,MesAutoAjustado, A.ID ")

fcDiabaseReajuste = spark.sql("SELECT substring(DiaBase,0,10) as DIABASE, A.ID  \
                                    FROM   \
                                    contratos AS C LEFT JOIN \
                                    Aditivos AS A ON (C.ID = A.IdContrato) LEFT OUTER JOIN \
                                    CaracteristicasContrato AS CC ON A.Id = CC.IdAditivo LEFT JOIN \
                                    CaracteristicasAjuste AS CA ON (CA.IdCaracteristica = CC.Id) LEFT JOIN \
                                    MetodologiasAjuste AS MA ON (CC.ID = MA.IDCARACTERISTICAAJUSTE) LEFT JOIN \
                                    SERIES S ON (MA.IDSERIE = S.ID)  \
                                    WHERE  CC.IDTIPO = 6	  \
                                    GROUP BY  DiaBase,A.ID")

# cria FC_GET_NATUREZA
fcNatureza.createOrReplaceTempView("FC_GET_NATUREZA")

# cria FC_GET_SERIE_REAJUSTE
fcSerieReajuste.createOrReplaceTempView("FC_GET_SERIE_REAJUSTE")

# cria FC_GET_QTD_PARTES_CONTRATUAIS
fcQtdPartesContratuais.createOrReplaceTempView("FC_GET_QTD_PARTES_CONTRATUAIS")

# cria FC_GET_PRECO_FATURA
fcPrecoFatura.createOrReplaceTempView("FC_GET_PRECO_FATURA")

# cria FC_GET_PLD
fcPLD.createOrReplaceTempView("FC_GET_PLD")

# cria FC_GET_COMPRADOR_VENDEDOR
fcCompradorVendedor.createOrReplaceTempView("FC_GET_COMPRADOR_VENDEDOR")

# cria FC_GET_PLD_MAX
fcPldMax.createOrReplaceTempView("FC_GET_PLD_MAX")

# cria FC_GET_PRECO_COMISSAO_PRECO
fcPrecoComissaoPreco.createOrReplaceTempView("FC_GET_PRECO_COMISSAO_PRECO")

# cria FC_GET_PRECO
fcPreco.createOrReplaceTempView("FC_GET_PRECO")

# cria FC_GET_PRECO_COMISSAO_CURSOR
fcPrecoComissaoCursor.createOrReplaceTempView("FC_GET_PRECO_COMISSAO_CURSOR")

# cria FC_GET_RECORRENCIA_REAJUSTE
fcRecorrenciaReajuste.createOrReplaceTempView("FC_GET_RECORRENCIA_REAJUSTE")

# cria FC_GET_DIABASE_REAJUSTE
fcDiabaseReajuste.createOrReplaceTempView("FC_GET_DIABASE_REAJUSTE")

# cria VW_SERIE_IPCA
vwIpca = spark.sql("SELECT TO_DATE(SUBSTR(DATAVIGENCIA,0,10),'yyyy-MM-dd') as DataVigencia, \
                               IdSerie, \
                               Valor \
                        FROM \
                          ( SELECT DataVigencia, \
                                   IdSerie, \
                                   Valor \
                           FROM IPCA \
                           UNION ALL SELECT DataVigencia, \
                                            IdSerie, \
                                            Valor \
                           FROM IPCA_PROJETADO \
                           WHERE TO_DATE(SUBSTR(DATAVIGENCIA,0,10),'yyyy-MM-dd') > \
                               (SELECT MAX(TO_DATE(SUBSTR(DATAVIGENCIA,0,10),'yyyy-MM-dd')) \
                                FROM IPCA) ) AS SERIE_IPCA")

vwIpca.createOrReplaceTempView("VW_SERIE_IPCA")

# cria VW_ISENTOS_PIS_COFINS
vwIsentosPisConfim = spark.sql("SELECT A.Id AS Id, \
                                        CT.Nome \
                                    FROM CategoriasContratos AS CTC \
                                    INNER JOIN Categorias AS CT ON CTC.IdCategoria = CT.Id \
                                    INNER JOIN Contratos AS C ON C.Id = CTC.IdContrato \
                                    LEFT OUTER JOIN Aditivos AS A ON C.Id = A.IdContrato \
                                    WHERE CT.id = " + idCategIsentosPisConfim + " \
                                    GROUP BY A.Id , \
                                    CT.Nome ")

vwIsentosPisConfim.createOrReplaceTempView("VW_ISENTOS_PIS_COFINS")

# cria VW_09_NUM_NOTA_FISCAL
vw09NumNotaFiscal = spark.sql("SELECT SI.IdArmazenadorCamposSI, \
                                           MAX(CASE WHEN CSI.IdTipoCampo = 5 THEN CSI.Valor END) AS NumNotaFiscal, \
                                           MAX(CASE WHEN CSI.IdTipoCampo = 24 THEN CSI.Valor END) AS NumDocumento \
                                    FROM CamposReaisSI SI \
                                    LEFT JOIN CamposSI CSI ON (SI.Id = CSI.Id) \
                                    WHERE CSI.IdTipoCampo IN " + idTipoCampovw09NumNotaFiscal + " \
                                    GROUP BY SI.IdArmazenadorCamposSI")

vw09NumNotaFiscal.createOrReplaceTempView("VW_09_NUM_NOTA_FISCAL")

# cria VW_10_GENE_FATURAS

vw10GeneFaturas = spark.sql("SELECT F.Id AS IdFatura, \
                                           F.IdContrato, \
                                           F.IdAditivo, \
                                           F.IdEmpresaEmitente AS IdEmpresaOrigem, \
                                           F.IdEmpresa AS IdEmpresaDestino, \
                                           F.MesReferencia, \
                                           F.IdNatureza, \
                                           F.Flags, \
                                           F.DataVencimento, \
                                           F.DataApresentacao, \
                                           F.IdFonteNativa, \
                                           ef.DescricaoEnergia, \
                                           EF.Energia, \
                                           EF.Preco, \
                                           EF.IdTipoEnergiaFatura, \
                                           EEEF.IdEntradaEnergiaEntregue, \
                                           EEEF.IdEnergiaFatura, \
                                           EEEF.IdTipoVinculo, \
                                           EEEF.EstaAtivo, \
                                           NF.NumNotaFiscal, \
                                           NF.NumDocumento \
                                    FROM EntradasEEEnergiaFaturas EEEF \
                                    LEFT JOIN EnergiaFaturas EF ON (EEEF.IdEnergiaFatura = EF.Id) \
                                    LEFT JOIN Faturas F ON (F.ID = EF.IDFATURA) \
                                    LEFT JOIN VW_09_NUM_NOTA_FISCAL NF ON (F.IdArmazenadorCamposSI = NF.IdArmazenadorCamposSI) \
                                    WHERE Flags = " + FlagsVw10GeneFaturas + " \
                                      AND EEEF.IdEntradaEnergiaEntregue IS NOT NULL \
                                      AND EF.MesReferencia >= " + mesReferenciaVw10GeneFaturas)

vw10GeneFaturas.createOrReplaceTempView("VW_10_GENE_FATURAS")

vwGeneFlexibilidade = spark.sql("SELECT A.IdContrato, \
                                            CCFLEX.IdAditivo, \
                                            CF.Periodicidade, \
                                            CF.Descricao, \
                                            CF.DiaLimite, \
                                            DF.FatorMaximo AS FLEX_MAX, \
                                            DF.FatorMinimo AS FLEX_MIN, \
                                            NVL(DF.InicioIntervalo, A.InicioVigenciaContrato) AS InicioIntervalo, \
                                            NVL(DF.FimIntervalo, A.FimVigenciaContrato) AS FimIntervalo \
                                     FROM Aditivos AS A \
                                     LEFT OUTER JOIN CaracteristicasContrato AS CCFLEX ON A.Id = CCFLEX.IdAditivo \
                                     LEFT OUTER JOIN CaracteristicasFlexibilidade AS CF ON CCFLEX.Id = CF.IdCaracteristica \
                                     LEFT OUTER JOIN DadosFlexibilidades AS DF ON CF.IdCaracteristica = DF.IdCaracteristicaFlexibilidade \
                                     WHERE (CCFLEX.IdTipo IN (1)) \
                                       AND (CF.Periodicidade = 12) \
                                       AND (NVL(CF.Sufixo, '') <> 'MOD') \
                                       AND (A.FimValidade IS NULL)")

vwGeneFlexibilidade.createOrReplaceTempView("VW_GENE_FLEXIBILIDADE")

vwContatosEol = spark.sql("SELECT A.Id as Id, CT.Nome \
                                FROM             \
                                CategoriasContratos AS CTC INNER JOIN \
                                Categorias AS CT ON CTC.IdCategoria = CT.Id INNER JOIN \
                                Contratos AS C ON C.Id = CTC.IdContrato LEFT OUTER JOIN \
                                Aditivos AS A ON C.Id = A.IdContrato \
                                WHERE (CT.id = 135) \
                                GROUP BY  A.Id , CT.Nome")

vwContatosEol.createOrReplaceTempView("VW_CONTATOS_EOL")

# cria VW_GENE_CONTRATOS_NEW
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

# cria VW_GENE_ENERGIA_CONTRATADA_STEP1
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
                                            AND CC.IdTipo = " + idTipoVwEnergiaContratadaStep1 + " \
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
                                            WHERE (CC.IdTipo IN " + idTipoVwEnergiaContratadaStep2Aux + ") AND \
                                                    S.IDADITIVO IS NOT NULL")

vwEnergiaContratadaStep2Rang = vwEnergiaContratadaStep2Aux.withColumn("records", fc_Range_Dat_UDF(
    vwEnergiaContratadaStep2Aux.InicioIntervalo, vwEnergiaContratadaStep2Aux.Qtd_Meses)) \
    .withColumn("DtRef", explode("records")) \
    .drop("records")
vwEnergiaContratadaStep2Rang = fc_Qtd_Horas_Periodo(vwEnergiaContratadaStep2Rang,
                                                    vwEnergiaContratadaStep2Rang.pInicio,
                                                    vwEnergiaContratadaStep2Rang.pFim)

vwEnergiaContratadaStep2Rang = vwEnergiaContratadaStep2Rang.withColumnRenamed("Qtd_horas", "QtdHorasPeriodoIniFim")

vwEnergiaContratadaStep2Rang = fc_Qtd_Horas_Mes(vwEnergiaContratadaStep2Rang, vwEnergiaContratadaStep2Rang.DtRef)

vwEnergiaContratadaStep2Rang = vwEnergiaContratadaStep2Rang.withColumnRenamed("Qtd_horas", "QtdHorasMes")

vwEnergiaContratadaStep2Parts = vwEnergiaContratadaStep2Rang.withColumn("Ano",
                                                                        year(vwEnergiaContratadaStep2Rang.DtRef)) \
    .withColumn("Mes", month(vwEnergiaContratadaStep2Rang.DtRef)) \
    .withColumn("FimMes", last_day(vwEnergiaContratadaStep2Rang.DtRef))

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

vwEnergiaSazonalizadaAux = fc_Qtd_Horas_Periodo(vwEnergiaSazonalizadaAux, vwEnergiaSazonalizadaAux.pInicio,
                                                vwEnergiaSazonalizadaAux.pFim)

vwEnergiaSazonalizadaQtdHoras = vwEnergiaSazonalizadaAux.withColumnRenamed("Qtd_horas", "QtdHorasPeriodoIniFim")

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
                                        FROM VW_GENE_ENERGIA_SAZONALIZADA_HORAS")  # .repartition("ANO")

vwEnergiaSazonalizada.createOrReplaceTempView("VW_GENE_ENERGIA_SAZONALIZADA")

# cria VW_GENE_ENERGIA_ENTREGUE

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
                                       WHERE (IdTipo IN " + iIdTipoVwEnergiaEntregue + ")) AS CC2 ON A.Id = CC2.IdAditivo \
                                    LEFT OUTER JOIN CaracteristicasPreco AS CPR ON CC2.Id = CPR.IdCaracteristica \
                                    LEFT OUTER JOIN DadosPrecoFuncaoPLD AS PLD ON CPR.IdCaracteristica = PLD.IdCaracteristicaPreco \
                                    LEFT JOIN VW_10_GENE_FATURAS AS F ON (EEE.Id = F.IdEntradaEnergiaEntregue)")

vwEnergiaEntregueStep1 = fc_Qtd_Horas_Periodo(vwEnergiaEntregueStep1, vwEnergiaEntregueStep1.pInicio,
                                              vwEnergiaEntregueStep1.pFim)

vwEnergiaEntregueStep2 = vwEnergiaEntregueStep1.withColumnRenamed("Qtd_horas", "QtdHorasPeriodoIniFim")

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

vwBalancoServicosStep2Rang = vwBalancoServicosStep2Aux.withColumn("records", fc_Range_Dat_UDF(
    vwBalancoServicosStep2Aux.DTA_INICIO, vwBalancoServicosStep2Aux.Qtd_Meses)) \
    .withColumn("DtRef", explode("records")) \
    .drop("records")

vwBalancoServicosStep2Rang = fc_Qtd_Horas_Mes(vwBalancoServicosStep2Rang, vwBalancoServicosStep2Rang.DtRef)

vwBalancoServicosStep2Rang = vwBalancoServicosStep2Rang.withColumnRenamed("Qtd_horas", "horas")

vwBalancoServicosStep2Data = vwBalancoServicosStep2Rang.withColumn("INICIO_MES", vwBalancoServicosStep2Rang.DtRef) \
    .withColumn("FIM_MES", last_day(vwBalancoServicosStep2Rang.DtRef))

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

vwBalancoServicosStep2PrecoComStep3Ini.createOrReplaceTempView(
    "VW_GENE_BALANCO_SERVICOS_STEP2_PRECO_COMISSAO_STEP3_INI")

vwBalancoServicosStep2PrecoComStep3Fim = spark.sql("SELECT A.DiaBase, \
                                                   A.DtRef, \
                                                   MAX(DT_FIM.DataVigencia) AS DT_FIM_DataVigencia \
                                            FROM VW_GENE_BALANCO_SERVICOS_STEP2_PRECO_COMISSAO_STEP2 AS A \
                                            LEFT JOIN VW_SERIE_IPCA AS DT_FIM ON TO_DATE(SUBSTR(DT_FIM.DataVigencia,0,10), 'yyyy-MM-dd') < A.DtRef \
                                            GROUP BY A.DiaBase, \
                                                     A.DtRef")

vwBalancoServicosStep2PrecoComStep3Fim.createOrReplaceTempView(
    "VW_GENE_BALANCO_SERVICOS_STEP2_PRECO_COMISSAO_STEP3_FIM")

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

vwBalancoServicosStep2PrecoComStep5 = fc_Preco(vwBalancoServicosStep2PrecoComStep4,
                                               vwBalancoServicosStep2PrecoComStep4.DtRef,
                                               vwBalancoServicosStep2PrecoComStep4.AtrasoReajuste,
                                               vwBalancoServicosStep2PrecoComStep4.MesAutoAjustado,
                                               vwBalancoServicosStep2PrecoComStep4.DiaAutoAjustado,
                                               vwBalancoServicosStep2PrecoComStep4.DiaBase,
                                               vwBalancoServicosStep2PrecoComStep4.PRECO,
                                               vwBalancoServicosStep2PrecoComStep4.Peso,
                                               vwBalancoServicosStep2PrecoComStep4.Id,
                                               vwBalancoServicosStep2PrecoComStep4.IniValor,
                                               vwBalancoServicosStep2PrecoComStep4.FimValor)

vwBalancoServicosStep2PrecoComStep5 = vwBalancoServicosStep2PrecoComStep5.withColumnRenamed("precoIntermediario",
                                                                                            "PrecoComFinal")

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

vwBalancoServicosStep2 = fc_Preco(vwBalancoServicosStep2PrecoStep4, vwBalancoServicosStep2PrecoStep4.DtRef,
                                  vwBalancoServicosStep2PrecoStep4.AtrasoReajuste,
                                  vwBalancoServicosStep2PrecoStep4.MesAutoAjustado,
                                  vwBalancoServicosStep2PrecoStep4.DiaAutoAjustado,
                                  vwBalancoServicosStep2PrecoStep4.DiaBase, vwBalancoServicosStep2PrecoStep4.PRECO,
                                  vwBalancoServicosStep2PrecoStep4.Peso, vwBalancoServicosStep2PrecoStep4.Id,
                                  vwBalancoServicosStep2PrecoStep4.IniValor,
                                  vwBalancoServicosStep2PrecoStep4.FimValor)

vwBalancoServicosStep2 = vwBalancoServicosStep2.withColumnRenamed("precoIntermediario", "PrecoFinal")

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

vwBalancoServicosStep3 = fc_Qtd_Horas_Mes(vwBalancoServicosStep3Aux, vwBalancoServicosStep3Aux.DtRef)

vwBalancoServicosStep3 = vwBalancoServicosStep3.withColumnRenamed("Qtd_horas", "horas")

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
                                      FROM VW_GENE_ENERGIA_CONTRATADA_STEP2")  # .repartition("ANO")

tmpEnergiaContratada.createOrReplaceTempView("TMP_ENERGIA_CONTRATADA")

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

tmpEnergiaContratadaSazoStep6 = fc_Preco(tmpEnergiaContratadaSazoStep5, tmpEnergiaContratadaSazoStep5.DtRef,
                                         tmpEnergiaContratadaSazoStep5.AtrasoReajuste,
                                         tmpEnergiaContratadaSazoStep5.MesAutoAjustado,
                                         tmpEnergiaContratadaSazoStep5.DiaAutoAjustado,
                                         tmpEnergiaContratadaSazoStep5.DiaBase, tmpEnergiaContratadaSazoStep5.PRECO,
                                         tmpEnergiaContratadaSazoStep5.Peso, tmpEnergiaContratadaSazoStep5.Id,
                                         tmpEnergiaContratadaSazoStep5.IniValor,
                                         tmpEnergiaContratadaSazoStep5.FimValor)

tmpEnergiaContratadaSazoStep6 = tmpEnergiaContratadaSazoStep6.withColumnRenamed("precoIntermediario", "ValorPreco")

tmpEnergiaContratadaSazo = tmpEnergiaContratadaSazoStep6

tmpEnergiaContratadaSazo.createOrReplaceTempView("TMP_ENERGIA_CONTRATADA_SAZO")

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

tmpEnergiaEntregueStep1 = fc_Qtd_Horas_Periodo(tmpEnergiaEntregueStep1, tmpEnergiaEntregueStep1.pInicio,
                                               tmpEnergiaEntregueStep1.pFim)

tmpEnergiaEntregueStep2 = tmpEnergiaEntregueStep1.withColumnRenamed("Qtd_horas", "HORAS_PERIODO")

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

tmpEnergiaEntregueStep7 = fc_Preco(tmpEnergiaEntregueStep6, tmpEnergiaEntregueStep6.DtRef,
                                   tmpEnergiaEntregueStep6.AtrasoReajuste, tmpEnergiaEntregueStep6.MesAutoAjustado,
                                   tmpEnergiaEntregueStep6.DiaAutoAjustado, tmpEnergiaEntregueStep6.DiaBase,
                                   tmpEnergiaEntregueStep6.PRECO, tmpEnergiaEntregueStep6.Peso,
                                   tmpEnergiaEntregueStep6.Id, tmpEnergiaEntregueStep6.IniValor,
                                   tmpEnergiaEntregueStep6.FimValor)

tmpEnergiaEntregueStep7 = tmpEnergiaEntregueStep7.withColumnRenamed("precoIntermediario", "ValorPrecoFinal")

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
                                                NVL(P.PRECO, P.CoeficientePLD * NVL(GP.VALOR,GPM.VALOR) + P.PrecoBase) AS PRECO_BASE\
                                         FROM UF_EMPRESA UF \
                                         RIGHT JOIN TMP_ENERGIA_ENTREGUE E ON (E.IdEmpresa= UF.IdEmpresa) \
                                         LEFT JOIN Empresas emp ON (E.IdEmpresa = emp.Id) \
                                         LEFT JOIN FC_GET_PRECO P ON E.IDADITIVO = P.ID_CONTRATO AND E.INICIO_MES BETWEEN P.DataInicioVigencia AND P.DataFimVigencia \
                                         LEFT JOIN FC_GET_PLD_MAX AS GPM ON GPM.IdSubmercado = E.IdSubmercado \
                                         LEFT JOIN FC_GET_PLD AS GP ON GP.IdSubmercado = E.IdSubmercado \
                                         WHERE NATUREZA IN ('Venda', \
                                                            'Compra')")

tmpEnergiaEntregueFinal = tmpEnergiaEntregueFinal.withColumn("IdSubmercado",
                                                             when(tmpEnergiaEntregueFinal.Submercado == lit(
                                                                 'Sudeste/Centro-oeste'), lit(1))
                                                             .when(tmpEnergiaEntregueFinal.Submercado == lit('Sul'),
                                                                   lit(2))
                                                             .when(
                                                                 tmpEnergiaEntregueFinal.Submercado == lit('Norte'),
                                                                 lit(3))
                                                             .when(tmpEnergiaEntregueFinal.Submercado == lit(
                                                                 'Nordeste'), lit(8))
                                                             .otherwise(lit(1)))

tmpEnergiaEntregueFinal.createOrReplaceTempView("TMP_ENERGIA_ENTREGUE_FINAL")

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

# tmpEnergiaContratadaSazoFinalStep2.repartition("ANO")
# tmpEnergiaEntregue.repartition("ANO")

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
                                                    LEFT JOIN (SELECT IDADITIVO,ANO,MES FROM TMP_ENERGIA_ENTREGUE WHERE NATUREZA IN ('Venda','Compra')) AS B ON (A.IDADITIVO = B.IDADITIVO \
                                                    AND A.ANO = B.ANO \
                                                    AND A.MES = B.MES) WHERE B.IDADITIVO IS NULL")

tmpEnergiaContratadaSazoFinalStep3 = fc_Qtd_Horas_Mes(tmpEnergiaContratadaSazoFinalStep3,
                                                      tmpEnergiaContratadaSazoFinalStep3.DtRef)

tmpEnergiaContratadaSazoFinalStep4 = tmpEnergiaContratadaSazoFinalStep3.withColumnRenamed("Qtd_horas", "MESHORA")

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
                                                                         when(
                                                                             tmpEnergiaContratadaSazoFinal.Submercado == lit(
                                                                                 'Sudeste/Centro-oeste'), lit(1))
                                                                         .when(
                                                                             tmpEnergiaContratadaSazoFinal.Submercado == lit(
                                                                                 'Sul'), lit(2))
                                                                         .when(
                                                                             tmpEnergiaContratadaSazoFinal.Submercado == lit(
                                                                                 'Norte'), lit(3))
                                                                         .when(
                                                                             tmpEnergiaContratadaSazoFinal.Submercado == lit(
                                                                                 'Nordeste'), lit(8))
                                                                         .otherwise(lit(1)))

tmpEnergiaContratadaSazoFinal.createOrReplaceTempView("TMP_ENERGIA_CONTRATADA_SAZO_FINAL")

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

tmpOutrosContratosStep2 = fc_Qtd_Horas_Mes(tmpOutrosContratosStep1, tmpOutrosContratosStep1.DtRef)

tmpOutrosContratosStep2 = tmpOutrosContratosStep2.withColumnRenamed("Qtd_horas", "MESHORA")

tmpOutrosContratos = tmpOutrosContratosStep2.select("DTA_REFERENCIA", "CONTRATO", "TIPO", "CONTRAPARTE", "CNPJ",
                                                    "NATUREZA", "MESHORA", "VALOR", "MMH", "PERIODO", "ANO", "MES",
                                                    "ENERGIA", "SUBMERCADO", "PRECO", "X", "UF", "DTA_OPERACAO",
                                                    "FLEX_MAX", "FLEX_MIN", "EXTENSAO", "SERIE", "DIABASE",
                                                    "RECORRENTE", "FLAG_ISENCAO", "NumNotaFiscal", "NumDocumento",
                                                    "PRECO_BASE")

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

tmpLiquidacaoStep2 = fc_Qtd_Horas_Mes(tmpLiquidacaoStep1, tmpLiquidacaoStep1.DtRef)

tmpLiquidacaoStep2 = tmpLiquidacaoStep2.withColumnRenamed("Qtd_horas", "MESHORA")

tmpLiquidacao = tmpLiquidacaoStep2.select("DTA_REFERENCIA", "CTO-LIQUIDACAO", "TIPO", "CONTRAPARTE", "CNPJ",
                                          "NATUREZA", "VALOR", "MESHORA", "MMH", "PERIODO", "ANO", "MES", "ENERGIA",
                                          "SUBMERCADO", "PRECO", "X", "UF", "DTA_OPERACAO", "FLEX_MAX", "FLEX_MIN",
                                          "EXTENSAO", "SERIE", "DIABASE", "RECORRENTE", "FLAG_ISENCAO",
                                          "NumNotaFiscal", "NumDocumento", "PRECO_BASE")

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

tmpModulacaoStep2 = fc_Qtd_Horas_Mes(tmpModulacaoStep1, tmpModulacaoStep1.DtRef)

tmpModulacaoStep2 = tmpModulacaoStep2.withColumnRenamed("Qtd_horas", "MESHORA")

tmpModulacao = tmpModulacaoStep2.select("DTA_REFERENCIA", "CTO-MODULACAO-CEMIG", "TIPO", "CONTRAPARTE", "CNPJ",
                                        "NATUREZA", "MESHORA", "VALOR", "MMH", "PERIODO", "ANO", "MES", "ENERGIA",
                                        "SUBMERCADO", "PRECO", "X", "UF", "DTA_OPERACAO", "FLEX_MAX", "FLEX_MIN",
                                        "EXTENSAO", "SERIE", "DIABASE", "RECORRENTE", "FLAG_ISENCAO",
                                        "NumNotaFiscal", "NumDocumento", "PRECO_BASE")

tmpEnergiaEntregueFinal.coalesce(50)
tmpEnergiaContratadaSazoFinal.coalesce(50)
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

TbFinalStep2 = TbFinalStep1.withColumn("FontePreco", fc_Fonte_Preco_UDF("Periodo", "DTA_REFERENCIA"))

TbFinalStep2.createOrReplaceTempView("TAB_FINAL_STEP2")

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

TbFinalStep6 = fc_Pld_Submercado(TbFinalStep5, TbFinalStep5.Energia, TbFinalStep5.DTA_REF, TbFinalStep5.Periodo,
                                 TbFinalStep5.C_M0, TbFinalStep5.C_M1, TbFinalStep5.C_M2, TbFinalStep5.C_M3,
                                 TbFinalStep5.C_A0, TbFinalStep5.C_A1, TbFinalStep5.C_A2, TbFinalStep5.C_A3,
                                 TbFinalStep5.C_A4, TbFinalStep5.I_M0, TbFinalStep5.I_M1, TbFinalStep5.I_M2,
                                 TbFinalStep5.I_M3, TbFinalStep5.I_A0, TbFinalStep5.I_A1, TbFinalStep5.I_A2,
                                 TbFinalStep5.I_A3, TbFinalStep5.I_A4)

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
                              LEFT JOIN TAB_FINAL_STEP8_INI AS DT_INI ON TO_DATE(SUBSTR(DT_INI.MAX_DATA ,0,10), 'yyyy-MM-dd') = TO_DATE(SUBSTR(A.MAX_DATA ,0,10), 'yyyy-MM-dd') AND DT_INI.Periodo = A.Periodo \
                              LEFT JOIN TAB_FINAL_STEP8_FIM AS DT_FIM ON DT_FIM.Periodo = A.Periodo AND TO_DATE(SUBSTR(DT_FIM.MAX_DATA ,0,10), 'yyyy-MM-dd') = TO_DATE(SUBSTR(A.MAX_DATA ,0,10), 'yyyy-MM-dd') ")

TbFinalStep8.createOrReplaceTempView("TAB_FINAL_STEP8")

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

TbFinalEntregueStep1 = spark.sql("SELECT  TAB1.DTA_REFERENCIA, TAB1.Contrato_Operacao, TAB1.Tipo, TAB1.Contraparte, TAB1.Cnpj, TAB1.Natureza, TAB1.MesHora,  \
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
                            FROM TMP_ENERGIA_ENTREGUE_FINAL as TAB1 \
                            LEFT JOIN PLDMedio AS B ON SUBSTR(B.data,0,10) = TAB1.Periodo AND B.IdSubmercado = TAB1.IdSubmercado where TAB1.Periodo is not null and TAB1.Periodo <>  'PERIODO' ")

TbFinalEntregueStep2 = TbFinalEntregueStep1.withColumn("FontePreco",
                                                       fc_Fonte_Preco_UDF("Periodo", "DTA_REFERENCIA"))

TbFinalEntregueStep2.createOrReplaceTempView("TAB_FINAL_STEP_ENTREGUE_2")

TbFinalEntregueStep3 = spark.sql("SELECT A.DTA_REFERENCIA, \
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
                              FROM TAB_FINAL_STEP_ENTREGUE_2 AS A \
                              LEFT JOIN DCIDE AS B ON ((CASE WHEN A.valor IS NULL \
                                                   OR A.FontePreco = 'DCIDE'THEN ADD_MONTHS(A.DTA_REFERENCIA,-1) ELSE A.DTA_REFERENCIA END) BETWEEN TO_DATE(SUBSTR(B.DTA_INICIO_REF,0,10),'yyyy-MM-dd') AND TO_DATE(SUBSTR(B.DTA_FIM_REF,0,10),'yyyy-MM-dd') \
                                                   AND B.SUBMERCADO = A.Submercado)")

TbFinalEntregueStep3.createOrReplaceTempView("TAB_FINAL_STEP_ENTREGUE_3")

TbFinalEntregueStep4 = spark.sql("SELECT A.DTA_REFERENCIA, \
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
                              FROM TAB_FINAL_STEP_ENTREGUE_3 AS A \
                              LEFT JOIN (SELECT MAX(DTA_FIM_REF) AS MAX_DATA, 1 AS DCIDE FROM DCIDE) AS B ON (A.DCIDE = B.DCIDE)")

TbFinalEntregueStep4.createOrReplaceTempView("TAB_FINAL_STEP_ENTREGUE_4")

TbFinalEntregueStep5 = spark.sql("SELECT A.DTA_REFERENCIA, \
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
                              FROM TAB_FINAL_STEP_ENTREGUE_4 AS A \
                              LEFT JOIN DCIDE AS B ON (A.DTA_REF IS NULL AND B.DTA_FIM_REF = A.MAX_DATA AND B.SUBMERCADO = A.Submercado)")

TbFinalEntregueStep6 = TbFinalEntregueStep5.filter("DTA_REF is not null")

TbFinalEntregueStep7 = fc_Pld_Submercado(TbFinalEntregueStep6, TbFinalEntregueStep6.Energia,
                                         TbFinalEntregueStep6.DTA_REF, TbFinalEntregueStep6.Periodo,
                                         TbFinalEntregueStep6.C_M0, TbFinalEntregueStep6.C_M1,
                                         TbFinalEntregueStep6.C_M2, TbFinalEntregueStep6.C_M3,
                                         TbFinalEntregueStep6.C_A0, TbFinalEntregueStep6.C_A1,
                                         TbFinalEntregueStep6.C_A2, TbFinalEntregueStep6.C_A3,
                                         TbFinalEntregueStep6.C_A4, TbFinalEntregueStep6.I_M0,
                                         TbFinalEntregueStep6.I_M1, TbFinalEntregueStep6.I_M2,
                                         TbFinalEntregueStep6.I_M3, TbFinalEntregueStep6.I_A0,
                                         TbFinalEntregueStep6.I_A1, TbFinalEntregueStep6.I_A2,
                                         TbFinalEntregueStep6.I_A3, TbFinalEntregueStep6.I_A4)

TbFinalEntregueStep8 = TbFinalEntregueStep7.coalesce(50)

TbFinalEntregueStep8.createOrReplaceTempView("TAB_FINAL_STEP_ENTREGUE_8")

TbFinalEntregueStep9Ini = spark.sql("SELECT A.MAX_DATA, \
                                        A.Periodo, \
                                        MAX(DT_INI.DataVigencia) AS DT_INI_DataVigencia \
                                    FROM TAB_FINAL_STEP_ENTREGUE_8 AS A \
                                    LEFT JOIN VW_SERIE_IPCA AS DT_INI ON TO_DATE(SUBSTR(DT_INI.DataVigencia,0,10), 'yyyy-MM-dd') < TO_DATE(SUBSTR(A.MAX_DATA ,0,10), 'yyyy-MM-dd') \
                                    GROUP BY A.MAX_DATA, \
                                             A.Periodo")

TbFinalEntregueStep9Ini.createOrReplaceTempView("TAB_FINAL_STEP_ENTREGUE_9_INI")

TbFinalEntregueStep9Fim = spark.sql("SELECT A.MAX_DATA, \
                                        A.Periodo, \
                                        MAX(DT_FIM.DataVigencia) AS DT_FIM_DataVigencia \
                                    FROM TAB_FINAL_STEP_ENTREGUE_8 AS A \
                                    LEFT JOIN VW_SERIE_IPCA AS DT_FIM ON TO_DATE(SUBSTR(DT_FIM.DataVigencia,0,10), 'yyyy-MM-dd') < A.Periodo \
                                    GROUP BY A.MAX_DATA, \
                                             A.Periodo")

TbFinalEntregueStep9Fim.createOrReplaceTempView("TAB_FINAL_STEP_ENTREGUE_9_FIM")

TbFinalEntregueStep9 = spark.sql("SELECT A.DTA_REFERENCIA, \
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
                              FROM TAB_FINAL_STEP_ENTREGUE_8 AS A \
                              LEFT JOIN TAB_FINAL_STEP_ENTREGUE_9_INI AS DT_INI ON TO_DATE(SUBSTR(DT_INI.MAX_DATA ,0,10), 'yyyy-MM-dd') = TO_DATE(SUBSTR(A.MAX_DATA ,0,10), 'yyyy-MM-dd') AND DT_INI.Periodo = A.Periodo \
                              LEFT JOIN TAB_FINAL_STEP_ENTREGUE_9_FIM AS DT_FIM ON DT_FIM.Periodo = A.Periodo AND TO_DATE(SUBSTR(DT_FIM.MAX_DATA ,0,10), 'yyyy-MM-dd') = TO_DATE(SUBSTR(A.MAX_DATA ,0,10), 'yyyy-MM-dd')")

TbFinalEntregueStep9.createOrReplaceTempView("TAB_FINAL_STEP_ENTREGUE_9")

TbFinalEntregueStep10 = spark.sql("SELECT \
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
                              FROM TAB_FINAL_STEP_ENTREGUE_9 AS A \
                              LEFT JOIN \
                                (SELECT VALOR, \
                                        DataVigencia \
                                 FROM VW_SERIE_IPCA) AS INI ON INI.DataVigencia = A.DT_INI_DataVigencia \
                              LEFT JOIN \
                                (SELECT VALOR, \
                                        DataVigencia \
                                 FROM VW_SERIE_IPCA) AS FIM ON FIM.DataVigencia = A.DT_FIM_DataVigencia")

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

TbFinalSazoStep2 = TbFinalSazoStep1.withColumn("FontePreco", fc_Fonte_Preco_UDF("Periodo", "DTA_REFERENCIA"))

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

TbFinalSazoStep7 = fc_Pld_Submercado(TbFinalSazoStep6, TbFinalSazoStep6.Energia, TbFinalSazoStep6.DTA_REF,
                                     TbFinalSazoStep6.Periodo, TbFinalSazoStep6.C_M0, TbFinalSazoStep6.C_M1,
                                     TbFinalSazoStep6.C_M2, TbFinalSazoStep6.C_M3, TbFinalSazoStep6.C_A0,
                                     TbFinalSazoStep6.C_A1, TbFinalSazoStep6.C_A2, TbFinalSazoStep6.C_A3,
                                     TbFinalSazoStep6.C_A4, TbFinalSazoStep6.I_M0, TbFinalSazoStep6.I_M1,
                                     TbFinalSazoStep6.I_M2, TbFinalSazoStep6.I_M3, TbFinalSazoStep6.I_A0,
                                     TbFinalSazoStep6.I_A1, TbFinalSazoStep6.I_A2, TbFinalSazoStep6.I_A3,
                                     TbFinalSazoStep6.I_A4)

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

TbFinalSazoStep10.show(10)
TbFinalSazoStep10.write.parquet("s3a://ve-datalake-analytics-vg-dev/balanco1/teste/",mode="append")
