%pyspark

#import das bibliotecas necessarias

from pyspark.sql.types import  StringType, StructField, StructType, IntegerType, DateType, DoubleType, ArrayType
import datetime
from dateutil.relativedelta import relativedelta
from pyspark.sql.functions import udf, collect_list,explode,concat_ws,to_date,lit,when,month,year,dayofmonth,concat,round,isnull,coalesce,abs,datediff,add_months,date_sub,trunc,max,last_day
from pyspark.sql.types import IntegerType

spark.conf.set("spark.sql.crossJoin.enabled", 'true')
spark.conf.set("spark.memory.offHeap.size","16g")


tpCaracContrPartesContratuais = '(11,20,24,32,43)'	#fcQtdPartesContratuais tipo CaracteristicasContrato

tpCaracContrSerieReajuste = '6'					#fcSerieReajuste        tipo CaracteristicasContrato

idCategIsentosPisConfim = '131'					#vwIsentosPisConfim     ID   Categorias

tipoPeriodoVigenciaContrCont = '1'					#vwContratosCont        TIPOPERIODOVIGENCIA   Contratos

dadospropostaContrCont = 0					#vwContratosCont        DADOSPROPOSTA    ContratosAprovaveis

dadosPropostaContrProp = 1					#vwContratosPropostas        DADOSPROPOSTA    Contratos

tipoFcNaturezaStep1 = '(1,2)' #fcNaturezaStep1 tipo PartesContratuais

idTipoFcPrecoComissaoPreco = '(8)' #fcPrecoComissaoPreco IdTipo CaracteristicasContrato

idTipoFcPrecoComissaoCursor = '6' #fcPrecoComissaoCursor IDTIPO CaracteristicasContrato

idTipoCampovw09NumNotaFiscal = '(5,24)' #vw09NumNotaFiscal IdTipoCampo CamposSI

mesReferenciaVw10GeneFaturas = "'01/01/2017'" #vw10GeneFaturas MesReferencia EnergiaFaturas

FlagsVw10GeneFaturas = '32' #vw10GeneFaturas Flags EnergiaFaturas

idTipoVwEnergiaContratadaStep1 = '2' #vwEnergiaContratadaStep1 IdTipo CaracteristicasContrato

idTipoVwEnergiaContratadaStep2Aux = '(11,20, 32)' #vwEnergiaContratadaStep2Aux IdTipo CaracteristicasContrato 

iIdTipoVwEnergiaEntregue = '(5)' #vwEnergiaEntregue IdTipo CaracteristicasContrato

idTipoVwBalancoServicosStep1 = '(8)' # vwBalancoServicosStep1 IdTipo CaracteristicasContrato


#declaracao do schema do data frame


aditivosSchema = StructType ([
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

categoriasSchema = StructType ([
									StructField("Id", StringType(), True),
									StructField("IdTipo", StringType(), True),
									StructField("IdEscopo", StringType(), True),
									StructField("Nome", StringType(), True),
									StructField("Grupo", StringType(), True),
									StructField("Cor", StringType(), True),
									StructField("IdGeneSharp", StringType(), True),
									StructField("Editavel", StringType(), True)])

categoriasContratosSchema = StructType ([
											StructField("IdCategoria", StringType(), True),
											StructField("IdContrato", StringType(), True)])

contratosSchema = StructType ([
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
									#StructField("Observacoes", StringType(), True),
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

caracteristicasFlexibilidadeSchema = StructType ([
													StructField("IdCaracteristica", StringType(), True),
													StructField("IdParteContratual", StringType(), True),
													StructField("IdVariabilidade", StringType(), True),
													StructField("Descricao", StringType(), True),
													StructField("Sufixo", StringType(), True),
													StructField("Periodicidade", StringType(), True),
													StructField("DiaLimite", StringType(), True)])

dadosFlexibilidadesSchema = StructType ([
											StructField("Id", StringType(), True),
											StructField("IdCaracteristicaFlexibilidade", StringType(), True),
											StructField("InicioIntervalo", StringType(), True),
											StructField("FimIntervalo", StringType(), True),
											StructField("IdModelo", StringType(), True),
											StructField("IdPatamar", StringType(), True),
											StructField("FatorMaximo", StringType(), True),
											StructField("FatorMinimo", StringType(), True),
											StructField("Dados", StringType(), True)])

flexCompraSchema = StructType ([
								StructField("COD_CONTRATO", StringType(), True),
								StructField("DTA_REFERENCIA", StringType(), True),
								StructField("VALOR_FLEX", StringType(), True)])

enderecoEempresasSchema = StructType([
										StructField("IdEndereco", StringType(), True),
										StructField("IdEmpresa", StringType(), True),
										StructField("Tipo", StringType(), True),
										StructField("Descricao", StringType(), True),
										StructField("UsarParaFaturamento", StringType(), True)])

enderecosSchema = StructType ([
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

municipiosSchema = StructType ([
								StructField("CodigoIBGE", StringType(), True),
								StructField("CodigoIBGEEstado", StringType(), True),
								StructField("Nome", StringType(), True)])

estadosSchema = StructType ([
								StructField("Id", StringType(), True),
								StructField("CodigoIBGE", StringType(), True),
								StructField("Nome", StringType(), True),
								StructField("Sigla", StringType(), True),
								StructField("IdSubmercado", StringType(), True),
								StructField("IdPais", StringType(), True)])

outrosContratosSchema = StructType ([
										StructField("PERIODO", StringType(), True),
										StructField("CONTRATO", StringType(), True),
										StructField("VALOR", StringType(), True)])

liquidacaoSchema = StructType ([
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

#ler Arquivo
a   = spark.read.csv('s3://ve-datalake-stage-vg-dev/gene/contratos/comercial/tmp_aditivos/', schema=aditivosSchema).coalesce(20)
ct  = spark.read.csv('s3://ve-datalake-stage-vg-dev/gene/contratos/comercial/tmp_categorias/', schema=categoriasSchema)
ctc = spark.read.csv('s3://ve-datalake-stage-vg-dev/gene/contratos/comercial/tmp_categoriasContratos/', schema=categoriasContratosSchema).coalesce(20)
c   = spark.read.csv('s3://ve-datalake-stage-vg-dev/gene/contratos/comercial/tmp_contratos/', schema=contratosSchema, multiLine = True,escape="\n").coalesce(20)
ea  = spark.read.csv('s3://ve-datalake-stage-vg-dev/gene/contratos/comercial/tmp_entidadesAtivaveis/', schema=entidadesAtivaveisSchema).coalesce(20)
ca  = spark.read.csv('s3://ve-datalake-stage-vg-dev/gene/contratos/comercial/tmp_contratosAprovaveis/', schema=contratosAprovaveisSchema).coalesce(20)
eap = spark.read.csv('s3://ve-datalake-stage-vg-dev/gene/contratos/comercial/tmp_entidadesAprovaveis/', schema=entidadesAprovaveisSchema).coalesce(20)
denf = spark.read.csv('s3://ve-datalake-stage-vg-dev/gene/contratos/comercial/tmp_dadosEnergiaFixa/', schema=dadosEnergiaFixaSchema).coalesce(20)
ue  = spark.read.csv('s3://ve-datalake-stage-vg-dev/gene/contratos/comercial/tmp_unidadesEnergia/', schema=unidadesEnergiaSchema)
cc  = spark.read.csv('s3://ve-datalake-stage-vg-dev/gene/contratos/comercial/tmp_caracteristicasContrato/', schema=caracteristicasContratoSchema).coalesce(20)
ag  = spark.read.csv('s3://ve-datalake-stage-vg-dev/gene/contratos/comercial/tmp_agentes/', schema=agentesSchema)
ce  = spark.read.csv('s3://ve-datalake-stage-vg-dev/gene/contratos/comercial/tmp_caracteristicasEnergiaEntregue/', schema=caracteristicasEnergiaEntregueSchema).coalesce(20)
sub  = spark.read.csv('s3://ve-datalake-stage-vg-dev/gene/contratos/comercial/tmp_submercados/', schema=submercadosSchema)
cs  = spark.read.csv('s3://ve-datalake-stage-vg-dev/gene/contratos/comercial/tmp_caracteristicaSazonalizacao/', schema=caracteristicaSazonalizacaoSchema)
bs  = spark.read.csv('s3://ve-datalake-stage-vg-dev/gene/contratos/comercial/tmp_blocosSazonalizacao/', schema=blocosSazonalizacaoSchema).coalesce(20)
das  = spark.read.csv('s3://ve-datalake-stage-vg-dev/gene/contratos/comercial/tmp_dadosAnosSazonalizacao/', schema=dadosAnosSazonalizacaoSchema)
dis  = spark.read.csv('s3://ve-datalake-stage-vg-dev/gene/contratos/comercial/tmp_dadosItemSazonalizacao/', schema=dadosItemSazonalizacaoSchema).coalesce(20)
dms  = spark.read.csv('s3://ve-datalake-stage-vg-dev/gene/contratos/comercial/tmp_dadosMesesSazonalizacao/', schema=dadosMesesSazonalizacaoSchema).coalesce(20)
caj  = spark.read.csv('s3://ve-datalake-stage-vg-dev/gene/contratos/comercial/tmp_caracteristicasAjuste/', schema=caracteristicasAjusteSchema)
ma  = spark.read.csv('s3://ve-datalake-stage-vg-dev/gene/contratos/comercial/tmp_metodologiasAjuste/', schema=metodologiasAjusteSchema)
s  = spark.read.csv('s3://ve-datalake-stage-vg-dev/gene/contratos/comercial/tmp_series/', schema=seriesSchema)
eeef  = spark.read.csv('s3://ve-datalake-stage-vg-dev/gene/contratos/comercial/tmp_entradasEEEnergiaFaturas/', schema=entradasEEEnergiaFaturasSchema).coalesce(20)
ef  = spark.read.csv('s3://ve-datalake-stage-vg-dev/gene/contratos/comercial/tmp_energiaFaturas/', schema=energiaFaturasSchema).coalesce(20)
f  = spark.read.csv('s3://ve-datalake-stage-vg-dev/gene/contratos/comercial/tmp_faturas/', schema=faturasSchema).coalesce(20)
pldm  = spark.read.csv('s3://ve-datalake-stage-vg-dev/gene/contratos/comercial/tmp_pLDMedio/', schema=PLDMedioSchema)
pldm1  = spark.read.csv('s3://ve-datalake-stage-vg-dev/gene/contratos/comercial/tmp_pLDMedio/', schema=PLDMedioSchema)
si  = spark.read.csv('s3://ve-datalake-stage-vg-dev/gene/contratos/comercial/tmp_camposReaisSI/', schema=camposReaisSISchema).coalesce(20)
csi  = spark.read.csv('s3://ve-datalake-stage-vg-dev/gene/contratos/comercial/tmp_camposSI/', schema=camposSISchema).coalesce(20)
cpr  = spark.read.csv('s3://ve-datalake-stage-vg-dev/gene/contratos/comercial/tmp_caracteristicasPreco/', schema=caracteristicasPrecoSchema).coalesce(20)
bee  = spark.read.csv('s3://ve-datalake-stage-vg-dev/gene/contratos/comercial/tmp_blocosEnergiaEntregue/', schema=blocosEnergiaEntregueSchema).coalesce(20)
pld  = spark.read.csv('s3://ve-datalake-stage-vg-dev/gene/contratos/comercial/tmp_dadosPrecoFuncaoPLD/', schema=dadosPrecoFuncaoPLDSchema)
pe  = spark.read.csv('s3://ve-datalake-stage-vg-dev/gene/contratos/comercial/tmp_perfilEnergia/', schema=perfilEnergiaSchema)
eee = spark.read.csv('s3://ve-datalake-stage-vg-dev/gene/contratos/comercial/tmp_entradasEnergiaEntregue/', schema=entradasEnergiaEntregueSchema).coalesce(20)
pc  = spark.read.csv('s3://ve-datalake-stage-vg-dev/gene/contratos/comercial/tmp_partesContratuais/', schema=partesContratuaisSchema).coalesce(20)
e  = spark.read.csv('s3://ve-datalake-stage-vg-dev/gene/contratos/comercial/tmp_empresas/', schema=empresasSchema)
cm  = spark.read.csv('s3://ve-datalake-stage-vg-dev/gene/contratos/comercial/tmp_caracteristicasComissao/', schema=caracteristicasComissaoSchema)
dcp  = spark.read.csv('s3://ve-datalake-stage-vg-dev/gene/contratos/comercial/tmp_dadosPrecoComissao/', schema=dadosPrecoComissaoSchema)
i = spark.read.csv('s3://ve-datalake-stage-vg-dev/gene/contratos/comercial/tmp_ipca/', schema=ipcaSchema)
ip = spark.read.csv('s3://ve-datalake-stage-vg-dev/gene/contratos/comercial/tmp_ipcaProjetado/', schema=ipcaProjetadoSchema)
cprf = spark.read.csv('s3://ve-datalake-stage-vg-dev/gene/contratos/comercial/tmp_caracteristicaPrecoFixo/', schema=caracteristicaPrecoFixoSchema)
dpf = spark.read.csv('s3://ve-datalake-stage-vg-dev/gene/contratos/comercial/tmp_dadosPrecoFixo/', schema=dadosPrecoFixoSchema).coalesce(20)
cf = spark.read.csv('s3://ve-datalake-stage-vg-dev/gene/contratos/comercial/tmp_caracteristicasFlexibilidade/', schema=caracteristicasFlexibilidadeSchema)
df = spark.read.csv('s3://ve-datalake-stage-vg-dev/gene/contratos/comercial/tmp_dadosFlexibilidades/', schema=dadosFlexibilidadesSchema)
fc = spark.read.csv('s3://ve-datalake-stage-vg-dev/gene/contratos/comercial//tmp_flexCompra/', schema=flexCompraSchema)
ee = spark.read.csv('s3://ve-datalake-stage-vg-dev/gene/contratos/comercial/tmp_enderecosEmpresas/', schema=enderecoEempresasSchema)
ed = spark.read.csv('s3://ve-datalake-stage-vg-dev/gene/contratos/comercial/tmp_enderecos/', schema=enderecosSchema)
m = spark.read.csv('s3://ve-datalake-stage-vg-dev/gene/contratos/comercial/tmp_municipios/', schema=municipiosSchema)
es = spark.read.csv('s3://ve-datalake-stage-vg-dev/gene/contratos/comercial/tmp_estados/', schema=estadosSchema)
oc = spark.read.csv('s3://ve-datalake-stage-vg-dev/gene/contratos/comercial/tmp_outrosContratos/', schema=outrosContratosSchema)
l = spark.read.csv('s3://ve-datalake-stage-vg-dev/gene/contratos/comercial/tmp_liquidacao/', schema=liquidacaoSchema)
d = spark.read.csv('s3://ve-datalake-stage-vg-dev/gene/contratos/comercial/tmp_dcide/', schema=dcideSchema)

#cria TempView

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
cc = spark.sql("select * from CaracteristicasContrato where dt_ref = (select max(dt_ref) from CaracteristicasContrato)")
cc.createOrReplaceTempView("CaracteristicasContrato")
ag.createOrReplaceTempView("Agentes")
ag = spark.sql("select * from Agentes where dt_ref = (select max(dt_ref) from Agentes)")
ag.createOrReplaceTempView("Agentes")
ce.createOrReplaceTempView("CaracteristicasEnergiaEntregue")
ce = spark.sql("select * from CaracteristicasEnergiaEntregue where dt_ref = (select max(dt_ref) from CaracteristicasEnergiaEntregue)")
ce.createOrReplaceTempView("CaracteristicasEnergiaEntregue")
sub.createOrReplaceTempView("Submercados")
sub = spark.sql("select * from Submercados where dt_ref = (select max(dt_ref) from Submercados)")
sub.createOrReplaceTempView("Submercados")
cs.createOrReplaceTempView("CaracteristicaSazonalizacao")
cs = spark.sql("select * from CaracteristicaSazonalizacao where dt_ref = (select max(dt_ref) from CaracteristicaSazonalizacao)")
cs.createOrReplaceTempView("CaracteristicaSazonalizacao")
bs.createOrReplaceTempView("BlocosSazonalizacao")
bs = spark.sql("select * from BlocosSazonalizacao where dt_ref = (select max(dt_ref) from BlocosSazonalizacao)")
bs.createOrReplaceTempView("BlocosSazonalizacao")
das.createOrReplaceTempView("DadosAnosSazonalizacao")
das = spark.sql("select * from DadosAnosSazonalizacao where dt_ref = (select max(dt_ref) from DadosAnosSazonalizacao)")
das.createOrReplaceTempView("DadosAnosSazonalizacao")
dis.createOrReplaceTempView("DadosItemSazonalizacao")
dis = spark.sql("select * from DadosItemSazonalizacao where dt_ref = (select max(dt_ref) from DadosItemSazonalizacao)")
dis.createOrReplaceTempView("DadosItemSazonalizacao")
dms.createOrReplaceTempView("DadosMesesSazonalizacao")
dms = spark.sql("select * from DadosMesesSazonalizacao where dt_ref = (select max(dt_ref) from DadosMesesSazonalizacao)")
dms.createOrReplaceTempView("DadosMesesSazonalizacao")
caj.createOrReplaceTempView("CaracteristicasAjuste")
caj = spark.sql("select * from CaracteristicasAjuste where dt_ref = (select max(dt_ref) from CaracteristicasAjuste)")
caj.createOrReplaceTempView("CaracteristicasAjuste")
ma.createOrReplaceTempView("MetodologiasAjuste")
ma = spark.sql("select * from MetodologiasAjuste where dt_ref = (select max(dt_ref) from MetodologiasAjuste)")
ma.createOrReplaceTempView("MetodologiasAjuste")
s.createOrReplaceTempView("Series")
s = spark.sql("select * from Series where dt_ref = (select max(dt_ref) from Series)")
s.createOrReplaceTempView("Series")
eeef.createOrReplaceTempView("EntradasEEEnergiaFaturas")
eeef = spark.sql("select * from EntradasEEEnergiaFaturas where dt_ref = (select max(dt_ref) from EntradasEEEnergiaFaturas)")
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
bee = spark.sql("select * from BlocosEnergiaEntregue where dt_ref = (select max(dt_ref) from BlocosEnergiaEntregue)")
bee.createOrReplaceTempView("BlocosEnergiaEntregue")
pld.createOrReplaceTempView("DadosPrecoFuncaoPLD")
pld = spark.sql("select * from DadosPrecoFuncaoPLD where dt_ref = (select max(dt_ref) from DadosPrecoFuncaoPLD)")
pld.createOrReplaceTempView("DadosPrecoFuncaoPLD")
pe.createOrReplaceTempView("PerfilEnergia")
pe = spark.sql("select * from PerfilEnergia where dt_ref = (select max(dt_ref) from PerfilEnergia)")
pe.createOrReplaceTempView("PerfilEnergia")
eee.createOrReplaceTempView("EntradasEnergiaEntregue")
eee = spark.sql("select * from EntradasEnergiaEntregue where dt_ref = (select max(dt_ref) from EntradasEnergiaEntregue)")
eee.createOrReplaceTempView("EntradasEnergiaEntregue")
pc.createOrReplaceTempView("PartesContratuais")
pc = spark.sql("select * from PartesContratuais where dt_ref = (select max(dt_ref) from PartesContratuais)")
pc.createOrReplaceTempView("PartesContratuais")
e.createOrReplaceTempView("Empresas")
e = spark.sql("select * from Empresas where dt_ref = (select max(dt_ref) from Empresas)")
e.createOrReplaceTempView("Empresas")
cm.createOrReplaceTempView("CaracteristicasComissao")
cm = spark.sql("select * from CaracteristicasComissao where dt_ref = (select max(dt_ref) from CaracteristicasComissao)")
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
cprf = spark.sql("select * from CaracteristicaPrecoFixo where dt_ref = (select max(dt_ref) from CaracteristicaPrecoFixo)")
cprf.createOrReplaceTempView("CaracteristicaPrecoFixo")
dpf.createOrReplaceTempView("DadosPrecoFixo")
dpf = spark.sql("select * from DadosPrecoFixo where dt_ref = (select max(dt_ref) from DadosPrecoFixo)")
dpf.createOrReplaceTempView("DadosPrecoFixo")
cf.createOrReplaceTempView("CaracteristicasFlexibilidade")
cf = spark.sql("select * from CaracteristicasFlexibilidade where dt_ref = (select max(dt_ref) from CaracteristicasFlexibilidade)")
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
#cria funcoes UDF

def fc_Qtd_Horas_Periodo(df,dtInicio, dtFim) :
    
    df = df.withColumn("dtInicio",to_date(df.pInicio)) \
           .withColumn("dtFim",to_date(df.pFim)) \
           .withColumn("Qtd_horas",lit(0))
    df = df.withColumn("Qtd_horas",
            when((year(df.dtInicio) < lit(2018)) & (month(df.dtInicio) == lit(2)), abs(datediff(df.dtFim,df.dtInicio)+lit(1))*lit(24) + lit(1))
           .when((year(df.dtInicio) < lit(2018)) & (month(df.dtInicio) == lit(10)), abs(datediff(df.dtFim,df.dtInicio)+lit(1))*lit(24) - lit(1))
           .when((year(df.dtInicio) < lit(2019)) & (month(df.dtInicio) == lit(11)), abs(datediff(df.dtFim,df.dtInicio)+lit(1))*lit(24))
           .when((year(df.dtInicio) < lit(2019)) & (month(df.dtInicio) == lit(2)), abs(datediff(df.dtFim,df.dtInicio)+lit(1))*lit(24) + lit(1))
           .when((year(df.dtInicio) < lit(2020)) & (month(df.dtInicio) == lit(11)), abs(datediff(df.dtFim,df.dtInicio)+lit(1))*lit(24) - lit(1))
           .when((year(df.dtInicio) < lit(2020)) & (month(df.dtInicio) == lit(2)), abs(datediff(df.dtFim,df.dtInicio)+lit(1))*lit(24))
           .when((year(df.dtInicio) > lit(2020)) & (month(df.dtInicio) == lit(11)), abs(datediff(df.dtFim,df.dtInicio)+lit(1))*lit(24)  - lit(1))
           .when((year(df.dtInicio) > lit(2020)) & (month(df.dtInicio) == lit(2)), abs(datediff(df.dtFim,df.dtInicio)+lit(1))*lit(24)  + lit(1))
           .otherwise(abs(datediff(df.dtFim,df.dtInicio)+lit(1))*lit(24)))
    df2 = df.drop(df.dtInicio) \
             .drop(df.dtFim)
    return df2
        
        
def fc_Fonte_Preco(pRef, pBalanco) :
    
        dtRef = datetime.datetime.strptime(str(pRef), '%Y-%m-%d')
        dtBalanco = datetime.datetime.strptime(str(pBalanco), '%Y-%m-%d')
    
        if (dtRef.year - dtBalanco.year) < 0 or ((dtRef.year - dtBalanco.year) == 0 and (dtRef.month - dtBalanco.month) <=0) :
            retorno = 'DCIDE'
            return retorno
        else:
            inicio = dtRef + relativedelta(months=+1)
            fim = dtBalanco
            dias = 0
            
        while inicio <= fim:
            if inicio.weekday() != 1 or inicio.weekday() != 7:
                dias = dias +1
            inicio = inicio + relativedelta(days=+1)
        
        if dias >= 8 :
            retorno = 'PLD'
        else:
            retorno = 'DCIDE'
        return retorno

def fc_Qtd_Horas_Mes(df,Ref) :
     
     df = df.withColumn("pRef",to_date(df.DtRef)) \
            .withColumn("nextMonth",add_months(to_date(df.DtRef),1)) \
            .withColumn("firstDay",trunc(df.DtRef, "month")) \
            .withColumn("endDay",date_sub(add_months(trunc(df.DtRef, "month"),1),1)) \
            .withColumn("Qtd",lit(0)) \
            .withColumn("Qtd_horas",lit(0))
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
     df = df.withColumn("Qtd_horas",abs(datediff(df.endDay,df.firstDay)+lit(1))*lit(24)+ df.Qtd)

     df2 = df.drop(df.pRef) \
              .drop(df.nextMonth) \
              .drop(df.firstDay) \
              .drop(df.endDay) \
              .drop(df.Qtd)
     return df2

def fc_Natureza(df,check, tipo) :
    df = df.withColumn("pTipo",df.TIPO) \
           .withColumn("check",df.Count.cast(IntegerType()))
    df = df.withColumn("Natureza",
            when((df.pTipo == lit("IGUAL")) &( df.check > lit(0)), "Serviços")
           .when((df.pTipo == lit("IGUAL")) & (df.check <= lit(0)), "Terceiros")
           .when((df.pTipo != lit("IGUAL")) & (df.check == lit(2)), "Venda")
           .when((df.pTipo != lit("IGUAL")) & (df.check == lit(1)), "Compra")
           .otherwise("Terceiros"))
    df2 = df.drop(df.pTipo) \
            .drop(df.check)
    return df2

def fc_Pld_Submercado(df,vPerfilEnergia,pRef, pBalanco,vconvM0,vconvM1,vconvM2,vconvM3,vconvA0,vconvA1,vconvA2,vconvA3,vconvA4,viM0,viM1,viM2,viM3,viA0,viA1,viA2,viA3,viA4) :
    
    df = df.withColumn("PerfilEnergia",df.Energia) \
           .withColumn("dtRef",to_date(df.DTA_REF)) \
           .withColumn("dtBalanco",to_date(df.Periodo)) \
           .withColumn("convM0",df.C_M0.cast("double")) \
           .withColumn("convM1",df.C_M1.cast("double")) \
           .withColumn("convM2",df.C_M2.cast("double")) \
           .withColumn("convM3",df.C_M3.cast("double")) \
           .withColumn("convA0",df.C_A0.cast("double")) \
           .withColumn("convA1",df.C_A1.cast("double")) \
           .withColumn("convA2",df.C_A2.cast("double")) \
           .withColumn("convA3",df.C_A3.cast("double")) \
           .withColumn("convA4",df.C_A4.cast("double")) \
           .withColumn("iM0",df.I_M0.cast("double")) \
           .withColumn("iM1",df.I_M1.cast("double")) \
           .withColumn("iM2",df.I_M2.cast("double")) \
           .withColumn("iM3",df.I_M3.cast("double")) \
           .withColumn("iA0",df.I_A0.cast("double")) \
           .withColumn("iA1",df.I_A1.cast("double")) \
           .withColumn("iA2",df.I_A2.cast("double")) \
           .withColumn("iA3",df.I_A3.cast("double")) \
           .withColumn("iA4",df.I_A4.cast("double")) \
           .withColumn("CURVA_PLD",lit(0.0))
    df = df.withColumn("iM0", \
            when(df.PerfilEnergia == 'I1',df.convM0+lit(150)) \
            .otherwise(df.iM0))
    df = df.withColumn("iM1", \
            when(df.PerfilEnergia == 'I1',df.convM1+lit(150)) \
            .otherwise(df.iM1))
    df = df.withColumn("iM2", \
            when(df.PerfilEnergia == 'I1',df.convM2+lit(150)) \
            .otherwise(df.iM2))
    df = df.withColumn("iM3", \
            when(df.PerfilEnergia == 'I1',df.convM3+lit(150)) \
            .otherwise(df.iM3))
    df = df.withColumn("iA0", \
            when(df.PerfilEnergia == 'I1',df.convA0+lit(150)) \
            .otherwise(df.iA0))
    df = df.withColumn("iA1", \
            when(df.PerfilEnergia == 'I1',df.convA1+lit(150)) \
            .otherwise(df.iA1))
    df = df.withColumn("iA2", \
            when(df.PerfilEnergia == 'I1',df.convA2+lit(150)) \
            .otherwise(df.iA2))
    df = df.withColumn("iA3", \
            when(df.PerfilEnergia == 'I1',df.convA3+lit(150)) \
            .otherwise(df.iA3))
    df = df.withColumn("iA4", \
            when(df.PerfilEnergia == 'CQ5',df.convA4+lit(15)) \
            .otherwise(df.iA4))
    df = df.withColumn("iM0", \
            when(df.PerfilEnergia == 'CQ5',df.convM0+lit(15)) \
            .otherwise(df.iM0))
    df = df.withColumn("iM1", \
            when(df.PerfilEnergia == 'CQ5',df.convM1+lit(15)) \
            .otherwise(df.iM1))
    df = df.withColumn("iM2", \
            when(df.PerfilEnergia == 'CQ5',df.convM2+lit(15)) \
            .otherwise(df.iM2))
    df = df.withColumn("iM3", \
            when(df.PerfilEnergia == 'CQ5',df.convM3+lit(15)) \
            .otherwise(df.iM3))
    df = df.withColumn("iA0", \
            when(df.PerfilEnergia == 'CQ5',df.convA0+lit(15)) \
            .otherwise(df.iA0))
    df = df.withColumn("iA1", \
            when(df.PerfilEnergia == 'CQ5',df.convA1+lit(15)) \
            .otherwise(df.iA1))
    df = df.withColumn("iA2", \
            when(df.PerfilEnergia == 'CQ5',df.convA2+lit(15)) \
            .otherwise(df.iA2))
    df = df.withColumn("iA3", \
            when(df.PerfilEnergia == 'CQ5',df.convA3+lit(15)) \
            .otherwise(df.iA3))
    df = df.withColumn("iA4", \
            when(df.PerfilEnergia == 'CQ5',df.convA4+lit(15)) \
            .otherwise(df.iA4))
    df = df.withColumn("CURVA_PLD", 
            when(((df.PerfilEnergia == lit("Convencional")) | (df.PerfilEnergia == lit("Indefinido")) | (df.PerfilEnergia == lit("I0"))) & (((year(df.dtRef) - year(df.dtBalanco))*lit(12)) + (month(df.dtRef) - month(df.dtBalanco)) <=lit(0)),df.convM0) 
           .when(((df.PerfilEnergia == lit("Convencional")) | (df.PerfilEnergia == lit("Indefinido")) | (df.PerfilEnergia == lit("I0"))) & (((year(df.dtRef) - year(df.dtBalanco))*lit(12)) + (month(df.dtRef) - month(df.dtBalanco)) ==lit(1)),df.convM1) 
           .when(((df.PerfilEnergia == lit("Convencional")) | (df.PerfilEnergia == lit("Indefinido")) | (df.PerfilEnergia == lit("I0"))) & (((year(df.dtRef) - year(df.dtBalanco))*lit(12)) + (month(df.dtRef) - month(df.dtBalanco)) ==lit(2)),df.convM2) 
           .when(((df.PerfilEnergia == lit("Convencional")) | (df.PerfilEnergia == lit("Indefinido")) | (df.PerfilEnergia == lit("I0"))) & (((year(df.dtRef) - year(df.dtBalanco))*lit(12)) + (month(df.dtRef) - month(df.dtBalanco)) ==lit(3)),df.convM3)    
           .when(((df.PerfilEnergia == lit("Convencional")) | (df.PerfilEnergia == lit("Indefinido")) | (df.PerfilEnergia == lit("I0"))) & (((year(df.dtRef) - year(df.dtBalanco))*lit(12)) + (month(df.dtRef) - month(df.dtBalanco)) > lit(3)) & ((year(df.dtRef) - year(df.dtBalanco)) == lit(0)),df.convA0) 
           .when(((df.PerfilEnergia == lit("Convencional")) | (df.PerfilEnergia == lit("Indefinido")) | (df.PerfilEnergia == lit("I0"))) & (((year(df.dtRef) - year(df.dtBalanco))*lit(12)) + (month(df.dtRef) - month(df.dtBalanco)) > lit(3)) & ((year(df.dtRef) - year(df.dtBalanco)) == lit(1)) &  (month(df.dtBalanco) < lit(9)),df.convA1) 
           .when(((df.PerfilEnergia == lit("Convencional")) | (df.PerfilEnergia == lit("Indefinido")) | (df.PerfilEnergia == lit("I0"))) & (((year(df.dtRef) - year(df.dtBalanco))*lit(12)) + (month(df.dtRef) - month(df.dtBalanco)) > lit(3)) & ((year(df.dtRef) - year(df.dtBalanco)) == lit(1)) &  (month(df.dtBalanco) >= lit(9)),df.convA0) 
           .when(((df.PerfilEnergia == lit("Convencional")) | (df.PerfilEnergia == lit("Indefinido")) | (df.PerfilEnergia == lit("I0"))) & (((year(df.dtRef) - year(df.dtBalanco))*lit(12)) + (month(df.dtRef) - month(df.dtBalanco)) > lit(3)) & ((year(df.dtRef) - year(df.dtBalanco)) == lit(2)) &  (month(df.dtBalanco) < lit(9)),df.convA2) 
           .when(((df.PerfilEnergia == lit("Convencional")) | (df.PerfilEnergia == lit("Indefinido")) | (df.PerfilEnergia == lit("I0"))) & (((year(df.dtRef) - year(df.dtBalanco))*lit(12)) + (month(df.dtRef) - month(df.dtBalanco)) > lit(3)) & ((year(df.dtRef) - year(df.dtBalanco)) == lit(2)) &  (month(df.dtBalanco) >= lit(9)),df.convA1) 
           .when(((df.PerfilEnergia == lit("Convencional")) | (df.PerfilEnergia == lit("Indefinido")) | (df.PerfilEnergia == lit("I0"))) & (((year(df.dtRef) - year(df.dtBalanco))*lit(12)) + (month(df.dtRef) - month(df.dtBalanco)) > lit(3)) & ((year(df.dtRef) - year(df.dtBalanco)) == lit(3)) &  (month(df.dtBalanco) < lit(9)),df.convA3) 
           .when(((df.PerfilEnergia == lit("Convencional")) | (df.PerfilEnergia == lit("Indefinido")) | (df.PerfilEnergia == lit("I0"))) & (((year(df.dtRef) - year(df.dtBalanco))*lit(12)) + (month(df.dtRef) - month(df.dtBalanco)) > lit(3)) & ((year(df.dtRef) - year(df.dtBalanco)) == lit(3)) &  (month(df.dtBalanco) >= lit(9)),df.convA2) 
           .when(((df.PerfilEnergia == lit("Convencional")) | (df.PerfilEnergia == lit("Indefinido")) | (df.PerfilEnergia == lit("I0"))) & (((year(df.dtRef) - year(df.dtBalanco))*lit(12)) + (month(df.dtRef) - month(df.dtBalanco)) > lit(3)) & ((year(df.dtRef) - year(df.dtBalanco)) == lit(4)) &  (month(df.dtBalanco) < lit(9)),df.convA4) 
           .when(((df.PerfilEnergia == lit("Convencional")) | (df.PerfilEnergia == lit("Indefinido")) | (df.PerfilEnergia == lit("I0"))) & (((year(df.dtRef) - year(df.dtBalanco))*lit(12)) + (month(df.dtRef) - month(df.dtBalanco)) > lit(3)) & ((year(df.dtRef) - year(df.dtBalanco)) == lit(4)) &  (month(df.dtBalanco) >= lit(9)),df.convA3) 
           .otherwise(df.convA4))
    df = df.withColumn("CURVA_PLD", 
            when((((year(df.dtRef) - year(df.dtBalanco))*lit(12)) + (month(df.dtRef) - month(df.dtBalanco)) <=lit(3)) & (((year(df.dtRef) - year(df.dtBalanco))*lit(12)) + (month(df.dtRef) - month(df.dtBalanco)) <=lit(0)),df.iM0) 
           .when((((year(df.dtRef) - year(df.dtBalanco))*lit(12)) + (month(df.dtRef) - month(df.dtBalanco)) <=lit(3)) & (((year(df.dtRef) - year(df.dtBalanco))*lit(12)) + (month(df.dtRef) - month(df.dtBalanco)) ==lit(1)),df.iM1) 
           .when((((year(df.dtRef) - year(df.dtBalanco))*lit(12)) + (month(df.dtRef) - month(df.dtBalanco)) <=lit(3)) & (((year(df.dtRef) - year(df.dtBalanco))*lit(12)) + (month(df.dtRef) - month(df.dtBalanco)) ==lit(2)),df.iM2) 
           .when((((year(df.dtRef) - year(df.dtBalanco))*lit(12)) + (month(df.dtRef) - month(df.dtBalanco)) <=lit(3)) & (((year(df.dtRef) - year(df.dtBalanco))*lit(12)) + (month(df.dtRef) - month(df.dtBalanco)) ==lit(3)),df.iM3) 
           .when(((((year(df.dtRef) - year(df.dtBalanco))*lit(12)) + (month(df.dtRef) - month(df.dtBalanco)) >lit(3)) & ((year(df.dtRef) - year(df.dtBalanco)) == lit(1)) & (month(df.dtRef) < lit(9))),df.iA1) 
           .when(((((year(df.dtRef) - year(df.dtBalanco))*lit(12)) + (month(df.dtRef) - month(df.dtBalanco)) >lit(3)) & ((year(df.dtRef) - year(df.dtBalanco)) == lit(1)) & (month(df.dtRef) >= lit(9))),df.iA0) 
           .when(((((year(df.dtRef) - year(df.dtBalanco))*lit(12)) + (month(df.dtRef) - month(df.dtBalanco)) >lit(3)) & ((year(df.dtRef) - year(df.dtBalanco)) == lit(2)) & (month(df.dtRef) < lit(9))),df.iA2) 
           .when(((((year(df.dtRef) - year(df.dtBalanco))*lit(12)) + (month(df.dtRef) - month(df.dtBalanco)) >lit(3)) & ((year(df.dtRef) - year(df.dtBalanco)) == lit(2)) & (month(df.dtRef) >= lit(9))),df.iA1) 
           .when(((((year(df.dtRef) - year(df.dtBalanco))*lit(12)) + (month(df.dtRef) - month(df.dtBalanco)) >lit(3)) & ((year(df.dtRef) - year(df.dtBalanco)) == lit(3)) & (month(df.dtRef) < lit(9))),df.iA3) 
           .when(((((year(df.dtRef) - year(df.dtBalanco))*lit(12)) + (month(df.dtRef) - month(df.dtBalanco)) >lit(3)) & ((year(df.dtRef) - year(df.dtBalanco)) == lit(3)) & (month(df.dtRef) >= lit(9))),df.iA2) 
           .when(((((year(df.dtRef) - year(df.dtBalanco))*lit(12)) + (month(df.dtRef) - month(df.dtBalanco)) >lit(3)) & ((year(df.dtRef) - year(df.dtBalanco)) == lit(4)) & (month(df.dtRef) < lit(9))),df.iA4) 
           .when(((((year(df.dtRef) - year(df.dtBalanco))*lit(12)) + (month(df.dtRef) - month(df.dtBalanco)) >lit(3)) & ((year(df.dtRef) - year(df.dtBalanco)) == lit(4)) & (month(df.dtRef) >= lit(9))),df.iA3) 
           .when(((((year(df.dtRef) - year(df.dtBalanco))*lit(12)) + (month(df.dtRef) - month(df.dtBalanco)) >lit(3)) & ((year(df.dtRef) - year(df.dtBalanco)) > lit(4))),df.iA4) 
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

def fc_Preco(df,pDtRef,pDtAtrasoReajuste,pMesReajuste,pDiaReajuste,pDataBase,pPreco,pPeso,serie,valorIndiceInicio,valorIndiceFim) :
    df = df.withColumn("pDtRef", to_date(df.DtRef)) \
           .withColumn("pDtAtrasoReajuste",to_date(df.AtrasoReajuste)) \
           .withColumn("pMesReajuste",df.MesAutoAjustado) \
           .withColumn("pDiaReajuste", df.DiaAutoAjustado) \
           .withColumn("pDataBase", to_date(df.DiaBase)) \
           .withColumn("pPreco", df.PRECO.cast("double")) \
           .withColumn("pPeso",df.Peso.cast("double")) \
           .withColumn("serie",df.Id.cast("integer")) \
           .withColumn("valorIndiceInicio",df.IniValor.cast("double")) \
           .withColumn("valorIndiceFim",df.FimValor.cast("double")) \
           .withColumn("precoIntermediario",lit(0.0))
    df = df.withColumn("pDtRef", \
            when((df.pDtRef >= df.pDtAtrasoReajuste) & (month(df.pDtRef) == df.pMesReajuste) & (dayofmonth(df.pDtRef) == df.pDiaReajuste),df.pDtRef) \
            .when((df.pDtRef >= df.pDtAtrasoReajuste) & (month(df.pDtRef) < df.pMesReajuste),to_date(concat(year(df.pDtRef)-1, lit("-"), df.pMesReajuste, lit("-"), df.pDiaReajuste),"yyyy-mm-dd")) \
           .when((df.pDtRef >= df.pDtAtrasoReajuste) & (month(df.pDtRef) != df.pMesReajuste),to_date(concat(year(df.pDtRef), lit("-"), df.pMesReajuste, lit("-"), df.pDiaReajuste),"yyyy-mm-dd")) \
            .otherwise(df.pDtRef))
    df = df.withColumn("precoIntermediario",
            when(year(df.pDtRef) <=year(df.pDataBase),df.pPreco * df.pPeso )
            .when((df.serie == lit(6)) | (df.serie == lit(5)),round(df.pPreco * (df.valorIndiceInicio/df.valorIndiceFim),3) * df.pPeso )
            .when((df.serie == lit(19)), df.pPreco)
            .when((df.serie.isNull()) & (df.pDiaReajuste.isNull()),df.pPreco * coalesce(df.pPeso,lit(1)))
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

#cria UDF

fc_Fonte_Preco_UDF = udf(lambda dtRef,dtBalanco: fc_Fonte_Preco(dtRef,dtBalanco), StringType())

#fc_Ano_Data_UDF = udf(lambda dtRef: [ datetime.datetime.strptime(str(dtRef), '%Y-%m-%d').year], StringType())

#fc_Mes_Data_UDF = udf(lambda dtRef: [ datetime.datetime.strptime(str(dtRef), '%Y-%m-%d').month], StringType())

#c_Fim_Mes_UDF = udf(lambda dtRef: [ str((datetime.datetime.strptime(str(dtRef), '%Y-%m-%d') + relativedelta(months=+1)) - relativedelta(days=+1))], StringType())

fc_Range_Dat_UDF = udf(lambda dtInicio, x: [ datetime.datetime.strptime(dtInicio, '%Y-%m-%d') + relativedelta(months=int(y)) for y in range(0,int(x))], ArrayType(DateType()) )


#aux Join

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


#cria Funcoes de Join

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
											WHERE IdTipo IN "+tpCaracContrPartesContratuais+" \
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
  					.groupby("CodigoReferencia","IDTIPO","TIPO")
  					.agg(collect_list("NOMEFANTASIA")))

fcCompradorVendedor = fcCompradorVendedor.withColumnRenamed("collect_list(NOMEFANTASIA)", "NOMEFANTASIA")

#Funcoes complexas

#FC_GET_NATUREZA

fcNaturezaStep1 = spark.sql("SELECT COUNT(*) AS Count, PC.IDADITIVO \
								FROM PartesContratuais AS PC \
								WHERE PC.IDTIPO IN "+tipoFcNaturezaStep1+" \
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
					  
fcNatureza = fc_Natureza(fcNaturezaStep2,fcNaturezaStep2.Count,fcNaturezaStep2.TIPO)


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
									WHERE (CCENER.IdTipo IN "+idTipoFcPrecoComissaoPreco+")")

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
									WHERE CC.IDTIPO ="+idTipoFcPrecoComissaoCursor)

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



#cria FC_GET_NATUREZA
fcNatureza.createOrReplaceTempView("FC_GET_NATUREZA")

#cria FC_GET_SERIE_REAJUSTE
fcSerieReajuste.createOrReplaceTempView("FC_GET_SERIE_REAJUSTE")

#cria FC_GET_QTD_PARTES_CONTRATUAIS
fcQtdPartesContratuais.createOrReplaceTempView("FC_GET_QTD_PARTES_CONTRATUAIS")

#cria FC_GET_PRECO_FATURA
fcPrecoFatura.createOrReplaceTempView("FC_GET_PRECO_FATURA")

#cria FC_GET_PLD
fcPLD.createOrReplaceTempView("FC_GET_PLD")

#cria FC_GET_COMPRADOR_VENDEDOR
fcCompradorVendedor.createOrReplaceTempView("FC_GET_COMPRADOR_VENDEDOR")

#cria FC_GET_PLD_MAX
fcPldMax.createOrReplaceTempView("FC_GET_PLD_MAX")

#cria FC_GET_PRECO_COMISSAO_PRECO
fcPrecoComissaoPreco.createOrReplaceTempView("FC_GET_PRECO_COMISSAO_PRECO")

#cria FC_GET_PRECO
fcPreco.createOrReplaceTempView("FC_GET_PRECO")

#cria FC_GET_PRECO_COMISSAO_CURSOR
fcPrecoComissaoCursor.createOrReplaceTempView("FC_GET_PRECO_COMISSAO_CURSOR")

#cria FC_GET_RECORRENCIA_REAJUSTE
fcRecorrenciaReajuste.createOrReplaceTempView("FC_GET_RECORRENCIA_REAJUSTE")

#cria FC_GET_DIABASE_REAJUSTE
fcDiabaseReajuste.createOrReplaceTempView("FC_GET_DIABASE_REAJUSTE")




#cria VW_SERIE_IPCA
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


#cria VW_ISENTOS_PIS_COFINS
vwIsentosPisConfim = spark.sql("SELECT A.Id AS Id, \
       								CT.Nome \
								FROM CategoriasContratos AS CTC \
								INNER JOIN Categorias AS CT ON CTC.IdCategoria = CT.Id \
								INNER JOIN Contratos AS C ON C.Id = CTC.IdContrato \
								LEFT OUTER JOIN Aditivos AS A ON C.Id = A.IdContrato \
								WHERE CT.id = "+idCategIsentosPisConfim+" \
								GROUP BY A.Id , \
         						CT.Nome ")

vwIsentosPisConfim.createOrReplaceTempView("VW_ISENTOS_PIS_COFINS")

#cria VW_09_NUM_NOTA_FISCAL
vw09NumNotaFiscal = spark.sql("SELECT SI.IdArmazenadorCamposSI, \
								       MAX(CASE WHEN CSI.IdTipoCampo = 5 THEN CSI.Valor END) AS NumNotaFiscal, \
								       MAX(CASE WHEN CSI.IdTipoCampo = 24 THEN CSI.Valor END) AS NumDocumento \
								FROM CamposReaisSI SI \
								LEFT JOIN CamposSI CSI ON (SI.Id = CSI.Id) \
								WHERE CSI.IdTipoCampo IN "+idTipoCampovw09NumNotaFiscal+" \
								GROUP BY SI.IdArmazenadorCamposSI")

vw09NumNotaFiscal.createOrReplaceTempView("VW_09_NUM_NOTA_FISCAL")

#cria VW_10_GENE_FATURAS

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
								WHERE Flags = "+FlagsVw10GeneFaturas+" \
								  AND EEEF.IdEntradaEnergiaEntregue IS NOT NULL \
								  AND EF.MesReferencia >= "+mesReferenciaVw10GeneFaturas)

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