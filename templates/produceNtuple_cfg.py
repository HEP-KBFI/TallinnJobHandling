import FWCore.ParameterSet.Config as cms

from TallinnNtupleProducer.Framework.hhWeights_cfi import hhWeights as config_hhWeights
from TallinnNtupleProducer.Framework.recommendedMEtFilters_cfi import recommendedMEtFilters_2016 as config_recommendedMEtFilters_2016, recommendedMEtFilters_2017 as config_recommendedMEtFilters_2017, recommendedMEtFilters_2018 as config_recommendedMEtFilters_2018
from TallinnNtupleProducer.Framework.triggers_cfi import triggers_2016 as config_triggers_2016, triggers_2017 as config_triggers_2017, triggers_2018 as config_triggers_2018

from TallinnNtupleProducer.Writers.BDTVarWriter_HH_2lss_cfi import bdtVariables_HH_2lss as writers_bdtVariables_HH_2lss
from TallinnNtupleProducer.Writers.EvtReweightWriter_HH_cfi import evtReweight_HH as writers_evtReweight_HH
from TallinnNtupleProducer.Writers.EvtReweightWriter_tH_cfi import evtReweight_tH as writers_evtReweight_tH
from TallinnNtupleProducer.Writers.EvtWeightWriter_cfi import evtWeight as writers_evtWeight
from TallinnNtupleProducer.Writers.GenHHKinematicsWriter_cfi import genHHKinematics as writers_genHHKinematics
from TallinnNtupleProducer.Writers.GenPhotonFilterWriter_cfi import genPhotonFilter as writers_genPhotonFilter
from TallinnNtupleProducer.Writers.HtoZZto4lVetoWriter_cfi import HtoZZto4lVeto as writers_HtoZZto4lVeto
from TallinnNtupleProducer.Writers.LowMassLeptonPairVetoWriter_cfi import lowMassLeptonPairVeto as writers_lowMassLeptonPairVeto
from TallinnNtupleProducer.Writers.MEtFilterWriter_cfi import metFilters as writers_metFilters
from TallinnNtupleProducer.Writers.ProcessWriter_cfi import process as writers_process
from TallinnNtupleProducer.Writers.RecoHadTauMultiplicityWriter_cfi import hadTauMultiplicity as writers_hadTauMultiplicity
from TallinnNtupleProducer.Writers.RecoHadTauWriter_cfi import fakeableHadTaus as writers_fakeableHadTaus
from TallinnNtupleProducer.Writers.RecoJetWriterAK4_cfi import selJetsAK4 as writers_selJetsAK4, selJetsAK4_btagLoose as writers_selJetsAK4_btagLoose, selJetsAK4_btagMedium as writers_selJetsAK4_btagMedium
from TallinnNtupleProducer.Writers.RecoJetWriterAK8_Wjj_cfi import selJetsAK8_Wjj as writers_selJetsAK8_Wjj
from TallinnNtupleProducer.Writers.RecoLeptonMultiplicityWriter_cfi import leptonMultiplicity as writers_leptonMultiplicity
from TallinnNtupleProducer.Writers.RecoLeptonWriter_cfi import fakeableLeptons as writers_fakeableLeptons
from TallinnNtupleProducer.Writers.RecoMEtWriter_cfi import met as writers_met
from TallinnNtupleProducer.Writers.RunLumiEventWriter_cfi import run_lumi_event as writers_run_lumi_event
from TallinnNtupleProducer.Writers.TriggerInfoWriter_cfi import triggerInfo as writers_triggerInfo
from TallinnNtupleProducer.Writers.ZbosonMassVetoWriter_cfi import ZbosonMassVeto as writers_ZbosonMassVeto

process = cms.PSet()

process.fwliteInput = cms.PSet(
    fileNames = cms.vstring(),
    maxEvents = cms.int32(10000),
    outputEvery = cms.uint32(1000)
)

process.fwliteOutput = cms.PSet(
    fileName = cms.string('')
)

process.produceNtuple = cms.PSet(
    treeName = cms.string('Events'),

    process = cms.string(''),
    era = cms.string(''),

    # number of "nominal" leptons (electrons and muons) and hadronic taus
    numNominalLeptons = cms.uint32(2),
    applyNumNominalLeptonsCut = cms.bool(False),
    numNominalHadTaus = cms.uint32(1),
    applyNumNominalHadTausCut = cms.bool(False),

    # selection of electrons, muons, and hadronic taus
    lep_mva_cut_mu = cms.double(0.5),
    lep_mva_cut_e = cms.double(0.3),
    lep_mva_wp = cms.string('hh_multilepton'),
    hadTauWP_againstJets_tight = cms.string("deepVSjMedium"),
    hadTauWP_againstJets_fakeable = cms.string("deepVSjVVLoose"),
    hadTauWP_againstElectrons = cms.string(""),
    hadTauWP_againstMuons = cms.string(""),

    # fake rates for electrons, muons, and hadronic taus
    applyFakeRateWeights = cms.string(""),
    leptonFakeRateWeight = cms.PSet(
        inputFileName = cms.string(""),
        histogramName_e = cms.string(""),
        histogramName_mu = cms.string(""),
        era = cms.string(""),
        applyNonClosureCorrection = cms.bool(False),
    ),
    hadTauFakeRateWeight = cms.PSet(
        inputFileName = cms.string(""),
        lead = cms.PSet(
            absEtaBins = cms.vdouble(-1., 1.479, 9.9),
            graphName = cms.string("jetToTauFakeRate/$hadTauSelection/$etaBin/jetToTauFakeRate_mc_hadTaus_pt"),
            applyGraph = cms.bool(True),
            fitFunctionName = cms.string("jetToTauFakeRate/$hadTauSelection/$etaBin/fitFunction_data_div_mc_hadTaus_pt"),
            applyFitFunction = cms.bool(True)
        ),
        sublead = cms.PSet(
            absEtaBins = cms.vdouble(-1., 1.479, 9.9),
            graphName = cms.string("jetToTauFakeRate/$hadTauSelection/$etaBin/jetToTauFakeRate_mc_hadTaus_pt"),
            applyGraph = cms.bool(True),
            fitFunctionName = cms.string("jetToTauFakeRate/$hadTauSelection/$etaBin/fitFunction_data_div_mc_hadTaus_pt"),
            applyFitFunction = cms.bool(True)
        )
    ),
 
    # trigger information
    triggers = cms.PSet(),
    
    # different event weights
    isMC = cms.bool(True),
    lumiScale = cms.VPSet(),
    ref_genWeight = cms.double(0.),
    apply_genWeight = cms.bool(True),
    apply_DYMCReweighting = cms.bool(False),
    apply_DYMCNormScaleFactors = cms.bool(False),
    apply_topPtReweighting = cms.string(''),
    apply_l1PreFireWeight = cms.bool(True),
    btagSFRatio = cms.PSet(),
    apply_btagSFRatio = cms.bool(True),
    #metFilters = cms.PSet(),
    #apply_hadTauFakeRateSF = cms.bool(False),
    #apply_genPhotonFilter = cms.string("disabled"),
    disable_ak8_corr = cms.vstring(['JMS', 'JMR', 'PUPPI']),
    apply_chargeMisIdRate = cms.bool(APPLYCHARGEMISIDRATE), # CV: set to True for 2lss and 2lss+1tau channels, and to False for all other channels 

    evtWeight = cms.PSet(
        apply = cms.bool(False),
        histogramFile = cms.string(''),
        histogramName = cms.string(''),
        branchNameXaxis = cms.string(''),
        branchNameYaxis = cms.string(''),
        branchTypeXaxis = cms.string(''),
        branchTypeYaxis = cms.string(''),
    ),
    thWeights = cms.VPSet(),
    hhWeights = config_hhWeights,

    # reconstructed objects
    branchName_electrons = cms.string('Electron'),
    branchName_muons = cms.string('Muon'),
    branchName_hadTaus = cms.string('Tau'),
    branchName_jets_ak4 = cms.string('Jet'),
    branchName_jets_ak8_Hbb = cms.string('FatJet'),
    branchName_subjets_ak8_Hbb = cms.string('SubJet'),
    branchName_jets_ak8_Wjj = cms.string('FatJet'),
    branchName_subjets_ak8_Wjj = cms.string('SubJet'),
    branchName_met = cms.string('MET'),
    branchName_vertex = cms.string('PV'),

    # generator level information
    branchName_muonGenMatch = cms.string('MuonGenMatch'),
    branchName_electronGenMatch = cms.string('ElectronGenMatch'),
    branchName_hadTauGenMatch = cms.string('TauGenMatch'),
    branchName_jetGenMatch = cms.string('JetGenMatch'),

    branchName_genLeptons = cms.string('GenLep'),
    branchName_genHadTaus = cms.string('GenVisTau'),
    branchName_genPhotons = cms.string('GenPhoton'),
    branchName_genJets = cms.string('GenJet'),
    branchName_genHiggses = cms.string('GenHiggs'),

    # PS weights
    branchName_PSweights = cms.string('PSWeight'),
    branchName_LHE_nom = cms.string('LHEWeight_originalXWGTUP'),
    has_PS_weights = cms.bool(False),
    apply_LHE_nom = cms.bool(False),

    # LHE/PDF weights
    branchName_scale_weights = cms.string('LHEScaleWeight'),
    branchName_pdf_weights = cms.string('LHEPdfWeight'),
    branchName_envelope_weight_up = cms.string('LHEEnvelopeWeightUp'),
    branchName_envelope_weight_down = cms.string('LHEEnvelopeWeightDown'),
    has_LHE_weights = cms.bool(False),
    has_pdf_weights = cms.bool(False),

    branchName_LHE_particle = cms.string('LHEPart'),

    redoGenMatching = cms.bool(False),
    genMatchingByIndex = cms.bool(True),
    jetCleaningByIndex = cms.bool(True),

    writerPlugins = cms.VPSet(
        # writers_bdtVariables_HH_2lss,
        # # CV: use EvtReweightWriter_HH plugin only when processing non-resonant HH MC samples
        # writers_evtReweight_HH,
        # # CV: use EvtReweightWriter_tH plugin only when processing tH MC samples
        # #writers_evtReweight_tH,
        # writers_evtWeight,
        # writers_fakeableHadTaus,
        # writers_fakeableLeptons,
        # # CV: use GenHHKinematicsWriter plugin only when processing HH MC samples (resonant and non-resonant)
        # writers_genHHKinematics,
        # # CV: GenPhotonFilterWriter plugin can be run for some MC samples only,
        # #     because the collection "GenPhotonCandidate" does not exist in all MC samples !!
        # #writers_genPhotonFilter,
        # writers_hadTauMultiplicity,
        # writers_leptonMultiplicity,
        # writers_lowMassLeptonPairVeto,
        # writers_met,
        # writers_metFilters,
        # writers_process,
        # writers_run_lumi_event,
        # writers_selJetsAK4,
        # writers_selJetsAK4_btagLoose,
        # writers_selJetsAK4_btagMedium,
        # writers_selJetsAK8_Wjj,
        # writers_triggerInfo,
        # writers_ZbosonMassVeto,
        # writers_HtoZZto4lVeto
        WRITERS
    ),

    selEventsFileName = cms.string(''),

    blacklist = cms.PSet(
        sampleName = cms.string(""),
        inputFileNames = cms.vstring()
    ),
    enable_blacklist = cms.bool(False),

    selection = cms.string(""),

    isDEBUG = cms.bool(False)
)

process.fwliteInput.fileNames = cms.vstring([INPUTFILENAMES])
process.fwliteOutput.fileName = cms.string('OUTFILENAME')
process.produceNtuple.era                                            = cms.string('CMSERA')
process.produceNtuple.redoGenMatching                                = cms.bool(REDOGENMATCHING)
process.produceNtuple.isDEBUG                                        = cms.bool(False)
writers_metFilters.flags                                             = METFLAGS
process.produceNtuple.process                                        = cms.string('PROCESSNAME')
process.produceNtuple.process_hh                                     = cms.string('PROCESSHH')
process.produceNtuple.isMC                                           = cms.bool(ISMC)
process.produceNtuple.has_LHE_weights                                = cms.bool(HASLHE)
process.produceNtuple.has_PS_weights                                 = cms.bool(HASPS)
process.produceNtuple.lep_mva_cut_mu                                 = cms.double(MVACUTMU)
process.produceNtuple.lep_mva_cut_e                                  = cms.double(MVACUTE)
process.produceNtuple.lep_mva_wp                                     = cms.string('MVAWP')
process.produceNtuple.lumiScale                                      = cms.VPSet([LUMISCALE])
process.produceNtuple.ref_genWeight                                  = cms.double(REFGENWEIGHT)
#process.produceNtuple.applyFakeRateWeights                           = cms.string('disabled')
process.produceNtuple.apply_genWeight                                = cms.bool(APPLYGENWEIGHT)
process.produceNtuple.apply_DYMCReweighting                          = cms.bool(DYMCREWEIGHTING)
process.produceNtuple.apply_DYMCNormScaleFactors                     = cms.bool(DYMCNORMSCALEFACTORS)
process.produceNtuple.apply_l1PreFireWeight                          = cms.bool(L1PREFIREWEIGHT)
process.produceNtuple.leptonFakeRateWeight.inputFileName             = cms.string('LEPTONFAKERATEWEIGHTINPUTFILENAME')
process.produceNtuple.leptonFakeRateWeight.histogramName_e           = cms.string('LEPTONFAKERATEWEIGHTHISTNAME')
process.produceNtuple.leptonFakeRateWeight.histogramName_mu          = cms.string('LEPTONFAKERATEWEIGHTHISTNAMMU')
process.produceNtuple.leptonFakeRateWeight.applyNonClosureCorrection = cms.bool(APPLYNONCLOSURECORRECTION)
process.produceNtuple.hadTauFakeRateWeight.inputFileName             = cms.string('HADTAUFAKERATEWEIGHTINPUTFILENAME')
process.produceNtuple.hadTauFakeRateWeight.lead.fitFunctionName      = cms.string('HADTAUFAKERATEWEIGHTLEADFITFUNCNAME')
process.produceNtuple.hadTauFakeRateWeight.sublead.fitFunctionName   = cms.string('HADTAUFAKERATEWEIGHTSUBLEADFITFUNCNAME')
process.produceNtuple.hadTauFakeRateWeight.lead.graphName            = cms.string('HADTAUFAKERATEWEIGHTLEADGRAPHNAME')
process.produceNtuple.hadTauFakeRateWeight.sublead.graphName         = cms.string('HADTAUFAKERATEWEIGHTSUBLEADGRAPHNAME')
process.produceNtuple.triggers                                       = TRIGGERCONF
process.produceNtuple.triggers.type_2mu.use_it                       = cms.bool(TRIGGERTYPE2MUUSE)
process.produceNtuple.triggers.type_1e1mu.use_it                     = cms.bool(TRIGGERTYPE1E1MUUSE)
process.produceNtuple.triggers.type_2e.use_it                        = cms.bool(TRIGGERTYPE2EUSE)
process.produceNtuple.triggers.type_1mu.use_it                       = cms.bool(TRIGGERTYPE1MUUSE)
process.produceNtuple.triggers.type_1e.use_it                        = cms.bool(TRIGGERTYPE1EUSE)
process.produceNtuple.jetCleaningByIndex                             = cms.bool(JETCLEANBYINDEX)
process.produceNtuple.genMatchingByIndex                             = cms.bool(GENMATCHBYINDEX)
process.produceNtuple.hhWeights.denominator_file_lo                  = cms.string('DENOMLOFILE')
process.produceNtuple.hhWeights.denominator_file_nlo                 = cms.string('DENOMNLOFILE')
process.produceNtuple.hhWeights.histtitle                            = cms.string('HHWEIGHTHISTTITLE')
process.produceNtuple.hhWeights.JHEP04Scan_file                      = cms.string('HHWEIGHTSJHEP04SCANFILE')
process.produceNtuple.hhWeights.JHEP03Scan_file                      = cms.string('HHWEIGHTSJHEP03SCANFILE')
process.produceNtuple.hhWeights.klScan_file                          = cms.string('HHWEIGHTSKLSCANFILE')
process.produceNtuple.hhWeights.c2Scan_file                          = cms.string('HHWEIGHTSC2SCANFILE')
process.produceNtuple.hhWeights.extraScan_file                       = cms.string('HHWEIGHTSEXTRASCANFILE')
process.produceNtuple.hhWeights.scanMode                             = cms.vstring([HHWEIGHTSSCANMODE])
process.produceNtuple.hhWeights.apply_rwgt_lo                        = cms.bool(APPLYRWGTLO)
process.produceNtuple.hhWeights.rwgt_nlo_mode                        = cms.string('RWGTNLOMODE')
process.produceNtuple.apply_topPtReweighting                         = cms.string('APPLYTOPPTRWGT')
#process.produceNtuple.useAssocJetBtag                                = cms.bool(False)
process.produceNtuple.apply_btagSFRatio                              = cms.bool(APPLYBTAGSFRATIO)
#process.produceNtuple.gen_mHH                                        = cms.vdouble([250.0, 260.0, 270.0, 280.0, 300.0, 320.0, 350.0, 400.0, 450.0, 500.0, 550.0, 600.0, 650.0, 700.0, 750.0, 800.0, 850.0, 900.0, 1000.0])
writers_triggerInfo.PD                                               = cms.string("TRIGGERINFO")
writers_genPhotonFilter.apply_genPhotonFilter                        = cms.string('APPLYGENPHOTONFILTER')
#process.produceNtuple.nonRes_BMs                                     = cms.vstring(['SM', 'JHEP04BM1', 'JHEP04BM2', 'JHEP04BM3', 'JHEP04BM4', 'JHEP04BM5', 'JHEP04BM6', 'JHEP04BM7', 'JHEP04BM8', 'JHEP04BM9', 'JHEP04BM10', 'JHEP04BM11', 'JHEP04BM12', 'JHEP04BM8a', 'JHEP03BM1', 'JHEP03BM2', 'JHEP03BM3', 'JHEP03BM4', 'JHEP03BM5', 'JHEP03BM6', 'JHEP03BM7', 'kl_2p45_kt_1p00_c2_0p00_BM9', 'kl_1p00_kt_1p00_c2_0p00_BM9', 'kl_5p00_kt_1p00_c2_0p00_BM9', 'kl_2p45_kt_1p00_c2_0p00_BM7', 'kl_1p00_kt_1p00_c2_0p00_BM7', 'kl_5p00_kt_1p00_c2_0p00_BM7', 'kl_1p00_kt_1p00_c2_0p35_BM9', 'kl_1p00_kt_1p00_c2_3p00_BM9', 'kl_1p00_kt_1p00_c2_0p10_BM9', 'kl_1p00_kt_1p00_c2_m2p00_BM9', 'kl_1p00_kt_1p00_c2_0p35_BM7', 'kl_1p00_kt_1p00_c2_3p00_BM7', 'kl_1p00_kt_1p00_c2_0p10_BM7', 'kl_1p00_kt_1p00_c2_m2p00_BM7', 'kl_0p00_kt_1p00_c2_0p00_BM9', 'kl_0p00_kt_1p00_c2_1p00_BM9', 'kl_0p00_kt_1p00_c2_0p00_BM7', 'kl_0p00_kt_1p00_c2_1p00_BM7'])
process.produceNtuple.enable_blacklist                               = cms.bool(ENABLEBLACKLIST)
process.produceNtuple.blacklist.inputFileNames                       = cms.vstring([BLACKLISTFILENAMES])
process.produceNtuple.blacklist.sampleName                           = cms.string('BLACKLISTSAMPLENAME')
process.produceNtuple.disable_ak8_corr                               = cms.vstring([DISABLEAK8CORR])
process.produceNtuple.has_pdf_weights                                = cms.bool(HADPDFWEIGHTS)
process.produceNtuple.btagSFRatio                                    = cms.PSet(BTAGSFRATIOVALUES)
#process.produceNtuple.selection                                      = cms.string("nlep == 2 & ntau == 1")
process.produceNtuple.selection                                      = cms.string("NTUPLESELECTIONSTRING")
#process.produceNtuple.selEventsFileName                              = cms.string('selEvents_DEBUG.txt')
