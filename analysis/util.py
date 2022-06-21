
"""
Dummy function to provide inputs for the config file generation
"""
import os
import re
import json
import cataloging
from cataloging.postprocessing.tools import import_postproc_info

__location__ = os.path.dirname(__file__)


def getJobDicts(
        analysis='hh-multilepton',
        era='2017',
        channel='2lss',
        mode='default',
        selection=''
):
    dict_file = os.path.join(__location__, 'jobDict.json')
    with open(dict_file, 'rt') as in_file:
        dict_ = json.load(in_file)
    dictList.append(dict_)
    return dictList
    return dictList


def getJobDicts2(analysis = 'hh-multilepton', era='2017', channel = '2lss', mode='default', selection = ''):
    outlist=[]
    baseDict = {}
    baseDict['JETCLEANBYINDEX']='True'
    baseDict['GENMATCHBYINDEX']='True'
    baseDict['NTUPLESELECTIONSTRING']=selection
    if era=='2016':
        from metaDicts.metaDict_2016_mc import sum_events, meta_dictionary
        from metaDicts.metaDict_2016_hh import meta_dictionary as meta_dictionary_hh
        from metaDicts.metaDict_2016_hh import sum_events as sum_events_hh
        baseDict['CMSERA']='2016'
        baseDict['METFLAGS']='config_recommendedMEtFilters_2016'
        baseDict['TRIGGERCONF']='config_triggers_2016'
    elif era=='2017':
        from metaDicts.metaDict_2017_mc import sum_events, meta_dictionary
        from metaDicts.metaDict_2017_hh import meta_dictionary as meta_dictionary_hh
        from metaDicts.metaDict_2017_hh import sum_events as sum_events_hh
        baseDict['CMSERA']='2017'
        baseDict['METFLAGS']='config_recommendedMEtFilters_2017'
        baseDict['TRIGGERCONF']='config_triggers_2017'
    elif era=='2018':
        from metaDicts.metaDict_2018_mc import sum_events, meta_dictionary
        from metaDicts.metaDict_2018_hh import meta_dictionary as meta_dictionary_hh
        from metaDicts.metaDict_2018_hh import sum_events as sum_events_hh
        baseDict['CMSERA']='2018'
        baseDict['METFLAGS']='config_recommendedMEtFilters_2018'
        baseDict['TRIGGERCONF']='config_triggers_2018'
    else:
        raise(Exception("era not implemented"))
    writers = ['writers_evtWeight', 'writers_fakeableHadTaus','writers_fakeableLeptons', 'writers_genHHKinematics','writers_hadTauMultiplicity', 'writers_leptonMultiplicity', 'writers_lowMassLeptonPairVeto', 'writers_met', 'writers_metFilters', 'writers_process', 'writers_run_lumi_event', 'writers_selJetsAK4', 'writers_selJetsAK4_btagLoose', 'writers_selJetsAK4_btagMedium', 'writers_triggerInfo', 'writers_ZbosonMassVeto', 'writers_HtoZZto4lVeto']
    if analysis=='hh-multilepton':
        writers.extend(['writers_evtReweight_HH'])
        if era=='2016':
            baseDict['DENOMLOFILE']='TallinnNtupleProducer/EvtWeightTools/data/HHReweighting/denom_2016.root'
            baseDict['DENOMNLOFILE']='TallinnNtupleProducer/EvtWeightTools/data/HHReweighting/denom_2016_nlo.root'
        elif era=='2017':
            baseDict['DENOMLOFILE']='TallinnNtupleProducer/EvtWeightTools/data/HHReweighting/denom_2017.root'
            baseDict['DENOMNLOFILE']='TallinnNtupleProducer/EvtWeightTools/data/HHReweighting/denom_2017_nlo.root'
        elif era=='2018':
            baseDict['DENOMLOFILE']='TallinnNtupleProducer/EvtWeightTools/data/HHReweighting/denom_2018.root'
            baseDict['DENOMNLOFILE']='TallinnNtupleProducer/EvtWeightTools/data/HHReweighting/denom_2018_nlo.root'
        baseDict['HHWEIGHTSJHEP04SCANFILE']='TallinnNtupleProducer/EvtWeightTools/data/HHReweighting/jhep04_scan.dat'
        baseDict['ENABLEBLACKLIST']='True'
        baseDict['BLACKLISTFILENAMES']= ["'TallinnNtupleProducer/Framework/data/blacklist/blacklist_postproc_2017.txt'", "'TallinnNtupleProducer/Framework/data/blacklist/blacklist_skimmed_multilepton_2017.txt'"]
        baseDict['DISABLEAK8CORR']=["'JMS'", "'JMR'", "'PUPPI'"]
        baseDict['REDOGENMATCHING']='True'
        baseDict['HHWEIGHTSJHEP03SCANFILE']='TallinnNtupleProducer/EvtWeightTools/data/HHReweighting/jhep03_scan.dat'
        baseDict['HHWEIGHTSJHEP04SCANFILE']='TallinnNtupleProducer/EvtWeightTools/data/HHReweighting/jhep04_scan.dat'
        baseDict['HHWEIGHTSKLSCANFILE']='TallinnNtupleProducer/EvtWeightTools/data/HHReweighting/kl_scan.dat'
        baseDict['HHWEIGHTSC2SCANFILE']='TallinnNtupleProducer/EvtWeightTools/data/HHReweighting/c2_scan.dat'
        baseDict['HHWEIGHTSEXTRASCANFILE']='TallinnNtupleProducer/EvtWeightTools/data/HHReweighting/extra_scan.dat'
        baseDict['HHWEIGHTSSCANMODE']=["'JHEP04'", "'JHEP03'", "'kl'", "'c2'", "'extra'"]
        baseDict['APPLYRWGTLO']='False'
        baseDict['RWGTNLOMODE']='v3'
        baseDict['LEPTONFAKERATEWEIGHTINPUTFILENAME']='TallinnNtupleProducer/EvtWeightTools/data/FakeRate/FR_lep_mva_hh_multilepton_wFullSyst_2017_KBFI_2020Dec21_wCERNUncs2_FRErrTheshold_0p01.root'
        baseDict['LEPTONFAKERATEWEIGHTHISTNAME']='FR_mva030_el_data_comb'
        baseDict['LEPTONFAKERATEWEIGHTHISTNAMMU']='FR_mva050_mu_data_comb'
        baseDict['MVACUTMU']='0.5'
        baseDict['MVACUTE']='0.3'
        baseDict['MVAWP']='hh-multilepton'
        baseDict['HADTAUFAKERATEWEIGHTINPUTFILENAME']='TallinnNtupleProducer/EvtWeightTools/data/FakeRate/FR_deeptau_DYmumu_2017_v6.root'
        baseDict['HADTAUFAKERATEWEIGHTLEADFITFUNCNAME'] = 'jetToTauFakeRate/deepVSjMedium/$etaBin/fitFunction_data_div_mc_hadTaus_pt'
        baseDict['HADTAUFAKERATEWEIGHTSUBLEADFITFUNCNAME'] = 'jetToTauFakeRate/deepVSjMedium/$etaBin/fitFunction_data_div_mc_hadTaus_pt'
        baseDict['HADTAUFAKERATEWEIGHTLEADGRAPHNAME']= 'jetToTauFakeRate/deepVSjMedium/$etaBin/jetToTauFakeRate_mc_hadTaus_pt'
        baseDict['HADTAUFAKERATEWEIGHTSUBLEADGRAPHNAME']='jetToTauFakeRate/deepVSjMedium/$etaBin/jetToTauFakeRate_mc_hadTaus_pt'
    else:
        raise(Exception("analysis not implemented!"))
    if channel=='2lss' :
        baseDict['APPLYCHARGEMISIDRATE']='True'
        baseDict['TRIGGERTYPE2MUUSE']='True'
        baseDict['TRIGGERTYPE1E1MUUSE']='True'
        baseDict['TRIGGERTYPE2EUSE']='True'
        baseDict['TRIGGERTYPE1MUUSE']='True'
        baseDict['TRIGGERTYPE1EUSE']='True'
        writers.extend(['writers_bdtVariables_HH_2lss','writers_selJetsAK8_Wjj'])
    else:
        raise(Exception("channel not implemented!"))
        baseDict['APPLYCHARGEMISIDRATE']='False'
        writers.extend([])
    baseDict['WRITERS']=writers
    for key in meta_dictionary:
        sdict=baseDict.copy()
        ## dummy implementation!!!
        sdict['pnamespecific']=meta_dictionary[key]['process_name_specific'] # sould be PROCESSNAME
        sdict['PROCESSNAME'] = 'signal_ggf_nonresonant_hh'
        sdict['PROCESSHH'] = 'signal_ggf_nonresonant_hh_wwww'
        sdict['ISMC'] = 'True'
        sdict['HASLHE'] = 'True'
        sdict['HASPS'] = 'False'
        sdict['OUTFILENAME']='ntuple_'+meta_dictionary[key]['sample_category']+ '_' +meta_dictionary[key]['process_name_specific'] + '_'+ era+ '.root'
        sdict['INPUTFILENAMES']=['"/hdfs/local/snandan/hhAnalysis/2017/official_nanoaod_file/F1B4499C-4A59-D142-8C2E-8364C1C6F90Cii.root"']
        #sdict['INPUTFILENAMES']=['"/hdfs/local/karl/ttHNtupleProduction/2017/2021Mar05_wPresel_nom_all_hh_multilepton/ntuples/signal_ggf_nonresonant_node_sm_hh_4v/0000/tree_1.root"']
        sdict['APPLYTOPPTRWGT']=''
        sdict['APPLYBTAGSFRATIO']='True'
        sdict['TRIGGERINFO']='MC'
        sdict['APPLYGENPHOTONFILTER']='disabled'
        sdict['HADPDFWEIGHTS'] = 'False'
        sdict['BTAGSFRATIOVALUES'] = ['central = cms.vdouble(1.0, 0.983850754831, 0.970806608203, 0.95589515666, 0.941090355157, 0.919510668991, 0.896747198034, 0.869121413881, 0.843409507134, 0.788891130366)']
        sdict['LUMISCALE']= ["cms.PSet(central_or_shift = cms.string('central'), lumi = cms.double(0.00018092350036))", "cms.PSet( central_or_shift = cms.string('CMS_ttHl_thu_shape_HHUp'), lumi = cms.double(0.000141651207997))", "cms.PSet(central_or_shift = cms.string('CMS_ttHl_pileupUp'), lumi = cms.double(0.000180930084191))", "cms.PSet( central_or_shift = cms.string('CMS_ttHl_pileupDown'), lumi = cms.double(0.000180917254171))", "cms.PSet(central_or_shift = cms.string('CMS_ttHl_l1PreFireUp'), lumi = cms.double(0.000182202670616))", "cms.PSet(central_or_shift = cms.string('CMS_ttHl_l1PreFireDown'),lumi = cms.double(0.000179697637174))"]
        sdict['HHWEIGHTHISTTITLE']='signal_ggf_nonresonant_hh_wwww'
        sdict['REFGENWEIGHT']='1.0'
        sdict['APPLYGENWEIGHT']='True'
        sdict['DYMCREWEIGHTING']='False'
        sdict['DYMCNORMSCALEFACTORS']='False'
        sdict['L1PREFIREWEIGHT']='True'
        sdict['APPLYNONCLOSURECORRECTION']='True'
        sdict['BLACKLISTSAMPLENAME']='signal_ggf_nonresonant_node_sm_hh_4v'
        outlist.append(sdict)
    for key in meta_dictionary_hh:
        sdict=baseDict.copy()
        ## dummy implementation!!!
        sdict['pnamespecific']=meta_dictionary_hh[key]['process_name_specific']
        sdict['PROCESSNAME'] = 'signal_ggf_nonresonant_hh'
        sdict['PROCESSHH'] = 'signal_ggf_nonresonant_hh_wwww'
        sdict['ISMC'] = 'True'
        sdict['HASLHE'] = 'True'
        sdict['HASPS'] = 'False'
        sdict['OUTFILENAME']='ntuple_' +meta_dictionary_hh[key]['process_name_specific'] + '_'+ era+ '.root'
        sdict['INPUTFILENAMES']=['"/hdfs/local/snandan/hhAnalysis/2017/official_nanoaod_file/F1B4499C-4A59-D142-8C2E-8364C1C6F90Cii.root"']
        #sdict['INPUTFILENAMES']=['"/hdfs/local/karl/ttHNtupleProduction/2017/2021Mar05_wPresel_nom_all_hh_multilepton/ntuples/signal_ggf_nonresonant_node_sm_hh_4v/0000/tree_1.root"']
        sdict['APPLYTOPPTRWGT']=''
        sdict['APPLYBTAGSFRATIO']='True'
        sdict['TRIGGERINFO']='MC'
        sdict['APPLYGENPHOTONFILTER']='disabled'
        sdict['HADPDFWEIGHTS'] = 'False'
        sdict['BTAGSFRATIOVALUES'] = ['central = cms.vdouble(1.0, 0.983850754831, 0.970806608203, 0.95589515666, 0.941090355157, 0.919510668991, 0.896747198034, 0.869121413881, 0.843409507134, 0.788891130366)']
        sdict['LUMISCALE']=["cms.PSet(central_or_shift = cms.string('central'), lumi = cms.double(0.00018092350036))", "cms.PSet( central_or_shift = cms.string('CMS_ttHl_thu_shape_HHUp'), lumi = cms.double(0.000141651207997))", "cms.PSet(central_or_shift = cms.string('CMS_ttHl_pileupUp'), lumi = cms.double(0.000180930084191))", "cms.PSet( central_or_shift = cms.string('CMS_ttHl_pileupDown'), lumi = cms.double(0.000180917254171))", "cms.PSet(central_or_shift = cms.string('CMS_ttHl_l1PreFireUp'), lumi = cms.double(0.000182202670616))", "cms.PSet(central_or_shift = cms.string('CMS_ttHl_l1PreFireDown'),lumi = cms.double(0.000179697637174))"]
        sdict['HHWEIGHTHISTTITLE']='signal_ggf_nonresonant_hh_wwww'
        sdict['REFGENWEIGHT']='1.0'
        sdict['APPLYGENWEIGHT']='True'
        sdict['DYMCREWEIGHTING']='False'
        sdict['DYMCNORMSCALEFACTORS']='False'
        sdict['L1PREFIREWEIGHT']='True'
        sdict['APPLYNONCLOSURECORRECTION']='True'
        sdict['BLACKLISTSAMPLENAME']='signal_ggf_nonresonant_node_sm_hh_4v'
        outlist.append(sdict)
    return outlist


def getPostProcJobInfo(metadict_dir):
    postproc_settings = import_postproc_info(metadict_dir)
    return postproc_settings


def get_sample_name(dbs_name):
    dataset_name = dbs_name.split('/')[1]
    elements_to_ignore = [
        'Tune', 'PSweight', 'powheg', 'pythia8', 'madgraph', 'TeV',
        'gen', 'Gen', 'madspin', 'MadGraph', 'Autumn', 'Fall', 'Spring',
        'Winter', 'Summer']
    dataset_name_elements = re.split('_|-', dataset_name)
    elements_of_interest = []
    for element in dataset_name_elements:
        element_badness = 0
        for ig_elem in elements_to_ignore:
            if ig_elem in element:
                element_badness += 1
        if element_badness == 0:
            elements_of_interest.append(element)
    return "_".join(elements_of_interest)


def getDatasetList(analysis='HH/multilepton', era='2017') -> list:
    datasets_file_path = os.path.join(
            cataloging.__path__[0],
            'analyses',
            analysis,
            era,
            'datasets.txt'
    )
    job_dicts = []
    with open(datasets_file_path, 'rt') as in_file:
        for line in in_file:
            job_dicts.append({
                    'dataset': line.strip('\n'),
                    'sample_name': get_sample_name(line.strip('\n'))
                })
    return job_dicts