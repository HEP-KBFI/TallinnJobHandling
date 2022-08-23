import json
import os
import cataloging

TYPE_MAPPING = {
    str: 'string',
    int: 'int32',
    float: 'double',
    bool: 'bool'
}

CFG_STUB = """\
import FWCore.ParameterSet.Config as cms
from TallinnNtupleProducer.Framework.recommendedMEtFilters_cfi import \\
    recommendedMEtFilters_2016 as config_recommendedMEtFilters_2016, \\
    recommendedMEtFilters_2017 as config_recommendedMEtFilters_2017, \\
    recommendedMEtFilters_2018 as config_recommendedMEtFilters_2018
from TallinnNtupleProducer.Framework.triggers_cfi import \\
    triggers_2016 as config_triggers_2016, \\
    triggers_2018 as config_triggers_2018, \\
    triggers_2018 as config_triggers_2018
from TallinnNtupleProducer.Writers.BDTVarWriter_HH_2lss_cfi import bdtVariables_HH_2lss as writers_bdtVariables_HH_2lss
from TallinnNtupleProducer.Writers.EvtReweightWriter_HH_cfi import evtReweight_HH as writers_evtReweight_HH
from TallinnNtupleProducer.Writers.EvtReweightWriter_tH_cfi import evtReweight_tH as writers_evtReweight_tH
from TallinnNtupleProducer.Writers.EvtWeightWriter_cfi import evtWeight as writers_evtWeight
from TallinnNtupleProducer.Writers.GenHHKinematicsWriter_cfi import genHHKinematics as writers_genHHKinematics
from TallinnNtupleProducer.Writers.GenPhotonFilterWriter_cfi import genPhotonFilter as writers_genPhotonFilter
from TallinnNtupleProducer.Writers.EvtInfoWriter_cfi import evtInfo as writers_evtInfo
from TallinnNtupleProducer.Writers.HtoZZto4lVetoWriter_cfi import HtoZZto4lVeto as writers_HtoZZto4lVeto
from TallinnNtupleProducer.Writers.LowMassLeptonPairVetoWriter_cfi import lowMassLeptonPairVeto as writers_lowMassLeptonPairVeto
from TallinnNtupleProducer.Writers.MEtFilterWriter_cfi import metFilters as writers_metFilters
from TallinnNtupleProducer.Writers.ProcessWriter_cfi import process as writers_process
from TallinnNtupleProducer.Writers.RecoHadTauMultiplicityWriter_cfi import hadTauMultiplicity as writers_hadTauMultiplicity
from TallinnNtupleProducer.Writers.RecoHadTauWriter_cfi import fakeableHadTaus as writers_fakeableHadTaus
from TallinnNtupleProducer.Writers.RecoJetWriterAK4_cfi import \\
    selJetsAK4 as writers_selJetsAK4, \\
    selJetsAK4_btagLoose as writers_selJetsAK4_btagLoose, \\
    selJetsAK4_btagMedium as writers_selJetsAK4_btagMedium
from TallinnNtupleProducer.Writers.RecoJetWriterAK8_Wjj_cfi import selJetsAK8_Wjj as writers_selJetsAK8_Wjj
from TallinnNtupleProducer.Writers.RecoLeptonMultiplicityWriter_cfi import leptonMultiplicity as writers_leptonMultiplicity
from TallinnNtupleProducer.Writers.RecoLeptonWriter_cfi import fakeableLeptons as writers_fakeableLeptons
from TallinnNtupleProducer.Writers.RecoMEtWriter_cfi import met as writers_met
from TallinnNtupleProducer.Writers.RunLumiEventWriter_cfi import run_lumi_event as writers_run_lumi_event
from TallinnNtupleProducer.Writers.TriggerInfoWriter_cfi import triggerInfo as writers_triggerInfo
from TallinnNtupleProducer.Writers.ZbosonMassVetoWriter_cfi import ZbosonMassVeto as writers_ZbosonMassVeto


process = cms.PSet()
"""


def read_json(path):
    with open(path, 'rt') as in_file:
        entries = json.load(in_file)
    return entries


def collect_prodNtuple_entries(
        dataset_cfi_path,
        analysis='HH/multilepton',
        era='2017',
        channel='2lss_leq1tau'
):
    base_dir = os.path.join(cataloging.__path__[0], 'analyses')
    overall_cfi_path = os.path.join(base_dir, 'prodntuple_cfi.json')
    analysis_cfi_path = os.path.join(base_dir, analysis, 'prodntuple_cfi.json')
    era_cfi_path = os.path.join(base_dir, analysis, era, 'prodntuple_cfi.json')
    channel_cfi_path = os.path.join(base_dir, analysis, era, 'channels', f"{channel}.json")
    cfi_path_list = [overall_cfi_path, analysis_cfi_path, era_cfi_path, channel_cfi_path]
    dataset_cfi = read_json(dataset_cfi_path)['prodNtuple_cfi']
    for cfi_path in cfi_path_list:
        cfi = read_json(cfi_path)
        for key, value in cfi.items():
            if key not in dataset_cfi.keys():
                dataset_cfi[key] = value
            else:
                for sub_key, sub_value in value.items():
                    if type(sub_value) == dict:
                        if sub_key not in dataset_cfi[key].keys():
                            dataset_cfi[key][sub_key] = sub_value
                        else:
                            dataset_cfi[key][sub_key].update(sub_value)
                    else:
                        dataset_cfi[key].update(value)
    return dataset_cfi


def construct_prodNtuple_cfi(meta):
    rows = []
    rows.append("process.produceNtuple = cms.PSet(")
    n_items = len(meta)
    for i, (key, value) in enumerate(meta.items()):
        if type(value) not in [dict, list]:
            type_ = TYPE_MAPPING[type(value)]
            if type_ == 'int32':
                type_ = 'uint32' if value > 0 else 'int32'
            addition = "" if i == n_items - 1 else ","
            if type(value) == str:
                if not "|" in value:
                    value = f'"{value}"'
                    row = f'    {key} = cms.{type_}({value}){addition}'
                else:
                    value = value.replace("|", "")
                    row = f'    {key} = {value}{addition}'
            else:
                row = f'    {key} = cms.{type_}({value}){addition}'
            rows.append(row)
        elif type(value) == list:
            addition = "" if i == n_items - 1 else ","
            if key == "writerPlugins":
                rows.append(f'    {key} = cms.VPSet(')
                n_writers = len(value)
                for j, writer in enumerate(value):
                    addition = "" if j == n_writers - 1 else ","
                    rows.append(f'        {writer}{addition}')
                rows.append(f'    ){addition}')
            elif type(value[0]) != dict:
                type_ = f'v{TYPE_MAPPING[type(value[0])]}'
                if type_ == 'int32':
                    type_ = 'uint32' if value > 0 else 'int32'
                addition = "" if i == n_items - 1 else ","
                if value[0] == '':
                    value = '()'
                else:
                    value = tuple(value)
                row = f'    {key} = cms.{type_}{value}{addition}'
                rows.append(row)
            else:
                rows.append(f'    {key} = cms.VPSet([')
                n_dicts = len(value)
                for j, dict_ in enumerate(value):
                    rows.append(f'        cms.PSet(')
                    n_subitems = len(dict_)
                    for k, (sub_key, sub_value) in enumerate(dict_.items()):
                        addition = "" if k == n_subitems - 1 else ","
                        type_ = TYPE_MAPPING[type(sub_value)]
                        if type_ == 'int32':
                            type_ = 'uint32' if sub_value > 0 else 'int32'
                        if type(sub_value) == str:
                            if not "|" in sub_value:
                                sub_value = f'"{sub_value}"'
                                row = f'            {sub_key} = cms.{type_}({sub_value}){addition}'
                            else:
                                sub_value = sub_value.replace("|", "")
                                row = f'            {sub_key} = {sub_value}{addition}'
                        else:
                            row = f'            {sub_key} = cms.{type_}({sub_value}){addition}'
                        rows.append(row)
                    j_addition = "" if j == n_dicts - 1 else ","
                    rows.append(f'        ){j_addition}')
                addition = "" if i == n_items - 1 else ","
                rows.append(f'    ]){addition}')
        else:
            if key == 'triggers':
                continue
            rows.append(f'    {key} = cms.PSet(')
            n_subitems = len(value)
            for j, (sub_key, sub_value) in enumerate(value.items()):
                if type(sub_value) == list:
                    type_ = f'v{TYPE_MAPPING[type(sub_value[0])]}'
                    if type_ == 'int32':
                        type_ = 'uint32' if sub_value[0] > 0 else 'int32'
                    addition = "" if j == n_subitems - 1 else ","
                    if sub_value[0] == '':
                        sub_value = '()'
                    else:
                        sub_value = tuple(sub_value)
                    row = f'        {sub_key} = cms.{type_}{sub_value}{addition}'
                    rows.append(row)
                else:
                    if type(sub_value) == dict:
                        rows.append(f'        {sub_key} = cms.PSet(')
                        n_subsubitems = len(sub_value)
                        for k, (sub_sub_key, sub_sub_value) in enumerate(sub_value.items()):
                            if type(sub_sub_value) == list:
                                type_ = f'v{TYPE_MAPPING[type(sub_sub_value[0])]}'
                                addition = "" if k == n_subsubitems - 1 else ","
                                rows.append(f'            {sub_sub_key} = cms.{type_}{tuple(sub_sub_value)}{addition}')
                            else:
                                type_ = TYPE_MAPPING[type(sub_sub_value)]
                                if type_ == 'int32':
                                    type_ = 'uint32' if sub_sub_value > 0 else 'int32'
                                addition = "" if k == n_subsubitems - 1 else ","
                                if type(sub_sub_value) == str:
                                    if not "|" in sub_sub_value:
                                        sub_sub_value = f'"{sub_sub_value}"'
                                        row = f'            {sub_sub_key} = cms.{type_}({sub_sub_value}){addition}'
                                    else:
                                        sub_sub_value = sub_sub_value.replace("|", "")
                                        row = f'            {sub_sub_key} = {sub_sub_value}{addition}'
                                else:
                                    row = f'            {sub_sub_key} = cms.{type_}({sub_sub_value}){addition}'
                                rows.append(row)
                        addition = "" if j == n_subitems - 1 else ","
                        rows.append(f'        ){addition}')
                    else:
                        type_ = TYPE_MAPPING[type(sub_value)]
                        addition = "" if j == n_subitems - 1 else ","
                        if type_ == 'int32':
                            type_ = 'uint32' if sub_value > 0 else 'int32'
                        if type(sub_value) == str:
                            if not "|" in sub_value:
                                sub_value = f'"{sub_value}"'
                                row = f'        {sub_key} = cms.{type_}({sub_value}){addition}'
                            else:
                                sub_value = sub_value.replace("|", "")
                                row = f'        {sub_key} = {sub_value}{addition}'
                        else:
                            row = f'        {sub_key} = cms.{type_}({sub_value}){addition}'
                        rows.append(row)
            addition = "" if i == n_items - 1 else ","
            rows.append(f'    ){addition}')
    rows.append(')')
    return rows


def chunk_fwliteInput_fileNames(dataset_cfi, max_events):
    """ Chunks all the dataset files into chunks of less than max_events to be
    processed in a single job.

        Args:
            dataset_cfi : dict
                     file for a given dataset
            max_events : int
                Number of maximum events to be processed within one job. Actual
                max events to be processed is +- 10% of the given number.

        Returns:
            chunks : list of lists
                List of all the chunks.
    """
    chunks = []
    dataset_files = dataset_cfi
    input_paths = []
    job_events = 0
    for input_path, n_events in dataset_files.items():
        if job_events < 0.9*max_events:
            job_events += n_events
            input_paths.append(input_path)
        elif job_events > max_events * 0.9 and job_events + n_events < max_events*1.1:
            input_paths.append(input_path)
            chunks.append(input_paths)
            job_events = 0
            input_paths = []
        else:
            chunks.append(input_paths)
            job_events = 0
            input_paths = []
    if len(input_paths) != 0:
        chunks.append(input_paths)
    return chunks


def construct_fwliteInput_cfi(dataset_cfi, max_events):
    """ Constructs the fwliteInput part of the python config file

        Args:
        dataset_cfi : dict
            Config file for a given dataset
        max_events : int
            Number of maximum events to be processed within one job. Actual max
            events to be processed is +- 10% of the given number.
    """
    cfis = []
    chunks = chunk_fwliteInput_fileNames(dataset_cfi, max_events)
    for chunk in chunks:
        rows = []
        rows.append('process.fwliteInput = cms.PSet(')
        fwliteInput_cfi = {
            'fileNames': chunk,
            'skipEvents': 0,
            'maxEvents': 10,
            # 'maxEvents': -1,
            'outputEvery': 1000
        }
        n_parameters = len(fwliteInput_cfi)
        for i, (key, value) in enumerate(fwliteInput_cfi.items()):
            addition = '' if i == n_parameters - 1 else ','
            if type(value) == list:
                type_ = f'v{TYPE_MAPPING[type(value[0])]}'
                if type_ == 'int32':
                    type_ = 'uint32' if (value > 0 and key != 'maxEvents') else 'int32'
                rows.append(f'    {key} = cms.{type_}([')
                n_entries = len(value)
                for j, entry in enumerate(value):
                    addition = '' if j == n_entries - 1 else ','
                    rows.append(f'        "{entry}"{addition}')
                addition = '' if i == n_parameters - 1 else ','
                rows.append(f'    ]){addition}')
            else:
                type_ = f'{TYPE_MAPPING[type(value)]}'
                if type_ == 'int32':
                    type_ = 'uint32' if (value > 0 and key != 'maxEvents') else 'int32'
                addition = '' if i == n_parameters - 1 else ','
                rows.append(f'    {key} = cms.{type_}({value}){addition}')
        rows.append(')')
        cfis.append(rows)
    return cfis


def construct_fwliteOutput_cfi(
        n_fwliteInput_rows,
        sample_name
):
    """ Based on the number of chunks from the fwliteInput, creates appropriate
    output filename for a given configuration file
    """
    cfis = []
    for i in range(n_fwliteInput_rows):
        rows = []
        rows.append('process.fwliteOutput = cms.PSet(')
        rows.append(f'    fileName = cms.string("{sample_name}_tree_{i}.root")')
        rows.append(')')
        cfis.append(rows)
    return cfis


def construct_fwlite_cfi(
        dataset_cfi_path,
        max_events=8640000
):
    """ Since on average the new framework processes ~ 100ev/s then per day
    it would process ~8 640 000 events, giving some buffer for the 2 day max
    running time of the 'main' queue.

    Args:
        dataset_cfi_path : str
            Path to the given datasets config file
        max_events : int
            Number of maximum events to be processed within one job. Actual max
            events to be processed is +- 10% of the given number.
    """
    dataset_cfi = read_json(dataset_cfi_path)
    fwliteInput_cfis = construct_fwliteInput_cfi(
                                                dataset_cfi['dataset_files'],
                                                max_events)
    fwliteOutput_cfis = construct_fwliteOutput_cfi(
                                                len(fwliteInput_cfis),
                                                dataset_cfi['sample_name'])
    return fwliteInput_cfis, fwliteOutput_cfis


def write_cfg_file(
        output_dir,
        dataset_cfi_path,
        analysis,
        era,
        channel,
        is_mc=True
):
    os.makedirs(output_dir, exist_ok=True)
    output_paths = []
    fwliteInput_cfis, fwliteOutput_cfis = construct_fwlite_cfi(
                                                            dataset_cfi_path,
                                                            max_events=8640000)
    dataset_cfg = collect_prodNtuple_entries(
                                            dataset_cfi_path,
                                            analysis,
                                            era,
                                            channel)
    prodNtuple_rows = construct_prodNtuple_cfi(dataset_cfg)
    for i, (fwliteInput_cfi, fwliteOutput_cfi) in enumerate(zip(fwliteInput_cfis, fwliteOutput_cfis)):
        output_path = os.path.join(output_dir, f'tree_{i}_cfg.py')
        with open(output_path, 'wt') as out_file:
            for line in CFG_STUB:
                out_file.write(line)
            out_file.write('\n')
            for row in fwliteInput_cfi:
                out_file.write(row + '\n')
            out_file.write('\n')
            for row in fwliteOutput_cfi:
                out_file.write(row + '\n')
            out_file.write('\n')
            for row in prodNtuple_rows:
                out_file.write(row + '\n')
            out_file.write('writers_triggerInfo.PD = cms.string("MC")\n')
            out_file.write(f'writers_metFilters.flags = config_recommendedMEtFilters_{era}\n')
            out_file.write('writers_genPhotonFilter.apply_genPhotonFilter = cms.string("disabled")\n')
            for key, value in dataset_cfg['triggers'].items():
                if key == 'config':
                    out_file.write(f'process.produceNtuple.triggers = {value.replace("|", "")}\n')
                else:
                    out_file.write(f'process.produceNtuple.triggers.{key}.use_it = cms.bool({value["use_it"]})\n')
        output_paths.append(output_path)
    return output_paths




# # dataset_cfi_path = '/home/laurits/Analysis/CMSSW_12_3_1/src/TallinnJobHandling/data/MetaDictFractionCreator/v1/HH/multilepton/2018/fragments/TTTo2L2Nu_TuneCP5_13TeV-powheg-pythia8.json'
# dataset_cfi_path = '/home/laurits/Analysis/CMSSW_12_3_1/src/TallinnJobHandling/data/MetaDictFractionCreator/v1/HH/multilepton/2018/fragments/GluGluToHHTo4V_node_SM_TuneCP5_PSWeights_13TeV-madgraph-pythia8.json'


# output_paths = write_cfg_file(
#                             '/home/laurits/tmp/test_cfg',
#                             dataset_cfi_path,
#                             'HH/multilepton',
#                             '2018',
#                             '2lss_leq1tau')
