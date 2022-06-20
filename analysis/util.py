
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
    dictList = []
    dict_file = os.path.join(__location__, 'jobDict.json')
    with open(dict_file, 'rt') as in_file:
        dict_ = json.load(in_file)
    dictList.append(dict_)
    return dictList


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
