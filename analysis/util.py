
"""
Dummy function to provide inputs for the config file generation
"""
import os
import json
from cataloging.postprocessing import import_postproc_info

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


def getPostProcJobInfo(analysis='HH/multilepton', era='2017'):
    postproc_settings = import_postproc_info(analysis, era)
    raise NotImplementedError("Here one should return the job info for PPS")

