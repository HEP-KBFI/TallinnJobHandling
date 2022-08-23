
"""
Dummy function to provide inputs for the config file generation
"""
import os
import re
import json
import cataloging


__location__ = os.path.dirname(__file__)


def load_datasets_file(analysis_dir: str, datasets_file='datasets.txt') -> list:
    datasets_file = os.path.join(analysis_dir, datasets_file)
    with open(datasets_file, 'rt') as in_file:
        datasets = [line.strip('\n') for line in in_file]
    return datasets


def construct_joblist_for_metadict_creation(
        analysis='HH/multilepton', era='2017', datasets_file='datasets.txt'
) -> list:
    analysis_dir = os.path.join(
            cataloging.__path__[0],
            'analyses',
            analysis,
            era
    )
    datasets = load_datasets_file(analysis_dir, datasets_file)
    return datasets


