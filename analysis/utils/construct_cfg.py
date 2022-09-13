import os
import json
from jinja2 import Template
import cataloging

__location__ = os.path.dirname(__file__)
JINJA_TEMPLATE_PATH = os.path.join(__location__, '.cfg_template.jinja2')
# JINJA_TEMPLATE_PATH = '.cfg_template.jinja2'


def read_json(path):
    with open(path, 'rt') as in_file:
        entries = json.load(in_file)
    return entries


def collect_prodNtuple_entries(
        dataset_cfi,
        analysis='HH/multilepton',
        era='2018',
        channel='2lss_leq1tau'
):
    base_dir = os.path.join(cataloging.__path__[0], 'analyses')
    overall_cfi_path = os.path.join(base_dir, 'prodntuple_cfi.json')
    analysis_cfi_path = os.path.join(base_dir, analysis, 'prodntuple_cfi.json')
    era_cfi_path = os.path.join(base_dir, analysis, era, 'prodntuple_cfi.json')
    channel_cfi_path = os.path.join(base_dir, analysis, era, 'channels', f"{channel}.json")
    cfi_path_list = [overall_cfi_path, analysis_cfi_path, era_cfi_path, channel_cfi_path]
    dataset_cfi = dataset_cfi['prodNtuple_cfi']
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


def construct_fwliteOutput_cfi(
        n_fwliteInput_rows,
        sample_name
):
    """ Based on the number of chunks from the fwliteInput, creates appropriate
    output filename for a given configuration file
    """
    output_files = []
    for i in range(n_fwliteInput_rows):
        output_files.append(f"{sample_name}_tree_{i}.root")
    return output_files


def construct_fwlite_cfi(
        dataset_cfi,
        max_events=8640000
):
    """ Since on average the new framework processes ~ 100ev/s then per day
    it would process ~8 640 000 events, giving some buffer for the 2 day max
    running time of the 'main' queue.

    Args:
        dataset_cfi : dict
            Configuration for a given dataset
        max_events : int
            Number of maximum events to be processed within one job. Actual max
            events to be processed is +- 10% of the given number.
    """
    fwliteInput_cfis = chunk_fwliteInput_fileNames(
                                                dataset_cfi['dataset_files'],
                                                max_events)
    fwliteOutput_cfis = construct_fwliteOutput_cfi(
                                                len(fwliteInput_cfis),
                                                dataset_cfi['sample_name'])
    return fwliteInput_cfis, fwliteOutput_cfis


def fill_template(
        dataset_cfg,
        in_cfi,
        out_cfi,
        output_path,
        skipEvents=0,
        maxEvents=8640000,
        outputEvery=10000,
        is_mc=True
):
    """
    Possible **kwargs:
        output_every : int
            print after every x event
        skipEvents : int
            How many events to skip
        maxEvents : int
            Maximum number of events to process
    """
    full_info = {
        'fwliteInput': in_cfi,
        'fwliteOutput': out_cfi,
        'skipEvents': skipEvents,
        'maxEvents': maxEvents,
        'outputEvery': outputEvery,
        'writers_triggerInfo': {
            'PD': 'MC'
        },
        'writers_genPhotonFilter': {
            "apply_genPhotonFilter": 'disabled'
        }
    }
    with open(JINJA_TEMPLATE_PATH, 'rt') as in_file:
        template = in_file.read()
    unrendered_template = Template(template)
    full_info.update(dataset_cfg)
    with open(output_path, 'wt') as out_file:
        out_file.write(unrendered_template.render(full_info))


def write_cfg_file(
        output_dir,
        dataset_cfi_path,
        analysis,
        era,
        channel,
        is_mc=True,
        **kwargs
):
    dataset_cfi = read_json(dataset_cfi_path)
    dataset_cfg = collect_prodNtuple_entries(
            dataset_cfi=dataset_cfi,
            analysis=analysis,
            era=era,
            channel=channel
    )
    fwliteIn_cfis, fwliteOut_cfis = construct_fwlite_cfi(dataset_cfi)
    for i, (in_cfi, out_cfi) in enumerate(zip(fwliteIn_cfis, fwliteOut_cfis)):
        sample_name = dataset_cfi['sample_name']
        output_path = os.path.join(output_dir, f'{sample_name}_tree_{i}_cfg.py')
        fill_template(dataset_cfg, in_cfi, out_cfi, output_path, **kwargs)
