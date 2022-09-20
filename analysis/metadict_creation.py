import os
import re
import law
import glob
import json
import luigi
import cataloging
from law.util import flatten
from analysis.tasks import KBFIBaseTask
from analysis.framework import SlurmWorkflow
from analysis.utils.metadict_tools import construct_joblist_for_metadict_creation
from cataloging.tools.metadict_creator import MetaDictProducer
from cataloging.tools.general import nth_parent_directory as pardir


def assign_folder_name(datasets_file):
    if datasets_file == 'datasets.txt':
        name = 'default'
    elif '_' in datasets_file:
        name = '_'.join(datasets_file.split('.')[0].split('_')[1:])
    else:
        name = 'Misc'
    return name


def get_sample_name(dbs_name):
    dataset_name = dbs_name.split('/')[1]
    elements_to_ignore = [
        'Tune', 'PSweight', 'powheg', 'pythia8', 'madgraph', 'TeV',
        'gen', 'Gen', 'madspin', 'MadGraph', 'Autumn', 'Fall', 'Spring',
        'Winter', 'Summer', 'PSWeights']
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


#class MetaDictFractionCreator(KBFIBaseTask, law.LocalWorkflow):
class MetaDictFractionCreator(KBFIBaseTask, SlurmWorkflow, law.LocalWorkflow):
    default_store = "$ANALYSIS_DATA_PATH"
    cms_local_dir = '/hdfs/cms/store'

    analysis = luigi.Parameter(
        default='HH/multilepton',
        significant=False,
        description="analysis e.g. hh-multilepton",
    )

    era = luigi.Parameter(
        default='2017',
        significant=False,
        description="era e.g. 2017",
    )

    datasets_file = luigi.Parameter(
        default='datasets.txt',
        significant=False,
        description="Datasets file name",
    )

    @law.cached_workflow_property
    def jobDicts(self):
        datasets = construct_joblist_for_metadict_creation(
                self.analysis,
                self.era,
                datasets_file=self.datasets_file
        )
        return datasets

    def create_branch_map(self):
        branchmap = {}
        for branch, branchdata in enumerate(self.jobDicts):
            branchmap[branch] = branchdata
        return branchmap

    def output(self):
        # sample = self.branch_data.split('/')[1]
        sample_name = get_sample_name(self.branch_data)
        metadict_output_path = os.path.join(
                self.analysis,
                self.era,
                'fragments',
                assign_folder_name(self.datasets_file),
                f'{sample_name}.json')
        return self.local_target(metadict_output_path)

    def run(self):
        md_creator = MetaDictProducer(
            self.branch_data,
            self.era,
            cms_loc=self.cms_local_dir,
        )
        metadict_output_dir = os.path.dirname(self.output().path)
        os.makedirs(metadict_output_dir, exist_ok=True)
        with open(self.output().path, 'wt') as out_file:
            json.dump(md_creator.metadict, out_file, indent=4)


class MetaDictCreator(KBFIBaseTask, SlurmWorkflow, law.LocalWorkflow):
#class MetaDictCreator(KBFIBaseTask, law.LocalWorkflow):
    default_store = "$ANALYSIS_DATA_PATH"
    cms_local_dir = '/hdfs/cms/store'

    analysis = luigi.Parameter(
        default='HH/multilepton',
        significant=False,
        description="analysis e.g. hh-multilepton",
    )

    era = luigi.Parameter(
        default='2017',
        significant=False,
        description="era e.g. 2017",
    )

    @law.cached_workflow_property
    def jobList(self):
        analysis_dir = os.path.join(
                cataloging.__path__[0], 'analyses', self.analysis, self.era)
        datasets_wcp = os.path.join(analysis_dir, 'datasets*.txt')
        dataset_files = [os.path.basename(path) for path in glob.glob(datasets_wcp)]
        print(dataset_files, cataloging.__path__[0])
        return dataset_files

    def create_branch_map(self):
        branchmap = {}
        for branch, branchdata in enumerate(self.jobList):
            input_paths = [target.path for target in self.input()[branchdata]['collection'].targets.values()]
            branchmap[branch] = {
                'key': branchdata,
                'input_paths': input_paths
            }
        return branchmap

    def requires(self):
        for dataset_file in self.jobList:
            yield MetaDictFractionCreator(
                    analysis=self.analysis, era=self.era,
                    datasets_file=dataset_file, version=self.version, workflow="local")

    def workflow_requires(self):
        return {
                dataset_file: MetaDictFractionCreator.req(
                    self, analysis=self.analysis, era=self.era,
                    datasets_file=dataset_file, workflow="local",_prefer_cli=["workflow"]) for dataset_file in self.jobList
            }

    def output(self):
        subfolder_name = assign_folder_name(self.branch_data['key'])
        output_path = os.path.join(
                        self.analysis,
                        self.era,
                        f'metaDict_{self.era}_{subfolder_name}.json')
        return self.local_target(output_path)

    def run(self):
        type_metadict = {}
        # Unclear why the 'input_paths' key in the branch_data is filled correctly
        # but read wrong. In printout it is printing the input_paths 2 times
        # and the first time it is correct, but not the second.
        input_paths = os.path.join(
            os.path.dirname(self.branch_data['input_paths'][0]), '*')
        for path in glob.glob(input_paths):
            with open(path, 'rt') as in_file:
                fragment = json.load(in_file)
                type_metadict.update({fragment['dbs_name']: fragment})
        os.makedirs(os.path.dirname(self.output().path), exist_ok=True)
        with open(self.output().path, 'wt') as out_file:
            json.dump(type_metadict, out_file, indent=4)
