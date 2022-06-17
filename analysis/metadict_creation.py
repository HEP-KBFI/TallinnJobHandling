import law
import os
import luigi
import cataloging
from cataloging.analysis import DataSet
from analysis.framework import SlurmWorkflow
from analysis.util import getDatasetList
from analysis.tasks import CommandTask


class MetaDictCreator(CommandTask, SlurmWorkflow, law.LocalWorkflow):
    default_store = "$ANALYSIS_DATA_PATH"
    cms_local_dir = '/hdfs/cms/store'

    def __init__(self, *args, **kwargs):
        super(MetaDictCreator, self).__init__(*args, **kwargs)

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

    pps_output_dir = luigi.Parameter(
        default='/hdfs/local/$USER',
        significant=False,
        description="The directory where postprocessed ntuples will be written",
    )

    sample_name = ""

    @law.cached_workflow_property
    def jobDicts(self):
        job_dicts = getDatasetList(
                self.analysis,
                self.era
        )
        return job_dicts

    def create_branch_map(self):
        branchmap = {}
        for branch, branchdata in enumerate(self.jobDicts):
            branchmap[branch] = branchdata
        return branchmap

    def output(self):
        metadict_output_path = os.path.join(
                cataloging.__path__[0],
                'analyses',
                self.analysis,
                self.era,
                'metadicts',
                f'{self.sample_name}.json')
        return self.local_target(metadict_output_path)

    def run(self):
        dataset = DataSet(
            self.branch_data,
            postproc_out_dir=self.output_dir,
            cms_local_dir=self.cms_local_dir
        )
        metadict_output_dir = os.path.join(
                cataloging.__path__[0],
                'analyses',
                self.analysis,
                self.era,
                'metadicts')
        os.makedirs(metadict_output_dir, exist_ok=True)
        self.sample_name = dataset.sample_name
        dataset.save_json(metadict_output_dir)

    # output file nimi on Dataset.sample_name