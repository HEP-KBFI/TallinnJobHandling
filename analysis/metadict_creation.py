import law
import os
import luigi
import cataloging
from cataloging.analysis import DataSet
from analysis.framework import SlurmWorkflow
from analysis.util import getDatasetList
from analysis.tasks import KBFIBaseTask


class MetaDictCreator(KBFIBaseTask, SlurmWorkflow, law.LocalWorkflow):
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

    output_dir = luigi.Parameter(
        default='/hdfs/local/$USER',
        significant=False,
        description="The directory where postprocessed ntuples will be written",
    )

    n_events = luigi.IntParameter(
        default=100000,
        significant=False,
        description="Maximum number of events per postprocessed file",
    )

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
                self.analysis,
                self.era,
                f'{self.branch_data["sample_name"]}.json')
        return self.local_target(metadict_output_path)

    def run(self):
        dataset = DataSet(
            self.branch_data['dataset'],
            postproc_out_dir=self.output_dir,
            cms_local_dir=self.cms_local_dir,
            max_events_per_file=self.n_events
        )
        metadict_output_dir = os.path.dirname(self.output().path)
        os.makedirs(metadict_output_dir, exist_ok=True)
        dataset.save_json(metadict_output_dir)
