import six
import law
import os
import luigi
import glob
from datetime import datetime
from collections import OrderedDict
from analysis.framework import Task, SlurmWorkflow
from analysis.util import getPostProcJobInfo
from analysis.task import CommandTask
import cataloging


class Postprocessing(CommandTask, SlurmWorkflow, law.LocalWorkflow):
    default_store = "$ANALYSIS_DATA_PATH"

    def __init__(self, *args, **kwargs):
        super(Postprocessing, self).__init__(*args, **kwargs)

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
    def jobDicts(self):
        job_dicts = getPostProcJobInfo(
                self.analysis,
                self.era
        )
        return job_dicts

    def create_branch_map(self):
        branchmap = {}
        for branch, branchdata in enumerate(self.jobDicts):
            branchmap[branch] = branchdata
        return branchmap

    def workflow_requires(self):
        return None

    def output(self):
        # return self.local_target(os.path.basename(self.branch_data['output_path']))
        return self.local_target("/home/laurits/tmp/testing.txt")

    def build_command(self):
        postproc_script = os.path.join(
                os.path.expandvars("$CMSSW_BASE"),
                "src/PhysicsTools/NanoAODTools/scripts/nano_postproc.py")
        suffix = f"_B{self.branch_data['batch_idx']}"
        modules_list_path = os.path.join(
                cataloging.__path__[0],
                'postprocessing',
                'modules.txt')
        with open(modules_list_path, 'rt') as in_file:
            modules = ','.join([line.strip('\n') for line in in_file])
            modules = modules.replace('[ERA]', self.era)
        output_dir = '/home/laurits/tmp' ## REMOVE
        cmd = f"python3 {postproc_script} -s {suffix} -N {self.branch_data['maxEntries']} --first-entry "\
        f"{self.branch_data['firstEntry']} -I cataloging.postprocessing.config {modules} {output_dir} {self.branch_data['input_path']}"
        fake_cmd = f"echo '{cmd}' >> /home/laurits/tmp/testing.txt"
        return fake_cmd
