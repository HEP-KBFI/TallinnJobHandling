import law
import os
import luigi
import glob
from analysis.framework import SlurmWorkflow
from analysis.util import getPostProcJobInfo
from analysis.tasks import CommandTask
from analysis.metadict_creation import MetaDictCreator
import cataloging


class Postprocessing(CommandTask, SlurmWorkflow, law.LocalWorkflow):
    default_store = "$ANALYSIS_ROOT_PATH"
    cms_loc = '/hdfs/cms/store'

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

    def gethash(self):
        return 'Postproduction'+ str(law.util.create_hash(self.jobDicts, to_int=True)) + '_' + self.version

    @law.cached_workflow_property
    def workDir(self):
        workDirName = os.path.expandvars('$ANALYSIS_WORKAREA') + ('/tmp_' + self.gethash())
        workDir = law.LocalDirectoryTarget(workDirName)
        workDir.touch()
        return workDir

    @law.cached_workflow_property
    def jobDicts(self):
        job_dicts = getPostProcJobInfo(
                os.path.dirname(self.input()['metadicts']['collection'][1].path)
        )
        return job_dicts[:2]

    def create_branch_map(self):
        branchmap = {}
        for branch, branchdata in enumerate(self.jobDicts):
            branchmap[branch] = branchdata
        return branchmap

    def workflow_requires(self):
        return {
            'metadicts': MetaDictCreator.req(
                self, analysis=self.analysis, era=self.era,
                output_dir=self.output_dir, n_events=self.n_events)
        }

    def on_success(self):
        if self.is_workflow():
            os.rmdir(self.workDir.path)
            cleanDir = (os.path.expandvars("${ANALYSIS_LOGFILE_PATH}")+'/' +self.gethash()+'*.txt').strip(' ')
            logFileList = glob.glob(cleanDir)
            for f in logFileList:
                os.remove(f)
        return super(Postprocessing, self).on_success()

    def on_failure(self, exception):
        if self.is_workflow():
            cleanDir = (os.path.expandvars("${ANALYSIS_LOGFILE_PATH}")+'/' +self.gethash()+'*.txt').strip(' ')
            if not self.debug:
                os.rmdir(self.workDir.path)
                logFileList = glob.glob(cleanDir)
                for f in logFileList:
                    os.remove(f)
            else:
                print("Encountered error, preserving workdir (to be deleted manually) ", self.workDir.path)
                print("Encountered error, preserving logfiles (to be deleted manually) ", cleanDir)
        return super(Postprocessing, self).on_failure(exception)

    def output(self):
        outfile_path = os.path.join(
                self.workDir.path,
                os.path.basename(self.branch_data['output_path'])
        )
        return self.local_target(outfile_path)

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
        self.branch_data['maxEntries'] = 10
        final_dir = os.path.dirname(self.branch_data['input_path'].replace(
                self.cms_loc,
                os.path.expandvars(self.output_dir))
        )
        postproc_cmd = f"python3 {postproc_script} -s {suffix} -N {self.branch_data['maxEntries']} --first-entry "\
        f"{self.branch_data['firstEntry']} -I cataloging.postprocessing.config {modules} {self.workDir.path} {self.branch_data['input_path']}"
        outfile_path = os.path.join(
                self.workDir.path,
                os.path.basename(self.branch_data['output_path'])
        )
        move_cmd = f'mv {outfile_path} {final_dir}'
        full_cmd = ' && '.join([postproc_cmd, move_cmd])
        return full_cmd


class PostprocessingWrapper(CommandTask, SlurmWorkflow, law.LocalWorkflow):
    def __init__(self, *args, **kwargs):
        super(PostprocessingWrapper, self).__init__(*args, **kwargs)

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

    def create_branch_map(self):
        branchmap = {}
        for branch, branchdata in enumerate([{'version': self.version, 'era': self.era, 'analysis': self.analysis}]):
            branchmap[branch] = branchdata
        return branchmap

    def output(self):
        log_path = os.path.join(
                os.path.expandvars("${ANALYSIS_LOGFILE_PATH}"),
                'pps_wrapper.txt')
        return self.local_target(log_path)

    def on_success(self):
        log_path = os.path.join(
                os.path.expandvars("${ANALYSIS_LOGFILE_PATH}"),
                'pps_wrapper.txt')
        return super(Postprocessing, self).on_success()

    def build_command(self):
        cmd = f'bash pps_wrapper.sh {self.version} {"local"} {self.era} {self.analysis}'
        log_path = os.path.join(
                os.path.expandvars("${ANALYSIS_LOGFILE_PATH}"),
                'pps_wrapper.txt')
        cmd = ' && '.join([cmd, f'touch {log_path}'])
        return cmd
