import os
import law
import glob
import json
import luigi
import shutil
from law.util import flatten
import cataloging
from analysis.tasks import KBFIBaseTask, CommandTask
from analysis.utils.construct_cfg import write_cfg_file
from analysis.utils.construct_cfg import read_json
from analysis.utils.construct_cfg import chunk_fwliteInput_fileNames
from analysis.framework import SlurmWorkflow
from analysis.metadict_creation import MetaDictFractionCreator
from cataloging.tools.general import nth_parent_directory as pardir
import traceback

"""
 Task to create the configs for running TallinnNtuple jobs.
This is later to be done with the file catalog
"""


def assign_folder_name(datasets_file):
    if datasets_file == 'datasets.txt':
        name = 'default'
    elif '_' in datasets_file:
        name = '_'.join(datasets_file.split('.')[0].split('_')[1:])
    else:
        name = 'Misc'
    return name

# class CreateTallinnNtupleConfigs(KBFIBaseTask, SlurmWorkflow, law.LocalWorkflow):
class CreateTallinnNtupleConfigs(KBFIBaseTask, law.LocalWorkflow):
    default_store = "$ANALYSIS_CONFIG_PATH"
    analysis = luigi.Parameter(
        default='HH/multilepton',
        significant=False,
        description="analysis e.g. HH/multilepton",
    )

    era = luigi.Parameter(
        default='2018',
        significant=False,
        description="era e.g. 2018",
    )

    channel = luigi.Parameter(
        default='2lss_leq1tau',
        significant=False,
        description="channel e.g. 2lss_leq1tau",
    )

    mode = luigi.Parameter(
        default='default',
        significant=False,
        description="mode e.g. default",
    )

    region = luigi.Parameter(
        default='',
        significant=False,
        description="region e.g. 'SS_SR' or selection string",
    )

    @law.cached_workflow_property
    def jobList(self):
        analysis_dir = os.path.join(
                cataloging.__path__[0], 'analyses', self.analysis, self.era)
        datasets_wcp = os.path.join(analysis_dir, 'datasets*.txt')
        dataset_files = [os.path.basename(path) for path in glob.glob(datasets_wcp)]
        return dataset_files

    def create_branch_map(self):
        branchmap = {}
        jobs = []
        input_paths = []
        for idx, ds_file in enumerate(self.jobList):
            target_values = self.input()[ds_file]['collection'].targets.values()
            input_paths.extend([target.path for target in target_values])
        for input_path in input_paths:
            dataset_cfi = read_json(input_path)
            n_chunks = len(chunk_fwliteInput_fileNames(
                dataset_cfi['dataset_files'], job_max_events=8640000))
            for i in range(n_chunks):
                jobs.append({
                    "idx": i,
                    "dataset_cfi": dataset_cfi,
                })
        for idx, job in enumerate(jobs):
            branchmap[idx] = job
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
        dataset_name = self.branch_data['dataset_cfi']['sample_name']
        i = self.branch_data['idx']
        config_path = os.path.join(f'{dataset_name}_tree_{i}_cfg.py')
        return self.local_target(config_path)

    def run(self):
        output_dir = os.path.dirname(self.output().path)
        os.makedirs(output_dir, exist_ok=True)
        output_paths = write_cfg_file(
                self.output().path,
                self.branch_data['dataset_cfi'],
                self.branch_data['idx'],
                self.analysis,
                self.era,
                self.channel,
                is_mc=True, # This needs to be done differently
                region=self.region
        )


class ProdTallinnNTuples(CommandTask, law.LocalWorkflow):
# class ProdTallinnNTuples(CommandTask, SlurmWorkflow, law.LocalWorkflow):
    default_store = "$ANALYSIS_ROOT_PATH"
    NTUPLE_FINAL_STORE = "/hdfs/local/"

    def __init__(self, *args, **kwargs):
        super(ProdTallinnNTuples, self).__init__(*args, **kwargs)

    analysis = luigi.Parameter(
        default='HH/multilepton',
        significant=False,
        description="analysis e.g. HH/multilepton",
    )

    era = luigi.Parameter(
        default='2018',
        significant=False,
        description="era e.g. 2018",
    )

    channel = luigi.Parameter(
        default='2lss_leq1tau',
        significant=False,
        description="channel e.g. 2lss_leq1tau",
    )

    mode = luigi.Parameter(
        default='default',
        significant=False,
        description="mode e.g. default",
    )

    region = luigi.Parameter(
        default='',
        significant=False,
        description="region e.g. 'SS_SR' or selection string",
    )

    mode = luigi.Parameter(
        default='default',
        significant=False,
        description="mode e.g. default",
    )

    debug = luigi.BoolParameter(
        default=False,
        significant=False,
        description='Whether keep the temporary files')

    def gethash(self):
        return 'ProdTallinnNTuples'+ str(law.util.create_hash(self.jobList, to_int=True)) + '_'+ self.version

    @law.cached_workflow_property
    def workDir(self):
        workDirName = os.path.join(
            os.path.expandvars('$ANALYSIS_WORKAREA'),
            f'tmp_{self.gethash()}'
        )
        workDir = law.LocalDirectoryTarget(workDirName)
        workDir.touch()
        return workDir

    @law.cached_workflow_property
    def jobList(self):
        input_odict = self.input()['configs']['collection'].targets.values()
        config_paths = [path.path for path in list(input_odict)]
        return config_paths[:2]

    def create_branch_map(self):
        branches = {}
        for idx, path in enumerate(self.jobList):
            branches[idx] = path
        return branches

    def workflow_requires(self):
        return {
            'configs': CreateTallinnNtupleConfigs.req(
                self, workflow="local",
                _prefer_cli=[
                    "workflow", "version", "mode", "region"
                ],
                analysis=self.analysis,
                era=self.era,
                version=self.version,
                region=self.region
            )
        }

    def requires(self):
        yield CreateTallinnNtupleConfigs(
                analysis=self.analysis,
                era=self.era,
                version=self.version,
                region=self.region)

    def on_success(self):
        if self.is_workflow():
            shutil.rmtree(self.workDir.path)
            cleanDir = os.path.join(
                os.path.expandvars("${ANALYSIS_LOGFILE_PATH}"),
                self.gethash() + '*.txt'
            )
            logFileList = glob.glob(cleanDir)
            for f in logFileList:
                os.remove(f)
        #return super(ProdTallinnNTuples, self).on_success()

    def on_failure(self, exception):
        if self.is_workflow():
            cleanDir = os.path.join(
                os.path.expandvars("${ANALYSIS_LOGFILE_PATH}"),
                self.gethash() + '*.txt'
            )
            if not self.debug:
                shutil.rmtree(self.workDir.path)
                logFileList = glob.glob(cleanDir)
                for f in logFileList:
                    os.remove(f)
            else:
                print(
                    "Encountered error, preserving workdir",
                    "(Manual deletion required): ",
                    self.workDir.path)
                print(
                    "Encountered error, preserving logfiles",
                    "(Manual deletion required): ",
                    self.cleanDir)
        traceback_string = traceback.format_exc()
        return "Runtime error:\n%s" % traceback_string
        #return super(ProdTallinnNTuples, self).on_failure(exception)

    def output(self):
        tree_name = os.path.basename(self.branch_data).replace('_cfg.py', '.root')
        sample_name = os.path.basename(os.path.dirname(self.branch_data))
        output_dir = os.path.join(
            "produceNTuple",
            self.analysis,
            self.era)
        output_path = os.path.join(output_dir, f"{tree_name}")
        return self.local_target(output_path)

    def build_command(self):
        sample_name = os.path.basename(os.path.dirname(self.branch_data))
        tmp_output_dir = os.path.join(self.workDir.path, sample_name)
        os.makedirs(tmp_output_dir, exist_ok=True)
        cdCMD = f'cd {tmp_output_dir}'
        outFileName = self.output().path.split('/')[-1]
        outDirName = self.output().path.strip(outFileName)
        os.makedirs(outDirName, exist_ok=True)
        mvCMD = f"mv {outFileName} {outDirName}"
        cmd = " && ".join([cdCMD, "produceNtuple " + str(self.branch_data), mvCMD])
        return cmd


"""
 Task to create the configs for running TallinnNtuple jobs.
This is later to be done with the file catalog
"""
class TallinnAnalyzeConfigsForRegion(KBFIBaseTask, SlurmWorkflow, law.LocalWorkflow):
    default_store = "$ANALYSIS_CONFIG_PATH"
    analysis = luigi.Parameter(
        default='HH/multilepton',
        significant=False,
        description="analysis e.g. HH/multilepton",
    )

    era = luigi.Parameter(
        default='2018',
        significant=False,
        description="era e.g. 2018",
    )

    channel = luigi.Parameter(
        default='2lss_leq1tau',
        significant=False,
        description="channel e.g. 2lss_leq1tau",
    )

    mode = luigi.Parameter(
        default='default',
        significant=False,
        description="mode e.g. default",
    )

    region = luigi.Parameter(
        default='',
        significant=False,
        description="region e.g. 'SS_SR' or selection string",
    )

    analysis_region = luigi.Parameter(
        default='OS_SR',
        significant=True,
        description="OS_SR/OS_Fakable/SS_SR/SS_Fakable",
    )

    withSyst =  luigi.BoolParameter(
        default=True,
        significant=False,
        description="with or without systematics"
    )

    def workflow_requires(self):
        return {'ntuples':ProdTallinnNTuples.req(self, _prefer_cli=["workflow","version"])}

    def create_branch_map(self):
        branches = {}
        ntuples = self.input()['ntuples']['collection'].targets.items()
        to_run = ['central', 'CMS_ttHl_tauESUp']
        if not self.withSyst:
            to_run=['central']
        for branch, branchdata in ntuples:
            branches[branch]=[branchdata.path,self.analysis_region]
        outbranches={}
        for e, en in enumerate(to_run):
            for key in branches:
                outbranches[e*len(branches)+int(key)] = [branches[key][0],branches[key][1],en]
        return outbranches

    def output(self):
        ntuple = self.branch_data[0].split('/')[-1]
        target = self.branch_data[1] + "/config_analyze_{sampleandcategory}_{srorcr}_{systorshift}.py".format(sampleandcategory=ntuple.strip('ntuple_').strip('.root'),srorcr=self.branch_data[1],systorshift=self.branch_data[2])
        return self.local_target(target)

    def createConfig(self, prms):
        template = self.template[:]
        template = template.replace('INFILE',prms[0])
        outfilebase = prms[0].split('/')[-1].strip('ntuple_').strip('.root')+'_'+prms[1]
        template = template.replace('ANOUTFILE',outfilebase+'.root')
        template = template.replace('ANOUTFILE','rle_'+outfilebase+'.txt')
        template = template.replace('SYSTORSHIFT',prms[2])
        template = template.replace('PROCESS',outfilebase)
        if self.channel == '2lss_leq1tau':
            template = template.replace('CHANNELINPUTS','from TallinnAnalysis.HistogramTools.datacard_HH_2lss_cfi import datacard_HH_2lss_nonresonant as histograms_datacard_HH_2lss_nonresonant, datacard_HH_2lss_resonant_spin0 as histograms_datacard_HH_2lss_resonant_spin0, datacard_HH_2lss_resonant_spin2 as histograms_datacard_HH_2lss_resonant_spin2')
            sel = ''
            basesel = 'nlep == 2 && ntau == 1 && passesTrigger && lep1_pt > 25.  && lep1_tightCharge >= 2 && lep2_pt > 15.  && lep2_tightCharge >= 2  && tau1_pt > 20. && tau1_isTight && (njetAK4 >= 2 || njetAK8Wjj >= 1) && njetAK4bL <= 1 && njetAK4bM == 0 && (lep1_pdgId == 13 || lep2_pdgId == 13 || met_LD > 30.) && passesLowMassLeptonPairVeto && passesZbosonVeto && passesHtoZZto4lVeto && passesMEtFilters'
            if prms[1] == 'OS_SR':
                sel = basesel + "&& ntightlep == 2 && ntighttau == 1 && lep1_isTight && lep2_isTight &&lep1_charge*lep2_charge < 0"
            elif prms[1] == 'SS_SR':
                sel = basesel + "&& ntightlep == 2 && ntighttau == 1 && lep1_isTight && lep2_isTight && lep1_charge*lep2_charge > 0"
            elif prms[1] == 'OS_Fakable':
                sel = basesel + "&& lep1_isFakeable && lep2_isFakeable && tau1_isFakeable && !(lep1_isTight && lep2_isTight && tau1_isTight) &&ntightlep <= 2 && ntighttau <= 1 && lep1_charge*lep2_charge < 0"
            elif prms[1] == 'SS_Fakable':
                sel = basesel + "&& lep1_isFakeable && lep2_isFakeable && tau1_isFakeable && !(lep1_isTight && lep2_isTight && tau1_isTight) &&ntightlep <= 2 && ntighttau <= 1 && lep1_charge*lep2_charge > 0"
            template = template.replace('SELECTION', sel)
            template = template.replace('HISTPLUGINS','histograms_datacard_HH_2lss_nonresonant')
        else:
            raise NotImplementedError("channel not implemented %s"%(self.channel))
        return template

    def gethash(self):
        return 'ProdTallinnNTuples'+ str(law.util.create_hash(self.jobDicts, to_int=True)) + '_' +self.version

    def run(self):
        self.template = ""
        with open(str(os.getenv("ANALYSIS_PATH"))+'/templates/analyze_cfg.py')  as f:
            lines = f.readlines()
            for l in lines: self.template += l# + "\n"
        prms = self.branch_data
        config = self.createConfig(prms)
        output = self.output()
        output.dump(config,formatter='text')


class ProdTallinnAnalysisHistosForRegion(CommandTask, SlurmWorkflow, law.LocalWorkflow):
    default_store = "$ANALYSIS_ROOT_PATH"
    def __init__(self, *args, **kwargs):
        super(ProdTallinnAnalysisHistosForRegion, self).__init__(*args, **kwargs)

    analysis = luigi.Parameter(
        default='HH/multilepton',
        significant=False,
        description="analysis e.g. HH/multilepton",
    )

    era = luigi.Parameter(
        default='2018',
        significant=False,
        description="era e.g. 2018",
    )

    channel = luigi.Parameter(
        default='2lss_leq1tau',
        significant=False,
        description="channel e.g. 2lss_leq1tau",
    )

    mode = luigi.Parameter(
        default='default',
        significant=False,
        description="mode e.g. default",
    )

    region = luigi.Parameter(
        default='',
        significant=False,
        description="region e.g. 'SS_SR' or selection string",
    )

    analysis_region = luigi.Parameter(
        default='OS_SR',
        significant=True,
        description="OS_SR/OS_Fakable/SS_SR/SS_Fakable",
    )

    withSyst =  luigi.BoolParameter(
        default=True,
        significant=False,
        description="with or without systematics"
    )

    debug = luigi.BoolParameter(
        default=False,
        significant=False,
        description='Whether keep the temporary files'
    )

    def gethash(self):
        return 'ProdTallinnAnalysisHistosForRegion'+ str(law.util.create_hash(str(self.withSyst)  + self.analysis_region + self.channel + self.region + self.mode + self.analysis + self.era, to_int=True)) + '_'+ self.version

    @law.cached_workflow_property
    def workDir(self):
        workDirName = os.path.expandvars('$ANALYSIS_WORKAREA') + ('/tmp_' + self.gethash())
        workDir = law.LocalDirectoryTarget(workDirName)
        workDir.touch()
        return workDir

    def create_branch_map(self):
        branches = {}
        for branch, branchdata in self.input()['configs']['collection'].targets.items():
            branches[branch]=branchdata.path
        return branches

    def workflow_requires(self):
        return {'configs':TallinnAnalyzeConfigsForRegion.req(self,workflow='local',_prefer_cli=["workflow", "version","mode","region"])}

    def on_success(self):
        if self.is_workflow():
            shutil.rmtree(self.workDir.path)
            #os.rmdir(self.workDir.path)
            cleanDir = (os.path.expandvars("${ANALYSIS_LOGFILE_PATH}")+'/' +self.gethash()+'*.txt').strip(' ')
            logFileList = glob.glob(cleanDir)
            for f in logFileList:
                os.remove(f)
        #return super(ProdTallinnNTuples, self).on_success()

    def on_failure(self, exception):
        if self.is_workflow():
            cleanDir = (os.path.expandvars("${ANALYSIS_LOGFILE_PATH}")+'/' +self.task.gethash()+'*.txt').strip(' ')
            if not self.debug:
                shutil.rmtree(self.workDir.path)
                #os.rmdir(self.workDir.path)
                logFileList = glob.glob(cleanDir)
                for f in logFileList:
                    os.remove(f)
            else:
                print("Encountered error, preserving workdir (to be deleted manually) ", self.workDir.path)
                print("Encountered error, preserving logfiles (to be deleted manually) ", cleanDir)
        traceback_string = traceback.format_exc()
        return "Runtime error:\n%s" % traceback_string
        #return super(ProdTallinnNTuples, self).on_failure(exception)

    def output(self):\
        return self.local_target(self.branch_data.split('/')[-1][len('config_analyze_'):].replace('.py','.root'))

    def build_command(self):
        cdCMD = 'cd '+ self.workDir.path
        outFileName = self.output().path.split('/')[-1]
        outDirName = self.output().path.strip(outFileName)
        mvCMD = "mv *.root " + outFileName + " && mv " + outFileName + " " + outDirName 
        mvCMD2 = "mv *.json " + "rle_" + outFileName.replace('.root','.json') + " && mv " + "rle_" + outFileName.replace('root', 'json') + " " + outDirName
        #mvCMD ="mv "+ outFileName + " " + outDirName + "/" + outFileNameTarget
        #mvCMD2 ="mv "+ outFileNameJSON + " " + outDirName + "/rle_" + outFileNameTarget.replace('.root','.json')
        cmd = cdCMD + ' && ' + 'analyze '+ str(self.branch_data) + ' && ' + mvCMD  + ' && ' + mvCMD2
        return cmd


"""
 Task to create the configs for running TallinnNtuple jobs.
This is later to be done with the file catalog
"""
class ProdTallinnAnalysisHistos(law.WrapperTask):
    default_store = "$ANALYSIS_CONFIG_PATH"
    analysis = luigi.Parameter(
        default='HH/multilepton',
        significant=False,
        description="analysis e.g. HH/multilepton",
    )

    era = luigi.Parameter(
        default='2018',
        significant=False,
        description="era e.g. 2018",
    )

    channel = luigi.Parameter(
        default='2lss_leq1tau',
        significant=False,
        description="channel e.g. 2lss_leq1tau",
    )

    mode = luigi.Parameter(
        default='default',
        significant=False,
        description="mode e.g. default",
    )

    region = luigi.Parameter(
        default='',
        significant=False,
        description="region e.g. 'SS_SR' or selection string",
    )

    version = luigi.Parameter(
        default=None,
        significant=False,
        description="version",
    )

    workflow = luigi.Parameter(
        default="local",
        significant=False,
        description="local or slurm",
    )

    withSyst =  luigi.BoolParameter(
        default=True,
        significant=False,
        description="with or without systematics"
    )

    def requires(self):
        yield ProdTallinnAnalysisHistosForRegion(analysis_region='OS_SR', version=self.version, analysis=self.analysis, era=self.era, channel=self.channel, mode=self.mode, region=self.region, withSyst=self.withSyst, workflow=self.workflow)
        yield ProdTallinnAnalysisHistosForRegion(analysis_region='SS_SR', version=self.version, analysis=self.analysis, era=self.era, channel=self.channel, mode=self.mode, region=self.region, withSyst=self.withSyst, workflow=self.workflow)
        yield ProdTallinnAnalysisHistosForRegion(analysis_region='OS_Fakable', version=self.version, analysis=self.analysis, era=self.era, channel=self.channel, mode=self.mode, region=self.region, withSyst=self.withSyst, workflow=self.workflow)
        yield ProdTallinnAnalysisHistosForRegion(analysis_region='SS_Fakable',  version=self.version, analysis=self.analysis, era=self.era, channel=self.channel, mode=self.mode, region=self.region, withSyst=self.withSyst, workflow=self.workflow)
