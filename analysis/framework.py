# coding: utf-8

"""
Law example tasks to demonstrate Slurm workflows at the Desy Maxwell cluster.

In this file, some really basic tasks are defined that can be inherited by
other tasks to receive the same features. This is usually called "framework"
and only needs to be defined once per user / group / etc.
"""


import os

import luigi
import law
import six
from collections import OrderedDict, defaultdict

# the slurm workflow implementation is part of a law contrib package
# so we need to explicitly load it
law.contrib.load("slurm")


class Task(law.Task):
    """
    Base task that we use to force a version parameter on all inheriting tasks, and that provides
    some convenience methods to create local file and directory targets at the default data path.
    """

    version = luigi.Parameter()

    def store_parts(self):
        return (self.__class__.__name__, self.version)

    def local_path(self, *path):
        # ANALYSIS_DATA_PATH is defined in setup.sh
        parts = (os.getenv("ANALYSIS_DATA_PATH"),) + self.store_parts() + path
        return os.path.join(*parts)

    def local_target(self, *path):
        return law.LocalFileTarget(self.local_path(*path))

class SlurmWorkflowProxy(law.slurm.SlurmWorkflowProxy):
    def create_job_file(self, job_num, branches):
        out = super(SlurmWorkflowProxy, self).create_job_file(job_num, branches)
        out['log'] =  os.path.expandvars("$ANALYSIS_LOGFILE_PATH")+'/' +self.task.gethash() +out['log'].split('/')[-1]
        return out

class SlurmWorkflow(law.slurm.SlurmWorkflow):
    """
    Batch systems are typically very heterogeneous by design, and so is Slurm. Law does not aim
    to "magically" adapt to all possible Slurm setups which would certainly end in a mess.
    Therefore we have to configure the base Slurm workflow in law.contrib.slurm to work with
    the Maxwell cluster environment. In most cases, like in this example, only a minimal amount of
    configuration is required.
    """
    workflow_proxy_cls = SlurmWorkflowProxy

    def gethash(self):
        return '12345678'

    transfer_logs = luigi.BoolParameter(
        default=True,
        significant=False,
        description="transfer job logs to the output directory; default: True",
    )

    slurm_partition = luigi.Parameter(
        default="main",
        significant=False,
        description="target queue partition; default: main",
    )
    max_runtime = law.DurationParameter(
        default=1.0,
        unit="h",
        significant=False,
        description="the maximum job runtime; default unit is hours; default: 1h",
    )

    slurm_blacklist_nodes = luigi.Parameter(
        default=None,
        significant=False,
        description="blacklisted nodes",
    )

    def on_success(self):
        print("BLACKLISTED NODES:", self.slurm_blacklist_nodes)
        super(SlurmWorkflow, self).on_success()

    def on_failure(self, exception):
        print("BLACKLISTED NODES:", self.slurm_blacklist_nodes)
        super(SlurmWorkflow, self).on_failure(exception)

    def failureLog_callback(self, log_file, taskRef):
        if log_file:
            with open(log_file)  as f:
                lines = f.readlines()
                lasthost = ''
                lasterror = ''
                for l in lines:
                    if 'Static hostname:' in l: lasthost=l[l.find('comp'):].split('.')[0]
                    if 'exit code' in l:
                        if 'exit code:' in l:
                            lasterror = int(l[l.find('exit code:')+len('exit code:'):])
                        elif 'exit code :' in l:
                            lasterror = int(l[l.find('exit code :')+len('exit code :'):])
                        else:
                            lasterror = ''
                        if lasterror is 21 or lasterror is 23 or lasterror: # cvms or bus
                            print('backlisting node!')
                            if taskRef.slurm_blacklist_nodes:
                                if lasthost not in taskRef.slurm_blacklist_nodes:
                                    taskRef.slurm_blacklist_nodes += (',' + lasthost)
                            else:
                                taskRef.slurm_blacklist_nodes = lasthost
        return super(SlurmWorkflow, self).failureLog_callback(log_file, taskRef)

    def slurm_output_directory(self):
        # the directory where submission meta data should be stored
        return law.LocalDirectoryTarget(self.local_path())

    def slurm_bootstrap_file(self):
        # each job can define a bootstrap file that is executed prior to the actual job
        # in order to setup software and environment variables
        return law.util.rel_path(__file__, "bootstrap.sh")

    def slurm_job_config(self, config, job_num, branches):
        # render_variables are rendered into all files sent with a job
        config.render_variables["analysis_path"] = os.getenv("ANALYSIS_PATH")

        # useful defaults
        job_time = law.util.human_duration(
            seconds=law.util.parse_duration(self.max_runtime, input_unit="h") - 1,
            colon_format=True,
        )
        config.custom_content.append(("time", job_time))
        config.custom_content.append(("nodes", 1))
        print(config)
        if self.slurm_blacklist_nodes:
            config.custom_content.append(("exclude", self.slurm_blacklist_nodes))

        return config
