# coding: utf-8

"""
Law example tasks to demonstrate Slurm workflows at the Desy Maxwell cluster.

The actual payload of the tasks is rather trivial.
"""
import six
import law
import os
import luigi
import glob
from datetime import datetime
from collections import OrderedDict
from analysis.framework import Task


"""
Basic task class copied from hh-inference to handle custom file locations
"""
class KBFIBaseTask(Task):
    version = luigi.Parameter(
        description="mandatory version that is encoded into output paths")
    output_collection_cls = law.SiblingFileCollection
    default_store = "$ANALYSIS_DATA_PATH"
    store_by_family = False

    @classmethod
    def modify_param_values(cls, params):
        return params

    @classmethod
    def req_params(cls, inst, **kwargs):
        # always prefer certain parameters given as task family parameters (--TaskFamily-parameter)
        _prefer_cli = law.util.make_list(kwargs.get("_prefer_cli", []))
        if "version" not in _prefer_cli:
            _prefer_cli.append("version")
        kwargs["_prefer_cli"] = set(_prefer_cli) | cls.prefer_params_cli

        return super(KBFIBaseTask, cls).req_params(inst, **kwargs)

    def __init__(self, *args, **kwargs):
        super(KBFIBaseTask, self).__init__(*args, **kwargs)

        # generic hook to change task parameters in a customizable way
        #self.call_hook("init_analysis_task")

    def store_parts(self):
        parts = OrderedDict()
        parts["task_class"] = self.task_family if self.store_by_family else self.__class__.__name__
        return parts

    def store_parts_ext(self):
        parts = OrderedDict()
        if self.version is not None:
            parts["version"] = self.version
        return parts

    def local_path(self, *path, **kwargs):
        store = kwargs.get("store") or self.default_store
        parts = tuple(self.store_parts().values()) + tuple(self.store_parts_ext().values()) + path
        return os.path.join(store, *(str(p) for p in parts))

    def local_target(self, *path, **kwargs):
        cls = law.LocalFileTarget if not kwargs.pop("dir", False) else law.LocalDirectoryTarget
        store = kwargs.pop("store", None)
        path = self.local_path(*path, store=store)
        return cls(path, **kwargs)

    def join_postfix(self, parts, sep1="__", sep2="_"):
        def repl(s):
            # replace certain characters
            s = str(s).replace("*", "X").replace("?", "Y")
            # remove remaining unknown characters
            s = re.sub(r"[^a-zA-Z0-9\.\_\-\+]", "", s)
            return s

        return sep1.join(
            (sep2.join(repl(p) for p in part) if isinstance(part, (list, tuple)) else repl(part))
            for part in parts
            if (isinstance(part, int) or part)
        )

    #def call_hook(self, name, **kwargs):
    #    return call_hook(name, self, **kwargs)

    def _repr_params(self, *args, **kwargs):
        params = super(KBFIBaseTask, self)._repr_params(*args, **kwargs)

        # remove empty params by default
        for key, value in list(params.items()):
            if not value and value != 0:
                del params[key]

        return params

    def _print_command(self, args):
        max_depth = int(args[0])

        print("print task commands with max_depth {}".format(max_depth))

        for dep, _, depth in self.walk_deps(max_depth=max_depth, order="pre"):
            offset = depth * ("|" + law.task.interactive.ind)
            print(offset)

            print("{}> {}".format(offset, dep.repr(color=True)))
            offset += "|" + law.task.interactive.ind

            if isinstance(dep, CommandTask):
                # when dep is a workflow, take the first branch
                text = law.util.colored("command", style="bright")
                if isinstance(dep, law.BaseWorkflow) and dep.is_workflow():
                    dep = dep.as_branch(0)
                    text += " (from branch {})".format(law.util.colored("0", "red"))
                text += ": "

                cmd = dep.get_command()
                if cmd:
                    # when cmd is a 2-tuple, i.e. the real command and a representation for printing
                    # pick the second one
                    if isinstance(cmd, tuple) and len(cmd) == 2:
                        cmd = cmd[1]
                    else:
                        if isinstance(cmd, list):
                            cmd = law.util.quote_cmd(cmd)
                        # defaut highlighting
                        cmd = law.util.colored(cmd, "cyan")
                    text += cmd
                else:
                    text += law.util.colored("empty", "red")
                print(offset + text)
            else:
                print(offset + law.util.colored("not a CommandTask", "yellow"))


class CommandTask(KBFIBaseTask):
    """
    A task that provides convenience methods to work with shell commands, i.e., printing them on the
    command line and executing them with error handling.
    """

    custom_args = luigi.Parameter(
        default="",
        description="custom arguments that are forwarded to the underlying command; they might not "
        "be encoded into output file paths; no default",
    )

    exclude_index = True
    exclude_params_req = {"custom_args"}

    # by default, do not run in a tmp dir
    run_command_in_tmp = False

    # # by default, do not cleanup tmp dirs on error, except when running as a remote job
    # cleanup_tmp_on_error = False

    def build_command(self):
        # this method should build and return the command to run
        raise NotImplementedError

    def get_command(self):
        # this method is returning the actual, possibly cleaned command
        return self.build_command()

    def touch_output_dirs(self):
        # keep track of created uris so we can avoid creating them twice
        handled_parent_uris = set()

        for outp in law.util.flatten(self.output()):
            # get the parent directory target
            parent = None
            if isinstance(outp, law.SiblingFileCollection):
                parent = outp.dir
            elif isinstance(outp, law.FileSystemFileTarget):
                parent = outp.parent

            # create it
            if parent and parent.uri() not in handled_parent_uris:
                parent.touch()
                handled_parent_uris.add(parent.uri())

    def run_command(self, cmd, highlighted_cmd=None, optional=False, **kwargs):
        # proper command encoding
        cmd = (law.util.quote_cmd(cmd) if isinstance(cmd, (list, tuple)) else cmd).strip()

        # default highlighted command
        if not highlighted_cmd:
            highlighted_cmd = law.util.colored(cmd, "cyan")

        # when no cwd was set and run_command_in_tmp is True, create a tmp dir
        tmp_dir = None
        if "cwd" not in kwargs and self.run_command_in_tmp:
            tmp_dir = law.LocalDirectoryTarget(is_tmp=True)
            tmp_dir.touch()
            kwargs["cwd"] = tmp_dir.path
        self.publish_message("cwd: {}".format(kwargs.get("cwd", os.getcwd())))

        # call it
        print(highlighted_cmd, cmd)
        with self.publish_step("running '{}' ...".format(highlighted_cmd)):
            p, lines = law.util.readable_popen(cmd, shell=True, executable="/bin/bash", **kwargs)

        # raise an exception when the call failed and optional is not True
        if p.returncode != 0 and not optional:
            # when requested, make the tmp_dir non-temporary to allow for checks later on
            if tmp_dir and not self.cleanup_tmp_on_error:
                tmp_dir.is_tmp = False

            # raise exception
            msg = "command execution failed"
            msg += "\nexit code: {}".format(p.returncode)
            msg += "\ncwd      : {}".format(kwargs.get("cwd", os.getcwd()))
            msg += "\ncommand  : {}".format(cmd)
            raise Exception(msg)

        return p

    @law.decorator.log
    @law.decorator.notify
    def run(self, **kwargs):
        self.pre_run_command()

        # default run implementation
        # first, create all output directories
        self.touch_output_dirs()

        # get the command
        cmd = self.get_command()
        if isinstance(cmd, tuple) and len(cmd) == 2:
            kwargs["highlighted_cmd"] = cmd[1]
            cmd = cmd[0]

        # run it
        self.run_command(cmd, **kwargs)

        self.post_run_command()

    def pre_run_command(self):
        return

    def post_run_command(self):
        return
