# coding: utf-8

"""
Law example tasks to demonstrate workflows using HTCondor at CERN.

In this file, some really basic tasks are defined that can be inherited by other tasks to receive
the same features. This is usually called "framework" and only needs to be defined once per
user / group / etc.
"""

import os
import math

import luigi  # type: ignore
import law  # type: ignore

from typing import Any, Callable

law.contrib.load("htcondor")
law.contrib.load("git")

class Task(law.Task):
    """
    Base task that we use to force a version parameter on all inheriting tasks, and that provides
    some convenience methods to create local file and directory targets at the default data path.
    """

    task_namespace = ""
    output_collection_cls = law.SiblingFileCollection

    version = luigi.Parameter()

    def store_parts(self):
        parts = law.util.InsertableDict()

        parts["task_family"] = self.task_family
        if self.version is not None:
            parts["version"] = self.version

        return parts

    def local_path(self, *path, store_dir="$LCT_STORE_DIR"):
        parts = (store_dir,) + tuple(self.store_parts().values()) + path
        return os.path.join(*map(str, parts))

    def local_target(self, *path, dir=False):
        cls = law.LocalDirectoryTarget if dir else law.LocalFileTarget
        return cls(self.local_path(*path))

    def remote_path(self, *path):
        parts = tuple(self.store_parts().values()) + path
        return os.path.join(*map(str, parts))

    def remote_target(self, *path, dir=False):
        cls = law.wlcg.WLCGDirectoryTarget if dir else law.wlcg.WLCGFileTarget
        return cls(self.remote_path(*path))


class PlotTask(Task):

    view_cmd = luigi.Parameter(
        default=law.NO_STR,
        significant=False,
        description="a command to execute after the task has run to visualize plots right in the "
        "terminal; no default",
    )


class HTCondorWorkflow(law.htcondor.HTCondorWorkflow):
    """
    Batch systems are typically very heterogeneous by design, and so is HTCondor. Law does not aim
    to "magically" adapt to all possible HTCondor setups which would certainly end in a mess.
    Therefore we have to configure the base HTCondor workflow in law.contrib.htcondor to work with
    the CERN HTCondor environment. In most cases, like in this example, only a minimal amount of
    configuration is required.
    """

    # example for a parameter whose value is propagated to the htcondor job configuration
    htcondor_runtime = law.DurationParameter(
        default=0.5,
        unit="h",
        significant=False,
        description="maximum runtime; default unit is hours; default: 0.5",
    )
    transfer_logs = luigi.BoolParameter(
        default=True,
        significant=False,
        description="transfer job logs to the output directory; default: True",
    )

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # keep a reference to the BundleRepo requirement to avoid redundant checksum calculations
        if getattr(self, "bundle_repo_req", None) is None:
            self.bundle_repo_req = BundleRepo.req(self)

    def htcondor_workflow_requires(self):
        # definition of requirements for the htcondor workflow to start
        reqs = super().htcondor_workflow_requires()

        # add repo and software bundling as requirements
        reqs["repo"] = self.bundle_repo_req

        return reqs

    def htcondor_output_directory(self):
        # the directory where submission meta data should be stored
        return law.LocalDirectoryTarget(self.local_path(store_dir="$LCT_STORE_DIR_AFS"))

    def htcondor_log_directory(self):
        # the directory where job logs should be stored
        return law.LocalDirectoryTarget(self.local_path(store_dir="$LCT_STORE_DIR_AFS"))

    def htcondor_bootstrap_file(self):
        # each job can define a bootstrap file that is executed prior to the actual job
        # configure it to be shared across jobs and rendered as part of the job itself
        bootstrap_file = law.util.rel_path(__file__, "remote_bootstrap.sh")
        return law.JobInputFile(bootstrap_file, copy=False, render_job=True)

    def htcondor_job_config(self, config, job_num, branches):
        # send the voms proxy file with jobs
        vomsproxy_file = law.wlcg.get_vomsproxy_file()
        if not law.wlcg.check_vomsproxy_validity(proxy_file=vomsproxy_file):
            raise Exception("voms proxy not valid, submission aborted")
        config.input_files["vomsproxy_file"] = law.JobInputFile(
            vomsproxy_file,
            share=True,
            render=False,
        )

        # add wlcg tools
        config.input_files["wlcg_tools"] = law.JobInputFile(
            law.util.law_src_path("contrib/wlcg/scripts/law_wlcg_tools.sh"),
            copy=True,
            render=False,
        )

        # helper to return uris and a file pattern for replicated bundles
        reqs = self.htcondor_workflow_requires()
        def get_bundle_info(task):
            uris = task.output().dir.uri(base_name="filecopy", return_all=True)
            pattern = os.path.basename(task.get_file_pattern())
            return ",".join(uris), pattern

        # repo bundle variables
        uris, pattern = get_bundle_info(reqs["repo"])
        config.render_variables["lct_repo_uris"] = uris
        config.render_variables["lct_repo_pattern"] = pattern

        # render_variables are rendered into all files sent with a job
        config.render_variables["bootstrap_name"] = "htcondor"
        config.render_variables["lct_user"] = os.getenv("LCT_USER")
        config.render_variables["lct_data_dir"] = os.getenv("LCT_DATA_DIR")
        config.render_variables["lct_store_dir_afs"] = os.getenv("LCT_STORE_DIR_AFS")
        config.render_variables["lct_store_dir_eos"] = os.getenv("LCT_STORE_DIR_EOS")
        config.render_variables["lct_store_dir"] = os.getenv("LCT_STORE_DIR")
        config.render_variables["lct_software_dir"] = os.getenv("LCT_SOFTWARE_DIR")

        # force to run on el9, http://batchdocs.web.cern.ch/batchdocs/local/submit.html#os-choice
        config.custom_content.append(("MY.WantOS", "el9"))

        # maximum runtime
        max_runtime = int(math.floor(self.htcondor_runtime * 3600)) - 1
        config.custom_content.append(("+MaxRuntime", max_runtime))
        config.custom_content.append(("+RequestRuntime", max_runtime))

        # the CERN htcondor setup requires a "log" config, but we can safely set it to /dev/null
        # if you are interested in the logs of the batch system itself, set a meaningful value here
        config.custom_content.append(("log", "/dev/null"))

        return config


class BundleRepo(Task, law.git.BundleGitRepository, law.tasks.TransferLocalFile):
    """
    This task is needed by the CrabWorkflow above as it bundles the example repository and uploads
    it to a remote storage where crab jobs can access it. Each job then fetches and unpacks a bundle
    to be able to access your code before the actual payload commences.
    """

    replicas = luigi.IntParameter(
        default=5,
        description="number of replicas to generate; default: 5",
    )
    version = None  # type: ignore[assignment]

    exclude_files = ["data", ".law"]

    def get_repo_path(self):  # required by BundleGitRepository
        # location of the repository to bundle
        return os.environ["LCT_DIR"]

    def single_output(self):  # required by TransferLocalFile
        # single output target definition, might be used to infer names and locations of replicas
        repo_base = os.path.basename(self.get_repo_path())
        return self.remote_target(f"{repo_base}.{self.checksum}.tgz")

    def get_file_pattern(self):
        # returns a pattern (format "{}") into which the replica number can be injected
        path = os.path.expandvars(os.path.expanduser(self.single_output().path))
        return self.get_replicated_path(path, i=None if self.replicas <= 0 else r"[^\.]+")

    def output(self):  # both BundleGitRepository and TransferLocalFile define an output, so overwrite
        # the actual output definition, simply using what TransferLocalFile outputs
        return law.tasks.TransferLocalFile.output(self)

    @law.decorator.log
    @law.decorator.safe_output
    def run(self):
        # create the bundle
        bundle = law.LocalFileTarget(is_tmp="tgz")
        self.bundle(bundle)  # method of BundleGitRepository

        # log the size
        self.publish_message(f"size is {law.util.human_bytes(bundle.stat().st_size, fmt=True)}")

        # transfer the bundle
        self.transfer(bundle)  # method of TransferLocalFile


@law.decorator.factory(accept_generator=True)
def view_output_plots(
    fn: Callable,
    opts: Any,
    task: law.Task,
    *args: Any,
    **kwargs: Any,
) -> tuple[Callable, Callable, Callable]:
    """ view_output_plots()
    This decorator is used to view the output plots of a task. It checks if the task has a view command, collects all
    the paths of the output files, and then opens each file using the view command.

    :param fn: The decorated function.
    :param opts: Options for the decorator.
    :param task: The task instance.
    :param args: Arguments to be passed to the function call.
    :param kwargs: Keyword arguments to be passed to the function call.
    :return: A tuple containing the before_call, call, and after_call functions.
    """
    def before_call() -> None:
        return None

    def call(state: Any) -> Any:
        return fn(task, *args, **kwargs)

    def after_call(state: Any) -> None:
        view_cmd = getattr(task, "view_cmd", None)
        if not view_cmd or view_cmd == law.NO_STR:
            return

        # prepare the view command
        if "{}" not in view_cmd:
            view_cmd += " {}"

        # collect all paths to view
        seen: set[str] = set()
        outputs: list[Any] = law.util.flatten(task.output())
        while outputs:
            output = outputs.pop(0)
            if isinstance(output, law.TargetCollection):
                outputs.extend(output._flat_target_list)
                continue
            if not getattr(output, "abspath", None):
                continue
            path = output.abspath
            if not path.endswith((".pdf", ".png")):
                continue
            if path in seen:
                continue
            seen.add(path)
            task.publish_message(f"showing {path}")
            with output.localize("r") as tmp:
                law.util.interruptable_popen(
                    view_cmd.format(tmp.abspath),
                    shell=True,
                    executable="/bin/bash",
                )

    return before_call, call, after_call
