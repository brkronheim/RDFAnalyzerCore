# tasks.py
import law
import os

law.contrib.load("htcondor")

class CreateFile(law.Task):

    filename = law.Parameter()

    def output(self):
        return law.LocalFileTarget("/eos/user/b/bkronhei/law/" + self.filename)

    def run(self):
        print(self.output().path)
        



class ProcessFile(law.LocalWorkflow):#law.htcondor.HTCondorWorkflow):

    input_file = law.Parameter()
    

    def workflow_requires(self):
        pass
        #return CreateFile(filename=self.input_file)

    def requires(self):
        pass

    def store_parts(self):
        parts = law.util.InsertableDict()

        parts["task_family"] = self.task_family
        #if self.version is not None:
        #    parts["version"] = self.version

        return parts

    def local_path(self, *path, store_dir="/eos/user/b/bkronhei/law/"):
        parts = (store_dir,) + tuple(self.store_parts().values()) + path
        return os.path.join(*map(str, parts))

    """
    def htcondor_output_directory(self):
        # the directory where submission meta data should be stored
        return law.LocalDirectoryTarget(self.local_path(store_dir="/eos/user/b/bkronhei/law/"))

    def htcondor_log_directory(self):
        # the directory where job logs should be stored
        return law.LocalDirectoryTarget(self.local_path(store_dir="/eos/user/b/bkronhei/law/"))
    """
    
    
    # parse config file once
    def read_config(self):

       with open(self.input_file) as f:
           entries = [line.strip() for line in f if line.strip()]

       return entries
    

    # THIS defines number of chunks dynamically
    def create_branch_map(self):

       entries = self.read_config()

       # branch_map maps branch number → payload
       return {
           i: entry
           for i, entry in enumerate(entries)
        }
    
    def output(self):
        return law.LocalFileTarget("/eos/user/b/bkronhei/law/" + f"config_{self.branch}.txt")

    def run(self):

        self.output().touch()


    """
    # access entry for this branch
    @property
    def entry(self):
        return self.branch_data

    def output(self):

        filename = self.entry.replace(".root", "_output.root")

        return law.LocalFileTarget(f"output/{filename}")

    def run(self):

        input_file = self.entry
        output_file = self.output().path

        self.run_cmd(
            f"./my_processor --input {input_file} --output {output_file}"
        )
    """
          
           
