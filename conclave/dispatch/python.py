from subprocess import call

from conclave.dispatch import Dispatcher


class PythonDispatcher(Dispatcher):
    """ Dispatches Python jobs. """

    def dispatch(self, job):

        cmd = "{}/workflow.py".format(job.code_dir)

        print("\nDispatching python job: {}".format(job.name))
        print("{}: {}/workflow.py running"
              .format(job.name, job.code_dir))

        try:
            call(["python", cmd])
        except Exception as e:
            print(e)
