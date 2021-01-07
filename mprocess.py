import logging
import subprocess
from io import StringIO
from multiprocessing import Pool

from schammer import utils


class MultiprocessingPool:

    class FunctionCall:

        def __init__(self, func, arg_tuple: tuple):

            assert callable(func), f'func must be a callable function'
            assert isinstance(arg_tuple, tuple), f'arg tuple must be a tuple'

            self.func = func
            self.arg_tuple = arg_tuple

    def __init__(self):

        self.function_call_list = list()

    def add_function_call(self, function_call) -> None:

        assert isinstance(function_call, self.FunctionCall), f'call must be a MultiprocessPool.Call'

        self.function_call_list.append(function_call)

    def run_processes(self, processes: int) -> None:

        assert isinstance(processes, int), f'processes must be an int, was a {type(processes)}'
        assert 0 < processes < 37, f'processes must be between 1 and 36 inclusive, was {processes}'

        # create pool
        pool = Pool(processes=processes)

        # dispatch function calls to the pool
        result_list = list()
        for function_call in self.function_call_list:

            result_list.append(
                pool.apply_async(func=_call_func_and_return_log_and_error_caught, kwds={'function_call': function_call})
            )

        # get results
        log_str_list = [result.get() for result in result_list]

        # log the logs and check for errors
        for log_str, error_caught in log_str_list:

            if log_str != '':
                logging.info(f' - multiprocessing function call completed - \n{log_str}')

            if error_caught:
                raise RuntimeError(f'multiprocessing function call caught an error')

        # close pool
        pool.close()


def _call_func_and_return_log_and_error_caught(function_call: MultiprocessingPool.FunctionCall) -> (str, bool):

    # setup stream logging
    log_stream = StringIO()
    utils.setup_string_stream_logging(log_stream=log_stream)

    error_caught = False
    try:

        # call function
        function_call.func(*function_call.arg_tuple)

    except Exception:  # error to be raised in calling function

        # log the error and raise a new error containing the log
        utils.log_last_error()
        error_caught = True

    # return the log
    return log_stream.getvalue().strip(), error_caught


class SubprocessPool:

    def __init__(self):

        self.cmd_list = list()

    def add_command(self, cmd: list) -> None:

        utils.check_types([('cmd', cmd, list)])
        utils.check_types([('cmd str', cs, str) for cs in cmd])
        assert len(cmd) > 0, f'cmd must not be empty'

        self.cmd_list.append(cmd)

    def add_list_of_commands(self, cmd_list: list) -> None:

        utils.check_types([('cmd_list', cmd_list, list)])

        for cmd in cmd_list:
            self.add_command(cmd=cmd)

    def run_processes(self, processes: int, shell: bool = False, output: bool = True, retries: int = 0) -> None:

        utils.check_types([('processes', processes, int), ('shell', shell, bool), ('output', output, bool),
                           ('retries', retries, int)])
        assert 0 < processes < 37, f'processes must be between 1 and 36 inclusive, was {processes}'
        assert self.cmd_list is not None, f'cmd list is None'
        assert 0 <= retries <= 30, f'retries must be an int from 0 to 30, was {retries}'

        class CMDTracker:

            def __init__(self, cmd: list):
                self.cmd = cmd
                self.retries = 0
                self.popen = None
                self.stdout_bytes = None

        # create command trackers and clear cmd list
        to_run_cmd_tracker_list = [CMDTracker(cmd=cmd) for cmd in self.cmd_list]
        self.cmd_list.clear()

        # start processes
        running_cmd_tracker_list = list()
        while len(to_run_cmd_tracker_list) > 0 or len(running_cmd_tracker_list) > 0:

            # start processes if we can and we need to
            while len(running_cmd_tracker_list) < processes and len(to_run_cmd_tracker_list) > 0:

                # take a cmd from the list
                cmd_tracker = to_run_cmd_tracker_list.pop()

                # start a subprocess
                if output:
                    stdout, stderr = (subprocess.PIPE, subprocess.STDOUT)
                else:
                    stdout, stderr = (subprocess.DEVNULL, subprocess.DEVNULL)

                cmd = cmd_tracker.cmd if not shell else ' '.join(cmd_tracker.cmd)

                cmd_tracker.popen = subprocess.Popen(
                    args=cmd,
                    stdout=stdout,
                    stderr=stderr,
                    shell=shell
                )

                running_cmd_tracker_list.append(cmd_tracker)

            # determine which processes have completed
            still_running_cmd_tracker_list = list()
            completed_cmd_tracker_list = list()
            for cmd_tracker in running_cmd_tracker_list:

                try:
                    stdout_data, stderr_data = cmd_tracker.popen.communicate(timeout=1)

                    cmd_tracker.stdout_bytes = stdout_data

                    completed_cmd_tracker_list.append(cmd_tracker)

                except subprocess.TimeoutExpired:

                    still_running_cmd_tracker_list.append(cmd_tracker)

            running_cmd_tracker_list = still_running_cmd_tracker_list

            # check completed popens
            def get_std_out_from_tracker(_cmd_tracker):

                try:
                    _stdout_str = cmd_tracker.stdout_bytes.decode().strip()

                except UnicodeDecodeError:
                    _stdout_str = ''

                return '\n' + _stdout_str

            for cmd_tracker in completed_cmd_tracker_list:

                # check for errors
                if cmd_tracker.popen.returncode != 0:

                    # raise error if we have reached our retry limit
                    if cmd_tracker.retries == retries:

                        stdout_str = get_std_out_from_tracker(cmd_tracker)

                        logging.info(f' - subprocess exited with errors - {stdout_str}')

                        raise RuntimeError(f'subprocess exited with a non-zero return code and retries limit reached, '
                                           f'{cmd_tracker.popen.returncode}')

                    else:

                        # update the tracker
                        cmd_tracker.retries += 1
                        cmd_tracker.popen = None

                        to_run_cmd_tracker_list.append(cmd_tracker)

                else:

                    # log output
                    if output:

                        stdout_str = get_std_out_from_tracker(cmd_tracker)

                        logging.info(f' - subprocess completed - {stdout_str}')
