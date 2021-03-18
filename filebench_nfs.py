import os
import sys
import copy
import re
import _thread
import time
import shutil
from pathlib import Path
from subprocess import Popen, PIPE


host_num = [1, 4, 8]

common_config = {
    'workspace': {'pm': '/pmfs'},
    'nthreads': {'1': 1},
    'sync': {'sync': ',dsync'}
}

files_number_per_thread = {
    'copyfiles': 30000,
    'fileserver': 10000,
    'mongo': 30000,
    'webserver': 1000,
    'varmail': 1000
}

configs = {
    'copyfiles': {
        **common_config,
        'runtime': {'2s': 2}
    },
    'fileserver': {
        **common_config,
        'runtime': {'60s': 60}
    },
    'mongo': {
        **common_config,
        'runtime': {'2s': 2}
    },
    'webserver': {
        **common_config,
        'runtime': {'60s': 60}
    },
    'varmail': {
        **common_config,
        'runtime': {'60s': 60}
    }
}


def args_generator():
    for workload, param in configs.items():
        conditions = []
        for param_name, options in param.items():
            if len(conditions) == 0:
                for opt_key, opt_value in options.items():
                    conditions.append({'friendly': {param_name: opt_key}, 'options': {param_name: opt_value}})
            else:
                conditions_new = []
                for condition in conditions:
                    for opt_key, opt_value in options.items():
                        condition_new = {}
                        condition_new['friendly'] = copy.deepcopy(condition['friendly'])
                        condition_new['friendly'][param_name] = opt_key
                        condition_new['options'] = copy.deepcopy(condition['options'])
                        condition_new['options'][param_name] = opt_value
                        conditions_new.append(condition_new)
                conditions = conditions_new

        for condition in conditions:
            condition['friendly']['nfiles'] = f"{condition['options']['nthreads'] * files_number_per_thread[workload]}"
            condition['options']['nfiles'] = condition['options']['nthreads'] * files_number_per_thread[workload]

        for condition in conditions:
            print('run filebench:')
            print(workload, condition['friendly'])
            yield workload, condition


def get_hosts(hosts):
    return " -H " + " -H ".join(hosts)


def check(hosts):
    count = 0
    while count < len(hosts):
        p = Popen(f'parallel-ssh {get_hosts(hosts)} -t 0 -P "ps -aux | grep filebench"', shell=True, stdout=PIPE, stderr=PIPE)
        out, err = p.communicate()

        result = out.decode()
        count = 0
        index = result.find('T')
        while index != -1:
            count += 1
            result = result[index+1:]
            index = result.find('T')
        print(f"prepared: {count} of {len(hosts)}")
        time.sleep(1)
    os.system(f"parallel-ssh {get_hosts(hosts)} -P 'pkill -CONT filebench;date +%s.%N'")


def run(hosts, path_out, path_err, cmd):
    _thread.start_new_thread(check, (hosts,))
    client_instr = f"parallel-ssh {get_hosts(hosts)} -t 0 -o {path_out} -e {path_err} '{cmd}'"
    print(client_instr)
    os.system(client_instr)


def filebench_nfs(hosts, filebench_dir):
    run_py = os.path.join(filebench_dir, 'run.py')
    if os.path.exists(result_path):
        shutil.rmtree(result_path)
    os.system(f"mkdir {result_path}")

    for workload, condition in args_generator():
        shorten_name = workload
        for v in condition['friendly'].values():
            shorten_name += '_' + v
        path1 = os.path.join(result_path, shorten_name)
        os.system(f"mkdir {path1}")


        cmd = f"python3 {run_py} {workload} {condition['options']['workspace']} {condition['options']['nthreads']} {condition['options']['runtime']} {condition['options']['nfiles']} {condition['options']['sync']}"

        for num in host_num:
            path2 = os.path.join(path1, f"{num}client")
            os.system(f"mkdir {path2}")

            path_out = os.path.join(path2, 'out')
            path_err = os.path.join(path2, 'err')
            os.system(f"mkdir {path_out}")
            os.system(f"mkdir {path_err}")
            run(hosts[:num], path_out, path_err, cmd)


def disbale_randomize_va_space():
    print('disbale_randomize_va_space')
    cmd = 'echo 0 > /proc/sys/kernel/randomize_va_space'
    client_instr = f"parallel-ssh -h hosts -t 0 '{cmd}'"
    os.system(client_instr)


def parse_result(results_path):
    data_regex = re.compile(r'^[\d\.]+:\s+IO\s+Summary:\s+(\d+)\s+ops\s+([\d\.]+)\s+ops/s\s+(\d+)/(\d+)\s+rd/wr\s+([\d\.]+)mb/s\s([\d\.]+)ms/op')
    duration_regex = re.compile(r'^[\d\.]+:\s+Run took (\d+) seconds...')
    results_dir = Path(results_path)
    output_file = Path('test_results.csv')

    with output_file.open('w') as output:
        output.write(',ops,ops/s,rd,wr,mb/s,ms/op\n')
        for workload in results_dir.iterdir():
            name = workload.name
            for num in host_num:
                outs = workload.joinpath(f'{num}client').joinpath('out')
                ops, ops_s, rd, wr, mb_s = 0, 0, 0, 0, 0
                duration = 0
                for file in outs.iterdir():
                    with file.open() as f:
                        line = f.readline()
                        while line:
                            duration_matcher = duration_regex.match(line)
                            if duration_matcher:
                                duration = int(duration_matcher.groups()[0])
                            data_matcher = data_regex.match(line)
                            if data_matcher:
                                groups = data_matcher.groups()
                                ops += float(groups[0])
                                ops_s += float(groups[1])
                                rd += float(groups[2])
                                wr += float(groups[3])
                                mb_s += float(groups[4])
                                break
                            line = f.readline()
                        else:
                            raise Exception(f"no result in file {file.name}")

                output.write(f'{name}_{num},{ops},{ops_s},{rd},{wr},{mb_s},{duration/1000/ops}\n')


if __name__ == '__main__':
    args = sys.argv
    result_path = 'result_path'
    client_filebench_dir = args[1]

    disbale_randomize_va_space()

    with open("hosts", "r") as f:
        hosts = [x.strip() for x in f.readlines()]
    filebench_nfs(hosts, client_filebench_dir)

    parse_result(result_path)

