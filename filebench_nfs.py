import os
import sys
import copy
import _thread
import time
from subprocess import Popen, PIPE


host_num = [1, 4, 8]

common_config = {
    'workspace': {'pm': '/pmfs'},
    'nthreads': {'1': 1},
    'sync': {'sync': ',dsync', 'async': ''}
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
        print("prepared: " + str(count))
        time.sleep(1)
    time.sleep(10)
    os.system(f"parallel-ssh {get_hosts(hosts)} 'pkill -CONT filebench'")


def run(hosts, path_out, path_err, cmd):
    _thread.start_new_thread(check, (hosts,))
    client_instr = f"parallel-ssh {get_hosts(hosts)} -t 0 -o {path_out} -e {path_err} '{cmd}'"
    print(client_instr)
    os.system(client_instr)


def filebench_nfs(hosts, filebench_dir):
    run_py = os.path.join(filebench_dir, 'run.py')
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


if __name__ == '__main__':
    args = sys.argv
    result_path = args[1]
    client_filebench_dir = args[2]

    with open("hosts", "r") as f:
        hosts = [x.strip() for x in f.readlines()]
    filebench_nfs(hosts, client_filebench_dir)

