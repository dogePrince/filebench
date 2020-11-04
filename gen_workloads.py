from jinja2 import Template
import os
import copy

ssd_path = '/root/file_tmp'
pm_path = '/pmfs/file_tmp'

common_config = {
    'workspace': {'pm': pm_path},
    'nthreads': {1: 1},
    'sync': {'sync': ',dsync'}
}

configs = {
    'copyfiles': {
        **common_config,
        'runtime': {'2s': 2},
        'nfiles': {'20kf': 20000}
    },
    'fileserver': {
        **common_config,
        'runtime': {'10s': 10},
        'nfiles': {'10kf': 10000}
    },
    'mongo': {
        **common_config,
        'runtime': {'2s': 2},
        'nfiles': {'20kf': 20000}
    },
    'netsfs': {
        **common_config,
        'runtime': {'10s': 10},
        'nfiles': {'10kf': 10000}
    },
    'webserver': {
        **common_config,
        'runtime': {'10s': 10},
        'nfiles': {'1kf': 1000}
    }
}

temp_dir = 'workloads_templates'
res_dir = 'special_workloads'
if not os.path.exists(res_dir):
    os.mkdir(res_dir)

for workload, param in configs.items():
    with open(os.path.join(temp_dir, workload)) as f:
        contents = f.read()
        t = Template(contents, variable_start_string='{^', variable_end_string='^}')

        conditions = []
        param_list = list(param.items())
        param_list.sort(key=lambda x: x[0])
        for param_name, options in param_list:
            if len(conditions) == 0:
                for opt_key, opt_value in options.items():
                    conditions.append({'postfix': '_' + str(opt_key), 'options': {param_name: opt_value}})
                # print(conditions)
            else:
                conditions_ex = []
                for condition in conditions:
                    for opt_key, opt_value in options.items():
                        condition_new = {}
                        condition_new['postfix'] = condition['postfix'] + '_' + str(opt_key)
                        condition_new['options'] = copy.deepcopy(condition['options'])
                        condition_new['options'][param_name] = opt_value
                        conditions_ex.append(condition_new)
                #     print(condition_new, conditions_ex)
                # print(conditions_ex)
                conditions = conditions_ex

        for condition in conditions:
            # with open(os.path.join(res_dir, workload + condition['postfix'] + '.f'), 'w') as file_out:
            with open(os.path.join(res_dir, workload + '.f'), 'w') as file_out:
                file_out.write(t.render(**condition['options']))

        # print(t.render(workspace='/root/fileserver', nthreads=1, sync=''))
