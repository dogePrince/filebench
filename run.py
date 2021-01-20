import os
from jinja2 import Template
import sys
import socket


filebench_dir = os.path.split(os.path.realpath(__file__))[0]
workloads_template_dir = os.path.join(filebench_dir, 'workloads_templates')
workloads_dir = os.path.join(filebench_dir, 'custom_workloads')


def parse_options(args):
    host_name = socket.gethostname()
    options = {
        'workspace': os.path.join(args[0], f'file_tmp_{host_name}'),
        'nthreads': args[1],
        'runtime': args[2],
        'nfiles': args[3],
    }
    if len(args) > 4:
        options['sync'] = args[4]

    return options


def gen_workloads(workload, options):
    if not os.path.exists(workloads_dir):
        os.mkdir(workloads_dir)

    output_file = os.path.join(workloads_dir, workload)
    with open(os.path.join(workloads_template_dir, workload)) as template, \
            open(output_file, 'w') as output:
        contents = template.read()
        t = Template(contents, variable_start_string='{^', variable_end_string='^}')
        output.write(t.render(**options))

    return output_file


def run(workload, args):
    options = parse_options(args)
    workload_final = gen_workloads(workload, options)
    os.system(f'filebench -f {workload_final}')


if __name__ == '__main__':
    run(sys.argv[1], sys.argv[2:])
