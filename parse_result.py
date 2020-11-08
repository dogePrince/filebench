import re
import os
import shutil


flowop_regex = re.compile(r'\s*flowop \w+ name=([\w]+)')
spliter = re.compile(r'(?<!-)\s+(?!-)')
result_regex = re.compile(r'^.+\s+\d+ops\s+\d+ops/s')
summary_spliter = re.compile(r'^[\d\.]+: IO Summary: ')

wml_dir = '/root/filebench/custom_workloads'
src = '/root/workloads/log.txt'
res_dir = '/root/workloads/results'

table_head = 'flowOP,ops,op/s,throughput,time/op,tmin_latency-max_latency\n'


def sort_by_op(file_base_name, data):
    wml_file = os.path.join(wml_dir, file_base_name + '.f')
    name_to_line_number = {}
    with open(wml_file) as f:
        index = 0
        line = f.readline()
        while line:
            flowop_matcher = flowop_regex.match(line)
            if flowop_matcher:
                name_to_line_number[flowop_matcher.groups()[0]] = index
            line = f.readline()
            index += 1

    def line_value(x):
        ans = []
        keys = x.split(',')[0].split('.')
        for key in keys:
            ans.append(name_to_line_number[key])
        return ans

    data.sort(key=line_value)


def write_result(file_base_name, data, summary):
    with open(os.path.join(res_dir, file_base_name + '.csv'), 'w') as file_out:
        file_out.write(table_head)
        sort_by_op(file_base_name, data)
        for line in data:
            file_out.write(line + '\n')
        file_out.write(summary)
    with open(os.path.join(res_dir, 'summary.txt'), 'a') as summary_file:
        summary_file.write(file_base_name + '\n')
        summary_file.write(summary + '\n')

def parse_result(log_file):
    if os.path.exists(res_dir):
        shutil.rmtree(res_dir)
    os.mkdir(res_dir)
  
    cur_workload, res, summary = '', [], ''
    
    with open(src) as file_in:
        line = file_in.readline()
        while line:
            if line.startswith('[workloads]'):
                cur_workload = line.strip().split(' ')[1]
            elif cur_workload and result_regex.match(line):
                res.append(','.join(spliter.split(line.strip())))
            elif summary_spliter.match(line):
                summary = 'IO Summary: ' + summary_spliter.split(line)[1]
                write_result(cur_workload, res, summary)
                cur_workload, res, summary = '', [], ''

            line = file_in.readline()


if __name__ == '__main__':
    parse_result(src)

