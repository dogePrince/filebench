import re
import os

file_out = None
flowop_regex = re.compile(r'\s*flowop \w+ name=([\w]+)')
spliter = re.compile(r'(?<!-)\s+(?!-)')
result_regex = re.compile(r'^.+\s+\d+ops\s+\d+ops/s')

wml_dir = '/root/filebench/custom_workloads'
src = '/root/workloads/log.txt'
res_dir = '/root/workloads/results'


def sort_csv(file_base_name):
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

    with open(os.path.join(res_dir, file_base_name + '.csv')) as f:
        lines = f.readlines()

    def line_value(x):
        ans = []
        keys = x.split(',')[0].split('.')
        for key in keys:
            ans.append(name_to_line_number[key])
        return ans

    lines.sort(key=line_value)
    with open(os.path.join(res_dir, file_base_name + '.csv'), 'w') as f:
        for line in lines:
            f.write(line)


if not os.path.exists(res_dir):
    os.mkdir(res_dir)

with open(src) as file_in:
    line = file_in.readline()
    while line:
        if line.startswith('[workloads]'):
            cur = line.strip().split(' ')[1]
            if file_out:
                file_out.close()
            file_out = open(os.path.join(res_dir, cur + '.csv'), 'w')
        elif result_regex.match(line):
            if file_out:
                res = ','.join(spliter.split(line.strip()))
                file_out.write(res + '\n')

        line = file_in.readline()

if file_out:
    file_out.close()


for file in os.listdir(res_dir):
    sort_csv(file.split('.')[0])
