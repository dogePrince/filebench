from pathlib import Path
import re
import chardet


digit_regex = re.compile(r'^\d+$')
except_name = ['raw', 'results']
static_workloads = Path('/root/workloads')


for sub in static_workloads.iterdir():
    if sub.is_dir() and sub.name not in except_name:
        for file in sub.iterdir():
            if not digit_regex.match(file.name):
                file.unlink()


for sub in static_workloads.iterdir():
    if sub.is_dir() and sub.name not in except_name:
        pre_count = 0
        run_count = 0
        for workload in sub.iterdir():
            is_pre = True
            with workload.open('rb') as f:
                c = f.read()
                encoding = chardet.detect(c)['encoding']

            tmp_file = sub.joinpath('des')
            with workload.open(encoding=encoding) as f_in:
                with tmp_file.open('w') as f_out:
                    line = f_in.readline()
                    while line:
                        f_out.write(line)
                        if line.startswith('[FLOWOP]'):
                            is_pre = False
                        line = f_in.readline()


            if is_pre:
                des_file = sub.joinpath('prepare_' + str(pre_count))
                pre_count += 1
                print(workload, 'pre', pre_count)
            else:
                des_file = sub.joinpath('run_' + str(run_count))
                run_count += 1
                print(workload, 'run', run_count)

            workload.replace(sub.joinpath('run_' + str(run_count)))
