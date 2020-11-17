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


rm_files_str = '[OP] fb_lfs_recur_rm(path=/pmfs)\n'
for sub in static_workloads.iterdir():
    if sub.is_dir() and sub.name not in except_name:
        pre_count = 0
        run_count = 0
        for workload in sub.iterdir():
            is_pre = True
            # with workload.open('rb') as f:
            #     c = f.read()
            #     encoding = chardet.detect(c)['encoding']
            #
            #     print(workload, encoding)

            tmp_file = sub.joinpath('des')
            with tmp_file.open('w') as f_out:
                f_out.write(rm_files_str)
                with workload.open() as f_in:
                    line = f_in.readline()
                    while line:
                        f_out.write(line)
                        if line.startswith('[FLOWOP]'):
                            is_pre = False
                        line = f_in.readline()


            if is_pre:
                des_file = sub.joinpath('prepare_' + str(pre_count))
                pre_count += 1
            else:
                des_file = sub.joinpath('run_' + str(run_count))
                run_count += 1

            tmp_file.replace(des_file)
