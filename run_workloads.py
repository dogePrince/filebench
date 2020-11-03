import os
import shutil
import subprocess

wml_dir = '/root/filebench/custom_workloads'
result_dir = '/root/workloads'
tmp1 = '/root/file_tmp'
tmp2 = '/pmfs/file_tmp'
src = os.path.join(result_dir, 'raw')
log = os.path.join(result_dir, 'log.txt')

if os.path.exists(log):
    os.remove(log)

with open(log, 'w') as log_file:
    for wml in os.listdir(wml_dir):
        wml_name = wml.split('.')[0]

        des = os.path.join(result_dir, wml_name)

        if os.path.exists(tmp1):
            shutil.rmtree(tmp1)
        if os.path.exists(tmp2):
            shutil.rmtree(tmp2)

        if os.path.exists(src):
            shutil.rmtree(src)
        os.mkdir(src)

        cur = '[workloads] ' + wml_name
        print(cur)
        log_file.write(cur + '\n')
        log_file.flush()
        p = subprocess.Popen(['filebench', '-f', os.path.join(wml_dir, wml)], stdout=log_file, stderr=log_file)
        p.wait()
        log_file.flush()

        if len(os.listdir(src)) != 0:
            if os.path.exists(des):
                shutil.rmtree(des)
            shutil.copytree(src, des)

