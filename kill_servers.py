import subprocess
import os

ports = []
with open("chunk_server_info.conf", "r") as f:
    for line in f:
        _, p_no = line.strip().split()
        ports.append(p_no)

for port in ports:
    try:
        op = str(subprocess.check_output("sudo netstat -nlp | grep {}".format(port), shell=True))
        pid = op.split()[6].split('/')[0]
        os.system("kill -9 {}".format(pid))
        print("Port : {} freed.".format(port))
    except:
        pass
