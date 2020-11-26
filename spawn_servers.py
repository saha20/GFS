import os
import sys

chunk_server_file = "chunk_server.py"
if len(sys.argv) == 2:
    chunk_server_file = sys.argv[1]

chunk_servers = list()
with open("chunk_server_info.conf", "r") as f:
    for line in f.readlines():
        chunk_servers.append(line.strip())

n_servers = len(chunk_servers)

for i in range(1, n_servers+1):
    os.system("mkdir server{}".format(i))

for i in range(1, n_servers+1):
    os.system("cp {} ./server{}/".format(chunk_server_file, i))
    os.chdir("server{}".format(i))
    os.system("python3 {} {} &".format(chunk_server_file, chunk_servers[i-1]))
    os.chdir("..")
    print("Server{} Spawned !".format(i))
