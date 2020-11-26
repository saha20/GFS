# H - Heartbeat signal from Master to ChunkServer
# S - State dead/alive etc response from ChunkServer to Master
# U - Client wants to upload(write a file to GFS)
#       Format ==> U|file_name|file_size
#       Example ==> U|GFS.txt|890
# D - Client wants to download(read a file to GFS)
#       Format ==> D|file_name|
#       Example ==> D|GFS.txt
# E - Master to client , as a reply to D. i t sends details of all the chunkservers
#       Format ==> E|chunk_num1,chunk_num2:chunkserver_ip1:chunkserver_port1|
#                    chunk_num1:chunkserver_ip2:chunkserver_port2
#       Ex : E|1,5:127.0.0.1:3333|2:127.0.0.1:4444|3,4:127.0.0.1:5555

def command_parser(arg):
    if arg[0] == 'U':
        return upload_command_from_client(arg[2:])
    elif arg[0] == 'D':
        return download_command_from_client(arg[2:])
    elif arg[0] == 'E':
        return details_of_chunkservers(arg[2:])
    elif arg[0] == 'D':
        return funcD(arg[2:])


def upload_command_from_client(arg):
    # data is a list; data[0] = file_name and data[1] = file_size
    data = arg.split('|')
    data = data[:-1]
    data[1] = int(data[1])
    return data

def download_command_from_client(arg):
    data = arg.split('|')
    data = data[:-1]
    return data

def details_of_chunkservers(arg):
    data = arg.split('|')
    data = data[:-1]
    list_of_chunkservers=[]
    for i in data:
        list_of_chunkservers.append(i.split(":"))
    for i in list_of_chunkservers:
        i[2] = int(i[2])  #port_num
    return list_of_chunkservers #list of list
