from socket import socket, AF_INET, SOCK_STREAM
import command_parser
import os
import threading
import hashlib

MESSAGE_SIZE = 1024
CHUNK_SIZE= 512 * 1024 #512K
BUFF_SIZE_HASH = 65536
MASTER_IP, MASTER_PORT = None, None
BKP_MASTER_IP, BKP_MASTER_PORT = None, None
with open("master_ip.conf", "r") as f:
    MASTER_IP, MASTER_PORT, BKP_MASTER_IP, BKP_MASTER_PORT= f.read().strip().split()
    MASTER_PORT = int(MASTER_PORT)
    BKP_MASTER_PORT = int(BKP_MASTER_PORT)

def s_to_i(msg):
    parts = msg.split("|")
    if parts[0] == "N":
        return int(parts[1])
    return None

def i_to_s(x):
    msg = f"N|{x}|"
    msg = msg + '\0' * (MESSAGE_SIZE - len(msg))
    return msg

def hashFunction(file_name) :
    sha1 = hashlib.sha1()
    with open(file_name, 'rb') as fp1:
        while True:
            data = fp1.read(BUFF_SIZE_HASH)
            if not data :
                break
            sha1.update(data)
    return sha1.hexdigest()

def merge(file_name,chunk_num,file_hash_value):
    chunk_num = list(map(int, chunk_num))
    chunk_num.sort()
    chunk_files = [str(x)+".chunk" for x in chunk_num]
    with open(file_name, 'wb') as outfile:
        for fname in chunk_files:
            print('copying',fname)
            with open(fname,"rb") as infile:
                outfile.write(infile.read())
            if os.path.exists(fname):
                os.remove(fname)
    #merge_file_hash = hashFunction(file_name)
    #print("HASH value of downloaded file : ", merge_file_hash)
    #print("HASH value of original file : ", file_hash_value)

def connect_to_chunk_Server(details):	#	['1,5', '127.0.0.1', 3333]
    csock = socket(AF_INET, SOCK_STREAM)
    csock.connect((details[1], details[2]))
    command = "X|{}|".format(details[0])
    command = command + '\0'*(MESSAGE_SIZE - len(command))
    cmd_bytes = str.encode(command)
    print(f"Sending {len(cmd_bytes)} of data.")
    csock.send(cmd_bytes)
    c_no = details[0].split(",")
    for c in c_no:
    	with open(f"{c}.chunk", "wb") as f :
            read_size = csock.recv(MESSAGE_SIZE).decode()
            read_size = s_to_i(read_size)
            while read_size > 0:
                dr = csock.recv(MESSAGE_SIZE)
                f.write(dr)
                read_size -= len(dr)
            csock.send(str.encode("A"*1024))
        	# recv_file(csock,chunk_ids)

def chunk_server_details(parsed_details,file_name,file_hash_value):
    all_chunk_ids = []
    thread_list = list()
    for i in parsed_details :
        if i[0] == '':
            continue
        chunk_list = i[0].split(',')
        for j in chunk_list :
            all_chunk_ids.append(j)
        # connect_to_chunk_Server(i)
        thread = threading.Thread(target=connect_to_chunk_Server, args=[i])
        thread.start()
        thread_list.append(thread)

    for thread in thread_list:
        thread.join()
    merge(file_name,all_chunk_ids,file_hash_value)

def send_single_chunk(f, sock, chunk_number, offset):
    #sleep(20)
    msg = i_to_s(chunk_number)
    print("Send Chunk number : ", msg)
    msg = str.encode(msg)
    sock.send(msg)
    act_cn = chunk_number - offset
    f.seek((act_cn-1)*CHUNK_SIZE)
    c_data = f.read(CHUNK_SIZE)
    read_size = len(c_data)
    msg = i_to_s(read_size)
    msg = str.encode(msg)
    sock.send(msg)
    ctr = 0
    while read_size > 0 :
        dts = c_data[ctr:ctr+MESSAGE_SIZE]
        sock.send(dts)
        read_size -= len(dts)
        ctr += len(dts)


    ack = sock.recv(MESSAGE_SIZE).decode()
    if ack[0] != '9':
        print("ERRR ERR ERRRRRRR")
        return -1
    print(f"{chunk_number} sent")
    return 0


def send_chunks(ip, port, chunks, file_name, offset):
    ssock = socket(AF_INET, SOCK_STREAM)
    ssock.connect((ip, port))
    f_name = f"T|{file_name}|"
    f_name = f_name + '\0'*(MESSAGE_SIZE - len(f_name))
    ssock.send(str.encode(f_name))
    num_chunks = i_to_s(len(chunks))
    ssock.send(str.encode(num_chunks))
    f = open(file_name, "rb")
    for chunk in chunks:
        v = send_single_chunk(f, ssock, chunk, offset)
        if v == -1:
            break
    f.close()
    ssock.close()

def upload_single_file(file_name):
    if not os.path.isfile(file_name):
        print(f"{file_name} file not found.")
        return
    file_size = os.path.getsize(file_name)
    file_hash_value = hashFunction(file_name)
    send_sock = socket()
    # send_sock.connect((MASTER_IP, MASTER_PORT))
    #########################################################3
    try :
        send_sock.connect((MASTER_IP, MASTER_PORT))
        print ("Connected with : ",MASTER_IP, MASTER_PORT)
    except ConnectionRefusedError :
        send_sock.connect((BKP_MASTER_IP,BKP_MASTER_PORT))
        print ("Connected to : ",BKP_MASTER_IP,BKP_MASTER_PORT)
################3####################################################3
    str_to_send = "|".join(["U", file_name, str(file_size), file_hash_value, ""])
    str_to_send = str_to_send + '\0'*(MESSAGE_SIZE - len(str_to_send))
    #encode the string into bytes
    str_bytes = str.encode(str_to_send)
    print(f"Sending {len(str_bytes)} of data.")
    send_sock.send(str_bytes)
    details = send_sock.recv(MESSAGE_SIZE).decode()
    print("I got ", details)
    '''
    details format : E|1,5:127.0.0.1:3333|2:127.0.0.1:4444|3,4:127.0.0.1:5555|
    after parsing : [['1,5', '127.0.0.1', 3333], ['2', '127.0.0.1', 4444]]
    '''
    parsed_details = command_parser.command_parser(details)
    print("Parsed details : ", parsed_details)
    thread_list = list()
    offset = None
    for chunks, ip, port in parsed_details:
        chunk_nums = list(map(int, chunks.split(",")))
        if offset is None:
            offset = chunk_nums[0] - 1 #because i work with 1 based indexing.
        print("chunk nums : ", chunk_nums)
        thread = threading.Thread(target=send_chunks, args=[ip, port, chunk_nums, file_name, offset])
        thread.start()
        thread_list.append(thread)

    for thread in thread_list:
        thread.join()

    msg = "A|{}|".format(file_name)
    msg = msg + "\0"*(MESSAGE_SIZE - len(msg))
    send_sock.send(str.encode(msg))
    print("ACK Sent.")
    send_sock.close()

def upload_file(file_names):
    for file_name in file_names:
        upload_single_file(file_name)


def parse_file_names(names, recv_sock):
    vals = names.split("|")
    if vals[0] == 'f':
        f_names = [x.strip() for x in vals[1:-1]]
        return f_names

    ret_val = None
    if vals[0] == 'S' :
        print(f'File currently not available, will abort in 20 seconds if not available ...')
        ret_val = recv_sock.recv(MESSAGE_SIZE).decode()
    if ret_val[0] == 'F' :
        print(f'File is blocked')
        return -1
    #msg = recv_sock.recv(MESSAGE_SIZE).decode()
    #print("DEBUGGGG : ", msg)
    vals = ret_val.split("|")
    if vals[0] != 'f':
        print("errrrr")
        return -1
    f_names = [x.strip() for x in vals[1:-1]]
    return f_names

def merge_all_files(f_names):
    out_name = "temp_file"
    with open(out_name, "w") as f:
        for f_name in f_names:
            with open(f_name, "r") as ff:
                f.write(ff.read())

    for f_name in f_names:
        os.remove(f_name)
    os.rename(out_name, f_names[0])

def download_single_file(f_name) :
    recv_sock = socket()
    # recv_sock.connect((MASTER_IP, MASTER_PORT))
    #####################################################
    try :
        recv_sock.connect((MASTER_IP, MASTER_PORT))
        print ("Connected with : ",MASTER_IP, MASTER_PORT)
    except ConnectionRefusedError :
        recv_sock.connect((BKP_MASTER_IP,BKP_MASTER_PORT))
        print ("Connected to : ",BKP_MASTER_IP,BKP_MASTER_PORT)
    #######################################################
    str_to_send = "|".join(["D", f_name, ""])
    str_to_send = str_to_send + '\0'*(MESSAGE_SIZE - len(str_to_send))
    str_bytes = str.encode(str_to_send)
    print(f"Sending {len(str_bytes)} of data.")
    recv_sock.send(str_bytes)
    ####################################################
    f_names = recv_sock.recv(MESSAGE_SIZE).decode()
    f_names = parse_file_names(f_names, recv_sock)
    if f_names == -1:
        return
    for file_name in f_names:
        details = recv_sock.recv(MESSAGE_SIZE).decode()
        print("I got ", details)
        if details[0] == 'S' :
            print(f'{file_name} currently not available, will abort in 20 seconds if not available ...')
            details = recv_sock.recv(MESSAGE_SIZE).decode()
        if details[0] == 'F' :
            print(f'{file_name} is blocked')
        else :
            msg = "A|{}|".format(file_name)
            msg = msg + "\0"*(MESSAGE_SIZE - len(msg))
            recv_sock.send(str.encode(msg))
            print("ACK Sent.")
            msg_from_server = recv_sock.recv(MESSAGE_SIZE).decode()
            print("from server ", msg_from_server)
            temp_list = msg_from_server.split("|")
            file_hash_value = ""
            if temp_list[0] == 'Z' :
                file_hash_value = temp_list[1]
            print(file_hash_value)
            parsed_details = command_parser.command_parser(details)
            chunk_server_details(parsed_details,file_name,file_hash_value)
    recv_sock.close()
    if len(f_names) == 1:
        return
    out_name = f_names[0]
    for fname in f_names[:-1]:
        os.remove(fname)
    os.rename(f_names[-1], out_name)

def download_file(file_names) :
    for file_name in file_names :
        download_single_file(file_name)

def leaseFile(file_name) :
    lease_sock = socket()
    # lease_sock.connect((MASTER_IP, MASTER_PORT))
    #####################################################
    try :
        lease_sock.connect((MASTER_IP, MASTER_PORT))
        print ("Connected with : ",MASTER_IP, MASTER_PORT)

    except ConnectionRefusedError :
        lease_sock.connect((BKP_MASTER_IP,BKP_MASTER_PORT))
        print ("Connected to : ",BKP_MASTER_IP,BKP_MASTER_PORT)
    #######################################################

    str_to_send = "|".join(["L", file_name[0], ""])
    str_to_send = str_to_send + '\0'*(MESSAGE_SIZE - len(str_to_send))
    str_bytes = str.encode(str_to_send)
    lease_sock.send(str_bytes)

def update_file(old_file, new_file):
    msg = f"u|{old_file}|{new_file}|"
    msg = msg + '\0'*(MESSAGE_SIZE-len(msg))
    master_sock = socket()
    # master_sock.connect((MASTER_IP, MASTER_PORT))
    #####################################################
    try :
        master_sock.connect((MASTER_IP, MASTER_PORT))
        print ("Connected with : ",MASTER_IP, MASTER_PORT)
    except ConnectionRefusedError :
        master_sock.connect((BKP_MASTER_IP,BKP_MASTER_PORT))
        print ("Connected to : ",BKP_MASTER_IP,BKP_MASTER_PORT)

    #######################################################

    master_sock.send(str.encode(msg))
    master_sock.close()
    upload_single_file(new_file)

while True:
    inp = input("> ")
    if inp == "":
        continue
    inp = inp.strip().split()
    command = inp[0]
    if command == "exit":
        break
    elif command == "upload":
        upload_file(inp[1:])
    elif command == "download" :
        download_file(inp[1:])
    elif command == "lease" :
        leaseFile(inp[1:])
    elif command == "update":
        update_file(inp[1], inp[2])
