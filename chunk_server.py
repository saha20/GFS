import os
from socket import socket
import sys
import threading

#     COMMANDLINE PARSING
argc = len(sys.argv)
MY_IP, MY_PORT = "", 33333

print(sys.argv)
if argc < 2:
    print("Invalid launch options.")
    sys.exit()

if argc == 3:
    MY_IP = sys.argv[1].strip()
MY_PORT = int(sys.argv[-1].strip())
#     COMMANDLINE PARSING END

MESSAGE_SIZE = 1024
CHUNK_SIZE = 512 * 1024

def i_to_s(x):
    msg = f"N|{x}|"
    msg = msg + '\0' * (MESSAGE_SIZE - len(msg))
    return msg

def s_to_i(msg):
    parts = msg.split("|")
    if parts[0] == "N":
        return int(parts[1])
    return None


def recv_file(csock, file_name):
    n_chunks = csock.recv(MESSAGE_SIZE).decode()
    n_chunks = s_to_i(n_chunks)
    print("n_chunks", n_chunks)
    for ppp in range(n_chunks):
        i = csock.recv(MESSAGE_SIZE).decode()
        i = s_to_i(i)
        print("Starting receive for chunk", i)
        f = open(f"{i}.chunk", "wb")
        read_size = csock.recv(MESSAGE_SIZE).decode()
        read_size = s_to_i(read_size)
        while read_size > 0:
            dr = csock.recv(MESSAGE_SIZE)
            f.write(dr)
            read_size -= len(dr)

        f.close()
        print("Chunk {} written.".format(i))
        msg = "9"*MESSAGE_SIZE
        csock.send(str.encode(msg))
    csock.close()

def replicate_chunks(msg):
    chunks, ip, port = msg.split(":")
    chunks = list(map(int, chunks.split(",")))
    port = int(port)
    chunk_sock = socket()
    chunk_sock.connect((ip, port))
    msg = "t|{}|".format(len(chunks))
    msg = msg + '\0'*(MESSAGE_SIZE-len(msg))
    chunk_sock.send(str.encode(msg))
    for chunk_no in chunks:
        msg = i_to_s(chunk_no)
        chunk_sock.send(str.encode(msg)) #sends the chunk number which it wants.
        with open("{}.chunk".format(chunk_no), "wb") as f:
            msg = chunk_sock.recv(MESSAGE_SIZE).decode()
            file_size = s_to_i(msg)
            size_to_recv = file_size
            print("SIZE TO ReCV : ", size_to_recv)
            data_received = 0
            while size_to_recv > 0:
                d = chunk_sock.recv(MESSAGE_SIZE)
                data_received += len(d)
                f.write(d)
                size_to_recv -= len(d)
            print("Total Data Received: ", data_received)
            msg = "A"*MESSAGE_SIZE
            chunk_sock.send(str.encode(msg))
            print("REPLICATED CHUNK {}".format(chunk_no))

    chunk_sock.close()

def send_chunks(csock ,chunks):
    c_no = chunks.split(",")
    for c in c_no:
        f_name = "{}.chunk".format(c)
        f_size = os.path.getsize(f_name)
        msg = i_to_s(f_size)
        csock.send(str.encode(msg)) #sending the filesize/chunksize to the client.
        with open(f_name, "rb") as f:
            data = f.read()
            send_size = f_size
            sent = 0
            while send_size > 0:
                d = data[sent:sent+MESSAGE_SIZE]
                sent += MESSAGE_SIZE
                csock.send(d)
                send_size -= MESSAGE_SIZE
        msg = csock.recv(MESSAGE_SIZE).decode()
        if msg[0] != 'A':
            print("ERR ACK.")

def send_replication_chunks(csock, n_chunks):
    n_chunks = int(n_chunks)
    for _ in range(n_chunks):
        msg = csock.recv(MESSAGE_SIZE).decode()
        chunk_number = s_to_i(msg)
        with open("{}.chunk".format(chunk_number), "rb") as f:
            file_size = os.path.getsize("{}.chunk".format(chunk_number))
            msg = i_to_s(file_size)
            csock.send(str.encode(msg))
            size_to_send = file_size
            data = f.read()
            sent = 0
            while size_to_send > 0:
                d = data[sent:sent+MESSAGE_SIZE]
                sent += len(d)
                csock.send(d)
                size_to_send -= len(d)
            msg = csock.recv(MESSAGE_SIZE).decode()
            if msg[0] != 'A':
                print("ACK ERROR WHILE REPLICATION.")

def process_request(csock, caddr):
    msg = csock.recv(MESSAGE_SIZE).decode()
    parts = msg.split("|")
    if parts[0] == "T":
        #File transmission mode.
        recv_file(csock, parts[1])
    elif parts[0] == "H":
        msg = "A|"+'\0'*(MESSAGE_SIZE - 2)
        csock.send(str.encode(msg))
        csock.close()
    elif parts[0] == "R":
        replicate_chunks(parts[1])
    elif parts[0] == "X":
        send_chunks(csock, parts[1])
        csock.close()
    elif parts[0] == "t":
        #chunk server to chunk server comms for replication.
        send_replication_chunks(csock, parts[1])

list_sock = socket()
list_sock.bind((MY_IP, MY_PORT))
list_sock.listen(10)

while True:
    csock, caddr = list_sock.accept()
    thrd = threading.Thread(target=process_request, args=[csock, caddr])
    thrd.start()

list_sock.close()
