#!/usr/bin/python3

import os
from socket import socket
import threading
import math
import json
import time

MASTER_IP, MASTER_PORT = None, None
CHUNK_SIZE = 524288
MESSAGE_SIZE = 1024
chunk_id=1
dict_chunk_details={}
dict_all_chunk_info={}
dict_size_info={}
dict_status_bit={}
dict_chunkserver_ids={}
dict_file_status={} #to store the status of file for lease feature
dict_file_hash={} #to store hash value of the files
dict_filename_update={} #to store mapping of file name as list of multiple files for update feature


def load_after_restart():
    global dict_chunk_details
    global dict_all_chunk_info
    global dict_size_info
    global dict_status_bit
    global dict_chunkserver_ids
    global dict_file_status
    global dict_file_hash
    global dict_filename_update
    global chunk_id
    f=open("file_table.json","r")
    dict_chunk_details=eval(f.read())
    f=open("file_chunk_info.json","r")
    dict_all_chunk_info=eval(f.read())
    f=open("file_size.json","r")
    dict_size_info=eval(f.read())
    f=open("file_status_bit.json","r")
    dict_status_bit=eval(f.read())
    f=open('file_chunkserver_ids.json','r')
    dict_chunkserver_ids=eval(f.read())
    f=open('file_lease_status.json','r')
    dict_file_status=eval(f.read())
    # print("dict_file_status  :", dict_file_status)
    f=open('file_hash_value.json','r')
    dict_file_hash=eval(f.read())
    f=open('file_when_update.json','r')
    dict_filename_update=eval(f.read())
    # print("dict_chunk_details : : ",dict_chunk_details)
    f=open('counter.json','r')
    chunk_id=int(f.read())


def writetofile_backup():
    with open('file_table.json', 'w') as outfile:
        json.dump(dict_chunk_details, outfile)
    with open('file_chunk_info.json', 'w') as outfile:
        json.dump(dict_all_chunk_info, outfile)
    with open('file_size.json', 'w') as outfile:
        json.dump(dict_size_info, outfile)
    with open('file_status_bit.json','w') as outfile:
        json.dump(dict_status_bit, outfile)
    with open('file_chunkserver_ids.json','w') as outfile:
        json.dump(dict_chunkserver_ids, outfile)
    with open('file_lease_status.json','w') as outfile:
        json.dump(dict_file_status, outfile)
    with open('file_hash_value.json','w') as outfile:
        json.dump(dict_file_hash, outfile)
    with open('file_when_update.json','w') as outfile:
        json.dump(dict_filename_update, outfile)

def backup():
    print("inside backup")
    while True:
        master_sock = socket()
        time.sleep(10)
        try :
            # print(" TRY MASTER_IP, MASTER_PORT ", MASTER_IP, MASTER_PORT )
            master_sock.connect((MASTER_IP, MASTER_PORT))
        except ConnectionRefusedError :
            print("Refused Connection")#writetofile_backup()
            # master_sock.close()
            continue
        print ("loading.................")
        load_after_restart()
        master_sock.close()

def checkStatus(temp_list) :
    # print(temp_list[0], temp_list[1])
    chunk_ip = str(temp_list[0])
    chunk_port = int(temp_list[1])
    chunk_sock = socket()
    chunk_sock.settimeout(1)
    flag = 0
    try :
        chunk_sock.connect((chunk_ip, chunk_port))
    except ConnectionRefusedError :
        flag = 1
    if flag == 0 :
        str3 = "H|"
        message1 = str3 + '\0'*(MESSAGE_SIZE - len(str3))
        chunk_sock.send(message1.encode())
        try :
            message = chunk_sock.recv(MESSAGE_SIZE).decode()
            if message[0] != 'A' :
                flag = 1
        except socket.timeout: # fail after 1 second of no activity
            flag = 1
    temp_str = temp_list[0] + ":" + temp_list[1]
    if flag == 1 :
        # print(temp_str,'is down!!!!!!!!!!')
        if dict_status_bit[temp_str] == 'A' :
            dict_status_bit[temp_str] = 'C'
            print(temp_str,'is down.')
            node_down(temp_str)
        elif dict_status_bit[temp_str] == 'C' :
            dict_status_bit[temp_str] = 'D'
    elif flag == 0 :
        dict_status_bit[temp_str] = 'A'
    chunk_sock.close()

def node_down(ip_port):
    primary_list=[]
    secondary_list = []
    ip_ports = find_cs(ip_port)
    for i in ip_ports:
        print(i)
    # print(ip_ports)
    print("before loop : ",dict_chunk_details)
    sel_key = ip_ports[1]
    third = ip_ports[2]
    for f_name in dict_chunk_details:
        primary_list = primary_list + dict_chunk_details[f_name]['P'][ip_port]
        dict_chunk_details[f_name]["P"][sel_key].extend(dict_chunk_details[f_name]["S"][sel_key])
        secondary_list = secondary_list + dict_chunk_details[f_name]['S'][ip_port]
        dict_chunk_details[f_name]["S"][sel_key].clear()
        dict_chunk_details[f_name]["S"][sel_key].extend(dict_chunk_details[f_name]["S"][ip_port])
        dict_chunk_details[f_name]["S"][third].extend(dict_chunk_details[f_name]["P"][ip_port])


    primary_str = 'R|' + ','.join(primary_list) + ':'+ ip_ports[1] + '|'
    secondary_str = 'R|' + ','.join(secondary_list) + ':'+ ip_ports[0] + '|'
    connect_when_replica(ip_ports[2],primary_str)
    connect_when_replica(ip_ports[1],secondary_str)
    # ip_port1 = distribute_primary(ip_port)

    # ip_port2 = distribute_secondary(ip_port)

def find_cs(ip_port):
    chunk_server_list = []
    prev_active = ''
    first_active = ''
    second_active = ''
    for key in dict_status_bit.keys():
        chunk_server_list.append(key)
    print('inside find_cs :', chunk_server_list)
    i = len(chunk_server_list) - 1
    while(chunk_server_list[i] != ip_port):
        i = (i-1) %len(chunk_server_list)
    while(dict_status_bit[chunk_server_list[i]] != 'A'):
        i = (i-1) %len(chunk_server_list)
    prev_active = chunk_server_list[i]
    i=0
    while(chunk_server_list[i] != ip_port):
        i = (i+1) %len(chunk_server_list)
    while(dict_status_bit[chunk_server_list[i]] != 'A'):
        i = (i+1) %len(chunk_server_list)
    first_active = chunk_server_list[i]
    i = (i+1) %len(chunk_server_list)
    while(dict_status_bit[chunk_server_list[i]] != 'A'):
        i = (i+1) %len(chunk_server_list)
    second_active = chunk_server_list[i]
    l = [prev_active, first_active, second_active]
    return l

def checkChunkServers() :
    list_ip_port_input = []
    with open("chunk_server_info.conf", "r") as fp1:
        for line in fp1.readlines():
            list_ip_port_input.append(line.strip().split())
    while True :
        for chunk_server_details in list_ip_port_input :
            checkStatus(chunk_server_details)
        print(dict_status_bit)
        time.sleep(10)

def connect_when_replica(ip_port,str_to_send):
    temp_list=ip_port.split(":")
    chunk_ip = str(temp_list[0])
    chunk_port = int(temp_list[1])
    chunk_sock = socket()
    chunk_sock.connect((chunk_ip, chunk_port))
    message1 =str_to_send + '\0'*(MESSAGE_SIZE - len(str_to_send))
    print(message1)
    chunk_sock.send(message1.encode())
    chunk_sock.close()

def send_replica_info_all(file_name,dict_chunk_details):
    dict_chunkid_ipport={}
    for key,value in dict_chunk_details[file_name]['P'].items():
        for i in value:
            dict_chunkid_ipport[i]=key
    for key,value in dict_chunk_details[file_name]['S'].items():
        if(len(value)>0):
            str_to_send="R|"
            str_temp=""
            temp=""
            #print(value)
            str_temp=','.join(value)
            str_to_send=str_to_send+str_temp+":"+dict_chunkid_ipport[value[0]]+"|"
            #print(key)
            #print(str_to_send)
            connect_when_replica(key,str_to_send)

def format_to_json(file_size,file_name,final_list_chunks,list_ip_port):
    temp_dict_pri={}
    list_temp=[]
    for list1 in final_list_chunks:
        for j in list1:
            list_temp.append(j)
    dict_all_chunk_info[file_name]=list_temp
    dict_size_info[file_name]=file_size
    i=0
    from copy import deepcopy
    for list1 in final_list_chunks:
        temp=list_ip_port[i][0]+":"+list_ip_port[i][1]
        temp_dict_pri[temp]=deepcopy(list1)
        i=i+1
    i=1
    temp_dict_sec={}
    for list1 in final_list_chunks:
        temp=list_ip_port[i][0]+":"+list_ip_port[i][1]
        temp_dict_sec[temp]=deepcopy(list1)
        i=(i+1)%len(list_ip_port)

    temp_dict={}
    temp_dict['P']=temp_dict_pri
    temp_dict['S']=temp_dict_sec
    dict_chunk_details[file_name]=temp_dict
    # with open('file_table.json', 'w') as outfile:
    #     json.dump(dict_chunk_details, outfile)
    # with open('file_chunk_info.json', 'w') as outfile:
    #     json.dump(dict_all_chunk_info, outfile)
    # with open('file_size.json', 'w') as outfile:
    #     json.dump(dict_size_info, outfile)
    #with open('file_table.json', 'w', encoding='utf-8') as outfile:
    #    json.dump(dict_chunk_details, outfile, ensure_ascii=False, indent=4)
    #with open('file_chunk_info.json', 'w', encoding='utf-8') as outfile:
    #    json.dump(dict_all_chunk_info, outfile, ensure_ascii=False, indent=4)
    #with open('file_size.json', 'w', encoding='utf-8') as outfile:
    #    json.dump(dict_size_info, outfile, ensure_ascii=False, indent=4)
    # send_replica_info_all(file_name,dict_chunk_details)

    #print(dict_chunk_details)
def create_status():
    list_ip_port = []
    with open("chunk_server_info.conf", "r") as fp1:
        for line in fp1.readlines():
            list_ip_port.append(line.strip().split())
    for list1 in list_ip_port :
        temp=list1[0]+":"+list1[1]
        dict_status_bit[temp]="D"

def create_dict_chunkserver(list_ip_port,final_list_chunks):
    i=0
    for list1 in list_ip_port:
        list3=[]
        temp=list1[0]+":"+list1[1]
        if temp in dict_chunkserver_ids:
            for ids in dict_chunkserver_ids[temp]:
                list3.append(ids)
        for j in final_list_chunks[i]:
            list3.append(j)
        dict_chunkserver_ids[temp]=list3
        i=i+1
    print(dict_chunkserver_ids)
#read from meta data of master and prepare the string to be sent to client

def uploadChunks(data_from_client) :
    print(f"upload {data_from_client[1]} {data_from_client[2]} {data_from_client[3]}")
    global chunk_id
    list_ip_port_input=[]
    list_ip_port = []
    with open("chunk_server_info.conf", "r") as fp1:
        for line in fp1.readlines():
            list_ip_port_input.append(line.strip().split())

    for list1 in list_ip_port_input :
        temp_str = list1[0]+":"+list1[1]
        if dict_status_bit[temp_str] == 'A' :
            list_ip_port.append(list1)

    file_name=data_from_client[1]
    file_size = int(data_from_client[2])
    file_hash_value = data_from_client[3]
    no_of_chunk_servers = len(list_ip_port)
    no_of_chunks = math.ceil(file_size/CHUNK_SIZE)
    print("no of chunks ", no_of_chunks)
    data_info={}

    final_list_chunks = list()
    for i in range(1,no_of_chunk_servers+1) :
        temp_list1 = list()
        counter = 0
        while i+(counter*no_of_chunk_servers) <= no_of_chunks :
            temp_list1.append(str(i+chunk_id+(counter*no_of_chunk_servers)))
            counter = counter + 1
            print(temp_list1)
        final_list_chunks.append(temp_list1)
    print(final_list_chunks)
    chunk_id+=no_of_chunks
    #print(final_list_chunks)
    #added ds for update ##############
    dict_filename_update[file_name]=[file_name]
    ########################################3
    dict_file_status[file_name]="A" #to store the status of file for lease feature
    dict_file_hash[file_name] = file_hash_value #to store hash value of a file
    format_to_json(file_size,file_name,final_list_chunks,list_ip_port)
    create_dict_chunkserver(list_ip_port,final_list_chunks)
    str3 = 'E'
    str_to_send = ""
    for i in range(0, len(final_list_chunks)) :
        if i == no_of_chunks:
            break
        str1 = ','.join(final_list_chunks[i])
        str2 = ':'.join([str1, list_ip_port[i][0], list_ip_port[i][1]])
        str3 = '|'.join([str3, str2])
    str3 = f"{str3}|"
    str_to_send = str3 + '\0'*(MESSAGE_SIZE - len(str3))
    return str_to_send

def downloadChunks(data_from_client,send_sock,state = 'X') :
    file_name = data_from_client[1]
    print(" line 325 dict_file_status  :", dict_file_status)
    if(dict_file_status[file_name]=="A"):
        list_temp = []
        print(dict_chunk_details[file_name]['P'])
        final_str = ""
        print("dict_chunk_details inside download :",dict_chunk_details)
        print("dict_status_bit inside download :",dict_status_bit)
        #to send the number of files coressponding to each file name
        str_name="f|"
        for fname in dict_filename_update[file_name]:
            str_name=str_name+fname+"|"
        str_to_send = str_name + '\0'*(MESSAGE_SIZE - len(str_name))
        send_sock.send(str.encode(str_to_send))
        #####################################################
        for file1 in dict_filename_update[file_name]:
            for key,value in dict_chunk_details[file1]['P'].items():
                if dict_status_bit[key] != 'A':
                    continue
                chunk_nums = ','.join(value)
                temp_str = ':'.join([chunk_nums, key])
                final_str += temp_str + '|'
            final_str1 = 'E|' + final_str
            str_to_send = final_str1 + '\0'*(MESSAGE_SIZE - len(final_str1))
            print("inside downloadChunks : ",str_to_send)
            print("SENDING ..... : ", str_to_send)
            str_bytes = str.encode(str_to_send)
            send_sock.send(str_bytes)
            msg = send_sock.recv(MESSAGE_SIZE).decode()
            if(msg[0]!='A'):
                print("Erron in recv ACK from client")
            else:
                str_hash = 'Z|' + dict_file_hash[data_from_client[1]] + '|'
                str_to_send = str_hash + '\0'*(MESSAGE_SIZE - len(str_hash))
                print("2 ",str_to_send)
                str_bytes = str.encode(str_to_send)
                send_sock.send(str_bytes)

        send_sock.close()
        return "lol"
    else:
        if state == 'S' :
            final_str1 = 'F|101|'
            str_to_send =  final_str1 + '\0'*(MESSAGE_SIZE - len(final_str1))
            return str_to_send
        else :
            final_str1 = 'S|101|'
            str_to_send =  final_str1 + '\0'*(MESSAGE_SIZE - len(final_str1))
            return str_to_send

def leaseFile(data_from_client):
    file_name = data_from_client[1]
    #change status bit for that file
    dict_file_status[file_name]="B"
    time.sleep(60)
    #again change status bit for that file
    dict_file_status[file_name]="A"

def updateFile(old_filename,new_filename):
    dict_filename_update[old_filename].append(new_filename)

#accept request from client and call function accordingly and send reply to client
def accceptRequest(data_from_client, send_sock) :
    if data_from_client[0] == 'U' :
        str_bytes = ""
        temp_str = ""
        temp_str = uploadChunks(data_from_client)
        str_bytes = str.encode(temp_str)
        send_sock.send(str_bytes)
        #blocking call for ack
        msg=send_sock.recv(MESSAGE_SIZE).decode()
        if(msg[0]!='A'):
                print("Erron in recv ACK from client")
        send_sock.close()
        send_replica_info_all(data_from_client[1],dict_chunk_details)
    elif data_from_client[0] == 'D' :
        str_bytes = ""
        temp_str = ""
        temp_str = downloadChunks(data_from_client,send_sock)
        if temp_str[0] == 'S' :
            str_bytes = str.encode(temp_str)
            send_sock.send(str_bytes)
            time.sleep(20)
            temp_str = downloadChunks(data_from_client,send_sock,'S')
        if temp_str[0] == 'F' :
            str_bytes = str.encode(temp_str)
            send_sock.send(str_bytes)
            send_sock.close()
        #elif temp_str[0] == 'E' :

    elif data_from_client[0] == 'L' :
        leaseFile(data_from_client)
    #adding code in case of update feature
    elif data_from_client[0] == 'u':
        old_filename=data_from_client[1]
        new_filename=data_from_client[2]
        updateFile(old_filename,new_filename)


def clientReceive() :
    global  MASTER_IP,MASTER_PORT
    fp1 = open("master_ip.conf", "r")
    MASTER_IP, MASTER_PORT  = fp1.readline().strip().split()
    MASTER_PORT = int(MASTER_PORT)
    print(" MASTER_IP, MASTER_PORT ", MASTER_IP, MASTER_PORT )
    thread2 = threading.Thread(target = backup)
    thread2.start()
    read_data =fp1.readline().strip()
    fp1.close()
    master_ip, master_port = read_data.split(" ")
    print(f"{master_ip} {master_port}")
    master_sock = socket()
    master_sock.bind(("", int(master_port)))
    master_sock.listen(10)
    while True :
        client_sock, client_addr = master_sock.accept()
        packet = client_sock.recv(MESSAGE_SIZE)
        packet_from_client = packet.decode()
        data_from_client = packet_from_client.split("|")
        thread1 = threading.Thread(target = accceptRequest, args = (data_from_client, client_sock, ))
        thread1.start()


        #thread1.join()
    master_sock.close()


if __name__ == "__main__" :
    create_status()
    thread1 = threading.Thread(target = checkChunkServers)
    thread1.start()
    clientReceive()
    # thread2 = threading.Thread(target = backup)
    # thread2.start()
