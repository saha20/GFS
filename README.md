# Google File System [GFS] Research paper implementation
What is it?
* Google File System (GFS) is a scalable distributed file system developed by Google to provide efficient,
 reliable access to data using large clusters of commodity hardware. 
* It provides fault tolerance while running on inexpensive commodity hardware, 
and it delivers	high aggregate performance to a large number of clients.
* Constant monitoring
* Error detection and Automatic recovery .
* Fault tolerance 
* Files are broken down into chunks of fixed size and distributed into different chunk servers.
* Checksum matching is done to maintain data integrity.
* Inter communication between client/master/CS are done with message passing of a fixed size of 1024 bytes

Architechture/ Main building blocks.

 *  Master
 *  Multiple chunk Servers
 *  Multiple Clients
 
 ![Architechure](https://github.com/saha20/GFS/blob/main/image.JPG)
 

Functions supoorted :
1. Uploading and downloading files : Client will send request to master-> Master will give a list of chunk servers , and they will communicate and perform their work
2. Repication (for fault tolerance) with a (rep_factor of 2) between the chunk servers after upload operation is finished.
3. Updating existing file
4. Leasing a file
5. Chunk redistribution upon faliure of some chunk servers.
6. Hash matching while downloading the file
7. Monitoring of the chunk servers using heartbeat mechanism.


We have used dictionaries for various functionality like :

	i) dict_chunk_details ==> File_name : "P/S" : ip-port of chunk_servers which contains the preces of this file
	ii) dict_all_chunk_info  ==> For mapping all the chunks of a file we mainintined 
        {filename : {"P" or "S" : { chunk_server_ip_port : chunkids}}}
	
	iii) dict_size_info ==> filename : file_size
	iv) dict_status_bit ==> ip-port : status ( "A" Available; "C" Just gone down,need to repliacte ; "D"  Dead
	v) dict_file_hash ==> filename : File_hash
	vi) dict_file_status ==> filename : status ("A"vailable : "B"lock)

 

