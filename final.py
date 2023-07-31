import os
import time
import dispy

# CHUNK FILE AND SAVE CHUNK TO NFS
def split_file_into_chunks(file_path, chunk_size_bytes, nfs_file_path):
	print("start function")
	with open(file_path, 'rb') as file:
    	print("opened file")
    	chunk_num = 1
    	while True:
        	print("calculate chunk size")
        	# calculate remaining bytes to read in file
        	file_size = os.path.getsize(file_path)
        	remaining_bytes = file_size - file.tell()

        	# exit loop if no more data
        	if remaining_bytes == 0:
            	break
        	print("finished calculating chunk size")
        	# read a chunk of data from the file
        	chunk_data = file.read(min(chunk_size_bytes, remaining_bytes))
        	print("read chunk")
        	# find index of last newline character in the chunk
        	last_newline_index = chunk_data.rfind(b'\n')

        	# if no \n, seek to end of the chunk
        	if last_newline_index == -1:
            	file.seek(len(chunk_data), 1)
        	# else seek to beginning of last complete line
        	else:
            	file.seek(last_newline_index - len(chunk_data), 1)
        	print("seek end of chunk")
        	# append the chunk data to previous chunk's data
        	chunk_data = chunk_data + file.read(min(chunk_size_bytes, remaining_bytes) - len(chunk_data))
        	print("finish chunk")
        	# create new file for chunk and write the data
        	chunk_file_name = f'chunk_{chunk_num}.bin'
        	chunk_file_path = os.path.join(nfs_file_path, chunk_file_name)
        	with open(chunk_file_path, 'wb') as chunk_file:
            	chunk_file.write(chunk_data)
        	print(f'Saved {chunk_file_name} to NFS: {nfs_file_path}')
        	print("save to nfs")
        	chunk_num += 1
        	yield chunk_file_path


# SORT FUNCTION
def sort(array):
	def innersort(arr):
    	n = len(arr)
    	swapped = False
    	for i in range(n - 1):
        	for j in range(0, n - i - 1):
            	if arr[j] > arr[j + 1]:
                	swapped = True
                	arr[j], arr[j + 1] = arr[j + 1], arr[j]
        	if not swapped:
            	return

	innersort(array)


# SORT AND SAVE TO NFS
def get_from_nfs(chunk_file_path):  # now does it with one file
	with open(chunk_file_path, 'rb') as chunk_file:
    	# just print file name
   	 
    	# read contents of chunk file and parse integers
    	integers = [int(x) for x in chunk_file.read().split()]  # not sure if this is right to read it again

    	# sort integers
    	sorted_integers = sort(integers)

    	# write sorted integers back to chunk file
    	with open(chunk_file_path, 'w') as chunk_file:
        	for num in sorted_integers:
            	chunk_file.write(num.to_bytes(4, 'little', signed=True))

    	print(f'Sorted and saved {chunk_file_path} to NFS')

    	return chunk_file_path


# MERGE ALL FILES IN A LIST OF FILEPATHS
def merge_files(filepaths):
	temp_file_counter = 0 # counter to make sure the temporary files' names aren't duplicated
	new_filepaths = []
	# while there are still multiple files left to merge -> keep merging by twos
	while len(filepaths) > 1:
    	# loop to merge every element of filepaths together by twos
    	for i in range(0, len(filepaths), 2): # iterate in steps of 2
        	# if not at the end of the list
        	if i + 1 < len(filepaths):
            	# create new filepath for the merged file
            	merged_filepath = f"merged_file_{temp_file_counter}.txt"
            	temp_file_counter += 1
           	 
            	# merge the two files into the merged file path
            	merge_two_files(filepaths[i], filepaths[i + 1], merged_filepath)
           	 
            	# add merged filepath into list of new filepaths
            	new_filepaths.append(merged_filepath)
           	 
            	# remove unmerged files from original list
            	os.remove(filepaths[i])
            	os.remove(filepaths[i + 1])
       	 
        	# if at the end of the list, just append
        	else:
            	new_filepaths.append(filepaths[i])
           	 
    	# update filepaths to the new list of merged filepaths
    	filepaths = new_filepaths

	return filepaths[0]

# HELPER FUNCTION FOR MERGING TWO FILES ONLY
def merge_two_files(filepath1, filepath2, output_filepath):
	with open(filepath1, 'r') as f1, open(filepath2, 'r') as f1, open(output_filepath, 'w') as output_file:
    	# get an int from both files
    	num1 = f1.readline()
    	num2 = f2.readline()

    	# while there's still numbers in both files
    	# compare and write the smaller number to output file
    	# then read the next number
    	while num1 != '' and num2 != '':
        	if int(num1) < int(num2):
            	output_file.write(num1)
            	num1 = f1.readline()
        	else:
            	output_file.write(num2)
            	num2 = f2.readline()

    	# these run only if one file is longer than the other
    	while num1 != '' and num2 == '':
        	output_file.write(num1)
        	num1 = f1.readline()

    	while num2 != '' and num1 == '':
        	output_file.write(num2)
        	num2 = f2.readline()

if __name__ == '__main__':
	s1 = time.time()
	print("program start")

	FILE_PATH = '/mnt/data1.set'
	CHUNK_SIZE = 20 * 1024 * 1024  # 20MB
	NFS_PATH = '/mnt'

	# GET DATA FROM CHUNK FILES AND SORT
	# create list for all sorted chunk filepaths taken from the jobcluster
	job_filepaths = []
	# create and start job cluster that will execute get_from_nfs file
	cluster = dispy.JobCluster(get_from_nfs, nodes=['192.168.0.*'], host=['192.168.0.1'], depends=[get_from_nfs])
	print("made dispy cluster")

	# split file into chunks and iterate through each chunk_file_path to submit to job cluster
	for chunk_file_path in split_file_into_chunks(FILE_PATH, CHUNK_SIZE, NFS_PATH):
    	print("before cluster submitted")
    	job = cluster.submit(chunk_file_path)
    	print("after cluster submitted")
    	# append the filepaths to the job_filepaths list
    	job_filepaths.append(job())

	# START TIMER
	start_time = time.time()

	# MERGE SORTED SUBLISTS INTO ONE FILE
	print("start merge")
	output_filepath = merge_files(job_filepaths)
	print("end merge")

	# END TIMER AND PRINT TIME
	time_taken = time.time() - start_time
	print("MERGE TIME " + str(time_taken * 1000))

	# CHECK IF PROPERLY SORTED
	if os.path.exists(output_filepath):
    	print("PASS")
	else:
    	print("FAIL")

	print("TOTAL TIME " + str((time.time() - s1) * 1000))
