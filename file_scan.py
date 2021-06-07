import time
from datetime import datetime
import json
import qumulo
from qumulo.rest_client import RestClient
import logging

# Read credentials
json_file = open('credentials.json','r')
json_data = json_file.read()
json_object = json.loads(json_data)

# Parse cluster credentials
cluster_address = json_object['cluster_address']
port_number = json_object['port_number']
username = json_object['username']
password = json_object['password']

# Connect to the cluster
rc = RestClient(cluster_address, port_number)
rc.login(username, password)
logging.info('Connection established with {}'.format(cluster_address))

# Script inputs
directory_path = json_object['directory_path']
time_periods = json_object['time_periods']
file_sizes = json_object['file_sizes']

# Dictionary definitions
time_based_file_count = dict()
time_based_file_size = dict()
owner_based_file_size = dict()
owner_based_file_count = dict()
file_type_count = dict()
file_size_count = dict()


start_time = datetime.now().strftime("%s")

def add_dict_entry (dict_name, key_value):
	if key_value in dict_name:
		dict_name[key_value] += 1
	else:
		dict_name[key_value] = 1

def file_operation (object):
	modification_time = object['modification_time'].split("T")[0]
	modification_time_epoch = datetime.strptime(modification_time, '%Y-%m-%d').strftime("%s")
	current_time = datetime.now().strftime("%s")
	diff_time = int(current_time) - int(modification_time_epoch)
	for x in range(len(time_periods)):
		if x == 0:
			if diff_time <= (86400 * time_periods[x]):
				key_value = "<"+str(time_periods[x])+"days"  
				dict_name = time_based_file_count
				add_dict_entry (dict_name, key_value)
				key_value = "<"+str(time_periods[x])+"days"
				dict_name = time_based_file_size
				if key_value in dict_name:
					dict_name[key_value] += int(object['size'])
				else:
					dict_name[key_value] = int(object['size'])
		else:
			if (86400 * time_periods[x-1]) <= diff_time <= (86400 * time_periods[x]):
				key_value = str(time_periods[x-1]) + "< X <"+ str(time_periods[x])+"days"
				dict_name = time_based_file_count
				add_dict_entry (dict_name, key_value)
				key_value = str(time_periods[x-1]) + "< X <"+ str(time_periods[x])+"days"
				dict_name = time_based_file_size
				if key_value in dict_name:
					dict_name[key_value] += int(object['size'])
				else:
					dict_name[key_value] = int(object['size'])
	if diff_time >= (86400 * time_periods[len(time_periods)-1]):
		key_value = "<"+str(time_periods[len(time_periods)-1])+"days"
		dict_name = time_based_file_count
		add_dict_entry (dict_name, key_value)
		key_value = "<"+str(time_periods[x])+"days"
		dict_name = time_based_file_size
		if key_value in dict_name:
			dict_name[key_value] += int(object['size'])
		else:
			dict_name[key_value] = int(object['size'])
	file_type = os.path.splitext(object['name'])[1]
	key_value = file_type
	dict_name = file_type_count
	add_dict_entry(dict_name, key_value)
	owner_id = object['owner']
	key_value = owner_id
	dict_name = owner_based_file_count
	add_dict_entry(dict_name, key_value)
	key_value = owner_id
	dict_name = owner_based_file_size
	if key_value in dict_name:
		dict_name[key_value] += int(object['size'])
	else:
		dict_name[key_value] = int(object['size'])
	file_size = object['size']
	for y in range(len(file_sizes)):
		if y == 0:
			if int(file_size) <= file_sizes[y]:
				key_value = "<"+str(file_sizes[y])
				dict_name = file_size_count
				add_dict_entry (dict_name, key_value)
		else:
			if file_sizes[y-1] <= int(file_size) <= file_sizes[y]:
				key_value = str(file_sizes[y-1])+"< X <"+str(file_sizes[y])
				dict_name = file_size_count
				add_dict_entry (dict_name, key_value)
	if int(file_size) >= file_sizes[len(file_sizes)-1]:
		key_value = "<"+str(file_sizes[y])
		dict_name = file_size_count
		add_dict_entry (dict_name, key_value)


def tree_walk (objects):
	for r in range(len(objects)):
		current_time = datetime.now().strftime("%s")
		delta_time = int(current_time) - int(start_time)
		print ("Processing..."+ str(delta_time) + "seconds", end="\r")
		object = objects[r]
		if object['type'] == "FS_FILE_TYPE_FILE":
			file_operation (object)
		elif object['type'] == "FS_FILE_TYPE_DIRECTORY":
			next_page = "first"
			while next_page != "":
				r = None
				if next_page == "first":
					try:
						r = rc.fs.read_directory(path=object['path'], page_size=1000)
					except:
						next
				else:
					r = rc.request("GET", next_page)
				if not r:
					break
				dir_objects = r['files']
				tree_walk (dir_objects)
				if 'paging' in r and 'next' in r['paging']:
					next_page = r['paging']['next']
				else:
					next_page = ""

objects = rc.fs.read_directory(path=directory_path, page_size=10000)['files']
tree_walk (objects)

print (time_based_file_count)
print (time_based_file_size)
print (file_type_count)
print (file_size_count)
print (owner_based_file_count)
print (owner_based_file_size)
