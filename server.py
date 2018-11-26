from kazoo.client import KazooClient
from kazoo.client import KazooState
from kazoo.client import KazooRetry
from kazoo.exceptions import ConnectionClosedError

import falcon
import os
import logging
import requests
from waitress import serve
import sys

import time


NUMBER_OF_SERVERS = None
CLASS = None
PORT = sys.argv[1]
IP = "0.0.0.0"


memory_db = {}
backup_db = {}

def zk_status_listener(state):
	if state == KazooState.LOST:
		print("Register somewhere that the session was lost")
		exit(0)
		
	elif state == KazooState.SUSPENDED:
		print("Handle being disconnected from Zookeeper")
		exit(0)

def to_string(value):
	return str(value.decode())

def to_response(string):
	return string.content.decode('utf-8').strip("\"")

def hash_function(string):
	return sum(map(ord, list(string)))

try:
	logging.basicConfig()
	zkr = KazooRetry(max_tries=-1)
	client = KazooClient(hosts="127.0.0.1:2181", connection_retry=zkr)
	client.add_listener(zk_status_listener)
	client.start()

	if client.exists("/servers/master"):
		@client.ChildrenWatch("/servers/")
		def become_master(children):
			if "master" not in children:
				instances = client.get_children("/servers/slaves/")
				if instances[0].split("_")[-1] == PORT:
					print(">>> This server is the new master")
					client.create("/servers/master", ephemeral = True)
					client.set("/servers/master", PORT.encode())

					if client.exists("/servers/status"):
						client.delete("/servers/status")

					client.create("/servers/status", ephemeral = True)
					client.set("/servers/status", "READY".encode())



		print("This is not a master server")
		print("Master Server:")
		print(to_string(client.get("/servers/master")[0]))
		print("Other servers online:")
		print(client.get_children("/servers/slaves/"))
		client.ensure_path("/servers/slaves")

		seq_number = client.create("/servers/slaves/instance_{0}".format(PORT), ephemeral=True)
		client.set("/servers/slaves/instance_{0}".format(PORT), PORT.encode())

		TEMP_PATH = "/mapping/"
		if client.exists(TEMP_PATH):
			children = client.get_children(TEMP_PATH)

			for child in children:
				if to_string(client.get(TEMP_PATH + child)[0]) == PORT:
					# global CLASS, NUMBER_OF_SERVERS
					CLASS = int(child.split("_")[-1])
					print("Received Mapping Data: Class = {0}".format(str(CLASS)))
					NUMBER_OF_SERVERS = int(to_string(client.get("/servers/count_servers")[0]))
					print("Server Restarted!")					

					client.ensure_path("/mapping/class_{0}/memory".format(str(CLASS)))
					client.ensure_path("/mapping/class_{0}/backup".format(str(CLASS)))

					backup_port = to_string(client.get("/mapping/class_{0}".format(str((CLASS+1)%NUMBER_OF_SERVERS)))[0])
					if client.exists("/servers/slaves/instance_{0}".format(backup_port)):
						memory_data = to_response(requests.get("http://{0}:{1}/send/backup".format(IP, backup_port))) 
					else:
						print("Error: Backup Server Down. Unable to Fetch data. Exiting...")
						exit(0)

					for record in memory_data.split(";"):
						key, value = record.split(":")
						memory_db[key] = value
						client.create("/mapping/class_{0}/memory/{1}".format(str(CLASS), key), ephemeral = True)
						client.set("/mapping/class_{0}/memory/{1}".format(str(CLASS), key), value.encode())

					memory_port = to_string(client.get("/mapping/class_{0}".format(str((CLASS-1)%NUMBER_OF_SERVERS)))[0])
					
					if client.exists("/servers/slaves/instance_{0}".format(memory_port)):
						backup_data = to_response(requests.get("http://{0}:{1}/send/main".format(IP, memory_port))) 
					else:
						print("Error: Backup Server Down. Unable to Fetch data. Exiting...")
						exit(0)

					for record in backup_data.split(";"):
						key, value = record.split(":")
						backup_db[key] = value
						client.create("/mapping/class_{0}/backup/{1}".format(str(CLASS), key), ephemeral = True)
						client.set("/mapping/class_{0}/backup/{1}".format(str(CLASS), key), value.encode())
					
					print(">>> Done fetching the data from neighbouring servers.")
					print(">>> Server setup complete")
	else:
		client.ensure_path("/servers")
		print("Electing Master")
		client.create("/servers/master", ephemeral = True)
		client.set("/servers/master", str(PORT).encode())

		client.ensure_path("/servers/slaves")

		seq_number = client.create("/servers/slaves/instance_{0}".format(PORT), ephemeral=True)
		client.set("/servers/slaves/instance_{0}".format(PORT), PORT.encode())

		if client.exists("/servers/status"):
			client.delete("/servers/status")

		client.create("/servers/status", ephemeral = True)
		print("Initializing the cluster.")
		client.set("/servers/status", "INITIALIZING".encode())

		time.sleep(25)  # Value in seconds

		print("Status set to ready.")
		client.set("/servers/status", "READY".encode())

		WORKER_PATH = "/servers/slaves"
		
		workers = [znode for znode in client.get_children(WORKER_PATH)]

		worker_nodes = [to_string(client.get(WORKER_PATH + "/" + child)[0]) for child in workers]


		NUMBER_OF_SERVERS = len(workers)

		for index,worker in enumerate(worker_nodes):

			if worker != PORT:
				respose = to_response(requests.get("http://{0}:{1}/setup/mapping/{2}".format(IP, worker, str(index))))

				if respose == worker:
					print("Mapping Acknowledged by {0}".format(worker))
			else:
				print("Master is mapped to Class = {0}".format(index))

				client.ensure_path("/mapping")
				if not client.exists("/mapping/class_{0}".format(str(index))):
					client.create("/mapping/class_{0}".format(str(index)))
				client.set("/mapping/class_{0}".format(str(index)), PORT.encode())

		print("Sending Total Number of servers to all servers:")

		for index,worker in enumerate(worker_nodes):

			if worker != PORT:
				respose = to_response(requests.get("http://{0}:{1}/setup/length/{2}".format(IP, worker, str(NUMBER_OF_SERVERS))))

				if respose == worker:
					print("Total # of servers acknowledged by {0}".format(worker))

		client.ensure_path("/servers/count_servers")
		client.set("/servers/count_servers", str(NUMBER_OF_SERVERS).encode())


	print("Sequence Number:"+str(seq_number))       

except:
	raise ConnectionClosedError("Connection is closed")
	client.close()
	exit(0)

class GetRequest:
	def on_get(self, req, resp, key):

		global memory_db, client
		
		if key in memory_db.keys():
			value = memory_db[key]
			print("* Client queried for key: {0}".format(key))
			resp.media =  "Received Key:{0}. Corresponding Value: {1} ".format(key, value)
		
		else:
			resp.media = "Error: Key {0} does not exist in the DB.".format(key)

class PutRequest:
	def on_get(self, req, resp, key_value):
		
		global memory_db, client, NUMBER_OF_SERVERS

		key, value = str(key_value).split("&")
		memory_db[key] = value
		print("* Added {0} to the database".format(key))

		client.ensure_path("/mapping/class_{0}".format(hash_function(key)%NUMBER_OF_SERVERS))
		client.ensure_path("/mapping/class_{0}/memory".format(hash_function(key)%NUMBER_OF_SERVERS))
		if not client.exists("/mapping/class_{0}/memory/{1}".format(hash_function(key)%NUMBER_OF_SERVERS, key)):
			client.create("/mapping/class_{0}/memory/{1}".format(hash_function(key)%NUMBER_OF_SERVERS, key), ephemeral = True)
		client.set("/mapping/class_{0}/memory/{1}".format(hash_function(key)%NUMBER_OF_SERVERS, key), value.encode())

		backup_server_class = (hash_function(key) + 1) % NUMBER_OF_SERVERS
		backup_server_port = to_string(client.get("/mapping/class_{0}".format(backup_server_class))[0])

		try:
			response = to_response(requests.get("http://{0}:{1}/backup/{2}&{3}".format(IP, backup_server_port, key, value)))
			if response == backup_server_port:
				print("Key {0} has a backup on Server: {1}".format(key, backup_server_port))

			resp.media = "Received Key: {0}, Received Value: {1}. Value successfully added to the database.".format(*str(key_value).split("&"))

		except:
			response = "Error: Backup Server Down for Key: {0}".format(key)
			print(response)
			resp.media = response

		

class BackupRequest:
	def on_get(self, req, resp, key_value):
		
		global backup_db, client

		key, value = str(key_value).split("&")
		backup_db[key] = value
		
		# print("Received Key: {0}, Received Value: {1}. Value successfully added to the backup database.".format(*str(key_value).split("&")))

		client.ensure_path("/mapping/class_{0}".format((hash_function(key)+1)%NUMBER_OF_SERVERS))
		client.ensure_path("/mapping/class_{0}/backup".format((hash_function(key)+1)%NUMBER_OF_SERVERS))
		if not client.exists("/mapping/class_{0}/backup/{1}".format((hash_function(key)+1)%NUMBER_OF_SERVERS, key)):
			client.create("/mapping/class_{0}/backup/{1}".format((hash_function(key)+1)%NUMBER_OF_SERVERS, key), ephemeral = True)
		client.set("/mapping/class_{0}/backup/{1}".format((hash_function(key)+1)%NUMBER_OF_SERVERS, key), value.encode())
		
		print("** Added key:{0} mapping as a backup".format(key))

		resp.media = PORT


class setupRequest:
	def on_get(self, req, resp, request_type, data):
		# print(request_type)
		# print(data)
		global NUMBER_OF_SERVERS, CLASS

		if request_type == "mapping":
			client.ensure_path("/mapping")
			if not client.exists("/mapping/class_{0}".format(str(data))):
				client.create("/mapping/class_{0}".format(str(data)))
			client.set("/mapping/class_{0}".format(str(data)), PORT.encode())
			print("Received Mapping Data: Class = {0}".format(data))
			CLASS = int(data)
			resp.media = PORT
		elif request_type == "length":
			NUMBER_OF_SERVERS = int(data)
			resp.media = PORT


class queryRequest:
	def on_get(self, req, resp, key):
		global NUMBER_OF_SERVERS

		key_class = hash_function(key) % NUMBER_OF_SERVERS

		if client.exists("/mapping/class_{0}".format(key_class)):
			server_port = to_string(client.get("/mapping/class_{0}".format(key_class))[0])
			resp.media = server_port
		else:
			resp.media = "The corresponding server is down. HA is yet to be implemented" #here

class retryRequest:
	def on_get(self, req, resp, key):
		global NUMBER_OF_SERVERS

		backup_class = (hash_function(key) + 1) % NUMBER_OF_SERVERS

		if client.exists("/mapping/class_{0}".format(backup_class)):
			resp.media = to_string(client.get("/mapping/class_{0}".format(backup_class))[0])
		else:
			resp.media = "Error : Backup server down!"

class getBackupRequest:
	def on_get(self, req, resp, key):
		global backup_db, client
		
		if key in backup_db.keys():
			value = backup_db[key]
			print("** Client queried for Key: {0} from Backup DB".format(key))
			resp.media =  "Received Key:{0}. Corresponding Value: {1} ".format(key, value)
		
		else:
			resp.media = "Error: Key {0} does not exist in the Backup DB.".format(key)
		
class putBackupRequest:
	def on_get(self, req, resp, key_value):

		global backup_db, client, NUMBER_OF_SERVERS

		key, value = str(key_value).split("&")
		backup_db[key] = value
		print("** Added {0} to the Backup DB".format(key))

		client.ensure_path("/mapping/class_{0}".format((hash_function(key) + 1)%NUMBER_OF_SERVERS))
		client.ensure_path("/mapping/class_{0}/backup".format((hash_function(key) + 1)%NUMBER_OF_SERVERS))
		if not client.exists("/mapping/class_{0}/backup/{1}".format((hash_function(key) + 1)%NUMBER_OF_SERVERS, key)):
			client.create("/mapping/class_{0}/backup/{1}".format((hash_function(key) + 1)%NUMBER_OF_SERVERS, key), ephemeral = True)
		client.set("/mapping/class_{0}/backup/{1}".format((hash_function(key) + 1)%NUMBER_OF_SERVERS, key), value.encode())

		resp.media = "** Added key:{0} mapping as a backup".format(key)

class sendData:
	def on_get(self, req, resp, database):
		global memory_db, backup_db, client

		if database == 'main':
			resp.media = ";".join(["{0}:{1}".format(item[0], item[1]) for item in memory_db.items()])
		elif database == 'backup':
			resp.media = ";".join(["{0}:{1}".format(item[0], item[1]) for item in backup_db.items()])


api = falcon.API()
api.add_route('/get/{key}', GetRequest())
api.add_route('/put/{key_value}', PutRequest())
api.add_route('/backup/{key_value}', BackupRequest())
api.add_route('/setup/{request_type}/{data}', setupRequest())
api.add_route('/query/{key}', queryRequest())
api.add_route('/retry/{key}', retryRequest())
api.add_route('/getbackup/{key}', getBackupRequest())
api.add_route('/putbackup/{key_value}', putBackupRequest())
api.add_route('/send/{database}', sendData())

serve(api, listen = "0.0.0.0:{0}".format(PORT))