import requests

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


MASTER = None
BASE_IP = "0.0.0.0"


def to_string(value):
    return str(value.decode())


def to_response(string):
    return string.content.decode('utf-8').strip("\"")


def zk_status_listener(state):
    if state == KazooState.LOST:
        print("Register somewhere that the session was lost")
        exit(0)

    elif state == KazooState.SUSPENDED:
        print("Handle being disconnected from Zookeeper")
        exit(0)


try:
    logging.basicConfig()
    zkr = KazooRetry(max_tries=-1)
    client = KazooClient(hosts="127.0.0.1:2181",
                         connection_retry=zkr, read_only=True)
    client.add_listener(zk_status_listener)
    client.start()

except:
    raise ConnectionClosedError("Connection is closed")
    client.close()
    exit(0)


while True:

    if client.exists("/servers/status") and to_string(client.get("/servers/status")[0]) == "READY":

        if client.exists("/servers/master"):
            MASTER = to_string(client.get("/servers/master")[0])

        IP = "0.0.0.0:" + MASTER

        try:
            print("Commands:")
            print("1. Put(char* key, char* value)")
            print("2. Get(char* key) or GetMultiple(char** keys)")

            choice = int(input())

            if client.exists("/servers/master"):
                MASTER = to_string(client.get("/servers/master")[0])

            IP = "0.0.0.0:" + MASTER

            if choice == 1:

                key, value = input().split()

                server_port = to_response(requests.get(
                    "http://{0}/query/{1}".format(IP, key)))

                try:
                    response = to_response(requests.get("http://{2}/put/{0}&{1}".format(key, value, BASE_IP + ':' + server_port)))
                    print(response)
                except:
                    print("Main server down for Key: {0}".format(key))
                    try:
                        backup_port = to_response(requests.get("http://{0}/retry/{1}".format(IP, key)))

                        if "Error" in backup_port:
                            print("Backup server down for Key:{0}".format(key))
                        else:
                            response = to_response(requests.get(
                                "http://{0}/putbackup/{1}&{2}".format(BASE_IP + ':' + backup_port, key, value)))
                            print("** Response from Backup Server")
                            print(response)
                    except:
                        print(
                            "*** Backup Server down for Key: {0}".format(key))

            elif choice == 2:

                keys = list(input().split())
                for key in keys:

                    server_port = to_response(requests.get(
                        "http://{0}/query/{1}".format(IP, key)))
                    # print("Response received")
                    response = None

                    try:
                        response = to_response(requests.get(
                            "http://{1}/get/{0}".format(key, BASE_IP + ":" + server_port)))
                        print(response)
                    except:

                        try:
                            backup_port = to_response(requests.get(
                                "http://{0}/retry/{1}".format(IP, key)))

                            if "Error" in backup_port:
                                print(
                                    "Backup server doesn't have the key:{0}".format(key))
                            else:
                                response = to_response(requests.get(
                                    "http://{0}/getbackup/{1}".format(BASE_IP + ':' + backup_port, key)))
                                print("** Response from Backup Server")
                                print(response)

                        except:
                            print(
                                "*** Backup Server Down for Key: {0}".format(key))

        except (KeyboardInterrupt, SystemExit):
            exit(0)
        except Exception as e:
            print("Connection to the Server not possible: {0}".format(e))

    else:
        print("Waiting for the server to be ready.")
        time.sleep(0.5)
        if client.exists("/servers/status"):
            print("Current Status:" +
                  to_string(client.get("/servers/status")[0]))
        else:
            print("Master Server Down")
