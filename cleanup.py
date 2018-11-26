from kazoo.client import KazooClient
from kazoo.client import KazooState
from kazoo.client import KazooRetry
from kazoo.exceptions import ConnectionClosedError


def zk_status_listener(state):
    if state == KazooState.LOST:
        print("Cleaned Working Tree.")
        # print("Session was lost")
        exit(0)

    elif state == KazooState.SUSPENDED:
        print("Handle being disconnected from Zookeeper")
        exit(0)


try:
    zkr = KazooRetry(max_tries=-1)
    client = KazooClient(hosts="127.0.0.1:2181", connection_retry=zkr)
    client.add_listener(zk_status_listener)
    client.start()
    if client.exists("/servers"):
        client.delete("/servers", recursive=True)
    if client.exists("/mapping"):
        client.delete("/mapping", recursive=True)

    client.stop()
    client.close()
except:
    raise ConnectionClosedError("Connection is closed")
    client.close()
    exit(0)
