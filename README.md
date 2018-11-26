# hexagon
A distributed key values store running client server model underneath.

To run the server

```
python server.py <port number>
```

> It is recommended to run multiple servers on different ports to use all the feature offered by the system. 

To run the client

```
python client.py
```

The client can handle:

- PUT REQUEST: To store the key value pair in the server.
- GET REQUEST: To retrieve a value for the required key.
- GET MULTIPLE: To get required values for a list of keys. 

#### High availability is implemented using a backup hash table on the server (SERVER_ID + 1) % NUM_OF_SERVERS for each server.

