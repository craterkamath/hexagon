# hexagon
[![forthebadge made-with-python](http://ForTheBadge.com/images/badges/made-with-python.svg)](https://www.python.org/) [![PyPI pyversions](https://img.shields.io/pypi/pyversions/ansicolortags.svg)](https://pypi.python.org/pypi/ansicolortags/) [![Open Source Love svg2](https://badges.frapsoft.com/os/v2/open-source.svg?v=103)](https://github.com/ellerbrock/open-source-badges/) [![ForTheBadge built-with-love](http://ForTheBadge.com/images/badges/built-with-love.svg)](https://GitHub.com/Naereen/) [![GitHub license](https://img.shields.io/github/license/Naereen/StrapDown.js.svg)](https://github.com/Naereen/StrapDown.js/blob/master/LICENSE)

A distributed key values store running client server model underneath.

----

To run the server

```
python server.py <port number>
```

> It is recommended to run multiple servers on different ports to use all the feature offered by the system. 

To run the client

```
python client.py
```

----

The client can handle:

- PUT REQUEST: To store the key value pair in the server.
- GET REQUEST: To retrieve a value for the required key.
- GET MULTIPLE: To get required values for a list of keys. 

#### High availability is implemented using a backup hash table on the server (SERVER_ID + 1) % NUM_OF_SERVERS for each server.

----

### License

The code is made available under the [MIT License](https://opensource.org/licenses/mit-license.php). Enhancements are welcomed.

----

