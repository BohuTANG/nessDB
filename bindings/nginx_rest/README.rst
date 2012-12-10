
Nginx REST API for NessDB
============================================================

Compile
------------------------------------------------------------

this api depend on `nginx lua module <http://wiki.nginx.org/HttpLuaModule#Installation>`_ and `nessdb lua binding <lua>`_.

* build nginx lua module

* build nessdb lua binding

Install
------------------------------------------------------------

if you hava install nginx in installdir '/opt/nginx', then make subdir libs, and cp so and lua to that dir. cp nginx.conf to installdir.::
    
    .
    ├── libs
    │   ├── libnessdb.so
    │   ├── nessdb.lua
    │   ├── nessdb.so
    │   └── rest.lua
    ├── logs
    ├── nginx
    ├── nginx.conf
    └── run

Config
------------------------------------------------------------

you can change datapath and key in nginx.conf. see nginx.conf::

    set $db_path '/tmp/nessdb';   # Nessdb Store Path
    set $key $uri$args;           # Nessdb Key
    
Run
------------------------------------------------------------

you can use below run file to start nginx::
    
    #! /bin/bash 
    ulimit -n 32768
    export LD_LIBRARY_PATH=`pwd`/libs
    ./nginx -c `pwd`/nginx.conf -p `pwd`/ $@

Use
------------------------------------------------------------

* SET::

    curl -X PUT -d "testdata" "http://127.0.0.1:8090/test1"

* GET::
    
    curl -X GET "http://127.0.0.1:8090/test1"

* DELETE::

    curl -X DELETE "http://127.0.0.1:8090/test1"

* Exist::
    
    curl -X HEAD "http://127.0.0.1:8080/test1"

