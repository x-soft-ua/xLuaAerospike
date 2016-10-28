#Calling Aerospike from Lua
###Build Instructions

Run the build script located in the root directory of the repository to build the library "xLuaAerospikeLayer.so"

```
./build_linux.sh
```

The shared library “as_lua.so” has dependencies on the Aerospike C Client API. Refer to the [Aerospike C Client](https://docs.aerospike.com/pages/viewpage.action?pageId=3807998) documentation for instructions on insallation and dependencies.


**IMPORTANT:** The shared library as_lua.so should be placed in the Lua library path:
```
./xLuaAerospikeLayer.so
/usr/local/lib/lua/5.1/xLuaAerospikeLayer.so
/usr/lib/lua/5.1/xLuaAerospikeLayer.so
```
