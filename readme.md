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

###Usage

Require library and init connection
```lua
local as_layer = require "lib.xLuaAerospike"
local as_connection = as_layer.as_conf_init("127.0.0.1", 3000, "test", "demo")
local asKey = "testkey"
```

Increment bin value
```lua
local count, status = as_layer.as_bin_incr(as_connection, asKey, "incr", 15)
if (status == as_layer.AS_LAYER_OK) then
    ngx.say("Incr count = "..count)
end
```

Set bin value
```lua
local status = as_layer.as_bin_set(as_connection, asKey, "foo", "bar")
if (status == as_layer.AS_LAYER_OK) then
    ngx.say("Set binval ok")
end
```

Read record 
```lua
local rec, status = as_layer.as_get_record(as_connection, asKey)
if (status == as_layer.AS_LAYER_OK) then
    ngx.say("Get record = " .. json:encode(rec))
end
```

