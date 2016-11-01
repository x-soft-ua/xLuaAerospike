local json = require "lib.json"


local asKey = 'new_test_key'

local as_layer = require "lib.xLuaAerospike"
local as_connection = as_layer.as_conf_init("127.0.0.1", 3000, "test", "demo")

local count, status = as_layer.as_bin_incr(as_connection, asKey, "incr", 15)
if (status == as_layer.AS_LAYER_OK) then
    ngx.say("Incr count = "..count)
end

local status = as_layer.as_bin_set(as_connection, asKey, "strval", ngx.md5(ngx.now()))
if (status == as_layer.AS_LAYER_OK) then
    ngx.say("Set strval ok")
end

local status = as_layer.as_bin_set(as_connection, asKey, "intval", ngx.now())
if (status == as_layer.AS_LAYER_OK) then
    ngx.say("Set intval ok")
end

local rec, status = as_layer.as_get_record(as_connection, asKey)
if (status == as_layer.AS_LAYER_OK) then
    ngx.say("Get record = " .. json:encode(rec))
end
