--
-- Lua Aerospike connection layer
--
local as = require "xLuaAerospikeLayer"

local sysmem = ngx.shared.sysmem;

local Method = {["default"] = {} }

AS_OK               = 0
AS_NEED_RECONNECT   = 1
AS_SHM_ERR          = 2
AS_SHM_CREATE_ERR   = 3
AS_SHM_TRUNCATE_ERR = 4
AS_CLIENT_ERROR     = 5
LSTACK_PEEK         = 100

AS_STR              = 1
AS_INT              = 2
AS_INCR             = 3

AS_LAYER_OK         = 1
AS_LAYER_ERR        = -1
AS_LAYER_ARGS_ERR   = -2

Method.default.host = "127.0.0.1"
Method.default.port = 3000
Method.default.ns = "test"
Method.default.set = "demo"
Method.default.bin_name = ""
Method.default.binval = ""
Method.default.key_str = ""
Method.default.query_timeout = 10
Method.default.operation = 0



local function as_init(currentConfId)

    --get worker pid
    wpid = ngx.worker.pid()

    --get worker id
    worker_id = sysmem:get('w'..wpid)

    --shm key
    shamem_id = 'hc' .. Method[currentConfId].host .. Method[currentConfId].port .. worker_id

    --init new aerospike connection flag
    newexec = 0
    if(not sysmem:get('newexec'..shamem_id)) then
        sysmem:set('newexec'..shamem_id,1)
        newexec = 1
    end

    return shamem_id, worker_id, newexec
end



local function as_conf_init(host, port, ns, set)
    local currentConfId = ngx.md5(host..port..ns..set)
    Method[currentConfId] = Method["default"]
    Method[currentConfId].host = host
    Method[currentConfId].port = port
    Method[currentConfId].ns = ns
    Method[currentConfId].set = set

    return currentConfId
end

local function as_bin_incr(currentConfId, key_str, bin_name, incr)
    if(key_str == "" or bin_name == "" or type(incr) ~= "number") then return AS_LAYER_ARGS_ERR end

    shamem_id, worker_id, newexec = as_init(currentConfId)
    response, err, msg = as.put_bin(newexec, shamem_id, Method[currentConfId].host, Method[currentConfId].port, Method[currentConfId].ns, Method[currentConfId].set, bin_name, key_str, "", incr, AS_INCR)


    if(err==0) then
        return response, AS_LAYER_OK
    end

    if(err==AS_NEED_RECONNECT) then
        err, message = as.disconnect_as(shamem_id)
        sysmem:delete('newexec'..shamem_id)
        return 0, AS_LAYER_ERR
    end
    return 0, AS_LAYER_ERR
end

local function as_bin_set(currentConfId, key_str, bin_name, bin_val)
    if(key_str == "" or bin_name == "" or not bin_val) then return AS_LAYER_ARGS_ERR end

    shamem_id, worker_id, newexec = as_init(currentConfId)
    if (type(bin_val) == "number") then
        response, err, msg = as.put_bin(newexec, shamem_id, Method[currentConfId].host, Method[currentConfId].port, Method[currentConfId].ns, Method[currentConfId].set, bin_name, key_str, "", bin_val, AS_INT)
    else
        response, err, msg = as.put_bin(newexec, shamem_id, Method[currentConfId].host, Method[currentConfId].port, Method[currentConfId].ns, Method[currentConfId].set, bin_name, key_str, bin_val, 0, AS_STR)
    end

    if(err==0) then
        return AS_LAYER_OK
    end

    if(err==AS_NEED_RECONNECT) then
        err, message = as.disconnect_as(shamem_id)
        sysmem:delete('newexec'..shamem_id)
        return AS_LAYER_ERR
    end
    return AS_LAYER_ERR
end

local function as_get_record(currentConfId, key_str, query_timeout)
    if(key_str == "") then return AS_LAYER_ARGS_ERR end
    if(not query_timeout) then query_timeout = 10 end

    shamem_id, worker_id, newexec = as_init(currentConfId)
    response, err, msg = as.query_as(newexec, shamem_id, Method[currentConfId].host, Method[currentConfId].port, Method[currentConfId].ns, Method[currentConfId].set, "", key_str, query_timeout, 1)

    if(err==0) then
        return response, AS_LAYER_OK
    end

    if(err==AS_NEED_RECONNECT) then
        err, message = as.disconnect_as(shamem_id)
        sysmem:delete('newexec'..shamem_id)
        return 0, AS_LAYER_ERR
    end
    return 0, AS_LAYER_ERR
end

local function as_get_record_by_second_key(currentConfId, key_str, query_timeout)
    if(key_str == "") then return AS_LAYER_ARGS_ERR end
    if(not query_timeout) then query_timeout = 10 end

    shamem_id, worker_id, newexec = as_init(currentConfId)
    response, err, msg = as.query_as(newexec, shamem_id, Method[currentConfId].host, Method[currentConfId].port, Method[currentConfId].ns, Method[currentConfId].set, "", key_str, query_timeout, 0)

    if(err==0) then
        return response, AS_LAYER_OK
    end

    if(err==AS_NEED_RECONNECT) then
        err, message = as.disconnect_as(shamem_id)
        sysmem:delete('newexec'..shamem_id)
        return 0, AS_LAYER_ERR
    end
    return 0, AS_LAYER_ERR
end

local function as_get_lstack(currentConfId, key_str, bin_name, stack_size)
    if(key_str == "" or bin_name == "") then return AS_LAYER_ARGS_ERR end
    if(not stack_size) then stack_size = 50 end

    shamem_id, worker_id, newexec = as_init(currentConfId)
    response, err, msg = as.lstack_as(newexec, shamem_id, Method[currentConfId].host, Method[currentConfId].port, Method[currentConfId].ns, Method[currentConfId].set, bin_name, key_str, LSTACK_PEEK, stack_size)

    if(err==0) then
        return response, AS_LAYER_OK
    end

    if(err==AS_NEED_RECONNECT) then
        err, message = as.disconnect_as(shamem_id)
        sysmem:delete('newexec'..shamem_id)
        return 0, AS_LAYER_ERR
    end
    return 0, AS_LAYER_ERR
end


local retObj = {}
retObj.as_conf_init = as_conf_init
retObj.as_bin_incr = as_bin_incr
retObj.as_get_record = as_get_record
retObj.as_get_record_by_second_key = as_get_record_by_second_key
retObj.as_get_lstack = as_get_lstack
retObj.as_bin_set = as_bin_set
retObj.AS_LAYER_OK = AS_LAYER_OK
retObj.AS_LAYER_ERR = AS_LAYER_ERR
retObj.AS_LAYER_ARGS_ERR = AS_LAYER_ARGS_ERR

return retObj