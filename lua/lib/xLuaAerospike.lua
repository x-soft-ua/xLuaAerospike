--
-- Lua Aerospike connection layer
--
local as = require "xLuaAerospikeLayer"

local sysmem = ngx.shared.sysmem;

local Method = {}

Method.host = "127.0.0.1"
Method.port = 3000
Method.ns = "test"
Method.set = "demo"
Method.bin_name = ""
Method.binval_str = ""
Method.binval_int = 0
Method.key_str = ""
Method.query_timeout = 10
Method.operation = 1


local function as_method(conf)

    for i,v in pairs(conf) do
        Method[i] = v
    end

    --get worker pid
    wpid = ngx.worker.pid()

    --get worker id
    worker_id = sysmem:get('w'..wpid)

    AS_OK               = 0
    AS_NEED_RECONNECT   = 1
    AS_SHM_ERR          = 2
    AS_SHM_CREATE_ERR   = 3
    AS_SHM_TRUNCATE_ERR = 4
    AS_CLIENT_ERROR     = 5
    LSTACK_PEEK         = 100

    AS_STR              = 1
    AS_INT              = 2

    if(Method.key_str == "") then return -2 end

    --shm key
    shamem_id = 'hc' .. Method.host .. Method.port .. worker_id

    --init new aerospike connection flag
    newexec = 0
    if(not sysmem:get('newexec'..shamem_id)) then
        sysmem:set('newexec'..shamem_id,1)
        newexec = 1
    end

    get_pk = 0
    if(Method.operation=='get_pk') then
        --get record via primary key
        response, err, msg = as.query_as(newexec, shamem_id, Method.host, Method.port, Method.ns, Method.set, Method.bin_name, Method.key_str, Method.query_timeout, 1)
    elseif(Method.operation=='lstack_peek') then
        --get large stack via secondary key
        response, err, msg = as.lstack_as(newexec, shamem_id, Method.host, Method.port, Method.ns, Method.set, Method.bin_name, Method.key_str, LSTACK_PEEK, 50)
    elseif(Method.operation=='put_bin') then
        response, err, msg = as.put_bin(newexec, shamem_id, Method.host, Method.port, Method.ns, Method.set, Method.bin_name, Method.key_str, Method.binval_str, Method.binval_int, AS_STR)
    else
        --get record via secondary key
        response, err, msg = as.query_as(newexec, shamem_id, Method.host, Method.port, Method.ns, Method.set, Method.bin_name, Method.key_str, Method.query_timeout, 0)
    end

    if(err==0 and type(response)=='table') then
        return response, 1
        --local raw_json_response = json:encode(response)
        --return raw_json_response
    end

    if(err==AS_NEED_RECONNECT) then
        err, message = as.disconnect_as(shamem_id)
        sysmem:delete('newexec'..shamem_id)
        return -1
    end
end

Method.as_method = as_method

return Method