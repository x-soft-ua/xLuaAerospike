--
-- Lua Aerospike connection layer
--
local as = require "xLuaAerospikeLayer"

local sysmem = ngx.shared.sysmem;

local M = {}

local function get_from_as(host, port, ns, set, where, whereval, query_timeout, operation)

    if (not operation) then operation = 'none' end

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

    if(not whereval) then return -2 end

    --shm key
    shamem_id = 'hc'..host..port..worker_id

    --init new aerospike connection flag
    newexec = 0
    if(not sysmem:get('newexec'..shamem_id)) then
        sysmem:set('newexec'..shamem_id,1)
        newexec = 1
    end

    get_pk = 0
    if(operation=='get_pk') then
        --get record via primary key
        response, err, msg = as.query_as(newexec, shamem_id, host, port, ns, set, where, whereval, query_timeout, 1)
    elseif(operation=='lstack_peek') then
        --get large stack via secondary key
        response, err, msg = as.lstack_as(newexec, shamem_id, host, port, ns, set, whereval, where, LSTACK_PEEK, 50)
    else
        --get record via secondary key
        response, err, msg = as.query_as(newexec, shamem_id, host, port, ns, set, where, whereval, query_timeout, 0)
    end


    if(err==0 and type(response)=='table') then
        return response, 1
        --local raw_json_response = json:encode(response)
        --return raw_json_response
    end

    if(err==AS_NEED_RECONNECT) then
        err, message = ngx.disconnect_as(shamem_id)
        sysmem:delete('newexec'..shamem_id)
        return -1
    end
end

M.get_from_as = get_from_as

return M