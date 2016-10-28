local as_layer = require "lib.xLuaAerospike"

host = '127.0.0.1'
port = 3000
query_timeout = 10
ns = 'test'
set = 'demo'
where = 'pk'
whereval = 'key1'

return_table_dmp_rec, return_ok = as_layer.get_from_as(host, port, ns, set, where, whereval, query_timeout, 'get_pk')

if(return_ok) then
    --return_table_dmp_rec
end

