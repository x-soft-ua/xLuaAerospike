--
--  InitW module
--

--get worker id
wpid = ngx.worker.pid()

--get shared memc
local sysmem = ngx.shared.sysmem;

--save workerid
workers_id = sysmem:incr('worker_id',1)
sysmem:set('w'..wpid, workers_id)

