local skynet = require "skynet"

local clusterd
local cluster = {}
local sender = {} --<node, clustersender服务>
local task_queue = {}

local function repack(address, ...)
	return address, skynet.pack(...)
end

local function request_sender(q, node)
	local ok, c = pcall(skynet.call, clusterd, "lua", "sender", node)
	if not ok then
		skynet.error(c)
		c = nil
	end
	-- run tasks in queue
	local confirm = coroutine.running()
	q.confirm = confirm
	q.sender = c
	--执行获得 sender 服务期间的请求
	for _, task in ipairs(q) do
		if type(task) == "string" then
			if c then
				skynet.send(c, "lua", "push", repack(skynet.unpack(task)))
			end
		else
			--这里并不是唤醒 task 协程, 而是将它放入待唤醒队列, 在 skynet.wait 调用时从待唤醒队列找一个最近的协程恢复执行
			skynet.wakeup(task)
			skynet.wait(confirm)
		end
	end
	task_queue[node] = nil
	sender[node] = c
end

local function get_queue(t, node)
	local q = {}
	t[node] = q
	skynet.fork(request_sender, q, node)
	return q
end

setmetatable(task_queue, { __index = get_queue } )

--获得 clustersender 服务的地址
local function get_sender(node)
	local s = sender[node]
	if not s then
		--[[
			获得 sender 期间的请求会保存在task, 在 sender 地址返回之后, 由协程唤醒执行
		]]
		local q = task_queue[node]
		local task = coroutine.running()
		table.insert(q, task)
		skynet.wait(task)
		--[[
			有点疑惑, 这里只是加入唤醒队列, 但是接下来的流程应该没有调用唤醒才对?
			skynet的协程, 在执行时, 都会是这样调用 suspend(co, coroutine_resume(co, ...))
			在 co_create 函数中, 执行完协程函数后, 会调用 coroutine_yield "SUSPEND",
			此时 SUSPEND 作为 coroutine_resume 的返回值会传递给 suspend 函数, 
			接收到这个字符串会让 suspend 挑选一个挂起的协程执行
		]]
		skynet.wakeup(q.confirm)
		return q.sender
	end
	return s
end

cluster.get_sender = get_sender

function cluster.call(node, address, ...)
	-- skynet.pack(...) will free by cluster.core.packrequest
	local s = sender[node]
	if not s then
		local task = skynet.packstring(address, ...)
		return skynet.call(get_sender(node), "lua", "req", repack(skynet.unpack(task)))
	end
	return skynet.call(s, "lua", "req", address, skynet.pack(...))
end

function cluster.send(node, address, ...)
	-- push is the same with req, but no response
	local s = sender[node]
	if not s then
		table.insert(task_queue[node], skynet.packstring(address, ...))
	else
		skynet.send(sender[node], "lua", "push", address, skynet.pack(...))
	end
end

function cluster.open(port, maxclient)
	if type(port) == "string" then
		return skynet.call(clusterd, "lua", "listen", port, nil, maxclient)
	else
		return skynet.call(clusterd, "lua", "listen", "0.0.0.0", port, maxclient)
	end
end

function cluster.reload(config)
	skynet.call(clusterd, "lua", "reload", config)
end

function cluster.proxy(node, name)
	return skynet.call(clusterd, "lua", "proxy", node, name)
end

function cluster.snax(node, name, address)
	local snax = require "skynet.snax"
	if not address then
		address = cluster.call(node, ".service", "QUERY", "snaxd" , name)
	end
	local handle = skynet.call(clusterd, "lua", "proxy", node, address)
	return snax.bind(handle, name)
end

function cluster.register(name, addr)
	assert(type(name) == "string")
	assert(addr == nil or type(addr) == "number")
	return skynet.call(clusterd, "lua", "register", name, addr)
end

function cluster.unregister(name)
	assert(type(name) == "string")
	return skynet.call(clusterd, "lua", "unregister", name)
end

function cluster.query(node, name)
	return skynet.call(get_sender(node), "lua", "req", 0, skynet.pack(name))
end

skynet.init(function()
	clusterd = skynet.uniqueservice("clusterd")
end)

return cluster
