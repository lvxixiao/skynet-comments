local skynet = require "skynet"
local sc = require "skynet.socketchannel"
local socket = require "skynet.socket"
local cluster = require "skynet.cluster.core"

local channel
local session = 1
local node, nodename, init_host, init_port = ...

local command = {}

local function send_request(addr, msg, sz)
	-- msg is a local pointer, cluster.packrequest will free it
	local current_session = session
	local request, new_session, padding = cluster.packrequest(addr, session, msg, sz)
	session = new_session

	local tracetag = skynet.tracetag()
	if tracetag then
		if tracetag:sub(1,1) ~= "(" then
			-- add nodename
			local newtag = string.format("(%s-%s-%d)%s", nodename, node, session, tracetag)
			skynet.tracelog(tracetag, string.format("session %s", newtag))
			tracetag = newtag
		end
		skynet.tracelog(tracetag, string.format("cluster %s", node))
		channel:request(cluster.packtrace(tracetag))
	end
	-- padding 表示包过大被切分成了多个包, 会利用低优先级通道发, 极端情况下可能没有空闲发。
	-- request接口会等待请求返回
	return channel:request(request, current_session, padding)
end

--comment 发送请求, 等待返回
function command.req(...)
	--[[
		疑问, 返回数据为什么会是在这里, 而不是在 gateserver 的 socket 类型消息
		答: 这个连接是由本服务发起的, 不需要调用 socket.start 来添加 EPOLLIN 事件, socket消息会发送给发起连接的服务
	]]
	local ok, msg = pcall(send_request, ...)
	if ok then
		-- msg 是个 table 表示消息由多个短小的消息合成。
		if type(msg) == "table" then
			skynet.ret(cluster.concat(msg))
		else
			skynet.ret(msg)
		end
	else
		skynet.error(msg)
		skynet.response()(false)
	end
end

--comment 发送请求, 抛弃返回数据
function command.push(addr, msg, sz)
	local request, new_session, padding = cluster.packpush(addr, session, msg, sz)
	if padding then	-- is multi push
		session = new_session
	end

	channel:request(request, nil, padding)
end

local function read_response(sock)
	local sz = socket.header(sock:read(2))
	local msg = sock:read(sz)
	return cluster.unpackresponse(msg)	-- session, ok, data, padding
end

function command.changenode(host, port)
	if not host then
		skynet.error(string.format("Close cluster sender %s:%d", channel.__host, channel.__port))
		channel:close()
	else
		channel:changehost(host, tonumber(port))
		channel:connect(true)
	end
	skynet.ret(skynet.pack(nil))
end

skynet.start(function()
	--[[
		session 模式
	]]
	channel = sc.channel {
			host = init_host,
			port = tonumber(init_port),
			response = read_response,
			nodelay = true,
		}
	skynet.dispatch("lua", function(session , source, cmd, ...)
		local f = assert(command[cmd])
		f(...)
	end)
end)
