local skynet = require "skynet"
local netpack = require "skynet.netpack"
local socketdriver = require "skynet.socketdriver"

local gateserver = {}

local socket	-- listen socket
local queue		-- message queue
local maxclient	-- max client
local client_number = 0
local CMD = setmetatable({}, { __gc = function() netpack.clear(queue) end })
local nodelay = false

local connection = {}
-- true : connected
-- nil : closed
-- false : close read

function gateserver.openclient(fd)
	if connection[fd] then
		--调用start之后本服务才会收到socket数据, 并且该socket才可以开始写数据
		socketdriver.start(fd)
	end
end

function gateserver.closeclient(fd)
	local c = connection[fd]
	if c ~= nil then
		connection[fd] = nil
		socketdriver.close(fd)
	end
end

function gateserver.start(handler)
	assert(handler.message)
	assert(handler.connect)

	local listen_context = {}

	function CMD.open( source, conf )
		assert(not socket)
		local address = conf.address or "0.0.0.0"
		local port = assert(conf.port)
		maxclient = conf.maxclient or 1024
		nodelay = conf.nodelay
		skynet.error(string.format("Listen on %s:%d", address, port))
		--backlog是最大排队等待连接的数量
		socket = socketdriver.listen(address, port, conf.backlog)
		listen_context.co = coroutine.running()
		listen_context.fd = socket
		skynet.wait(listen_context.co)
		conf.address = listen_context.addr
		conf.port = listen_context.port
		listen_context = nil
		socketdriver.start(socket)
		if handler.open then
			return handler.open(source, conf)
		end
	end

	function CMD.close()
		assert(socket)
		socketdriver.close(socket)
	end

	local MSG = {}

	local function dispatch_msg(fd, msg, sz)
		if connection[fd] then
			handler.message(fd, msg, sz)
		else
			skynet.error(string.format("Drop message from fd (%d) : %s", fd, netpack.tostring(msg,sz)))
		end
	end

	MSG.data = dispatch_msg

	local function dispatch_queue()
		local fd, msg, sz = netpack.pop(queue)
		if fd then
			-- may dispatch even the handler.message blocked
			-- If the handler.message never block, the queue should be empty, so only fork once and then exit.
			skynet.fork(dispatch_queue)
			dispatch_msg(fd, msg, sz)

			for fd, msg, sz in netpack.pop, queue do
				dispatch_msg(fd, msg, sz)
			end
		end
	end

	MSG.more = dispatch_queue

	function MSG.open(fd, msg)
		client_number = client_number + 1
		if client_number >= maxclient then
			socketdriver.shutdown(fd)
			return
		end
		if nodelay then
			socketdriver.nodelay(fd)
		end
		connection[fd] = true
		handler.connect(fd, msg)
	end

	function MSG.close(fd)
		if fd ~= socket then
			client_number = client_number - 1
			if connection[fd] then
				connection[fd] = false	-- close read
			end
			if handler.disconnect then
				handler.disconnect(fd)
			end
		else
			socket = nil
		end
	end

	function MSG.error(fd, msg)
		if fd == socket then
			skynet.error("gateserver accept error:",msg)
		else
			socketdriver.shutdown(fd)
			if handler.error then
				handler.error(fd, msg)
			end
		end
	end

	function MSG.warning(fd, size)
		if handler.warning then
			handler.warning(fd, size)
		end
	end

	function MSG.init(id, addr, port)
		if listen_context then
			local co = listen_context.co
			if co then
				assert(id == listen_context.fd)
				listen_context.addr = addr
				listen_context.port = port
				skynet.wakeup(co)
				listen_context.co = nil
			end
		end
	end

	skynet.register_protocol {
		name = "socket",
		id = skynet.PTYPE_SOCKET,	-- PTYPE_SOCKET = 6
		unpack = function ( msg, sz )
			--[[
				-- filter 返回 type 和 参数
				type 种类 “data”, “more”, “error”, “open”, “close”

				对于SOCKET_DATA事件，filter会进行数据分包，如果分包后刚好有一条完整消息，filter返回的type为”data”，其后跟fd msg size。 如果不止一条消息，那么数据将被依次压入queue参数中，并且仅返回一个type为”more”。 queue是一个userdata，可以通过netpack.pop 弹出queue中的一条消息。

				其余type类型”open”，”error”, “close”分别SOCKET_ACCEPT, SOCKET_ERROR, SOCKET_CLOSE事件。

				关于包格式 2字节表示大小+数据, 大端编码
				netpack 会尽可能多地分包，交给上层。并且通过一个哈希表保存每个套接字ID对应的粘包，在下次数据到达时，取出上次留下的粘包数据，重新分包.
				https://github.com/cloudwu/skynet/wiki/GateServer
			]]
			return netpack.filter( queue, msg, sz)
		end,
		dispatch = function (_, _, q, type, ...)
			queue = q
			if type then
				MSG[type](...)
			end
		end
	}

	local function init()
		skynet.dispatch("lua", function (_, address, cmd, ...)
			local f = CMD[cmd]
			if f then
				skynet.ret(skynet.pack(f(address, ...)))
			else
				skynet.ret(skynet.pack(handler.command(cmd, address, ...)))
			end
		end)
	end

	if handler.embed then
		init()
	else
		skynet.start(init)
	end
end

return gateserver
