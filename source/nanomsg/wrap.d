/**
    nanomsg is a socket library that provides several common communication patterns. It aims to make the networking layer fast, scalable, and easy to use.
    Implemented in C, it works on a wide range of operating systems with no further dependencies.

    This module implements a convenience wrapper API for nanomsg

    Authors: Laeeth Isharc and Atila Neves (Kaleidic Associates Advisory Limited)

    Example code:
        http://nanomsg.code.kaleidic.io/nanomsg.wrap.responder.html
        http://nanomsg.code.kaleidic.io/nanomsg.wrap.checkNanoSocket.html
        http://nanomsg.code.kaleidic.io/examples/nanomsg.examples.html

 */
module nanomsg.wrap;


import nanomsg.bindings;
import concepts: models;
public import std.typecons: Yes, No; // to facilitate using send, receive
import std.typecons : Flag;

/// wrapper for a string uri to connect to
struct ConnectTo {
    string uri;
}

/// wrapper for a string uri to bind to
struct BindTo {

    this(in string uri) @safe pure {
        this([uri]);
    }

    this(in string[] uris) @safe pure {
        this.uris = uris.dup;
    }

    string[] uris;
}

struct TotalDuration {
    import std.datetime: Duration;
    Duration value;
    alias value this;
}

struct RetryDuration {
    import std.datetime: Duration;
    Duration value;
    alias value this;
}


/**

    NanoSocket - high level wrapper for a nanomsg socket

*/
@models!(NanoSocket, isNanoSocket)
struct NanoSocket {

    import std.traits: isArray;
    import std.typecons: Flag;
    import std.datetime: Duration;

    /// nanomsg protocol
    enum Protocol {
        request,
        response,
        subscribe,
        publish,
        pull,
        push,
        pair,
        surveyor,
        respondent,
        bus,
    }

    /// nanomsg socket options
    enum Option {
        lingerMs, /// How long to try and send pending messages after nn_close. -1 means infinite
        sendBufferSize, // Size of the send buffer in bytes
        receiveBufferSize, // Size of the receive buffer in bytes
        receiveMaxSize, /// Maximum message size that can be received, in bytes
        sendTimeoutMs, /// How long in milliseconds it takes for send to timeout
        receiveTimeoutMs, /// How long in milliseconds it takes for receive to timeout
        reconnectIntervalMs, /// How long to wait to re-establish connection
        reconnectIntervalMax, /// Maximum reconnect interval
        sendPriority, /// Outbound priority for endpoints added to socket
        receivePriority, /// Inbout priority for endpoints added to socket
        ipv4Only, /// Self-explanatory
        socketName, /// Socket name for error reporting and statistics
        timeToLive, /// Number of hops before message is dropped
        subscribeTopic, /// Subscribe to topic
        unsubscribeTopic, /// Unsubscribe to topic
        tcpNoDelay, /// Disables Nagle's algorithm
        surveyorDeadlineMs, /// How long to wait for responses in milliseconds
    }

    /// this(this) disabled to avoid sockets being destroyed
    @disable this(this);

    /// invalid FD
    enum INVALID_FD = -1;

    /// constructor
    this(in Protocol protocol, in int domain = AF_SP) @safe {
        initialize(protocol, domain);
    }

    /// constructor
    this(in Protocol protocol, in BindTo bindTo, int domain = AF_SP) @safe {
        initialize(protocol, bindTo, domain);
    }

    /// constructor
    this(in Protocol protocol, in ConnectTo connectTo, int domain = AF_SP) @safe {
        initialize(protocol, connectTo, domain);
    }

    /// destructor
    ~this() @safe nothrow {
        close;
    }

    /// Same as the revelant constructor, can be used on exisiting objects
    void initialize(in Protocol protocol, in int domain = AF_SP) @trusted {
        int protocolToInt(Protocol protocol) {
            final switch(protocol) with(Protocol) {
                case request:
                    return NN_REQ;
                case response:
                    return NN_REP;
                case publish:
                    return NN_PUB;
                case subscribe:
                    return NN_SUB;
                case pull:
                    return NN_PULL;
                case push:
                    return NN_PUSH;
                case pair:
                    return NN_PAIR;
                case surveyor:
                    return NN_SURVEYOR;
                case respondent:
                    return NN_RESPONDENT;
                case bus:
                    return NN_BUS;
                }
        }

        close; // init can be called twice
        _nanoSock = nn_socket(domain, protocolToInt(protocol));
        _protocol = protocol;
        enforceNanoMsgRet(_nanoSock);
    }

    /// Same as the revelant constructor, can be used on exisiting objects
    void initialize(in Protocol protocol, in BindTo bindTo, int domain = AF_SP) @trusted {
        import std.string: replace;

        initialize(protocol, domain);

        // this is so it's easy to specify the same string
        // for both ends of the socket
        foreach(uri; bindTo.uris) {
            bind(uri.replace("localhost", "*"));
        }
    }

    /// Same as the revelant constructor, can be used on exisiting objects
    void initialize(in Protocol protocol, in ConnectTo connectTo, int domain = AF_SP) @trusted {
        initialize(protocol, domain);
        connect(connectTo.uri);

        version(Windows) {
            // on Windows sometimes the socket tries to send before the TCP handshake
            import core.thread;
            Thread.sleep(100.msecs);
        }
    }

    /// close socket
    void close() @trusted nothrow {
        if(_nanoSock != INVALID_FD) {
            _nanoSock.nn_close;
        }
    }

    /// set socket option to a value
    ref inout(NanoSocket) setOption(T)(Option option, T val) inout {
        const optionC = toOptionC(option);
        setOption(optionC.level, optionC.option, val);
        return this;
    }

    /// get socket option value
    T getOption(T)(Option option) const {
        const optionC = toOptionC(option);
        return getOption!T(optionC.level, optionC.option);
    }

    /**
       Receive bytes on this socket.
       Memory is allocated by nanomsg and deleted in the `NanoBuffer` destructor.
     */
    NanoBuffer receive(Flag!"blocking" blocking = Yes.blocking,
                       in string file = __FILE__,
                       in size_t line = __LINE__)
        @safe scope return const
    {
        static void[] buffer;
        return receiveImpl(buffer, blocking, file, line);
    }

    /**
       A version of `receive` that takes a user supplied buffer to fill
     */
    void[] receive(return scope void[] buffer,
                   Flag!"blocking" blocking = Yes.blocking,
                   in string file = __FILE__,
                   in size_t line = __LINE__)
        const @safe
    {
        auto ptr = &buffer[0];
        return receiveImpl(buffer, blocking, file, line).bytes;
    }


    /**
       Sends the bytes as expected. If the protocol is Request, then returns
       the response, otherwise returns an empty array.
     */
    NanoBuffer send(T)(T[] data,
                       Flag!"blocking" blocking = Yes.blocking,
                       in string file = __FILE__,
                       in size_t line = __LINE__)
        const
    {
        import std.conv: text;

        const sent = () @trusted { return nn_send(_nanoSock, data.ptr, data.length, flags(blocking)); }();
        enforceNanoMsgRet(sent, file, line);

        void[] empty;
        return () @trusted { return _protocol == Protocol.request
                ? receive(blocking)
                : NanoBuffer(cast(void[]) data, false /*shouldDelete*/);
        }();
    }

    /**
     Tries to send bytes to the other side.
     duration is how long to try for
     recvBlocking controls whether or not to block on reception of a response.
     This only matters when the protocol is request/response
     Returns the response if in request mode, otherwise an empty byte slice.
     */
    auto trySend(T)(T[] data, Duration totalDuration, Flag!"blocking" recvBlocking = Yes.blocking) {
        import std.datetime: msecs;
        return trySend(data, TotalDuration(totalDuration), RetryDuration(10.msecs), recvBlocking);
    }

    /**
     Tries to send bytes to the other side.
     duration is how long to try for
     recvBlocking controls whether or not to block on reception of a response.
     This only matters when the protocol is request/response
     Returns the response if in request mode, otherwise an empty byte slice.
     */
    NanoBuffer trySend(T)(T[] data,
                          TotalDuration totalDuration,
                          RetryDuration retryDuration,
                          Flag!"blocking" recvBlocking = Yes.blocking)
    {
        import std.exception: enforce;
        static if(__VERSION__ >= 2077)
            import std.datetime.stopwatch: StopWatch, AutoStart;
        else
            import std.datetime: StopWatch, AutoStart;
        import std.datetime: msecs;
        import std.conv: text;
        import core.thread: Thread;

        int sent;
        auto sw = StopWatch(AutoStart.yes);
        do {
            sent = () @trusted { return nn_send(_nanoSock, &data[0], data.length, flags(No.blocking)); }();
            if(sent != data.length) () @trusted { Thread.sleep(retryDuration); }();
        } while(sent != data.length && cast(Duration) sw.peek < totalDuration);

        enforce(sent == data.length,
                text("Expected to send ", data.length, " bytes but sent ", sent));

        return _protocol == Protocol.request ? receive(recvBlocking) : NanoBuffer();
    }

    /// connect
    void connect(in string uri, in string file = __FILE__, in size_t line = __LINE__) {
        import std.string: toStringz;
        enforceNanoMsgRet(nn_connect(_nanoSock, uri.toStringz), file, line);
        _uri = uri;
        _connection = Connection.connected;
    }

    /// bind
    void bind(in string uri, in string file = __FILE__, in size_t line = __LINE__) {
        import std.string: toStringz;
        enforceNanoMsgRet(nn_bind(_nanoSock, uri.toStringz), file, line);
        _uri = uri;
        _connection = Connection.bound;
    }

    /// get protocol
    Protocol protocol() @safe @nogc pure const nothrow {
        return _protocol;
    }

    /// get URI
    string uri() @safe @nogc pure const nothrow {
        return _uri;
    }

    /// toString
    string toString() @safe pure const {
        import std.conv: text;

        if(_connection == Connection.none)
            return text(protocol);

        const connText = _connection == Connection.bound ? "@" : "@@";
        return text(_protocol, connText, _uri);
    }

private:

    enum Connection {
        none,
        bound,
        connected,
    }

    int _nanoSock = INVALID_FD;
    Protocol _protocol;
    string _uri;
    Connection _connection;

    NanoBuffer receiveImpl(return scope void[] buffer,
                           Flag!"blocking" blocking = Yes.blocking,
                           in string file = __FILE__,
                           in size_t line = __LINE__)
        @safe return scope const
    {
        import std.algorithm: min;
        static import core.stdc.errno;

        void* nanomsgBuffer = null;
        // can't use &buffer[0] here since it might be empty
        const haveBuffer = () @trusted { return buffer.ptr !is null; }();
        const shouldDelete = !haveBuffer;

        auto recvPointer = () @trusted {
            return haveBuffer ? &buffer[0] : cast(void*) &nanomsgBuffer;
        }();


        const length = haveBuffer ? buffer.length : NN_MSG;
        const numBytes = () @trusted { return nn_recv(_nanoSock, recvPointer, length, flags(blocking)); }();

        bool isErrnoEagain() @trusted {
            return nn_errno == core.stdc.errno.EAGAIN;
        }

        if(blocking || (numBytes < 0 && !isErrnoEagain)) {
            enforceNanoMsgRet(numBytes, file, line);
        }

        auto pointer = haveBuffer ? &buffer[0] : nanomsgBuffer;
        const retSliceLength = haveBuffer ? min(numBytes, buffer.length) : numBytes;

        return numBytes >= 0
            ? NanoBuffer(() @trusted { return pointer[0 .. retSliceLength]; }(), shouldDelete)
            : NanoBuffer();
    }

    // the int level and option values needed by the nanomsg C API
    static struct OptionC {
        int level;
        int option;
    }

    static OptionC toOptionC(Option option) @safe {
        final switch(option) with(Option) {
            case lingerMs:
                return OptionC(NN_SOL_SOCKET, NN_LINGER);

            case sendBufferSize:
                return OptionC(NN_SOL_SOCKET, NN_SNDBUF);

            case receiveBufferSize:
                return OptionC(NN_SOL_SOCKET, NN_RCVBUF);

            case receiveMaxSize:
                return OptionC(NN_SOL_SOCKET, NN_RCVMAXSIZE);

            case sendTimeoutMs:
                return OptionC(NN_SOL_SOCKET, NN_SNDTIMEO);

            case receiveTimeoutMs:
                return OptionC(NN_SOL_SOCKET, NN_RCVTIMEO);

            case reconnectIntervalMs:
                return OptionC(NN_SOL_SOCKET, NN_RECONNECT_IVL);

            case reconnectIntervalMax:
                return OptionC(NN_SOL_SOCKET, NN_RECONNECT_IVL_MAX);

            case sendPriority:
                return OptionC(NN_SOL_SOCKET, NN_SNDPRIO);

            case receivePriority:
                return OptionC(NN_SOL_SOCKET, NN_RCVPRIO);

            case ipv4Only:
                return OptionC(NN_SOL_SOCKET, NN_IPV4ONLY);

            case socketName:
                return OptionC(NN_SOL_SOCKET, NN_SOCKET_NAME);

            case timeToLive:
                return OptionC(NN_SOL_SOCKET, NN_TTL);

            case subscribeTopic:
                return OptionC(NN_SUB, NN_SUB_SUBSCRIBE);

            case unsubscribeTopic:
                return OptionC(NN_SUB, NN_SUB_UNSUBSCRIBE);

            case tcpNoDelay:
                return OptionC(NN_TCP, NN_TCP_NODELAY);

            case surveyorDeadlineMs:
                return OptionC(NN_SURVEYOR, NN_SURVEYOR_DEADLINE);
        }
    }

    void setOption(T)(int level, int option, ref T val) const if(isArray!T) {
        const ret = () @trusted { return nn_setsockopt(_nanoSock, level, option, val.ptr, val.length); }();
        enforceNanoMsgRet(ret);
    }

    void setOption(T)(int level, int option, T val) const if(!isArray!T) {
        const ret = () @trusted { return nn_setsockopt(_nanoSock, level, option, &val, val.sizeof); }();
        enforceNanoMsgRet(ret);
    }

    T getOption(T)(int level, int option) const if(isArray!T) {
        import std.traits: Unqual;
        import std.conv: to;

        // ElementType!string is dchar, and we don't want that,
        // so instead we use this typeof
        alias U = Unqual!(typeof(T.init[0]))[1000];
        U val;
        ulong length = val.length;
        const ret = () @trusted {
            return nn_getsockopt(_nanoSock, level, option, cast(void*) val.ptr, &length);
        }();
        enforceNanoMsgRet(ret);
        return val[0 .. length].to!T;
    }

    T getOption(T)(int level, int option) const if(!isArray!T) {
        import std.exception: enforce;
        import std.conv: text;

        T val;
        size_t length = T.sizeof;
        const ret = () @trusted { return nn_getsockopt(_nanoSock, level, option, cast(void*) &val, &length); }();
        enforceNanoMsgRet(ret);
        enforce(length == T.sizeof,
                text("getsockopt returned ", length, " but sizeof(", T.stringof, ") is ", T.sizeof));
        return val;
    }

    static int flags(Flag!"blocking" blocking) @safe @nogc pure nothrow {
        return blocking ? 0 : NN_DONTWAIT;
    }
}

int enforceNanoMsgRet(E)(lazy E expr, string file = __FILE__, size_t line = __LINE__) @trusted {
    import std.conv: text;
    const value = expr();
    if(value < 0) {
        version(NanomsgWrapperNoGcException) {
            import nogc: NoGcException;
            NoGcException.throwNewWithFileAndLine(
                file, line, "nanomsg expression failed with value ", numBytes,
                " errno ", nn_errno, ", error: ", nn_strerror(nn_errno));
        } else {
            throw new Exception(text("nanomsg expression failed with value ", value,
                                     " errno ", nn_errno, ", error: ", nn_strerror(nn_errno)),
                                file,
                                line);
        }
    }
    return value;
}


struct PollResult(Flag!"blocking" blocking = Yes.blocking)
{
    private NanoSocket* sock_;
    private short idx_;

    auto idx() @property
    in (!timedOut, "Fetching idx for timed-out PollResult")
    {
        return idx_;
    }

    auto sock() @property
    in (!timedOut, "Fetching sock for timed-out PollResult")
    {
        return sock_;
    }

    static if (blocking)
        enum timedOut = false;
    else
        auto timedOut() @property
        {
            return idx_ == -1;
        }

    enum PollResult timedOutResult = PollResult(null, -1);
}

struct ReceivePoller(Flag!"blocking" blocking = Yes.blocking)
{
    private NanoSocket*[] sockets;
    private nn_pollfd[] pds;
    private PollResult!blocking front_;
    static if (!blocking)
        int timeoutMs;
    enum empty = false;

    static if (!blocking)
        this(NanoSocket*[] sockets, int timeoutMs)
        {
            this.timeoutMs = timeoutMs;
            initPds(sockets);
        }
    else
        this(NanoSocket*[] sockets)
        {
            initPds(sockets);
        }

    private void initPds(NanoSocket*[] sockets)
    {
        import std.exception : enforce;

        this.sockets = sockets;
        auto mP = cast(nn_pollfd*)nn_allocmsg(nn_pollfd.sizeof * sockets.length, 0);
        enforce(mP, "memory allocation failed");
        pds = mP[0 .. sockets.length];
        foreach (i, ref pd; pds)
        {
            pd.fd = sockets[i]._nanoSock;
            pd.events = NN_POLLIN;
            pd.revents = 0;
        }
        popFront();
    }

    auto front() @property
    {
        return front_;
    }

    void popFront()
    {
        import std.algorithm : countUntil;
        import std.range : dropExactly;

        bool findNextIdx(ref short idx)
        {
            auto found = pds
                .dropExactly(idx)
                .countUntil!(pd => bool(pd.revents & NN_POLLIN));
            if (found >= 0)
            {
                idx += cast(short)found;
                return true;
            }
            idx = cast(short)pds.length;
            return false;
        }

        short idx = front.idx_;

        if (front.timedOut || !findNextIdx(idx))
        {
            static if (!blocking)
                int timeout = timeoutMs;
            else
                enum timeout = 1000;

            while(nn_poll(pds.ptr, pds.length, timeout).enforceNanoMsgRet == 0)
            {
                static if (!blocking)
                {
                    front_ = PollResult!blocking.timedOutResult;
                    return;
                }
            }

            idx = 0;
            auto r = findNextIdx(idx);
            assert(r);
        }

        front_ = PollResult!blocking(sockets[idx], idx);
        pds[idx].revents = nn_pollfd.init.revents;
    }
}

auto receivePoll(NanoSocket*[] sockets)
{
    return ReceivePoller!(Yes.blocking)(sockets);
}

auto receivePoll(NanoSocket*[] sockets, int timeoutMs)
{
    return ReceivePoller!(No.blocking)(sockets, timeoutMs);
}

auto act(actions...)(PollResult p)
{
    switch (p.idx)
    {
        static foreach (i, action; actions)
            case i:
                return action(p.socket);
            default:
                assert(0, "no action for this case");
    }
}

/// check nanomsg socket
void checkNanoSocket(T)() {
    T s = T(NanoSocket.Protocol.subscribe, ConnectTo("foobar"));
    s.send("foobar");
    s.setOption(NanoSocket.Option.subscribeTopic, "topic");
    s.setOption(NanoSocket.Option.receiveTimeoutMs, 100);
    auto msg = s.receive(Yes.blocking);
    void[] bytes = msg.bytes;
    s.send(bytes);
}

enum isNanoSocket(T) = is(typeof(checkNanoSocket!T));


/// RAII struct for nn_freemsg
struct NanoBuffer {

    /// Could be allocated by nanomsg
    void[] bytes;
    private bool shouldDelete;

    private this(void[] bytes, bool shouldDelete) @safe @nogc pure nothrow scope {
        this.bytes = bytes;
        this.shouldDelete = shouldDelete;
    }

    @disable this(this);

    ~this() @trusted @nogc scope {
        import nanomsg.bindings: nn_freemsg;
        if(shouldDelete && bytes.length > 0) nn_freemsg(&bytes[0]);
    }
}
