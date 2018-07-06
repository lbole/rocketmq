/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.remoting.netty;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.*;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.util.concurrent.DefaultEventExecutorGroup;
import org.apache.rocketmq.remoting.ChannelEventListener;
import org.apache.rocketmq.remoting.InvokeCallback;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.RemotingServer;
import org.apache.rocketmq.remoting.common.Pair;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.common.RemotingUtil;
import org.apache.rocketmq.remoting.common.TlsMode;
import org.apache.rocketmq.remoting.exception.RemotingSendRequestException;
import org.apache.rocketmq.remoting.exception.RemotingTimeoutException;
import org.apache.rocketmq.remoting.exception.RemotingTooMuchRequestException;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.security.cert.CertificateException;
import java.util.NoSuchElementException;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Netty实现的RPC远程服务端
 *
 * @Author Administrator
 */
public class NettyRemotingServer extends NettyRemotingAbstract implements RemotingServer {
    
    private static final Logger log = LoggerFactory.getLogger(RemotingHelper.ROCKETMQ_REMOTING);
    
    private final ServerBootstrap serverBootstrap;
    private final EventLoopGroup eventLoopGroupSelector;
    private final EventLoopGroup eventLoopGroupBoss;
    private final NettyServerConfig nettyServerConfig;
    
    // 服务端公共的线程池执行器
    private final ExecutorService publicExecutor;
    // 连接通道事件监听器
    private final ChannelEventListener channelEventListener;
    
    // 服务端维护服务相关定时器
    private final Timer timer = new Timer("ServerHouseKeepingService", true);
    // Netty中默认的
    private DefaultEventExecutorGroup defaultEventExecutorGroup;
    
    // RPC请求响应钩子
    private RPCHook rpcHook;
    
    // 端口
    private int port = 0;
    
    // Netty中TLS握手协议处理器名称
    private static final String HANDSHAKE_HANDLER_NAME = "handshakeHandler";
    private static final String TLS_HANDLER_NAME = "sslHandler";
    private static final String FILE_REGION_ENCODER_NAME = "fileRegionEncoder";
    
    /**
     * 构造器，连接通道事件监听器为空
     *
     * @param nettyServerConfig
     */
    public NettyRemotingServer(final NettyServerConfig nettyServerConfig) {
        this(nettyServerConfig, null);
    }
    
    /**
     * 构造器，提供自定义连接通道事件监听器
     *
     * @param nettyServerConfig
     * @param channelEventListener
     */
    public NettyRemotingServer(final NettyServerConfig nettyServerConfig, final ChannelEventListener channelEventListener) {
        // 设置单向请求和异步请求信号量大小
        super(nettyServerConfig.getServerOnewaySemaphoreValue(), nettyServerConfig.getServerAsyncSemaphoreValue());
        // 初始化Netty服务端引导实例
        this.serverBootstrap = new ServerBootstrap();
        // 设置Netty服务端配置对象
        this.nettyServerConfig = nettyServerConfig;
        // 设置连接通道事件处理器
        this.channelEventListener = channelEventListener;
        
        // 如果服务端公用线程池大小设置为不大于0，则设置为默认的4
        int publicThreadNums = nettyServerConfig.getServerCallbackExecutorThreads();
        if (publicThreadNums <= 0) {
            publicThreadNums = 4;
        }
        
        // 初始化服务端公用线程池，用来处理得到响应之后的ResponseFuture的回调任务
        // 如果为空则使用Netty用于处理Channel的IO读写的线程池（EventLoopGroup）或者处理Pipeline中Handler的线程池（EventExecutorGroup）执行响应回调任务
        this.publicExecutor = Executors.newFixedThreadPool(publicThreadNums, new ThreadFactory() {
            // 线程名称计数器
            private AtomicInteger threadIndex = new AtomicInteger(0);
            
            @Override
            public Thread newThread(Runnable r) {
                // 设置生成线程时的线程名称
                return new Thread(r, "NettyServerPublicExecutor_" + this.threadIndex.incrementAndGet());
            }
        });
        
        // Netty中用于Acceptor接收连接（可能需要进行传输层加密认证）的线程池
        this.eventLoopGroupBoss = new NioEventLoopGroup(1, new ThreadFactory() {
            private AtomicInteger threadIndex = new AtomicInteger(0);
            
            @Override
            public Thread newThread(Runnable r) {
                return new Thread(r, String.format("NettyBoss_%d", this.threadIndex.incrementAndGet()));
            }
        });
        
        // 使用Linux的Epoll或者Netty实现的线程池
        // 主要用来做Channel的IO读写操作，如果DefaultEventExecutorGroup为空，则其同时还肩负着执行Pipeline中的可能会非常耗时的Handler任务
        if (useEpoll()) {
            this.eventLoopGroupSelector = new EpollEventLoopGroup(nettyServerConfig.getServerSelectorThreads(), new ThreadFactory() {
                private AtomicInteger threadIndex = new AtomicInteger(0);
                private int threadTotal = nettyServerConfig.getServerSelectorThreads();
                
                @Override
                public Thread newThread(Runnable r) {
                    return new Thread(r, String.format("NettyServerEPOLLSelector_%d_%d", threadTotal, this.threadIndex.incrementAndGet()));
                }
            });
        } else {
            this.eventLoopGroupSelector = new NioEventLoopGroup(nettyServerConfig.getServerSelectorThreads(), new ThreadFactory() {
                private AtomicInteger threadIndex = new AtomicInteger(0);
                private int threadTotal = nettyServerConfig.getServerSelectorThreads();
                
                @Override
                public Thread newThread(Runnable r) {
                    return new Thread(r, String.format("NettyServerNIOSelector_%d_%d", threadTotal, this.threadIndex.incrementAndGet()));
                }
            });
        }
        
        TlsMode tlsMode = TlsSystemConfig.tlsMode;
        log.info("Server is running in TLS {} mode", tlsMode.getName());
        
        // 判断是否使用传输层加密协议
        if (tlsMode != TlsMode.DISABLED) {
            try {
                // 创建SSL上下文
                sslContext = TlsHelper.buildSslContext(false);
                log.info("SSLContext created for server");
            } catch (CertificateException e) {
                log.error("Failed to create SSLContext for server", e);
            } catch (IOException e) {
                log.error("Failed to create SSLContext for server", e);
            }
        }
    }
    
    /**
     * 判断是否使用Linux平台自带的Epoll
     *
     * @return
     */
    private boolean useEpoll() {
        return RemotingUtil.isLinuxPlatform() && nettyServerConfig.isUseEpollNativeSelector() && Epoll.isAvailable();
    }
    
    /**
     * 启动服务端
     */
    @Override
    public void start() {
        // Netty中Pipeline中的可能会非常耗时的Handler任务，如果为空，则其任务由EventLoopGroup来完成
        this.defaultEventExecutorGroup = new DefaultEventExecutorGroup(
                nettyServerConfig.getServerWorkerThreads(),
                new ThreadFactory() {
                    
                    private AtomicInteger threadIndex = new AtomicInteger(0);
                    
                    @Override
                    public Thread newThread(Runnable r) {
                        return new Thread(r, "NettyServerCodecThread_" + this.threadIndex.incrementAndGet());
                    }
                });
        
        ServerBootstrap childHandler = this.serverBootstrap.group(this.eventLoopGroupBoss, this.eventLoopGroupSelector)
                .channel(useEpoll() ? EpollServerSocketChannel.class : NioServerSocketChannel.class)
                // 该参数对应的是tcp/ip协议listen函数中的backlog参数
                // eventLoopGroupBoss线程池处理客户端连接请求时，如果线程已满时用于临时存放已完成三次握手的请求的队列的最大长度（线程池大小）。如果未设置或所设置的值小于1，Java将使用默认值50。
                .option(ChannelOption.SO_BACKLOG, 1024)
                // 该参数对应于套接字选项中的SO_REUSEADDR
                // 这个参数表示允许重复使用TIME_WAIT状态的本地地址和端口，比如，某个服务器进程占用了TCP的80端口进行监听，此时再次监听该端口就会返回错误，使用该参数就可以解决问题，
                // 该参数允许共用该端口，这个在服务器程序中比较常使用，比如某个进程非正常退出，该程序占用的端口可能要被占用一段时间才能允许其他进程使用，而且程序死掉以后，
                // 内核一需要一定的时间才能够释放此端口，不设置SO_REUSEADDR就无法正常使用该端口。
                .option(ChannelOption.SO_REUSEADDR, true)
                // 该参数参数对应于套接字选项中的SO_KEEPALIVE，该参数用于设置TCP连接
                // 当设置该选项以后，连接会测试链接的状态，这个选项用于可能长时间没有数据交流的连接。当设置该选项以后，如果在两小时内没有数据的通信时，TCP会自动发送一个活动探测数据报文。
                // 这套机制只是操作系统底层使用的一个被动机制，原理上不应该被上层应用层使用。当系统关闭一个由KEEPALIVE机制检查出来的死连接时，是不会主动通知上层应用的，只有在调用相应的IO操作在返回值中检查出来。
                .option(ChannelOption.SO_KEEPALIVE, false)
                // 参数对应于套接字选项中的TCP_NODELAY,该参数的使用与Nagle算法有关
                // Nagle算法是将小的数据包组装为更大的帧然后进行发送，而不是输入一次发送一次，因此在数据包不足的时候会等待其他数据的到了，组装成大的数据包进行发送，虽然该方式有效提高网络的有效
                // 负载，但是却造成了延时，而该参数的作用就是禁止使用Nagle算法，使用于小数据即时传输，于TCP_NODELAY相对应的是TCP_CORK，该选项是需要等到发送的数据量最大的时候，一次性发送
                // 数据，适用于文件传输。
                .childOption(ChannelOption.TCP_NODELAY, true)
                // 参数对应于套接字选项中的SO_SNDBUF
                // 用于设置接收缓冲区大小，接收缓冲区用于保存网络协议站内收到的数据，直到应用程序读取成功
                .childOption(ChannelOption.SO_SNDBUF, nettyServerConfig.getServerSocketSndBufSize())
                // 对应于套接字选项中的SO_RCVBUF
                // 用于发送缓冲大小，发送缓冲区用于保存发送数据，直到发送成功。
                .childOption(ChannelOption.SO_RCVBUF, nettyServerConfig.getServerSocketRcvBufSize())
                .localAddress(new InetSocketAddress(this.nettyServerConfig.getListenPort()))
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    public void initChannel(SocketChannel ch) throws Exception {
                        ch.pipeline()
                                .addLast(defaultEventExecutorGroup, HANDSHAKE_HANDLER_NAME, new HandshakeHandler(TlsSystemConfig.tlsMode))
                                .addLast(defaultEventExecutorGroup,
                                        new NettyEncoder(),
                                        new NettyDecoder(),
                                        new IdleStateHandler(0, 0, nettyServerConfig.getServerChannelMaxIdleTimeSeconds()),
                                        new NettyConnectManageHandler(),
                                        new NettyServerHandler()
                                );
                    }
                });
        
        if (nettyServerConfig.isServerPooledByteBufAllocatorEnable()) {
            // 启用ByteBuf对象池
            childHandler.childOption(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT);
        }
        
        try {
            // 绑定本地端口和默认IP，并启动服务端
            ChannelFuture sync = this.serverBootstrap.bind().sync();
            // 获取绑定的地址和端口
            InetSocketAddress addr = (InetSocketAddress) sync.channel().localAddress();
            // 设置服务端端口
            this.port = addr.getPort();
        } catch (InterruptedException e1) {
            throw new RuntimeException("this.serverBootstrap.bind().sync() InterruptedException", e1);
        }
        
        // 如果Pipeline通道事件执行器不为空，则自动执行器
        if (this.channelEventListener != null) {
            this.nettyEventExecutor.start();
        }
        
        // 扫描响应表定时器，处理过期请求
        this.timer.scheduleAtFixedRate(new TimerTask() {
            
            @Override
            public void run() {
                try {
                    NettyRemotingServer.this.scanResponseTable();
                } catch (Throwable e) {
                    log.error("scanResponseTable exception", e);
                }
            }
        }, 1000 * 3, 1000);
    }
    
    @Override
    public void shutdown() {
        try {
            // 关闭扫描响应列表的定时器
            if (this.timer != null) {
                this.timer.cancel();
            }
            
            // 优雅关闭Netty Server处理连接的线程池
            this.eventLoopGroupBoss.shutdownGracefully();
            
            // 优雅关闭Netty Server处理IO的线程池
            this.eventLoopGroupSelector.shutdownGracefully();
            
            // 释放处理Netty Pipeline事件的服务线程
            if (this.nettyEventExecutor != null) {
                this.nettyEventExecutor.shutdown();
            }
            
            // 释放用于处理Netty Pipeline的线程池
            if (this.defaultEventExecutorGroup != null) {
                this.defaultEventExecutorGroup.shutdownGracefully();
            }
        } catch (Exception e) {
            log.error("NettyRemotingServer shutdown exception, ", e);
        }
        
        // 释放用于处理响应的线程池
        if (this.publicExecutor != null) {
            try {
                this.publicExecutor.shutdown();
            } catch (Exception e) {
                log.error("NettyRemotingServer shutdown exception, ", e);
            }
        }
    }
    
    @Override
    public void registerRPCHook(RPCHook rpcHook) {
        this.rpcHook = rpcHook;
    }
    
    @Override
    public void registerProcessor(int requestCode, NettyRequestProcessor processor, ExecutorService executor) {
        // 如果传入的线程池为空，则使用公用的线程池
        ExecutorService executorThis = executor;
        if (null == executor) {
            executorThis = this.publicExecutor;
        }
        
        Pair<NettyRequestProcessor, ExecutorService> pair = new Pair<NettyRequestProcessor, ExecutorService>(processor, executorThis);
        this.processorTable.put(requestCode, pair);
    }
    
    @Override
    public void registerDefaultProcessor(NettyRequestProcessor processor, ExecutorService executor) {
        this.defaultRequestProcessor = new Pair<NettyRequestProcessor, ExecutorService>(processor, executor);
    }
    
    @Override
    public int localListenPort() {
        return this.port;
    }
    
    @Override
    public Pair<NettyRequestProcessor, ExecutorService> getProcessorPair(int requestCode) {
        return processorTable.get(requestCode);
    }
    
    @Override
    public RemotingCommand invokeSync(final Channel channel, final RemotingCommand request, final long timeoutMillis) throws InterruptedException, RemotingSendRequestException, RemotingTimeoutException {
        return this.invokeSyncImpl(channel, request, timeoutMillis);
    }
    
    @Override
    public void invokeAsync(Channel channel, RemotingCommand request, long timeoutMillis, InvokeCallback invokeCallback) throws InterruptedException, RemotingTooMuchRequestException, RemotingTimeoutException, RemotingSendRequestException {
        this.invokeAsyncImpl(channel, request, timeoutMillis, invokeCallback);
    }
    
    @Override
    public void invokeOneway(Channel channel, RemotingCommand request, long timeoutMillis) throws InterruptedException, RemotingTooMuchRequestException, RemotingTimeoutException, RemotingSendRequestException {
        this.invokeOnewayImpl(channel, request, timeoutMillis);
    }
    
    @Override
    public ChannelEventListener getChannelEventListener() {
        return channelEventListener;
    }
    
    @Override
    public RPCHook getRPCHook() {
        return this.rpcHook;
    }
    
    @Override
    public ExecutorService getCallbackExecutor() {
        return this.publicExecutor;
    }
    
    class HandshakeHandler extends SimpleChannelInboundHandler<ByteBuf> {
        
        private final TlsMode tlsMode;
        
        private static final byte HANDSHAKE_MAGIC_CODE = 0x16;
        
        HandshakeHandler(TlsMode tlsMode) {
            this.tlsMode = tlsMode;
        }
        
        @Override
        protected void channelRead0(ChannelHandlerContext ctx, ByteBuf msg) throws Exception {
            
            // mark the current position so that we can peek the first byte to determine if the content is starting with
            // TLS handshake
            msg.markReaderIndex();
            
            byte b = msg.getByte(0);
            
            if (b == HANDSHAKE_MAGIC_CODE) {
                switch (tlsMode) {
                    case DISABLED:
                        ctx.close();
                        log.warn("Clients intend to establish a SSL connection while this server is running in SSL disabled mode");
                        break;
                    case PERMISSIVE:
                    case ENFORCING:
                        if (null != sslContext) {
                            ctx.pipeline()
                                    .addAfter(defaultEventExecutorGroup, HANDSHAKE_HANDLER_NAME, TLS_HANDLER_NAME, sslContext.newHandler(ctx.channel().alloc()))
                                    .addAfter(defaultEventExecutorGroup, TLS_HANDLER_NAME, FILE_REGION_ENCODER_NAME, new FileRegionEncoder());
                            log.info("Handlers prepended to channel pipeline to establish SSL connection");
                        } else {
                            ctx.close();
                            log.error("Trying to establish a SSL connection but sslContext is null");
                        }
                        break;
                    
                    default:
                        log.warn("Unknown TLS mode");
                        break;
                }
            } else if (tlsMode == TlsMode.ENFORCING) {
                ctx.close();
                log.warn("Clients intend to establish an insecure connection while this server is running in SSL enforcing mode");
            }
            
            // reset the reader index so that handshake negotiation may proceed as normal.
            msg.resetReaderIndex();
            
            try {
                // Remove this handler
                ctx.pipeline().remove(this);
            } catch (NoSuchElementException e) {
                log.error("Error while removing HandshakeHandler", e);
            }
            
            // Hand over this message to the next .
            ctx.fireChannelRead(msg.retain());
        }
    }
    
    class NettyServerHandler extends SimpleChannelInboundHandler<RemotingCommand> {
        
        @Override
        protected void channelRead0(ChannelHandlerContext ctx, RemotingCommand msg) throws Exception {
            processMessageReceived(ctx, msg);
        }
    }
    
    class NettyConnectManageHandler extends ChannelDuplexHandler {
        @Override
        public void channelRegistered(ChannelHandlerContext ctx) throws Exception {
            final String remoteAddress = RemotingHelper.parseChannelRemoteAddr(ctx.channel());
            log.info("NETTY SERVER PIPELINE: channelRegistered {}", remoteAddress);
            super.channelRegistered(ctx);
        }
        
        @Override
        public void channelUnregistered(ChannelHandlerContext ctx) throws Exception {
            final String remoteAddress = RemotingHelper.parseChannelRemoteAddr(ctx.channel());
            log.info("NETTY SERVER PIPELINE: channelUnregistered, the channel[{}]", remoteAddress);
            super.channelUnregistered(ctx);
        }
        
        @Override
        public void channelActive(ChannelHandlerContext ctx) throws Exception {
            final String remoteAddress = RemotingHelper.parseChannelRemoteAddr(ctx.channel());
            log.info("NETTY SERVER PIPELINE: channelActive, the channel[{}]", remoteAddress);
            super.channelActive(ctx);
            
            if (NettyRemotingServer.this.channelEventListener != null) {
                NettyRemotingServer.this.putNettyEvent(new NettyEvent(NettyEventType.CONNECT, remoteAddress, ctx.channel()));
            }
        }
        
        @Override
        public void channelInactive(ChannelHandlerContext ctx) throws Exception {
            final String remoteAddress = RemotingHelper.parseChannelRemoteAddr(ctx.channel());
            log.info("NETTY SERVER PIPELINE: channelInactive, the channel[{}]", remoteAddress);
            super.channelInactive(ctx);
            
            if (NettyRemotingServer.this.channelEventListener != null) {
                NettyRemotingServer.this.putNettyEvent(new NettyEvent(NettyEventType.CLOSE, remoteAddress, ctx.channel()));
            }
        }
        
        @Override
        public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
            if (evt instanceof IdleStateEvent) {
                IdleStateEvent event = (IdleStateEvent) evt;
                if (event.state().equals(IdleState.ALL_IDLE)) {
                    final String remoteAddress = RemotingHelper.parseChannelRemoteAddr(ctx.channel());
                    log.warn("NETTY SERVER PIPELINE: IDLE exception [{}]", remoteAddress);
                    RemotingUtil.closeChannel(ctx.channel());
                    if (NettyRemotingServer.this.channelEventListener != null) {
                        NettyRemotingServer.this.putNettyEvent(new NettyEvent(NettyEventType.IDLE, remoteAddress, ctx.channel()));
                    }
                }
            }
            
            ctx.fireUserEventTriggered(evt);
        }
        
        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
            final String remoteAddress = RemotingHelper.parseChannelRemoteAddr(ctx.channel());
            log.warn("NETTY SERVER PIPELINE: exceptionCaught {}", remoteAddress);
            log.warn("NETTY SERVER PIPELINE: exceptionCaught exception.", cause);
            
            if (NettyRemotingServer.this.channelEventListener != null) {
                NettyRemotingServer.this.putNettyEvent(new NettyEvent(NettyEventType.EXCEPTION, remoteAddress, ctx.channel()));
            }
            
            RemotingUtil.closeChannel(ctx.channel());
        }
    }
}
