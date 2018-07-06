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

/**
 * Netty服务端配置
 *
 * @Author Administrator
 * @Date 2018/6/29 0029 09 14
 * @Description
 */
public class NettyServerConfig implements Cloneable {
    
    // 本地监听端口
    private int listenPort = 8888;
    // 服务端工作线程数
    private int serverWorkerThreads = 8;
    // 服务端回调执行器线程数
    private int serverCallbackExecutorThreads = 0;
    // 服务端Selector线程数
    private int serverSelectorThreads = 3;
    // 服务端单向请求数（信号量）
    private int serverOnewaySemaphoreValue = 256;
    // 服务端异步请求数（信号量）
    private int serverAsyncSemaphoreValue = 64;
    // 服务端通道最大空闲秒数
    private int serverChannelMaxIdleTimeSeconds = 120;
    
    // 定义发送发送传输缓冲区大小
    private int serverSocketSndBufSize = NettySystemConfig.socketSndbufSize;
    // 定义接收缓冲区大小
    private int serverSocketRcvBufSize = NettySystemConfig.socketRcvbufSize;
    // 是否使用ByteBuf池，重用缓冲区
    private boolean serverPooledByteBufAllocatorEnable = true;
    
    /**
     * 如果硬件环境支持Epoll，这里则控制是否使用本地Epoll
     * make make install
     * <p>
     * <p>
     * ../glibc-2.10.1/configure \ --prefix=/usr \ --with-headers=/usr/include \
     * --host=x86_64-linux-gnu \ --build=x86_64-pc-linux-gnu \ --without-gd
     */
    private boolean useEpollNativeSelector = false;
    
    public int getListenPort() {
        return listenPort;
    }
    
    public void setListenPort(int listenPort) {
        this.listenPort = listenPort;
    }
    
    public int getServerWorkerThreads() {
        return serverWorkerThreads;
    }
    
    public void setServerWorkerThreads(int serverWorkerThreads) {
        this.serverWorkerThreads = serverWorkerThreads;
    }
    
    public int getServerSelectorThreads() {
        return serverSelectorThreads;
    }
    
    public void setServerSelectorThreads(int serverSelectorThreads) {
        this.serverSelectorThreads = serverSelectorThreads;
    }
    
    public int getServerOnewaySemaphoreValue() {
        return serverOnewaySemaphoreValue;
    }
    
    public void setServerOnewaySemaphoreValue(int serverOnewaySemaphoreValue) {
        this.serverOnewaySemaphoreValue = serverOnewaySemaphoreValue;
    }
    
    public int getServerCallbackExecutorThreads() {
        return serverCallbackExecutorThreads;
    }
    
    public void setServerCallbackExecutorThreads(int serverCallbackExecutorThreads) {
        this.serverCallbackExecutorThreads = serverCallbackExecutorThreads;
    }
    
    public int getServerAsyncSemaphoreValue() {
        return serverAsyncSemaphoreValue;
    }
    
    public void setServerAsyncSemaphoreValue(int serverAsyncSemaphoreValue) {
        this.serverAsyncSemaphoreValue = serverAsyncSemaphoreValue;
    }
    
    public int getServerChannelMaxIdleTimeSeconds() {
        return serverChannelMaxIdleTimeSeconds;
    }
    
    public void setServerChannelMaxIdleTimeSeconds(int serverChannelMaxIdleTimeSeconds) {
        this.serverChannelMaxIdleTimeSeconds = serverChannelMaxIdleTimeSeconds;
    }
    
    public int getServerSocketSndBufSize() {
        return serverSocketSndBufSize;
    }
    
    public void setServerSocketSndBufSize(int serverSocketSndBufSize) {
        this.serverSocketSndBufSize = serverSocketSndBufSize;
    }
    
    public int getServerSocketRcvBufSize() {
        return serverSocketRcvBufSize;
    }
    
    public void setServerSocketRcvBufSize(int serverSocketRcvBufSize) {
        this.serverSocketRcvBufSize = serverSocketRcvBufSize;
    }
    
    public boolean isServerPooledByteBufAllocatorEnable() {
        return serverPooledByteBufAllocatorEnable;
    }
    
    public void setServerPooledByteBufAllocatorEnable(boolean serverPooledByteBufAllocatorEnable) {
        this.serverPooledByteBufAllocatorEnable = serverPooledByteBufAllocatorEnable;
    }
    
    public boolean isUseEpollNativeSelector() {
        return useEpollNativeSelector;
    }
    
    public void setUseEpollNativeSelector(boolean useEpollNativeSelector) {
        this.useEpollNativeSelector = useEpollNativeSelector;
    }
    
    @Override
    public Object clone() throws CloneNotSupportedException {
        return (NettyServerConfig) super.clone();
    }
}
