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

public class NettyClientConfig {
    
    /**
     * Worker thread number
     * 工作线程数
     */
    private int clientWorkerThreads = 4;
    // 客户端回调执行器线程数
    private int clientCallbackExecutorThreads = Runtime.getRuntime().availableProcessors();
    // 客户端单向请求信号量（最大请求数）
    private int clientAsyncSemaphoreValue = NettySystemConfig.CLIENT_ASYNC_SEMAPHORE_VALUE;
    // 客户端异步请求信号量（最大请求数）
    private int clientOnewaySemaphoreValue = NettySystemConfig.CLIENT_ONEWAY_SEMAPHORE_VALUE;
    // 连接超时毫秒数
    private int connectTimeoutMillis = 3000;
    //
    private long channelNotActiveInterval = 1000 * 60;
    
    /**
     * 客户端连接通道最大空闲秒数
     * IdleStateEvent will be triggered when neither read nor write was performed for
     * the specified period of this time. Specify {@code 0} to disable
     */
    private int clientChannelMaxIdleTimeSeconds = 120;
    
    // 定义发送发送传输缓冲区大小
    private int clientSocketRcvBufSize = NettySystemConfig.socketRcvbufSize;
    // 定义接收缓冲区大小
    private int clientSocketSndBufSize = NettySystemConfig.socketSndbufSize;
    // 是否使用ByteBuf池，重用缓冲区
    private boolean clientPooledByteBufAllocatorEnable = false;
    // 客户端连接如果超时是否关闭连接
    private boolean clientCloseSocketIfTimeout = false;
    
    // 是否使用TLS安全传输层协议
    private boolean useTLS;
    
    public boolean isClientCloseSocketIfTimeout() {
        return clientCloseSocketIfTimeout;
    }
    
    public void setClientCloseSocketIfTimeout(final boolean clientCloseSocketIfTimeout) {
        this.clientCloseSocketIfTimeout = clientCloseSocketIfTimeout;
    }
    
    public int getClientWorkerThreads() {
        return clientWorkerThreads;
    }
    
    public void setClientWorkerThreads(int clientWorkerThreads) {
        this.clientWorkerThreads = clientWorkerThreads;
    }
    
    public int getClientOnewaySemaphoreValue() {
        return clientOnewaySemaphoreValue;
    }
    
    public void setClientOnewaySemaphoreValue(int clientOnewaySemaphoreValue) {
        this.clientOnewaySemaphoreValue = clientOnewaySemaphoreValue;
    }
    
    public int getConnectTimeoutMillis() {
        return connectTimeoutMillis;
    }
    
    public void setConnectTimeoutMillis(int connectTimeoutMillis) {
        this.connectTimeoutMillis = connectTimeoutMillis;
    }
    
    public int getClientCallbackExecutorThreads() {
        return clientCallbackExecutorThreads;
    }
    
    public void setClientCallbackExecutorThreads(int clientCallbackExecutorThreads) {
        this.clientCallbackExecutorThreads = clientCallbackExecutorThreads;
    }
    
    public long getChannelNotActiveInterval() {
        return channelNotActiveInterval;
    }
    
    public void setChannelNotActiveInterval(long channelNotActiveInterval) {
        this.channelNotActiveInterval = channelNotActiveInterval;
    }
    
    public int getClientAsyncSemaphoreValue() {
        return clientAsyncSemaphoreValue;
    }
    
    public void setClientAsyncSemaphoreValue(int clientAsyncSemaphoreValue) {
        this.clientAsyncSemaphoreValue = clientAsyncSemaphoreValue;
    }
    
    public int getClientChannelMaxIdleTimeSeconds() {
        return clientChannelMaxIdleTimeSeconds;
    }
    
    public void setClientChannelMaxIdleTimeSeconds(int clientChannelMaxIdleTimeSeconds) {
        this.clientChannelMaxIdleTimeSeconds = clientChannelMaxIdleTimeSeconds;
    }
    
    public int getClientSocketSndBufSize() {
        return clientSocketSndBufSize;
    }
    
    public void setClientSocketSndBufSize(int clientSocketSndBufSize) {
        this.clientSocketSndBufSize = clientSocketSndBufSize;
    }
    
    public int getClientSocketRcvBufSize() {
        return clientSocketRcvBufSize;
    }
    
    public void setClientSocketRcvBufSize(int clientSocketRcvBufSize) {
        this.clientSocketRcvBufSize = clientSocketRcvBufSize;
    }
    
    public boolean isClientPooledByteBufAllocatorEnable() {
        return clientPooledByteBufAllocatorEnable;
    }
    
    public void setClientPooledByteBufAllocatorEnable(boolean clientPooledByteBufAllocatorEnable) {
        this.clientPooledByteBufAllocatorEnable = clientPooledByteBufAllocatorEnable;
    }
    
    public boolean isUseTLS() {
        return useTLS;
    }
    
    public void setUseTLS(boolean useTLS) {
        this.useTLS = useTLS;
    }
}
