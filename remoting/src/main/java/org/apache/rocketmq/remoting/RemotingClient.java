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
package org.apache.rocketmq.remoting;

import java.util.List;
import java.util.concurrent.ExecutorService;
import org.apache.rocketmq.remoting.exception.RemotingConnectException;
import org.apache.rocketmq.remoting.exception.RemotingSendRequestException;
import org.apache.rocketmq.remoting.exception.RemotingTimeoutException;
import org.apache.rocketmq.remoting.exception.RemotingTooMuchRequestException;
import org.apache.rocketmq.remoting.netty.NettyRequestProcessor;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

/**
 * 远程RPC客户端
 *
 * @Author Administrator
 * @Date 2018/6/29 0029 09 14
 * @Description
 */
public interface RemotingClient extends RemotingService {
    
    /**
     * 修改名称服务器地址列表
     *
     * @param addrs
     */
    void updateNameServerAddressList(final List<String> addrs);
    
    /**
     * 获取名称服务器地址列表
     *
     * @return
     */
    List<String> getNameServerAddressList();
    
    /**
     * 执行同步请求
     *
     * @param addr
     * @param request
     * @param timeoutMillis
     * @return
     * @throws InterruptedException
     * @throws RemotingConnectException
     * @throws RemotingSendRequestException
     * @throws RemotingTimeoutException
     */
    RemotingCommand invokeSync(final String addr, final RemotingCommand request, final long timeoutMillis) throws InterruptedException, RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException;
    
    /**
     * 执行异步请求
     *
     * @param addr
     * @param request
     * @param timeoutMillis
     * @param invokeCallback
     * @throws InterruptedException
     * @throws RemotingConnectException
     * @throws RemotingTooMuchRequestException
     * @throws RemotingTimeoutException
     * @throws RemotingSendRequestException
     */
    void invokeAsync(final String addr, final RemotingCommand request, final long timeoutMillis, final InvokeCallback invokeCallback) throws InterruptedException, RemotingConnectException, RemotingTooMuchRequestException, RemotingTimeoutException, RemotingSendRequestException;
    
    /**
     * 执行单向请求
     *
     * @param addr
     * @param request
     * @param timeoutMillis
     * @throws InterruptedException
     * @throws RemotingConnectException
     * @throws RemotingTooMuchRequestException
     * @throws RemotingTimeoutException
     * @throws RemotingSendRequestException
     */
    void invokeOneway(final String addr, final RemotingCommand request, final long timeoutMillis) throws InterruptedException, RemotingConnectException, RemotingTooMuchRequestException, RemotingTimeoutException, RemotingSendRequestException;
    
    /**
     * 注册请求处理器
     *
     * @param requestCode
     * @param processor
     * @param executor
     */
    void registerProcessor(final int requestCode, final NettyRequestProcessor processor, final ExecutorService executor);
    
    /**
     * 设置回调处理器
     *
     * @param callbackExecutor
     */
    void setCallbackExecutor(final ExecutorService callbackExecutor);
    
    /**
     * 判断远程地址的对应的Channel通道写操作是否可以立即被IO线程处理
     *
     * @param addr
     * @return
     */
    boolean isChannelWritable(final String addr);
}