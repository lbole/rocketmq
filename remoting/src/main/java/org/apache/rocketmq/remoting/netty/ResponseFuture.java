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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.rocketmq.remoting.InvokeCallback;
import org.apache.rocketmq.remoting.common.SemaphoreReleaseOnlyOnce;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

/**
 * 请求响应的Future
 *
 * @Author Administrator
 * @Date 2018/6/29 0029 09 14
 * @Description
 */
public class ResponseFuture {
    
    // 请求命令的ID
    private final int opaque;
    // 请求超时毫秒数
    private final long timeoutMillis;
    // 请求成功执行的回调
    private final InvokeCallback invokeCallback;
    // 开始请求的时间毫秒数
    private final long beginTimestamp = System.currentTimeMillis();
    // 用于发送同步请求时，作为等待的CountDownLatch
    private final CountDownLatch countDownLatch = new CountDownLatch(1);
    
    // 对java原始Semaphore（信号量）封装，保证当前ResponseFuture只能执行一次信号量的release方法
    // 用于限制异步请求和单向请求的最大请求数量
    private final SemaphoreReleaseOnlyOnce once;
    
    // 判断请求回调方法是否被执行，保证请求回调方法只执行一次
    private final AtomicBoolean executeCallbackOnlyOnce = new AtomicBoolean(false);
    // 请求或者响应命令
    private volatile RemotingCommand responseCommand;
    // 是否发送成功
    private volatile boolean sendRequestOK = true;
    // 请求或者响应过程中的异常
    private volatile Throwable cause;
    
    public ResponseFuture(int opaque, long timeoutMillis, InvokeCallback invokeCallback, SemaphoreReleaseOnlyOnce once) {
        this.opaque = opaque;
        this.timeoutMillis = timeoutMillis;
        this.invokeCallback = invokeCallback;
        this.once = once;
    }
    
    /**
     * 执行回调方法
     */
    public void executeInvokeCallback() {
        if (invokeCallback != null) {
            // 判断是否false，如果是false，则将false修改为true，并执行回调函数
            if (this.executeCallbackOnlyOnce.compareAndSet(false, true)) {
                invokeCallback.operationComplete(this);
            }
        }
    }
    
    /**
     * 释放当前Future的仅一次信号量
     */
    public void release() {
        if (this.once != null) {
            this.once.release();
        }
    }
    
    /**
     * 判断请求是否超时
     *
     * @return
     */
    public boolean isTimeout() {
        long diff = System.currentTimeMillis() - this.beginTimestamp;
        return diff > this.timeoutMillis;
    }
    
    /**
     * 发送同步请求的时候，等待响应或者超时
     *
     * @param timeoutMillis
     * @return
     * @throws InterruptedException
     */
    public RemotingCommand waitResponse(final long timeoutMillis) throws InterruptedException {
        this.countDownLatch.await(timeoutMillis, TimeUnit.MILLISECONDS);
        return this.responseCommand;
    }
    
    /**
     * 将得到的响应命令设置到ResponseFuture，并通知可能正在等待响应的同步请求线程
     *
     * @param responseCommand
     */
    public void putResponse(final RemotingCommand responseCommand) {
        this.responseCommand = responseCommand;
        this.countDownLatch.countDown();
    }
    
    public long getBeginTimestamp() {
        return beginTimestamp;
    }
    
    public boolean isSendRequestOK() {
        return sendRequestOK;
    }
    
    public void setSendRequestOK(boolean sendRequestOK) {
        this.sendRequestOK = sendRequestOK;
    }
    
    public long getTimeoutMillis() {
        return timeoutMillis;
    }
    
    public InvokeCallback getInvokeCallback() {
        return invokeCallback;
    }
    
    public Throwable getCause() {
        return cause;
    }
    
    public void setCause(Throwable cause) {
        this.cause = cause;
    }
    
    public RemotingCommand getResponseCommand() {
        return responseCommand;
    }
    
    public void setResponseCommand(RemotingCommand responseCommand) {
        this.responseCommand = responseCommand;
    }
    
    public int getOpaque() {
        return opaque;
    }
    
    @Override
    public String toString() {
        return "ResponseFuture [responseCommand=" + responseCommand + ", sendRequestOK=" + sendRequestOK
                + ", cause=" + cause + ", opaque=" + opaque + ", timeoutMillis=" + timeoutMillis
                + ", invokeCallback=" + invokeCallback + ", beginTimestamp=" + beginTimestamp
                + ", countDownLatch=" + countDownLatch + "]";
    }
}
