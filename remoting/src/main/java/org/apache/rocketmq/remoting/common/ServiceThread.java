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
package org.apache.rocketmq.remoting.common;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base class for background thread
 * 后台线程的基类
 * <p>
 * 线程的join():
 * join()方法的原理就是调用相应线程的wait方法进行等待操作的，例如A线程中调用了B线程的join方法，则相当于在A线程中调用了B线程的wait方法，
 * 当B线程执行完（或者到达等待时间），B线程会自动调用自身的notifyAll方法唤醒A线程，从而达到同步的目的。
 * <p>
 * 线程的interrupt():
 * 当调用线程的interrupt()方法之后，其实并不能将线程进行中断。其作用是，如果线程正在阻塞状态（比如Thread.sleep(...)、Object.wait()、
 * thread.join()、ServerSocket.accept()、DatagramSocket.receive()），那么该方法会取消线程的所有阻塞，然后被阻塞的线程抛出一个InterruptedException异常。
 * 而如果线程没有被阻塞，则该方法不会起到任何作用，仅仅只是可以使用interrupted()方法检查线程是否被标记并取消标记和isInterrupte()方法仅作为检查标记。
 * 所以，如果想中断某一个线程，那么最好的办法是需要给线程一个停止状态，初始为false。停止之前先调用interrupt()取消可能会存在的阻塞（或者按正常的方式取
 * 消对应的阻塞，比方说使用notify()取消wait()的阻塞），然后在代码内部判断是否停止状态，使用业务代码进行优雅的停止线程
 */
public abstract class ServiceThread implements Runnable {
    
    private static final Logger log = LoggerFactory.getLogger(RemotingHelper.ROCKETMQ_REMOTING);
    
    private static final long JOIN_TIME = 90 * 1000;
    // 后台执行的线程
    protected final Thread thread;
    // 是否已经最后释放了等待（最后调用this.notify()）
    protected volatile boolean hasNotified = false;
    // 已经停止状态
    protected volatile boolean stopped = false;
    
    public ServiceThread() {
        this.thread = new Thread(this, this.getServiceName());
    }
    
    public abstract String getServiceName();
    
    public void start() {
        this.thread.start();
    }
    
    public void shutdown() {
        this.shutdown(false);
    }
    
    public void shutdown(final boolean interrupt) {
        this.stopped = true;
        log.info("shutdown thread " + this.getServiceName() + " interrupt " + interrupt);
        synchronized (this) {
            if (!this.hasNotified) {
                // 最后一次通知在此对象上等待的线程
                // 这是用来处理实现类有可能由于某种原因（比如没有当前没有任务，需要等待新的任务进行处理）正在此对象上进行等待（wait()），而造成不能立刻进行优雅的关闭
                this.hasNotified = true;
                this.notify();
            }
        }
        
        try {
            if (interrupt) {
                // 取消线程可能存在的任何阻塞
                this.thread.interrupt();
            }
            
            long beginTime = System.currentTimeMillis();
            // 上面已经通过一些办法将肯能存在的阻塞取消掉了
            // 那么就在有限的时间之内完成未完成的工作，如果超时了则不等待（这可能会出现未知的情况）
            this.thread.join(this.getJointime());
            long eclipseTime = System.currentTimeMillis() - beginTime;
            log.info("join thread " + this.getServiceName() + " eclipse time(ms) " + eclipseTime + " " + this.getJointime());
        } catch (InterruptedException e) {
            log.error("Interrupted", e);
        }
    }
    
    public long getJointime() {
        return JOIN_TIME;
    }
    
    public boolean isStopped() {
        return stopped;
    }
}
