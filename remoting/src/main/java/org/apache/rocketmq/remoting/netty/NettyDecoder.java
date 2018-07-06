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

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import java.nio.ByteBuffer;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.common.RemotingUtil;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Netty解码器
 *
 * @Author Administrator
 * @Date 2018/6/29 0029 09 14
 * @Description
 */
public class NettyDecoder extends LengthFieldBasedFrameDecoder {
    
    private static final Logger log = LoggerFactory.getLogger(RemotingHelper.ROCKETMQ_REMOTING);
    
    // 最大接收并解码的消息长度
    private static final int FRAME_MAX_LENGTH = Integer.parseInt(System.getProperty("com.rocketmq.remoting.frameMaxLength", "16777216"));
    
    public NettyDecoder() {
        /**
         * maxFrameLength: 表示的是包的最大长度，超出包的最大长度netty将会做一些特殊处理；
         * lengthFieldOffset：指的是长度域的偏移量，表示跳过指定长度个字节之后的才是长度域；
         * lengthFieldLength：记录该帧数据长度的字段本身的长度；
         * lengthAdjustment：该字段加长度字段等于数据帧的长度，包体长度调整的大小，长度域的数值表示的长度加上这个修正值表示的就是带header的包；
         * initialBytesToStrip：从数据帧中跳过的字节数，表示获取完一个完整的数据包之后，忽略前面的指定的位数个字节，应用解码器拿到的就是不带长度域的数据包；
         */
        super(FRAME_MAX_LENGTH, 0, 4, 0, 4);
    }
    
    @Override
    public Object decode(ChannelHandlerContext ctx, ByteBuf in) throws Exception {
        ByteBuf frame = null;
        try {
            // 校验本次调用解码器时传入的数据包是否符合解码规则，如果不符合则校验方法返回值为空，表示还要继续等待接收新的数据包才能拼接成一个完整的数据包。
            // 本方法返回null，netty则放弃本次解码，不会将解码结果传入后面的ChannelHandler。只有返回的只不为空，表示解码出了一个完成的数据包，才会调用后面的ChannelHandler链
            frame = (ByteBuf) super.decode(ctx, in);
            if (null == frame) {
                return null;
            }
            
            // 将ByteBuf转换为固定大小的ByteBuffer
            ByteBuffer byteBuffer = frame.nioBuffer();
            
            // 解码整个命令并返回，表示本次解码成功，接收到了一个完整的数据包
            return RemotingCommand.decode(byteBuffer);
        } catch (Exception e) {
            // 解码数据时出现异常
            log.error("decode exception, " + RemotingHelper.parseChannelRemoteAddr(ctx.channel()), e);
            // 关闭连接通道
            RemotingUtil.closeChannel(ctx.channel());
        } finally {
            // 释放ByteBuf
            // 注意，这里不能释放传入的ByteBuf，因为该ByteBuf在其他的地方会被释放，如果这里释放了，则会抛出异常
            if (null != frame) {
                frame.release();
            }
        }
        
        return null;
    }
}
