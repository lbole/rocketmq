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
package org.apache.rocketmq.remoting.protocol;

import com.alibaba.fastjson.annotation.JSONField;
import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.rocketmq.remoting.CommandCustomHeader;
import org.apache.rocketmq.remoting.annotation.CFNotNull;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 远程命令协议
 */
public class RemotingCommand {
    
    // 命令（消息）序列化类型在JVM全局变量中的KEY，RocketMQ序列化、FastJSON序列化
    public static final String SERIALIZE_TYPE_PROPERTY = "rocketmq.serialize.type";
    // 命令（消息）序列化类型在环境变量中的KEY，RocketMQ序列化、FastJSON序列化
    public static final String SERIALIZE_TYPE_ENV = "ROCKETMQ_SERIALIZE_TYPE";
    // 命令（消息）版本信息在JVM全局变量中的KEY
    public static final String REMOTING_VERSION_KEY = "rocketmq.remoting.version";
    // 日志
    private static final Logger log = LoggerFactory.getLogger(RemotingHelper.ROCKETMQ_REMOTING);
    // 0, REQUEST_COMMAND 1, RESPONSE_COMMAND
    private static final int RPC_TYPE = 0;
    // 0, RPC 1, Oneway
    private static final int RPC_ONEWAY = 1;
    // 用于收集已知的自定义命令头，并与自定义头中的字段进行对应，在校验和编解码时使用。避免同一个自定义类型重复反射
    private static final Map<Class<? extends CommandCustomHeader>, Field[]> CLASS_HASH_MAP = new HashMap<>();
    // 用于收集自定义命令头字段类型的全限定名，用以判断该类型是否被支持编解码，避免同一类型执行getCanonicalName()方法
    private static final Map<Class, String> CANONICAL_NAME_CACHE = new HashMap<>();
    // 用于收集不能为空的自定义头部字段，用于校验字段时使用
    private static final Map<Field, Boolean> NULLABLE_FIELD_CACHE = new HashMap<>();
    // 配置支持编解码的自定义头部中的字段属性类型
    private static final String STRING_CANONICAL_NAME = String.class.getCanonicalName();
    private static final String DOUBLE_CANONICAL_NAME_1 = Double.class.getCanonicalName();
    private static final String DOUBLE_CANONICAL_NAME_2 = double.class.getCanonicalName();
    private static final String INTEGER_CANONICAL_NAME_1 = Integer.class.getCanonicalName();
    private static final String INTEGER_CANONICAL_NAME_2 = int.class.getCanonicalName();
    private static final String LONG_CANONICAL_NAME_1 = Long.class.getCanonicalName();
    private static final String LONG_CANONICAL_NAME_2 = long.class.getCanonicalName();
    private static final String BOOLEAN_CANONICAL_NAME_1 = Boolean.class.getCanonicalName();
    private static final String BOOLEAN_CANONICAL_NAME_2 = boolean.class.getCanonicalName();
    // 全局信息版本信息
    private static volatile int configVersion = -1;
    // 用于生成请求响应ID的计数器
    private static AtomicInteger requestId = new AtomicInteger(0);
    // 全局命令序列化配置
    private static SerializeType serializeTypeConfigInThisServer = SerializeType.JSON;
    
    /**
     * 初始化时获取配置序列化配置信息
     */
    static {
        final String protocol = System.getProperty(SERIALIZE_TYPE_PROPERTY, System.getenv(SERIALIZE_TYPE_ENV));
        if (!isBlank(protocol)) {
            try {
                serializeTypeConfigInThisServer = SerializeType.valueOf(protocol);
            } catch (IllegalArgumentException e) {
                throw new RuntimeException("parser specified protocol error. protocol=" + protocol, e);
            }
        }
    }
    
    // 请求代码，用来区分命令类型（成功、失败、系统繁忙、不支持的命令、事务错误），可用以在服务端匹配对应的请求处理器
    private int code;
    // 客户端语言类型
    private LanguageCode language = LanguageCode.JAVA;
    // 当前命令实例版本（协议版本，用于TLS传输层安全认证握手请求阶段使用）
    private int version = 0;
    // 命令ID（唯一识别码），用于发送请求和接受响应时做对应
    private int opaque = requestId.getAndIncrement();
    // 命令标记，可通过按位运算获取命令类型（请求命令、响应命令）、RPC类型（是否为单向的请求命令）
    private int flag = 0;
    // 命令的备注
    private String remark;
    // 存放当前命令实例自定义头的所有属性有效名称与值的映射，值全部转化为字符串存储
    private HashMap<String, String> extFields;
    // 自定义命令类型
    private transient CommandCustomHeader customHeader;
    // 当前命令实例序列化类型
    private SerializeType serializeTypeCurrentRPC = serializeTypeConfigInThisServer;
    // 命令类容
    private transient byte[] body;
    
    protected RemotingCommand() {
    }
    
    /**
     * 创建一个请求命令
     *
     * @param code
     * @param customHeader
     * @return
     */
    public static RemotingCommand createRequestCommand(int code, CommandCustomHeader customHeader) {
        RemotingCommand cmd = new RemotingCommand();
        // 设置命令代码
        cmd.setCode(code);
        // 设置自定义命令头部
        cmd.customHeader = customHeader;
        // 设置命令版本
        setCmdVersion(cmd);
        return cmd;
    }
    
    /**
     * 设置静态的全局版本（更新静态变量的值、并且更新JVM全局变量的值）
     *
     * @param cmd
     */
    private static void setCmdVersion(RemotingCommand cmd) {
        if (configVersion >= 0) {
            cmd.setVersion(configVersion);
        } else {
            String v = System.getProperty(REMOTING_VERSION_KEY);
            if (v != null) {
                int value = Integer.parseInt(v);
                cmd.setVersion(value);
                configVersion = value;
            }
        }
    }
    
    /**
     * 创建一个异常的响应命名
     *
     * @param classHeader
     * @return
     */
    public static RemotingCommand createResponseCommand(Class<? extends CommandCustomHeader> classHeader) {
        return createResponseCommand(RemotingSysResponseCode.SYSTEM_ERROR, "not set any response code", classHeader);
    }
    
    /**
     * 创建响应命令
     *
     * @param code
     * @param remark
     * @param classHeader
     * @return
     */
    public static RemotingCommand createResponseCommand(int code, String remark, Class<? extends CommandCustomHeader> classHeader) {
        RemotingCommand cmd = new RemotingCommand();
        // 将命令标记为响应命令
        cmd.markResponseType();
        // 设置命令代码
        cmd.setCode(code);
        // 设置命令备注
        cmd.setRemark(remark);
        // 设置命令版本
        setCmdVersion(cmd);
        
        // 如果命令头类型不为空，则说明需要头信息
        if (classHeader != null) {
            try {
                // 利用反射实例化一个命令头，并赋值
                CommandCustomHeader objectHeader = classHeader.newInstance();
                cmd.customHeader = objectHeader;
            } catch (InstantiationException e) {
                return null;
            } catch (IllegalAccessException e) {
                return null;
            }
        }
        
        return cmd;
    }
    
    /**
     * 没有命令头的响应命令
     *
     * @param code
     * @param remark
     * @return
     */
    public static RemotingCommand createResponseCommand(int code, String remark) {
        return createResponseCommand(code, remark, null);
    }
    
    /**
     * 命令解码
     *
     * @param array
     * @return
     */
    public static RemotingCommand decode(final byte[] array) {
        ByteBuffer byteBuffer = ByteBuffer.wrap(array);
        return decode(byteBuffer);
    }
    
    /**
     * 命令解码
     *
     * @param byteBuffer
     * @return
     */
    public static RemotingCommand decode(final ByteBuffer byteBuffer) {
        // 获取命令的长度（原始长度字节在调用该方法之前已经处理了，并没有写入传入的参数ByteBuffer中）
        int length = byteBuffer.limit();
        // 获取命令头与命令长度之间的4个字节
        int oriHeaderLen = byteBuffer.getInt();
        // 获取命令头长度
        int headerLength = getHeaderLength(oriHeaderLen);
        // 获取命令头类容
        byte[] headerData = new byte[headerLength];
        byteBuffer.get(headerData);
        
        // 先将头解码，并返回一个命令对象
        RemotingCommand cmd = headerDecode(headerData, getProtocolType(oriHeaderLen));
        
        // 获取消息主体数据长度
        int bodyLength = length - 4 - headerLength;
        byte[] bodyData = null;
        // 如果数据命令主题长度大于0，则赋值
        if (bodyLength > 0) {
            bodyData = new byte[bodyLength];
            byteBuffer.get(bodyData);
        }
        cmd.body = bodyData;
        
        return cmd;
    }
    
    /**
     * 获取命令长度
     *
     * @param length
     * @return
     */
    public static int getHeaderLength(int length) {
        // 不去最高位字节
        return length & 0xFFFFFF;
    }
    
    /**
     * 命令头解码
     *
     * @param headerData
     * @param type
     * @return
     */
    private static RemotingCommand headerDecode(byte[] headerData, SerializeType type) {
        switch (type) {
            case JSON:
                RemotingCommand resultJson = RemotingSerializable.decode(headerData, RemotingCommand.class);
                resultJson.setSerializeTypeCurrentRPC(type);
                return resultJson;
            case ROCKETMQ:
                RemotingCommand resultRMQ = RocketMQSerializable.rocketMQProtocolDecode(headerData);
                resultRMQ.setSerializeTypeCurrentRPC(type);
                return resultRMQ;
            default:
                break;
        }
        
        return null;
    }
    
    /**
     * 获取序列化类型（协议类型）
     *
     * @param source
     * @return
     */
    public static SerializeType getProtocolType(int source) {
        // 只取最高位
        return SerializeType.valueOf((byte) ((source >> 24) & 0xFF));
    }
    
    /**
     * 创建一个新的请求ID
     *
     * @return
     */
    public static int createNewRequestId() {
        return requestId.incrementAndGet();
    }
    
    /**
     * 获取全局的序列化协议类型
     *
     * @return
     */
    public static SerializeType getSerializeTypeConfigInThisServer() {
        return serializeTypeConfigInThisServer;
    }
    
    /**
     * 判断字符串是否为空白，是为true，否为false
     * 字符串为空或者长度为零，再或者字符串中存在java空白符等，都返回true
     *
     * @param str
     * @return
     */
    private static boolean isBlank(String str) {
        int strLen;
        if (str == null || (strLen = str.length()) == 0) {
            return true;
        }
        for (int i = 0; i < strLen; i++) {
            if (!Character.isWhitespace(str.charAt(i))) {
                return false;
            }
        }
        return true;
    }
    
    /**
     * 生成协议（序列化）类型（包括序列化类型和命令头长度）
     *
     * @param source
     * @param type
     * @return
     */
    public static byte[] markProtocolType(int source, SerializeType type) {
        byte[] result = new byte[4];
        
        // 设置序列化类型
        result[0] = type.getCode();
        // 设置命令有长度
        result[1] = (byte) ((source >> 16) & 0xFF);
        result[2] = (byte) ((source >> 8) & 0xFF);
        result[3] = (byte) (source & 0xFF);
        return result;
    }
    
    /**
     * 将命令设置成相应类型
     */
    public void markResponseType() {
        int bits = 1 << RPC_TYPE;
        this.flag |= bits;
    }
    
    /**
     * 获命令头
     *
     * @return
     */
    public CommandCustomHeader readCustomHeader() {
        return customHeader;
    }
    
    /**
     * 设置命令头
     *
     * @param customHeader
     */
    public void writeCustomHeader(CommandCustomHeader customHeader) {
        this.customHeader = customHeader;
    }
    
    /**
     * 解码命令头
     *
     * @param classHeader
     * @return
     * @throws RemotingCommandException
     */
    public CommandCustomHeader decodeCommandCustomHeader(Class<? extends CommandCustomHeader> classHeader) throws RemotingCommandException {
        CommandCustomHeader objectHeader;
        try {
            // 将自定义命令头类进行实例化
            objectHeader = classHeader.newInstance();
        } catch (InstantiationException e) {
            return null;
        } catch (IllegalAccessException e) {
            return null;
        }
        
        // 如果命令头字段不为空（存放命令头字段值的容器）
        if (this.extFields != null) {
            
            // 获取自定义命令头的所有字段，并遍历进行处理
            Field[] fields = getClazzFields(classHeader);
            for (Field field : fields) {
                // 不处理静态属性
                if (!Modifier.isStatic(field.getModifiers())) {
                    // 不处理以this开头名称的字段
                    String fieldName = field.getName();
                    if (!fieldName.startsWith("this")) {
                        try {
                            // 获取字段的值（值全部为字符串），并值判断是否为空
                            String value = this.extFields.get(fieldName);
                            if (null == value) {
                                // 如果字段值为空，则判断该字段值是否能够为空，如果不能则抛出异常
                                if (!isFieldNullable(field)) {
                                    throw new RemotingCommandException("the custom field <" + fieldName + "> is null");
                                }
                                // 字段值为空，并且字段值可以为空，跳过当前循环
                                continue;
                            }
                            
                            // 无论该字段是否为私有的，都设置为可以访问
                            field.setAccessible(true);
                            // 获取字段类型全限定名
                            String type = getCanonicalName(field.getType());
                            Object valueParsed;
                            
                            // 将字段的值转化为相应的类型
                            if (type.equals(STRING_CANONICAL_NAME)) {
                                valueParsed = value;
                            } else if (type.equals(INTEGER_CANONICAL_NAME_1) || type.equals(INTEGER_CANONICAL_NAME_2)) {
                                valueParsed = Integer.parseInt(value);
                            } else if (type.equals(LONG_CANONICAL_NAME_1) || type.equals(LONG_CANONICAL_NAME_2)) {
                                valueParsed = Long.parseLong(value);
                            } else if (type.equals(BOOLEAN_CANONICAL_NAME_1) || type.equals(BOOLEAN_CANONICAL_NAME_2)) {
                                valueParsed = Boolean.parseBoolean(value);
                            } else if (type.equals(DOUBLE_CANONICAL_NAME_1) || type.equals(DOUBLE_CANONICAL_NAME_2)) {
                                valueParsed = Double.parseDouble(value);
                            } else {
                                // 如果字段的类型不被支持，则抛出异常
                                throw new RemotingCommandException("the custom field <" + fieldName + "> type is not supported");
                            }
                            
                            // 将值设置到实例化的对象中
                            field.set(objectHeader, valueParsed);
                            
                        } catch (Throwable e) {
                            log.error("Failed field [{}] decoding", fieldName, e);
                        }
                    }
                }
            }
            // 检查字段
            objectHeader.checkFields();
        }
        
        return objectHeader;
    }
    
    /**
     * 根据自定义命令头类型获取所有字段
     * 如果该类型没有被缓存，则使用反射获取所有字段进行缓存并返回
     *
     * @param classHeader
     * @return
     */
    private Field[] getClazzFields(Class<? extends CommandCustomHeader> classHeader) {
        Field[] field = CLASS_HASH_MAP.get(classHeader);
        
        if (field == null) {
            field = classHeader.getDeclaredFields();
            synchronized (CLASS_HASH_MAP) {
                CLASS_HASH_MAP.put(classHeader, field);
            }
        }
        return field;
    }
    
    /**
     * 判断字段是否能够为空
     * 首先按从缓存中获取字段是否能够为空的注解，如果不存在则先反射获取字段的注解进行缓存，然后再判断
     *
     * @param field
     * @return
     */
    private boolean isFieldNullable(Field field) {
        if (!NULLABLE_FIELD_CACHE.containsKey(field)) {
            Annotation annotation = field.getAnnotation(CFNotNull.class);
            synchronized (NULLABLE_FIELD_CACHE) {
                NULLABLE_FIELD_CACHE.put(field, annotation == null);
            }
        }
        return NULLABLE_FIELD_CACHE.get(field);
    }
    
    /**
     * 获取类型的有效名称（全限定名）
     * 首先从缓存中获取，如果不存在先反射获取字段名进行缓存，然后返回
     *
     * @param clazz
     * @return
     */
    private String getCanonicalName(Class clazz) {
        String name = CANONICAL_NAME_CACHE.get(clazz);
        
        if (name == null) {
            name = clazz.getCanonicalName();
            synchronized (CANONICAL_NAME_CACHE) {
                CANONICAL_NAME_CACHE.put(clazz, name);
            }
        }
        return name;
    }
    
    /**
     * 编码命令
     *
     * @return
     */
    public ByteBuffer encode() {
        // 1> 命令头长度字节数组大大小
        int length = 4;
        
        // 2> 编码命令头，并加上命令头长度
        byte[] headerData = this.headerEncode();
        length += headerData.length;
        
        // 3> 加上主题数据长度
        if (this.body != null) {
            length += body.length;
        }
        
        // 申请一个ByteBuffer
        ByteBuffer result = ByteBuffer.allocate(4 + length);
        
        // 向ByteBuffer中添加命令长度
        result.putInt(length);
        
        // 添加序列化协议和命令头长度
        result.put(markProtocolType(headerData.length, serializeTypeCurrentRPC));
        
        // 添加命令头数据
        result.put(headerData);
        
        // 添加主题数据
        if (this.body != null) {
            result.put(this.body);
        }
        
        result.flip();
        
        return result;
    }
    
    /**
     * 命令头编码
     *
     * @return
     */
    private byte[] headerEncode() {
        // 将自定义头的字段的值添加到自定义命令头字段容器中
        this.makeCustomHeaderToNet();
        // 设置命令序列化类型
        if (SerializeType.ROCKETMQ == serializeTypeCurrentRPC) {
            return RocketMQSerializable.rocketMQProtocolEncode(this);
        } else {
            return RemotingSerializable.encode(this);
        }
    }
    
    /**
     * 构建自定义命令头
     * 将自定义命令头中的字段值放进扩展字段容器中
     */
    public void makeCustomHeaderToNet() {
        if (this.customHeader != null) {
            // 获取自定义头的所有字段
            Field[] fields = getClazzFields(customHeader.getClass());
            // 如果自定义头字段容器为空，则初始化
            if (null == this.extFields) {
                this.extFields = new HashMap<>();
            }
            
            for (Field field : fields) {
                // 不处理静态属性
                if (!Modifier.isStatic(field.getModifiers())) {
                    // 不处理以this开头名称的属性
                    String name = field.getName();
                    if (!name.startsWith("this")) {
                        Object value = null;
                        try {
                            // 将可能为私有的字段设置为可以访问的
                            field.setAccessible(true);
                            // 获取字段的值
                            value = field.get(this.customHeader);
                        } catch (Exception e) {
                            log.error("Failed to access field [{}]", name, e);
                        }
                        
                        if (value != null) {
                            // 如果字段的值不为空，则将值加入自定义头字段容器
                            this.extFields.put(name, value.toString());
                        }
                    }
                }
            }
        }
    }
    
    /**
     * 编码自定义命令头
     *
     * @return
     */
    public ByteBuffer encodeHeader() {
        return encodeHeader(this.body != null ? this.body.length : 0);
    }
    
    /**
     * 编码自定义命令头
     *
     * @param bodyLength
     * @return
     */
    public ByteBuffer encodeHeader(final int bodyLength) {
        // 1> header length size
        int length = 4;
        
        // 2> header data length
        byte[] headerData;
        headerData = this.headerEncode();
        
        length += headerData.length;
        
        // 3> body data length
        length += bodyLength;
        
        ByteBuffer result = ByteBuffer.allocate(4 + length - bodyLength);
        
        // length
        result.putInt(length);
        
        // header length
        result.put(markProtocolType(headerData.length, serializeTypeCurrentRPC));
        
        // header data
        result.put(headerData);
        
        result.flip();
        
        return result;
    }
    
    /**
     * 设置命令为单向命令
     */
    public void markOnewayRPC() {
        int bits = 1 << RPC_ONEWAY;
        this.flag |= bits;
    }
    
    @JSONField(serialize = false)
    public boolean isOnewayRPC() {
        int bits = 1 << RPC_ONEWAY;
        return (this.flag & bits) == bits;
    }
    
    public int getCode() {
        return code;
    }
    
    public void setCode(int code) {
        this.code = code;
    }
    
    @JSONField(serialize = false)
    public RemotingCommandType getType() {
        if (this.isResponseType()) {
            return RemotingCommandType.RESPONSE_COMMAND;
        }
        
        return RemotingCommandType.REQUEST_COMMAND;
    }
    
    @JSONField(serialize = false)
    public boolean isResponseType() {
        int bits = 1 << RPC_TYPE;
        return (this.flag & bits) == bits;
    }
    
    public LanguageCode getLanguage() {
        return language;
    }
    
    public void setLanguage(LanguageCode language) {
        this.language = language;
    }
    
    public int getVersion() {
        return version;
    }
    
    public void setVersion(int version) {
        this.version = version;
    }
    
    public int getOpaque() {
        return opaque;
    }
    
    public void setOpaque(int opaque) {
        this.opaque = opaque;
    }
    
    public int getFlag() {
        return flag;
    }
    
    public void setFlag(int flag) {
        this.flag = flag;
    }
    
    public String getRemark() {
        return remark;
    }
    
    public void setRemark(String remark) {
        this.remark = remark;
    }
    
    public byte[] getBody() {
        return body;
    }
    
    public void setBody(byte[] body) {
        this.body = body;
    }
    
    public HashMap<String, String> getExtFields() {
        return extFields;
    }
    
    public void setExtFields(HashMap<String, String> extFields) {
        this.extFields = extFields;
    }
    
    public void addExtField(String key, String value) {
        if (null == extFields) {
            extFields = new HashMap<>();
        }
        extFields.put(key, value);
    }
    
    @Override
    public String toString() {
        return "RemotingCommand [code=" + code + ", language=" + language + ", version=" + version + ", opaque=" + opaque + ", flag(B)="
                + Integer.toBinaryString(flag) + ", remark=" + remark + ", extFields=" + extFields + ", serializeTypeCurrentRPC="
                + serializeTypeCurrentRPC + "]";
    }
    
    public SerializeType getSerializeTypeCurrentRPC() {
        return serializeTypeCurrentRPC;
    }
    
    public void setSerializeTypeCurrentRPC(SerializeType serializeTypeCurrentRPC) {
        this.serializeTypeCurrentRPC = serializeTypeCurrentRPC;
    }
}