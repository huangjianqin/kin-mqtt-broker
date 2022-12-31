package org.kin.mqtt.broker.core;

import com.google.common.base.Preconditions;
import org.kin.framework.utils.StringUtils;

/**
 * @author huangjianqin
 * @date 2022/12/23
 */
public class MqttBrokerConfig {
    /** broker id, 会被用作client id, 比如系统topic消息, 默认是0 */
    private int brokerId;
    /** mqtt broker port, default 1883 */
    private int port = 1883;
    /** mqtt broker websocket port, default 0, 默认不开启 */
    private int wsPort;
    /** websocket握手地址 */
    private String wsPath = "/";
    /** 最大消息大小, 默认4MB */
    private int messageMaxSize = 4194304;
    /** 底层tcp连接是否启动ssl */
    private boolean ssl;
    /** 证书 */
    private String certFile;
    /** 证书密钥 */
    private String certKeyFile;
    /** CA根证书 */
    private String caFile;
    /** 是否开启系统topic */
    private boolean enableSysTopic;
    /** 系统topic推送间隔(秒), 只针对部分系统topic有效, 默认1分钟 */
    private int sysTopicInterval = 60;
    /** 单个接收端愿意同时处理的QoS为1和2的PUBLISH消息最大数量, 默认64 */
    private int receiveMaximum = 64;

    public static MqttBrokerConfig create() {
        return new MqttBrokerConfig();
    }

    protected MqttBrokerConfig() {
    }

    /**
     * 检查配置是符合要求
     */
    public void selfCheck() {
        Preconditions.checkArgument(port > 0, "mqtt broker port must be greater than 0");
        Preconditions.checkArgument(wsPort >= 0, "mqtt broker websocket port must be greater than 0");
        Preconditions.checkArgument(StringUtils.isNotBlank(wsPath), "mqtt broker websocket handshake path must be not blank");
        Preconditions.checkArgument(messageMaxSize > 0, "messageMaxSize must be greater than 0");
        if (ssl) {
            Preconditions.checkArgument(StringUtils.isNotBlank(certFile), "certFile must be not blank if open ssl");
            Preconditions.checkArgument(StringUtils.isNotBlank(certKeyFile), "certKeyFile must be not blank if open ssl");
            Preconditions.checkArgument(StringUtils.isNotBlank(caFile), "caFile must be not blank if open ssl");
        }
        Preconditions.checkArgument(sysTopicInterval > 0, "sysTopicInterval must be greater than 0");
        Preconditions.checkArgument(receiveMaximum > 0, "receiveMaximum must be greater than 0");
    }

    /**
     * 定义broker唯一id
     */
    public MqttBrokerConfig brokerId(int brokerId) {
        this.brokerId = brokerId;
        return this;
    }

    /**
     * 定义mqtt server port
     */
    public MqttBrokerConfig port(int port) {
        Preconditions.checkArgument(port > 0, "port must be greater than 0");
        this.port = port;
        return this;
    }


    /**
     * 定义mqtt server websocket port
     */
    public MqttBrokerConfig wsPort(int wsPort) {
        this.wsPort = wsPort;
        return this;
    }

    /**
     * websocket握手地址, 默认'/'
     */
    public MqttBrokerConfig wsPath(String wsPath) {
        this.wsPath = wsPath;
        return this;
    }

    /**
     * 最大消息大小设置
     */
    public MqttBrokerConfig messageMaxSize(int messageMaxSize) {
        Preconditions.checkArgument(port > 0, "messageMaxSize must be greater than 0");
        this.messageMaxSize = messageMaxSize;
        return this;
    }

    /**
     * 是否开启系统topic
     */
    public MqttBrokerConfig enableSysTopic() {
        this.enableSysTopic = true;
        return this;
    }

    /**
     * 系统topic推送间隔(秒), 只针对部分系统topic有效, 默认1分钟
     */
    public MqttBrokerConfig sysTopicInterval(int sysTopicInterval) {
        this.sysTopicInterval = sysTopicInterval;
        return this;
    }

    /**
     * 接收端愿意同时处理的QoS为1和2的PUBLISH消息最大数量, 默认64
     */
    public MqttBrokerConfig receiveMaximum(int receiveMaximum) {
        this.receiveMaximum = receiveMaximum;
        return this;
    }

    //setter && getter
    public int getBrokerId() {
        return brokerId;
    }

    public void setBrokerId(int brokerId) {
        this.brokerId = brokerId;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public int getWsPort() {
        return wsPort;
    }

    public void setWsPort(int wsPort) {
        this.wsPort = wsPort;
    }

    public String getWsPath() {
        return wsPath;
    }

    public void setWsPath(String wsPath) {
        this.wsPath = wsPath;
    }

    public int getMessageMaxSize() {
        return messageMaxSize;
    }

    public void setMessageMaxSize(int messageMaxSize) {
        this.messageMaxSize = messageMaxSize;
    }

    public boolean isSsl() {
        return ssl;
    }

    public void setSsl(boolean ssl) {
        this.ssl = ssl;
    }

    public String getCertFile() {
        return certFile;
    }

    public void setCertFile(String certFile) {
        this.certFile = certFile;
    }

    public String getCertKeyFile() {
        return certKeyFile;
    }

    public void setCertKeyFile(String certKeyFile) {
        this.certKeyFile = certKeyFile;
    }

    public String getCaFile() {
        return caFile;
    }

    public void setCaFile(String caFile) {
        this.caFile = caFile;
    }

    public boolean isEnableSysTopic() {
        return enableSysTopic;
    }

    public void setEnableSysTopic(boolean enableSysTopic) {
        this.enableSysTopic = enableSysTopic;
    }

    public int getSysTopicInterval() {
        return sysTopicInterval;
    }

    public void setSysTopicInterval(int sysTopicInterval) {
        this.sysTopicInterval = sysTopicInterval;
    }

    public int getReceiveMaximum() {
        return receiveMaximum;
    }
}
