package org.kin.mqtt.broker.core;

import com.google.common.base.Preconditions;
import org.kin.framework.utils.StringUtils;
import org.kin.mqtt.broker.core.cluster.ClusterConfig;

/**
 * @author huangjianqin
 * @date 2022/12/23
 */
public class MqttBrokerConfig {
    /** broker id, 会被用作client id, 比如系统topic消息, 默认是0 */
    private String brokerId = "0";
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
    /** 单个连接流量整型(/s), 默认无限制 */
    private int connBytesPerSec = -1;
    /** 单个连接消息速率整型(/s), 默认无限制 */
    private int connMessagePerSec = -1;
    /** 允许连接建立速率(/s), 默认无限制 */
    private int connectPerSec = -1;
    /** cluster store数据存储目录 */
    private String dataPath = "data";
    /** mqtt broker集群配置 */
    private ClusterConfig cluster;

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

        if (cluster == null) {
            cluster = ClusterConfig.DEFAULT;
        }
    }

    /**
     * 定义broker唯一id
     */
    public MqttBrokerConfig brokerId(String brokerId) {
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

    /**
     * 开启ssl
     */
    public MqttBrokerConfig ssl(boolean ssl) {
        this.ssl = ssl;
        return this;
    }

    /**
     * 配置cert file
     */
    public MqttBrokerConfig certFile(String certFile) {
        this.certFile = certFile;
        return this;
    }

    /**
     * 配置cert key file
     */
    public MqttBrokerConfig certKeyFile(String certKeyFile) {
        this.certKeyFile = certKeyFile;
        return this;
    }

    /**
     * 配置ca file
     */
    public MqttBrokerConfig caFile(String caFile) {
        this.caFile = caFile;
        return this;
    }

    /**
     * 是否开启系统topic
     */
    public MqttBrokerConfig enableSysTopic(boolean enableSysTopic) {
        this.enableSysTopic = enableSysTopic;
        return this;
    }

    /**
     * 单个连接流量整型(/s), 默认无限制
     */
    public MqttBrokerConfig connBytesPerSec(int connBytesPerSec) {
        this.connBytesPerSec = connBytesPerSec;
        return this;
    }

    /**
     * 单个连接消息数整型(/s), 默认无限制
     */
    public MqttBrokerConfig connMessagePerSec(int connMessagePerSec) {
        this.connMessagePerSec = connMessagePerSec;
        return this;
    }

    /**
     * 允许连接建立速率(/s), 默认无限制
     */
    public MqttBrokerConfig connectPerSec(int connectPerSec) {
        this.connectPerSec = connectPerSec;
        return this;
    }

    /**
     * mqtt broker集群配置
     */
    public MqttBrokerConfig cluster(ClusterConfig cluster) {
        this.cluster = cluster;
        return this;
    }


    /**
     * cluster store数据存储目录
     */
    public MqttBrokerConfig dataPath(String dataPath) {
        this.dataPath = dataPath;
        return this;
    }

    //setter && getter
    public String getBrokerId() {
        return brokerId;
    }

    public void setBrokerId(String brokerId) {
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

    public void setReceiveMaximum(int receiveMaximum) {
        this.receiveMaximum = receiveMaximum;
    }

    public int getConnBytesPerSec() {
        return connBytesPerSec;
    }

    public int getConnMessagePerSec() {
        return connMessagePerSec;
    }

    public int getConnectPerSec() {
        return connectPerSec;
    }

    public void setConnectPerSec(int connectPerSec) {
        this.connectPerSec = connectPerSec;
    }

    public String getDataPath() {
        return dataPath;
    }

    public void setDataPath(String dataPath) {
        this.dataPath = dataPath;
    }

    public ClusterConfig getCluster() {
        return cluster;
    }

    public void setCluster(ClusterConfig cluster) {
        this.cluster = cluster;
    }

}
