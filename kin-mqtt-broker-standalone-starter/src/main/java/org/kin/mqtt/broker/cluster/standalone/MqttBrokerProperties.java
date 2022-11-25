package org.kin.mqtt.broker.cluster.standalone;

import org.kin.mqtt.broker.Constants;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * @author huangjianqin
 * @date 2022/11/19
 */
@ConfigurationProperties(Constants.COMMON_PROPERTIES_PREFIX)
public class MqttBrokerProperties {
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

    /**
     * @return 是否开启mqtt broker over websocket
     */
    public boolean isOverWebsocket() {
        return wsPort > 0;
    }

    //setter && getter
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
}
