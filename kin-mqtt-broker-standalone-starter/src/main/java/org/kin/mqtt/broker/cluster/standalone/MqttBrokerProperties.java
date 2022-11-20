package org.kin.mqtt.broker.cluster.standalone;

import org.kin.mqtt.broker.core.Constants;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * @author huangjianqin
 * @date 2022/11/19
 */
@ConfigurationProperties(Constants.PROPERTIES_PREFIX)
public class MqttBrokerProperties {
    /** mqtt broker port, default 1883 */
    private int port = 1883;
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

    //setter && getter

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
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
