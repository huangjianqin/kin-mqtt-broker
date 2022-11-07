package org.kin.mqtt.broker;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * @author huangjianqin
 * @date 2022/11/6
 */
@ConfigurationProperties("org.kin.mqtt.broker")
public class MqttBrokerProperties {
    /** mqtt broker port, default 1883 */
    private int port = 1883;

    //setter && getter
    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }
}
