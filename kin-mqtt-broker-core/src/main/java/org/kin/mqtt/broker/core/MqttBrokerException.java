package org.kin.mqtt.broker.core;

/**
 * mqtt broker相关异常
 *
 * @author huangjianqin
 * @date 2023/4/16
 */
public class MqttBrokerException extends RuntimeException {
    private static final long serialVersionUID = 4300617163388896045L;

    public MqttBrokerException() {
    }

    public MqttBrokerException(String message) {
        super(message);
    }

    public MqttBrokerException(String message, Throwable cause) {
        super(message, cause);
    }

    public MqttBrokerException(Throwable cause) {
        super(cause);
    }

    public MqttBrokerException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
