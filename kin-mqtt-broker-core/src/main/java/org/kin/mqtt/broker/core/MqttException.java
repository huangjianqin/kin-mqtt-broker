package org.kin.mqtt.broker.core;

/**
 * mqtt相关异常
 *
 * @author huangjianqin
 * @date 2023/4/25
 */
public class MqttException extends RuntimeException {
    private static final long serialVersionUID = -1104874739544607942L;

    public MqttException() {
    }

    public MqttException(String message) {
        super(message);
    }

    public MqttException(String message, Throwable cause) {
        super(message, cause);
    }

    public MqttException(Throwable cause) {
        super(cause);
    }

    public MqttException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}