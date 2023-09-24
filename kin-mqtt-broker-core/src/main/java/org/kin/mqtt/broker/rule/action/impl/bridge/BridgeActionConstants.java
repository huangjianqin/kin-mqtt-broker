package org.kin.mqtt.broker.rule.action.impl.bridge;

/**
 * @author huangjianqin
 * @date 2023/9/23
 */
public final class BridgeActionConstants {
    /** bridge name */
    public static final String BRIDGE_KEY = "bridge";

    //----------------------------------------------------------------------------------------http
    /** http bridge post uri */
    public static final String HTTP_URI_KEY = "uri";
    /** http bridge post headers */
    public static final String HTTP_HEADERS_KEY = "headers";

    //-----------------------------------------------------------------------------------------kafka
    /** 发送的kafka topic */
    public static final String KAFKA_TOPIC_KEY = "topic";

    //-----------------------------------------------------------------------------------------rabbitMQ
    /** 要发送的rabbitMQ queue */
    public static final String RABBITMQ_QUEUE_KEY = "queue";

    private BridgeActionConstants() {
    }
}
