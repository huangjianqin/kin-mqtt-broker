package org.kin.mqtt.broker.bridge;

/**
 * 数据桥接会使用的规则链属性名
 *
 * @author huangjianqin
 * @date 2022/11/22
 */
public interface BridgeAttrNames {
    //--------------------------------------------http--------------------------------------------
    /** http bridge post uri */
    String HTTP_URI = "HTTP_URI";
    /** http bridge post headers */
    String HTTP_HEADERS = "HTTP_HEADERS";

    //---------------------------------------------kafka--------------------------------------------
    /** 发送的kafka topic */
    String KAFKA_TOPIC = "KAFKA_TOPIC";

    //---------------------------------------------rabbitMQ--------------------------------------------
    /** 要发送的rabbitMQ queue */
    String RABBITMQ_QUEUE = "RABBITMQ_QUEUE";

}
