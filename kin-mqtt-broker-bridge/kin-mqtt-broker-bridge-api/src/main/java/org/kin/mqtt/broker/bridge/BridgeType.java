package org.kin.mqtt.broker.bridge;

/**
 * 消息数据桥接类型
 *
 * @author huangjianqin
 * @date 2022/11/21
 */
public interface BridgeType {
    String HTTP = "http";
    String KAFKA = "kafka";
    String RABBITMQ = "rabbitMQ";
}
