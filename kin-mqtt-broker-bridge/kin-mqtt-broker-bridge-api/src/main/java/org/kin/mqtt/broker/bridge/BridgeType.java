package org.kin.mqtt.broker.bridge;

/**
 * 消息数据桥接类型
 *
 * @author huangjianqin
 * @date 2022/11/21
 */
public enum BridgeType {
    /** http 传输 */
    HTTP,
    /** 传输到kafka topic */
    KAFKA,
    /** 传输到rabbitmq topic */
    RABBITMQ,
    ;
}
