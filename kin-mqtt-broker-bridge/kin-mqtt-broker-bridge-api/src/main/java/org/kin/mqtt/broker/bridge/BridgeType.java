package org.kin.mqtt.broker.bridge;

/**
 * 消息数据桥接类型
 *
 * @author huangjianqin
 * @date 2022/11/21
 */
public enum BridgeType {
    /** http 传输 */
    HTTP("default_http_bridge"),
    /** 传输到kafka topic */
    KAFKA("default_kafka_bridge"),
    /** 传输到rabbitmq queue */
    RABBITMQ("default_rabbitmq_bridge"),
    ;

    public static BridgeType[] VALUES = values();

    private String defaultName;

    BridgeType(String defaultName) {
        this.defaultName = defaultName;
    }

    /**
     * 获取数据桥接类型名字获取{@link  BridgeType}枚举
     *
     * @param name 据桥接类型名字
     * @return {@link  BridgeType}枚举
     */
    public static BridgeType getByName(String name) {
        for (BridgeType bridgeType : VALUES) {
            if (bridgeType.name().equalsIgnoreCase(name)) {
                return bridgeType;
            }
        }

        throw new IllegalArgumentException(String.format("can not find bridge type for name '%s'", name));
    }

    public String getDefaultName() {
        return defaultName;
    }
}
