package org.kin.mqtt.broker.core.cluster;

/**
 * @author huangjianqin
 * @date 2023/5/19
 */
public final class ClusterStoreKeys {
    /** mqtt broker规则引擎规则key */
    public static final String RULE_KEY_PREFIX = "kinMqttBroker:rule:";
    /** mqtt broker 持久化session key */
    public static final String SESSION_KEY_PREFIX = "kinMqttBroker:session:";
    /** mqtt broker订阅关系信息key */
    public static final String BROKER_TOPIC_SUBSCRIPTION_KEY_PREFIX = "kinMqttBroker:topicSubscription:";

    private ClusterStoreKeys() {
    }

    /**
     * 生成指定mqtt client session数据存储key
     * @param clientId  mqtt client id
     * @return  mqtt client session数据存储key
     */
    public static String getSessionKey(String clientId){
        return SESSION_KEY_PREFIX + clientId;
    }

    /**
     * 生成指定mqtt broker 订阅关系数据存储key
     * @param brokerId  mqtt broker id
     * @return  mqtt broker 订阅关系数据存储key
     */
    public static String getBrokerTopicSubscriptionKey(String brokerId){
        return BROKER_TOPIC_SUBSCRIPTION_KEY_PREFIX + brokerId;
    }
}