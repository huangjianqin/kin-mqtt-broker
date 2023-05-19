package org.kin.mqtt.broker.core.cluster;

import java.util.Collections;
import java.util.Set;

/**
 * mqtt broker集群节点
 *
 * @author huangjianqin
 * @date 2022/11/15
 */
public class MqttBrokerNode {
    /** broker id */
    private String id;
    /** host */
    private String host;
    /** gossip暴露端口 */
    private int port;
    /** jraft-rheakv store rpc暴露端口 */
    private int storePort;
    /** 已注册订阅topic(正则表达式) */
    private volatile Set<String> subscribedRegexTopics = Collections.emptySet();

    public static MqttBrokerNode create(String id, MqttBrokerMetadata metadata){
        MqttBrokerNode node = new MqttBrokerNode();
        node.id = id;
        node.host = metadata.getHost();
        node.port = metadata.getPort();
        node.storePort = metadata.getStorePort();
        return node;
    }

    /**
     * 更新broker订阅信息
     */
    public void updateSubscriptions(Set<String> subscribedRegexTopics){
        if (subscribedRegexTopics == null) {
            subscribedRegexTopics = Collections.emptySet();
        }
        this.subscribedRegexTopics = subscribedRegexTopics;
    }

    /**
     * 该broker节点是否有订阅{@code topic}的mqtt client
     *
     * @param topic mqtt topic
     */
    public boolean hasSubscription(String topic) {
        for (String regexTopic : subscribedRegexTopics) {
            if (topic.matches(regexTopic)) {
                return true;
            }
        }
        return false;
    }

    //getter
    public String getId() {
        return id;
    }

    public String getHost() {
        return host;
    }

    public Integer getPort() {
        return port;
    }

    public int getStorePort() {
        return storePort;
    }

    @Override
    public String toString() {
        return "GossipNode{" +
                "name='" + id + '\'' +
                ", host='" + host + '\'' +
                ", port=" + port +
                ", storePort=" + storePort +
                ", subscribedRegexTopics=" + subscribedRegexTopics +
                '}';
    }
}
