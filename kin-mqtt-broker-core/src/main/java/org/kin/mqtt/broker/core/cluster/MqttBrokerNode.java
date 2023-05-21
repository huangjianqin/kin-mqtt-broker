package org.kin.mqtt.broker.core.cluster;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Set;

/**
 * mqtt broker集群节点
 *
 * @author huangjianqin
 * @date 2022/11/15
 */
public class MqttBrokerNode {
    private static final Logger log = LoggerFactory.getLogger(MqttBrokerNode.class);
    /** broker id */
    private String id;
    /** host */
    private String host;
    /** gossip暴露端口 */
    private int port;
    /** cluster store rpc暴露端口 */
    private int storePort;
    /** 是否是core节点 */
    private boolean core;
    /** 已注册订阅topic(正则表达式) */
    private volatile Set<String> subRegexTopics = Collections.emptySet();

    public static MqttBrokerNode create(String id, MqttBrokerMetadata metadata) {
        MqttBrokerNode node = new MqttBrokerNode();
        node.id = id;
        node.host = metadata.getHost();
        node.port = metadata.getPort();
        node.storePort = metadata.getStorePort();
        node.core = metadata.isCore();
        return node;
    }

    /**
     * 更新broker订阅信息
     */
    public void updateSubscriptions(Set<String> subRegexTopics) {
        log.debug("mqtt broker '{}' subscription changed, {}", id, subRegexTopics);
        if (subRegexTopics == null) {
            subRegexTopics = Collections.emptySet();
        }
        this.subRegexTopics = subRegexTopics;
    }

    /**
     * 该broker节点是否有订阅{@code topic}的mqtt client
     *
     * @param topic mqtt topic
     */
    public boolean hasSubscription(String topic) {
        for (String regexTopic : subRegexTopics) {
            if (topic.matches(regexTopic)) {
                return true;
            }
        }
        return false;
    }

    /**
     * 获取gossip address
     *
     * @return gossip address
     */
    public String getAddress() {
        return host + ":" + port;
    }

    /**
     * 获取cluster store address
     *
     * @return cluster store address
     */
    public String getStoreAddress() {
        return host + ":" + storePort;
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

    public boolean isCore() {
        return core;
    }

    @Override
    public String toString() {
        return "MqttBrokerNode{" +
                "name='" + id + '\'' +
                ", host='" + host + '\'' +
                ", port=" + port +
                ", storePort=" + storePort +
                ", core=" + core +
                ", subRegexTopics=" + subRegexTopics +
                '}';
    }
}
