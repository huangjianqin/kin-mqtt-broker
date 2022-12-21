package org.kin.mqtt.broker.cluster;

/**
 * mqtt broker集群节点接口
 *
 * @author huangjianqin
 * @date 2022/11/15
 */
public interface MqttBrokerNode {
    /**
     * 集群mqtt broker name
     *
     * @return 集群mqtt broker name
     */
    String getName();

    /**
     * 集群mqtt broker host
     *
     * @return 集群mqtt broker host
     */
    String getHost();

    /**
     * 集群mqtt broker port
     *
     * @return 集群mqtt broker port
     */
    Integer getPort();

    /**
     * 集群mqtt broker namespace
     *
     * @return 集群mqtt broker namespace
     */
    String getNamespace();
}
