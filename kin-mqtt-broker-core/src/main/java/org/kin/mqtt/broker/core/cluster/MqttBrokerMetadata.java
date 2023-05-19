package org.kin.mqtt.broker.core.cluster;

import org.kin.framework.utils.NetUtils;

import java.io.Serializable;

/**
 * gossip节点绑定的元数据
 *
 * @author huangjianqin
 * @date 2023/5/18
 */
public class MqttBrokerMetadata implements Serializable {
    private static final long serialVersionUID = 3458874339382194930L;

    /** 暴露host */
    private String host = NetUtils.getIp();

    //---------------------------------------------------gossip
    /** gossip暴露端口 */
    private int port = 11000;
    //---------------------------------------------------cluster store
    /** cluster store rpc暴露端口 */
    private int storePort = 11100;
    /** 是否是core节点 */
    private boolean core;

    public static MqttBrokerMetadata create(Cluster mqttBrokerCluster) {
        ClusterConfig config = mqttBrokerCluster.getConfig();

        MqttBrokerMetadata metadata = new MqttBrokerMetadata();
        metadata.host = config.getHost();
        metadata.port = config.getPort();
        metadata.storePort = config.getStorePort();
        metadata.core = mqttBrokerCluster.isCore();
        return metadata;
    }

    /**
     * 获取gossip address
     * @return  gossip address
     */
    public String getAddress(){
        return host + ":" + port;
    }

    /**
     * 获取cluster store address
     * @return  cluster store address
     */
    public String getStoreAddress(){
        return host + ":" + storePort;
    }

    //setter && getter
    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public int getStorePort() {
        return storePort;
    }

    public void setStorePort(int storePort) {
        this.storePort = storePort;
    }

    public boolean isCore() {
        return core;
    }

    public void setCore(boolean core) {
        this.core = core;
    }
}
