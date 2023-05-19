package org.kin.mqtt.broker.core.cluster;

import org.kin.framework.utils.NetUtils;
import org.kin.framework.utils.StringUtils;
import org.kin.framework.utils.SysUtils;

/**
 * mqtt broker集群化配置
 * @author huangjianqin
 * @date 2022/11/19
 */
public class ClusterConfig {
    /** 默认mqtt broker集群化配置 */
    public static final ClusterConfig DEFAULT = create();

    /** 暴露host */
    private String host = NetUtils.getIp();
    /**
     * core节点配置(包含gossip集群seeds和jraft-rheakv参与选举的节点)
     * ','分割
     * 至少配置3个节点, 否则jraft-rheakv选举leader失败
     */
    private String seeds;

    //---------------------------------------------------gossip
    /** gossip暴露端口 */
    private int port = 11000;
    //---------------------------------------------------cluster store
    /** cluster store数据存储目录 */
    private String dataPath = "data";
    /** cluster store rpc暴露端口 */
    private int storePort = 11100;
    /** cluster store用到的cpu processor数量 */
    private int storeProcessors = SysUtils.CPU_NUM;

    /**
     * 判断是否开启集群模式
     * @return  true, 则是开启集群模式
     */
    public boolean isClusterMode(){
        return StringUtils.isNotBlank(seeds);
    }

    /**
     * 判断是否是core节点
     * @return  true, 则是core节点
     */
    public boolean isCore(){
        return seeds.contains(getStoreAddress());
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

    //----------------------------------------------------------------------------------------------------------------
    public static ClusterConfig create() {
        return new ClusterConfig();
    }

    /**
     * 暴露host
     */
    public ClusterConfig host(String host) {
        this.host = host;
        return this;
    }

    /**
     * core节点配置(包含gossip集群seeds和jraft-rheakv参与选举的节点)
     * ','分割
     * 至少配置3个节点, 否则jraft-rheakv选举leader失败
     */
    public ClusterConfig seeds(String seeds) {
        this.seeds = seeds;
        return this;
    }

    /**
     * gossip暴露端口
     */
    public ClusterConfig port(int port) {
        this.port = port;
        return this;
    }

    /**
     * cluster store数据存储目录
     */
    public ClusterConfig dataPath(String dataPath) {
        this.dataPath = dataPath;
        return this;
    }

    /**
     * cluster store rpc暴露端口
     */
    public ClusterConfig storePort(int storePort) {
        this.storePort = storePort;
        return this;
    }

    /**
     * cluster store用到的cpu processor数量
     */
    public ClusterConfig storeProcessors(int storeProcessors) {
        this.storeProcessors = storeProcessors;
        return this;
    }

    //setter && getter
    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public String getSeeds() {
        return seeds;
    }

    public void setSeeds(String seeds) {
        this.seeds = seeds;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public String getDataPath() {
        return dataPath;
    }

    public void setDataPath(String dataPath) {
        this.dataPath = dataPath;
    }

    public int getStorePort() {
        return storePort;
    }

    public void setStorePort(int storePort) {
        this.storePort = storePort;
    }

    public int getStoreProcessors() {
        return storeProcessors;
    }

    public void setStoreProcessors(int storeProcessors) {
        this.storeProcessors = storeProcessors;
    }
}
