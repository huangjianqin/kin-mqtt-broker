package org.kin.mqtt.broker.core.cluster;

import org.kin.framework.utils.NetUtils;
import org.kin.framework.utils.StringUtils;
import org.kin.framework.utils.SysUtils;

import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * mqtt broker集群化配置
 *
 * @author huangjianqin
 * @date 2022/11/19
 */
public class ClusterConfig {
    /** 默认mqtt broker集群化配置 */
    public static final ClusterConfig DEFAULT = create();
    /** gossip port下标 */
    private static final int GOSSIP_PORT_IDX = 1;
    /** cluster store port下标 */
    private static final int STORE_PORT_IDX = 2;

    /** 暴露host */
    private String host = NetUtils.getLocalhostIp();
    /**
     * core节点配置(包含gossip集群seeds和jraft-rheakv参与选举的节点)
     * ','分割
     * 至少配置3个节点, 否则jraft-rheakv选举leader失败
     * 格式host:port:storePort
     */
    private String seeds;

    //---------------------------------------------------gossip
    /** gossip暴露端口 */
    private int port = 11000;
    //---------------------------------------------------cluster store
    /** cluster store rpc暴露端口 */
    private int storePort = 11100;
    /** cluster store用到的cpu processor数量 */
    private int storeProcessors = SysUtils.CPU_NUM;

    public static ClusterConfig create() {
        return new ClusterConfig();
    }

    protected ClusterConfig() {
    }

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
    public boolean isCore() {
        for (String hpp : seeds.split(",")) {
            String[] splits = hpp.split(":");
            if (splits[0].equals(host) &&
                    splits[STORE_PORT_IDX].equals(Integer.toString(storePort))) {
                return true;
            }
        }

        return false;
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
     *
     * @return cluster store address
     */
    public String getStoreAddress() {
        return host + ":" + storePort;
    }

    /**
     * 获取gossip集群seeds
     *
     * @return gossip集群seeds
     */
    public String getGossipSeeds() {
        return parseSeeds(GOSSIP_PORT_IDX);
    }

    /**
     * 获取raft kv store集群seeds
     *
     * @return raft kv store集群seeds
     */
    public String getStoreSeeds() {
        return parseSeeds(STORE_PORT_IDX);
    }

    /**
     * 解析seeds, 并取第{@code idx}个port作为正式的port
     *
     * @param idx 第n个port
     * @return 正常host:port数组且以,分割的字符串
     */
    private String parseSeeds(int idx) {
        return StringUtils.mkString(Stream.of(seeds.split(",")).map(hpp -> {
            String[] splits = hpp.split(":");
            return splits[0] + ":" + Integer.parseInt(splits[idx]);
        }).collect(Collectors.toList()));
    }

    //----------------------------------------------------------------------------------------------------------------
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
     * 格式host:port:storePort
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
