package org.kin.mqtt.broker.boot;

import org.kin.mqtt.broker.Constants;
import org.kin.mqtt.broker.bridge.BridgeConfiguration;
import org.kin.mqtt.broker.core.MqttBrokerConfig;
import org.kin.mqtt.broker.core.cluster.ClusterConfig;
import org.kin.mqtt.broker.rule.RuleConfiguration;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.NestedConfigurationProperty;

import javax.annotation.PostConstruct;
import java.util.Collections;
import java.util.List;

/**
 * @author huangjianqin
 * @date 2022/11/19
 */
@ConfigurationProperties(Constants.COMMON_PROPERTIES_PREFIX)
public class MqttBrokerProperties extends MqttBrokerConfig {
    /** 默认mqtt broker集群化配置 */
    private static final ClusterConfig DEFAULT_CLUSTER_CONFIG = ClusterConfig.create();

    /**
     * mqtt broker集群化配置
     * 单独重新定义是为了兼容spring-boot-configuration-processor无法解析父类中static class和非基础类
     */
    private ClusterConfig cluster = DEFAULT_CLUSTER_CONFIG;
    /** 规则链配置 */
    @NestedConfigurationProperty
    private RuleConfiguration rule;
    /** 规则链配置 */
    private List<RuleConfiguration> rules = Collections.emptyList();
    /** 桥接配置 */
    @NestedConfigurationProperty
    private BridgeConfiguration bridge;
    /** 桥接配置 */
    private List<BridgeConfiguration> bridges = Collections.emptyList();

    @PostConstruct
    public void init() {
        setCluster(this.cluster);
    }

    //setter && getter
    @Override
    public ClusterConfig getCluster() {
        return cluster;
    }

    public void setCluster(ClusterConfig cluster) {
        this.cluster = cluster;
        super.setCluster(this.cluster);
    }

    public RuleConfiguration getRule() {
        return rule;
    }

    public void setRule(RuleConfiguration rule) {
        this.rule = rule;
    }

    public void setBridge(BridgeConfiguration bridge) {
        this.bridge = bridge;
    }

    public List<RuleConfiguration> getRules() {
        return rules;
    }

    public void setRules(List<RuleConfiguration> rules) {
        this.rules = rules;
    }

    public BridgeConfiguration getBridge() {
        return bridge;
    }

    public MqttBrokerProperties bridge(BridgeConfiguration bridge) {
        this.bridge = bridge;
        return this;
    }

    public List<BridgeConfiguration> getBridges() {
        return bridges;
    }

    public void setBridges(List<BridgeConfiguration> bridges) {
        this.bridges = bridges;
    }
}
