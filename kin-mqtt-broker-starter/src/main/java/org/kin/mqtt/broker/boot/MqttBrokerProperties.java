package org.kin.mqtt.broker.boot;

import org.kin.mqtt.broker.Constants;
import org.kin.mqtt.broker.bridge.BridgeConfiguration;
import org.kin.mqtt.broker.core.MqttBrokerConfig;
import org.kin.mqtt.broker.core.cluster.ClusterConfig;
import org.kin.mqtt.broker.rule.RuleDefinition;
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
    /** 规则链定义 */
    @NestedConfigurationProperty
    private RuleDefinition rule;
    /** 规则链定义 */
    private List<RuleDefinition> rules = Collections.emptyList();
    /** 桥接定义 */
    @NestedConfigurationProperty
    private BridgeConfiguration bridge;
    /** 桥接定义 */
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

    public RuleDefinition getRule() {
        return rule;
    }

    public void setRule(RuleDefinition rule) {
        this.rule = rule;
    }

    public void setBridge(BridgeConfiguration bridge) {
        this.bridge = bridge;
    }

    public List<RuleDefinition> getRules() {
        return rules;
    }

    public void setRules(List<RuleDefinition> rules) {
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
