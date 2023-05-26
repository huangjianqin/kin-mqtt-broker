package org.kin.mqtt.broker.boot;

import org.kin.framework.utils.JSON;
import org.kin.mqtt.broker.Constants;
import org.kin.mqtt.broker.bridge.Bridges;
import org.kin.mqtt.broker.core.MqttBrokerConfig;
import org.kin.mqtt.broker.core.cluster.ClusterConfig;
import org.kin.mqtt.broker.rule.action.Actions;
import org.springframework.boot.context.properties.ConfigurationProperties;

import javax.annotation.PostConstruct;
import java.util.*;
import java.util.stream.Collectors;

/**
 * @author huangjianqin
 * @date 2022/11/19
 */
@ConfigurationProperties(Constants.COMMON_PROPERTIES_PREFIX)
public class MqttBrokerProperties extends MqttBrokerConfig {
    /**
     * mqtt broker集群化配置
     * 单独重新定义是为了兼容spring-boot-configuration-processor无法解析父类中static class和非基础类
     */
    private Cluster cluster = Cluster.DEFAULT;
    /** 规则链定义 */
    private List<RuleDefinition> rules = Collections.emptyList();
    /** 桥接定义 */
    private List<BridgeDefinition> bridges = Collections.emptyList();

    @PostConstruct
    public void init() {
        setCluster(this.cluster);
    }

    //setter && getter
    @Override
    public Cluster getCluster() {
        return cluster;
    }

    public void setCluster(Cluster cluster) {
        this.cluster = cluster;
        super.setCluster(this.cluster);
    }

    public List<RuleDefinition> getRules() {
        return rules;
    }

    public void setRules(List<RuleDefinition> rules) {
        this.rules = rules;
    }

    public List<BridgeDefinition> getBridges() {
        return bridges;
    }

    public void setBridges(List<BridgeDefinition> bridges) {
        this.bridges = bridges;
    }

    //----------------------------------------------------------------------------------------------------------------
    public static class Cluster extends ClusterConfig {
        /** 默认mqtt broker集群化配置 */
        public static final Cluster DEFAULT = new Cluster();
    }

    public static class RuleDefinition {
        /** 规则名 */
        private String name;
        /** 规则描述 */
        private String desc;
        /** sql */
        private String sql;
        /** 绑定的动作 */
        private Set<ActionDefinition> actions;

        /** 转换成{@link  org.kin.mqtt.broker.rule.RuleDefinition}实例 */
        public org.kin.mqtt.broker.rule.RuleDefinition toRuleDefinition() {
            return org.kin.mqtt.broker.rule.RuleDefinition.builder()
                    .name(name)
                    .desc(desc)
                    .sql(sql)
                    .actionDefs(actions.stream().map(ActionDefinition::toActionDefinition).collect(Collectors.toList()))
                    .build();
        }

        //setter && getter
        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getDesc() {
            return desc;
        }

        public void setDesc(String desc) {
            this.desc = desc;
        }

        public String getSql() {
            return sql;
        }

        public void setSql(String sql) {
            this.sql = sql;
        }

        public Set<ActionDefinition> getActions() {
            return actions;
        }

        public void setActions(Set<ActionDefinition> actions) {
            this.actions = actions;
        }
    }

    public static class ActionDefinition {
        private String type;
        private Map<String, Object> args = new LinkedHashMap<>();

        /** 转换成{@link  org.kin.mqtt.broker.rule.action.ActionDefinition}实例 */
        public org.kin.mqtt.broker.rule.action.ActionDefinition toActionDefinition() {
            return JSON.convert(args, Actions.getDefinitionClassByName(type));
        }

        //setter && getter
        public String getType() {
            return type;
        }

        public void setType(String type) {
            this.type = type;
        }

        public Map<String, Object> getArgs() {
            return args;
        }

        public void setArgs(Map<String, Object> args) {
            this.args = args;
        }
    }

    public static class BridgeDefinition {
        private String type;
        private Map<String, Object> args = new LinkedHashMap<>();

        /** 转换成{@link  org.kin.mqtt.broker.bridge.definition.BridgeDefinition}实例 */
        public org.kin.mqtt.broker.bridge.definition.BridgeDefinition toBridgeDefinition() {
            return JSON.convert(args, Bridges.getDefinitionClassByName(type));
        }

        //setter && getter
        public String getType() {
            return type;
        }

        public void setType(String type) {
            this.type = type;
        }

        public Map<String, Object> getArgs() {
            return args;
        }

        public void setArgs(Map<String, Object> args) {
            this.args = args;
        }
    }
}
