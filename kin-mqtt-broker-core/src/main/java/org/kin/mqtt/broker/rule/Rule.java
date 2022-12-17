package org.kin.mqtt.broker.rule;

import org.kin.framework.utils.JSON;
import org.kin.mqtt.broker.core.MqttBrokerContext;
import org.kin.mqtt.broker.core.message.MqttMessageReplica;
import org.kin.mqtt.broker.rule.action.Action;
import org.kin.mqtt.broker.rule.action.ActionDefinition;
import org.kin.mqtt.broker.rule.action.Actions;
import org.kin.mqtt.broker.utils.TopicUtils;
import org.kin.reactor.sql.ReactorSql;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Function;

/**
 * 规则
 *
 * @author huangjianqin
 * @date 2022/12/16
 */
public final class Rule implements Disposable {
    private static final Logger log = LoggerFactory.getLogger(Rule.class);
    private static final Function<RuleContext, Map<String, Object>> PAYLOAD_MAPPER = ctx -> {
        MqttMessageReplica message = ctx.getMessage();

        Map<String, Object> map = new HashMap<>(8);
        map.put(RuleChainAttrNames.MQTT_CLIENT_ID, message.getClientId());
        map.put(RuleChainAttrNames.MQTT_MSG_TOPIC, message.getTopic());
//        map.put(RuleChainAttrNames.MQTT_MSG_QOS, getQos());
//        map.put(RuleChainAttrNames.MQTT_MSG_RETAIN, isRetain());
        map.put(RuleChainAttrNames.MQTT_MSG_PAYLOAD, JSON.readMap(message.getPayload()));
        map.put(RuleChainAttrNames.MQTT_MSG_TIMESTAMP, message.getTimestamp());
        map.put(RuleChainAttrNames.MQTT_MSG_PROPERTIES, message.getProperties());
        return map;
    };

    /**
     * mqtt broker context
     * lazy init
     */
    private volatile MqttBrokerContext brokerContext;
    /** 规则定义 */
    private final RuleDefinition definition;
    /** 匹配的mqtt topic */
    private final String topicRegex;
    /** 消费队列 */
    private final Sinks.Many<RuleContext> sink = Sinks.many().multicast().onBackpressureBuffer();
    /** 动作实现 */
    private final List<Action> actions;
    /** sql执行结果订阅disposable */
    private final Disposable disposable;

    public Rule(RuleDefinition definition) {
        this.definition = definition;

        String sql = definition.getSql();
        ReactorSql reactorSql = ReactorSql.create(sql);
        String table = reactorSql.getTable();
        if (Objects.isNull(table)) {
            throw new IllegalStateException(String.format("can not find topic from sql, '%s'", sql));
        }
        this.topicRegex = TopicUtils.toRegexTopic(table);
        //创建action
        Set<ActionDefinition> actionDefs = definition.getActionDefs();
        List<Action> actions = new CopyOnWriteArrayList<>();
        for (ActionDefinition actionDefinition : actionDefs) {
            actions.add(Actions.createAction(actionDefinition));
        }
        this.actions = actions;
        //准备执行sql
        this.disposable = reactorSql
                .prepare()
                .apply(sink.asFlux(), PAYLOAD_MAPPER)
                .flatMap(result -> {
                    RuleContext ruleContext = result.getRaw();
                    Map<String, Object> columns = result.all();
                    ruleContext.getAttrs().updateAttrs(columns);
                    return Flux.fromIterable(actions)
                            .map(action -> {
                                try {
                                    return action.start(ruleContext);
                                } catch (Exception e) {
                                    log.error("action start error, message='{}', action='{}', {}", columns, action, e);
                                    return Mono.empty();
                                }
                            });
                })
                .subscribe();
    }

    /**
     * 添加动作
     *
     * @param actionDefinition 规则定义
     */
    public void addAction(ActionDefinition actionDefinition) {
        definition.addAction(actionDefinition);
        actions.add(Actions.createAction(actionDefinition));
    }

    /**
     * 移除动作
     *
     * @param actionDefinition 动作定义
     */
    public boolean removeAction(ActionDefinition actionDefinition) {
        if (definition.removeAction(actionDefinition)) {
            actions.removeIf(action -> action.definition().equals(actionDefinition));
            return true;
        }

        return false;
    }

    /**
     * 是否处理{@code topic}的publish消息
     *
     * @param topic mqtt topic
     */
    public boolean match(String topic) {
        return topic.matches(topicRegex);
    }

    /**
     * 规则执行
     *
     * @param brokerContext mqtt broker context
     * @param message       publish消息
     */
    public void execute(MqttBrokerContext brokerContext, MqttMessageReplica message) {
        if (Objects.isNull(this.brokerContext)) {
            //lazy init
            this.brokerContext = brokerContext;
        }

        sink.emitNext(new RuleContext(brokerContext, message), (signalType, emitResult) -> {
            throw new IllegalStateException(String.format("rule '%s' emit next message error, due to %s %s", definition.getName(), signalType, emitResult));
        });
    }

    @Override
    public void dispose() {
        disposable.dispose();
        sink.emitComplete((signalType, emitResult) -> {
            throw new IllegalStateException(String.format("rule '%s' dispose error, due to %s %s", definition.getName(), signalType, emitResult));
        });
    }

    //getter
    public RuleDefinition getDefinition() {
        return definition;
    }
}
