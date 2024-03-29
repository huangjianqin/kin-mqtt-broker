package org.kin.mqtt.broker.rule;

import org.kin.framework.reactor.utils.RetryNonSerializedEmitFailureHandler;
import org.kin.framework.utils.CollectionUtils;
import org.kin.framework.utils.JSON;
import org.kin.mqtt.broker.core.MqttBrokerContext;
import org.kin.mqtt.broker.core.message.MqttMessageReplica;
import org.kin.mqtt.broker.rule.action.Action;
import org.kin.mqtt.broker.rule.action.ActionConfiguration;
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
public class Rule implements Disposable {
    private static final Logger log = LoggerFactory.getLogger(Rule.class);
    private static final Function<RuleContext, Map<String, Object>> PAYLOAD_MAPPER = ctx -> {
        MqttMessageReplica message = ctx.getMessage();

        Map<String, Object> map = new HashMap<>(8);
        map.put(RuleCtxAttrNames.MQTT_CLIENT_ID, message.getClientId());
        map.put(RuleCtxAttrNames.MQTT_MSG_TOPIC, message.getTopic());
//        map.put(RuleChainAttrNames.MQTT_MSG_QOS, getQos());
//        map.put(RuleChainAttrNames.MQTT_MSG_RETAIN, isRetain());
        String str = new String(message.getPayload());
        if (str.startsWith("{")) {
            //json
            map.put(RuleCtxAttrNames.MQTT_MSG_PAYLOAD, JSON.readMap(str));
        } else {
            //普通字符串
            map.put(RuleCtxAttrNames.MQTT_MSG_PAYLOAD, str);
        }

        map.put(RuleCtxAttrNames.MQTT_MSG_TIMESTAMP, message.getRecTime());
        map.put(RuleCtxAttrNames.MQTT_MSG_PROPERTIES, message.getProperties());
        return map;
    };

    /**
     * mqtt broker context
     * lazy init
     */
    private volatile MqttBrokerContext brokerContext;
    /** 规则配置 */
    private final RuleConfiguration config;
    /** 匹配的mqtt topic */
    private final String regexTopic;
    /** 消费队列 */
    private final Sinks.Many<RuleContext> sink = Sinks.many().unicast().onBackpressureBuffer();
    /** 动作实现 */
    private final List<Action> actions;
    /** sql执行结果订阅disposable */
    private final Disposable disposable;

    public Rule(RuleConfiguration config) {
        this.config = config;

        String sql = config.getSql();
        ReactorSql reactorSql = ReactorSql.create(sql);
        String table = reactorSql.getTable();
        if (Objects.isNull(table)) {
            throw new IllegalStateException(String.format("can not find topic from sql, '%s'", sql));
        }
        this.regexTopic = TopicUtils.toRegexTopic(table);
        //创建action
        Set<ActionConfiguration> actionConfigs = config.getActions();
        List<Action> actions = new CopyOnWriteArrayList<>();
        for (ActionConfiguration actionConfiguration : actionConfigs) {
            actions.add(Actions.createAction(actionConfiguration));
        }
        this.actions = actions;
        //准备执行sql
        this.disposable = reactorSql
                .prepare()
                .apply(sink.asFlux(), PAYLOAD_MAPPER)
                .flatMap(result -> {
                    //sql逻辑执行完毕
                    RuleContext ruleContext = result.getRaw();
                    //执行结果
                    Map<String, Object> columns = result.all();
                    //保存到context
                    ruleContext.getAttrs().updateAttrs(columns);
                    return Flux.fromIterable(actions)
                            .flatMap(action -> {
                                try {
                                    return action.execute(ruleContext);
                                } catch (Exception e) {
                                    log.error("action start error, message='{}', action='{}'", columns, action, e);
                                    return Mono.empty();
                                }
                            });
                })
                //遇到异常仅仅打印log
                .onErrorContinue((t, o) -> log.error("", t))
                .subscribe();
    }

    /**
     * 添加动作
     *
     * @param actionConfig 规则配置
     */
    public void addAction(ActionConfiguration actionConfig) {
        config.addAction(actionConfig);
        actions.add(Actions.createAction(actionConfig));
    }

    /**
     * 移除动作
     *
     * @param actionConfig 动作配置
     */
    public boolean removeAction(ActionConfiguration actionConfig) {
        if (config.removeAction(actionConfig)) {
            actions.removeIf(action -> action.configuration().equals(actionConfig));
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
        return topic.matches(regexTopic);
    }

    /**
     * 执行规则
     *
     * @param brokerContext mqtt broker context
     * @param message       publish消息
     */
    public void execute(MqttBrokerContext brokerContext, MqttMessageReplica message) {
        if (Objects.isNull(this.brokerContext)) {
            //lazy init
            this.brokerContext = brokerContext;
        }

        if(CollectionUtils.isEmpty(actions)){
            //没有action
            return;
        }

        sink.emitNext(new RuleContext(brokerContext, message), RetryNonSerializedEmitFailureHandler.RETRY_NON_SERIALIZED);
    }

    @Override
    public void dispose() {
        disposable.dispose();
        sink.emitComplete(RetryNonSerializedEmitFailureHandler.RETRY_NON_SERIALIZED);
    }

    //getter
    public RuleConfiguration getConfig() {
        return config;
    }

    @Override
    public String toString() {
        return "Rule{" +
                "config=" + config +
                ", regexTopic='" + regexTopic + '\'' +
                ", actions=" + actions +
                '}';
    }
}
