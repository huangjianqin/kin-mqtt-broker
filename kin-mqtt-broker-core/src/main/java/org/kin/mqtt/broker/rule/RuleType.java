package org.kin.mqtt.broker.rule;

/**
 * @author huangjianqin
 * @date 2022/11/21
 */
public enum RuleType {
    /** 条件判断, 结果只能返回boolean */
    PREDICATE,
    /**
     * 脚本执行, 结果可以返回任何值, 存储在{@link RuleChainContext}中
     * 目的是想给转发(比如kafka, web之类)的消息体增加额外的字段和数值
     */
    SCRIPT,
    /**
     * topic转发
     * 有些设备不能随意或者不能更新, 那么就需要对原有的topic将进行转译
     */
    TOPIC,
    /** http post转发 */
    HTTP,
    /** kafka 转发 */
    KAFKA,
    /** rabbitMQ 转发 */
    RABBITMQ,
}
