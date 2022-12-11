package org.kin.mqtt.broker.rule.definition;

/**
 * topic转发规则定义
 *
 * @author huangjianqin
 * @date 2022/12/11
 * @see org.kin.mqtt.broker.rule.impl.TopicRule
 */
public final class TopicRuleDefinition extends RuleDefinition {
    /** 要转发的topic */
    private String topic;

    public static Builder builder() {
        return new Builder();
    }

    /** builder **/
    public static class Builder extends RuleDefinition.Builder<TopicRuleDefinition> {
        public Builder() {
            super(new TopicRuleDefinition());
        }

        public Builder topic(String topic) {
            definition.topic = topic;
            return this;
        }
    }

    //setter && getter
    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    @Override
    public String toString() {
        return "TopicRuleDefinition{" +
                super.toString() +
                "topic='" + topic + '\'' +
                "} ";
    }
}
