package org.kin.mqtt.broker.store;

/**
 * @author huangjianqin
 * @date 2022/11/16
 */
public abstract class AbstractMessageStore implements MqttMessageStore {
    /**
     * 将topic的+/#转换成java内置的+/.正则匹配
     */
    protected String toRegexTopic(String topic) {
        if (topic.startsWith("$")) {
            topic = "\\" + topic;
        }
        return topic
                .replaceAll("/", "\\\\/")
                .replaceAll("\\+", "[^/]+")
                .replaceAll("#", "(.+)") + "$";
    }

    @Override
    public void dispose() {
        //默认do nothing
    }
}
