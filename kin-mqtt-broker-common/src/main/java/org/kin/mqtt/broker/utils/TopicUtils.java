package org.kin.mqtt.broker.utils;

import org.kin.mqtt.broker.TopicNames;
import org.kin.mqtt.broker.core.topic.PubTopic;

/**
 * @author huangjianqin
 * @date 2022/11/21
 */
public class TopicUtils {
    private TopicUtils() {
    }

    /**
     * 将topic的+/#转换成java内置的+/.正则匹配
     */
    public static String toRegexTopic(String topic) {
        if (topic.startsWith("$")) {
            topic = "\\" + topic;
        }
        return topic
                .replaceAll("/", "\\\\/")
                .replaceAll("\\+", "[^/]+")
                .replaceAll("#", "(.+)") + "$";
    }

    /**
     * 解析publish消息的topic name, 包括读取delay
     */
    public static PubTopic parsePubTopic(String topic) {
        if (topic.indexOf(TopicNames.DELAY_TOPIC) == 0) {
            String[] splits = topic.split("/", 3);
            return new PubTopic(splits[2], Integer.parseInt(splits[1]));
        } else {
            return new PubTopic(topic);
        }
    }
}
