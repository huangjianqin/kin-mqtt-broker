package org.kin.mqtt.broker.utils;

/**
 * @author huangjianqin
 * @date 2022/11/21
 */
public final class TopicUtils {
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
}
