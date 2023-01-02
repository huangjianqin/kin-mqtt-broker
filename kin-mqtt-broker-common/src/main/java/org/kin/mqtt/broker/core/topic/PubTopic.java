package org.kin.mqtt.broker.core.topic;

/**
 * 解析publish消息的topic name
 *
 * @author huangjianqin
 * @date 2023/1/2
 */
public class PubTopic {
    /** topic name */
    private final String name;
    /** 延迟发布时间(秒) */
    private final int delay;

    public PubTopic(String name) {
        this(name, 0);
    }

    public PubTopic(String name, int delay) {
        this.name = name;
        this.delay = delay;
    }

    //getter
    public String getName() {
        return name;
    }

    public int getDelay() {
        return delay;
    }
}
