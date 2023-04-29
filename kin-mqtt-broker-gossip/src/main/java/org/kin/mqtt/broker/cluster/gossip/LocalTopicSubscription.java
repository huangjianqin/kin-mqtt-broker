package org.kin.mqtt.broker.cluster.gossip;

/**
 * 本broker节点的订阅信息
 *
 * @author huangjianqin
 * @date 2023/4/24
 */
public class LocalTopicSubscription {
    /** 转换成正则后的topic name */
    private final String regexTopic;
    /** 事务id生成器 */
    private long tid;
    /** 当前订阅数 */
    private long subscriptionNum;

    public LocalTopicSubscription(String regexTopic) {
        this.regexTopic = regexTopic;
    }

    /**
     * 注册订阅时触发
     *
     * @return 首次订阅返回tid, 否则返回-1
     */
    public long onSubscribe() {
        if (subscriptionNum++ == 0) {
            //首次订阅才需广播订阅变化, 故此时才变更tid
            tid++;
            return tid;
        }
        return -1;
    }

    /**
     * 取消订阅时触发
     *
     * @return 没有任何订阅返回tid, 否则返回-1
     */
    public long onUnsubscribe() {
        if (--subscriptionNum < 1) {
            //没有任何订阅才需广播订阅变化, 故此时才变更tid
            tid++;
            return tid;
        }
        return -1;
    }

    /**
     * 本broker节点, 是否有对该topic的订阅
     */
    public boolean isSubscribed() {
        return subscriptionNum > 0;
    }

    //getter
    public String getRegexTopic() {
        return regexTopic;
    }

    public long getTid() {
        return tid;
    }

    public long getSubscriptionNum() {
        return subscriptionNum;
    }
}
