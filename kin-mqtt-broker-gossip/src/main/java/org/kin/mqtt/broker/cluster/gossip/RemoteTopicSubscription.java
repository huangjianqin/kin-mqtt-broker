package org.kin.mqtt.broker.cluster.gossip;

import java.io.Serializable;

/**
 * remote broker节点的订阅信息
 *
 * @author huangjianqin
 * @date 2023/4/24
 */
public class RemoteTopicSubscription implements Serializable {
    private static final long serialVersionUID = -2975162165749998741L;

    /** 转换成正则后的topic name */
    private String regexTopic;
    /** 事务id */
    private long tid;
    /** 是否订阅中 */
    private boolean subscribed;

    public RemoteTopicSubscription() {
    }

    public RemoteTopicSubscription(String regexTopic) {
        this.regexTopic = regexTopic;
    }

    /**
     * 合并增量更新
     *
     * @param tid        事务id
     * @param subscribed 该节点是否订阅中
     * @return 更新则返回true
     */
    public boolean merge(long tid, boolean subscribed) {
        if (this.tid >= tid) {
            return false;
        }

        this.tid = tid;
        this.subscribed = subscribed;
        return true;
    }

    //setter && getter
    public String getRegexTopic() {
        return regexTopic;
    }

    public void setRegexTopic(String regexTopic) {
        this.regexTopic = regexTopic;
    }

    public RemoteTopicSubscription regexTopic(String regexTopic) {
        this.regexTopic = regexTopic;
        return this;
    }

    public long getTid() {
        return tid;
    }

    public void setTid(long tid) {
        this.tid = tid;
    }

    public RemoteTopicSubscription tid(long tid) {
        this.tid = tid;
        return this;
    }

    public boolean isSubscribed() {
        return subscribed;
    }

    public void setSubscribed(boolean subscribed) {
        this.subscribed = subscribed;
    }

    public RemoteTopicSubscription subscribed(boolean subscribed) {
        this.subscribed = subscribed;
        return this;
    }

    @Override
    public String toString() {
        return "RemoteTopicSubscription{" +
                "regexTopic='" + regexTopic + '\'' +
                ", tid=" + tid +
                ", subscribed=" + subscribed +
                '}';
    }
}
