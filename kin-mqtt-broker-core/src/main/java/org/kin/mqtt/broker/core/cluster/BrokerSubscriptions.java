package org.kin.mqtt.broker.core.cluster;

import java.io.Serializable;
import java.util.Set;

/**
 * 集群订阅信息
 *
 * @author huangjianqin
 * @date 2023/5/20
 */
public class BrokerSubscriptions implements Serializable {
    private static final long serialVersionUID = -2120219997878449585L;

    /** 已注册订阅topic(正则表达式) */
    private Set<String> subRegexTopics;

    public BrokerSubscriptions() {
    }

    public BrokerSubscriptions(Set<String> subRegexTopics) {
        this.subRegexTopics = subRegexTopics;
    }

    //setter && getter
    public Set<String> getSubRegexTopics() {
        return subRegexTopics;
    }

    public void setSubRegexTopics(Set<String> subRegexTopics) {
        this.subRegexTopics = subRegexTopics;
    }
}
