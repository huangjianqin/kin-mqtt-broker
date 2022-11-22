package org.kin.mqtt.broker.metrics;

/**
 * 监控指标命名
 *
 * @author huangjianqin
 * @date 2022/11/22
 */
public interface MetricsNames {
    /** 计数器指标后缀 */
    String COUNT_SUFFIX = ".count";
    /** 数量上报指标后缀 */
    String NUM_SUFFIX = ".num";

    //--------------------------------------------------------------------------------------------------------
    /** broker接收到的publish消息数量 */
    String PUBLISH_MSG_COUNT = "mqtt.broker.publish" + COUNT_SUFFIX;
    /** broker接收到来自于其他broker的publish消息数量 */
    String CLUSTER_PUBLISH_MSG_COUNT = "mqtt.broker.cluster.publish" + COUNT_SUFFIX;


    //--------------------------------------------------------------------------------------------------------
    /** 连接broker的mqtt client数量 */
    String ONLINE_NUM = "mqtt.broker.online" + NUM_SUFFIX;
}
