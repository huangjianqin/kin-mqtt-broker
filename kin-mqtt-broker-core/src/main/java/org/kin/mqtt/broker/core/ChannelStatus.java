package org.kin.mqtt.broker.core;

/**
 * channel状态
 *
 * @author huangjianqin
 * @date 2022/11/14
 */
public enum ChannelStatus {
    /**
     * 初始化
     */
    INIT,

    /**
     * 在线
     */
    ONLINE,

    /**
     * 离线
     */
    OFFLINE,
}
