package org.kin.mqtt.broker.core.event;

/**
 * 在线mqtt client数量变化事件
 *
 * @author huangjianqin
 * @date 2023/4/29
 */
public class OnlineClientNumEvent implements InternalMqttEvent {
    /** 当前mqtt client在线数量 */
    private long onlineNum;

    public OnlineClientNumEvent() {
    }

    public OnlineClientNumEvent(long onlineNum) {
        this.onlineNum = onlineNum;
    }

    //setter && getter
    public long getOnlineNum() {
        return onlineNum;
    }

    public void setOnlineNum(long onlineNum) {
        this.onlineNum = onlineNum;
    }
}
