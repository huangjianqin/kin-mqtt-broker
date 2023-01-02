package org.kin.mqtt.broker.acl;

/**
 * 资源类型
 *
 * @author huangjianqin
 * @date 2022/11/24
 */
public enum AclAction {
    /** 允许publish指定topic的消息 */
    PUBLISH(1),
    /** 允许subscribe指定topic */
    SUBSCRIBE(2),
    /** 允许publish指定topic的消息和订阅指定topic */
    PUBSUB(3),
    ;

    private final int flag;

    AclAction(int flag) {
        this.flag = flag;
    }

    public int getFlag() {
        return flag;
    }
}
