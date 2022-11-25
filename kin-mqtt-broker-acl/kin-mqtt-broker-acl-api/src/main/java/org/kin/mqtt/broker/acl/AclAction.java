package org.kin.mqtt.broker.acl;

/**
 * 资源类型
 *
 * @author huangjianqin
 * @date 2022/11/24
 */
public enum AclAction {
    /** 允许publish指定topic的消息 */
    PUBLISH,
    /** 允许subscribe指定topic */
    SUBSCRIBE,
}
