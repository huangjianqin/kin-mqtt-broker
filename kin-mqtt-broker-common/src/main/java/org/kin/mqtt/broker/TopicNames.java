package org.kin.mqtt.broker;

/**
 * @author huangjianqin
 * @date 2022/11/25
 */
public interface TopicNames {
    /** 主题树root节点的topic name, 用于表示是root节点 */
    String TREE_ROOT_TOPIC = "$ROOT$";

    /** 共享主题前缀 */
    String SHARE_TOPIC = "$share";

    //----------------------------------------------------系统topic------------------------------------------------------------------------------
    /** broker级别内置topic */
    String SYS_TOPIC = "$SYS$";
    /** broker级别内置topic, 当前broker已注册的client数, 在线+离线(持久化会话) */
    String SYS_TOPIC_CLIENTS_TOTAL = SYS_TOPIC + "/broker/clients/total";

    /**
     * @return 指定主题是否是系统内置主题
     */
    static boolean isSysTopic(String topicName) {
        if (topicName == null || topicName.length() == 0) {
            return false;
        }
        String rootPath = topicName.split("/")[0];
        return TREE_ROOT_TOPIC.equals(rootPath) ||
                SYS_TOPIC.equals(rootPath);
    }

}
