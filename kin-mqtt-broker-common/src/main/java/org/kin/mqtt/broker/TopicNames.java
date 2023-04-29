package org.kin.mqtt.broker;

/**
 * @author huangjianqin
 * @date 2022/11/25
 */
public interface TopicNames {
    /** topic树root节点的topic name, 用于表示是root节点 */
    String TREE_ROOT_TOPIC = "$ROOT$";

    /** 共享topic前缀 */
    String SHARE_TOPIC = "$share";
    /** 延迟发布前缀 */
    String DELAY_TOPIC = "$delayed";

    //----------------------------------------------------系统topic------------------------------------------------------------------------------
    /** broker级别内置topic */
    String SYS_TOPIC = "$SYS$";
    /** broker级别内置topic, 当前broker在线client数 */
    String SYS_ONLINE_CLIENTS_TOTAL = SYS_TOPIC + "/broker/clients/online";

    /**
     * 判断指定topic是否是系统内置topic
     */
    static boolean isSysTopic(String topicName) {
        if (topicName == null || topicName.length() == 0) {
            return false;
        }
        String rootPath = topicName.split("/")[0];
        return TREE_ROOT_TOPIC.equals(rootPath) ||
                SYS_TOPIC.equals(rootPath);
    }

    /**
     * 判断指定topic是否是共享订阅topic
     */
    static boolean isShareTopic(String topicName) {
        if (topicName == null || topicName.length() == 0) {
            return false;
        }
        return topicName.indexOf(TopicNames.SHARE_TOPIC) == 0;
    }

}
