package org.kin.mqtt.broker;

/**
 * @author huangjianqin
 * @date 2022/11/25
 */
public interface TopicNames {


    //----------------------------------------------------系统topic------------------------------------------------------------------------------
    /** 主题树root节点的topic name, 用于表示是root节点 */
    String TREE_ROOT_TOPIC = "$ROOT$";
    /** broker级别内置topic */
    String SYS_TOPIC = "$SYS$";

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
