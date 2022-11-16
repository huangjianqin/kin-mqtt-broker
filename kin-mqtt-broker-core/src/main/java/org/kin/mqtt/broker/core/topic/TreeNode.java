package org.kin.mqtt.broker.core.topic;

import org.jctools.maps.NonBlockingHashMap;
import org.kin.framework.utils.CollectionUtils;

import java.util.*;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.stream.Collectors;

/**
 * 将topic按'/'拆成一个一个节点, 组成都多叉树结构
 *
 * @author huangjianqin
 * @date 2022/11/13
 * @see TreeTopicFilter
 */
final class TreeNode {
    /** topic */
    private final String topic;
    /** 已注册的订阅 */
    private final Set<TopicSubscription> subscriptions = new CopyOnWriteArraySet<>();
    /** 子节点 */
    private final Map<String, TreeNode> childNodes = new NonBlockingHashMap<>();

    TreeNode(String topic) {
        this.topic = topic;
    }

    /**
     * 注册订阅
     *
     * @param subscription 订阅信息
     * @return 是否注册成功
     */
    boolean addSubscription(TopicSubscription subscription) {
        String[] topics = subscription.getTopic().split(TopicFilter.SEPARATOR);
        return insert(subscription, topics, 0);
    }

    /**
     * 建立订阅topic path数节点
     *
     * @param subscription 订阅信息
     * @param topics       订阅topic paths
     * @param index        当前订阅topic path index
     * @return 是否注册订阅成功
     */
    private boolean insert(TopicSubscription subscription, String[] topics, Integer index) {
        String lastTopic = topics[index];
        TreeNode treeNode = childNodes.computeIfAbsent(lastTopic, tp -> new TreeNode(lastTopic));
        if (index == topics.length - 1) {
            return subscriptions.add(subscription);
        } else {
            return treeNode.insert(subscription, topics, index + 1);
        }
    }

    /**
     * 根据publish消息topic寻找所有匹配的叶子节点, 然后返回订阅信息
     *
     * @param topic publish消息topic name
     * @return 订阅信息
     */
    List<TopicSubscription> getSubscriptions(String topic) {
        String[] topics = topic.split(TopicFilter.SEPARATOR);
        LinkedList<TopicSubscription> matchedSubscriptions = new LinkedList<>();
        searchTree(this, matchedSubscriptions, topics, 0);
        return matchedSubscriptions;
    }

    /**
     * 遍历节点, 寻找匹配的topic subscriber
     *
     * @param curNode              当前节点
     * @param matchedSubscriptions 返回的订阅信息
     * @param topics               publish消息topic path
     * @param index                当前publish消息topic path index
     */
    private void searchTree(TreeNode curNode, LinkedList<TopicSubscription> matchedSubscriptions, String[] topics, Integer index) {
        String lastTopic = topics[index];
        TreeNode moreTreeNode = curNode.getChild(TopicFilter.MORE_LV_SYMBOL);
        if (moreTreeNode != null) {
            //#通配符匹配
            matchedSubscriptions.addAll(moreTreeNode.getSubscriptions());
        }
        if (index == topics.length - 1) {
            TreeNode childNode = curNode.getChild(lastTopic);
            //path name匹配
            if (childNode != null) {
                Set<TopicSubscription> childSubscriptions = childNode.getSubscriptions();
                if (CollectionUtils.isNonEmpty(childSubscriptions)) {
                    matchedSubscriptions.addAll(childSubscriptions);
                }
            }
            //+通配符匹配
            childNode = curNode.getChild(TopicFilter.SINGLE_LV_SYMBOL);
            if (childNode != null) {
                Set<TopicSubscription> childSubscriptions = childNode.getSubscriptions();
                if (CollectionUtils.isNonEmpty(childSubscriptions)) {
                    matchedSubscriptions.addAll(childSubscriptions);
                }
            }

        } else {
            TreeNode singleLvTreeNode = curNode.getChild(TopicFilter.SINGLE_LV_SYMBOL);
            if (singleLvTreeNode != null) {
                //子节点包含+, 则继续遍历
                searchTree(singleLvTreeNode, matchedSubscriptions, topics, index + 1);
            }
            TreeNode node = curNode.getChild(lastTopic);
            if (node != null) {
                //子节点包含指定topic path, 则继续遍历
                searchTree(node, matchedSubscriptions, topics, index + 1);
            }
        }
    }

    /**
     * 获取子节点
     *
     * @param topic topic name
     * @return 子节点
     */
    private TreeNode getChild(String topic) {
        return childNodes.get(topic);
    }

    /**
     * 移除订阅
     *
     * @param subscribeTopic 订阅信息
     * @return 是否移除成功
     */
    boolean removeSubscription(TopicSubscription subscribeTopic) {
        TreeNode node = this;
        //node链
        String[] topics = subscribeTopic.getTopic().split(TopicFilter.SEPARATOR);
        List<TreeNode> nodes = new ArrayList<>(topics.length);
        nodes.add(node);
        for (String topic : topics) {
            if (node != null) {
                node = node.getChild(topic);
                nodes.add(node);
            }
        }
        if (node != null) {
            Set<TopicSubscription> subscribeTopics = node.getSubscriptions();
            if (CollectionUtils.isNonEmpty(subscribeTopics) && subscribeTopics.remove(subscribeTopic)) {
                //移除成功
                //尝试清理无用树节点
                for (int i = nodes.size() - 2; i >= 0; i--) {
                    TreeNode curNode = nodes.get(i);
                    TreeNode childNode = nodes.get(i + 1);
                    if (childNode.isValid()) {
                        break;
                    }

                    curNode.removeChild(childNode.getTopic());
                }
                return true;
            }
        }
        return false;
    }

    /**
     * @return 是否有效
     */
    private boolean isValid() {
        return CollectionUtils.isNonEmpty(childNodes) && CollectionUtils.isNonEmpty(subscriptions);
    }

    /**
     * 移除子节点
     *
     * @param topic 子节点topic path
     */
    private void removeChild(String topic) {
        childNodes.remove(topic);
    }

    /**
     * 获取所有订阅信息
     *
     * @return 所有订阅信息
     */
    Set<TopicSubscription> getAllSubscriptions() {
        return getSubscriptions(this);
    }

    /**
     * 获取指定树子节点下的所有订阅信息
     *
     * @param node 树节点
     * @return 指定树子节点下的所有订阅信息
     */
    private Set<TopicSubscription> getSubscriptions(TreeNode node) {
        Set<TopicSubscription> matchedSubscriptions = new HashSet<>();
        matchedSubscriptions.addAll(node.getSubscriptions());
        matchedSubscriptions.addAll(node.getChildNodes()
                .values()
                .stream()
                .flatMap(treeNode -> treeNode.getSubscriptions(treeNode).stream())
                .collect(Collectors.toSet()));
        return matchedSubscriptions;
    }

    //getter
    String getTopic() {
        return topic;
    }

    Set<TopicSubscription> getSubscriptions() {
        return subscriptions;
    }

    Map<String, TreeNode> getChildNodes() {
        return childNodes;
    }
}
