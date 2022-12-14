package org.kin.mqtt.broker.rule.action;

/**
 * 动作定义
 * 注意, 子类必须实现{@link  #equals(Object)}和{@link #hashCode()}, 依赖这两个方法判断动作定义式是否一致, 用于动作移除或更新操作
 *
 * @author huangjianqin
 * @date 2022/12/16
 */
public interface ActionDefinition {

}
