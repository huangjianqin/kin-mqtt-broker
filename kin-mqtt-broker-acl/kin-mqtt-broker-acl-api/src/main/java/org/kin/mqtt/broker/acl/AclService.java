package org.kin.mqtt.broker.acl;

import reactor.core.publisher.Mono;

/**
 * 访问控制权限管理, 目前主要针对指定mqtt client是否有权限对指定topic进行publish或者subscribe操作
 * <p>
 * 实际逻辑应该包含如下:
 * 同时对host和mqtt client id设置权限, 检查顺序是client id > host
 * <p>
 * 普通topic:
 * 默认都放行
 * <p>
 * 系统topic('$SYS$/*'):
 * 默认不放行
 *
 * @author huangjianqin
 * @date 2022/11/24
 */
public interface AclService {
    /**
     * 是否有权限对{@code topicName}进行{@code action}操作
     *
     * @param host      mqtt clientId host
     * @param clientId  mqtt clientId id
     * @param userName  mqtt clientId user name
     * @param topicName topic name
     * @param action    操作类型
     * @return 检查结果
     */
    Mono<Boolean> checkPermission(String host, String clientId, String userName, String topicName, AclAction action);
}
