package org.kin.mqtt.broker.acl;

import reactor.core.publisher.Mono;

/**
 * 不进行访问控制权限检查
 *
 * @author huangjianqin
 * @date 2022/11/24
 */
public class NoneAclService implements AclService {
    /** 单例 */
    public static final NoneAclService INSTANCE = new NoneAclService();

    private NoneAclService() {
    }

    @Override
    public Mono<Boolean> checkPermission(String host, String client, String topicName, AclAction action) {
        return Mono.just(true);
    }

    @Override
    public Mono<Boolean> addPermission(String host, String client, String topicName, AclAction action) {
        throw new UnsupportedOperationException("NoneAclManager always allow any action");
    }

    @Override
    public Mono<Boolean> rmPermission(String host, String client, String topicName, AclAction action) {
        throw new UnsupportedOperationException("NoneAclManager always allow any ation");
    }
}
