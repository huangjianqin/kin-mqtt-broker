package org.kin.mqtt.broker.auth;

import reactor.core.publisher.Mono;

/**
 * auth管理
 *
 * @author huangjianqin
 * @date 2022/11/15
 */
public interface AuthService {
    /**
     * mqtt client认证入口
     *
     * @param userName      用户名称
     * @param passwordBytes 密钥
     * @param clientId      mqtt client id
     * @return auth结果
     */
    Mono<Boolean> auth(String userName, byte[] passwordBytes, String clientId);
}
