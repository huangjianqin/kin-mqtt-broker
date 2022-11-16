package org.kin.mqtt.broker.core.auth;

import reactor.core.publisher.Mono;

/**
 * 不进行校验
 *
 * @author huangjianqin
 * @date 2022/11/15
 */
public final class NoneAuthService implements AuthService {
    /** 单例 */
    public static final AuthService INSTANCE = new NoneAuthService();

    private NoneAuthService() {
    }

    @Override
    public Mono<Boolean> auth(String userName, byte[] passwordBytes, String clientId) {
        return Mono.just(true);
    }
}
