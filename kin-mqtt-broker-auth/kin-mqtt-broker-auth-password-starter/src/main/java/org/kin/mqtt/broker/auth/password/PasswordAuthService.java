package org.kin.mqtt.broker.auth.password;

import org.kin.mqtt.broker.auth.AuthService;
import reactor.core.publisher.Mono;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/**
 * 基于用户名和密码验证
 *
 * @author huangjianqin
 * @date 2022/11/20
 */
public final class PasswordAuthService implements AuthService {
    /** username */
    private final String userName;
    /** password bytes */
    private final byte[] passwordBytes;

    public PasswordAuthService(PasswordAuthProperties properties) {
        userName = properties.getUsername();
        passwordBytes = properties.getPassword().getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public Mono<Boolean> auth(String userName, byte[] passwordBytes, String clientId) {
        return Mono.just(this.userName.equals(userName) && Arrays.equals(this.passwordBytes, passwordBytes));
    }
}
