package org.kin.mqtt.broker.auth.user;

import org.kin.mqtt.broker.auth.AuthService;
import org.springframework.util.CollectionUtils;
import reactor.core.publisher.Mono;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * 基于用户名和密码验证, 支持通过*匹配全部mqtt client
 *
 * @author huangjianqin
 * @date 2022/11/20
 */
public class UserAuthService implements AuthService {
    /** key -> user name, value -> password */
    private Map<String, String> users = Collections.emptyMap();

    public UserAuthService(UserAuthProperties properties) {
        Map<String, String> users = properties.getUsers();
        if (!CollectionUtils.isEmpty(users)) {
            this.users = Collections.unmodifiableMap(new HashMap<>(users));
        }
    }

    @Override
    public Mono<Boolean> auth(String userName, String password) {
        return Mono.fromCallable(() -> {
            String userPassword = users.get(userName);
            if (Objects.isNull(userPassword)) {
                //尝试通配符*匹配
                userPassword = users.get("root");
            }

            if (Objects.nonNull(userPassword)) {
                return userPassword.equals(password);
            }

            //找不到任何user, 则直接reject
            return false;
        });
    }
}
