package org.kin.mqtt.broker.auth.user;

import org.kin.mqtt.broker.auth.AuthService;
import org.springframework.util.CollectionUtils;
import reactor.core.publisher.Mono;

import java.util.*;

/**
 * 基于用户名和密码验证, 支持通过*匹配全部mqtt client
 *
 * @author huangjianqin
 * @date 2022/11/20
 */
public final class UserAuthService implements AuthService {
    /** key -> mqtt client id, value -> username & password */
    private Map<String, UserPasswordBytes> users = Collections.emptyMap();

    public UserAuthService(UserAuthProperties properties) {
        Map<String, UserPassword> users = properties.getUsers();
        if (!CollectionUtils.isEmpty(users)) {
            Map<String, UserPasswordBytes> tmp = new HashMap<>(users.size());
            for (Map.Entry<String, UserPassword> entry : users.entrySet()) {
                tmp.put(entry.getKey(), entry.getValue().toUserPasswordBytes());
            }
            this.users = Collections.unmodifiableMap(tmp);
        }
    }

    @Override
    public Mono<Boolean> auth(String userName, byte[] passwordBytes, String clientId) {
        return Mono.fromCallable(() -> {
            UserPasswordBytes userPasswordBytes = users.get(clientId);
            if (Objects.isNull(userPasswordBytes)) {
                //尝试通配符*匹配
                userPasswordBytes = users.get("root");
            }

            if (Objects.nonNull(userPasswordBytes)) {
                return userPasswordBytes.getUserName().equals(userName) &&
                        Arrays.equals(userPasswordBytes.getPasswordBytes(), passwordBytes);
            }

            return false;
        });
    }
}
