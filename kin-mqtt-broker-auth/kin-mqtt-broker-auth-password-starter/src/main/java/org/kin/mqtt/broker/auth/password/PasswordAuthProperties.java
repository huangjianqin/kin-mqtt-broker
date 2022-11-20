package org.kin.mqtt.broker.auth.password;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.Map;

/**
 * @author huangjianqin
 * @date 2022/11/20
 */
@ConfigurationProperties("org.kin.mqtt.broker.auth")
public class PasswordAuthProperties {
    /** key -> mqtt client id, value -> username & password */
    private Map<String, UserPassword> users;

    //setter && getter
    public Map<String, UserPassword> getUsers() {
        return users;
    }

    public void setUsers(Map<String, UserPassword> users) {
        this.users = users;
    }
}
