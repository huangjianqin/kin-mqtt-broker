package org.kin.mqtt.broker.auth.user;

import org.kin.mqtt.broker.Constants;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.Map;

/**
 * @author huangjianqin
 * @date 2022/11/20
 */
@ConfigurationProperties(Constants.AUTH_PROPERTIES_PREFIX)
public class UserAuthProperties {
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
