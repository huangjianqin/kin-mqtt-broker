package org.kin.mqtt.broker.auth.user;

import org.kin.mqtt.broker.Constants;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.Collections;
import java.util.Map;

/**
 * @author huangjianqin
 * @date 2022/11/20
 */
@ConfigurationProperties(Constants.AUTH_PROPERTIES_PREFIX)
public class UserAuthProperties {
    /** key -> user name, value -> password */
    private Map<String, String> users = Collections.emptyMap();

    //setter && getter
    public Map<String, String> getUsers() {
        return users;
    }

    public void setUsers(Map<String, String> users) {
        this.users = users;
    }
}
