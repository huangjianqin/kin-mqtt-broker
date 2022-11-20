package org.kin.mqtt.broker.auth.password;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * @author huangjianqin
 * @date 2022/11/20
 */
@ConfigurationProperties("org.kin.mqtt.broker.auth")
public class PasswordAuthProperties {
    /** username */
    private String username;
    /** password */
    private String password;

    //setter && getter
    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }
}
