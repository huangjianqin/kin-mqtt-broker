package org.kin.mqtt.broker.auth.password;

import java.nio.charset.StandardCharsets;

/**
 * @author huangjianqin
 * @date 2022/11/20
 */
public final class UserPassword {
    /** username */
    private String username;
    /** password */
    private String password;

    public UserPasswordBytes toUserPasswordBytes() {
        return new UserPasswordBytes(username, password.getBytes(StandardCharsets.UTF_8));
    }

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
