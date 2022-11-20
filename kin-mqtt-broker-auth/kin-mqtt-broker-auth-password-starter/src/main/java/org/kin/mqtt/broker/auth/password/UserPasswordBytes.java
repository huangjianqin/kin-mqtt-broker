package org.kin.mqtt.broker.auth.password;

/**
 * @author huangjianqin
 * @date 2022/11/20
 */
public final class UserPasswordBytes {
    /** username */
    private final String userName;
    /** password bytes */
    private final byte[] passwordBytes;

    public UserPasswordBytes(String userName, byte[] passwordBytes) {
        this.userName = userName;
        this.passwordBytes = passwordBytes;
    }

    //setter && getter
    public String getUserName() {
        return userName;
    }

    public byte[] getPasswordBytes() {
        return passwordBytes;
    }
}
