package org.kin.mqtt.broker.store.db;

import org.kin.mqtt.broker.Constants;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * @author huangjianqin
 * @date 2022/11/20
 */
@ConfigurationProperties(Constants.STORE_PROPERTIES_PREFIX + ".db")
public class DBMessageStoreProperties {
    /** db driver */
    private String driver;
    /** db host, 默认localhost */
    private String host = "localhost";
    /** db port */
    private int port;
    /** db user name */
    private String user;
    /** db password */
    private String password;
    /** database, 默认mqtt_message_store */
    private String database = "mqtt_message_store";

    //setter && getter
    public String getDriver() {
        return driver;
    }

    public void setDriver(String driver) {
        this.driver = driver;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getDatabase() {
        return database;
    }

    public void setDatabase(String database) {
        this.database = database;
    }
}
