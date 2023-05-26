package org.kin.mqtt.broker.bridge.definition;

import org.kin.mqtt.broker.core.Type;

/**
 * rabbitMQ bridge配置定义
 *
 * @author huangjianqin
 * @date 2023/5/26
 */
@Type("rabbitMQ")
public class RabbitMQBridgeDefinition extends AbstractBridgeDefinition {
    private static final long serialVersionUID = -2880098627036254119L;
    /** host */
    private String host;
    /** port */
    private int port;
    /** user name */
    private String userName;
    /** password */
    private String password;
    /** 连接超时(毫秒) */
    private int connectionTimeout = 300_000;
    /** 网络重连间隔(毫秒) */
    private int reconnectInterval = 5_000;
    /** rpc超时(毫秒) */
    private int rpcTimeout = 3_000;
    /** 连接池大小 */
    private int poolSize = 5;

    public static Builder builder() {
        return new Builder();
    }


    /** builder **/
    public static class Builder extends AbstractBridgeDefinition.Builder<RabbitMQBridgeDefinition, Builder> {
        protected Builder() {
            super(new RabbitMQBridgeDefinition());
        }

        public Builder host(String host) {
            definition.host = host;
            return this;
        }

        public Builder port(int port) {
            definition.port = port;
            return this;
        }

        public Builder userName(String userName) {
            definition.userName = userName;
            return this;
        }

        public Builder password(String password) {
            definition.password = password;
            return this;
        }

        public Builder connectionTimeout(int connectionTimeout) {
            definition.connectionTimeout = connectionTimeout;
            return this;
        }

        public Builder reconnectInterval(int reconnectInterval) {
            definition.reconnectInterval = reconnectInterval;
            return this;
        }

        public Builder rpcTimeout(int rpcTimeout) {
            definition.rpcTimeout = rpcTimeout;
            return this;
        }

        public Builder poolSize(int poolSize) {
            definition.poolSize = poolSize;
            return this;
        }
    }

    //setter && getter
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

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public int getConnectionTimeout() {
        return connectionTimeout;
    }

    public void setConnectionTimeout(int connectionTimeout) {
        this.connectionTimeout = connectionTimeout;
    }

    public int getReconnectInterval() {
        return reconnectInterval;
    }

    public void setReconnectInterval(int reconnectInterval) {
        this.reconnectInterval = reconnectInterval;
    }

    public int getRpcTimeout() {
        return rpcTimeout;
    }

    public void setRpcTimeout(int rpcTimeout) {
        this.rpcTimeout = rpcTimeout;
    }

    public int getPoolSize() {
        return poolSize;
    }

    public void setPoolSize(int poolSize) {
        this.poolSize = poolSize;
    }

    @Override
    public String toString() {
        return "RabbitMQBridgeDefinition{" +
                super.toString() +
                ", host='" + port + '\'' +
                ", host='" + port + '\'' +
                ", userName='" + userName + '\'' +
                ", password='" + password + '\'' +
                ", connectionTimeout=" + connectionTimeout +
                ", reconnectInterval=" + reconnectInterval +
                ", rpcTimeout=" + rpcTimeout +
                ", poolSize=" + poolSize +
                '}';
    }
}
