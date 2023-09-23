package org.kin.mqtt.broker.bridge.rabbitmq;

/**
 * @author huangjianqin
 * @date 2023/9/23
 */
public final class RabbitMQBridgeConstants {
    /** host */
    public static final String HOST_KEY = "host";
    /** port */
    public static final String PORT_KEY = "port";
    /** user name */
    public static final String USER_NAME_KEY = "userName";
    /** password */
    public static final String PASSWORD_KEY = "password";
    /** 连接超时(毫秒) */
    public static final String CONNECTION_TIMEOUT_KEY = "connectionTimeout";
    /** 网络重连间隔(毫秒) */
    public static final String RECONNECT_INTERVAL_KEY = "reconnectInterval";
    /** rpc超时(毫秒) */
    public static final String RPC_TIMEOUT_KEY = "rpcTimeout";
    /** 连接池大小 */
    public static final String POOL_SIZE_KEY = "poolSize";

    //------------------------------------------------------------------------default
    /** 默认端口 */
    public static final int DEFAULT_PORT = 5672;
    /** 默认连接超时(毫秒) */
    public static final int DEFAULT_CONNECTION_TIMEOUT = 300_000;
    /** 默认网络重连间隔(毫秒) */
    public static final int DEFAULT_RECONNECT_INTERVAL = 5_000;
    /** 默认rpc超时(毫秒) */
    public static final int DEFAULT_RPC_TIMEOUT = 3_000;
    /** 默认连接池大小 */
    public static final int DEFAULT_POOL_SIZE = 5;


    private RabbitMQBridgeConstants() {
    }
}
