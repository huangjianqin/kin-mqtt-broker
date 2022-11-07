package org.kin.mqtt.broker;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 全局mqtt session管理, 目前{@link MqttSession}是与{@link java.nio.channels.Channel}绑定的
 * 想做管理后台接口, 比如访问所有session或者特定session就比较麻烦
 * 这里提供一个入口访问已注册的mqtt session
 * @author huangjianqin
 * @date 2022/11/7
 */
public final class MqttSessionManager {
    /** 单例 */
    public static final MqttSessionManager INSTANCE = new MqttSessionManager();

    /** key -> session uuid, value -> {@link MqttSession} */
    private final Map<String, MqttSession> uuid2Session = new ConcurrentHashMap<>();

    /**
     * 注册mqtt session
     * @param session   mqtt session
     */
    public void register(MqttSession session){
        uuid2Session.put(session.getUuid(), session);
    }

    /**
     * 注销mqtt session
     * @param uuid  mqtt session uuid
     */
    public void unregister(String uuid){
        uuid2Session.remove(uuid);
    }

    /**
     * 根据uuid获取mqtt session
     * @param uuid  mqtt session uuid
     * @return  mqtt session
     */
    public MqttSession getByUuid(String uuid){
        return uuid2Session.get(uuid);
    }

    /**
     * @return  所有已注册的mqtt session
     */
    public Collection<MqttSession> all(){
        return uuid2Session.values();
    }
}
