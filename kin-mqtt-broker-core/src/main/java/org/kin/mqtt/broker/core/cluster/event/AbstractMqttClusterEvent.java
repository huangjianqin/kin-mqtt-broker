package org.kin.mqtt.broker.core.cluster.event;

/**
 * 带address的集群事件
 *
 * @author huangjianqin
 * @date 2022/12/21
 */
public abstract class AbstractMqttClusterEvent implements MqttClusterEvent {
    private static final long serialVersionUID = -5580537486030777665L;

    /** mqtt broker id */
    protected String id;
    /** lazy init, 由mqtt broker节点广播时才赋值 */
    protected String address;

    //setter && getter
    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
    }
}
