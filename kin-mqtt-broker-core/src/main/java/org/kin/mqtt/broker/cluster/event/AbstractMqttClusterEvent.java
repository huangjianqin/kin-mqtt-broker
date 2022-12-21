package org.kin.mqtt.broker.cluster.event;

/**
 * 带address的集群事件
 *
 * @author huangjianqin
 * @date 2022/12/21
 */
public abstract class AbstractMqttClusterEvent implements MqttClusterEvent {
    private static final long serialVersionUID = -5580537486030777665L;

    /** lazy init, 由mqtt broker节点receive时才赋值 */
    private String address;

    //setter && getter
    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
    }
}
