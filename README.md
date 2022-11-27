# kin-mqtt-broker

简易单机版本mqtt broker实现

## 设计

* 支持自动发现集群节点, gossip, nacos, zk等等
* 集群间基于rpc交换mqtt消息, 收到publish消息往其他集群节点广播

## 展望

* 补充更多broker系统内置topic和内置event
* coap网关
* 考虑规则持久化和集群共享场景; 思考规则是否集群所有broker同步, 如果同步, 按目前实现会存在重复publish的bug
* 自动订阅, 检查配置, connect时根据配置给指定client id自动注册订阅, 是否需要支持后台操作, 然后配置是否是持久化
* 共享订阅, 即通过topic前缀区分组, 然后组内负载均衡接受这一publish消息,而不是全部转发
* 延迟publish, 即开启调度任务延迟publish某些消息, 需考虑持久化
* 管理后台

## 系统Topic

默认不开启, 可以通过`MqttBrokerBootstrap.enableSysTopic()`配置开启. 有些topic是定时publish, 有些topic是数据发生变化才publish,
默认都是retain.
目前支持的系统topic name都定义在`TopicNames`. 具体有

* `$SYS$/broker/clients/total`: 当前broker已注册的client数, 在线+离线(持久化会话)

## References

* MQTT: http://mqtt.org/
* MQTT协议中文: https://mcxiaoke.gitbooks.io/mqtt-cn/content/
* MQTT 5: hhttps://docs.oasis-open.org/mqtt/mqtt/v5.0/mqtt-v5.0.html
* Eclipse Mosquitto: open source message broker that implements the MQTT protocol versions 3.1 and
  3.1.1 https://mosquitto.org/
* Eclipse Paho: open-source client implementations of MQTT and MQTT-SN messaging protocols https://www.eclipse.org/paho/
* Open-source IoT Platform: https://github.com/actorcloud/ActorCloud
* Open-source IoT Platform: https://github.com/emqx/emqx
* Open-source IoT Platform: https://github.com/mqttsnet/thinglinks
* eclipse-mosquitto所支持的系统主题: https://github.com/mqtt/mqtt.org/wiki/SYS-Topics