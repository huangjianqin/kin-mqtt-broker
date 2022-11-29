# kin-mqtt-broker

简易单机版本mqtt broker实现

## 设计

### 嵌入式数据库h2

负责持久化broker数据, 包括规则

### 集群

* 基于gossip的节点发现和节点间数据消息同步
* 节点分为两种角色
  * core: 负责数据写入, 这些数据以brokerId来区分, 各自broker仅仅更新属于自己的数据, 如果接收到修改其他broker数据的请求,
    则路由到指定broker中完成更新操作. 同时, 内存中也会维护其余broker的数据, 用于管理台访问. 通过这样子设计保证core节点间
    的数据一致性
  * replicator: 仅仅从core节点拉取相关数据, 并维护在内存中

### 规则引擎

* 基于jexl3
* 支持多种数据桥接:
  * http
  * kafka
  * rabbitmq

### 系统Topic

默认不开启, 可以通过`MqttBrokerBootstrap.enableSysTopic()`配置开启. 有些topic是定时publish, 有些topic是数据发生变化才publish,
默认都是retain.
目前支持的系统topic name都定义在`TopicNames`. 具体有

* `$SYS$/broker/clients/total`: 当前broker已注册的client数, 在线+离线(持久化会话)

## 展望

* 考虑规则持久化和集群共享场景
* 持久化session信息
* 补充更多broker系统内置topic和内置event
* coap网关
* 自动订阅, 检查配置, connect时根据配置给指定client id自动注册订阅, 是否需要支持后台操作, 然后配置是否是持久化
* 共享订阅, 即通过topic前缀区分组, 然后组内负载均衡接受这一publish消息,而不是全部转发
* 延迟publish, 即开启调度任务延迟publish某些消息, 需考虑持久化
* 管理后台

## References

* MQTT: http://mqtt.org/
* MQTT协议中文: https://mcxiaoke.gitbooks.io/mqtt-cn/content/
* MQTT 5: https://docs.oasis-open.org/mqtt/mqtt/v5.0/mqtt-v5.0.html
* Eclipse Mosquitto: open source message broker that implements the MQTT protocol versions 3.1 and
  3.1.1 https://mosquitto.org/
* Eclipse Paho: open-source client implementations of MQTT and MQTT-SN messaging protocols https://www.eclipse.org/paho/
* Open-source IoT Platform: https://github.com/actorcloud/ActorCloud
* Open-source IoT Platform: https://github.com/emqx/emqx
* Open-source IoT Platform: https://github.com/mqttsnet/thinglinks
* eclipse-mosquitto所支持的系统主题: https://github.com/mqtt/mqtt.org/wiki/SYS-Topics