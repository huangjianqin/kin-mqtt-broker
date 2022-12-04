# kin-mqtt-broker

简易单机版本mqtt broker实现

## 设计

### 嵌入式数据库h2

负责持久化broker数据, 包括规则

### 集群

* 基于gossip实现节点发现和元数据同步
* 基于req-resp实现节点间通讯, 比如广播publish消息
* 节点分为两种角色
  * core: 负责数据写入和持久化, 这些数据以brokerId来区分, 各自broker仅仅更新属于自己的数据, 如果接收到修改其他broker数据的请求,
    则路由到指定broker中完成更新操作. 同时, 内存中也会维护其余broker的数据, 用于管理台访问. 通过这样子设计保证core节点间
    的数据一致性
  * replicator: 仅仅从core节点拉取相关数据, 并维护在内存中
* 集群部署时, core节点一般不承担mqtt消息流量, 仅仅担任管理台职责, 负责后台人员对core节点进行新增规则等操作;
  而replicator节点
  则主要承担mqtt消息流量, 负责与mqtt client连接. 哪怕所有core节点崩溃, 集群任然可以工作, 只是丢失了管理台功能.

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

### 数据存储

考虑:

* 要求各节点可访问, 保证最终一致性?
* 数据变化还要广播通知所有节点

存储的数据类型:

* 规则
* mqtt client session
* topic retain消息
* 各节点订阅信息, 可通过集群节点之间广播信息维护一致性?

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
* Eclipse Paho: open-source client implementations of MQTT and MQTT-SN messaging protocols https://www.eclipse.org/paho/
* Open-source IoT Platform: https://github.com/emqx/emqx