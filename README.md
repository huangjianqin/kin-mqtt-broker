# kin-mqtt-broker

简易单机版本mqtt broker实现

## 设计

### 嵌入式数据库h2

负责持久化broker数据, 包括规则

### 集群

* 基于gossip实现节点发现和元数据同步
* 基于req-resp实现节点间通讯, 比如广播publish消息**

### 规则引擎

* 基于reactor-sql
* 支持多种数据桥接:
  * http
  * kafka
  * rabbitmq
  * 内部mqtt topic转发

### 系统Topic

默认不开启, 可以通过`MqttBrokerBootstrap.enableSysTopic()`配置开启. 有些topic是定时publish, 有些topic是数据发生变化才publish,
默认都是retain.
目前支持的系统topic name都定义在`TopicNames`. 具体有

* `$SYS$/broker/clients/total`: 当前broker已注册的client数, 在线+离线(持久化会话)

### 数据存储

考虑:

* 要求各节点可访问, 保证最终一致性?
* 数据变化还要广播通知所有节点
* 目前想法
  * 统一数据存储, 即外挂DB(集群), 用于存储所有配置, 规则, 还有业务数据
  * JRaft RheaKV
    * 好处是不需要依赖任何库, 同时可以使用soft-rpc进行集群间通信
    * 坏处, 不能水平扩展; 额外性能开销(raft不断追log, 节点状态维护==); 读写性能; 配置key切片问题, 还要考虑每个节点数据量负载均衡;
  * nacos的AP方案, 得自己实现. 优点, AP+节点分区写, 缺点, 不支持水平扩展
  * 基于gossip实现AP, 集群节点分为core和replicator, core负责写, replicator负责读. core节点不支持动态水平扩展,
    通过gossip协议发现的节点,
    哪怕是配置成core, 也会被认为是replicator. core写逻辑可以参考nacos的AP方案, 写完成时广播 replicator同步,
    同时也有定时checksum的机制. 每份数据
    都自带版本号, 用于core节点停机自动水平扩展时, 判断本地没有任何数据, 则从其他节点全量拉取, 并去版本号最大且本节点负责的数据存储.
  * 期待的框架: 读写分离, 几个节点负责维护写, 不承载mqtt流量; 其余节点负责读(还有本地缓存), 承载mqtt流量;

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