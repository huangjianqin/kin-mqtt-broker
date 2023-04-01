# kin-mqtt-broker

简易mqtt broker实现

* 支持集群广播mqtt消息
* 支持规则引擎
* 支持延迟发布
* 支持系统topic
* 支持retain和session离线消息存储

## 设计

### MQTT

完全支持mqtt3, 部分支持mqtt5, 包括

1. [✓]主题别名
2. [✓]会话延期
3. [✓]请求响应
4. [✓]共享订阅
5. [✓]流量控制
6. [✓]订阅标识符
7. [✓]订阅选项, 包括noLocal, retainHandling和retainAsPublish
8. [✓]消息过期间隔
9. [✘]客户标识符
10. [✘]Server redirection
11. [✘]增强认证

### 集群

* 基于gossip实现节点发现和元数据同步
* 基于send实现节点间通讯, 比如广播publish消息和内部事件

### 规则引擎

* 基于kin-reactor-sql
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

### 数据存储思考

需要支持集群节点共享访问

* 统一数据存储, 即外挂DB(集群), 用于存储所有配置, 规则, 还有业务数据. 绝对靠谱
* 类似nacos的AP方案, 但基于gossip实现AP, 集群节点分为core和replicator, core负责写, replicator负责读. core节点不支持动态水平扩展,
  通过gossip协议发现的节点,
  哪怕是配置成core, 也会被认为是replicator. core写逻辑可以参考nacos的AP方案, 写完成时广播 replicator同步,
  同时也有定时checksum的机制. 每份数据
  都自带版本号, 用于core节点停机自动水平扩展时, 判断本地没有任何数据, 则从其他节点全量拉取, 并取版本号最大且本节点负责的数据存储.
* 期待的框架: 读写分离, 几个节点负责维护写, 不承载mqtt流量; 其余节点负责读(还有本地缓存), 承载mqtt流量;

目前需要存储的数据类型:
* 规则
* mqtt client session
* topic retain消息

## 展望
* 持久化session信息, 需外部系统协助, 如redis
* 补充更多broker系统内置topic和内置event
* coap网关
* 自动订阅, 检查配置, connect时根据配置给指定client id自动注册订阅, 是否需要支持后台操作, 然后配置是否是持久化
* 管理后台

## References

* MQTT: http://mqtt.org/
* MQTT协议中文: https://mcxiaoke.gitbooks.io/mqtt-cn/content/
* MQTT 5: https://docs.oasis-open.org/mqtt/mqtt/v5.0/mqtt-v5.0.html
* Eclipse Paho: open-source client implementations of MQTT and MQTT-SN messaging protocols https://www.eclipse.org/paho/
* Open-source IoT Platform: https://github.com/emqx/emqx