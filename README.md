# kin-mqtt-broker
mqtt broker实现, 高性能, 易扩展, 高伸缩性

* 支持集群broker间广播mqtt消息
* 支持session持久化及集群共享(即HA)
* 支持规则引擎
* 支持延迟发布(未实现持久化, broker重启后会丢失)
* 支持系统topic
* 支持retain和offline消息存储

## 设计

### MQTT

基于reactor-netty实现mqtt协议

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
12. [✓]支持receiveMaximum

### 集群

* 基于gossip实现broker节点发现, broker间通讯, 元数据同步以及事件通知
* 基于jraft-rheakv实现broker间元数据和配置存储, 目前存储的数据有规则, mqtt session或者broker订阅

特性:

有点像无主复制和主从复制一种混合, 数据存储强一致性, 但replicator数据仅能保证最终一致性. 在该机制下, 不仅仅实现了集群数据共享,
还能保证集群节点高扩展性, 以及高伸缩性, 后期性能不足时, 可以直接增加replicator节点来提高性能,
同时, 鉴于replicator不负责写仅负责拉取数据的特性, 部署相对简单, 也方便devops

#### 实现细节

两个集群端口:

* 集群broker节点发现及通信端口, 即暴露gossip协议的端口
* 元数据和配置同步端口, 即jraft-rheakv暴露的端口, 用于实现嵌入式分布式kv存储

整体设计参考EMQX集群化设计

* core节点, 相当与raft group集群(jraft-rheakv), 用于保证broker间元数据和配置存储强一致性. 在raft中,
  broker节点角色可以为leader或follower
  * 可以选择不承担mqtt流量, 即mqtt端口不对外即可, 仅仅负责元数据和配置存储
  * 当相应配置变更时, 通过gossip广播所有broker. 其他broker访问jraft-rheakv获取最新的配置并apply. 因为基于gossip广播,
    所以broker集群存在一定时间的配置不一致性
  * 一般情况下, 不会动态增加core节点, core节点主要负责给replicator提供元数据和配置访问服务即可
* replicator节点
  * 主要承接mqtt流量, 处理mqtt业务
  * 在raft中, replicator节点角色为learner, 仅仅从leader拉取commit log. 通过read index方式访问集群元数据和配置

对于不同类型的数据, 有不一样的一致性语义:

* 对于会话, 保证强一致性, 仅mqtt client登录时需要获取session信息(read index), 其余场景都是异步持久化会话信息
* 对于规则等配置, 保证最终一致性, 底层基于gossip事件通知各broker, 有一定的滞后性
* 对于broker订阅, 为了减少对mqtt业务的影响, broker的订阅变化也是基于gossip事件通知各broker,
  各broker从jraft-rheakv访问最新的订阅数据并缓存

注意:

目前, 集群化部署的mqtt broker启动时需要保证raft端口暴露以及角色确定和gossip暴露后, 才会开始执行规则引擎初始化,
mqtt端口暴露等操作

#### 性能分析

* replicator节点设置为learner, 减少raft commit log一致性同步压力, 因为learner不参与投票, 只负责追数据
* 基于read index实现元数据和配置访问, 其性能取决于learner落后leader多少. 同时, 如果read index失败, 那么将直接请求leader获取数据
* 本质上仍然是主从, 可以将core节点理解为一个数据源, replicator仅负责从这个数据源同步数据, replicator间raft端口并不互通,
  仅仅与这个数据源的节点连接. leader数据同步压力很大, 因为learner也是从leader同步commit log的.
  同时, 元数据配置获取也至少需要一次与leader的rpc请求
* 多region, 分摊不同数据类型的log同步压力

#### 待优化

* 实现jraft-rheakv的watch机制, replicator主动监听配置变化. 当这些数据变化时, core主动推送给replicator,
  有效减少broker间配置不一致的时间

#### 思考

* 是否可以实现直接读取本地rocksdb数据, 不需要从core节点全量拉取数据, 一般用于core挂掉后, 快速恢复replicator节点.
  但基于本地配置进行初始化, 然后暴露mqtt端口, 如果配置长时间没有同步, mqtt业务就会长时间存在不一致

#### 前期集群元数据配置存储方案思考

* 统一数据存储, 即外挂DB(集群)或者zk, 用于存储所有配置, 规则, 还有业务数据. 绝对靠谱
* 类似nacos的AP方案, 但基于gossip实现AP, 集群节点分为core和replicator, core负责写, replicator负责读.
  core节点不支持动态水平扩展(取决于配置, 比如配置所有core节点的ip:port),
  通过gossip协议发现的节点, 哪怕是配置成core, 也会被认为是replicator. core写逻辑可以参考nacos的AP方案, 写完成时广播
  replicator同步,
  同时也有定时checksum的机制. 每份数据都自带版本号, 用于core节点停机自动水平扩展时, 判断本地没有任何数据, 则从其他节点全量拉取,
  并取版本号最大且本节点负责的数据存储.
  缺点是因网络波动会导致数据短暂不一致. 同时, 因为core节点只写, 故业务层要实现修改数据只能连接core节点,
  或者通过replicator节点转发.
* 基于multi-raft: broker自身也是raft节点, 数据一致性基于raft, 同时利用raft机制和copy-on-write维护数据缓存, 用于broker快速访问.
  不同类型的数据使用不同的raft分组, 分摊数据同步压力.
  同时, 因为缓存支持快速访问, 几乎不影响broker业务性能. 缺点是因raft同步会导致数据短暂不一致, 并且实现复杂度较高.
* 期待的框架: 读写分离, 几个节点负责维护写, 不承载mqtt流量; 其余节点负责读(还有本地缓存), 承载mqtt流量;

### 规则引擎

* 基于[kin-reactor-sql](https://github.com/huangjianqin/kin-reactor-sql)
* 支持多种数据桥接:
  * http
  * kafka
  * rabbitmq
  * 内部mqtt topic转发

### 系统Topic

默认不开启, 可以通过`MqttBrokerBootstrap.enableSysTopic()`配置开启. 有些topic是定时publish, 有些topic是数据发生变化才publish,
默认都是retain.
目前支持的系统topic name都定义在`TopicNames`. 具体有

* `$SYS$/broker/clients/total`: broker在线client数

### session持久化思考

一般适用于mqtt client因网络波动等原因, 短暂离线后与broker重连的场景. session持久化及集群共享可以减少重连过程耗时,
比如减少了mqtt client重新恢复之前session状态的耗时, 包括恢复订阅关系

注意, 此时mqtt client进程还在, 如果mqtt client进程挂了, 重新拉起, 该场景还是需要重新走一遍正常的流程. 还有mqtt
client有且仅能同时连接一个broker, 如果同时连接多个broker, 后续的链接会被broker强行断开

### 离线消息思考

* 阿里: 仅支持qos1, 客户端ack超时才判断为离线消息
* emqx: 基于规则引擎路由到指定topic, 订阅者消费后删除
* kin-mqtt-broker: 往指定client发送消息, 如果该client离线, 则持久化该消息, 待该client上线后, 重新发布

## 展望
* 延迟发布消息未实现持久化, broker重启后会丢失
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