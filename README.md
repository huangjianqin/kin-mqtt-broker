# kin-mqtt-broker

简易单机版本mqtt broker实现

## 设计

* 支持自动发现集群节点, gossip, nacos, zk等等
* 集群间基于rpc交换mqtt消息, 收到publish消息往其他集群节点广播

## 展望

* 规则引擎支持,jexl
* 数据桥接
* acl
* 监控
* 主题重写, 即将某主题的消息路由到指定主题, 通过规则实现
* mqtt over websocket
* coap网关
* 系统主题, 即broker自身事件, client上下线事件==, publish到指定主题去, client可以订阅这些消息
* 自动订阅, 检查配置, connect时根据配置给指定client id自动注册订阅, 是否需要支持后台操作, 然后配置是否是持久化
* 共享订阅, 即通过topic前缀区分组, 然后组内负载均衡接受这一publish消息,而不是全部转发
* 延迟publish, 即开启调度任务延迟publish某些消息, 需考虑持久化
* 管理后台

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