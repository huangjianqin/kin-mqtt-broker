drop table if exists kin_mqtt_broker_offline;

create table kin_mqtt_broker_offline
(
    id          bigint AUTO_INCREMENT primary key,
    client_id   varchar(255) comment 'mqtt client id',
    topic       varchar(255) comment 'topic',
    qos         int comment 'mqtt message qos',
    retain      bit comment 'mqtt message retain',
    payload     text comment 'mqtt message payload',
    create_time bigint comment 'create_time',
    properties  tinytext comment 'properties'
);

create index i_topic on kin_mqtt_broker_offline (topic);


drop table if exists kin_mqtt_broker_retain;

create table kin_mqtt_broker_retain
(
    id          bigint AUTO_INCREMENT primary key,
    client_id   varchar(255) comment 'mqtt client id',
    topic       varchar(255) comment 'topic',
    qos         int comment 'mqtt message qos',
    retain      bit comment 'mqtt message retain',
    payload     text comment 'mqtt message payload',
    create_time bigint comment 'create_time',
    properties  tinytext comment 'properties'
);

create index i_client_id on kin_mqtt_broker_retain (client_id);