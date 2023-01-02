CREATE TABLE `kin_mqtt_acl`
(
    `id`        int(11) unsigned NOT NULL AUTO_INCREMENT COMMENT '主键',
    `allow`     int(1) DEFAULT 1 COMMENT '0: deny, 1: allow',
    `ip_addr`   varchar(60)           DEFAULT NULL COMMENT 'IpAddress',
    `username`  varchar(100)          DEFAULT NULL COMMENT 'Username',
    `client_id` varchar(100)          DEFAULT NULL COMMENT 'ClientId',
    `access`    int(2) NOT NULL COMMENT '1: publish, 2: subscribe, 3: pubsub',
    `topic`     varchar(100) NOT NULL DEFAULT '' COMMENT 'Topic Filter',
    PRIMARY KEY (`id`),
    INDEX (ipaddr),
    INDEX (username),
    INDEX (clientid)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

