CREATE TABLE `kin_mqtt_user`
(
    `id`          int(11) unsigned NOT NULL AUTO_INCREMENT COMMENT '主键',
    `username`    varchar(100) DEFAULT NULL COMMENT '用户名',
    `password`    varchar(100) DEFAULT NULL COMMENT '密码',
    `salt`        varchar(35)  DEFAULT NULL COMMENT '加盐',
    `create_time` datetime     DEFAULT NULL COMMENT '创建时间',
    PRIMARY KEY (`id`),
    UNIQUE KEY `kin_mqtt_username` (`username`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;