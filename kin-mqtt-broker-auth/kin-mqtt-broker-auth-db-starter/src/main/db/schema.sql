CREATE TABLE `kin_mqtt_user`
(
    `id`          int(11) unsigned NOT NULL AUTO_INCREMENT,
    `username`    varchar(100) DEFAULT NULL,
    `password`    varchar(100) DEFAULT NULL,
    `salt`        varchar(35)  DEFAULT NULL,
    `create_time` datetime     DEFAULT NULL,
    PRIMARY KEY (`id`),
    UNIQUE KEY `kin_mqtt_username` (`username`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;