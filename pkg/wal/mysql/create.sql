CREATE DATABASE zincsearch CHARSET utf8;

USE zincsearch;

CREATE TABLE IF NOT EXISTS `wal`  (
`id` bigint AUTO_INCREMENT PRIMARY KEY NOT NULL,
`name` varchar(100) NOT NULL,
`index` bigint NOT NULL,
`data` longtext NOT NULL,
INDEX WAL(`name`,`index`)
) ENGINE=INNODB DEFAULT CHARSET = utf8-mb4;

CREATE TABLE IF NOT EXISTS `redo`(
`id` bigint AUTO_INCREMENT PRIMARY KEY NOT NULL,
`name` varchar(100) NOT NULL,
`index` bigint NOT NULL,
`data` text NOT NULL,
INDEX REDO(`name`)
)ENGINE=INNODB DEFAULT CHARSET = utf8-mb4;