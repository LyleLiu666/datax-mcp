-- Synthetic values only. Never run this file against a production database.
CREATE DATABASE IF NOT EXISTS sync_fixture;
CREATE USER IF NOT EXISTS 'sync_reader'@'%' IDENTIFIED WITH mysql_native_password BY 'fixture-reader';
GRANT SELECT ON sync_fixture.* TO 'sync_reader'@'%';
USE sync_fixture;
CREATE TABLE IF NOT EXISTS source_items(id BIGINT PRIMARY KEY,body LONGBLOB,label TEXT,created_at DATETIME(6),amount DECIMAL(30,12));
INSERT INTO source_items VALUES
 (1,UNHEX('e4b8ade6968700f09f9880'),'', '2026-01-02 03:04:05.123456',123456789012345678.123456789012),
 (3,NULL,NULL,NULL,NULL),
 (9007199254740993,UNHEX(''), 'synthetic', '2026-01-02 03:04:05.000001',0.000000000001)
 ON DUPLICATE KEY UPDATE body=VALUES(body);
