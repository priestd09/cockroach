drop database if exists d;
CREATE DATABASE d;
CREATE TABLE t (g GEOGRAPHY, index (g));
INSERT INTO t VALUES ('{"type":"Point","coordinates":[1, 2]}');

select * from t;
---EXPLAIN SELECT * FROM t WHERE ST_DISTANCE(g, '{"type":"Point","coordinates":[1, 2]}') < 5;
SELECT * FROM t WHERE ST_DISTANCE(g, '{"type":"Point","coordinates":[1, 2]}') < 2;
