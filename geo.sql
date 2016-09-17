create database if not exists d;
set database = d;
drop table if exists t;
create table t (k geography, index(k));
insert into t values (ST_GeogFromGeoJSON('{"type":"Point","coordinates":[-73.97536754608154, 40.751255880455915]}'));

explain select * from t WHERE ST_DISTANCE(k, ST_GeogFromGeoJSON('{"type":"Point","coordinates":[-73.97536754608154, 40.751255880455915]}')) < .0000001;


