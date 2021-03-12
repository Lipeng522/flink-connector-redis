# flink-connector-redis
flink-connector-redis

支持三种连接方式：
- 主从模式（单机）
- 哨兵模式
- Redis Cluster 模式

参数 |	说明 |	是否必填 |取值
---|---|---|---
connector| 结果表类型 | 是 | 固定值为 redis
host| 单机/主从模式必填 | 否 | 示例：127.0.0.1
port| 单机/主从模式必填 | 否 | 示例：6379
cluster.nodes| Cluster模式必填 | 否 | 示例：127.0.0.1:6379,127.0.0.1:6380,127.0.0.1:6381
sentinel.nodes| 哨兵模式必填 | 否 | 示例：test
sentinel.master| 哨兵模式必填 | 否 | 示例：127.0.0.1:6379,127.0.0.1:6380,127.0.0.1:6381
password| redis 密码 | 否 |  
mode | redis数据类型 | mode和command必填一个 | string/list/set/hashmap
command | redis命令 | mode和command必填一个 | set/del/hset/hdel/lpush/rpush/sadd/srem/incr/hincrby
dbNum | 选择操作的数据库| 否 | 默认0
key.ttl | key的超时时间 | 否 | 单位秒
cache.enable | 是否启用维表缓存 | 否 | 取值：true/false
cache.size | 维表缓存大小 | 否 | 默认：10000
cache.ttl | 维表缓存过期时间 | 否 | 默认：60s


# Redis 维表（string类型已经测试可用，hashmap待测试）

```sql
CREATE TABLE redis_dim ( 
  id STRING, 
  name STRING
) WITH ( 
  'connector' = 'redis', 
  'host' = '<yourHost>', 
  'port' = '<yourPort>', 
  'mode' = 'string',
  'password' = '<yourPassword>', 
  'dbNum' = '<yourDbNum>' 
);
```

```sql
CREATE TABLE redis_dim ( 
  id STRING, 
  data MAP<STRING, STRING>
) WITH ( 
  'connector' = 'redis', 
  'host' = '<yourHost>', 
  'port' = '<yourPort>', 
  'mode' = 'hashmap',
  'password' = '<yourPassword>', 
  'dbNum' = '<yourDbNum>' 
);
```

# Redis Sink

## Mode 模式
对应Redis的数据结构，支持 string/list/set/hashmap 类型。

### STRING 类型（测试可用）

DDL 为两列：第1列为 key，第2列为 value。Redis 插入数据的命令为 `set key value`。

```sql
create table redis_sink (
  a STRING, 
  b STRING, 
  PRIMARY KEY (a) NOT ENFORCED -- 必填。 
) with ( 
  'connector' = 'redis', 
  'mode' = 'string', 
  'host' = '<yourHost>', 
  'port' = '<yourPort>', 
  'password' = '<yourPassword>', 
  'dbNum' = '<yourDbNum>'
);
```

### LIST 类型（测试可用）

DDL 为两列：第1列为 key，第2列为 value。Redis 插入数据的命令为 `lpush key value`。
```sql
create table redis_sink (
  a STRING, 
  b STRING, 
  PRIMARY KEY (a) NOT ENFORCED -- 必填。 
) with ( 
  'connector' = 'redis', 
  'mode' = 'list', 
  'host' = '<yourHost>', 
  'port' = '<yourPort>', 
  'password' = '<yourPassword>', 
  'dbNum' = '<yourDbNum>'
);
```


### SET 类型（不可用）

DDL 为两列：第1列为 key，第2列为 value。Redis 插入数据的命令为 `sadd key value`。
```sql
create table redis_sink (
  a STRING, 
  b STRING, 
  PRIMARY KEY (a) NOT ENFORCED -- 必填。 
) with ( 
  'connector' = 'redis', 
  'mode' = 'set', 
  'host' = '<yourHost>', 
  'port' = '<yourPort>', 
  'password' = '<yourPassword>', 
  'dbNum' = '<yourDbNum>'
);
```

### HASHMAP 类型（测试可用）

```sql
create table redis_sink (
  key STRING, 
  field STRING, 
  value STRING,
  PRIMARY KEY (key) NOT ENFORCED -- 必填。 
) with ( 
  'connector' = 'redis', 
  'mode' = 'hashmap', 
  'host' = '<yourHost>', 
  'port' = '<yourPort>', 
  'password' = '<yourPassword>', 
  'dbNum' = '<yourDbNum>'
);
```

## Command 模式

对应Redis的命令，支持 string/list/set/hashmap 类型。


### SET
```sql
create table redis_sink (
  key STRING, 
  value STRING
  PRIMARY KEY (key) NOT ENFORCED -- 必填。 
) with ( 
  'connector' = 'redis', 
  'command' = 'set', 
  'host' = '<yourHost>', 
  'port' = '<yourPort>', 
  'password' = '<yourPassword>', 
  'dbNum' = '<yourDbNum>'
);
```

### HSET
```sql
create table redis_sink (
  key STRING, 
  field STRING, 
  value STRING,
  PRIMARY KEY (key) NOT ENFORCED -- 必填。 
) with ( 
  'connector' = 'redis', 
  'command' = 'hset', 
  'host' = '<yourHost>', 
  'port' = '<yourPort>', 
  'password' = '<yourPassword>', 
  'dbNum' = '<yourDbNum>'
);
```

### HINCRBY

```sql
create table redis_sink (
  key STRING, 
  field STRING, 
  value STRING,
  PRIMARY KEY (key) NOT ENFORCED -- 必填。 
) with ( 
  'connector' = 'redis', 
  'command' = 'hincrby', 
  'host' = '<yourHost>', 
  'port' = '<yourPort>', 
  'password' = '<yourPassword>', 
  'dbNum' = '<yourDbNum>'
);
```
