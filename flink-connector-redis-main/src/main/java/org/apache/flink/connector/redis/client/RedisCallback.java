package org.apache.flink.connector.redis.client;

import redis.clients.jedis.Pipeline;

public interface RedisCallback<T> {
    T doInRedis(Pipeline pipeline);
}
