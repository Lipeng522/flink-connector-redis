package org.apache.flink.connector.redis.source;

import org.apache.flink.connector.redis.client.RedisClientBase;

import java.util.Map;

/**
 * RedisSourceOperator
 */
public class RedisSourceOperator {

    private RedisClientBase redisClient;

    public RedisSourceOperator(RedisClientBase redisClient) {
        this.redisClient = redisClient;
    }

    public String get(String key) {
        return redisClient.get(key);
    }

    public String hget(String key, String field) {
        return redisClient.hget(key, field);
    }

    public Map<String, String> hgetAll(String key) {
        return redisClient.hgetAll(key);
    }
}

