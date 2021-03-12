package org.apache.flink.connector.redis.client;

import java.io.IOException;
import java.io.Serializable;
import java.util.Map;

/**
 * RedisClientBase
 */
public interface RedisClientBase extends Serializable {

    void open() throws Exception;

    /**
     * string
     *
     * @param key
     * @param value
     */
    void set(String key, String value, Integer ttl);

    String get(String key);

    void del(String key);

    void setex(String key, String value, Integer ttl);

    Long incrBy(String key, Long value, Integer ttl);

    Long decrBy(String key, Long value, Integer ttl);

    /**
     * hset
     */
    void hset(String key, String hashField, String value, Integer ttl);

    void hdel(String key, String filed);

    String hget(String key, String hashField);

    Map<String,String> hgetAll(String key);

    Long hincrBy(String key, String hashField, Long value, Integer ttl);

    /**
     * list
     */
    void rpush(String listName, String value);

    void lpush(String listName, String value);

    /**
     * set
     */

    void sadd(String setName, String value,Integer ttl);

    void srem(String setName, String value);

    /**
     *
     */
    void publish(String channelName, String message);

    void pfadd(String key, String element);

    /**
     * zset
     */
    void zadd(String key, String score, String element);

    void zincrBy(String key, String score, String element);

    void zrem(String key, String element);

    <T> T execute(RedisCallback<T> action);

    void close() throws IOException;

}
