package org.apache.flink.connector.redis.client;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisSentinelPool;

import java.util.Objects;

/**
 * RedisClientBuilder
 */
public class RedisClientBuilder {

    public static RedisClientBase build(JedisConfigBase jedisConfigBase) {
        if (jedisConfigBase instanceof JedisPoolConfig) { // jedis连接池
            return build((JedisPoolConfig) jedisConfigBase);
        } else if (jedisConfigBase instanceof JedisClusterConfig) { // jedis cluster
            return build((JedisClusterConfig) jedisConfigBase);
        } else if (jedisConfigBase instanceof JedisSentinelConfig) { // jedis sentinel
            return build((JedisSentinelConfig) jedisConfigBase);
        } else {
            throw new IllegalArgumentException("Jedis configuration not found");
        }
    }

    /**
     * @param jedisPoolConfig
     * @return
     */
    public static RedisClientBase build(JedisPoolConfig jedisPoolConfig) {
        Objects.requireNonNull(jedisPoolConfig, "Redis pool config should not be Null");

        GenericObjectPoolConfig genericObjectPoolConfig = new GenericObjectPoolConfig();
        genericObjectPoolConfig.setMaxIdle(jedisPoolConfig.getMaxIdle());
        genericObjectPoolConfig.setMaxTotal(jedisPoolConfig.getMaxTotal());
        genericObjectPoolConfig.setMinIdle(jedisPoolConfig.getMinIdle());

        JedisPool jedisPool = new JedisPool(genericObjectPoolConfig,
                jedisPoolConfig.getHost(),
                jedisPoolConfig.getPort(),
                jedisPoolConfig.getConnectionTimeout(),
                jedisPoolConfig.getPassword(),
                jedisPoolConfig.getDatabase());
        return new RedisClient(jedisPool);
    }

    /**
     * @param jedisClusterConfig
     * @return
     */
    public static RedisClientBase build(JedisClusterConfig jedisClusterConfig) {
        Objects.requireNonNull(jedisClusterConfig, "Redis cluster config should not be Null");

        GenericObjectPoolConfig genericObjectPoolConfig = new GenericObjectPoolConfig();
        genericObjectPoolConfig.setMaxIdle(jedisClusterConfig.getMaxIdle());
        genericObjectPoolConfig.setMaxTotal(jedisClusterConfig.getMaxTotal());
        genericObjectPoolConfig.setMinIdle(jedisClusterConfig.getMinIdle());

        JedisCluster jedisCluster = new JedisCluster(jedisClusterConfig.getNodes(),
                jedisClusterConfig.getConnectionTimeout(),
                jedisClusterConfig.getConnectionTimeout(),
                jedisClusterConfig.getMaxRedirections(),
                jedisClusterConfig.getPassword(),
                genericObjectPoolConfig);
        return new RedisClusterClient(jedisCluster);
    }

    /**
     * @param jedisSentinelConfig
     * @return
     */
    public static RedisClientBase build(JedisSentinelConfig jedisSentinelConfig) {
        Objects.requireNonNull(jedisSentinelConfig, "Redis sentinel config should not be Null");

        GenericObjectPoolConfig genericObjectPoolConfig = new GenericObjectPoolConfig();
        genericObjectPoolConfig.setMaxIdle(jedisSentinelConfig.getMaxIdle());
        genericObjectPoolConfig.setMaxTotal(jedisSentinelConfig.getMaxTotal());
        genericObjectPoolConfig.setMinIdle(jedisSentinelConfig.getMinIdle());

        JedisSentinelPool jedisSentinelPool = new JedisSentinelPool(jedisSentinelConfig.getMasterName(),
                jedisSentinelConfig.getSentinels(), genericObjectPoolConfig,
                jedisSentinelConfig.getConnectionTimeout(), jedisSentinelConfig.getSoTimeout(),
                jedisSentinelConfig.getPassword(), jedisSentinelConfig.getDatabase());
        return new RedisClient(jedisSentinelPool);
    }
}
