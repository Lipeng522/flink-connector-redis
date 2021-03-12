package org.apache.flink.connector.redis.client;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.JedisCluster;

import java.io.IOException;
import java.io.Serializable;
import java.util.Map;

/**
 *
 */
public class RedisClusterClient implements RedisClientBase, Serializable {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(RedisClusterClient.class);

    private transient JedisCluster jedisCluster;

    public RedisClusterClient(JedisCluster jedisCluster) {
        this.jedisCluster = jedisCluster;
    }

    @Override
    public void open() throws Exception {

    }

    @Override
    public void hset(final String key, final String hashField, final String value, final Integer ttl) {
        try {
            jedisCluster.hset(key, hashField, value);
            if (ttl != null) {
                jedisCluster.expire(key, ttl);
            }
        } catch (Exception e) {
            if (LOG.isErrorEnabled()) {
                LOG.error("Cannot send Redis message with command HSET to hash {} of key {} error message {}",
                        hashField, key, e.getMessage());
            }
            throw e;
        }
    }

    @Override
    public void hdel(String key, String filed) {

    }

    @Override
    public Long hincrBy(final String key, final String hashField, final Long value, final Integer ttl) {
        Long ret = null;
        try {
            ret = jedisCluster.hincrBy(key, hashField, value);
            if (ttl != null) {
                jedisCluster.expire(key, ttl);
            }
        } catch (Exception e) {
            if (LOG.isErrorEnabled()) {
                LOG.error("Cannot send Redis message with command HINCRBY to hash {} of key {} error message {}",
                        hashField, key, e.getMessage());
            }
            throw e;
        }
        return ret;
    }

    @Override
    public void rpush(final String listName, final String value) {
        try {
            jedisCluster.rpush(listName, value);
        } catch (Exception e) {
            if (LOG.isErrorEnabled()) {
                LOG.error("Cannot send Redis message with command RPUSH to list {} error message: {}",
                        listName, e.getMessage());
            }
            throw e;
        }
    }

    @Override
    public void lpush(String listName, String value) {
        try {
            jedisCluster.lpush(listName, value);
        } catch (Exception e) {
            if (LOG.isErrorEnabled()) {
                LOG.error("Cannot send Redis message with command LPUSH to list {} error message: {}",
                        listName, e.getMessage());
            }
            throw e;
        }
    }

    @Override
    public void sadd(final String setName, final String value, Integer ttl) {
        try {
            jedisCluster.sadd(setName, value);
            if (ttl != null && ttl.intValue() > 0) {
                jedisCluster.expire(setName, ttl);
            }
        } catch (Exception e) {
            if (LOG.isErrorEnabled()) {
                LOG.error("Cannot send Redis message with command SADD to set {} error message {}",
                        setName, e.getMessage());
            }
            throw e;
        }
    }

    @Override
    public void srem(String setName, String value) {
        try {
            jedisCluster.srem(setName, value);
        } catch (Exception e) {
            if (LOG.isErrorEnabled()) {
                LOG.error("Cannot send Redis message with command SREM to set {} error message {}",
                        setName, e.getMessage());
            }
            throw e;
        }
    }

    @Override
    public void publish(final String channelName, final String message) {
        try {
            jedisCluster.publish(channelName, message);
        } catch (Exception e) {
            if (LOG.isErrorEnabled()) {
                LOG.error("Cannot send Redis message with command PUBLISH to channel {} error message {}",
                        channelName, e.getMessage());
            }
            throw e;
        }
    }

    @Override
    public void set(final String key, final String value, Integer ttl) {
        try {
            jedisCluster.set(key, value);
            if (ttl != null && ttl.intValue() > 0) {
                jedisCluster.expire(key, ttl);
            }
        } catch (Exception e) {
            if (LOG.isErrorEnabled()) {
                LOG.error("Cannot send Redis message with command SET to key {} error message {}",
                        key, e.getMessage());
            }
            throw e;
        }
    }

    @Override
    public String get(String key) {
        String ret = null;
        try {
            ret = jedisCluster.get(key);
        } catch (Exception e) {
            if (LOG.isErrorEnabled()) {
                LOG.error("Cannot send Redis message with command GET to key {} error message {}",
                        key, e.getMessage());
            }
            throw e;
        }
        return ret;
    }

    @Override
    public Long incrBy(String key, Long value, Integer ttl) {
        Long ret = null;
        try {
            ret = jedisCluster.incrBy(key, value);
            if (ttl != null) {
                jedisCluster.expire(key, ttl);
            }
        } catch (Exception e) {
            if (LOG.isErrorEnabled()) {
                LOG.error("Cannot send Redis message with command INCRBY to key {} with increment {} and tll {} error message {}",
                        key, value, ttl, e.getMessage());
            }
            throw e;
        }
        return ret;
    }

    @Override
    public Long decrBy(String key, Long value, Integer ttl) {
        Long ret = null;
        try {
            ret = jedisCluster.decrBy(key, value);
            if (ttl != null) {
                jedisCluster.expire(key, ttl);
            }
        } catch (Exception e) {
            if (LOG.isErrorEnabled()) {
                LOG.error("Cannot send Redis message with command descry to key {} with decrement {} and ttl {} error message {}",
                        key, value, ttl, e.getMessage());
            }
            throw e;
        }
        return ret;
    }


    @Override
    public void setex(final String key, final String value, final Integer ttl) {
        try {
            jedisCluster.setex(key, ttl, value);
        } catch (Exception e) {
            if (LOG.isErrorEnabled()) {
                LOG.error("Cannot send Redis message with command SETEX to key {} error message {}",
                        key, e.getMessage());
            }
            throw e;
        }
    }

    @Override
    public void pfadd(final String key, final String element) {
        try {
            jedisCluster.pfadd(key, element);
        } catch (Exception e) {
            if (LOG.isErrorEnabled()) {
                LOG.error("Cannot send Redis message with command PFADD to key {} error message {}",
                        key, e.getMessage());
            }
            throw e;
        }
    }

    @Override
    public void zadd(final String key, final String score, final String element) {
        try {
            jedisCluster.zadd(key, Double.valueOf(score), element);
        } catch (Exception e) {
            if (LOG.isErrorEnabled()) {
                LOG.error("Cannot send Redis message with command ZADD to set {} error message {}",
                        key, e.getMessage());
            }
            throw e;
        }
    }

    @Override
    public void zincrBy(final String key, final String score, final String element) {
        try {
            jedisCluster.zincrby(key, Double.valueOf(score), element);
        } catch (Exception e) {
            if (LOG.isErrorEnabled()) {
                LOG.error("Cannot send Redis message with command ZINCRBY to set {} error message {}",
                        key, e.getMessage());
            }
            throw e;
        }
    }

    @Override
    public void zrem(final String key, final String element) {
        try {
            jedisCluster.zrem(key, element);
        } catch (Exception e) {
            if (LOG.isDebugEnabled()) {
                LOG.error("Cannot send Redis message with command ZREM to set {} error message {}",
                        key, e.getMessage());
            }
        }
    }

    @Override
    public void del(String key) {
        try {
            jedisCluster.del(key);
        } catch (Exception e) {
            if (LOG.isErrorEnabled()) {
                LOG.error("Cannot send Redis message with command DEL to set {} error message {}",
                        key, e.getMessage());
            }
            throw e;
        }
    }

    @Override
    public String hget(String key, String hashField) {
        try {
            return jedisCluster.hget(key, hashField);
        } catch (Exception e) {
            if (LOG.isErrorEnabled()) {
                LOG.error("Cannot send Redis message with command HGET to key {} and hashField {} error message {}",
                        key, hashField, e.getMessage());
            }
            throw e;
        }
    }

    @Override
    public Map<String, String> hgetAll(String key) {
        try {
            return jedisCluster.hgetAll(key);
        } catch (Exception e) {
            if (LOG.isErrorEnabled()) {
                LOG.error("Cannot send Redis message with command HGETALL to key {} error message {}",
                        key, e.getMessage());
            }
            throw e;
        }
    }

    @Override
    public <T> T execute(RedisCallback<T> action) {
        return null;
    }

    /**
     * Closes the {@link JedisCluster}.
     */
    @Override
    public void close() throws IOException {
        this.jedisCluster.close();
    }
}
