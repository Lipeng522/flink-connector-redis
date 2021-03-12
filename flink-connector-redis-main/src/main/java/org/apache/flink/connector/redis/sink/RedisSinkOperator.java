package org.apache.flink.connector.redis.sink;

import org.apache.flink.connector.redis.CommandType;
import org.apache.flink.connector.redis.ModeType;
import org.apache.flink.connector.redis.client.RedisClientBase;
import org.apache.flink.shaded.guava18.com.google.common.base.Strings;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.RowKind;

/**
 * RedisSinkOperator
 */
public class RedisSinkOperator {

    private RedisClientBase redisClient;
    private ModeType modeType;
    private CommandType commandType;

    public RedisSinkOperator(RedisClientBase redisClient, ModeType modeType, CommandType commandType) {
        this.redisClient = redisClient;
        this.modeType = modeType;
        this.commandType = commandType;
    }

    private String getKey(RowData rowData, int index) {
        return rowData.getString(index).toString();
    }

    /**
     * string
     */

    public void set(RowData rowData, Integer ttl) {
        String key = getKey(rowData, 0);
        String value = rowData.getString(1).toString();
        redisClient.set(key, value, ttl);
    }

    public void del(RowData rowData) {
        String key = getKey(rowData, 0);
        if (!Strings.isNullOrEmpty(key))
            redisClient.del(key);
    }

    public void incr(RowData rowData, Integer ttl) {
        String key = getKey(rowData, 0);
        Long value = rowData.getLong(1);
        redisClient.incrBy(key, value, ttl);
    }

    /**
     * hash
     */

    public void hset(RowData rowData, Integer ttl) {
        String key = getKey(rowData, 0);
        String filed = rowData.getString(1).toString();
        String value = rowData.getString(2).toString();
        redisClient.hset(key, filed, value, null);
    }

    public void hdel(RowData rowData) {
        String key = getKey(rowData, 0);
        String filed = rowData.getString(1).toString();
        redisClient.hdel(key, filed);
    }

    public void hincrBy(RowData rowData, Integer ttl) {
        String key = getKey(rowData, 0);
        String field = rowData.getString(1).toString();
        Long value = rowData.getLong(2);
        redisClient.hincrBy(key, field, value, ttl);
    }

    /**
     * list
     */

    public void lpush(RowData rowData, Integer ttl) {
        String key = getKey(rowData, 0);
        String value = rowData.getString(1).toString();
        redisClient.lpush(key, value);
    }

    public void rpush(RowData rowData, Integer ttl) {
        String key = getKey(rowData, 0);
        String value = rowData.getString(1).toString();
        redisClient.lpush(key, value);
    }

    /**
     * set
     */

    public void sadd(RowData rowData, Integer ttl) {
        String key = getKey(rowData, 0);
        String value = rowData.getString(1).toString();
        redisClient.sadd(key, value, ttl);
    }

    public void srem(RowData rowData) {
        String key = getKey(rowData, 0);
        String value = rowData.getString(1).toString();
        redisClient.srem(key, value);
    }

    /**
     * string
     *
     * @param rowData
     * @param ttl
     * @throws Exception
     */
    public void string(RowData rowData, Integer ttl) throws Exception {
        GenericRowData genericRowData = (GenericRowData) rowData;
        RowKind rowKind = genericRowData.getRowKind();
        switch (rowKind) {
            case DELETE:
                del(rowData);
                break;
            case INSERT:
            case UPDATE_AFTER:
                set(rowData, ttl);
                break;
        }
    }

    /**
     * hash
     *
     * @param rowData
     * @param ttl
     * @throws Exception
     */
    public void hash(RowData rowData, Integer ttl) throws Exception {
        GenericRowData genericRowData = (GenericRowData) rowData;
        RowKind rowKind = genericRowData.getRowKind();
        switch (rowKind) {
            case DELETE:
                hdel(rowData);
                break;
            case INSERT:
            case UPDATE_AFTER:
                hset(rowData, ttl);
                break;
        }
    }

    /**
     * list
     *
     * @param rowData
     * @param ttl
     * @throws Exception
     */
    public void list(RowData rowData, Integer ttl) throws Exception {
        GenericRowData genericRowData = (GenericRowData) rowData;
        RowKind rowKind = genericRowData.getRowKind();
        switch (rowKind) {
            case DELETE:
                break;
            case INSERT:
            case UPDATE_AFTER:
                lpush(rowData, ttl);
                break;
        }
    }

    /**
     * set
     *
     * @param rowData
     * @param ttl
     * @throws Exception
     */
    public void sset(RowData rowData, Integer ttl) throws Exception {
        GenericRowData genericRowData = (GenericRowData) rowData;
        RowKind rowKind = genericRowData.getRowKind();
        switch (rowKind) {
            case DELETE:
                srem(rowData);
                break;
            case INSERT:
            case UPDATE_AFTER:
                sadd(rowData, ttl);
                break;
        }
    }

    /**
     * @param rowData
     * @param ttl
     * @throws Exception
     */
    public void process(RowData rowData, Integer ttl) throws Exception {
        if (modeType != null) {
            switch (modeType) {
                case STRING:
                    string(rowData, ttl);
                    break;
                case LIST:
                    list(rowData, ttl);
                    break;
                case SET:
                    sset(rowData, ttl);
                    break;
                case HASHMAP:
                    hash(rowData, ttl);
                    break;
                default:
                    throw new IllegalAccessException("No Supported.");
            }
        } else if (commandType != null) {
            switch (commandType) {
                case SET:
                    set(rowData, ttl);
                    break;
                case DEL:
                    del(rowData);
                    break;
                case INCR:
                    incr(rowData, ttl);
                    break;
                case HSET:
                    hset(rowData, ttl);
                    break;
                case HDEL:
                    hdel(rowData);
                    break;
                case HINCRBY:
                    hincrBy(rowData, ttl);
                    break;
                case LPUSH:
                    lpush(rowData, ttl);
                    break;
                case RPUSH:
                    rpush(rowData, ttl);
                    break;
                case SADD:
                    sadd(rowData, ttl);
                    break;
                case SREM:
                    srem(rowData);
                    break;
                default:
                    throw new IllegalAccessException("No Supported.");
            }
        }
    }

}
