package org.apache.flink.connector.redis.sink;

import org.apache.flink.connector.redis.CommandType;
import org.apache.flink.connector.redis.ModeType;
import org.apache.flink.connector.redis.client.JedisConfigBase;
import org.apache.flink.connector.redis.client.RedisClientBase;
import org.apache.flink.connector.redis.client.RedisClientBuilder;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.table.data.RowData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.exceptions.JedisException;

/**
 *
 */
public class RedisRowDataSinkFunction extends RichSinkFunction<RowData> implements CheckpointedFunction {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(RedisRowDataSinkFunction.class);

    private JedisConfigBase jedisConfigBase;
    private ModeType mode;
    private CommandType commandType;
    private transient RedisClientBase redisClient;
    private RedisSinkOperator redisOperator;
    private Integer ttl;

    public RedisRowDataSinkFunction(JedisConfigBase jedisConfigBase, ModeType mode, CommandType commandType, Integer ttl) {
        System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
        this.jedisConfigBase = jedisConfigBase;
        this.mode = mode;
        this.commandType = commandType;
        this.ttl = ttl;
    }


    @Override
    public void open(Configuration parameters) throws Exception {
        LOG.info("start open ...");
        try {
            if (null == redisClient) {
                this.redisClient = RedisClientBuilder.build(this.jedisConfigBase);
            }
            redisOperator = new RedisSinkOperator(redisClient, mode, commandType);
        } catch (Exception e) {
            LOG.error("Redis has not been properly initialized: ", e);
            throw e;
        }
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {

    }

    @Override
    public void invoke(RowData rowData, Context context) throws Exception {
        try {
            redisOperator.process(rowData, ttl);
        } catch (JedisException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            throw e;
        }
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {

    }

    @Override
    public void close() throws Exception {
        if (redisClient != null) {
            redisClient.close();
        }
    }
}
