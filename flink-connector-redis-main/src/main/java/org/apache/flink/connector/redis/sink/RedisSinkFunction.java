package org.apache.flink.connector.redis.sink;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.redis.CommandType;
import org.apache.flink.connector.redis.ModeType;
import org.apache.flink.connector.redis.client.JedisConfigBase;
import org.apache.flink.connector.redis.client.RedisClientBase;
import org.apache.flink.connector.redis.client.RedisClientBuilder;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RedisSinkFunction<T> extends RichSinkFunction<T> implements CheckpointedFunction {

    private static final Logger LOG = LoggerFactory.getLogger(RedisSinkFunction.class);

    private final JedisConfigBase jedisConfigBase;
    private final ModeType modeType;
    private final CommandType commandType;
    private final Integer ttl;

    private transient RedisClientBase redisClient;
    private RedisSinkOperator redisOperator;


    public RedisSinkFunction(JedisConfigBase jedisConfigBase, ModeType modeType, CommandType commandType, Integer ttl) {
        System.out.println("<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<");
        this.jedisConfigBase = jedisConfigBase;
        this.modeType = modeType;
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
            redisOperator = new RedisSinkOperator(redisClient, modeType, commandType);
        } catch (Exception e) {
            LOG.error("Redis has not been properly initialized: ", e);
            throw e;
        }
    }

    @Override
    public void invoke(T value, Context context) throws Exception {

    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {

    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {

    }
}
