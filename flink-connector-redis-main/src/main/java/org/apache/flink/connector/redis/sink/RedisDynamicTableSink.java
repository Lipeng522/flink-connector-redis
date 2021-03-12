package org.apache.flink.connector.redis.sink;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connector.redis.CommandType;
import org.apache.flink.connector.redis.ModeType;
import org.apache.flink.connector.redis.RedisDynamicTableFactory;
import org.apache.flink.connector.redis.client.JedisConfigBase;
import org.apache.flink.connector.redis.util.RedisUtils;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.types.RowKind;

/**
 * A {@link DynamicTableSink} for Redis.
 */
@Internal
public class RedisDynamicTableSink implements DynamicTableSink {

    private ReadableConfig options;

    public RedisDynamicTableSink(ReadableConfig options) {
        this.options = options;
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        JedisConfigBase jedisConfig = RedisUtils.getJedisConfig(options);

        String mode = options.get(RedisDynamicTableFactory.REDIS_MODE);
        String command = options.get(RedisDynamicTableFactory.REDIS_COMMAND);
        Integer ttl = options.get(RedisDynamicTableFactory.REDIS_KEY_TTL);

        RedisRowDataSinkFunction sinkFunction = new RedisRowDataSinkFunction(jedisConfig, ModeType.fromMode(mode), CommandType.fromCommand(command), ttl);
        return SinkFunctionProvider.of(sinkFunction);
    }


    @Override
    public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
        return ChangelogMode.newBuilder()
                .addContainedKind(RowKind.INSERT)
                .addContainedKind(RowKind.UPDATE_BEFORE)
                .addContainedKind(RowKind.UPDATE_AFTER)
                .addContainedKind(RowKind.DELETE)
                .build();
    }


    @Override
    public DynamicTableSink copy() {
        return new RedisDynamicTableSink(this.options);
    }

    @Override
    public String asSummaryString() {
        return "Redis";
    }
}
