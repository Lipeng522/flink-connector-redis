package org.apache.flink.connector.redis.sink;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.redis.CommandType;
import org.apache.flink.connector.redis.ModeType;
import org.apache.flink.connector.redis.RedisDynamicTableFactory;
import org.apache.flink.connector.redis.client.JedisConfigBase;
import org.apache.flink.connector.redis.util.RedisUtils;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.sinks.UpsertStreamTableSink;
import org.apache.flink.table.utils.TableConnectorUtils;
import org.apache.flink.table.utils.TableSchemaUtils;
import org.apache.flink.types.Row;

import java.util.Map;

import static org.apache.flink.table.descriptors.Schema.SCHEMA;

public class RedisUpsertTableSink implements UpsertStreamTableSink<Row> {

    private DescriptorProperties descriptorProperties;

    private JedisConfigBase jedisConfigBase;
    private ModeType modeType;
    private CommandType commandType;
    private Integer ttl;

    private TableSchema tableSchema;

    private String[] keyFields;
    private Map<String, String> properties = null;

    public RedisUpsertTableSink(DescriptorProperties descriptor) {
        this.descriptorProperties = descriptor;
        tableSchema = TableSchemaUtils.getPhysicalSchema(descriptor.getTableSchema(SCHEMA));

        jedisConfigBase = RedisUtils.getJedisConfig(descriptor);
        String mode = descriptor.getString(RedisDynamicTableFactory.REDIS_MODE.key());
        modeType = ModeType.fromMode(mode);

        String command = descriptor.getString(RedisDynamicTableFactory.REDIS_COMMAND.key());
        commandType = CommandType.fromCommand(command);
        ttl = descriptor.getInt(RedisDynamicTableFactory.REDIS_KEY_TTL.key());
    }

    @Override
    public void setKeyFields(String[] keys) {

    }

    @Override
    public void setIsAppendOnly(Boolean isAppendOnly) {
        // redis always upsert on key, even works in append only mode.
    }

    @Override
    public TypeInformation<Row> getRecordType() {
        return null;
    }

    @Override
    public DataStreamSink<?> consumeDataStream(DataStream<Tuple2<Boolean, Row>> dataStream) {
        return dataStream.addSink(new RedisSinkFunction(jedisConfigBase, modeType, commandType, ttl))
                .setParallelism(dataStream.getParallelism())
                .name(TableConnectorUtils.generateRuntimeName(this.getClass(), tableSchema.getFieldNames()));
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    @Override
    public TableSink<Tuple2<Boolean, Row>> configure(String[] fieldNames, TypeInformation<?>[] fieldTypes) {
        return new RedisUpsertTableSink(descriptorProperties);
    }
}
