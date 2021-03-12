package org.apache.flink.connector.redis.util;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.types.logical.RowType;

import java.io.Serializable;

@Internal
public class RedisTableSchema implements Serializable {

    private static final long serialVersionUID = 1L;

    private String key;

    public RedisTableSchema() {

    }

    RedisTableSchema(String key) {
        this.key = key;
    }

    public static RedisTableSchema fromTableSchema(TableSchema schema) {
        RedisTableSchema redisSchema = new RedisTableSchema();
        RowType rowType = (RowType) schema.toPhysicalRowDataType().getLogicalType();
        for (RowType.RowField field : rowType.getFields()) {
            RowType rt = (RowType) field.getType();
            String name = field.getName();
        }

        return redisSchema;
    }

}
