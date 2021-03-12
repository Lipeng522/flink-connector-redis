package org.apache.flink.connector.redis.source;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connector.redis.ModeType;
import org.apache.flink.connector.redis.RedisDynamicTableFactory;
import org.apache.flink.connector.redis.client.JedisConfigBase;
import org.apache.flink.connector.redis.util.RedisUtils;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.*;
import org.apache.flink.table.functions.AsyncTableFunction;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import java.util.Optional;

public class RedisDynamicTableSource implements  LookupTableSource {
    private static ReadableConfig options;
    private static TableSchema tableSchema;

    public RedisDynamicTableSource() {

    }

    public RedisDynamicTableSource(TableSchema tableSchema,ReadableConfig options) {
        this.options = options;
        this.tableSchema = tableSchema;

    }

    @Override
    public LookupRuntimeProvider getLookupRuntimeProvider(LookupContext context) {
        Preconditions.checkArgument(context.getKeys().length == 1 && context.getKeys()[0].length == 1
                ,"Redis source only supports lookup by single key");

        int fieldCount = tableSchema.getFieldCount();
        if (fieldCount != 2){
            throw new ValidationException("Redis source only supports 2 columns");
        }

        DataType[] dataTypes = tableSchema.getFieldDataTypes();
        for (int i = 0; i <fieldCount ; i++) {
            if (!dataTypes[i].getLogicalType().getTypeRoot().equals(LogicalTypeRoot.VARCHAR)){
                throw new ValidationException("Redis connector only supports string type");
            }
        }
        return TableFunctionProvider.of(new RedisLookupFunction(options));
    }


    @Override
    public DynamicTableSource copy() {
        return new RedisDynamicTableSource(tableSchema,options);
    }

    @Override
    public String asSummaryString() {
        return "redis lookup source";
    }
}
