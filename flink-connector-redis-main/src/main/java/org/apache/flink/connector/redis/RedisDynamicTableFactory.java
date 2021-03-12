package org.apache.flink.connector.redis;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connector.redis.sink.RedisDynamicTableSink;
import org.apache.flink.connector.redis.source.RedisDynamicTableSource;
import org.apache.flink.shaded.guava18.com.google.common.base.Strings;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;

import java.util.HashSet;
import java.util.Set;

import static org.apache.flink.table.factories.FactoryUtil.createTableFactoryHelper;

/**
 * Redis connector factory
 */
public class RedisDynamicTableFactory implements DynamicTableSourceFactory, DynamicTableSinkFactory {

    protected static final String IDENTIFIER = "redis";

    public static final ConfigOption<String> REDIS_HOST = ConfigOptions
            .key("host")
            .stringType()
            .noDefaultValue()
            .withDeprecatedKeys("The host of redis");

    public static final ConfigOption<Integer> REDIS_PORT = ConfigOptions
            .key("port")
            .intType()
            .noDefaultValue()
            .withDeprecatedKeys("The host of redis");

    public static final ConfigOption<String> REDIS_CLUSTER_NODES = ConfigOptions
            .key("cluster.nodes")
            .stringType()
            .noDefaultValue()
            .withDeprecatedKeys("Redis cluster模式连接配置");

    public static final ConfigOption<String> REDIS_SENTINEL_NODES = ConfigOptions
            .key("sentinel.nodes")
            .stringType()
            .noDefaultValue()
            .withDeprecatedKeys("Redis sentinel模式连接配置");

    public static final ConfigOption<String> REDIS_SENTINEL_MASTER = ConfigOptions
            .key("sentinel.master")
            .stringType()
            .noDefaultValue()
            .withDeprecatedKeys("Redis sentinel模式配置必须配置");

    public static final ConfigOption<String> REDIS_PASSWORD = ConfigOptions
            .key("password")
            .stringType()
            .noDefaultValue()
            .withDescription("Redis密码,默认值为空，不进行权限验证");

    public static final ConfigOption<String> REDIS_COMMAND = ConfigOptions
            .key("command")
            .stringType()
            .noDefaultValue()
            .withDeprecatedKeys("对应Redis的命令");

    public static final ConfigOption<String> REDIS_MODE = ConfigOptions
            .key("mode")
            .stringType()
            .noDefaultValue()
            .withDeprecatedKeys("对应Redis的数据结构");

    public static final ConfigOption<Integer> REDIS_DB = ConfigOptions
            .key("dbNum")
            .intType()
            .defaultValue(0)
            .withDescription("选择操作的数据库,默认值为0");

    public static final ConfigOption<Integer> REDIS_KEY_TTL = ConfigOptions
            .key("key.ttl")
            .intType()
            .noDefaultValue()
            .withDescription("key的超时时间");

    public static final ConfigOption<Boolean> REDIS_CACHE = ConfigOptions
            .key("cache.enable")
            .booleanType()
            .defaultValue(Boolean.FALSE)
            .withDescription("Redis 维表缓存策略，None：无缓存（默认），LRU：现在 Cache 中查找数据，如果没有找到，则去物理维表中查找");


    public static final ConfigOption<Integer> REDIS_CACHE_SIZE = ConfigOptions
            .key("cache.size")
            .intType()
            .defaultValue(10000)
            .withDescription("选择使用LRU缓存策略，设置缓存的大小，默认为10000行");

    public static final ConfigOption<Integer> REDIS_CACHE_TTL = ConfigOptions
            .key("cache.ttl")
            .intType()
            .defaultValue(60)
            .withDescription("选择使用LRU缓存策略，设置缓存超时时间，默认60s");

    public static final ConfigOption<Boolean> REDIS_CACHE_EMPTY = ConfigOptions
            .key("cache.empty")
            .booleanType()
            .defaultValue(Boolean.TRUE)
            .withDescription("选择使用LRU缓存策略，设置是否缓存空结果");

    /**
     * 创建动态的table sink
     * @param context
     * @return
     */
    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        FactoryUtil.TableFactoryHelper helper = createTableFactoryHelper(this, context);
        helper.validate();
        ReadableConfig options = helper.getOptions();

        validateParams(options);
        return new RedisDynamicTableSink(options);
    }

    /**
     * 创建动态的source
     * @param context
     * @return
     */
    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        FactoryUtil.TableFactoryHelper helper = createTableFactoryHelper(this, context);
        helper.validate();
        ReadableConfig options = helper.getOptions();
        validateParams(options);
        TableSchema tableSchema = context.getCatalogTable().getSchema();
        return new RedisDynamicTableSource(tableSchema,options);
    }

    /**
     * 参数验证
     *
     * @param options
     */
    private void validateParams(ReadableConfig options) {
        String mode = options.get(REDIS_MODE);
        String command = options.get(REDIS_COMMAND);

        // 普通模式
        String host = options.get(REDIS_HOST);
        int port = options.get(REDIS_PORT);

        // cluster模式
        String clusterNodes = options.get(REDIS_CLUSTER_NODES);

        // sentinel 模式
        String master = options.get(REDIS_SENTINEL_MASTER);
        String sentinelNodes = options.get(REDIS_SENTINEL_NODES);

        if (!((!Strings.isNullOrEmpty(mode) || !Strings.isNullOrEmpty(command)) && ((!Strings.isNullOrEmpty(host) && port > 0) || !Strings.isNullOrEmpty(clusterNodes) || (!Strings.isNullOrEmpty(master) && !Strings.isNullOrEmpty(sentinelNodes))))) {
            throw new IllegalArgumentException("args not enough ");
        }
    }

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> set = new HashSet<>();

        return set;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> set = new HashSet<>();
        set.add(REDIS_HOST);
        set.add(REDIS_PORT);
        set.add(REDIS_CLUSTER_NODES);
        set.add(REDIS_SENTINEL_MASTER);
        set.add(REDIS_SENTINEL_NODES);
        set.add(REDIS_DB);

        set.add(REDIS_MODE);
        set.add(REDIS_COMMAND);

        set.add(REDIS_COMMAND);
        set.add(REDIS_KEY_TTL);
        set.add(REDIS_CACHE);
        set.add(REDIS_CACHE_SIZE);
        set.add(REDIS_CACHE_EMPTY);
        set.add(REDIS_CACHE_TTL);

        return set;
    }
}
