package org.apache.flink.connector.redis.source;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connector.redis.ModeType;
import org.apache.flink.connector.redis.RedisDynamicTableFactory;
import org.apache.flink.connector.redis.client.JedisConfigBase;
import org.apache.flink.connector.redis.client.RedisClientBase;
import org.apache.flink.connector.redis.client.RedisClientBuilder;
import org.apache.flink.connector.redis.util.RedisUtils;
import org.apache.flink.shaded.guava18.com.google.common.cache.Cache;
import org.apache.flink.shaded.guava18.com.google.common.cache.CacheBuilder;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.functions.AsyncTableFunction;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * The RedisLookupFunction is a standard user-defined table function, it can be used in tableAPI
 * and also useful for temporal table join plan in SQL. It looks up the result as {@link Row}.
 */
@Internal
public class RedisLookupFunction extends TableFunction<RowData> {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(RedisLookupFunction.class);

    private static JedisConfigBase jedisConfigBase;
    private static ReadableConfig options;

    private Boolean cacheEnable;
    private Integer cacheSize;
    private Integer cacheTTL;
    private String  mode;


    private transient RedisClientBase redisClient;
    private RedisSourceOperator redisOperator;

    /**
     * 缓存
     */
    private transient Cache<RowData, RowData> cacheString;
    /**
     * TODO hash map 目前还没测试
     */
    private transient Cache<RowData, Map<String,String >> cacheHash;
    public RedisLookupFunction(ReadableConfig options) {
        Preconditions.checkNotNull(options,"no options supplied");

        this.options = options;
    }


    @Override
    public void open(FunctionContext context) throws Exception {
        jedisConfigBase = RedisUtils.getJedisConfig(options);
        mode = options.get(RedisDynamicTableFactory.REDIS_MODE);

        cacheEnable = options.get(RedisDynamicTableFactory.REDIS_CACHE);
        cacheSize = options.get(RedisDynamicTableFactory.REDIS_CACHE_SIZE);
        cacheTTL = options.get(RedisDynamicTableFactory.REDIS_CACHE_TTL);
        try {
            if (null == redisClient) {
                this.redisClient = RedisClientBuilder.build(this.jedisConfigBase);
            }
            redisOperator = new RedisSourceOperator(redisClient);

            if (cacheEnable.equals(true) && cacheSize > 0 && cacheTTL > 0){
                if (mode.equalsIgnoreCase("hashmap")){
                    this.cacheHash = !cacheEnable ? null : CacheBuilder.newBuilder()
                            .expireAfterAccess(cacheTTL, TimeUnit.SECONDS)
                            .maximumSize(cacheSize)
                            .build();
                }else if (mode.equalsIgnoreCase("string")){
                    this.cacheString = !cacheEnable ? null : CacheBuilder.newBuilder()
                            .expireAfterAccess(cacheTTL, TimeUnit.SECONDS)
                            .maximumSize(cacheSize)
                            .build();
                }else{
                    throw new Exception("mode arga not define");
                }
            }
        } catch (Exception e) {
            LOG.error("Redis has not been properly initialized: ", e);
            throw e;
        }
    }

    /**
     * STRING 模式已经测试成功，Hash模式待测试
     * @param obj
     */
    public void eval( Object obj) {
        RowData lookupKey = GenericRowData.of(obj);
        if (mode.equalsIgnoreCase("hashmap")){
            if (cacheHash != null) {
                Map<String, String> stringMap = cacheHash.getIfPresent(lookupKey);
                if (stringMap != null) {
                    collect(GenericRowData.of(stringMap));
                    return;
                } else {
                    Map<String, String> map = redisOperator.hgetAll(lookupKey.getString(0).toString());
                    if (map != null) {
                        collect(GenericRowData.of(stringMap));
                        if (cacheHash != null) {
                            cacheHash.put(lookupKey, map);
                        }
                    }
                }
            }
        }else if (mode.equalsIgnoreCase("string")){
            if (cacheString != null) {
                RowData cachedRow = cacheString.getIfPresent(lookupKey);
                if (cachedRow != null) {
                    collect(cachedRow);
                    return;
                }
            }
            StringData key = lookupKey.getString(0);
            String value = redisOperator.get(key.toString());
            RowData result = GenericRowData.of(key, StringData.fromString(value));


            if (cacheString != null) {
                cacheString.put(lookupKey, result);
            }
            collect(result);
        }
    }

    @Override
    public void close() throws Exception {
        if (redisClient != null) {
            redisClient.close();
        }
    }
}
