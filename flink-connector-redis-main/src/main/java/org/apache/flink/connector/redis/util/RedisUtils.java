package org.apache.flink.connector.redis.util;

import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connector.redis.RedisDynamicTableFactory;
import org.apache.flink.connector.redis.client.JedisClusterConfig;
import org.apache.flink.connector.redis.client.JedisConfigBase;
import org.apache.flink.connector.redis.client.JedisPoolConfig;
import org.apache.flink.connector.redis.client.JedisSentinelConfig;
import org.apache.flink.shaded.guava18.com.google.common.base.Strings;
import org.apache.flink.table.descriptors.DescriptorProperties;

import java.util.HashSet;
import java.util.Set;

public class RedisUtils {

    public static JedisConfigBase getJedisConfig(ReadableConfig options) {
        JedisConfigBase jedisConfigBase = null;

        String host = options.get(RedisDynamicTableFactory.REDIS_HOST);
        int port = options.get(RedisDynamicTableFactory.REDIS_PORT);

        String clusterNodes = options.get(RedisDynamicTableFactory.REDIS_CLUSTER_NODES);

        String sentinelMaster = options.get(RedisDynamicTableFactory.REDIS_SENTINEL_MASTER);
        String sentinelNodes = options.get(RedisDynamicTableFactory.REDIS_SENTINEL_NODES);

        String password = options.get(RedisDynamicTableFactory.REDIS_PASSWORD);
        int db = options.get(RedisDynamicTableFactory.REDIS_DB);

        if (!Strings.isNullOrEmpty(host) && port > 0) {
            jedisConfigBase = new JedisPoolConfig.Builder()
                    .setHost(host)
                    .setPort(port)
                    .setPassword(password)
                    .setDatabase(db)
                    .build();
        } else if (!Strings.isNullOrEmpty(clusterNodes)) {
            Set<String> nodes = new HashSet<>();
            String[] hostWithIps = clusterNodes.split(",");
            for (String hostWithIp : hostWithIps) {
                nodes.add(hostWithIp);
            }
            jedisConfigBase = new JedisClusterConfig.Builder()
                    .setNodes(nodes)
                    .setPassword(password)
                    //.setDatabase(db) // 集群客户端是不支持多数据库db的
                    .build();
        } else if (!Strings.isNullOrEmpty(sentinelMaster) && !Strings.isNullOrEmpty(sentinelNodes)) {
            Set<String> sentinels = new HashSet<>();
            String[] hostWithIps = sentinelNodes.split(",");
            for (String hostWithIp : hostWithIps) {
                sentinels.add(hostWithIp);
            }
            jedisConfigBase = new JedisSentinelConfig.Builder()
                    .setMasterName(sentinelMaster)
                    .setSentinels(sentinels)
                    .setPassword(password)
                    .setDatabase(db)
                    .builder();
        }
        return jedisConfigBase;
    }

    public static JedisConfigBase getJedisConfig(DescriptorProperties descriptorProperties) {
        JedisConfigBase jedisConfigBase = null;

        String host = descriptorProperties.getString(RedisDynamicTableFactory.REDIS_HOST.key());
        int port = descriptorProperties.getInt(RedisDynamicTableFactory.REDIS_PORT.key());

        String clusterNodes = descriptorProperties.getString(RedisDynamicTableFactory.REDIS_CLUSTER_NODES.key());

        String sentinelMaster = descriptorProperties.getString(RedisDynamicTableFactory.REDIS_SENTINEL_MASTER.key());
        String sentinelNodes = descriptorProperties.getString(RedisDynamicTableFactory.REDIS_SENTINEL_NODES.key());

        String password = descriptorProperties.getString(RedisDynamicTableFactory.REDIS_PASSWORD.key());
        int db = descriptorProperties.getInt(RedisDynamicTableFactory.REDIS_DB.key());

        if (!Strings.isNullOrEmpty(host) && port > 0) {
            jedisConfigBase = new JedisPoolConfig.Builder()
                    .setHost(host)
                    .setPort(port)
                    .setPassword(password)
                    .setDatabase(db)
                    .build();
        } else if (!Strings.isNullOrEmpty(clusterNodes)) {
            Set<String> nodes = new HashSet<>();
            String[] hostWithIps = clusterNodes.split(",");
            for (String hostWithIp : hostWithIps) {
                nodes.add(hostWithIp);
            }
            jedisConfigBase = new JedisClusterConfig.Builder()
                    .setNodes(nodes)
                    .setPassword(password)
                    //.setDatabase(db) // 集群客户端是不支持多数据库db的
                    .build();
        } else if (!Strings.isNullOrEmpty(sentinelMaster) && !Strings.isNullOrEmpty(sentinelNodes)) {
            Set<String> sentinels = new HashSet<>();
            String[] hostWithIps = sentinelNodes.split(",");
            for (String hostWithIp : hostWithIps) {
                sentinels.add(hostWithIp);
            }
            jedisConfigBase = new JedisSentinelConfig.Builder()
                    .setMasterName(sentinelMaster)
                    .setSentinels(sentinels)
                    .setPassword(password)
                    .setDatabase(db)
                    .builder();
        }
        return jedisConfigBase;
    }
}
