package org.apache.flink.connector.redis.client;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import redis.clients.jedis.Protocol;

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

public class JedisSentinelConfig extends JedisConfigBase {

    private static final long serialVersionUID = 1L;

    private final String masterName;
    private final Set<String> sentinels;
    private final int soTimeout;
    private final int database;

    public JedisSentinelConfig(String masterName, Set<String> sentinels,
                               int connectionTimeout, int soTimeout,
                               String password, int database,
                               int maxTotal, int maxIdle, int minIdle) {
        super(connectionTimeout, maxTotal, maxIdle, minIdle, password);
        Objects.requireNonNull(masterName, "Master name should be presented");
        Objects.requireNonNull(sentinels, "Sentinels information should be presented");

        this.masterName = masterName;
        this.sentinels = new HashSet<>(sentinels);
        this.soTimeout = soTimeout;
        this.database = database;
    }

    public static long getSerialVersionUID() {
        return serialVersionUID;
    }


    public String getMasterName() {
        return masterName;
    }

    public Set<String> getSentinels() {
        return sentinels;
    }

    public int getSoTimeout() {
        return soTimeout;
    }

    public int getDatabase() {
        return database;
    }

    public static class Builder {
        private String masterName;
        private Set<String> sentinels;
        private int connectionTimeout = Protocol.DEFAULT_TIMEOUT;
        private int soTimeout = Protocol.DEFAULT_TIMEOUT;
        private String password;
        private int database = Protocol.DEFAULT_DATABASE;
        private int maxTotal = GenericObjectPoolConfig.DEFAULT_MAX_TOTAL;
        private int maxIdle = GenericObjectPoolConfig.DEFAULT_MAX_IDLE;
        private int minIdle = GenericObjectPoolConfig.DEFAULT_MIN_IDLE;

        public Builder setMasterName(String masterName) {
            this.masterName = masterName;
            return this;
        }

        public Builder setSentinels(Set<String> sentinels) {
            this.sentinels = sentinels;
            return this;
        }

        public Builder setConnectionTimeout(int connectionTimeout) {
            this.connectionTimeout = connectionTimeout;
            return this;
        }

        public Builder setSoTimeout(int soTimeout) {
            this.soTimeout = soTimeout;
            return this;
        }

        public Builder setPassword(String password) {
            this.password = password;
            return this;
        }

        public Builder setDatabase(int database) {
            this.database = database;
            return this;
        }

        public Builder setMaxTotal(int maxTotal) {
            this.maxTotal = maxTotal;
            return this;
        }

        public Builder setMaxIdle(int maxIdle) {
            this.maxIdle = maxIdle;
            return this;
        }

        public Builder setMinIdle(int minIdle) {
            this.minIdle = minIdle;
            return this;
        }

        public JedisSentinelConfig builder() {
            return new JedisSentinelConfig(masterName, sentinels, connectionTimeout, soTimeout,
                    password, database, maxTotal, maxIdle, minIdle);
        }
    }


    @Override
    public String toString() {
        return "JedisSentinelConfig{" +
                "masterName='" + masterName + '\'' +
                ", connectionTimeout=" + connectionTimeout +
                ", soTimeout=" + soTimeout +
                ", database=" + database +
                ", maxTotal=" + maxTotal +
                ", maxIdle=" + maxIdle +
                ", minIdle=" + minIdle +
                '}';
    }
}
