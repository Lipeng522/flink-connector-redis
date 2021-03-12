package org.apache.flink.connector.redis.client;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Protocol;

import java.util.HashSet;
import java.util.Set;

public class JedisClusterConfig extends JedisConfigBase {

    private static final long serialVersionUID = 1L;

    private final Set<String> nodes;
    private final int maxRedirections;

    public JedisClusterConfig(Set<String> nodes, int connectionTimeout, int maxRedirections,
                              int maxTotal, int maxIdle, int minIdle,
                              String password) {
        super(connectionTimeout, maxTotal, maxIdle, minIdle, password);
        this.nodes = new HashSet<>(nodes);
        this.maxRedirections = maxRedirections;
    }

    public Set<HostAndPort> getNodes() {
        Set<HostAndPort> ret = new HashSet<>();
        for (String hostAndPort : nodes) {
            String[] parts = hostAndPort.split(":");
            if (parts != null && parts.length == 2) {
                ret.add(new HostAndPort(parts[0], Integer.parseInt(parts[1])));
            }
        }
        return ret;
    }

    public int getMaxRedirections() {
        return maxRedirections;
    }

    public static class Builder {
        private Set<String> nodes;
        private int timeout = Protocol.DEFAULT_TIMEOUT;
        private int maxRedirections = 5;
        private int maxTotal = GenericObjectPoolConfig.DEFAULT_MAX_TOTAL;
        private int maxIdle = GenericObjectPoolConfig.DEFAULT_MAX_IDLE;
        private int minIdle = GenericObjectPoolConfig.DEFAULT_MIN_IDLE;
        private String password;


        public Builder setNodes(Set<String> nodes) {
            this.nodes = nodes;
            return this;
        }

        public Builder setTimeout(int timeout) {
            this.timeout = timeout;
            return this;
        }

        public Builder setMaxRedirections(int maxRedirections) {
            this.maxRedirections = maxRedirections;
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

        public Builder setPassword(String password) {
            this.password = password;
            return this;
        }

        public JedisClusterConfig build() {
            return new JedisClusterConfig(nodes, timeout, maxRedirections, maxTotal, maxIdle, minIdle, password);
        }
    }

    @Override
    public String toString() {
        return "JedisClusterConfig{" +
                "nodes=" + nodes +
                ", timeout=" + connectionTimeout +
                ", maxRedirections=" + maxRedirections +
                ", maxTotal=" + maxTotal +
                ", maxIdle=" + maxIdle +
                ", minIdle=" + minIdle +
                '}';
    }

}
