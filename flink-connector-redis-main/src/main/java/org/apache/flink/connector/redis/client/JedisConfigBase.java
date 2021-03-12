package org.apache.flink.connector.redis.client;

import java.io.Serializable;

public class JedisConfigBase implements Serializable {

    private static final long serialVersionUID = 1L;

    protected final int maxTotal;
    protected final int maxIdle;
    protected final int minIdle;
    protected final int connectionTimeout;
    protected final String password;

    public JedisConfigBase(int connectionTimeout, int maxTotal, int maxIdle, int minIdle, String password) {
        this.connectionTimeout = connectionTimeout;
        this.maxTotal = maxTotal;
        this.maxIdle = maxIdle;
        this.minIdle = minIdle;
        this.password = password;
    }

    public int getMaxTotal() {
        return maxTotal;
    }

    public int getMaxIdle() {
        return maxIdle;
    }

    public int getMinIdle() {
        return minIdle;
    }

    public int getConnectionTimeout() {
        return connectionTimeout;
    }

    public String getPassword() {
        return password;
    }
}
