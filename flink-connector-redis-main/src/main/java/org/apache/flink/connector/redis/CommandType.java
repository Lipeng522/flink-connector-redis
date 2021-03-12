package org.apache.flink.connector.redis;

/**
 *
 */
public enum CommandType {
    SET("set", ""),
    DEL("del", ""),
    INCR("incr", ""),

    LPUSH("lpush", ""),
    RPUSH("rpush", ""),

    HSET("hset", ""),
    HDEL("hdel", ""),
    HINCRBY("hincrby", ""),

    SADD("sadd", ""),
    SREM("srem", ""),
    ;

    String command;
    String desc;

    CommandType(String command, String desc) {
        this.command = command;
        this.desc = desc;
    }

    public String getCommand() {
        return command;
    }

    public void setCommand(String command) {
        this.command = command;
    }

    public String getDesc() {
        return desc;
    }

    public void setDesc(String desc) {
        this.desc = desc;
    }


    public static CommandType fromCommand(String mode) {
        for (CommandType type : CommandType.values()) {
            if (type.getCommand().equalsIgnoreCase(mode)) {
                return type;
            }
        }
        return null;
    }
}
