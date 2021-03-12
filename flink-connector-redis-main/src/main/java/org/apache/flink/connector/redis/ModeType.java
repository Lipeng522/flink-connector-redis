package org.apache.flink.connector.redis;

/**
 *
 */
public enum ModeType {
    STRING("string", ""),
    LIST("list", ""),
    SET("sset", ""),
    HASHMAP("hashmap", ""),
    SORTEDSET("sortedset", ""),
    ;

    private String mode;
    private String desc;

    ModeType(String mode, String desc) {
        this.mode = mode;
        this.desc = desc;
    }

    public String getMode() {
        return mode;
    }

    public void setMode(String mode) {
        this.mode = mode;
    }

    public String getDesc() {
        return desc;
    }

    public void setDesc(String desc) {
        this.desc = desc;
    }

    public static ModeType fromMode(String mode) {
        for (ModeType type : ModeType.values()) {
            if (type.getMode().equalsIgnoreCase(mode)) {
                return type;
            }
        }
        return null;
    }
}
