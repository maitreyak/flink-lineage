package com.lineage;

import java.io.Serializable;

/**
 * POJO representing the source Avro record from Kafka.
 */
public class InputEvent implements Serializable {
    private static final long serialVersionUID = 1L;

    private String uuid;
    private long timestamp;

    public InputEvent() {}

    public InputEvent(String uuid, long timestamp) {
        this.uuid = uuid;
        this.timestamp = timestamp;
    }

    public String getUuid() { return uuid; }
    public void setUuid(String uuid) { this.uuid = uuid; }

    public long getTimestamp() { return timestamp; }
    public void setTimestamp(long timestamp) { this.timestamp = timestamp; }

    @Override
    public String toString() {
        return "InputEvent{uuid='" + uuid + "', timestamp=" + timestamp + "}";
    }
}
