package com.lineage.producer;

import org.apache.flink.api.common.serialization.SerializationSchema;

/**
 * Passthrough serialization schema for byte arrays.
 */
public class ByteArraySerializationSchema implements SerializationSchema<byte[]> {

    private static final long serialVersionUID = 1L;

    @Override
    public byte[] serialize(byte[] element) {
        return element;
    }
}
