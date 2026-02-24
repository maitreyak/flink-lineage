package com.lineage;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.LogicalTypes;

/**
 * Avro schema definitions for input and enriched events.
 */
public final class AvroSchema {

    private AvroSchema() {}

    /** Schema for the source Kafka Avro record. */
    public static final Schema INPUT_EVENT_SCHEMA;

    /** Schema for the enriched record written to Parquet. */
    public static final Schema ENRICHED_EVENT_SCHEMA;

    /** Schema for the offset index sidecar (gap detection without full scans). */
    public static final Schema OFFSET_INDEX_SCHEMA;

    static {
        // Input: uuid (string), timestamp (long/timestamp-millis)
        Schema timestampMillis = LogicalTypes.timestampMillis().addToSchema(Schema.create(Schema.Type.LONG));

        INPUT_EVENT_SCHEMA = SchemaBuilder.record("InputEvent")
                .namespace("com.lineage")
                .fields()
                .requiredString("uuid")
                .name("timestamp").type(timestampMillis).noDefault()
                .endRecord();

        // Enriched: uuid, timestamp, kafka_topic, kafka_partition, kafka_offset
        ENRICHED_EVENT_SCHEMA = SchemaBuilder.record("EnrichedEvent")
                .namespace("com.lineage")
                .fields()
                .requiredString("uuid")
                .name("timestamp").type(timestampMillis).noDefault()
                .requiredString("kafka_topic")
                .requiredInt("kafka_partition")
                .requiredLong("kafka_offset")
                .endRecord();

        // Offset index: per-partition min/max offset and record count
        OFFSET_INDEX_SCHEMA = SchemaBuilder.record("OffsetIndex")
                .namespace("com.lineage")
                .fields()
                .requiredInt("kafka_partition")
                .requiredLong("min_offset")
                .requiredLong("max_offset")
                .requiredLong("record_count")
                .name("window_start").type(timestampMillis).noDefault()
                .endRecord();
    }
}
