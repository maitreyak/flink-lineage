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
                .requiredLong("checkpoint_id")
                .endRecord();

    }
}
