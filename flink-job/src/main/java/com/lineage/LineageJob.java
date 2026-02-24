package com.lineage;

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.generic.GenericDatumReader;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.formats.avro.typeutils.GenericRecordAvroTypeInfo;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.parquet.avro.AvroParquetWriters;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.OnCheckpointRollingPolicy;
import org.apache.flink.util.Collector;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.ZoneOffset;

/**
 * Flink job that reads Avro messages from Kafka, enriches them with
 * Kafka metadata (topic, partition, offset), and writes Parquet files
 * to S3 (MinIO) partitioned by year/month/day/hour.
 */
public class LineageJob {

    private static final Logger LOG = LoggerFactory.getLogger(LineageJob.class);

    private static final String KAFKA_BOOTSTRAP = System.getenv().getOrDefault(
            "KAFKA_BOOTSTRAP_SERVERS", "kafka:9092");
    private static final String KAFKA_TOPIC = System.getenv().getOrDefault(
            "KAFKA_TOPIC", "lineage-input");
    private static final String OUTPUT_PATH = System.getenv().getOrDefault(
            "OUTPUT_PATH", "s3://flink-data/output");
    private static final String OFFSET_INDEX_PATH = System.getenv().getOrDefault(
            "OFFSET_INDEX_PATH", "s3://flink-data/offset-index");

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Kafka source using KafkaRecordDeserializationSchema for ConsumerRecord access
        KafkaSource<ConsumerRecord<byte[], byte[]>> kafkaSource = KafkaSource
                .<ConsumerRecord<byte[], byte[]>>builder()
                .setBootstrapServers(KAFKA_BOOTSTRAP)
                .setTopics(KAFKA_TOPIC)
                .setGroupId("flink-lineage-consumer")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setDeserializer(new RawKafkaDeserializer())
                .build();

        DataStream<ConsumerRecord<byte[], byte[]>> kafkaStream = env.fromSource(
                kafkaSource,
                WatermarkStrategy.noWatermarks(),
                "Kafka Source");

        // Deserialize Avro + enrich with Kafka metadata -> GenericRecord
        DataStream<GenericRecord> enrichedStream = kafkaStream
                .process(new EnrichFunction())
                .returns(new GenericRecordAvroTypeInfo(AvroSchema.ENRICHED_EVENT_SCHEMA));

        // File sink: Parquet with date-time bucket assigner
        FileSink<GenericRecord> fileSink = FileSink
                .forBulkFormat(
                        new Path(OUTPUT_PATH),
                        AvroParquetWriters.forGenericRecord(AvroSchema.ENRICHED_EVENT_SCHEMA))
                .withBucketAssigner(new DateTimeBucketAssigner<>("'year='yyyy'/month='MM'/day='dd'/hour='HH", ZoneOffset.UTC))
                .withRollingPolicy(OnCheckpointRollingPolicy.build())
                .build();

        enrichedStream.sinkTo(fileSink);

        // Offset index sink: lightweight per-partition offset summaries for gap detection
        DataStream<GenericRecord> offsetIndexStream = enrichedStream
                .keyBy(r -> (int) r.get("kafka_partition"))
                .process(new OffsetIndexFunction())
                .returns(new GenericRecordAvroTypeInfo(AvroSchema.OFFSET_INDEX_SCHEMA));

        FileSink<GenericRecord> offsetIndexSink = FileSink
                .forBulkFormat(
                        new Path(OFFSET_INDEX_PATH),
                        AvroParquetWriters.forGenericRecord(AvroSchema.OFFSET_INDEX_SCHEMA))
                .withRollingPolicy(OnCheckpointRollingPolicy.build())
                .build();

        offsetIndexStream.sinkTo(offsetIndexSink);

        env.execute("Kafka-to-S3 Lineage Pipeline");
    }

    /**
     * Deserializer that passes through raw Kafka ConsumerRecords for metadata access.
     */
    public static class RawKafkaDeserializer implements
            KafkaRecordDeserializationSchema<ConsumerRecord<byte[], byte[]>> {

        private static final long serialVersionUID = 1L;

        @Override
        public void deserialize(
                ConsumerRecord<byte[], byte[]> record,
                Collector<ConsumerRecord<byte[], byte[]>> out) {
            out.collect(record);
        }

        @Override
        public TypeInformation<ConsumerRecord<byte[], byte[]>> getProducedType() {
            @SuppressWarnings("unchecked")
            Class<ConsumerRecord<byte[], byte[]>> clazz =
                    (Class<ConsumerRecord<byte[], byte[]>>) (Class<?>) ConsumerRecord.class;
            return TypeInformation.of(clazz);
        }
    }

    /**
     * ProcessFunction that deserializes Avro payload and enriches with Kafka metadata.
     */
    public static class EnrichFunction
            extends org.apache.flink.streaming.api.functions.ProcessFunction<
                ConsumerRecord<byte[], byte[]>, GenericRecord> {

        private static final long serialVersionUID = 1L;
        private transient GenericDatumReader<GenericRecord> reader;
        private transient DecoderFactory decoderFactory;

        @Override
        public void open(org.apache.flink.configuration.Configuration parameters) {
            reader = new GenericDatumReader<>(AvroSchema.INPUT_EVENT_SCHEMA);
            decoderFactory = DecoderFactory.get();
        }

        @Override
        public void processElement(
                ConsumerRecord<byte[], byte[]> record,
                Context ctx,
                Collector<GenericRecord> out) {
            try {
                byte[] value = record.value();
                BinaryDecoder decoder = decoderFactory.binaryDecoder(value, null);
                GenericRecord inputRecord = reader.read(null, decoder);

                String uuid = inputRecord.get("uuid").toString();
                long timestamp = (Long) inputRecord.get("timestamp");

                GenericRecord enriched = new GenericRecordBuilder(AvroSchema.ENRICHED_EVENT_SCHEMA)
                        .set("uuid", uuid)
                        .set("timestamp", timestamp)
                        .set("kafka_topic", record.topic())
                        .set("kafka_partition", record.partition())
                        .set("kafka_offset", record.offset())
                        .build();

                out.collect(enriched);
            } catch (IOException e) {
                LOG.error("Failed to deserialize Avro record from partition={} offset={}",
                        record.partition(), record.offset(), e);
            }
        }
    }
}
