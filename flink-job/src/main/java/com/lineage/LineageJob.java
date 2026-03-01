package com.lineage;

import org.apache.avro.generic.GenericRecord;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.formats.avro.typeutils.GenericRecordAvroTypeInfo;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.KafkaSourceBuilder;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.parquet.avro.AvroParquetWriters;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.OnCheckpointRollingPolicy;
import org.apache.flink.util.Collector;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    private static final String WRITE_AHEAD_COMMIT_LOG_PATH = System.getenv().getOrDefault(
            "WRITE_AHEAD_COMMIT_LOG_PATH", "s3://flink-data/write-ahead-commit-log");
    private static final String DROPPED_PATH = System.getenv().getOrDefault(
            "DROPPED_PATH", "s3://flink-data/dropped");
    private static final String KAFKA_SECURITY_PROTOCOL = System.getenv().getOrDefault(
            "KAFKA_SECURITY_PROTOCOL", "PLAINTEXT");

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Kafka source using KafkaRecordDeserializationSchema for ConsumerRecord access
        KafkaSourceBuilder<ConsumerRecord<byte[], byte[]>> kafkaBuilder = KafkaSource
                .<ConsumerRecord<byte[], byte[]>>builder()
                .setBootstrapServers(KAFKA_BOOTSTRAP)
                .setTopics(KAFKA_TOPIC)
                .setGroupId("flink-lineage-consumer")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setDeserializer(new RawKafkaDeserializer());

        if (!"PLAINTEXT".equals(KAFKA_SECURITY_PROTOCOL)) {
            kafkaBuilder.setProperty("security.protocol", KAFKA_SECURITY_PROTOCOL);
        }

        KafkaSource<ConsumerRecord<byte[], byte[]>> kafkaSource = kafkaBuilder.build();

        DataStream<ConsumerRecord<byte[], byte[]>> kafkaStream = env.fromSource(
                kafkaSource,
                WatermarkStrategy.noWatermarks(),
                "Kafka Source");

        // Deserialize Avro + enrich with Kafka metadata
        SingleOutputStreamOperator<GenericRecord> enrichedStream = kafkaStream
                .process(new EnrichFunction())
                .returns(new GenericRecordAvroTypeInfo(AvroSchema.ENRICHED_EVENT_SCHEMA));

        // File sink: Parquet with date-time bucket assigner
        DateTimeBucketAssigner<GenericRecord> bucketAssigner =
                new DateTimeBucketAssigner<>("'year='yyyy'/month='MM'/day='dd'/hour='HH", ZoneOffset.UTC);

        FileSink<GenericRecord> fileSink = FileSink
                .forBulkFormat(
                        new Path(OUTPUT_PATH),
                        AvroParquetWriters.forGenericRecord(AvroSchema.ENRICHED_EVENT_SCHEMA))
                .withBucketAssigner(bucketAssigner)
                .withRollingPolicy(OnCheckpointRollingPolicy.build())
                .build();

        WriteAheadCommitLogFileSink<GenericRecord> walSink =
                new WriteAheadCommitLogFileSink<>(fileSink, WRITE_AHEAD_COMMIT_LOG_PATH);
        enrichedStream.sinkTo(walSink).uid("ParquetFileSink");

        // Dropped records sink: same Parquet format + partitioning as main sink
        DataStream<GenericRecord> droppedStream =
                enrichedStream.getSideOutput(EnrichFunction.DROPPED_TAG);

        FileSink<GenericRecord> droppedFileSink = FileSink
                .forBulkFormat(
                        new Path(DROPPED_PATH),
                        AvroParquetWriters.forGenericRecord(AvroSchema.ENRICHED_EVENT_SCHEMA))
                .withBucketAssigner(new DateTimeBucketAssigner<>("'year='yyyy'/month='MM'/day='dd'/hour='HH", ZoneOffset.UTC))
                .withRollingPolicy(OnCheckpointRollingPolicy.build())
                .build();

        WriteAheadCommitLogFileSink<GenericRecord> droppedWalSink =
                new WriteAheadCommitLogFileSink<>(droppedFileSink, WRITE_AHEAD_COMMIT_LOG_PATH, "-Dropped");
        droppedStream.sinkTo(droppedWalSink).uid("DroppedRecordsSink");

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

}
