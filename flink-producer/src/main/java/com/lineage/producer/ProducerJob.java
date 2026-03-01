package com.lineage.producer;

import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.util.Properties;
import java.util.UUID;

/**
 * Flink job that generates Avro-encoded messages and writes them to Kafka.
 * Replaces the Python data-generator with a Flink-native DataGeneratorSource.
 */
public class ProducerJob {

    private static final Logger LOG = LoggerFactory.getLogger(ProducerJob.class);

    private static final Schema INPUT_EVENT_SCHEMA;

    static {
        Schema timestampMillis = LogicalTypes.timestampMillis()
                .addToSchema(Schema.create(Schema.Type.LONG));

        INPUT_EVENT_SCHEMA = SchemaBuilder.record("InputEvent")
                .namespace("com.lineage")
                .fields()
                .requiredString("uuid")
                .name("timestamp").type(timestampMillis).noDefault()
                .endRecord();
    }

    private static final String KAFKA_BOOTSTRAP = System.getenv().getOrDefault(
            "KAFKA_BOOTSTRAP_SERVERS", "kafka:9092");
    private static final String KAFKA_TOPIC = System.getenv().getOrDefault(
            "KAFKA_TOPIC", "lineage-input");
    private static final String KAFKA_SECURITY_PROTOCOL = System.getenv().getOrDefault(
            "KAFKA_SECURITY_PROTOCOL", "PLAINTEXT");
    private static final int RATE_PER_SEC = Integer.parseInt(
            System.getenv().getOrDefault("RATE_PER_SEC", "10"));

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        LOG.info("Starting ProducerJob: bootstrap={}, topic={}, rate={}/s",
                KAFKA_BOOTSTRAP, KAFKA_TOPIC, RATE_PER_SEC);

        // DataGen source: generates Avro-encoded byte arrays
        GeneratorFunction<Long, byte[]> generatorFunction = index -> {
            GenericRecord record = new GenericData.Record(INPUT_EVENT_SCHEMA);
            record.put("uuid", UUID.randomUUID().toString());
            record.put("timestamp", System.currentTimeMillis());

            ByteArrayOutputStream out = new ByteArrayOutputStream();
            BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
            new GenericDatumWriter<GenericRecord>(INPUT_EVENT_SCHEMA).write(record, encoder);
            encoder.flush();
            return out.toByteArray();
        };

        DataGeneratorSource<byte[]> source = new DataGeneratorSource<>(
                generatorFunction,
                Long.MAX_VALUE,
                RateLimiterStrategy.perSecond(RATE_PER_SEC),
                PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO);

        DataStream<byte[]> stream = env.fromSource(
                source,
                WatermarkStrategy.noWatermarks(),
                "Avro Data Generator");

        // Kafka sink
        Properties kafkaProps = new Properties();
        if (!"PLAINTEXT".equals(KAFKA_SECURITY_PROTOCOL)) {
            kafkaProps.setProperty("security.protocol", KAFKA_SECURITY_PROTOCOL);
        }

        KafkaSink<byte[]> kafkaSink = KafkaSink.<byte[]>builder()
                .setBootstrapServers(KAFKA_BOOTSTRAP)
                .setKafkaProducerConfig(kafkaProps)
                .setRecordSerializer(KafkaRecordSerializationSchema.<byte[]>builder()
                        .setTopic(KAFKA_TOPIC)
                        .setValueSerializationSchema(new ByteArraySerializationSchema())
                        .build())
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();

        stream.sinkTo(kafkaSink).uid("KafkaSink");

        env.execute("Avro Data Producer");
    }
}
