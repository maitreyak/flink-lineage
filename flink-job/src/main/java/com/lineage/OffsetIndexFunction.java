package com.lineage;

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * Tracks per-partition min/max Kafka offset and record count.
 * Emits a summary record every 60 seconds (processing time), enabling
 * gap detection on the offset index without scanning data files.
 */
public class OffsetIndexFunction
        extends KeyedProcessFunction<Integer, GenericRecord, GenericRecord> {

    private static final long serialVersionUID = 1L;
    private static final long TIMER_INTERVAL_MS = 60_000L;

    private transient ValueState<Long> minOffsetState;
    private transient ValueState<Long> maxOffsetState;
    private transient ValueState<Long> countState;
    private transient ValueState<Long> windowStartState;
    private transient ValueState<Boolean> timerActiveState;

    @Override
    public void open(Configuration parameters) {
        minOffsetState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("minOffset", Types.LONG));
        maxOffsetState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("maxOffset", Types.LONG));
        countState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("count", Types.LONG));
        windowStartState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("windowStart", Types.LONG));
        timerActiveState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("timerActive", Types.BOOLEAN));
    }

    @Override
    public void processElement(GenericRecord record, Context ctx, Collector<GenericRecord> out)
            throws Exception {
        long offset = (Long) record.get("kafka_offset");

        Long currentMin = minOffsetState.value();
        Long currentMax = maxOffsetState.value();
        Long currentCount = countState.value();

        if (currentMin == null) {
            // First element in this window
            minOffsetState.update(offset);
            maxOffsetState.update(offset);
            countState.update(1L);
            windowStartState.update(ctx.timerService().currentProcessingTime());
        } else {
            minOffsetState.update(Math.min(currentMin, offset));
            maxOffsetState.update(Math.max(currentMax, offset));
            countState.update(currentCount + 1L);
        }

        // Register timer on first element after state was cleared
        Boolean timerActive = timerActiveState.value();
        if (timerActive == null || !timerActive) {
            long fireTime = ctx.timerService().currentProcessingTime() + TIMER_INTERVAL_MS;
            ctx.timerService().registerProcessingTimeTimer(fireTime);
            timerActiveState.update(true);
        }
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<GenericRecord> out)
            throws Exception {
        Long minOffset = minOffsetState.value();
        if (minOffset == null) {
            // No records arrived since last timer; don't emit empty index record
            timerActiveState.update(false);
            return;
        }

        GenericRecord indexRecord = new GenericRecordBuilder(AvroSchema.OFFSET_INDEX_SCHEMA)
                .set("kafka_partition", ctx.getCurrentKey())
                .set("min_offset", minOffsetState.value())
                .set("max_offset", maxOffsetState.value())
                .set("record_count", countState.value())
                .set("window_start", windowStartState.value())
                .build();

        out.collect(indexRecord);

        // Clear state for next window
        minOffsetState.clear();
        maxOffsetState.clear();
        countState.clear();
        windowStartState.clear();
        timerActiveState.update(false);
    }
}
