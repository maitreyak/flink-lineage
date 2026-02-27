package com.lineage;

import org.apache.avro.generic.GenericRecord;
import org.apache.flink.api.common.SupportsConcurrentExecutionAttempts;
import org.apache.flink.api.connector.sink2.Committer;
import org.apache.flink.api.connector.sink2.CommitterInitContext;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.StatefulSinkWriter;
import org.apache.flink.api.connector.sink2.SupportsCommitter;
import org.apache.flink.api.connector.sink2.SupportsWriterState;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.connector.file.sink.FileSinkCommittable;
import org.apache.flink.connector.file.sink.writer.FileWriterBucketState;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.formats.avro.typeutils.GenericRecordAvroTypeInfo;
import org.apache.flink.streaming.api.connector.sink2.CommittableMessage;
import org.apache.flink.streaming.api.connector.sink2.SupportsPreCommitTopology;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

import java.io.IOException;
import java.util.Collection;

/**
 * Wrapper around {@link FileSink} that injects a {@link CommitLogExtractingOperator}
 * into the pre-commit topology. The operator emits commit log records as side output,
 * which are written to Parquet by a {@link CommitLogParquetWriterOperator}.
 *
 * <p>The Parquet writer is attached via {@code .transform()} (not {@code .sinkTo()})
 * to avoid a {@code ConcurrentModificationException} in Flink's {@code SinkExpander},
 * which iterates a SubList of {@code env.transformations} and cannot tolerate nested
 * sink expansions.
 *
 * @param <IN> Type of elements written to the sink
 */
public class CommitLoggingFileSink<IN>
        implements Sink<IN>,
                   SupportsWriterState<IN, FileWriterBucketState>,
                   SupportsCommitter<FileSinkCommittable>,
                   SupportsWriterState.WithCompatibleState,
                   SupportsPreCommitTopology<FileSinkCommittable, FileSinkCommittable>,
                   SupportsConcurrentExecutionAttempts {

    private static final long serialVersionUID = 1L;

    private final FileSink<IN> delegate;
    private final String commitLogBasePath;

    public CommitLoggingFileSink(FileSink<IN> delegate, String commitLogBasePath) {
        this.delegate = delegate;
        this.commitLogBasePath = commitLogBasePath;
    }

    // --- Sink ---

    @Override
    public SinkWriter<IN> createWriter(InitContext context) throws IOException {
        return delegate.createWriter(context);
    }

    // --- SupportsWriterState ---

    @Override
    public StatefulSinkWriter<IN, FileWriterBucketState> createWriter(WriterInitContext context)
            throws IOException {
        return delegate.createWriter(context);
    }

    @Override
    public StatefulSinkWriter<IN, FileWriterBucketState> restoreWriter(
            WriterInitContext context,
            Collection<FileWriterBucketState> recoveredState) throws IOException {
        return delegate.restoreWriter(context, recoveredState);
    }

    @Override
    public SimpleVersionedSerializer<FileWriterBucketState> getWriterStateSerializer() {
        return delegate.getWriterStateSerializer();
    }

    // --- SupportsWriterState.WithCompatibleState ---

    @Override
    public Collection<String> getCompatibleWriterStateNames() {
        return delegate.getCompatibleWriterStateNames();
    }

    // --- SupportsCommitter ---

    @Override
    public Committer<FileSinkCommittable> createCommitter(CommitterInitContext context)
            throws IOException {
        return delegate.createCommitter(context);
    }

    @Override
    public SimpleVersionedSerializer<FileSinkCommittable> getCommittableSerializer() {
        return delegate.getCommittableSerializer();
    }

    // --- SupportsPreCommitTopology ---

    @Override
    public SimpleVersionedSerializer<FileSinkCommittable> getWriteResultSerializer() {
        return delegate.getWriteResultSerializer();
    }

    @Override
    public DataStream<CommittableMessage<FileSinkCommittable>> addPreCommitTopology(
            DataStream<CommittableMessage<FileSinkCommittable>> committableStream) {
        // 1. Let the delegate add its own pre-commit topology (e.g., compaction)
        DataStream<CommittableMessage<FileSinkCommittable>> delegateStream =
                delegate.addPreCommitTopology(committableStream);

        // 2. Transform with CommitLogExtractingOperator (stateless, emits side output)
        SingleOutputStreamOperator<CommittableMessage<FileSinkCommittable>> result =
                delegateStream
                        .transform(
                                "CommitLogExtractor",
                                delegateStream.getType(),
                                new CommitLogExtractingOperator())
                        .setParallelism(delegateStream.getParallelism())
                        .uid("CommitLogExtractor");

        // 3. Capture the side output and write CSV via .transform()
        //    (NOT .sinkTo() — that triggers nested SinkExpander expansion and CME)
        DataStream<GenericRecord> commitLogStream =
                result.getSideOutput(CommitLogExtractingOperator.COMMIT_LOG_TAG);

        commitLogStream
                .transform(
                        "CommitLogWriter",
                        new GenericRecordAvroTypeInfo(AvroSchema.COMMIT_LOG_SCHEMA),
                        new CommitLogWriterOperator(commitLogBasePath))
                .setParallelism(result.getParallelism())
                .uid("CommitLogWriter");

        // 4. Return main committable stream for the data sink's committer
        return result;
    }
}
