package com.lineage;

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
import org.apache.flink.streaming.api.connector.sink2.CommittableMessage;
import org.apache.flink.streaming.api.connector.sink2.SupportsPreCommitTopology;
import org.apache.flink.streaming.api.datastream.DataStream;

import java.io.IOException;
import java.util.Collection;

/**
 * Wrapper around {@link FileSink} that injects a {@link WriteAheadCommitLogOperator}
 * into the pre-commit topology. The operator eagerly writes CSV write-ahead commit
 * log entries in {@code processElement()}, ensuring entries are durable before the
 * checkpoint completes.
 *
 * @param <IN> Type of elements written to the sink
 */
public class WriteAheadCommitLogSink<IN>
        implements Sink<IN>,
                   SupportsWriterState<IN, FileWriterBucketState>,
                   SupportsCommitter<FileSinkCommittable>,
                   SupportsWriterState.WithCompatibleState,
                   SupportsPreCommitTopology<FileSinkCommittable, FileSinkCommittable>,
                   SupportsConcurrentExecutionAttempts {

    private static final long serialVersionUID = 1L;

    private final FileSink<IN> delegate;
    private final String commitLogBasePath;
    private final String sinkName;

    public WriteAheadCommitLogSink(FileSink<IN> delegate, String commitLogBasePath) {
        this(delegate, commitLogBasePath, "");
    }

    public WriteAheadCommitLogSink(FileSink<IN> delegate, String commitLogBasePath, String sinkName) {
        this.delegate = delegate;
        this.commitLogBasePath = commitLogBasePath;
        this.sinkName = sinkName;
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

        // 2. Transform with WriteAheadCommitLogOperator (eagerly writes CSV)
        String filePrefix = sinkName.isEmpty() ? "output" : sinkName.replaceFirst("^-", "").toLowerCase();

        return delegateStream
                .transform(
                        "WriteAheadCommitLog",
                        delegateStream.getType(),
                        new WriteAheadCommitLogOperator(commitLogBasePath, filePrefix))
                .setParallelism(delegateStream.getParallelism())
                .uid("WriteAheadCommitLog" + sinkName);
    }
}
