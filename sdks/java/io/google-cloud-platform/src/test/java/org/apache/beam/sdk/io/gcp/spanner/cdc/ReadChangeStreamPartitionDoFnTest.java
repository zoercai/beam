package org.apache.beam.sdk.io.gcp.spanner.cdc;

import static org.apache.beam.sdk.io.gcp.spanner.cdc.TestStructMapper.recordsToStruct;
import static org.apache.beam.sdk.io.gcp.spanner.cdc.model.PartitionMetadata.State.CREATED;
import static org.apache.beam.sdk.io.gcp.spanner.cdc.model.PartitionMetadata.State.FINISHED;
import static org.apache.beam.sdk.io.gcp.spanner.cdc.model.PartitionMetadata.State.SCHEDULED;
import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.mockStatic;

import com.google.cloud.Timestamp;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Struct;
import com.google.common.collect.ImmutableMap;
import java.util.Arrays;
import java.util.Collections;
import java.util.function.Function;
import org.apache.beam.sdk.io.gcp.spanner.SpannerConfig;
import org.apache.beam.sdk.io.gcp.spanner.cdc.dao.ChangeStreamDao;
import org.apache.beam.sdk.io.gcp.spanner.cdc.dao.DaoFactory;
import org.apache.beam.sdk.io.gcp.spanner.cdc.dao.PartitionMetadataDao;
import org.apache.beam.sdk.io.gcp.spanner.cdc.dao.PartitionMetadataDao.InTransactionContext;
import org.apache.beam.sdk.io.gcp.spanner.cdc.model.ChildPartitionsRecord;
import org.apache.beam.sdk.io.gcp.spanner.cdc.model.ChildPartitionsRecord.ChildPartition;
import org.apache.beam.sdk.io.gcp.spanner.cdc.model.ColumnType;
import org.apache.beam.sdk.io.gcp.spanner.cdc.model.DataChangesRecord;
import org.apache.beam.sdk.io.gcp.spanner.cdc.model.HeartbeatRecord;
import org.apache.beam.sdk.io.gcp.spanner.cdc.model.Mod;
import org.apache.beam.sdk.io.gcp.spanner.cdc.model.ModType;
import org.apache.beam.sdk.io.gcp.spanner.cdc.model.PartitionMetadata;
import org.apache.beam.sdk.io.gcp.spanner.cdc.model.TypeCode;
import org.apache.beam.sdk.io.gcp.spanner.cdc.model.ValueCaptureType;
import org.apache.beam.sdk.io.gcp.spanner.cdc.restriction.PartitionPosition;
import org.apache.beam.sdk.io.gcp.spanner.cdc.restriction.PartitionRestriction;
import org.apache.beam.sdk.transforms.DoFn.OutputReceiver;
import org.apache.beam.sdk.transforms.DoFn.ProcessContinuation;
import org.apache.beam.sdk.transforms.splittabledofn.ManualWatermarkEstimator;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.joda.time.Instant;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

@RunWith(PowerMockRunner.class)
@PrepareForTest(DaoFactory.class)
public class ReadChangeStreamPartitionDoFnTest {

  private static final String PARTITION_TOKEN = "partitionToken";
  private static final Timestamp PARTITION_START_TIMESTAMP = Timestamp.ofTimeSecondsAndNanos(10, 20);
  private static final Timestamp PARTITION_END_TIMESTAMP = Timestamp.ofTimeSecondsAndNanos(30, 40);
  public static final long PARTITION_HEARTBEAT_SECONDS = 30L;

  private PartitionMetadataDao partitionMetadataDao;
  private ChangeStreamDao changeStreamDao;
  private ReadChangeStreamPartitionDoFn doFn;
  private PartitionMetadata element;
  private RestrictionTracker<PartitionRestriction, PartitionPosition> restrictionTracker;
  private OutputReceiver<DataChangesRecord> outputReceiver;
  private ManualWatermarkEstimator<Instant> watermarkEstimator;
  private String tableName;

  @Before
  public void setUp() {
    final SpannerConfig spannerConfig = SpannerConfig
        .create()
        .withProjectId("project-id")
        .withInstanceId("instance-id")
        .withDatabaseId("database-id");
    mockStatic(DaoFactory.class);

    tableName = "table-id";
    partitionMetadataDao = mock(PartitionMetadataDao.class);
    changeStreamDao = mock(ChangeStreamDao.class);
    doFn = new ReadChangeStreamPartitionDoFn(spannerConfig, tableName);
    element = PartitionMetadata.newBuilder()
        .setPartitionToken(PARTITION_TOKEN)
        .setParentTokens(Collections.singletonList("parentToken"))
        .setStartTimestamp(PARTITION_START_TIMESTAMP)
        .setInclusiveStart(true)
        .setEndTimestamp(PARTITION_END_TIMESTAMP)
        .setInclusiveEnd(false)
        .setHeartbeatSeconds(PARTITION_HEARTBEAT_SECONDS)
        .setState(SCHEDULED)
        .build();
    restrictionTracker = mock(RestrictionTracker.class);
    outputReceiver = mock(OutputReceiver.class);
    watermarkEstimator = mock(ManualWatermarkEstimator.class);

    when(DaoFactory.partitionMetadataDaoFrom(spannerConfig)).thenReturn(partitionMetadataDao);
    when(DaoFactory.changeStreamDaoFrom(spannerConfig)).thenReturn(changeStreamDao);
    doFn.setup();
  }

  // --------------------------
  // Happy paths

  // DataChangesRecord
  // PartitionMetadata Record
  // Calls Spanner
  // Read DataRecord
  //   - Updates restriction
  //   - Updates watermark
  //   - Sends to output stream
  //   - Marks the current partition as finished
  //   - Waits for parent partitions
  //   - Deletes itself
  @Test
  public void testDoFnProcessesDataRecords() {
    final DataChangesRecord record = new DataChangesRecord(
        PARTITION_TOKEN,
        Timestamp.ofTimeSecondsAndNanos(10, 20),
        "transactionId456",
        false,
        "recordSequence789",
        "TableName",
        Arrays.asList(
            new ColumnType("column1", new TypeCode("typeCode1"), true),
            new ColumnType("column2", new TypeCode("typeCode2"), false)
        ),
        Collections.singletonList(
            new Mod(
                ImmutableMap.of("column1", "value1", "column2", "oldValue2"),
                ImmutableMap.of("column1", "value1", "column2", "newValue2")
            )
        ),
        ModType.UPDATE,
        ValueCaptureType.OLD_AND_NEW_VALUES
    );
    final Struct recordAsStruct = recordsToStruct(record);
    final ResultSet resultSet = mock(ResultSet.class);

    when(changeStreamDao.changeStreamQuery()).thenReturn(resultSet);
    when(resultSet.next()).thenReturn(true, false);
    when(resultSet.getCurrentRowAsStruct()).thenReturn(recordAsStruct);
    when(restrictionTracker.tryClaim(any(PartitionPosition.class))).thenReturn(true);

    final ProcessContinuation result = doFn.processElement(
        element,
        restrictionTracker,
        outputReceiver,
        watermarkEstimator
    );

    assertEquals(ProcessContinuation.stop(), result);
    verify(restrictionTracker).tryClaim(PartitionPosition.queryChangeStream(record.getCommitTimestamp()));
    verify(restrictionTracker).tryClaim(PartitionPosition.waitForParentPartitions());
    verify(restrictionTracker).tryClaim(PartitionPosition.deletePartition());
    verify(restrictionTracker).tryClaim(PartitionPosition.done());
    verify(outputReceiver).output(record);
    verify(watermarkEstimator).setWatermark(new Instant(record.getCommitTimestamp().toSqlTimestamp().getTime()));
    verify(partitionMetadataDao).updateState(PARTITION_TOKEN, FINISHED);
    verify(partitionMetadataDao).delete(PARTITION_TOKEN);
  }

  // HeartbeatRecord
  // PartitionMetadata record
  // Calls Spanner
  // Read HeartbeatRecord
  //   - Updates restriction
  //   - Updates watermark
  //   - Does NOT send to output stream
  //   - Marks the current partition as finished
  //   - Waits for parent partitions
  //   - Deletes itself
  @Test
  public void testDoFnProcessesHeartbeatRecords() {
    final HeartbeatRecord record = new HeartbeatRecord(Timestamp.ofTimeSecondsAndNanos(20, 20));
    final Struct recordAsStruct = recordsToStruct(record);
    final ResultSet resultSet = mock(ResultSet.class);

    when(changeStreamDao.changeStreamQuery()).thenReturn(resultSet);
    when(resultSet.next()).thenReturn(true, false);
    when(resultSet.getCurrentRowAsStruct()).thenReturn(recordAsStruct);
    when(restrictionTracker.tryClaim(any(PartitionPosition.class))).thenReturn(true);

    final ProcessContinuation result = doFn.processElement(
        element,
        restrictionTracker,
        outputReceiver,
        watermarkEstimator
    );

    assertEquals(ProcessContinuation.stop(), result);
    verify(restrictionTracker).tryClaim(PartitionPosition.queryChangeStream(record.getTimestamp()));
    verify(restrictionTracker).tryClaim(PartitionPosition.waitForParentPartitions());
    verify(restrictionTracker).tryClaim(PartitionPosition.deletePartition());
    verify(restrictionTracker).tryClaim(PartitionPosition.done());
    verify(outputReceiver, never()).output(any(DataChangesRecord.class));
    verify(watermarkEstimator).setWatermark(new Instant(record.getTimestamp().toSqlTimestamp().getTime()));
    verify(partitionMetadataDao).updateState(PARTITION_TOKEN, FINISHED);
    verify(partitionMetadataDao).delete(PARTITION_TOKEN);
  }

  // ChildPartitionRecord - Partition Split, Initial partition
  // PartitionMetadata record
  // Calls Spanner
  // Read ChildPartitionRecord Split
  //   - Updates restriction
  //   - Updates watermark
  //   - Marks current partition as finished
  //   - Inserts the child partitions in the metadata table with state CREATED
  //   - Waits for child partitions to start being read
  //   - Waits for parent partitions to be deleted (no parents will be available here)
  //   - Deletes the current partition from the metadata table
  @Test
  public void testDoFnProcessSplitChildPartitionRecords() {
    final ChildPartitionsRecord record = new ChildPartitionsRecord(
        Timestamp.ofTimeSecondsAndNanos(20L, 20),
        "childRecordSequence",
        Collections.singletonList(new ChildPartition("childToken", PARTITION_TOKEN))
    );
    final Struct recordAsStruct = recordsToStruct(record);
    final ResultSet resultSet = mock(ResultSet.class);

    when(changeStreamDao.changeStreamQuery()).thenReturn(resultSet);
    when(resultSet.next()).thenReturn(true, false);
    when(resultSet.getCurrentRowAsStruct()).thenReturn(recordAsStruct);
    when(restrictionTracker.tryClaim(any(PartitionPosition.class))).thenReturn(true);
    when(partitionMetadataDao.countChildPartitionsInStates(PARTITION_TOKEN, Arrays.asList(SCHEDULED, FINISHED))).thenReturn(1L);
    when(partitionMetadataDao.countExistingParents(PARTITION_TOKEN)).thenReturn(0L);

    final ProcessContinuation result = doFn.processElement(
        element,
        restrictionTracker,
        outputReceiver,
        watermarkEstimator
    );

    assertEquals(ProcessContinuation.stop(), result);
    verify(restrictionTracker).tryClaim(PartitionPosition.queryChangeStream(record.getStartTimestamp()));
    verify(restrictionTracker).tryClaim(PartitionPosition.waitForChildPartitions());
    verify(restrictionTracker).tryClaim(PartitionPosition.waitForParentPartitions());
    verify(restrictionTracker).tryClaim(PartitionPosition.deletePartition());
    verify(outputReceiver, never()).output(any(DataChangesRecord.class));
    verify(watermarkEstimator).setWatermark(new Instant(record.getStartTimestamp().toSqlTimestamp().getTime()));
    verify(partitionMetadataDao).insert(Collections.singletonList(PartitionMetadata.newBuilder()
        .setPartitionToken("childToken")
        .setParentTokens(Collections.singletonList(PARTITION_TOKEN))
        .setStartTimestamp(Timestamp.ofTimeSecondsAndNanos(20L, 20))
        .setInclusiveStart(true)
        .setEndTimestamp(PARTITION_END_TIMESTAMP)
        .setInclusiveEnd(false)
        .setHeartbeatSeconds(PARTITION_HEARTBEAT_SECONDS)
        .setState(CREATED)
        .build()
    ));
    verify(partitionMetadataDao).updateState(PARTITION_TOKEN, FINISHED);
    verify(partitionMetadataDao).delete(PARTITION_TOKEN);
  }

  // ChildPartitionRecord - Partition Merge, At least one parent NOT finished
  // PartitionMetadata record
  // Calls Spanner
  // Read ChildPartitionRecord Merge
  //   - Updates restriction
  //   - Updates watermark
  //   - Checks if all parents finished (in this case no)
  //   - Marks current partition as finished
  //   - Inserts the child partition in the metadata table with state CREATED
  //   - Waits for child partition to start being read
  //   - Waits for parent partitions to be deleted
  //   - Deletes the current partition from the metadata table

  // No more records in the current partition
  // PartitionMetadata record
  // Calls Spanner
  // No values read
  //   - Marks the current partition as FINISHED
  //   - Partition is NOT DELETED from the metadata table
  // --------------------------

  // --------------------------
  // Sad Paths

  // Client library errors:
  //   1. RESOURCE_EXHAUSTED error on client library
  //   2. DEADLINE_EXCEEDED error on client library
  //   3. INTERNAL error on client library
  //   4. UNAVAILABLE error on client library
  //   5. UNKNOWN error on client library (transaction outcome unknown)
  //   6. ABORTED error on client library
  //   7. UNAUTHORIZED error on client library

  // Resuming the SDF from restriction
  //  - Test resume is successful
  //  - Test deduplication works

  // Metadata table
  //   - Table is deleted
  //   - Database is deleted
  //   - No permissions for the metadata table
  // --------------------------

  // TODO: Remove if not necessary
  public static class TestTransactionAnswer implements Answer<Object> {

    private final InTransactionContext transaction;

    public TestTransactionAnswer(InTransactionContext transaction) {
      this.transaction = transaction;
    }

    @Override
    public Object answer(InvocationOnMock invocation) {
      Function<InTransactionContext, Object> callable = invocation.getArgument(1);
      return callable.apply(transaction);
    }
  }
}
