package org.apache.beam.sdk.io.gcp.spanner.cdc;

import static org.apache.beam.sdk.io.gcp.spanner.cdc.TestStructMapper.recordsToStruct;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.mockStatic;

import com.google.cloud.Timestamp;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.TransactionContext;
import com.google.common.collect.ImmutableMap;
import java.util.Arrays;
import java.util.Collections;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.io.gcp.spanner.SpannerAccessor;
import org.apache.beam.sdk.io.gcp.spanner.SpannerConfig;
import org.apache.beam.sdk.io.gcp.spanner.cdc.dao.PartitionMetadataDao;
import org.apache.beam.sdk.io.gcp.spanner.cdc.model.ChildPartitionsRecord;
import org.apache.beam.sdk.io.gcp.spanner.cdc.model.ChildPartitionsRecord.ChildPartition;
import org.apache.beam.sdk.io.gcp.spanner.cdc.model.ColumnType;
import org.apache.beam.sdk.io.gcp.spanner.cdc.model.DataChangesRecord;
import org.apache.beam.sdk.io.gcp.spanner.cdc.model.HeartbeatRecord;
import org.apache.beam.sdk.io.gcp.spanner.cdc.model.Mod;
import org.apache.beam.sdk.io.gcp.spanner.cdc.model.ModType;
import org.apache.beam.sdk.io.gcp.spanner.cdc.model.PartitionMetadata;
import org.apache.beam.sdk.io.gcp.spanner.cdc.model.PartitionMetadata.State;
import org.apache.beam.sdk.io.gcp.spanner.cdc.model.TypeCode;
import org.apache.beam.sdk.io.gcp.spanner.cdc.model.ValueCaptureType;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

// FIXME: We should assert on the restriction
// FIXME: We should assert on the watermark
@RunWith(PowerMockRunner.class)
@PrepareForTest(SpannerAccessor.class)
public class ReadChangeStreamPartitionDoFnTest {

  private static final String PARTITION_TOKEN = "partitionToken";
  private static final Timestamp PARTITION_START_TIMESTAMP = Timestamp.ofTimeSecondsAndNanos(10, 20);
  private static final Timestamp PARTITION_END_TIMESTAMP = Timestamp.ofTimeSecondsAndNanos(30, 40);
  public static final long PARTITION_HEARTBEAT_SECONDS = 30L;

  private String partitionMetadataTableName;
  private DatabaseClient databaseClient;
  private ReadChangeStreamPartitionDoFn doFn;
  private TestStream<PartitionMetadata> testStream;

  @Rule
  public TestPipeline pipeline = TestPipeline.create();

  @Before
  public void setUp() {
    final SpannerConfig spannerConfig = SpannerConfig
        .create()
        .withProjectId("project-id")
        .withInstanceId("instance-id")
        .withDatabaseId("database-id");
    final SpannerAccessor spannerAccessor = mock(SpannerAccessor.class);
    mockStatic(SpannerAccessor.class);

    partitionMetadataTableName = "table-id";
    databaseClient = mock(DatabaseClient.class, RETURNS_DEEP_STUBS);
    doFn = new ReadChangeStreamPartitionDoFn(spannerConfig, partitionMetadataTableName);
    testStream = TestStream
        .create(AvroCoder.of(PartitionMetadata.class))
        .addElements(PartitionMetadata.newBuilder()
            .setPartitionToken(PARTITION_TOKEN)
            .setParentTokens(Collections.singletonList("parentToken"))
            .setStartTimestamp(PARTITION_START_TIMESTAMP)
            .setInclusiveStart(true)
            .setEndTimestamp(PARTITION_END_TIMESTAMP)
            .setInclusiveEnd(false)
            .setHeartbeatSeconds(PARTITION_HEARTBEAT_SECONDS)
            .setState(State.SCHEDULED)
            .setCreatedAt(Timestamp.MAX_VALUE)
            .setUpdatedAt(Timestamp.MAX_VALUE)
            .build()
        )
        .advanceWatermarkToInfinity();

    when(SpannerAccessor.getOrCreate(spannerConfig)).thenReturn(spannerAccessor);
    when(spannerAccessor.getDatabaseClient()).thenReturn(databaseClient);
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

    when(databaseClient.singleUse().executeQuery(any(Statement.class))).thenReturn(resultSet);
    when(resultSet.next()).thenReturn(true, false);
    when(resultSet.getCurrentRowAsStruct()).thenReturn(recordAsStruct);

    final PCollection<DataChangesRecord> result = pipeline
        .apply(testStream)
        .apply(ParDo.of(doFn));

    PAssert.that(result).containsInAnyOrder(record);
    pipeline.run();
  }

  // HeartbeatRecord
  // PartitionMetadata record
  // Calls Spanner
  // Read HeartbeatRecord
  //   - Updates restriction
  //   - Updates watermark
  //   - Does NOT send to output stream
  @Test
  public void testDoFnProcessesHeartbeatRecords() {
    final HeartbeatRecord heartbeatRecord = new HeartbeatRecord(
        Timestamp.ofTimeSecondsAndNanos(20, 20));
    final Struct recordAsStruct = recordsToStruct(heartbeatRecord);
    final ResultSet resultSet = mock(ResultSet.class);

    when(databaseClient.singleUse().executeQuery(any(Statement.class))).thenReturn(resultSet);
    when(resultSet.next()).thenReturn(true, false);
    when(resultSet.getCurrentRowAsStruct()).thenReturn(recordAsStruct);

    final PCollection<DataChangesRecord> result = pipeline
        .apply(testStream)
        .apply(ParDo.of(doFn));

    PAssert.that(result).empty();
    pipeline.run();
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
    final TransactionContext transaction = mock(TransactionContext.class);
    final TransactionRunnerStub transactionRunner = new TransactionRunnerStub(transaction);

    when(databaseClient.singleUse().executeQuery(any(Statement.class))).thenReturn(resultSet);
    when(databaseClient.readWriteTransaction()).thenReturn(transactionRunner);
    when(resultSet.next()).thenReturn(true, false);
    when(resultSet.getCurrentRowAsStruct()).thenReturn(recordAsStruct);

    final PCollection<DataChangesRecord> result = pipeline
        .apply(testStream)
        .apply(ParDo.of(doFn));

    PAssert.that(result).empty();
    pipeline.run();

    verify(transaction).buffer(Arrays.asList(
        Mutation
            .newInsertBuilder(partitionMetadataTableName)
            .set(PartitionMetadataDao.COLUMN_PARTITION_TOKEN)
            .to("childToken")
            // FIXME: This should be a list of parents
            .set(PartitionMetadataDao.COLUMN_PARENT_TOKEN)
            .to(PARTITION_TOKEN)
            .set(PartitionMetadataDao.COLUMN_START_TIMESTAMP)
            .to(Timestamp.ofTimeSecondsAndNanos(20L, 20))
            .set(PartitionMetadataDao.COLUMN_INCLUSIVE_START)
            .to(true)
            .set(PartitionMetadataDao.COLUMN_END_TIMESTAMP)
            .to(PARTITION_END_TIMESTAMP)
            .set(PartitionMetadataDao.COLUMN_INCLUSIVE_END)
            .to(false)
            .set(PartitionMetadataDao.COLUMN_HEARTBEAT_SECONDS)
            .to(PARTITION_HEARTBEAT_SECONDS)
            .set(PartitionMetadataDao.COLUMN_STATE)
            .to(PartitionMetadata.State.CREATED.toString())
            .build(),
        Mutation
            .newUpdateBuilder(partitionMetadataTableName)
            .set(PartitionMetadataDao.COLUMN_PARTITION_TOKEN)
            .to(PARTITION_TOKEN)
            .set(PartitionMetadataDao.COLUMN_STATE)
            .to(PartitionMetadata.State.FINISHED.toString())
            .build()
    ));
  }

  // ChildPartitionRecord - Partition Split
  // PartitionMetadata record
  // Calls Spanner
  // Read ChildPartitionRecord Split
  //   - Updates restriction
  //   - Updates watermark
  //   - Marks current partition as finished
  //   - Inserts the child partitions in the metadata table with state CREATED
  //   - Waits for child partitions to start being read
  //   - Waits for parent partitions to be deleted
  //   - Deletes the current partition from the metadata table

  // ChildPartitionRecord - Partition Merge, All Parents Finished
  // PartitionMetadata record
  // Calls Spanner
  // Read ChildPartitionRecord Merge
  //   - Updates restriction
  //   - Updates watermark
  //   - Checks if all parents finished (in this case yes)
  //   - Marks current partition as finished
  //   - Inserts the child partition in the metadata table with state CREATED
  //   - Waits for child partition to start being read
  //   - Waits for parent partitions to be deleted
  //   - Deletes the current partition from the metadata table

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

}
