package org.apache.beam.sdk.io.gcp.spanner.cdc;

import static org.apache.beam.sdk.io.gcp.spanner.cdc.model.PartitionMetadata.State.SCHEDULED;
import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.mockStatic;

import com.google.cloud.Timestamp;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Struct;
import java.util.Arrays;
import java.util.Collections;
import java.util.Optional;
import org.apache.beam.sdk.io.gcp.spanner.SpannerConfig;
import org.apache.beam.sdk.io.gcp.spanner.cdc.actions.ActionFactory;
import org.apache.beam.sdk.io.gcp.spanner.cdc.actions.ChildPartitionsRecordAction;
import org.apache.beam.sdk.io.gcp.spanner.cdc.actions.DataChangesRecordAction;
import org.apache.beam.sdk.io.gcp.spanner.cdc.actions.DeletePartitionAction;
import org.apache.beam.sdk.io.gcp.spanner.cdc.actions.FinishPartitionAction;
import org.apache.beam.sdk.io.gcp.spanner.cdc.actions.HeartbeatRecordAction;
import org.apache.beam.sdk.io.gcp.spanner.cdc.actions.WaitForChildPartitionsAction;
import org.apache.beam.sdk.io.gcp.spanner.cdc.actions.WaitForParentPartitionsAction;
import org.apache.beam.sdk.io.gcp.spanner.cdc.dao.ChangeStreamDao;
import org.apache.beam.sdk.io.gcp.spanner.cdc.dao.DaoFactory;
import org.apache.beam.sdk.io.gcp.spanner.cdc.dao.PartitionMetadataDao;
import org.apache.beam.sdk.io.gcp.spanner.cdc.mapper.ChangeStreamRecordMapper;
import org.apache.beam.sdk.io.gcp.spanner.cdc.mapper.MapperFactory;
import org.apache.beam.sdk.io.gcp.spanner.cdc.model.ChildPartitionsRecord;
import org.apache.beam.sdk.io.gcp.spanner.cdc.model.DataChangesRecord;
import org.apache.beam.sdk.io.gcp.spanner.cdc.model.HeartbeatRecord;
import org.apache.beam.sdk.io.gcp.spanner.cdc.model.PartitionMetadata;
import org.apache.beam.sdk.io.gcp.spanner.cdc.restriction.PartitionPosition;
import org.apache.beam.sdk.io.gcp.spanner.cdc.restriction.PartitionRestriction;
import org.apache.beam.sdk.transforms.DoFn.OutputReceiver;
import org.apache.beam.sdk.transforms.DoFn.ProcessContinuation;
import org.apache.beam.sdk.transforms.splittabledofn.ManualWatermarkEstimator;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

@RunWith(PowerMockRunner.class)
@PrepareForTest({DaoFactory.class, ActionFactory.class, MapperFactory.class})
public class ReadChangeStreamPartitionDoFnTest {

  private static final String PARTITION_TOKEN = "partitionToken";
  private static final Timestamp PARTITION_START_TIMESTAMP = Timestamp.ofTimeSecondsAndNanos(10, 20);
  private static final Timestamp PARTITION_END_TIMESTAMP = Timestamp.ofTimeSecondsAndNanos(30, 40);
  public static final long PARTITION_HEARTBEAT_SECONDS = 30L;

  private ChangeStreamDao changeStreamDao;
  private ReadChangeStreamPartitionDoFn doFn;
  private PartitionMetadata partition;
  private RestrictionTracker<PartitionRestriction, PartitionPosition> restrictionTracker;
  private OutputReceiver<DataChangesRecord> outputReceiver;
  private ManualWatermarkEstimator<Instant> watermarkEstimator;
  private WaitForChildPartitionsAction waitForChildPartitionsAction;
  private FinishPartitionAction finishPartitionAction;
  private WaitForParentPartitionsAction waitForParentPartitionsAction;
  private DeletePartitionAction deletePartitionAction;
  private DataChangesRecordAction dataChangesRecordAction;
  private HeartbeatRecordAction heartbeatRecordAction;
  private ChildPartitionsRecordAction childPartitionsRecordAction;
  private ChangeStreamRecordMapper changeStreamRecordMapper;

  @Before
  public void setUp() {
    final SpannerConfig spannerConfig = SpannerConfig
        .create()
        .withProjectId("project-id")
        .withInstanceId("instance-id")
        .withDatabaseId("database-id");
    mockStatic(DaoFactory.class);
    mockStatic(ActionFactory.class);
    mockStatic(MapperFactory.class);

    final Duration resumeDuration = Duration.millis(100);
    final PartitionMetadataDao partitionMetadataDao = mock(PartitionMetadataDao.class);
    changeStreamDao = mock(ChangeStreamDao.class);
    waitForChildPartitionsAction = mock(WaitForChildPartitionsAction.class);
    finishPartitionAction = mock(FinishPartitionAction.class);
    waitForParentPartitionsAction = mock(WaitForParentPartitionsAction.class);
    deletePartitionAction = mock(DeletePartitionAction.class);
    dataChangesRecordAction = mock(DataChangesRecordAction.class);
    heartbeatRecordAction = mock(HeartbeatRecordAction.class);
    childPartitionsRecordAction = mock(ChildPartitionsRecordAction.class);
    changeStreamRecordMapper = mock(ChangeStreamRecordMapper.class);

    doFn = new ReadChangeStreamPartitionDoFn(spannerConfig);

    partition = PartitionMetadata.newBuilder()
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
    when(ActionFactory.waitForChildPartitionsAction(partitionMetadataDao, resumeDuration)).thenReturn(waitForChildPartitionsAction);
    when(ActionFactory.finishPartitionAction(partitionMetadataDao)).thenReturn(finishPartitionAction);
    when(ActionFactory.waitForParentPartitionsAction(partitionMetadataDao, resumeDuration)).thenReturn(waitForParentPartitionsAction);
    when(ActionFactory.deletePartitionAction(partitionMetadataDao)).thenReturn(deletePartitionAction);
    when(ActionFactory.dataChangesRecordAction()).thenReturn(dataChangesRecordAction);
    when(ActionFactory.heartbeatRecordAction()).thenReturn(heartbeatRecordAction);
    when(ActionFactory.childPartitionsRecordAction(partitionMetadataDao, waitForChildPartitionsAction)).thenReturn(childPartitionsRecordAction);
    when(MapperFactory.changeStreamRecordMapper()).thenReturn(changeStreamRecordMapper);

    doFn.setup();
  }

  @Test
  public void testDataChangeRecord() {
    final Struct rowAsStruct = mock(Struct.class);
    final ResultSet resultSet = mock(ResultSet.class);
    final DataChangesRecord record1 = mock(DataChangesRecord.class);
    final DataChangesRecord record2 = mock(DataChangesRecord.class);
    when(changeStreamDao.changeStreamQuery()).thenReturn(resultSet);
    when(resultSet.next()).thenReturn(true);
    when(resultSet.getCurrentRowAsStruct()).thenReturn(rowAsStruct);
    when(changeStreamRecordMapper.toChangeStreamRecords(PARTITION_TOKEN, rowAsStruct))
        .thenReturn(Arrays.asList(record1, record2));
    when(dataChangesRecordAction.run(record1, restrictionTracker, outputReceiver, watermarkEstimator))
        .thenReturn(Optional.empty());
    when(dataChangesRecordAction.run(record2, restrictionTracker, outputReceiver, watermarkEstimator))
        .thenReturn(Optional.of(ProcessContinuation.stop()));

    final ProcessContinuation result = doFn
        .processElement(partition, restrictionTracker, outputReceiver, watermarkEstimator);

    assertEquals(ProcessContinuation.stop(), result);
    verify(dataChangesRecordAction).run(record1, restrictionTracker, outputReceiver, watermarkEstimator);
    verify(dataChangesRecordAction).run(record2, restrictionTracker, outputReceiver, watermarkEstimator);

    verify(heartbeatRecordAction, never()).run(any(), any(), any());
    verify(childPartitionsRecordAction, never()).run(any(), any(), any(), any());
    verify(waitForChildPartitionsAction, never()).run(any(), any(), anyLong());
    verify(finishPartitionAction, never()).run(any(), any());
    verify(waitForParentPartitionsAction, never()).run(any(), any());
    verify(deletePartitionAction, never()).run(any(), any());
    verify(restrictionTracker, never()).tryClaim(any());
  }

  @Test
  public void testHeartbeatRecord() {
    final Struct rowAsStruct = mock(Struct.class);
    final ResultSet resultSet = mock(ResultSet.class);
    final HeartbeatRecord record1 = mock(HeartbeatRecord.class);
    final HeartbeatRecord record2 = mock(HeartbeatRecord.class);
    when(changeStreamDao.changeStreamQuery()).thenReturn(resultSet);
    when(resultSet.next()).thenReturn(true);
    when(resultSet.getCurrentRowAsStruct()).thenReturn(rowAsStruct);
    when(changeStreamRecordMapper.toChangeStreamRecords(PARTITION_TOKEN, rowAsStruct))
        .thenReturn(Arrays.asList(record1, record2));
    when(heartbeatRecordAction.run(record1, restrictionTracker, watermarkEstimator))
        .thenReturn(Optional.empty());
    when(heartbeatRecordAction.run(record2, restrictionTracker, watermarkEstimator))
        .thenReturn(Optional.of(ProcessContinuation.stop()));

    final ProcessContinuation result = doFn
        .processElement(partition, restrictionTracker, outputReceiver, watermarkEstimator);

    assertEquals(ProcessContinuation.stop(), result);
    verify(heartbeatRecordAction).run(record1, restrictionTracker, watermarkEstimator);
    verify(heartbeatRecordAction).run(record2, restrictionTracker, watermarkEstimator);

    verify(dataChangesRecordAction, never()).run(any(), any(), any(), any());
    verify(childPartitionsRecordAction, never()).run(any(), any(), any(), any());
    verify(waitForChildPartitionsAction, never()).run(any(), any(), anyLong());
    verify(finishPartitionAction, never()).run(any(), any());
    verify(waitForParentPartitionsAction, never()).run(any(), any());
    verify(deletePartitionAction, never()).run(any(), any());
    verify(restrictionTracker, never()).tryClaim(any());
  }

  @Test
  public void testChildPartitionsRecord() {
    final Struct rowAsStruct = mock(Struct.class);
    final ResultSet resultSet = mock(ResultSet.class);
    final ChildPartitionsRecord record1 = mock(ChildPartitionsRecord.class);
    final ChildPartitionsRecord record2 = mock(ChildPartitionsRecord.class);
    when(changeStreamDao.changeStreamQuery()).thenReturn(resultSet);
    when(resultSet.next()).thenReturn(true);
    when(resultSet.getCurrentRowAsStruct()).thenReturn(rowAsStruct);
    when(changeStreamRecordMapper.toChangeStreamRecords(PARTITION_TOKEN, rowAsStruct))
        .thenReturn(Arrays.asList(record1, record2));
    when(childPartitionsRecordAction.run(record1, partition, restrictionTracker, watermarkEstimator))
        .thenReturn(Optional.empty());
    when(childPartitionsRecordAction.run(record2, partition, restrictionTracker, watermarkEstimator))
        .thenReturn(Optional.of(ProcessContinuation.resume()));

    final ProcessContinuation result = doFn
        .processElement(partition, restrictionTracker, outputReceiver, watermarkEstimator);

    assertEquals(ProcessContinuation.resume(), result);
    verify(childPartitionsRecordAction).run(record1, partition, restrictionTracker, watermarkEstimator);
    verify(childPartitionsRecordAction).run(record2, partition, restrictionTracker, watermarkEstimator);

    verify(dataChangesRecordAction, never()).run(any(), any(), any(), any());
    verify(heartbeatRecordAction, never()).run(any(), any(), any());
    verify(waitForChildPartitionsAction, never()).run(any(), any(), anyLong());
    verify(finishPartitionAction, never()).run(any(), any());
    verify(waitForParentPartitionsAction, never()).run(any(), any());
    verify(deletePartitionAction, never()).run(any(), any());
    verify(restrictionTracker, never()).tryClaim(any());
  }

  @Test
  public void testStreamFinished() {
    final ResultSet resultSet = mock(ResultSet.class);
    when(changeStreamDao.changeStreamQuery()).thenReturn(resultSet);
    when(resultSet.next()).thenReturn(false);

    final ProcessContinuation result = doFn
        .processElement(partition, restrictionTracker, outputReceiver, watermarkEstimator);

    assertEquals(ProcessContinuation.stop(), result);
    verify(finishPartitionAction).run(partition, restrictionTracker);
    verify(waitForParentPartitionsAction).run(partition, restrictionTracker);
    verify(deletePartitionAction).run(partition, restrictionTracker);
    verify(restrictionTracker).tryClaim(PartitionPosition.done());

    verify(dataChangesRecordAction, never()).run(any(), any(), any(), any());
    verify(heartbeatRecordAction, never()).run(any(), any(), any());
    verify(childPartitionsRecordAction, never()).run(any(), any(), any(), any());
    verify(waitForChildPartitionsAction, never()).run(any(), any(), anyLong());
  }

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
