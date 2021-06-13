package org.apache.beam.sdk.io.gcp.spanner.cdc.actions;

import static org.apache.beam.sdk.io.gcp.spanner.cdc.model.PartitionMetadata.State.CREATED;
import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cloud.Timestamp;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import org.apache.beam.sdk.io.gcp.spanner.cdc.dao.PartitionMetadataDao;
import org.apache.beam.sdk.io.gcp.spanner.cdc.model.ChildPartitionsRecord;
import org.apache.beam.sdk.io.gcp.spanner.cdc.model.ChildPartitionsRecord.ChildPartition;
import org.apache.beam.sdk.io.gcp.spanner.cdc.model.PartitionMetadata;
import org.apache.beam.sdk.io.gcp.spanner.cdc.restriction.PartitionPosition;
import org.apache.beam.sdk.io.gcp.spanner.cdc.restriction.PartitionRestriction;
import org.apache.beam.sdk.transforms.DoFn.ProcessContinuation;
import org.apache.beam.sdk.transforms.splittabledofn.ManualWatermarkEstimator;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.joda.time.Instant;
import org.junit.Before;
import org.junit.Test;

public class ChildPartitionsRecordActionTest {

  private PartitionMetadataDao dao;
  private WaitForChildPartitionsAction waitForChildPartitionsAction;
  private ChildPartitionsRecordAction action;
  private RestrictionTracker<PartitionRestriction, PartitionPosition> tracker;
  private ManualWatermarkEstimator<Instant> watermarkEstimator;

  @Before
  public void setUp() {
    dao = mock(PartitionMetadataDao.class);
    waitForChildPartitionsAction = mock(WaitForChildPartitionsAction.class);
    action = new ChildPartitionsRecordAction(dao, waitForChildPartitionsAction);
    tracker = mock(RestrictionTracker.class);
    watermarkEstimator = mock(ManualWatermarkEstimator.class);
  }

  @Test
  public void testRestrictionClaimedAndIsSplitCase() {
    final String partitionToken = "partitionToken";
    final long heartbeat = 30L;
    final Timestamp startTimestamp = Timestamp.ofTimeSecondsAndNanos(10L, 20);
    final Timestamp endTimestamp = Timestamp.ofTimeSecondsAndNanos(30L, 40);
    final PartitionMetadata partition = mock(PartitionMetadata.class);
    final ChildPartitionsRecord record = new ChildPartitionsRecord(
        startTimestamp,
        "recordSequence",
        Arrays.asList(
            new ChildPartition("childPartition1", partitionToken),
            new ChildPartition("childPartition2", partitionToken)
        )
    );
    when(partition.getEndTimestamp()).thenReturn(endTimestamp);
    when(partition.getHeartbeatSeconds()).thenReturn(heartbeat);
    when(tracker.tryClaim(PartitionPosition.queryChangeStream(startTimestamp))).thenReturn(true);
    when(waitForChildPartitionsAction.run(partition, tracker, 2)).thenReturn(Optional.empty());

    final Optional<ProcessContinuation> maybeContinuation = action
        .run(record, partition, tracker, watermarkEstimator);

    assertEquals(Optional.empty(), maybeContinuation);
    verify(watermarkEstimator).setWatermark(new Instant(startTimestamp.toSqlTimestamp().getTime()));
    verify(dao).insert(Arrays.asList(
        PartitionMetadata.newBuilder()
            .setPartitionToken("childPartition1")
            .setParentTokens(Collections.singletonList(partitionToken))
            .setStartTimestamp(startTimestamp)
            .setInclusiveStart(true)
            .setEndTimestamp(endTimestamp)
            .setInclusiveEnd(false)
            .setHeartbeatSeconds(heartbeat)
            .setState(CREATED)
            .build(),
        PartitionMetadata.newBuilder()
            .setPartitionToken("childPartition2")
            .setParentTokens(Collections.singletonList(partitionToken))
            .setStartTimestamp(startTimestamp)
            .setInclusiveStart(true)
            .setEndTimestamp(endTimestamp)
            .setInclusiveEnd(false)
            .setHeartbeatSeconds(heartbeat)
            .setState(CREATED)
            .build()
    ));
  }

  @Test
  public void testRestrictionNotClaimed() {
    final String partitionToken = "partitionToken";
    final Timestamp startTimestamp = Timestamp.ofTimeSecondsAndNanos(10L, 20);
    final PartitionMetadata partition = mock(PartitionMetadata.class);
    final ChildPartitionsRecord record = new ChildPartitionsRecord(
        startTimestamp,
        "recordSequence",
        Arrays.asList(
            new ChildPartition("childPartition1", partitionToken),
            new ChildPartition("childPartition2", partitionToken)
        )
    );
    when(tracker.tryClaim(PartitionPosition.queryChangeStream(startTimestamp))).thenReturn(false);

    final Optional<ProcessContinuation> maybeContinuation = action
        .run(record, partition, tracker, watermarkEstimator);

    assertEquals(Optional.of(ProcessContinuation.stop()), maybeContinuation);
    verify(watermarkEstimator, never()).setWatermark(any(Instant.class));
    verify(dao, never()).insert(any(List.class));
  }
}
