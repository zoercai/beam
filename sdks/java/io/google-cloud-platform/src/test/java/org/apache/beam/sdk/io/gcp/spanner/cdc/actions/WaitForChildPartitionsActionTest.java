package org.apache.beam.sdk.io.gcp.spanner.cdc.actions;

import static org.apache.beam.sdk.io.gcp.spanner.cdc.model.PartitionMetadata.State.FINISHED;
import static org.apache.beam.sdk.io.gcp.spanner.cdc.model.PartitionMetadata.State.SCHEDULED;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Optional;
import org.apache.beam.sdk.io.gcp.spanner.cdc.dao.PartitionMetadataDao;
import org.apache.beam.sdk.io.gcp.spanner.cdc.model.PartitionMetadata;
import org.apache.beam.sdk.io.gcp.spanner.cdc.restriction.PartitionPosition;
import org.apache.beam.sdk.io.gcp.spanner.cdc.restriction.PartitionRestriction;
import org.apache.beam.sdk.transforms.DoFn.ProcessContinuation;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.joda.time.Duration;
import org.junit.Before;
import org.junit.Test;

public class WaitForChildPartitionsActionTest {

  private WaitForChildPartitionsAction action;
  private PartitionMetadataDao dao;
  private Duration resumeDuration;
  private RestrictionTracker<PartitionRestriction, PartitionPosition> tracker;

  @Before
  public void setUp() {
    dao = mock(PartitionMetadataDao.class);
    resumeDuration = Duration.millis(100L);
    action = new WaitForChildPartitionsAction(dao, resumeDuration);
    tracker = mock(RestrictionTracker.class);
  }

  @Test
  public void testRestrictionClaimedAndChildrenFinished() {
    final String partitionToken = "partitionToken";
    final PartitionMetadata partition = mock(PartitionMetadata.class);
    when(tracker.tryClaim(PartitionPosition.waitForChildPartitions())).thenReturn(true);
    when(partition.getPartitionToken()).thenReturn(partitionToken);
    when(dao.countChildPartitionsInStates(partitionToken, Arrays.asList(SCHEDULED, FINISHED))).thenReturn(10L);

    final Optional<ProcessContinuation> maybeContinuation = action.run(partition, tracker, 10);

    assertEquals(Optional.empty(), maybeContinuation);
  }

  @Test
  public void testRestrictionClaimedAndChildrenNotFinished() {
    final String partitionToken = "partitionToken";
    final PartitionMetadata partition = mock(PartitionMetadata.class);
    when(tracker.tryClaim(PartitionPosition.waitForChildPartitions())).thenReturn(true);
    when(partition.getPartitionToken()).thenReturn(partitionToken);
    when(dao.countChildPartitionsInStates(partitionToken, Arrays.asList(SCHEDULED, FINISHED))).thenReturn(9L);

    final Optional<ProcessContinuation> maybeContinuation = action.run(partition, tracker, 10);

    assertEquals(Optional.of(ProcessContinuation.resume().withResumeDelay(resumeDuration)), maybeContinuation);
  }

  @Test
  public void testRestrictionNotClaimed() {
    final PartitionMetadata partition = mock(PartitionMetadata.class);
    when(tracker.tryClaim(PartitionPosition.waitForChildPartitions())).thenReturn(false);

    final Optional<ProcessContinuation> maybeContinuation = action.run(partition, tracker, 10);

    assertEquals(Optional.of(ProcessContinuation.stop()), maybeContinuation);
  }
}
