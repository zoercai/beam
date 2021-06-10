package org.apache.beam.sdk.io.gcp.spanner.cdc.restriction;

import static org.apache.beam.sdk.io.gcp.spanner.cdc.restriction.PartitionMode.DELETE_PARTITION;
import static org.apache.beam.sdk.io.gcp.spanner.cdc.restriction.PartitionMode.DONE;
import static org.apache.beam.sdk.io.gcp.spanner.cdc.restriction.PartitionMode.PARTITION_QUERY;
import static org.apache.beam.sdk.io.gcp.spanner.cdc.restriction.PartitionMode.WAIT_FOR_CHILDREN;
import static org.apache.beam.sdk.io.gcp.spanner.cdc.restriction.PartitionMode.WAIT_FOR_PARENTS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.google.cloud.Timestamp;
import java.util.function.Consumer;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker.IsBounded;
import org.junit.Before;
import org.junit.Test;

public class PartitionRestrictionTrackerTest {

  private PartitionRestrictionTracker tracker;
  private PartitionRestriction restriction;

  @Before
  public void setUp() {
    final Timestamp startTimestamp = Timestamp.ofTimeSecondsAndNanos(10L, 20);
    restriction = new PartitionRestriction(startTimestamp);
    tracker = new PartitionRestrictionTracker(restriction);
  }

  @Test
  public void testTryClaimModeTransitions() {
    final TryClaimTestScenario runner = new TryClaimTestScenario(
        tracker, Timestamp.ofTimeSecondsAndNanos(10L, 20));

    runner.from(null).to(PARTITION_QUERY).assertSuccess();
    runner.from(null).to(WAIT_FOR_CHILDREN).assertError();
    runner.from(null).to(WAIT_FOR_PARENTS).assertError();
    runner.from(null).to(DELETE_PARTITION).assertError();
    runner.from(null).to(DONE).assertError();

    runner.from(PARTITION_QUERY).to(PARTITION_QUERY).assertSuccess();
    runner.from(PARTITION_QUERY).to(WAIT_FOR_CHILDREN).assertSuccess();
    runner.from(PARTITION_QUERY).to(WAIT_FOR_PARENTS).assertSuccess();
    runner.from(PARTITION_QUERY).to(DELETE_PARTITION).assertError();
    runner.from(PARTITION_QUERY).to(DONE).assertError();

    runner.from(WAIT_FOR_CHILDREN).to(PARTITION_QUERY).assertError();
    runner.from(WAIT_FOR_CHILDREN).to(WAIT_FOR_CHILDREN).assertError();
    runner.from(WAIT_FOR_CHILDREN).to(WAIT_FOR_PARENTS).assertSuccess();
    runner.from(WAIT_FOR_CHILDREN).to(DELETE_PARTITION).assertError();
    runner.from(WAIT_FOR_CHILDREN).to(DONE).assertError();

    runner.from(WAIT_FOR_PARENTS).to(PARTITION_QUERY).assertError();
    runner.from(WAIT_FOR_PARENTS).to(WAIT_FOR_CHILDREN).assertError();
    runner.from(WAIT_FOR_PARENTS).to(WAIT_FOR_PARENTS).assertError();
    runner.from(WAIT_FOR_PARENTS).to(DELETE_PARTITION).assertSuccess();
    runner.from(WAIT_FOR_PARENTS).to(DONE).assertError();

    runner.from(DELETE_PARTITION).to(PARTITION_QUERY).assertError();
    runner.from(DELETE_PARTITION).to(WAIT_FOR_CHILDREN).assertError();
    runner.from(DELETE_PARTITION).to(WAIT_FOR_PARENTS).assertError();
    runner.from(DELETE_PARTITION).to(DELETE_PARTITION).assertError();
    runner.from(DELETE_PARTITION).to(DONE).assertSuccess();

    runner.from(DONE).to(PARTITION_QUERY).assertError();
    runner.from(DONE).to(WAIT_FOR_CHILDREN).assertError();
    runner.from(DONE).to(WAIT_FOR_PARENTS).assertError();
    runner.from(DONE).to(DELETE_PARTITION).assertError();
    runner.from(DONE).to(DONE).assertError();
  }

  @Test(expected = IllegalArgumentException.class)
  public void testTryClaimTimestampInThePastThrowsAnError() {
    tracker.tryClaim(PartitionPosition.continueQuery(Timestamp.ofTimeSecondsAndNanos(10L, 19)));
  }

  @Test
  public void testTrySplit() {
    assertNull(tracker.trySplit(1D));
  }

  @Test
  public void testCheckDoneWithSomeTimestampAndDoneMode() {
    tracker.setLastClaimedTimestamp(Timestamp.ofTimeSecondsAndNanos(10L, 20));
    tracker.setLastClaimedMode(DONE);

    // No exceptions should be raised, which indicates success
    tracker.checkDone();
  }

  @Test(expected = IllegalStateException.class)
  public void testCheckDoneWithNullLastClaimedTimestamp() {
    tracker.checkDone();
  }

  @Test(expected = IllegalStateException.class)
  public void testCheckDoneWithNullLastClaimedMode() {
    tracker.setLastClaimedTimestamp(Timestamp.ofTimeSecondsAndNanos(10L, 20));

    tracker.checkDone();
  }

  @Test(expected = IllegalStateException.class)
  public void testCheckDoneWithLastClaimedModeAsPartitionQuery() {
    tracker.setLastClaimedTimestamp(Timestamp.ofTimeSecondsAndNanos(10L, 20));
    tracker.setLastClaimedMode(PARTITION_QUERY);

    tracker.checkDone();
  }

  @Test(expected = IllegalStateException.class)
  public void testCheckDoneWithLastClaimedModeAsWaitForChildren() {
    tracker.setLastClaimedTimestamp(Timestamp.ofTimeSecondsAndNanos(10L, 20));
    tracker.setLastClaimedMode(WAIT_FOR_CHILDREN);

    tracker.checkDone();
  }

  @Test(expected = IllegalStateException.class)
  public void testCheckDoneWithLastClaimedModeAsWaitForParents() {
    tracker.setLastClaimedTimestamp(Timestamp.ofTimeSecondsAndNanos(10L, 20));
    tracker.setLastClaimedMode(WAIT_FOR_PARENTS);

    tracker.checkDone();
  }

  @Test(expected = IllegalStateException.class)
  public void testCheckDoneWithLastClaimedModeAsDeletePartition() {
    tracker.setLastClaimedTimestamp(Timestamp.ofTimeSecondsAndNanos(10L, 20));
    tracker.setLastClaimedMode(DELETE_PARTITION);

    tracker.checkDone();
  }

  @Test
  public void testIsBounded() {
    assertEquals(IsBounded.UNBOUNDED, tracker.isBounded());
  }

  private static class TryClaimTestScenario {

    private final PartitionRestrictionTracker restrictionTracker;
    private final Timestamp timestamp;
    public PartitionMode fromMode;
    public PartitionMode toMode;

    private TryClaimTestScenario(
        PartitionRestrictionTracker restrictionTracker,
        Timestamp timestamp) {
      this.restrictionTracker = restrictionTracker;
      this.timestamp = timestamp;
    }

    public TryClaimTestScenario from(PartitionMode from) {
      this.fromMode = from;
      return this;
    }

    public TryClaimTestScenario to(PartitionMode to) {
      this.toMode = to;
      return this;
    }

    public void assertSuccess() {
      run(false);
    }

    public void assertError() {
      run(true);
    }

    private void run(boolean errorExpected) {
      restrictionTracker.setLastClaimedTimestamp(timestamp);
      restrictionTracker.setLastClaimedMode(fromMode);
      final Consumer<PartitionPosition> assertFn = errorExpected ?
          (PartitionPosition position) -> assertThrows(IllegalArgumentException.class,
              () -> restrictionTracker.tryClaim(position)) :
          (PartitionPosition position) -> assertTrue(restrictionTracker.tryClaim(position));

      switch (toMode) {
        case PARTITION_QUERY:
          assertFn.accept(PartitionPosition.continueQuery(timestamp));
          break;
        case WAIT_FOR_CHILDREN:
          assertFn.accept(PartitionPosition.waitForChildren());
          break;
        case WAIT_FOR_PARENTS:
          assertFn.accept(PartitionPosition.waitForParents());
          break;
        case DELETE_PARTITION:
          assertFn.accept(PartitionPosition.deletePartition());
          break;
        case DONE:
          assertFn.accept(PartitionPosition.done());
          break;
        default:
          throw new IllegalArgumentException("Unknown mode " + toMode);
      }
    }
  }
}
