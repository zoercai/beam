package org.apache.beam.sdk.io.gcp.spanner.cdc.restriction;

import static org.apache.beam.sdk.io.gcp.spanner.cdc.restriction.PartitionMode.DONE;
import static org.apache.beam.sdk.io.gcp.spanner.cdc.restriction.PartitionMode.PARTITION_QUERY;
import static org.apache.beam.sdk.io.gcp.spanner.cdc.restriction.PartitionMode.WAIT_FOR_CHILDREN;
import static org.apache.beam.sdk.io.gcp.spanner.cdc.restriction.PartitionMode.WAIT_FOR_PARENTS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.google.cloud.Timestamp;
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

  // tryClaim PARTITION_QUERY position
  @Test
  public void testTryClaimPartitionQueryPositionFromNullPosition() {
    final boolean isClaimed = tracker.tryClaim(new PartitionPosition(
        Timestamp.ofTimeSecondsAndNanos(10L, 20),
        PARTITION_QUERY
    ));

    assertTrue(isClaimed);
  }

  @Test
  public void testTryClaimPartitionQueryPositionFromPartitionQueryPosition() {
    tracker.setLastClaimedTimestamp(restriction.getStartTimestamp());
    tracker.setLastClaimedMode(PARTITION_QUERY);

    final boolean isClaimed = tracker.tryClaim(new PartitionPosition(
        Timestamp.ofTimeSecondsAndNanos(10L, 20),
        PARTITION_QUERY
    ));

    assertTrue(isClaimed);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testTryClaimPartitionQueryPositionFromWaitForChildrenPosition() {
    tracker.setLastClaimedTimestamp(restriction.getStartTimestamp());
    tracker.setLastClaimedMode(WAIT_FOR_CHILDREN);

    tracker.tryClaim(new PartitionPosition(
        Timestamp.ofTimeSecondsAndNanos(10L, 20),
        PARTITION_QUERY
    ));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testTryClaimPartitionQueryPositionFromWaitForParentsPosition() {
    tracker.setLastClaimedTimestamp(restriction.getStartTimestamp());
    tracker.setLastClaimedMode(WAIT_FOR_PARENTS);

    tracker.tryClaim(new PartitionPosition(
        Timestamp.ofTimeSecondsAndNanos(10L, 20),
        PARTITION_QUERY
    ));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testTryClaimPartitionQueryPositionFromDonePosition() {
    tracker.setLastClaimedTimestamp(Timestamp.MAX_VALUE);
    tracker.setLastClaimedMode(DONE);

    tracker.tryClaim(new PartitionPosition(
        Timestamp.ofTimeSecondsAndNanos(10L, 20),
        PARTITION_QUERY
    ));
  }

  // tryClaim WAIT_FROM_CHILDREN position
  @Test(expected = IllegalArgumentException.class)
  public void testTryClaimWaitForChildrenPositionFromNullPosition() {
    tracker.tryClaim(new PartitionPosition(
        Timestamp.ofTimeSecondsAndNanos(10L, 20),
        WAIT_FOR_CHILDREN
    ));
  }

  @Test
  public void testTryClaimWaitForChildrenPositionFromPartitionQueryPosition() {
    tracker.setLastClaimedTimestamp(restriction.getStartTimestamp());
    tracker.setLastClaimedMode(PARTITION_QUERY);

    final boolean isClaimed = tracker.tryClaim(new PartitionPosition(
        Timestamp.ofTimeSecondsAndNanos(20L, 30),
        WAIT_FOR_CHILDREN
    ));

    assertTrue(isClaimed);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testTryClaimWaitForChildrenPositionFromWaitForChildrenPosition() {
    tracker.setLastClaimedTimestamp(restriction.getStartTimestamp());
    tracker.setLastClaimedMode(WAIT_FOR_CHILDREN);

    tracker.tryClaim(new PartitionPosition(
        Timestamp.ofTimeSecondsAndNanos(10L, 20),
        WAIT_FOR_CHILDREN
    ));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testTryClaimWaitForChildrenPositionFromWaitForParentsPosition() {
    tracker.setLastClaimedTimestamp(restriction.getStartTimestamp());
    tracker.setLastClaimedMode(WAIT_FOR_PARENTS);

    tracker.tryClaim(new PartitionPosition(
        Timestamp.ofTimeSecondsAndNanos(10L, 20),
        WAIT_FOR_CHILDREN
    ));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testTryClaimWaitForChildrenPositionFromDonePosition() {
    tracker.setLastClaimedTimestamp(Timestamp.MAX_VALUE);
    tracker.setLastClaimedMode(DONE);

    tracker.tryClaim(new PartitionPosition(
        Timestamp.ofTimeSecondsAndNanos(10L, 20),
        WAIT_FOR_CHILDREN
    ));
  }

  // tryClaim WAIT_FROM_PARENTS position
  @Test(expected = IllegalArgumentException.class)
  public void testTryClaimWaitForParentsPositionFromNullPosition() {
    tracker.tryClaim(new PartitionPosition(
        Timestamp.ofTimeSecondsAndNanos(10L, 20),
        WAIT_FOR_PARENTS
    ));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testTryClaimWaitForParentsPositionFromPartitionQueryPosition() {
    tracker.setLastClaimedTimestamp(restriction.getStartTimestamp());
    tracker.setLastClaimedMode(PARTITION_QUERY);

    tracker.tryClaim(new PartitionPosition(
        Timestamp.ofTimeSecondsAndNanos(10L, 20),
        WAIT_FOR_PARENTS
    ));
  }

  @Test
  public void testTryClaimWaitForParentsPositionFromWaitForChildrenPosition() {
    tracker.setLastClaimedTimestamp(restriction.getStartTimestamp());
    tracker.setLastClaimedMode(WAIT_FOR_CHILDREN);

    final boolean isClaimed = tracker.tryClaim(new PartitionPosition(
        Timestamp.ofTimeSecondsAndNanos(20L, 30),
        WAIT_FOR_PARENTS
    ));

    assertTrue(isClaimed);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testTryClaimWaitForParentsPositionFromWaitForParentsPosition() {
    tracker.setLastClaimedTimestamp(restriction.getStartTimestamp());
    tracker.setLastClaimedMode(WAIT_FOR_PARENTS);

    tracker.tryClaim(new PartitionPosition(
        Timestamp.ofTimeSecondsAndNanos(10L, 20),
        WAIT_FOR_PARENTS
    ));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testTryClaimWaitForParentsPositionFromDonePosition() {
    tracker.setLastClaimedTimestamp(Timestamp.MAX_VALUE);
    tracker.setLastClaimedMode(DONE);

    tracker.tryClaim(new PartitionPosition(
        Timestamp.ofTimeSecondsAndNanos(10L, 20),
        WAIT_FOR_PARENTS
    ));
  }

  // tryClaim DONE position
  @Test(expected = IllegalArgumentException.class)
  public void testTryClaimDonePositionFromNullPosition() {
    final boolean isClaimed = tracker.tryClaim(new PartitionPosition(
        Timestamp.MAX_VALUE,
        DONE
    ));

    assertTrue(isClaimed);
  }

  @Test
  public void testTryClaimDonePositionFromPartitionQueryPosition() {
    tracker.setLastClaimedTimestamp(restriction.getStartTimestamp());
    tracker.setLastClaimedMode(PARTITION_QUERY);

    final boolean isClaimed = tracker.tryClaim(new PartitionPosition(
        Timestamp.MAX_VALUE,
        DONE
    ));

    assertTrue(isClaimed);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testTryClaimDonePositionFromWaitForChildrenPosition() {
    tracker.setLastClaimedTimestamp(restriction.getStartTimestamp());
    tracker.setLastClaimedMode(WAIT_FOR_CHILDREN);

    tracker.tryClaim(new PartitionPosition(
        Timestamp.ofTimeSecondsAndNanos(20L, 30),
        DONE
    ));
  }

  @Test
  public void testTryClaimDonePositionFromWaitForParentsPosition() {
    tracker.setLastClaimedTimestamp(restriction.getStartTimestamp());
    tracker.setLastClaimedMode(WAIT_FOR_PARENTS);

    final boolean isClaimed = tracker.tryClaim(new PartitionPosition(
        Timestamp.MAX_VALUE,
        DONE
    ));

    assertTrue(isClaimed);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testTryClaimDonePositionFromDonePosition() {
    tracker.setLastClaimedTimestamp(restriction.getStartTimestamp());
    tracker.setLastClaimedMode(DONE);

    final boolean isClaimed = tracker.tryClaim(new PartitionPosition(
        Timestamp.MAX_VALUE,
        DONE
    ));

    assertTrue(isClaimed);
  }

  @Test
  public void testTrySplit() {
    assertNull(tracker.trySplit(1D));
  }

  @Test
  public void testCheckDoneWithMaxTimestampAndDoneMode() {
    tracker.setLastClaimedTimestamp(Timestamp.MAX_VALUE);
    tracker.setLastClaimedMode(DONE);

    // No exceptions should be raised, which indicates success
    tracker.checkDone();
  }

  @Test(expected = IllegalStateException.class)
  public void testCheckDoneWithNullLastClaimedTimestamp() {
    tracker.checkDone();
  }

  @Test(expected = IllegalStateException.class)
  public void testCheckDoneWithLastClaimedTimestampNotEqualToMaxTimestamp() {
    tracker.setLastClaimedTimestamp(Timestamp.ofTimeSecondsAndNanos(10L, 20));
    tracker.setLastClaimedMode(DONE);

    tracker.checkDone();
  }

  @Test(expected = IllegalStateException.class)
  public void testCheckDoneWithNullLastClaimedMode() {
    tracker.checkDone();
  }

  @Test(expected = IllegalStateException.class)
  public void testCheckDoneWithLastClaimedModeAsPartitionQuery() {
    tracker.setLastClaimedTimestamp(Timestamp.MAX_VALUE);
    tracker.setLastClaimedMode(PARTITION_QUERY);

    tracker.checkDone();
  }

  @Test
  public void testIsBounded() {
    assertEquals(IsBounded.UNBOUNDED, tracker.isBounded());
  }
}
