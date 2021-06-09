package org.apache.beam.sdk.io.gcp.spanner.cdc.restriction;

import static org.apache.beam.sdk.io.gcp.spanner.cdc.restriction.PartitionMode.DONE;
import static org.apache.beam.sdk.io.gcp.spanner.cdc.restriction.PartitionMode.PARTITION_QUERY;
import static org.apache.beam.sdk.io.gcp.spanner.cdc.restriction.PartitionMode.WAIT_FOR_CHILDREN;
import static org.apache.beam.sdk.io.gcp.spanner.cdc.restriction.PartitionMode.WAIT_FOR_PARENTS;
import static org.junit.Assert.*;

import com.google.cloud.Timestamp;
import org.junit.Before;
import org.junit.Test;

public class PartitionPositionTest {

  private Timestamp timestamp;

  @Before
  public void setUp() {
    timestamp = Timestamp.now();
  }

  @Test
  public void testPositionContinueQuery() {
    assertEquals(
        new PartitionPosition(timestamp, PARTITION_QUERY),
        PartitionPosition.continueQuery(timestamp)
    );
  }

  @Test
  public void testPositionWaitForChildren() {
    assertEquals(
        new PartitionPosition(timestamp, WAIT_FOR_CHILDREN),
        PartitionPosition.waitForChildren(timestamp)
    );
  }

  @Test
  public void testPositionWaitForParents() {
    assertEquals(
        new PartitionPosition(timestamp, WAIT_FOR_PARENTS),
        PartitionPosition.waitForParents(timestamp)
    );
  }

  @Test
  public void testPositionDone() {
    assertEquals(
        new PartitionPosition(Timestamp.MAX_VALUE, DONE),
        PartitionPosition.done()
    );
  }
}
