package org.apache.beam.sdk.io.gcp.spanner.cdc.restriction;

import static org.apache.beam.sdk.io.gcp.spanner.cdc.restriction.PartitionMode.DELETE_PARTITION;
import static org.apache.beam.sdk.io.gcp.spanner.cdc.restriction.PartitionMode.DONE;
import static org.apache.beam.sdk.io.gcp.spanner.cdc.restriction.PartitionMode.FINISH_PARTITION;
import static org.apache.beam.sdk.io.gcp.spanner.cdc.restriction.PartitionMode.QUERY_CHANGE_STREAM;
import static org.apache.beam.sdk.io.gcp.spanner.cdc.restriction.PartitionMode.WAIT_FOR_CHILD_PARTITIONS;
import static org.apache.beam.sdk.io.gcp.spanner.cdc.restriction.PartitionMode.WAIT_FOR_PARENT_PARTITIONS;
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
  public void testPositionQueryChangeStream() {
    assertEquals(
        new PartitionPosition(timestamp, QUERY_CHANGE_STREAM),
        PartitionPosition.queryChangeStream(timestamp)
    );
  }

  @Test
  public void testPositionWaitForChildPartitions() {
    assertEquals(
        new PartitionPosition(WAIT_FOR_CHILD_PARTITIONS),
        PartitionPosition.waitForChildPartitions()
    );
  }

  @Test
  public void testPositionFinishPartition() {
    assertEquals(
        new PartitionPosition(FINISH_PARTITION),
        PartitionPosition.finishPartition()
    );
  }

  @Test
  public void testPositionWaitForParentPartitions() {
    assertEquals(
        new PartitionPosition(WAIT_FOR_PARENT_PARTITIONS),
        PartitionPosition.waitForParentPartitions()
    );
  }

  @Test
  public void testPositionDeletePartition() {
    assertEquals(
        new PartitionPosition(DELETE_PARTITION),
        PartitionPosition.deletePartition()
    );
  }

  @Test
  public void testPositionDone() {
    assertEquals(
        new PartitionPosition(DONE),
        PartitionPosition.done()
    );
  }
}
