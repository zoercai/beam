package org.apache.beam.sdk.io.gcp.spanner.changestreams.generator;

import com.google.cloud.Timestamp;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.ChangeStreamRecord;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.ChangeStreamRecordMetadata;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.ChildPartitionsRecord;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.HeartbeatRecord;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableSet;
import org.joda.time.Instant;

public class Generator {

  private static final Random RANDOM = new Random();

  // Configurations
  private int numStartingPartitions = 1;
  private long millisecondsToRun = 10000;
  private int maxChangesPerTransactionPerPartition = 2;
  private final List<Partition> partitions = new ArrayList<>();
  private TestStream.Builder<ChangeStreamRecord> builder;

  private long currentWaterMark = 0;
  // The next primary key ID, this is the total number of primary keys that ever existed
  private int nextPrimaryKey = 0;
  // The next partition ID, this is the total number of partition IDs that ever existed
  private int nextPartitionId = 0;
  // The next transaction ID, this is the total number of transactions
  private long nextTransactionId = 0;

  public Generator() {
  }

  public Generator(int numStartingPartitions, long millisecondsToRun) {
    Preconditions.checkArgument(
        numStartingPartitions > 0, "Must start with at least one partition");
    this.numStartingPartitions = numStartingPartitions;
    this.millisecondsToRun = millisecondsToRun;
  }

  public TestStream<ChangeStreamRecord> generate() throws InterruptedException {
    long startTime = System.currentTimeMillis();
    long endTime = startTime + millisecondsToRun;
    generatePartitions();
    this.builder =
        TestStream.create(AvroCoder.of(ChangeStreamRecord.class))
            .advanceWatermarkTo(new Instant(startTime));
    // Initial query returns all partitions
    for (Partition partition : this.partitions) {
      final Timestamp now = Timestamp.now();
      this.builder.addElements(
          new ChildPartitionsRecord(
              now,
              "0",
              ImmutableList.of(),
              ChangeStreamRecordMetadata.newBuilder()
                  .withPartitionToken(partition.getPartitionToken())
                  .withRecordTimestamp(now)
                  .build()
          ));
    }
    while (System.currentTimeMillis() < endTime) {
      generateRecords();
      maybeAdvanceWaterMark();
      TimeUnit.MILLISECONDS.sleep(50);
      generateHeartbeatRecords();
    }
    return this.builder.advanceWatermarkToInfinity();
  }

  private void generateHeartbeatRecords() {
    for (Partition partition : this.partitions) {
      final Timestamp now = Timestamp.now();
      this.builder =
          this.builder.addElements(
              new HeartbeatRecord(
                  now, ChangeStreamRecordMetadata.newBuilder()
                  .withPartitionToken(partition.getPartitionToken())
                  .withRecordTimestamp(now)
                  .build()));
    }
  }

  private void generatePartitions() {
    for (int i = 0; i < this.numStartingPartitions; i++) {
      this.partitions.add(new Partition("Partition-" + this.nextPartitionId++));
    }
  }

  private void generateRecords() {
    // Randomly decide whether to produce a data change record or a partition record
    Action actionType = Action.values()[RANDOM.nextInt(Action.values().length)];
    switch (actionType) {
      case NONE:
        // Do nothing
        break;
      case RECORD:
        generateTransactionRecord();
        break;
      case PARTITION:
        generatePartitionRecord();
        break;
    }
  }

  private void generateTransactionRecord() {
    for (Partition partition : partitions) {
      // Randomly generating record sequence
      long recordSequence = RANDOM.nextInt(maxChangesPerTransactionPerPartition * 2);
      // TODO mix up the records so that they're not in chronological order?
      for (int j = 0; j < RANDOM.nextInt(maxChangesPerTransactionPerPartition); j++) {
        RecordAction recordAction =
            RecordAction.values()[RANDOM.nextInt(RecordAction.values().length)];
        switch (recordAction) {
          case RECORD_NEW:
            addRecord(partition, recordSequence);
            break;
          case RECORD_DELETE:
            deleteRecord(partition, recordSequence);
            break;
          case RECORD_UPDATE:
            updateRecord(partition, recordSequence);
            break;
          case NONE:
            // do nothing
            break;
        }
        recordSequence = RANDOM.nextInt(maxChangesPerTransactionPerPartition * 2);
      }
    }
    nextTransactionId++;
  }

  private void generatePartitionRecord() {
    if (partitions.size() < 1) {
      return;
    }
    // Randomly generate a partition action
    PartitionAction partitionAction =
        PartitionAction.values()[RANDOM.nextInt(PartitionAction.values().length)];
    // Randomly generate a partition
    Partition partition = partitions.get(RANDOM.nextInt(partitions.size()));
    switch (partitionAction) {
      case PARTITION_SPLIT:
        splitPartition(partition);
        break;
      case PARTITION_MERGE:
        mergePartitions(partition);
        break;
      case PARTITION_MOVE:
        movePartition(partition);
        break;
    }
  }

  private void addRecord(Partition partition, long recordSequence) {
    partition.addRecord(nextPrimaryKey);
    this.builder =
        this.builder.addElements(
            mapChangeRecord(
                RecordAction.RECORD_NEW,
                partition,
                ImmutableList.of(nextPrimaryKey),
                nextTransactionId,
                recordSequence));
    nextPrimaryKey++;
  }

  private void deleteRecord(Partition partition, long recordSequence) {
    Integer deletedKey = partition.deleteRandomRecord();
    if (deletedKey != null) {
      this.builder =
          this.builder.addElements(
              mapChangeRecord(
                  RecordAction.RECORD_DELETE,
                  partition,
                  ImmutableList.of(deletedKey),
                  nextTransactionId,
                  recordSequence));
    }
  }

  private void updateRecord(Partition partition, long recordSequence) {
    int numRecordsInPartition = partition.getPrimaryKeys().size();
    if (numRecordsInPartition == 0) {
      return;
    }
    HashSet<Integer> keys = new HashSet<>();
    for (int i = 0; i < RANDOM.nextInt(numRecordsInPartition); i++) {
      // Get a random key from the partition
      Integer randomKey = partition.updateRandomRecord();
      if (randomKey != null) {
        keys.add(randomKey);
      }
    }
    this.builder =
        this.builder.addElements(
            mapChangeRecord(
                RecordAction.RECORD_UPDATE,
                partition,
                new ArrayList<>(keys),
                nextTransactionId,
                recordSequence));
  }

  private void splitPartition(Partition partition) {
    // TODO currently always splits keys into two even sizes, can randomise it
    // Can't split partition if there are fewer than 2 records in the partition
    if (partition.getPrimaryKeys().size() < 2) {
      return;
    }
    Set<Integer> primaryKeys = partition.partitionRecords();
    Partition newPartition = new Partition(PartitionId.of(nextPartitionId++), primaryKeys);
    partitions.add(newPartition);
    this.builder =
        this.builder.addElements(
            mapPartitionRecord(
                partition.getPartitionToken(),
                0,
                ImmutableSet.of(partition.getPartitionToken()),
                ImmutableSet.of(partition.getPartitionToken(), newPartition.getPartitionToken())));
    //    System.out.println(
    //        "Partition " + partition.getPartitionId() + " split into " + (nextPartitionId - 1));
  }

  private void mergePartitions(Partition partition) {
    // Can't merge if there are fewer than 2 partitions
    if (partitions.size() < 2) {
      return;
    }
    int partition2Index = RANDOM.nextInt(partitions.size());
    Partition partition2 = partitions.get(partition2Index);
    // Create a new partition to merge into
    Set<Integer> mergedPrimaryKeys = new HashSet<>(partition.getPrimaryKeys());
    mergedPrimaryKeys.addAll(partition2.getPrimaryKeys());
    Partition newPartition = new Partition(PartitionId.of(nextPartitionId++), mergedPrimaryKeys);
    partitions.add(newPartition);
    newPartition.addRecords(partition2.getPrimaryKeys());
    // Delete the merged partition
    partitions.remove(partition);
    partitions.remove(partition2Index);
    this.builder =
        this.builder
            .addElements(
                mapPartitionRecord(
                    partition.getPartitionToken(),
                    0L,
                    ImmutableSet.of(partition.getPartitionToken(), partition2.getPartitionToken()),
                    ImmutableSet.of(newPartition.getPartitionToken())))
            .addElements(
                mapPartitionRecord(
                    partition2.getPartitionToken(),
                    0L,
                    ImmutableSet.of(partition.getPartitionToken(), partition2.getPartitionToken()),
                    ImmutableSet.of(newPartition.getPartitionToken())));
    //    System.out.println(
    //        "Partition "
    //            + partition.getPartitionId()
    //            + " and "
    //            + partition2.getPartitionId()
    //            + " merged");
  }

  private void movePartition(Partition partition) {
    Partition newPartition = new Partition(PartitionId.of(nextPartitionId++),
        partition.getPrimaryKeys());
    partitions.add(newPartition);
    partitions.remove(partition);
    this.builder =
        this.builder.addElements(
            mapPartitionRecord(
                partition.getPartitionToken(),
                0,
                ImmutableSet.of(partition.getPartitionToken()),
                ImmutableSet.of(newPartition.getPartitionToken())));
    //    System.out.println(
    //        "Partition " + partition.getPartitionId() + " moved into " + (nextPartitionId - 1));
  }

  private void maybeAdvanceWaterMark() {
    // Go through all partitions, check if the latest record change time for all partitions >
    // this.currentWaterMark. If so, advance watermark.
    long earliestTime = partitions.get(0).getLatestChangeTimestamp();
    for (Partition partition : partitions) {
      earliestTime = Math.min(partition.getLatestChangeTimestamp(), earliestTime);
    }
    if (earliestTime > this.currentWaterMark) {
      this.currentWaterMark = earliestTime;
      builder = builder.advanceWatermarkTo(Instant.ofEpochMilli(earliestTime));
    }
  }

}
