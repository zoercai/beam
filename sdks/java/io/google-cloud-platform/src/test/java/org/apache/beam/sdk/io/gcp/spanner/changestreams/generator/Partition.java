package org.apache.beam.sdk.io.gcp.spanner.changestreams.generator;

import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;

public class Partition {
  private static final Random RANDOM = new Random();
  private final String partitionToken;
  private Set<Integer> primaryKeys;
  private long latestChangeTimestamp = 0;

  public Partition(String partitionToken) {
    this.partitionToken = partitionToken;
    this.primaryKeys = new HashSet<>();
  }

  public Partition(String partitionToken, Set<Integer> primaryKeys) {
    this.partitionToken = partitionToken;
    this.primaryKeys = primaryKeys;
  }

  public String getPartitionToken() {
    return this.partitionToken;
  }

  public Set<Integer> getPrimaryKeys() {
    return this.primaryKeys;
  }

  public long getLatestChangeTimestamp() {
    return this.latestChangeTimestamp;
  }

  public void addRecord(int primaryKey) {
    this.primaryKeys.add(primaryKey);
    updateLatestChangeTimestamp();
    //    System.out.println("key " + primaryKey + " added to partition " + getPartitionId());
  }

  public void addRecords(Set<Integer> primaryKeys) {
    this.primaryKeys.addAll(primaryKeys);
    updateLatestChangeTimestamp();
  }

  public Integer deleteRandomRecord() {
    if (this.primaryKeys.isEmpty()) {
      // TODO confirm whether this partition needs to be removed.
      return null;
    }
    Integer randomKey = getRandomKey();
    this.primaryKeys.remove(randomKey);
    updateLatestChangeTimestamp();
    //    System.out.println("key " + randomKey + " deleted from partition " + getPartitionId());
    return randomKey;
  }

  public Integer updateRandomRecord() {
    if (this.primaryKeys.isEmpty()) {
      return null;
    }
    updateLatestChangeTimestamp();
    //    System.out.println("key " + randomKey + " updated from partition " + getPartitionId());
    return getRandomKey();
  }

  public Set<Integer> partitionRecords() {
    // Split primary keys into 2 even sized parts
    Iterable<List<Integer>> partitions =
        Iterables.partition(primaryKeys, (int) Math.ceil(primaryKeys.size() / 2.0));
    // First part stays with this Partition
    Iterator<List<Integer>> it = partitions.iterator();
    // Second part gets returned
    this.primaryKeys = new HashSet<>(it.next());
    return new HashSet<>(it.next());
  }

  private Integer getRandomKey() {
    int randomIndex = RANDOM.nextInt(this.primaryKeys.size());
    return this.primaryKeys.toArray(new Integer[0])[randomIndex];
  }

  private void updateLatestChangeTimestamp() {
    this.latestChangeTimestamp = System.currentTimeMillis();
  }

  @Override
  public int hashCode() {
    return this.partitionToken.hashCode();
  }
  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    Partition that = (Partition) obj;
    return Objects.equals(partitionToken, that.partitionToken);
  }
}
