package org.apache.beam.sdk.io.gcp.spanner.changestreams.generator;

public enum PartitionAction {
  PARTITION_MERGE,
  PARTITION_SPLIT,
  PARTITION_MOVE
}