/*
 * Copyright 2021 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.beam.sdk.io.gcp.spanner.cdc.actions;

import static org.apache.beam.sdk.io.gcp.spanner.cdc.model.PartitionMetadata.State.CREATED;

import com.google.cloud.Timestamp;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
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

public class ChildPartitionsRecordAction {

  private final PartitionMetadataDao partitionMetadataDao;
  private final WaitForChildPartitionsAction waitForChildPartitionsAction;

  public ChildPartitionsRecordAction(
      PartitionMetadataDao partitionMetadataDao,
      WaitForChildPartitionsAction waitForChildPartitionsAction) {
    this.partitionMetadataDao = partitionMetadataDao;
    this.waitForChildPartitionsAction = waitForChildPartitionsAction;
  }

  public Optional<ProcessContinuation> run(
      ChildPartitionsRecord record,
      PartitionMetadata partition,
      RestrictionTracker<PartitionRestriction, PartitionPosition> tracker,
      ManualWatermarkEstimator<Instant> watermarkEstimator
  ) {
    final Timestamp startTimestamp = record.getStartTimestamp();
    if (!tracker.tryClaim(PartitionPosition.queryChangeStream(startTimestamp))) {
      return Optional.of(ProcessContinuation.stop());
    }
    watermarkEstimator.setWatermark(new Instant(startTimestamp.toSqlTimestamp().getTime()));

    if (isSplitOrMove(record)) {
      return processChildPartitionSplit(record, partition, tracker);
    } else {
      // TODO: Implement merge
      throw new UnsupportedOperationException("Merge is unimplemented");
    }
  }

  private Optional<ProcessContinuation> processChildPartitionSplit(
      ChildPartitionsRecord record,
      PartitionMetadata partition,
      RestrictionTracker<PartitionRestriction, PartitionPosition> tracker
  ) {
    // Updates the metadata table
    // FIXME: We will need to batch the records here
    // FIXME: Figure out what to do if this throws an exception
    final List<PartitionMetadata> newChildPartitions = partitionMetadataRowsFrom(record, partition);
    partitionMetadataDao.insert(newChildPartitions);

    return waitForChildPartitionsAction.run(
        partition,
        tracker,
        record.getChildPartitions().size()
    );
  }

  // TODO: Confirm the assumption that within a child partitions record it could either be a split or a merge, but not both
  private boolean isSplitOrMove(ChildPartitionsRecord record) {
    return record.getChildPartitions().get(0).getParentTokens().size() == 1;
  }

  private List<PartitionMetadata> partitionMetadataRowsFrom(
      ChildPartitionsRecord record,
      PartitionMetadata parentPartition) {
    return record
        .getChildPartitions()
        .stream()
        .map(childPartition -> toPartitionMetadata(
            record.getStartTimestamp(),
            parentPartition.getEndTimestamp(),
            parentPartition.getHeartbeatSeconds(),
            childPartition
        ))
        .collect(Collectors.toList());
  }

  private PartitionMetadata toPartitionMetadata(
      Timestamp startTimestamp,
      Timestamp endTimestamp,
      long heartbeatSeconds,
      ChildPartition childPartition
  ) {
    return PartitionMetadata
        .newBuilder()
        .setPartitionToken(childPartition.getToken())
        .setParentTokens(childPartition.getParentTokens())
        .setStartTimestamp(startTimestamp)
        .setInclusiveStart(true)
        .setEndTimestamp(endTimestamp)
        .setInclusiveEnd(false)
        .setHeartbeatSeconds(heartbeatSeconds)
        .setState(CREATED)
        .build();
  }
}
