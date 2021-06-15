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
import static org.apache.beam.sdk.io.gcp.spanner.cdc.model.PartitionMetadata.State.FINISHED;

import com.google.cloud.Timestamp;
import java.util.Collections;
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

    for (ChildPartition childPartition : record.getChildPartitions()) {
      if (isSplit(childPartition)) {
        final PartitionMetadata row = toPartitionMetadata(
            record.getStartTimestamp(),
            partition.getEndTimestamp(),
            partition.getHeartbeatSeconds(),
            childPartition
        );
        // Updates the metadata table
        // FIXME: Figure out what to do if this throws an exception
        // TODO: Make sure this does not fail if the rows already exist
        partitionMetadataDao.insert(row);
      } else {
        partitionMetadataDao.runInTransaction(transaction -> {
          final long finishedParents = transaction.countPartitionsInStates(
              childPartition.getParentTokens(),
              Collections.singletonList(FINISHED)
          );

          if (finishedParents == childPartition.getParentTokens().size() - 1) {
            transaction.insert(toPartitionMetadata(
                record.getStartTimestamp(),
                partition.getEndTimestamp(),
                partition.getHeartbeatSeconds(),
                childPartition
            ));
          }

          return null;
        });
      }
    }

    return waitForChildPartitionsAction.run(
        partition,
        tracker,
        record.getChildPartitions().size()
    );
  }

  private boolean isSplit(ChildPartition childPartition) {
    return childPartition.getParentTokens().size() == 1;
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
