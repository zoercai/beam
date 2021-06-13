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

import java.util.Optional;
import org.apache.beam.sdk.io.gcp.spanner.cdc.dao.PartitionMetadataDao;
import org.apache.beam.sdk.io.gcp.spanner.cdc.model.PartitionMetadata;
import org.apache.beam.sdk.io.gcp.spanner.cdc.restriction.PartitionPosition;
import org.apache.beam.sdk.io.gcp.spanner.cdc.restriction.PartitionRestriction;
import org.apache.beam.sdk.transforms.DoFn.ProcessContinuation;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.joda.time.Duration;

public class WaitForParentPartitionsAction {

  private final PartitionMetadataDao partitionMetadataDao;
  private final Duration resumeDuration;

  public WaitForParentPartitionsAction(
      PartitionMetadataDao partitionMetadataDao,
      Duration resumeDuration) {
    this.partitionMetadataDao = partitionMetadataDao;
    this.resumeDuration = resumeDuration;
  }

  public Optional<ProcessContinuation> run(
      PartitionMetadata partition,
      RestrictionTracker<PartitionRestriction, PartitionPosition> tracker
  ) {
    if (!tracker.tryClaim(PartitionPosition.waitForParentPartitions())) {
      return Optional.of(ProcessContinuation.stop());
    }
    long numberOfExistingParents = partitionMetadataDao.countExistingParents(partition.getPartitionToken());
    if (numberOfExistingParents > 0) {
      return Optional.of(ProcessContinuation.resume().withResumeDelay(resumeDuration));
    }

    return Optional.empty();
  }
}
