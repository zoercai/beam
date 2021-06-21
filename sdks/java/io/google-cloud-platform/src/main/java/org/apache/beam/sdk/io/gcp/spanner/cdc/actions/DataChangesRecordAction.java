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

import com.google.cloud.Timestamp;
import java.util.Optional;
import org.apache.beam.sdk.io.gcp.spanner.cdc.model.DataChangesRecord;
import org.apache.beam.sdk.io.gcp.spanner.cdc.restriction.PartitionPosition;
import org.apache.beam.sdk.io.gcp.spanner.cdc.restriction.PartitionRestriction;
import org.apache.beam.sdk.transforms.DoFn.OutputReceiver;
import org.apache.beam.sdk.transforms.DoFn.ProcessContinuation;
import org.apache.beam.sdk.transforms.splittabledofn.ManualWatermarkEstimator;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// TODO: Add java docs
public class DataChangesRecordAction {
  private static final Logger LOG = LoggerFactory.getLogger(DataChangesRecordAction.class);

  public Optional<ProcessContinuation> run(
      DataChangesRecord record,
      RestrictionTracker<PartitionRestriction, PartitionPosition> tracker,
      OutputReceiver<DataChangesRecord> outputReceiver,
      ManualWatermarkEstimator<Instant> watermarkEstimator) {
    LOG.debug("Processing data record for " + record.getPartitionToken());

    final Timestamp commitTimestamp = record.getCommitTimestamp();
    if (!tracker.tryClaim(PartitionPosition.queryChangeStream(commitTimestamp))) {
      LOG.debug("Could not claim, stopping");
      return Optional.of(ProcessContinuation.stop());
    }
    // TODO: Ask about this, do we need to output with timestamp?
    outputReceiver.output(record);
    watermarkEstimator.setWatermark(new Instant(commitTimestamp.toSqlTimestamp().getTime()));

    LOG.debug("Data record action completed successfully");
    return Optional.empty();
  }
}
