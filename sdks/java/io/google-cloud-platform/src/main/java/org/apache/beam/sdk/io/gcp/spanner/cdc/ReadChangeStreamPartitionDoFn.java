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

package org.apache.beam.sdk.io.gcp.spanner.cdc;

import com.google.common.annotations.VisibleForTesting;
import java.io.Serializable;
import java.util.Collections;
import org.apache.beam.sdk.io.gcp.spanner.SpannerAccessor;
import org.apache.beam.sdk.io.gcp.spanner.SpannerConfig;
import org.apache.beam.sdk.io.gcp.spanner.cdc.usermodel.DataChangesRecord;
import org.apache.beam.sdk.io.gcp.spanner.cdc.usermodel.ModType;
import org.apache.beam.sdk.io.gcp.spanner.cdc.usermodel.PartitionId;
import org.apache.beam.sdk.io.gcp.spanner.cdc.usermodel.RecordSequence;
import org.apache.beam.sdk.io.gcp.spanner.cdc.usermodel.TransactionId;
import org.apache.beam.sdk.io.gcp.spanner.cdc.usermodel.ValueCaptureType;
import org.apache.beam.sdk.io.range.OffsetRange;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFn.UnboundedPerElement;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@UnboundedPerElement
public class ReadChangeStreamPartitionDoFn extends DoFn<PartitionId, DataChangesRecord> implements
    Serializable {

  private static final Logger LOG = LoggerFactory.getLogger(ReadChangeStreamPartitionDoFn.class);
  private final SpannerConfig spannerConfig;
  private final transient SpannerAccessor spannerAccessor;

  public ReadChangeStreamPartitionDoFn(SpannerConfig spannerConfig) {
    this(spannerConfig, SpannerAccessor.getOrCreate(spannerConfig));
  }

  @VisibleForTesting
  ReadChangeStreamPartitionDoFn(SpannerConfig spannerConfig, SpannerAccessor spannerAccessor) {
    this.spannerConfig = spannerConfig;
    this.spannerAccessor = spannerAccessor;
  }

  @GetInitialRestriction
  public OffsetRange initialRestriction(@Element PartitionId partitionRecord) {
    return new OffsetRange(0, 1);
  }

  @ProcessElement
  public ProcessContinuation processElement(
      @Element PartitionId partitionRecord,
      RestrictionTracker<OffsetRange, Long> tracker,
      OutputReceiver<DataChangesRecord> receiver
  ) {
    LOG.info("Claiming restriction");
    if (!tracker.tryClaim(0L)) {
      LOG.warn("Could not claim restriction, stopping");
      return ProcessContinuation.stop();
    }

    LOG.info("Building data record");
    final DataChangesRecord record = new DataChangesRecord(
        PartitionId.of(1L),
        Instant.now(),
        TransactionId.of(2L),
        false,
        RecordSequence.of(3L),
        "TableName",
        Collections.emptyList(),
        Collections.emptyList(),
        ModType.INSERT,
        ValueCaptureType.OLD_AND_NEW_VALUES
    );

    final Instant timestamp = Instant.now();
    LOG.info("Outputting record with timestamp " + timestamp);
    receiver.outputWithTimestamp(record, timestamp);

    LOG.info("Finalising");
    return ProcessContinuation.stop();
  }
}
