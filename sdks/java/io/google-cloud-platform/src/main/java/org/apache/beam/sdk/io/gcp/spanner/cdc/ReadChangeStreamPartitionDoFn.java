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

import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.Struct;
import com.google.gson.Gson;
import java.io.Serializable;
import java.util.List;
import org.apache.beam.sdk.io.gcp.spanner.SpannerAccessor;
import org.apache.beam.sdk.io.gcp.spanner.SpannerConfig;
import org.apache.beam.sdk.io.gcp.spanner.cdc.mapper.ChangeStreamRecordMapper;
import org.apache.beam.sdk.io.gcp.spanner.cdc.model.ChangeStreamRecord;
import org.apache.beam.sdk.io.gcp.spanner.cdc.model.DataChangesRecord;
import org.apache.beam.sdk.io.gcp.spanner.cdc.model.HeartbeatRecord;
import org.apache.beam.sdk.io.gcp.spanner.cdc.model.PartitionMetadata;
import org.apache.beam.sdk.io.range.OffsetRange;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFn.UnboundedPerElement;
import org.apache.beam.sdk.transforms.splittabledofn.ManualWatermarkEstimator;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.sdk.transforms.splittabledofn.WatermarkEstimators.Manual;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@UnboundedPerElement
public class ReadChangeStreamPartitionDoFn extends DoFn<PartitionMetadata, DataChangesRecord> implements
    Serializable {

  private static final long serialVersionUID = -7574596218085711975L;
  private static final Logger LOG = LoggerFactory.getLogger(ReadChangeStreamPartitionDoFn.class);

  private final SpannerConfig spannerConfig;
  private transient DatabaseClient databaseClient;
  private transient ChangeStreamRecordMapper changeStreamRecordMapper;

  public ReadChangeStreamPartitionDoFn(SpannerConfig spannerConfig) {
    this.spannerConfig = spannerConfig;
  }

  @GetInitialRestriction
  public OffsetRange initialRestriction(@Element PartitionMetadata element) {
    final long startTimestamp = element.getStartTimestamp().toSqlTimestamp().getTime();
    final long endTimestamp = element.getEndTimestamp().toSqlTimestamp().getTime();
    return new OffsetRange(startTimestamp, endTimestamp);
  }

  @Setup
  public void setup() {
    final SpannerAccessor spannerAccessor = SpannerAccessor.getOrCreate(spannerConfig);
    this.databaseClient = spannerAccessor.getDatabaseClient();
    this.changeStreamRecordMapper = new ChangeStreamRecordMapper(new Gson());
  }

  @ProcessElement
  public ProcessContinuation processElement(
      @Element PartitionMetadata element,
      RestrictionTracker<OffsetRange, Long> tracker,
      OutputReceiver<DataChangesRecord> receiver,
      ManualWatermarkEstimator<Instant> watermarkEstimator
  ) {
    try (ResultSet resultSet = databaseClient.singleUse().executeQuery(Statement.of("SELECT 1"))) {
      while (resultSet.next()) {
        final Struct rowAsStruct = resultSet.getCurrentRowAsStruct();
        final List<ChangeStreamRecord> records = changeStreamRecordMapper
            .toChangeStreamRecords(element.getPartitionToken(), rowAsStruct);

        for (ChangeStreamRecord record : records) {
          // FIXME: We should process each type of record separately
          boolean isRecordClaimed = false;
          if (record instanceof DataChangesRecord) {
            isRecordClaimed = processDataChangesRecord(
                (DataChangesRecord) record,
                tracker,
                receiver,
                watermarkEstimator
            );
          } else if (record instanceof HeartbeatRecord) {
            isRecordClaimed = processHeartbeatRecord(
                (HeartbeatRecord) record,
                tracker,
                watermarkEstimator
            );
          }
          if (!isRecordClaimed) {
            return ProcessContinuation.stop();
          }
        }
      }

      // FIXME: This should be removed once we have our custom tracker
      tracker.tryClaim(element.getEndTimestamp().toSqlTimestamp().getTime());
      return ProcessContinuation.stop();
    }
  }

  private boolean processDataChangesRecord(
      DataChangesRecord record,
      RestrictionTracker<OffsetRange, Long> tracker,
      OutputReceiver<DataChangesRecord> receiver,
      ManualWatermarkEstimator<Instant> watermarkEstimator) {
    final long commitTimestamp = record.getCommitTimestamp().toSqlTimestamp().getTime();
    if (!tracker.tryClaim(commitTimestamp)) {
      return false;
    }
    receiver.output(record);
    watermarkEstimator.setWatermark(new Instant(commitTimestamp));

    return true;
  }

  private boolean processHeartbeatRecord(
      HeartbeatRecord record,
      RestrictionTracker<OffsetRange, Long> tracker,
      ManualWatermarkEstimator<Instant> watermarkEstimator
  ) {
    final long timestamp = record.getTimestamp().toSqlTimestamp().getTime();
    if (!tracker.tryClaim(timestamp)) {
      return false;
    }
    watermarkEstimator.setWatermark(new Instant(timestamp));

    return true;
  }

  @GetInitialWatermarkEstimatorState
  public Instant getInitialWatermarkEstimatorState(@Timestamp Instant currentElementTimestamp) {
    return currentElementTimestamp;
  }

  @NewWatermarkEstimator
  public ManualWatermarkEstimator<Instant> newWatermarkEstimator(
      @WatermarkEstimatorState Instant watermarkEstimatorState
  ) {
    return new Manual(watermarkEstimatorState);
  }
}
