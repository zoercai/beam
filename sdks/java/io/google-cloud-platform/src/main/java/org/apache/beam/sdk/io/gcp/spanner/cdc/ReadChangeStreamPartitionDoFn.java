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
import java.io.Serializable;
import org.apache.beam.sdk.io.gcp.spanner.SpannerAccessor;
import org.apache.beam.sdk.io.gcp.spanner.SpannerConfig;
import org.apache.beam.sdk.io.gcp.spanner.cdc.mapper.RecordMapper;
import org.apache.beam.sdk.io.gcp.spanner.cdc.model.DataChangesRecord;
import org.apache.beam.sdk.io.gcp.spanner.cdc.model.PartitionMetadata;
import org.apache.beam.sdk.io.range.OffsetRange;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFn.UnboundedPerElement;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@UnboundedPerElement
public class ReadChangeStreamPartitionDoFn extends DoFn<PartitionMetadata, DataChangesRecord> implements
    Serializable {

  private static final long serialVersionUID = -7574596218085711975L;
  private static final Logger LOG = LoggerFactory.getLogger(ReadChangeStreamPartitionDoFn.class);

  private SpannerConfig spannerConfig;

  // TODO: Check if this is really necessary
  private ReadChangeStreamPartitionDoFn() {}

  public ReadChangeStreamPartitionDoFn(SpannerConfig spannerConfig) {
    this.spannerConfig = spannerConfig;
  }

  @GetInitialRestriction
  public OffsetRange initialRestriction(@Element PartitionMetadata element) {
    return new OffsetRange(0, 1);
  }

  @ProcessElement
  public ProcessContinuation processElement(
      @Element PartitionMetadata element,
      RestrictionTracker<OffsetRange, Long> tracker,
      OutputReceiver<DataChangesRecord> receiver
  ) {
    // FIXME: These variables should be moved from here
    final SpannerAccessor spannerAccessor = SpannerAccessor.getOrCreate(spannerConfig);
    final DatabaseClient databaseClient = spannerAccessor.getDatabaseClient();
    final RecordMapper recordMapper = new RecordMapper();
    try (ResultSet resultSet = databaseClient.singleUse().executeQuery(Statement.of("SELECT 1"))) {
      while (resultSet.next()) {
        // FIXME: We should not only get the first record
        final Struct rowAsStruct = resultSet.getCurrentRowAsStruct().getStructList(0).get(0).getStruct("data_change_record");
        final DataChangesRecord dataChangesRecord = recordMapper.toDataChangesRecord(element.getPartitionToken(), rowAsStruct);
        if (!tracker.tryClaim(0L)) {
          return ProcessContinuation.stop();
        }
        // FIXME: The timestamp should be the record timestamp
        final Instant timestamp = Instant.now();
        LOG.info("Outputting record with timestamp " + timestamp);
        receiver.outputWithTimestamp(dataChangesRecord, timestamp);
      }

      // FIXME: This should be a resume
      return ProcessContinuation.stop();
    }
  }

  // TODO: Check if this is really necessary
  public void setSpannerConfig(SpannerConfig spannerConfig) {
    this.spannerConfig = spannerConfig;
  }
}
