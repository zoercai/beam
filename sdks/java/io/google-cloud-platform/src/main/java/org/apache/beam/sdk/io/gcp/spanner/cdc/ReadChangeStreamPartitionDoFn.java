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

  private final SpannerConfig spannerConfig;
  private transient DatabaseClient databaseClient;
  private transient ChangeStreamRecordMapper changeStreamRecordMapper;

  public ReadChangeStreamPartitionDoFn(SpannerConfig spannerConfig) {
    this.spannerConfig = spannerConfig;
  }

  @GetInitialRestriction
  public OffsetRange initialRestriction(@Element PartitionMetadata element) {
    return new OffsetRange(0, 1);
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
      OutputReceiver<DataChangesRecord> receiver
  ) {
    try (ResultSet resultSet = databaseClient.singleUse().executeQuery(Statement.of("SELECT 1"))) {
      while (resultSet.next()) {
        final Struct rowAsStruct = resultSet.getCurrentRowAsStruct();
        final List<ChangeStreamRecord> records = changeStreamRecordMapper
            .toChangeStreamRecords(element.getPartitionToken(), rowAsStruct);
        // FIXME: We should try to claim the timestamp of each record
        if (!tracker.tryClaim(0L)) {
          return ProcessContinuation.stop();
        }
        // FIXME: The timestamp should be the record timestamp
        final Instant timestamp = Instant.now();
        LOG.info("Outputting record with timestamp " + timestamp);
        // FIXME: We should not only emit the first record
        receiver.outputWithTimestamp((DataChangesRecord) records.get(0), timestamp);
      }

      return ProcessContinuation.stop();
    }
  }
}
