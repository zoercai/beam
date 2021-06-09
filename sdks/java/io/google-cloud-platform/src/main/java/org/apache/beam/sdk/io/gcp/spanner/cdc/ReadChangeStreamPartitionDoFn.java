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

import static org.apache.beam.sdk.io.gcp.spanner.cdc.model.PartitionMetadata.State.CREATED;
import static org.apache.beam.sdk.io.gcp.spanner.cdc.model.PartitionMetadata.State.FINISHED;

import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Struct;
import com.google.gson.Gson;
import java.io.Serializable;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.beam.sdk.io.gcp.spanner.SpannerConfig;
import org.apache.beam.sdk.io.gcp.spanner.cdc.dao.ChangeStreamDao;
import org.apache.beam.sdk.io.gcp.spanner.cdc.dao.DaoFactory;
import org.apache.beam.sdk.io.gcp.spanner.cdc.dao.PartitionMetadataDao;
import org.apache.beam.sdk.io.gcp.spanner.cdc.mapper.ChangeStreamRecordMapper;
import org.apache.beam.sdk.io.gcp.spanner.cdc.model.ChangeStreamRecord;
import org.apache.beam.sdk.io.gcp.spanner.cdc.model.ChildPartitionsRecord;
import org.apache.beam.sdk.io.gcp.spanner.cdc.model.ChildPartitionsRecord.ChildPartition;
import org.apache.beam.sdk.io.gcp.spanner.cdc.model.DataChangesRecord;
import org.apache.beam.sdk.io.gcp.spanner.cdc.model.HeartbeatRecord;
import org.apache.beam.sdk.io.gcp.spanner.cdc.model.PartitionMetadata;
import org.apache.beam.sdk.io.gcp.spanner.cdc.restriction.PartitionPosition;
import org.apache.beam.sdk.io.gcp.spanner.cdc.restriction.PartitionRestriction;
import org.apache.beam.sdk.io.gcp.spanner.cdc.restriction.PartitionRestrictionTracker;
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
  private final String tableName;
  private transient ChangeStreamRecordMapper changeStreamRecordMapper;
  private transient ChangeStreamDao changeStreamDao;
  private transient PartitionMetadataDao partitionMetadataDao;

  public ReadChangeStreamPartitionDoFn(SpannerConfig spannerConfig, String partitionMetadataTableName) {
    this.spannerConfig = spannerConfig;
    this.tableName = partitionMetadataTableName;
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

  @GetInitialRestriction
  public PartitionRestriction initialRestriction(@Element PartitionMetadata element) {
    return new PartitionRestriction(element.getStartTimestamp());
  }

  @NewTracker
  public PartitionRestrictionTracker newTracker(
      @Restriction PartitionRestriction restriction) {
    return new PartitionRestrictionTracker(restriction);
  }

  @Setup
  public void setup() {
    this.partitionMetadataDao = DaoFactory.partitionMetadataDaoFrom(spannerConfig);
    this.changeStreamDao = DaoFactory.changeStreamDaoFrom(spannerConfig);
    this.changeStreamRecordMapper = new ChangeStreamRecordMapper(new Gson());
  }

  @ProcessElement
  public ProcessContinuation processElement(
      @Element PartitionMetadata element,
      RestrictionTracker<PartitionRestriction, PartitionPosition> tracker,
      OutputReceiver<DataChangesRecord> receiver,
      ManualWatermarkEstimator<Instant> watermarkEstimator
  ) {
    // TODO: Add the real change stream query here
    try (ResultSet resultSet = changeStreamDao.changeStreamQuery()) {
      while (resultSet.next()) {
        final Struct rowAsStruct = resultSet.getCurrentRowAsStruct();
        final List<ChangeStreamRecord> records = changeStreamRecordMapper
            .toChangeStreamRecords(element.getPartitionToken(), rowAsStruct);

        for (ChangeStreamRecord record : records) {
          // FIXME: We should error if the record is of an unknown type
          boolean isClaimed = false;
          if (record instanceof DataChangesRecord) {
            isClaimed = processDataChangesRecord(
                (DataChangesRecord) record,
                tracker,
                receiver,
                watermarkEstimator
            );
          } else if (record instanceof HeartbeatRecord) {
            isClaimed = processHeartbeatRecord(
                (HeartbeatRecord) record,
                tracker,
                watermarkEstimator
            );
          } else if (record instanceof ChildPartitionsRecord) {
            isClaimed = processChildPartitionsRecord(
                (ChildPartitionsRecord) record,
                element,
                tracker,
                watermarkEstimator
            );
          }
          if (!isClaimed) {
            return ProcessContinuation.stop();
          }
        }
      }

      tracker.tryClaim(PartitionPosition.done());
      return ProcessContinuation.stop();
    }
  }

  private boolean processDataChangesRecord(
      DataChangesRecord record,
      RestrictionTracker<PartitionRestriction, PartitionPosition> tracker,
      OutputReceiver<DataChangesRecord> receiver,
      ManualWatermarkEstimator<Instant> watermarkEstimator) {
    final com.google.cloud.Timestamp commitTimestamp = record.getCommitTimestamp();
    if (!tracker.tryClaim(PartitionPosition.continueQuery(commitTimestamp))) {
      return false;
    }
    receiver.output(record);
    watermarkEstimator.setWatermark(new Instant(commitTimestamp.toSqlTimestamp().getTime()));

    return true;
  }

  private boolean processHeartbeatRecord(
      HeartbeatRecord record,
      RestrictionTracker<PartitionRestriction, PartitionPosition> tracker,
      ManualWatermarkEstimator<Instant> watermarkEstimator
  ) {
    final com.google.cloud.Timestamp timestamp = record.getTimestamp();
    if (!tracker.tryClaim(PartitionPosition.continueQuery(timestamp))) {
      return false;
    }
    watermarkEstimator.setWatermark(new Instant(timestamp.toSqlTimestamp().getTime()));

    return true;
  }

  private boolean processChildPartitionsRecord(
      ChildPartitionsRecord record,
      PartitionMetadata currentPartition,
      RestrictionTracker<PartitionRestriction, PartitionPosition> tracker,
      ManualWatermarkEstimator<Instant> watermarkEstimator
  ) {
    final com.google.cloud.Timestamp startTimestamp = record.getStartTimestamp();
    if (!tracker.tryClaim(PartitionPosition.continueQuery(startTimestamp))) {
      return false;
    }

    // Updates the metadata table
    // FIXME: Use the DAO if possible
    // FIXME: We will need to batch the records here
    // FIXME: Figure out what to do if this throws an exception
    final List<PartitionMetadata> newChildPartitions = partitionMetadataRowsFrom(record, currentPartition);
    partitionMetadataDao.runInTransaction(tableName, transaction -> {
      transaction.insert(newChildPartitions);
      transaction.updateState(currentPartition.getPartitionToken(), FINISHED);
      return null;
    });

    watermarkEstimator.setWatermark(new Instant(startTimestamp.toSqlTimestamp().getTime()));

    // TODO: Wait for child partitions to be scheduled
    // tracker.tryClaim(PartitionPosition.waitForChildren(startTimestamp));
    // TODO: Wait for parent partitions to be deleted
    // tracker.tryClaim(PartitionPosition.waitForParents(startTimestamp));
    // TODO: Delete the current partition from the partitions metadata table

    return true;
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
      com.google.cloud.Timestamp startTimestamp,
      com.google.cloud.Timestamp endTimestamp,
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
