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
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.Struct;
import com.google.gson.Gson;
import java.io.Serializable;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.beam.sdk.io.gcp.spanner.SpannerAccessor;
import org.apache.beam.sdk.io.gcp.spanner.SpannerConfig;
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
  private transient DatabaseClient databaseClient;
  private transient ChangeStreamRecordMapper changeStreamRecordMapper;

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
    final SpannerAccessor spannerAccessor = SpannerAccessor.getOrCreate(spannerConfig);
    this.databaseClient = spannerAccessor.getDatabaseClient();
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
    try (ResultSet resultSet = databaseClient.singleUse().executeQuery(Statement.of("SELECT 1"))) {
      while (resultSet.next()) {
        final Struct rowAsStruct = resultSet.getCurrentRowAsStruct();
        final List<ChangeStreamRecord> records = changeStreamRecordMapper
            .toChangeStreamRecords(element.getPartitionToken(), rowAsStruct);

        for (ChangeStreamRecord record : records) {
          // FIXME: We should error if the record is of an unknown type
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
          } else if (record instanceof ChildPartitionsRecord) {
            isRecordClaimed = processChildPartitionsRecord(
                (ChildPartitionsRecord) record,
                element,
                tracker,
                watermarkEstimator
            );
          }
          if (!isRecordClaimed) {
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
      PartitionMetadata partitionMetadata,
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
    databaseClient
        .readWriteTransaction()
        .run(transaction -> {
          final List<Mutation> mutations = record
              .getChildPartitions()
              .stream()
              .map(childPartition -> mutationFrom(
                  record.getStartTimestamp(),
                  partitionMetadata.getEndTimestamp(),
                  partitionMetadata.getHeartbeatSeconds(),
                  childPartition
              ))
              .collect(Collectors.toList());
          mutations.add(Mutation
              .newUpdateBuilder(tableName)
              .set(PartitionMetadataDao.COLUMN_PARTITION_TOKEN)
              .to(partitionMetadata.getPartitionToken())
              .set(PartitionMetadataDao.COLUMN_STATE)
              .to(PartitionMetadata.State.FINISHED.toString())
              // FIXME: This should be the pending commit timestamp
              // .set(PartitionMetadataDao.COLUMN_UPDATED_AT)
              // .to(com.google.cloud.Timestamp.now())
              .build());

          transaction.buffer(mutations);
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

  private Mutation mutationFrom(
      com.google.cloud.Timestamp startTimestamp,
      com.google.cloud.Timestamp endTimestamp,
      long heartbeatSeconds,
      ChildPartition childPartition
  ) {
    return Mutation
        .newInsertBuilder(tableName)
        .set(PartitionMetadataDao.COLUMN_PARTITION_TOKEN)
        .to(childPartition.getToken())
        // FIXME: This should be a list of parents
        .set(PartitionMetadataDao.COLUMN_PARENT_TOKEN)
        .to(childPartition.getParentTokens().get(0))
        .set(PartitionMetadataDao.COLUMN_START_TIMESTAMP)
        .to(startTimestamp)
        .set(PartitionMetadataDao.COLUMN_INCLUSIVE_START)
        .to(true)
        .set(PartitionMetadataDao.COLUMN_END_TIMESTAMP)
        .to(endTimestamp)
        .set(PartitionMetadataDao.COLUMN_INCLUSIVE_END)
        .to(false)
        .set(PartitionMetadataDao.COLUMN_HEARTBEAT_SECONDS)
        .to(heartbeatSeconds)
        .set(PartitionMetadataDao.COLUMN_STATE)
        .to(PartitionMetadata.State.CREATED.toString())
        // FIXME: This should be the pending commit timestamp
        // .set(PartitionMetadataDao.COLUMN_CREATED_AT)
        // .to(com.google.cloud.Timestamp.now())
        // FIXME: This should be the pending commit timestamp
        // .set(PartitionMetadataDao.COLUMN_UPDATED_AT)
        // .to(com.google.cloud.Timestamp.now())
        .build();
  }
}
