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

import static org.apache.beam.sdk.io.gcp.spanner.cdc.model.PartitionMetadata.State.FINISHED;

import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Struct;
import com.google.gson.Gson;
import java.io.Serializable;
import java.util.List;
import java.util.Optional;
import org.apache.beam.sdk.io.gcp.spanner.SpannerConfig;
import org.apache.beam.sdk.io.gcp.spanner.cdc.actions.FinishPartitionAction;
import org.apache.beam.sdk.io.gcp.spanner.cdc.dao.ChangeStreamDao;
import org.apache.beam.sdk.io.gcp.spanner.cdc.dao.DaoFactory;
import org.apache.beam.sdk.io.gcp.spanner.cdc.dao.PartitionMetadataDao;
import org.apache.beam.sdk.io.gcp.spanner.cdc.mapper.ChangeStreamRecordMapper;
import org.apache.beam.sdk.io.gcp.spanner.cdc.model.ChangeStreamRecord;
import org.apache.beam.sdk.io.gcp.spanner.cdc.model.ChildPartitionsRecord;
import org.apache.beam.sdk.io.gcp.spanner.cdc.model.DataChangesRecord;
import org.apache.beam.sdk.io.gcp.spanner.cdc.model.HeartbeatRecord;
import org.apache.beam.sdk.io.gcp.spanner.cdc.model.PartitionMetadata;
import org.apache.beam.sdk.io.gcp.spanner.cdc.actions.ChildPartitionsRecordAction;
import org.apache.beam.sdk.io.gcp.spanner.cdc.actions.DataChangesRecordAction;
import org.apache.beam.sdk.io.gcp.spanner.cdc.actions.HeartbeatRecordAction;
import org.apache.beam.sdk.io.gcp.spanner.cdc.actions.DeletePartitionAction;
import org.apache.beam.sdk.io.gcp.spanner.cdc.actions.WaitForChildPartitionsAction;
import org.apache.beam.sdk.io.gcp.spanner.cdc.actions.WaitForParentPartitionsAction;
import org.apache.beam.sdk.io.gcp.spanner.cdc.restriction.PartitionPosition;
import org.apache.beam.sdk.io.gcp.spanner.cdc.restriction.PartitionRestriction;
import org.apache.beam.sdk.io.gcp.spanner.cdc.restriction.PartitionRestrictionTracker;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFn.UnboundedPerElement;
import org.apache.beam.sdk.transforms.splittabledofn.ManualWatermarkEstimator;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.sdk.transforms.splittabledofn.WatermarkEstimators.Manual;
import org.joda.time.Duration;
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

  private transient WaitForChildPartitionsAction waitForChildPartitionsAction;
  private transient FinishPartitionAction finishPartitionAction;
  private transient WaitForParentPartitionsAction waitForParentPartitionsAction;
  private transient DeletePartitionAction deletePartitionAction;
  private transient DataChangesRecordAction dataChangesRecordAction;
  private transient HeartbeatRecordAction heartbeatRecordAction;
  private transient ChildPartitionsRecordAction childPartitionsRecordAction;

  public ReadChangeStreamPartitionDoFn(SpannerConfig spannerConfig, String partitionMetadataTableName) {
    this.spannerConfig = spannerConfig;
    this.tableName = partitionMetadataTableName;
  }

  @GetInitialWatermarkEstimatorState
  public Instant getInitialWatermarkEstimatorState(@Timestamp Instant currentElementTimestamp) {
    return currentElementTimestamp;
  }

  @NewWatermarkEstimator
  public ManualWatermarkEstimator<Instant> newWatermarkEstimator(@WatermarkEstimatorState Instant watermarkEstimatorState) {
    return new Manual(watermarkEstimatorState);
  }

  @GetInitialRestriction
  public PartitionRestriction initialRestriction(@Element PartitionMetadata element) {
    return new PartitionRestriction(element.getStartTimestamp());
  }

  @NewTracker
  public PartitionRestrictionTracker newTracker(@Restriction PartitionRestriction restriction) {
    return new PartitionRestrictionTracker(restriction);
  }

  @Setup
  public void setup() {
    this.partitionMetadataDao = DaoFactory.partitionMetadataDaoFrom(spannerConfig);
    this.changeStreamDao = DaoFactory.changeStreamDaoFrom(spannerConfig);
    this.changeStreamRecordMapper = new ChangeStreamRecordMapper(new Gson());

    this.waitForChildPartitionsAction = new WaitForChildPartitionsAction(partitionMetadataDao, Duration.millis(100));
    this.finishPartitionAction = new FinishPartitionAction(partitionMetadataDao);
    this.waitForParentPartitionsAction = new WaitForParentPartitionsAction(partitionMetadataDao, Duration.millis(100));
    this.deletePartitionAction = new DeletePartitionAction(partitionMetadataDao);

    this.dataChangesRecordAction = new DataChangesRecordAction();
    this.heartbeatRecordAction = new HeartbeatRecordAction();
    this.childPartitionsRecordAction = new ChildPartitionsRecordAction(partitionMetadataDao, waitForChildPartitionsAction);
  }

  @ProcessElement
  public ProcessContinuation processElement(
      @Element PartitionMetadata partition,
      RestrictionTracker<PartitionRestriction, PartitionPosition> tracker,
      OutputReceiver<DataChangesRecord> receiver,
      ManualWatermarkEstimator<Instant> watermarkEstimator
  ) {
    Optional<ProcessContinuation> maybeContinuation = Optional.empty();
    try (ResultSet resultSet = changeStreamDao.changeStreamQuery()) {
      while (resultSet.next()) {
        final Struct rowAsStruct = resultSet.getCurrentRowAsStruct();
        final List<ChangeStreamRecord> records = changeStreamRecordMapper
            .toChangeStreamRecords(partition.getPartitionToken(), rowAsStruct);

        for (ChangeStreamRecord record : records) {
          // FIXME: We should error if the record is of an unknown type
          if (record instanceof DataChangesRecord) {
            maybeContinuation = dataChangesRecordAction.run(
                (DataChangesRecord) record,
                tracker,
                receiver,
                watermarkEstimator
            );
          } else if (record instanceof HeartbeatRecord) {
            maybeContinuation = heartbeatRecordAction.run(
                (HeartbeatRecord) record,
                tracker,
                watermarkEstimator
            );
          } else if (record instanceof ChildPartitionsRecord) {
            maybeContinuation = childPartitionsRecordAction.run(
                (ChildPartitionsRecord) record,
                partition,
                tracker,
                watermarkEstimator
            );
          }
          if (maybeContinuation.isPresent()) {
            return maybeContinuation.get();
          }
        }
      }

      maybeContinuation = finishPartitionAction.run(partition, tracker);
      if (maybeContinuation.isPresent()) return maybeContinuation.get();

      maybeContinuation = waitForParentPartitionsAction.run(partition, tracker);
      if (maybeContinuation.isPresent()) return maybeContinuation.get();

      maybeContinuation = deletePartitionAction.run(partition, tracker);
      if (maybeContinuation.isPresent()) return maybeContinuation.get();

      tracker.tryClaim(PartitionPosition.done());
      return ProcessContinuation.stop();
    }
  }
}
