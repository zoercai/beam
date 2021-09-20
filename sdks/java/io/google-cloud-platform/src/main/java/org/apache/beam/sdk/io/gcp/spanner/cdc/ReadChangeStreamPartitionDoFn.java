/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.sdk.io.gcp.spanner.cdc;

import static org.apache.beam.sdk.io.gcp.spanner.cdc.ChangeStreamMetrics.PARTITION_ID_ATTRIBUTE_LABEL;

import io.opencensus.common.Scope;
import io.opencensus.trace.AttributeValue;
import io.opencensus.trace.Tracer;
import io.opencensus.trace.Tracing;
import java.io.Serializable;
import org.apache.beam.sdk.io.gcp.spanner.cdc.actions.ActionFactory;
import org.apache.beam.sdk.io.gcp.spanner.cdc.actions.ChildPartitionsRecordAction;
import org.apache.beam.sdk.io.gcp.spanner.cdc.actions.DataChangeRecordAction;
import org.apache.beam.sdk.io.gcp.spanner.cdc.actions.DeletePartitionAction;
import org.apache.beam.sdk.io.gcp.spanner.cdc.actions.DonePartitionAction;
import org.apache.beam.sdk.io.gcp.spanner.cdc.actions.FinishPartitionAction;
import org.apache.beam.sdk.io.gcp.spanner.cdc.actions.HeartbeatRecordAction;
import org.apache.beam.sdk.io.gcp.spanner.cdc.actions.QueryChangeStreamAction;
import org.apache.beam.sdk.io.gcp.spanner.cdc.actions.WaitForChildPartitionsAction;
import org.apache.beam.sdk.io.gcp.spanner.cdc.actions.WaitForParentPartitionsAction;
import org.apache.beam.sdk.io.gcp.spanner.cdc.dao.ChangeStreamDao;
import org.apache.beam.sdk.io.gcp.spanner.cdc.dao.DaoFactory;
import org.apache.beam.sdk.io.gcp.spanner.cdc.dao.PartitionMetadataDao;
import org.apache.beam.sdk.io.gcp.spanner.cdc.mapper.ChangeStreamRecordMapper;
import org.apache.beam.sdk.io.gcp.spanner.cdc.mapper.MapperFactory;
import org.apache.beam.sdk.io.gcp.spanner.cdc.mapper.RestrictionMapper;
import org.apache.beam.sdk.io.gcp.spanner.cdc.model.DataChangeRecord;
import org.apache.beam.sdk.io.gcp.spanner.cdc.model.PartitionMetadata;
import org.apache.beam.sdk.io.gcp.spanner.cdc.restriction.PartitionMode;
import org.apache.beam.sdk.io.gcp.spanner.cdc.restriction.PartitionPosition;
import org.apache.beam.sdk.io.gcp.spanner.cdc.restriction.PartitionRestriction;
import org.apache.beam.sdk.io.gcp.spanner.cdc.restriction.PartitionRestrictionMetadata;
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

// TODO: Add java docs
@UnboundedPerElement
public class ReadChangeStreamPartitionDoFn extends DoFn<PartitionMetadata, DataChangeRecord>
    implements Serializable {

  private static final long serialVersionUID = -7574596218085711975L;
  private static final Logger LOG = LoggerFactory.getLogger(ReadChangeStreamPartitionDoFn.class);
  private static final Tracer TRACER = Tracing.getTracer();

  private final DaoFactory daoFactory;
  private final MapperFactory mapperFactory;
  private final ActionFactory actionFactory;
  private final ChangeStreamMetrics metrics;

  private transient QueryChangeStreamAction queryChangeStreamAction;
  private transient WaitForChildPartitionsAction waitForChildPartitionsAction;
  private transient FinishPartitionAction finishPartitionAction;
  private transient WaitForParentPartitionsAction waitForParentPartitionsAction;
  private transient DeletePartitionAction deletePartitionAction;
  private transient DonePartitionAction donePartitionAction;
  private transient RestrictionMapper restrictionMapper;
  private transient PartitionMetadataDao partitionMetadataDao;

  public ReadChangeStreamPartitionDoFn(
      DaoFactory daoFactory,
      MapperFactory mapperFactory,
      ActionFactory actionFactory,
      ChangeStreamMetrics metrics) {
    this.daoFactory = daoFactory;
    this.mapperFactory = mapperFactory;
    this.actionFactory = actionFactory;
    this.metrics = metrics;
  }

  @GetInitialWatermarkEstimatorState
  public Instant getInitialWatermarkEstimatorState(
      @Element PartitionMetadata partition,
      @Timestamp Instant currentElementTimestamp) {
    LOG.info("[" + partition.getPartitionToken() + "] Initial watermark estimator state");
    return currentElementTimestamp;
  }

  @NewWatermarkEstimator
  public ManualWatermarkEstimator<Instant> newWatermarkEstimator(
      @Element PartitionMetadata partition,
      @WatermarkEstimatorState Instant watermarkEstimatorState) {
    LOG.info("[" + partition.getPartitionToken() + "] New watermark estimator");
    return new Manual(watermarkEstimatorState);
  }

  @GetInitialRestriction
  public PartitionRestriction initialRestriction(@Element PartitionMetadata partition) {
    final String token = partition.getPartitionToken();

    LOG.info("[" + token + "] Building initial restriction");
    final com.google.cloud.Timestamp restrictionInitializedAt = com.google.cloud.Timestamp.now();
    final com.google.cloud.Timestamp partitionCreatedAt = partition.getCreatedAt();
    final com.google.cloud.Timestamp partitionScheduledAt = partition.getScheduledAt();
    final com.google.cloud.Timestamp partitionRunningAt =
        partitionMetadataDao.updateToRunning(token);

    metrics.updatePartitionScheduledToRunning(
        new Duration(
            partitionScheduledAt.toSqlTimestamp().getTime(),
            partitionRunningAt.toSqlTimestamp().getTime()));

    final PartitionRestriction restriction = restrictionMapper.queryChangeStreamFrom(partition);

    final PartitionRestriction restrictionWithMetadata = restriction.withMetadata(
        PartitionRestrictionMetadata.newBuilder()
            .withPartitionToken(token)
            .withPartitionStartTimestamp(restriction.getStartTimestamp())
            .withPartitionEndTimestamp(restriction.getEndTimestamp())
            .withRestrictionInitializedAt(restrictionInitializedAt)
            .withPartitionCreatedAt(partitionCreatedAt)
            .withPartitionScheduledAt(partitionScheduledAt)
            .withPartitionRunningAt(partitionRunningAt)
            .build());

    LOG.info("[" + token + "] Restriction built successfully");
    return restrictionWithMetadata;
  }

  @NewTracker
  public PartitionRestrictionTracker newTracker(
      @Element PartitionMetadata partition, @Restriction PartitionRestriction restriction) {
    LOG.info("[" + partition.getPartitionToken() + "] New tracker");
    return new PartitionRestrictionTracker(restriction);
  }

  @Setup
  public void setup() {
    try {
      this.partitionMetadataDao = daoFactory.getPartitionMetadataDao();
      this.restrictionMapper = mapperFactory.restrictionMapper();
      final ChangeStreamDao changeStreamDao = daoFactory.getChangeStreamDao();
      final ChangeStreamRecordMapper changeStreamRecordMapper =
          mapperFactory.changeStreamRecordMapper();
      final DataChangeRecordAction dataChangeRecordAction = actionFactory.dataChangeRecordAction();
      final HeartbeatRecordAction heartbeatRecordAction =
          actionFactory.heartbeatRecordAction(metrics);
      final ChildPartitionsRecordAction childPartitionsRecordAction =
          actionFactory.childPartitionsRecordAction(partitionMetadataDao, metrics);

      this.queryChangeStreamAction =
          actionFactory.queryChangeStreamAction(
              changeStreamDao,
              partitionMetadataDao,
              changeStreamRecordMapper,
              dataChangeRecordAction,
              heartbeatRecordAction,
              childPartitionsRecordAction);
      this.waitForChildPartitionsAction =
          actionFactory.waitForChildPartitionsAction(partitionMetadataDao, Duration.millis(100));
      this.finishPartitionAction = actionFactory
          .finishPartitionAction(partitionMetadataDao, metrics);
      this.waitForParentPartitionsAction =
          actionFactory.waitForParentPartitionsAction(partitionMetadataDao, Duration.millis(100));
      this.deletePartitionAction = actionFactory.deletePartitionAction(partitionMetadataDao);
      this.donePartitionAction = actionFactory.donePartitionAction();
    } catch (Exception e) {
      LOG.error("Error during setup", e);
      throw e;
    }
  }

  // spotless:off
  /**
   * The following graph represents the state machine that will be followed by this function.
   * The states will be stored in the {@link PartitionRestriction} and claimed through the
   * {@link PartitionPosition}.
   *
   *                             HEARTBEAT RECORD
   *                           DATA CHANGE RECORD
   *                       CHILD PARTITION RECORD
   *                      |---------------------|
   *                      v                     |
   *           +---------------------+          |
   *           | QUERY_CHANGE_STREAM |----------|
   *           +---------------------+
   *                      | NO MORE RECORDS
   *                      v
   *         +---------------------------+
   *         | WAIT_FOR_CHILD_PARTITIONS |
   *         +---------------------------+
   *                      v
   *             +------------------+
   *             | FINISH_PARTITION |
   *             +------------------+
   *                      v
   *        +----------------------------+
   *        | WAIT_FOR_PARENT_PARTITIONS |
   *        +----------------------------+
   *                      v
   *             +------------------+
   *             | DELETE_PARTITION |
   *             +------------------+
   *                      v
   *                   +------+
   *                   | DONE |
   *                   +------+
   *
   */
  // spotless:on
  // TODO: Close DAOs on teardown
  @ProcessElement
  public ProcessContinuation processElement(
      @Element PartitionMetadata partition,
      RestrictionTracker<PartitionRestriction, PartitionPosition> tracker,
      OutputReceiver<DataChangeRecord> receiver,
      ManualWatermarkEstimator<Instant> watermarkEstimator) {

    final String token = partition.getPartitionToken();
    LOG.info(
        "[" + token + "] Processing element with watermark "
            + watermarkEstimator.currentWatermark()
            + " and restriction "
            + tracker.currentRestriction());

    try (Scope scope =
        TRACER
            .spanBuilder("ReadChangeStreamPartitionDoFn.processElement")
            .setRecordEvents(true)
            .startScopedSpan()) {
      TRACER
          .getCurrentSpan()
          .putAttribute(
              PARTITION_ID_ATTRIBUTE_LABEL,
              AttributeValue.stringAttributeValue(partition.getPartitionToken()));

      final PartitionMode mode = tracker.currentRestriction().getMode();
      switch (mode) {
        case QUERY_CHANGE_STREAM:
          return queryChangeStream(partition, tracker, receiver, watermarkEstimator);
        case WAIT_FOR_CHILD_PARTITIONS:
          return waitForChildPartitions(partition, tracker);
        case FINISH_PARTITION:
          return finishPartition(partition, tracker);
        case WAIT_FOR_PARENT_PARTITIONS:
          return waitForParentPartitions(partition, tracker);
        case DELETE_PARTITION:
          return deletePartition(partition, tracker);
        case DONE:
          return done(partition, tracker);
        default:
          // TODO: Verify what to do here
          LOG.error("[" + token + "] Unknown mode " + mode);
          throw new IllegalArgumentException("Unknown mode " + mode);
      }
    }
  }

  private ProcessContinuation queryChangeStream(
      PartitionMetadata partition,
      RestrictionTracker<PartitionRestriction, PartitionPosition> tracker,
      OutputReceiver<DataChangeRecord> receiver,
      ManualWatermarkEstimator<Instant> watermarkEstimator) {
    return queryChangeStreamAction
        .run(partition, tracker, receiver, watermarkEstimator)
        .orElseGet(() -> waitForChildPartitions(partition, tracker));
  }

  private ProcessContinuation waitForChildPartitions(
      PartitionMetadata partition,
      RestrictionTracker<PartitionRestriction, PartitionPosition> tracker) {
    return waitForChildPartitionsAction
        .run(partition, tracker)
        .orElseGet(() -> finishPartition(partition, tracker));
  }

  private ProcessContinuation finishPartition(
      PartitionMetadata partition,
      RestrictionTracker<PartitionRestriction, PartitionPosition> tracker) {
    return finishPartitionAction
        .run(partition, tracker)
        .orElseGet(() -> waitForParentPartitions(partition, tracker));
  }

  private ProcessContinuation waitForParentPartitions(
      PartitionMetadata partition,
      RestrictionTracker<PartitionRestriction, PartitionPosition> tracker) {
    return waitForParentPartitionsAction
        .run(partition, tracker)
        .orElseGet(() -> deletePartition(partition, tracker));
  }

  private ProcessContinuation deletePartition(
      PartitionMetadata partition,
      RestrictionTracker<PartitionRestriction, PartitionPosition> tracker) {
    return deletePartitionAction.run(partition, tracker).orElseGet(() -> done(partition, tracker));
  }

  private ProcessContinuation done(
      PartitionMetadata partition,
      RestrictionTracker<PartitionRestriction, PartitionPosition> tracker) {
    return donePartitionAction.run(partition, tracker);
  }
}
