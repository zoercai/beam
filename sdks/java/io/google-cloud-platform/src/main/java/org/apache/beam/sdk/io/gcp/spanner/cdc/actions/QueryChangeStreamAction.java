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
package org.apache.beam.sdk.io.gcp.spanner.cdc.actions;

import static org.apache.beam.sdk.io.gcp.spanner.cdc.ChangeStreamMetrics.PARTITION_ID_ATTRIBUTE_LABEL;

import com.google.cloud.Timestamp;
import com.google.cloud.spanner.ErrorCode;
import com.google.cloud.spanner.SpannerException;
import io.opencensus.common.Scope;
import io.opencensus.trace.AttributeValue;
import io.opencensus.trace.Tracer;
import io.opencensus.trace.Tracing;
import java.util.List;
import java.util.Optional;
import org.apache.beam.sdk.io.gcp.spanner.cdc.dao.ChangeStreamDao;
import org.apache.beam.sdk.io.gcp.spanner.cdc.dao.ChangeStreamResultSet;
import org.apache.beam.sdk.io.gcp.spanner.cdc.dao.PartitionMetadataDao;
import org.apache.beam.sdk.io.gcp.spanner.cdc.dao.PartitionMetadataDao.TransactionResult;
import org.apache.beam.sdk.io.gcp.spanner.cdc.mapper.ChangeStreamRecordMapper;
import org.apache.beam.sdk.io.gcp.spanner.cdc.model.ChangeStreamRecord;
import org.apache.beam.sdk.io.gcp.spanner.cdc.model.ChildPartitionsRecord;
import org.apache.beam.sdk.io.gcp.spanner.cdc.model.DataChangeRecord;
import org.apache.beam.sdk.io.gcp.spanner.cdc.model.HeartbeatRecord;
import org.apache.beam.sdk.io.gcp.spanner.cdc.model.PartitionMetadata;
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
public class QueryChangeStreamAction {

  private static final Logger LOG = LoggerFactory.getLogger(QueryChangeStreamAction.class);
  private static final Tracer TRACER = Tracing.getTracer();
  private static final String OUT_OF_RANGE_ERROR_MESSAGE = "Specified start_timestamp is invalid";

  private final ChangeStreamDao changeStreamDao;
  private final PartitionMetadataDao partitionMetadataDao;
  private final ChangeStreamRecordMapper changeStreamRecordMapper;
  private final DataChangeRecordAction dataChangeRecordAction;
  private final HeartbeatRecordAction heartbeatRecordAction;
  private final ChildPartitionsRecordAction childPartitionsRecordAction;

  public QueryChangeStreamAction(
      ChangeStreamDao changeStreamDao,
      PartitionMetadataDao partitionMetadataDao,
      ChangeStreamRecordMapper changeStreamRecordMapper,
      DataChangeRecordAction dataChangeRecordAction,
      HeartbeatRecordAction heartbeatRecordAction,
      ChildPartitionsRecordAction childPartitionsRecordAction) {
    this.changeStreamDao = changeStreamDao;
    this.partitionMetadataDao = partitionMetadataDao;
    this.changeStreamRecordMapper = changeStreamRecordMapper;
    this.dataChangeRecordAction = dataChangeRecordAction;
    this.heartbeatRecordAction = heartbeatRecordAction;
    this.childPartitionsRecordAction = childPartitionsRecordAction;
  }

  public Optional<ProcessContinuation> run(
      PartitionMetadata partition,
      RestrictionTracker<PartitionRestriction, PartitionPosition> tracker,
      OutputReceiver<DataChangeRecord> receiver,
      ManualWatermarkEstimator<Instant> watermarkEstimator) {
    final String token = partition.getPartitionToken();
    final Timestamp partitionEndTimestamp = partition.getEndTimestamp();
    final Timestamp restrictionStartTimestamp = tracker.currentRestriction().getStartTimestamp();
    final Timestamp restrictionEndTimestamp = tracker.currentRestriction().getEndTimestamp();

    try (Scope scope =
        TRACER.spanBuilder("QueryChangeStreamAction").setRecordEvents(true).startScopedSpan()) {
      TRACER
          .getCurrentSpan()
          .putAttribute(
              PARTITION_ID_ATTRIBUTE_LABEL,
              AttributeValue.stringAttributeValue(partition.getPartitionToken()));

      partitionMetadataDao.updateQueryStartedAt(token);

      LOG.info(
          "["
              + token
              + "] Querying change stream for "
              + token
              + " ("
              + restrictionStartTimestamp
              + ","
              + restrictionEndTimestamp
              + ")");
      try (ChangeStreamResultSet resultSet =
          changeStreamDao.changeStreamQuery(
              token,
              restrictionStartTimestamp,
              partition.isInclusiveStart(),
              restrictionEndTimestamp,
              partition.isInclusiveEnd(),
              partition.getHeartbeatMillis())) {

        boolean isOutOfRange = false;
        long recordsProcessed = 0;
        try {
          while (resultSet.next()) {
            // TODO: Check what should we do if there is an error here
            final List<ChangeStreamRecord> records =
                changeStreamRecordMapper.toChangeStreamRecords(
                    partition.getPartitionToken(),
                    resultSet.getCurrentRowAsStruct(),
                    resultSet.getMetadata(),
                    tracker.currentRestriction().getMetadata());

            Optional<ProcessContinuation> maybeContinuation;
            for (final ChangeStreamRecord record : records) {
              recordsProcessed++;
              if (record instanceof DataChangeRecord) {
                maybeContinuation =
                    dataChangeRecordAction.run(
                        partition,
                        (DataChangeRecord) record,
                        tracker,
                        receiver,
                        watermarkEstimator);
              } else if (record instanceof HeartbeatRecord) {
                maybeContinuation =
                    heartbeatRecordAction.run(
                        partition, (HeartbeatRecord) record, tracker, watermarkEstimator);
              } else if (record instanceof ChildPartitionsRecord) {
                maybeContinuation =
                    childPartitionsRecordAction.run(
                        partition, (ChildPartitionsRecord) record, tracker, watermarkEstimator);
              } else {
                LOG.error("[" + token + "] Unknown record type " + record.getClass());
                // FIXME: Check what should we do if the record is unknown
                throw new IllegalArgumentException("Unknown record type " + record.getClass());
              }
              if (maybeContinuation.isPresent()) {
                LOG.error("[" + token + "] Continuation present, returning " + maybeContinuation);
                return maybeContinuation;
              }
            }
          }
        } catch (SpannerException e) {
          if (isTimestampOutOfRange(e)) {
            LOG.info("[" + token + "] query change stream is out of range for "
                + restrictionStartTimestamp + " to "
                + restrictionEndTimestamp + ", finishing stream");
            isOutOfRange = true;
          } else {
            LOG.error("[" + token + "] Error querying change stream", e);
            throw e;
          }
        }

        LOG.info("[" + token + "] Query change stream completed, updating watermark");

        final Instant previousWatermark = watermarkEstimator.currentWatermark();
        final boolean isStreamFinished =
            isOutOfRange || restrictionEndTimestamp.compareTo(partitionEndTimestamp) >= 0;
        final long finalRecordsProcessed = recordsProcessed;
        final TransactionResult<Optional<ProcessContinuation>> transactionResult =
            partitionMetadataDao.runInTransaction(
                transaction -> {
                  transaction.updateRecordsProcessed(token, finalRecordsProcessed);
                  if (isStreamFinished) {
                    final Instant currentWatermark = new Instant(
                        partitionEndTimestamp.toSqlTimestamp().getTime());
                    LOG.info("[" + token + "] Updating watermark from "
                        + previousWatermark
                        + " to "
                        + currentWatermark);
                    watermarkEstimator.setWatermark(currentWatermark);
                    transaction.updateCurrentWatermark(token, partitionEndTimestamp);
                    return Optional.empty();
                  } else {
                    final Instant currentWatermark = new Instant(
                        restrictionEndTimestamp.toSqlTimestamp().getTime());
                    LOG.info("[" + token + "] Updating watermark from "
                        + previousWatermark
                        + " to "
                        + currentWatermark);
                    watermarkEstimator.setWatermark(currentWatermark);
                    transaction.updateCurrentWatermark(token, restrictionEndTimestamp);
                    transaction.updateToCreated(token);
                    return Optional.of(ProcessContinuation.stop());
                  }
                });

        LOG.info("[" + token + "] Query change stream action completed successfully");
        return transactionResult.getResult();
      }
    }
  }

  private boolean isTimestampOutOfRange(SpannerException e) {
    return (e.getErrorCode() == ErrorCode.INVALID_ARGUMENT
            || e.getErrorCode() == ErrorCode.OUT_OF_RANGE)
        && e.getMessage().contains(OUT_OF_RANGE_ERROR_MESSAGE);
  }
}
