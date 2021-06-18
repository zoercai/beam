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

import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Statement;
import org.apache.beam.sdk.io.gcp.spanner.SpannerAccessor;
import org.apache.beam.sdk.io.gcp.spanner.SpannerConfig;
import org.apache.beam.sdk.io.gcp.spanner.cdc.dao.PartitionMetadataDao;
import org.apache.beam.sdk.io.gcp.spanner.cdc.model.PartitionMetadata;
import org.apache.beam.sdk.io.range.OffsetRange;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFn.UnboundedPerElement;
import org.apache.beam.sdk.transforms.splittabledofn.GrowableOffsetRangeTracker;
import org.apache.beam.sdk.transforms.splittabledofn.OffsetRangeTracker;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.sdk.transforms.splittabledofn.WatermarkEstimator;
import org.apache.beam.sdk.transforms.splittabledofn.WatermarkEstimators.MonotonicallyIncreasing;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A SplittableDoFn which reads from {@link ChangeStreamSourceDescriptor} and outputs {@link
 * PartitionMetadata}.
 *
 * <p>{@link DetectNewPartitionsDoFn} implements the logic of querying the partition metadata table
 * from Cloud Spanner. The element is a {@link ChangeStreamSourceDescriptor}, and the restriction is
 * an {@link OffsetRange} which represents record offset. A {@link GrowableOffsetRangeTracker} is
 * used to track an {@link OffsetRange} ended with {@code Long.MAX_VALUE}. For a finite range, a
 * {@link OffsetRangeTracker} is created.
 */
@UnboundedPerElement
@SuppressWarnings({
  "rawtypes", // TODO(https://issues.apache.org/jira/browse/BEAM-10556)
  "nullness" // TODO(https://issues.apache.org/jira/browse/BEAM-10402)
})
public class DetectNewPartitionsDoFn extends DoFn<ChangeStreamSourceDescriptor, PartitionMetadata> {
  private static final Logger LOG = LoggerFactory.getLogger(DetectNewPartitionsDoFn.class);
  private SpannerAccessor spannerAccessor;
  private DatabaseClient databaseClient;
  private SpannerConfig spannerConfig;
  // TODO(hengfeng): Make this field configurable via constructor or spanner config.
  private Duration resumeDuration = Duration.millis(100L);;
  private String metadataTableName;
  private PartitionMetadataDao partitionMetadataDao;

  public DetectNewPartitionsDoFn(SpannerConfig config, String metadataTableName) {
    this.spannerConfig = config;
    this.metadataTableName = metadataTableName;
  }

  public DetectNewPartitionsDoFn(
      SpannerConfig config, String metadataTableName, Duration resumeDuration) {
    this(config, metadataTableName);
    this.resumeDuration = resumeDuration;
  }

  @GetInitialRestriction
  public OffsetRange initialRestriction(@Element ChangeStreamSourceDescriptor inputElement) {
    return new OffsetRange(0, Long.MAX_VALUE);
  }

  @NewTracker
  public OffsetRangeTracker restrictionTracker(
      @Element ChangeStreamSourceDescriptor inputElement, @Restriction OffsetRange restriction) {
    return new OffsetRangeTracker(new OffsetRange(restriction.getFrom(), Long.MAX_VALUE));
  }

  @ProcessElement
  public ProcessContinuation processElement(
      @Element ChangeStreamSourceDescriptor inputElement,
      RestrictionTracker<OffsetRange, Long> tracker,
      WatermarkEstimator watermarkEstimator,
      OutputReceiver<PartitionMetadata> receiver) {

    Instant start = Instant.now();

    LOG.info("Calling process element:" + start);

    // Find all records where their states are CREATED.
    // TODO(hengfeng): move this to DAO.
    String query =
        String.format("SELECT * FROM `%s` WHERE State = 'CREATED'", this.metadataTableName);
    ResultSet resultSet = this.databaseClient.singleUse().executeQuery(Statement.of(query));

    long currentIndex = tracker.currentRestriction().getFrom();

    // Output the records.
    while (resultSet.next()) {
      // TODO(hengfeng): change the log level in this file.
      LOG.info("Reading record currentIndex:" + currentIndex);
      if (!tracker.tryClaim(currentIndex)) {
        return ProcessContinuation.stop();
      }
      PartitionMetadata metadata = buildPartitionMetadata(resultSet);
      LOG.info(
          String.format("Get partition metadata currentIndex:%d meta:%s", currentIndex, metadata));

      currentIndex++;

      Instant now = Instant.now();
      LOG.info("Read watermark:" + watermarkEstimator.currentWatermark() + " now:" + now);
      receiver.output(metadata);

      // TODO(hengfeng): investigate if we can move this to DAO.
      this.databaseClient
          .readWriteTransaction()
          .run(
              transaction -> {
                // Update the record to SCHEDULED.
                // TODO(hengfeng): use mutations instead.
                Statement updateStatement =
                    Statement.newBuilder(
                            String.format(
                                "UPDATE `%s` "
                                    + "SET State = 'SCHEDULED' "
                                    + "WHERE PartitionToken = @PartitionToken",
                                this.metadataTableName))
                        .bind("PartitionToken")
                        .to(metadata.getPartitionToken())
                        .build();
                transaction.executeUpdate(updateStatement);
                LOG.info("Updated the record:" + metadata.getPartitionToken());
                return null;
              });
    }

    // TODO(hengfeng): investigate how we can terminate this function.
    return ProcessContinuation.resume().withResumeDelay(resumeDuration);
  }

  @Setup
  public void setup() throws Exception {
    this.spannerAccessor = SpannerAccessor.getOrCreate(this.spannerConfig);
    this.databaseClient = spannerAccessor.getDatabaseClient();
    this.partitionMetadataDao =
        new PartitionMetadataDao(this.databaseClient, this.metadataTableName);
  }

  @Teardown
  public void teardown() throws Exception {
    this.spannerAccessor.close();
  }

  @GetInitialWatermarkEstimatorState
  public Instant getInitialWatermarkEstimatorState(@Timestamp Instant currentElementTimestamp) {
    return currentElementTimestamp;
  }

  @NewWatermarkEstimator
  public WatermarkEstimator<Instant> newWatermarkEstimator(
      @WatermarkEstimatorState Instant watermarkEstimatorState) {
    return new MonotonicallyIncreasing(watermarkEstimatorState);
  }

  private PartitionMetadata buildPartitionMetadata(ResultSet resultSet) {
    return new PartitionMetadata(
        resultSet.getString(PartitionMetadataDao.COLUMN_PARTITION_TOKEN),
        resultSet.getStringList(PartitionMetadataDao.COLUMN_PARENT_TOKEN),
        resultSet.getTimestamp(PartitionMetadataDao.COLUMN_START_TIMESTAMP),
        resultSet.getBoolean(PartitionMetadataDao.COLUMN_INCLUSIVE_START),
        !resultSet.isNull(PartitionMetadataDao.COLUMN_END_TIMESTAMP)
            ? resultSet.getTimestamp(PartitionMetadataDao.COLUMN_END_TIMESTAMP)
            : null,
        resultSet.getBoolean(PartitionMetadataDao.COLUMN_INCLUSIVE_END),
        resultSet.getLong(PartitionMetadataDao.COLUMN_HEARTBEAT_SECONDS),
        PartitionMetadata.State.valueOf(resultSet.getString(PartitionMetadataDao.COLUMN_STATE)),
        resultSet.getTimestamp(PartitionMetadataDao.COLUMN_CREATED_AT),
        resultSet.getTimestamp(PartitionMetadataDao.COLUMN_UPDATED_AT));
  }
}
