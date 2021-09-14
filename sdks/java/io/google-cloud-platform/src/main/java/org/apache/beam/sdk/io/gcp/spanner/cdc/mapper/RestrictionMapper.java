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
package org.apache.beam.sdk.io.gcp.spanner.cdc.mapper;

import com.google.cloud.Timestamp;
import java.math.BigDecimal;
import org.apache.beam.sdk.io.gcp.spanner.cdc.InitialPartition;
import org.apache.beam.sdk.io.gcp.spanner.cdc.TimestampConverter;
import org.apache.beam.sdk.io.gcp.spanner.cdc.model.PartitionMetadata;
import org.apache.beam.sdk.io.gcp.spanner.cdc.restriction.PartitionRestriction;
import org.joda.time.Duration;

public class RestrictionMapper {

  private final Timestamp queryInterval;

  public RestrictionMapper(Duration queryInterval) {
    this.queryInterval = TimestampConverter.timestampFromMillis(queryInterval.getMillis());
  }

  public PartitionRestriction queryChangeStreamFrom(PartitionMetadata partition) {
    if (InitialPartition.isInitialPartition(partition.getPartitionToken())) {
      final Timestamp startTimestamp = partition.getStartTimestamp();
      final Timestamp endTimestamp = partition.getEndTimestamp();

      return PartitionRestriction.queryChangeStream(startTimestamp, endTimestamp);
    } else {
      final Timestamp startTimestamp = startTimestampFrom(partition.getCurrentWatermark());
      final Timestamp endTimestamp = endTimestampFrom(startTimestamp, partition.getEndTimestamp());

      return PartitionRestriction.queryChangeStream(startTimestamp, endTimestamp);
    }
  }

  private Timestamp startTimestampFrom(Timestamp currentWatermark) {
    // TODO: Convert to nanos when the backend supports it
    return Timestamp.ofTimeMicroseconds(
        TimestampConverter.timestampToMicros(currentWatermark).add(BigDecimal.ONE).longValue());
  }

  private Timestamp endTimestampFrom(Timestamp startTimestamp, Timestamp endTimestamp) {
    final Timestamp futureTimestamp =
        Timestamp.ofTimeSecondsAndNanos(
            startTimestamp.getSeconds() + queryInterval.getSeconds(),
            startTimestamp.getNanos() + queryInterval.getNanos());

    if (futureTimestamp.compareTo(endTimestamp) >= 0) {
      return endTimestamp;
    } else {
      return futureTimestamp;
    }
  }
}
