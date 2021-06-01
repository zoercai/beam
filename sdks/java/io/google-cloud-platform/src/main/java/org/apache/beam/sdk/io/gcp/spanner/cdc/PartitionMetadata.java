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

import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Mutation;
import java.util.List;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Objects;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions;

/**
 * Model for the partition metadata database table used in the Connector.
 */
public class PartitionMetadata {

  public enum State {
    // The partition has been discovered and is waiting to be started
    CREATED,
    // The partition has started and is being processed
    SCHEDULED,
    // The partition has ended
    FINISHED
  }

  // Metadata table column names
  static final String COLUMN_PARTITION_TOKEN = "PartitionToken";
  static final String COLUMN_PARENT_TOKEN = "ParentToken";
  static final String COLUMN_START_TIMESTAMP = "StartTimestamp";
  static final String COLUMN_INCLUSIVE_START = "InclusiveStart";
  static final String COLUMN_END_TIMESTAMP = "EndTimestamp";
  static final String COLUMN_INCLUSIVE_END = "InclusiveEnd";
  static final String COLUMN_HEARTBEAT_SECONDS = "HeartbeatSeconds";
  static final String COLUMN_STATE = "State";
  static final String COLUMN_CREATED_AT = "CreatedAt";
  static final String COLUMN_UPDATED_AT = "UpdatedAt";

  // Unique partition token, obtained from the Child Partition record from the Change Streams API
  // call.
  private String partitionToken;
  // Unique partition token of the parents that generated this partition.
  private List<String> parentTokens;
  // Start timestamp, used to query the partition.
  private Timestamp startTimestamp;
  // Whether the start timestamp is inclusive or exclusive.
  private boolean inclusiveStart;
  // The end timestamp, used to query the partition
  private Timestamp endTimestamp;
  // Whether the end timestamp is inclusive or exclusive.
  private boolean inclusiveEnd;
  // The interval for a heartbeat record to be returned for a partition when there are no changes
  // within the partition.
  private long heartbeatSeconds;
  // The current state of the partition in the Connector.
  private State state;
  // When the row was inserted.
  private Timestamp createdAt;
  // When the row was updated.
  private Timestamp updatedAt;

  PartitionMetadata(
      String partitionToken,
      List<String> parentTokens,
      Timestamp startTimestamp,
      boolean inclusiveStart,
      Timestamp endTimestamp,
      boolean inclusiveEnd,
      long heartbeatSeconds,
      State state,
      Timestamp createdAt,
      Timestamp updatedAt) {
    this.partitionToken = partitionToken;
    this.parentTokens = parentTokens;
    this.startTimestamp = startTimestamp;
    this.inclusiveStart = inclusiveStart;
    this.endTimestamp = endTimestamp;
    this.inclusiveEnd = inclusiveEnd;
    this.heartbeatSeconds = heartbeatSeconds;
    this.state = state;
    this.createdAt = createdAt;
    this.updatedAt = updatedAt;
  }

  public String getPartitionToken() {
    return partitionToken;
  }

  public void setPartitionToken(String partitionToken) {
    this.partitionToken = partitionToken;
  }

  public List<String> getParentTokens() {
    return parentTokens;
  }

  public void setParentTokens(List<String> parentTokens) {
    this.parentTokens = parentTokens;
  }

  public Timestamp getStartTimestamp() {
    return startTimestamp;
  }

  public void setStartTimestamp(Timestamp startTimestamp) {
    this.startTimestamp = startTimestamp;
  }

  public boolean isInclusiveStart() {
    return inclusiveStart;
  }

  public void setInclusiveStart(boolean inclusiveStart) {
    this.inclusiveStart = inclusiveStart;
  }

  public Timestamp getEndTimestamp() {
    return endTimestamp;
  }

  public void setEndTimestamp(Timestamp endTimestamp) {
    this.endTimestamp = endTimestamp;
  }

  public boolean isInclusiveEnd() {
    return inclusiveEnd;
  }

  public void setInclusiveEnd(boolean inclusiveEnd) {
    this.inclusiveEnd = inclusiveEnd;
  }

  public long getHeartbeatSeconds() {
    return heartbeatSeconds;
  }

  public void setHeartbeatSeconds(long heartbeatSeconds) {
    this.heartbeatSeconds = heartbeatSeconds;
  }

  public State getState() {
    return state;
  }

  public void setState(State state) {
    this.state = state;
  }

  public Timestamp getCreatedAt() {
    return createdAt;
  }

  public void setCreatedAt(Timestamp createdAt) {
    this.createdAt = createdAt;
  }

  public Timestamp getUpdatedAt() {
    return updatedAt;
  }

  public void setUpdatedAt(Timestamp updatedAt) {
    this.updatedAt = updatedAt;
  }

  public Mutation toMutation(String table) {
    return Mutation.newInsertBuilder(table)
        .set(COLUMN_PARTITION_TOKEN)
        .to(getPartitionToken())
        .set(COLUMN_PARENT_TOKEN)
        .toStringArray(getParentTokens())
        .set(COLUMN_START_TIMESTAMP)
        .to(getStartTimestamp())
        .set(COLUMN_INCLUSIVE_START)
        .to(isInclusiveStart())
        .set(COLUMN_END_TIMESTAMP)
        .to(getEndTimestamp())
        .set(COLUMN_INCLUSIVE_END)
        .to(isInclusiveEnd())
        .set(COLUMN_HEARTBEAT_SECONDS)
        .to(getHeartbeatSeconds())
        .set(COLUMN_STATE)
        .to(getState().toString())
        .set(COLUMN_CREATED_AT)
        .to(getCreatedAt())
        .set(COLUMN_UPDATED_AT)
        .to(getUpdatedAt())
        .build();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    PartitionMetadata partitionMetadata = (PartitionMetadata) o;
    return isInclusiveStart() == partitionMetadata.isInclusiveStart()
        && isInclusiveEnd() == partitionMetadata.isInclusiveEnd()
        && getHeartbeatSeconds() == partitionMetadata.getHeartbeatSeconds()
        && Objects.equal(getPartitionToken(), partitionMetadata.getPartitionToken())
        && Objects.equal(getParentTokens(), partitionMetadata.getParentTokens())
        && Objects.equal(getStartTimestamp(), partitionMetadata.getStartTimestamp())
        && Objects.equal(getEndTimestamp(), partitionMetadata.getEndTimestamp())
        && getState() == partitionMetadata.getState()
        && Objects.equal(getCreatedAt(), partitionMetadata.getCreatedAt())
        && Objects.equal(getUpdatedAt(), partitionMetadata.getUpdatedAt());
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(
        getPartitionToken(),
        getParentTokens(),
        getStartTimestamp(),
        isInclusiveStart(),
        getEndTimestamp(),
        isInclusiveEnd(),
        getHeartbeatSeconds(),
        getState(),
        getCreatedAt(),
        getUpdatedAt());
  }

  public static PartitionMetadata.Builder newBuilder() {
    return new PartitionMetadata.Builder();
  }

  public static class Builder {

    private String partitionToken;
    private List<String> parentTokens;
    private Timestamp startTimestamp;
    private Boolean inclusiveStart;
    private Timestamp endTimestamp;
    private Boolean inclusiveEnd;
    private Long heartbeatSeconds;
    private State state;
    private Timestamp createdAt;
    private Timestamp updatedAt;

    public Builder setPartitionToken(String partitionToken) {
      this.partitionToken = partitionToken;
      return this;
    }

    public Builder setParentTokens(List<String> parentTokens) {
      this.parentTokens = parentTokens;
      return this;
    }

    public Builder setStartTimestamp(Timestamp startTimestamp) {
      this.startTimestamp = startTimestamp;
      return this;
    }

    public Builder setInclusiveStart(boolean inclusiveStart) {
      this.inclusiveStart = inclusiveStart;
      return this;
    }

    public Builder setEndTimestamp(Timestamp endTimestamp) {
      this.endTimestamp = endTimestamp;
      return this;
    }

    public Builder setInclusiveEnd(Boolean inclusiveEnd) {
      this.inclusiveEnd = inclusiveEnd;
      return this;
    }

    public Builder setHeartbeatSeconds(long heartbeatSeconds) {
      this.heartbeatSeconds = heartbeatSeconds;
      return this;
    }

    public Builder setState(State state) {
      this.state = state;
      return this;
    }

    public Builder setCreatedAt(Timestamp createdAt) {
      this.createdAt = createdAt;
      return this;
    }

    public Builder setUpdatedAt(Timestamp updatedAt) {
      this.updatedAt = updatedAt;
      return this;
    }

    public PartitionMetadata build() {
      Preconditions.checkState(partitionToken != null, "partitionToken");
      Preconditions.checkState(parentTokens != null, "parentTokens");
      Preconditions.checkState(startTimestamp != null, "startTimestamp");
      Preconditions.checkState(heartbeatSeconds != null, "heartbeatSeconds");
      Preconditions.checkState(state != null, "state");
      if (inclusiveStart == null) {
        inclusiveStart = true;
      }
      if (inclusiveEnd == null) {
        inclusiveEnd = false;
      }
      return new PartitionMetadata(
          partitionToken,
          parentTokens,
          startTimestamp,
          inclusiveStart,
          endTimestamp,
          inclusiveEnd,
          heartbeatSeconds,
          state,
          createdAt,
          updatedAt);
    }
  }
}
