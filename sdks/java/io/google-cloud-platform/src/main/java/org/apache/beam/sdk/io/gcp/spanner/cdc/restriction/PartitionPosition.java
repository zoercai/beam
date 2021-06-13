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

package org.apache.beam.sdk.io.gcp.spanner.cdc.restriction;

import com.google.cloud.Timestamp;
import com.google.common.annotations.VisibleForTesting;
import java.io.Serializable;
import java.util.Objects;
import java.util.Optional;

public class PartitionPosition implements Serializable {

  private static final long serialVersionUID = -9088898012221404492L;
  private final Optional<Timestamp> maybeTimestamp;
  private final PartitionMode mode;

  public static PartitionPosition queryChangeStream(Timestamp timestamp) {
    return new PartitionPosition(timestamp, PartitionMode.QUERY_CHANGE_STREAM);
  }

  public static PartitionPosition waitForChildPartitions() {
    return new PartitionPosition(PartitionMode.WAIT_FOR_CHILD_PARTITIONS);
  }

  public static PartitionPosition finishPartition() {
    return new PartitionPosition(PartitionMode.FINISH_PARTITION);
  }

  public static PartitionPosition waitForParentPartitions() {
    return new PartitionPosition(PartitionMode.WAIT_FOR_PARENT_PARTITIONS);
  }

  public static PartitionPosition deletePartition() {
    return new PartitionPosition(PartitionMode.DELETE_PARTITION);
  }

  public static PartitionPosition done() {
    return new PartitionPosition(PartitionMode.DONE);
  }

  @VisibleForTesting
  protected PartitionPosition(Timestamp timestamp, PartitionMode mode) {
    this.maybeTimestamp = Optional.ofNullable(timestamp);
    this.mode = mode;
  }

  @VisibleForTesting
  protected PartitionPosition(PartitionMode mode) {
    this(null, mode);
  }

  public Optional<Timestamp> getTimestamp() {
    return maybeTimestamp;
  }

  public PartitionMode getMode() {
    return mode;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof PartitionPosition)) {
      return false;
    }
    PartitionPosition that = (PartitionPosition) o;
    return Objects.equals(maybeTimestamp, that.maybeTimestamp) &&
        mode == that.mode;
  }

  @Override
  public int hashCode() {
    return Objects.hash(maybeTimestamp, mode);
  }

  @Override
  public String toString() {
    return "PartitionPosition{" +
        "maybeTimestamp=" + maybeTimestamp +
        ", mode=" + mode +
        '}';
  }
}
