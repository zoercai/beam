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

public class PartitionPosition implements Serializable {

  private static final long serialVersionUID = -9088898012221404492L;
  private final Timestamp timestamp;
  private final PartitionMode mode;

  public static PartitionPosition continueQuery(Timestamp timestamp) {
    return new PartitionPosition(timestamp, PartitionMode.PARTITION_QUERY);
  }

  public static PartitionPosition waitForChildren(Timestamp timestamp) {
    return new PartitionPosition(timestamp, PartitionMode.WAIT_FOR_CHILDREN);
  }

  public static PartitionPosition waitForParents(Timestamp timestamp) {
    return new PartitionPosition(timestamp, PartitionMode.WAIT_FOR_PARENTS);
  }

  public static PartitionPosition done() {
    return new PartitionPosition(Timestamp.MAX_VALUE, PartitionMode.DONE);
  }

  @VisibleForTesting
  protected PartitionPosition(Timestamp timestamp, PartitionMode mode) {
    this.timestamp = timestamp;
    this.mode = mode;
  }

  public Timestamp getTimestamp() {
    return timestamp;
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
    return Objects.equals(timestamp, that.timestamp) &&
        mode == that.mode;
  }

  @Override
  public int hashCode() {
    return Objects.hash(timestamp, mode);
  }

  @Override
  public String toString() {
    return "PartitionPosition{" +
        "timestamp=" + timestamp +
        ", mode=" + mode +
        '}';
  }
}
