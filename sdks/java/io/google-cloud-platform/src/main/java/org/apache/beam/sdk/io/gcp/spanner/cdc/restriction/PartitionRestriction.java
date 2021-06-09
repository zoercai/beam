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

import static org.apache.beam.sdk.io.gcp.spanner.cdc.restriction.PartitionMode.PARTITION_QUERY;

import com.google.cloud.Timestamp;
import java.io.Serializable;
import java.util.Objects;

public class PartitionRestriction implements Serializable {

  private static final long serialVersionUID = -7009236776208644264L;

  private final Timestamp startTimestamp;
  private final PartitionMode mode;

  public PartitionRestriction(Timestamp startTimestamp) {
    this.startTimestamp = startTimestamp;
    this.mode = PARTITION_QUERY;
  }

  public Timestamp getStartTimestamp() {
    return startTimestamp;
  }

  public PartitionMode getMode() {
    return mode;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof PartitionRestriction)) {
      return false;
    }
    PartitionRestriction that = (PartitionRestriction) o;
    return Objects.equals(startTimestamp, that.startTimestamp) &&
        mode == that.mode;
  }

  @Override
  public int hashCode() {
    return Objects.hash(startTimestamp, mode);
  }

  @Override
  public String toString() {
    return "PartitionRestriction{" +
        "startTimestamp=" + startTimestamp +
        ", mode=" + mode +
        '}';
  }
}
