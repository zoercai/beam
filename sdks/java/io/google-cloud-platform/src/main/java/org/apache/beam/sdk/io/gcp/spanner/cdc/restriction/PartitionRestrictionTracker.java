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

import static org.apache.beam.sdk.io.gcp.spanner.cdc.restriction.PartitionMode.DELETE_PARTITION;
import static org.apache.beam.sdk.io.gcp.spanner.cdc.restriction.PartitionMode.DONE;
import static org.apache.beam.sdk.io.gcp.spanner.cdc.restriction.PartitionMode.FINISH_PARTITION;
import static org.apache.beam.sdk.io.gcp.spanner.cdc.restriction.PartitionMode.QUERY_CHANGE_STREAM;
import static org.apache.beam.sdk.io.gcp.spanner.cdc.restriction.PartitionMode.WAIT_FOR_CHILD_PARTITIONS;
import static org.apache.beam.sdk.io.gcp.spanner.cdc.restriction.PartitionMode.WAIT_FOR_PARENT_PARTITIONS;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkState;

import com.google.cloud.Timestamp;
import java.util.Optional;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.sdk.transforms.splittabledofn.SplitResult;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.checkerframework.checker.nullness.qual.Nullable;

// TODO: Add unit tests
public class PartitionRestrictionTracker
    extends RestrictionTracker<PartitionRestriction, PartitionPosition> {

  private final PartitionRestriction restriction;
  private @Nullable Timestamp lastClaimedTimestamp;
  private @Nullable PartitionMode lastClaimedMode;

  public PartitionRestrictionTracker(PartitionRestriction restriction) {
    this.restriction = restriction;
  }

  @Override
  public boolean tryClaim(PartitionPosition position) {
    final Optional<Timestamp> maybeTimestamp = position.getTimestamp();
    final PartitionMode mode = position.getMode();
    checkArgument(
        lastClaimedTimestamp == null || maybeTimestamp.map(t -> t.compareTo(lastClaimedTimestamp) >= 0).orElse(true),
        "Trying to claim timestamp %s while last claimed was %s.",
        position, lastClaimedTimestamp
    );
    checkArgument(
        maybeTimestamp.map(t -> t.compareTo(restriction.getStartTimestamp()) >= 0).orElse(true),
        "Trying to claim timestamp %s before start timestamp %s.",
        maybeTimestamp.orElse(null), restriction.getStartTimestamp()
    );
    checkArgument(
        (lastClaimedMode == null && mode == QUERY_CHANGE_STREAM) ||
            (lastClaimedMode == QUERY_CHANGE_STREAM && mode == QUERY_CHANGE_STREAM) ||
            (lastClaimedMode == QUERY_CHANGE_STREAM && mode == WAIT_FOR_CHILD_PARTITIONS) ||
            (lastClaimedMode == QUERY_CHANGE_STREAM && mode == FINISH_PARTITION) ||
            (lastClaimedMode == WAIT_FOR_CHILD_PARTITIONS && mode == FINISH_PARTITION) ||
            (lastClaimedMode == WAIT_FOR_PARENT_PARTITIONS && mode == DELETE_PARTITION) ||
            (lastClaimedMode == DELETE_PARTITION && mode == DONE),
        "Invalid mode transition claim, from %s to %s",
        lastClaimedMode, mode
    );
    maybeTimestamp.ifPresent(this::setLastClaimedTimestamp);
    setLastClaimedMode(mode);
    return true;
  }

  @Override
  public PartitionRestriction currentRestriction() {
    return restriction;
  }

  @Override
  public @Nullable SplitResult<PartitionRestriction> trySplit(
      double fractionOfRemainder) {
    // Always deny splitting
    return null;
  }

  @Override
  public void checkDone() throws IllegalStateException {
    checkState(
        lastClaimedTimestamp != null,
        "Last attempted timestamp should not be null. No work was claimed from timestamp %s.",
        restriction.getStartTimestamp()
    );
    checkState(
        lastClaimedMode != null,
        "Last attempted position mode should not be null. No work was claimed from timestamp %s.",
        restriction.getStartTimestamp()
    );
    checkState(
        lastClaimedMode == DONE,
        "Last attempted position mode was %s. Position was never marked as done.",
        lastClaimedMode
    );
  }

  @Override
  public IsBounded isBounded() {
    return IsBounded.UNBOUNDED;
  }

  @VisibleForTesting
  protected void setLastClaimedTimestamp(Timestamp timestamp) {
    this.lastClaimedTimestamp = timestamp;
  }

  @VisibleForTesting
  protected void setLastClaimedMode(PartitionMode mode) {
    this.lastClaimedMode = mode;
  }
}
