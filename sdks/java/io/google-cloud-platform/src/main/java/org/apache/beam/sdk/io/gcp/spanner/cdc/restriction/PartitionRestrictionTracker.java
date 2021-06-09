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

import static org.apache.beam.sdk.io.gcp.spanner.cdc.restriction.PartitionMode.DONE;
import static org.apache.beam.sdk.io.gcp.spanner.cdc.restriction.PartitionMode.PARTITION_QUERY;
import static org.apache.beam.sdk.io.gcp.spanner.cdc.restriction.PartitionMode.WAIT_FOR_CHILDREN;
import static org.apache.beam.sdk.io.gcp.spanner.cdc.restriction.PartitionMode.WAIT_FOR_PARENTS;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkState;

import com.google.cloud.Timestamp;
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
    final Timestamp timestamp = position.getTimestamp();
    final PartitionMode mode = position.getMode();
    checkArgument(
        lastClaimedTimestamp == null || timestamp.compareTo(lastClaimedTimestamp) >= 0,
        "Trying to claim timestamp %s while last claimed was %s.",
        position, lastClaimedTimestamp
    );
    checkArgument(
        timestamp.compareTo(restriction.getStartTimestamp()) >= 0,
        "Trying to claim timestamp %s before start timestamp %s.",
        timestamp, restriction.getStartTimestamp()
    );
    checkArgument(
        (lastClaimedMode == null && mode == PARTITION_QUERY) ||
            (lastClaimedMode == PARTITION_QUERY && mode == PARTITION_QUERY) ||
            (lastClaimedMode == PARTITION_QUERY && mode == DONE) ||
            (lastClaimedMode == PARTITION_QUERY && mode == WAIT_FOR_CHILDREN) ||
            (lastClaimedMode == WAIT_FOR_CHILDREN && mode == WAIT_FOR_PARENTS) ||
            (lastClaimedMode == WAIT_FOR_PARENTS && mode == DONE),
        "Invalid mode transition claim, from %s to %s",
        lastClaimedMode, mode
    );
    setLastClaimedTimestamp(timestamp);
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
        lastClaimedTimestamp.compareTo(Timestamp.MAX_VALUE) == 0,
        "Last attempted timestamp was %s. Final claim was never made.",
        lastClaimedTimestamp
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
