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

import static org.apache.beam.sdk.io.gcp.spanner.cdc.TimestampConverter.timestampToMicros;
import static org.apache.beam.sdk.io.gcp.spanner.cdc.restriction.PartitionMode.DELETE_PARTITION;
import static org.apache.beam.sdk.io.gcp.spanner.cdc.restriction.PartitionMode.DONE;
import static org.apache.beam.sdk.io.gcp.spanner.cdc.restriction.PartitionMode.FINISH_PARTITION;
import static org.apache.beam.sdk.io.gcp.spanner.cdc.restriction.PartitionMode.QUERY_CHANGE_STREAM;
import static org.apache.beam.sdk.io.gcp.spanner.cdc.restriction.PartitionMode.STOP;
import static org.apache.beam.sdk.io.gcp.spanner.cdc.restriction.PartitionMode.WAIT_FOR_CHILD_PARTITIONS;
import static org.apache.beam.sdk.io.gcp.spanner.cdc.restriction.PartitionMode.WAIT_FOR_PARENT_PARTITIONS;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.apache.beam.sdk.io.gcp.spanner.cdc.TimestampConverter;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker.Progress;

public class PartitionRestrictionProgressChecker {

  private static final BigDecimal TOTAL_MODE_TRANSITIONS = BigDecimal.valueOf(5L);

  /**
   * Indicates the work left from mode transitions (including the current state). The transitions
   * are as follows: (1) QUERY_CHANGE_STREAM, (2) WAIT_FOR_CHILD_PARTITIONS, (3) FINISH_PARTITION,
   * (4) WAIT_FOR_PARENT_PARTITIONS, (5) DELETE_PARTITION, (6) DONE.
   *
   * - QUERY_CHANGE_STREAM: 6 mode transitions until done. - WAIT_FOR_CHILD_PARTITIONS: 5 mode
   * transitions until done. - FINISH_PARTITION: 4 mode transitions until done. -
   * WAIT_FOR_PARENT_PARTITIONS: 3 mode transitions until done. - DELETE_PARTITION: 2 mode
   * transitions until done.
   */
  private final Map<PartitionMode, BigDecimal> modeToTransitionsCompleted;

  public PartitionRestrictionProgressChecker() {
    modeToTransitionsCompleted = new HashMap<>();
    modeToTransitionsCompleted.put(QUERY_CHANGE_STREAM, BigDecimal.valueOf(0L));
    modeToTransitionsCompleted.put(WAIT_FOR_CHILD_PARTITIONS, BigDecimal.valueOf(1L));
    modeToTransitionsCompleted.put(FINISH_PARTITION, BigDecimal.valueOf(2L));
    modeToTransitionsCompleted.put(WAIT_FOR_PARENT_PARTITIONS, BigDecimal.valueOf(3L));
    modeToTransitionsCompleted.put(DELETE_PARTITION, BigDecimal.valueOf(4L));
    modeToTransitionsCompleted.put(DONE, BigDecimal.valueOf(5L));
  }

  public Progress getProgress(
      PartitionRestriction restriction,
      PartitionPosition lastClaimedPosition) {
    final PartitionMode currentMode = Optional
        .ofNullable(lastClaimedPosition)
        .map(PartitionPosition::getMode)
        .orElse(restriction.getMode() == STOP ? restriction.getStoppedMode() : restriction.getMode());
    final BigDecimal transitionsCompleted = modeToTransitionsCompleted
        .getOrDefault(currentMode, BigDecimal.ZERO);

    final BigDecimal startTimestampAsMicros = timestampToMicros(restriction.getStartTimestamp());
    final BigDecimal endTimestampAsMicros = timestampToMicros(restriction.getEndTimestamp());
    final BigDecimal currentTimestampAsMicros = Optional
        .ofNullable(lastClaimedPosition)
        .flatMap(PartitionPosition::getTimestamp)
        .map(TimestampConverter::timestampToMicros)
        .orElse(currentMode == QUERY_CHANGE_STREAM ? startTimestampAsMicros : endTimestampAsMicros);

    final BigDecimal workCompleted = currentTimestampAsMicros
        .subtract(startTimestampAsMicros)
        .add(transitionsCompleted);
    final BigDecimal workLeft = endTimestampAsMicros
        .subtract(startTimestampAsMicros)
        .add(TOTAL_MODE_TRANSITIONS)
        .subtract(workCompleted);

    return Progress.from(workCompleted.doubleValue(), workLeft.doubleValue());
  }
}
