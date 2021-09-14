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
package org.apache.beam.sdk.io.gcp.spanner.cdc.restriction;

import com.google.cloud.Timestamp;
import org.apache.beam.sdk.transforms.splittabledofn.SplitResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// TODO: Add javadocs
public class PartitionRestrictionSplitter {

  private static final Logger LOG = LoggerFactory.getLogger(PartitionRestrictionSplitter.class);

  public SplitResult<PartitionRestriction> trySplit(
      double fractionOfRemainder,
      PartitionPosition lastClaimedPosition,
      PartitionRestriction restriction) {
    if (lastClaimedPosition == null) {
      return null;
    }

    final String token = restriction.getMetadata().getPartitionToken();
    final PartitionMode positionMode = lastClaimedPosition.getMode();
    final Timestamp startTimestamp = restriction.getStartTimestamp();
    final Timestamp endTimestamp = restriction.getEndTimestamp();

    SplitResult<PartitionRestriction> splitResult;
    switch (positionMode) {
      case QUERY_CHANGE_STREAM:
        // Does not allow splitting in query change stream (checkpointing is done manually)
        return null;
      case WAIT_FOR_CHILD_PARTITIONS:
        // If we need to split the wait for child partitions, we remain at the same mode. That is
        // because the primary restriction might resume and it might so happen that the residual
        // restriction gets scheduled before the primary.
        splitResult =
            SplitResult.of(
                PartitionRestriction.stop(restriction),
                PartitionRestriction.waitForChildPartitions(startTimestamp, endTimestamp)
                    .withMetadata(restriction.getMetadata()));
        break;
      case FINISH_PARTITION:
        splitResult =
            SplitResult.of(
                PartitionRestriction.stop(restriction),
                PartitionRestriction.waitForParentPartitions(startTimestamp, endTimestamp)
                    .withMetadata(restriction.getMetadata()));
        break;
      case WAIT_FOR_PARENT_PARTITIONS:
        // If we need to split the wait for parent partitions, we remain at the same mode. That is
        // because the primary restriction might resume and it might so happen that the residual
        // restriction gets scheduled before the primary.
        splitResult =
            SplitResult.of(
                PartitionRestriction.stop(restriction),
                PartitionRestriction.waitForParentPartitions(startTimestamp, endTimestamp)
                    .withMetadata(restriction.getMetadata()));
        break;
      case DELETE_PARTITION:
        splitResult =
            SplitResult.of(
                PartitionRestriction.stop(restriction),
                PartitionRestriction.done(startTimestamp, endTimestamp)
                    .withMetadata(restriction.getMetadata()));
        break;
      case DONE:
        return null;
      case STOP:
        splitResult = null;
        break;
      default:
        // TODO: See if we need to throw or do something else
        throw new IllegalArgumentException("Unknown mode " + positionMode);
    }

    LOG.debug(
        "["
            + token
            + "] Split result for ("
            + fractionOfRemainder
            + ", "
            + lastClaimedPosition
            + ", "
            + restriction
            + ") is "
            + splitResult);
    return splitResult;
  }
}
