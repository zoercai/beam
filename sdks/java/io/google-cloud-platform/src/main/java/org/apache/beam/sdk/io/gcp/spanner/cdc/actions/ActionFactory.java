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

package org.apache.beam.sdk.io.gcp.spanner.cdc.actions;

import org.apache.beam.sdk.io.gcp.spanner.cdc.dao.PartitionMetadataDao;
import org.joda.time.Duration;

public class ActionFactory {

  public static DataChangesRecordAction dataChangesRecordAction() {
    return new DataChangesRecordAction();
  }

  public static HeartbeatRecordAction heartbeatRecordAction() {
    return new HeartbeatRecordAction();
  }

  public static ChildPartitionsRecordAction childPartitionsRecordAction(
      PartitionMetadataDao partitionMetadataDao,
      WaitForChildPartitionsAction waitForChildPartitionsAction) {
    return new ChildPartitionsRecordAction(partitionMetadataDao, waitForChildPartitionsAction);
  }

  public static FinishPartitionAction finishPartitionAction(
      PartitionMetadataDao partitionMetadataDao) {
    return new FinishPartitionAction(partitionMetadataDao);
  }

  public static WaitForChildPartitionsAction waitForChildPartitionsAction(
      PartitionMetadataDao partitionMetadataDao,
      Duration resumeDuration) {
    return new WaitForChildPartitionsAction(partitionMetadataDao, resumeDuration);
  }

  public static WaitForParentPartitionsAction waitForParentPartitionsAction(
      PartitionMetadataDao partitionMetadataDao,
      Duration resumeDuration) {
    return new WaitForParentPartitionsAction(partitionMetadataDao, resumeDuration);
  }

  public static DeletePartitionAction deletePartitionAction(
      PartitionMetadataDao partitionMetadataDao) {
    return new DeletePartitionAction(partitionMetadataDao);
  }
}