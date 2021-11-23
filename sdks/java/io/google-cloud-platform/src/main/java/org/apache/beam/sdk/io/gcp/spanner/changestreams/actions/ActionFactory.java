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
package org.apache.beam.sdk.io.gcp.spanner.changestreams.actions;

import java.io.Serializable;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.ChangeStreamMetrics;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.dao.ChangeStreamDao;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.dao.PartitionMetadataDao;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.mapper.ChangeStreamRecordMapper;

/**
 * Factory class for creating instances that will handle each type of record within a change stream
 * query. The instances created are all singletons.
 */
public class ActionFactory implements Serializable {

  private static final long serialVersionUID = -4060958761369602619L;
  private static DataChangeRecordAction dataChangeRecordActionInstance;
  private static HeartbeatRecordAction heartbeatRecordActionInstance;
  private static ChildPartitionsRecordAction childPartitionsRecordActionInstance;
  private static QueryChangeStreamAction queryChangeStreamActionInstance;

  /**
   * Creates and returns a singleton instance of an action class capable of processing {@link
   * org.apache.beam.sdk.io.gcp.spanner.changestreams.model.DataChangeRecord}s.
   *
   * <p>This method is thread safe.
   *
   * @return singleton instance of the {@link DataChangeRecordAction}
   */
  public synchronized DataChangeRecordAction dataChangeRecordAction() {
    if (dataChangeRecordActionInstance == null) {
      dataChangeRecordActionInstance = new DataChangeRecordAction();
    }
    return dataChangeRecordActionInstance;
  }

  /**
   * Creates and returns a singleton instance of an action class capable of processing {@link
   * org.apache.beam.sdk.io.gcp.spanner.changestreams.model.HeartbeatRecord}s. This method is thread
   * safe.
   *
   * @param metrics metrics gathering class
   * @return singleton instance of the {@link HeartbeatRecordAction}
   */
  public synchronized HeartbeatRecordAction heartbeatRecordAction(ChangeStreamMetrics metrics) {
    if (heartbeatRecordActionInstance == null) {
      heartbeatRecordActionInstance = new HeartbeatRecordAction(metrics);
    }
    return heartbeatRecordActionInstance;
  }

  /**
   * Creates and returns a singleton instance of an action class capable of process {@link
   * org.apache.beam.sdk.io.gcp.spanner.changestreams.model.ChildPartitionsRecord}s. This method is
   * thread safe.
   *
   * @param partitionMetadataDao DAO class to access the Connector's metadata tables
   * @param metrics metrics gathering class
   * @return singleton instance of the {@link ChildPartitionsRecordAction}
   */
  public synchronized ChildPartitionsRecordAction childPartitionsRecordAction(
      PartitionMetadataDao partitionMetadataDao, ChangeStreamMetrics metrics) {
    if (childPartitionsRecordActionInstance == null) {
      childPartitionsRecordActionInstance =
          new ChildPartitionsRecordAction(partitionMetadataDao, metrics);
    }
    return childPartitionsRecordActionInstance;
  }

  /**
   * Creates and returns a single instance of an action class capable of performing a change stream
   * query for a given partition. It uses the {@link DataChangeRecordAction}, {@link
   * HeartbeatRecordAction} and {@link ChildPartitionsRecordAction} to dispatch the necessary
   * processing depending on the type of record received.
   *
   * @param changeStreamDao DAO class to perform a change stream query
   * @param partitionMetadataDao DAO class to access the Connector's metadata tables
   * @param changeStreamRecordMapper mapper class to transform change stream records into the
   *     Connector's domain models
   * @param dataChangeRecordAction action class to process {@link
   *     org.apache.beam.sdk.io.gcp.spanner.changestreams.model.DataChangeRecord}s
   * @param heartbeatRecordAction action class to process {@link
   *     org.apache.beam.sdk.io.gcp.spanner.changestreams.model.HeartbeatRecord}s
   * @param childPartitionsRecordAction action class to process {@link
   *     org.apache.beam.sdk.io.gcp.spanner.changestreams.model.ChildPartitionsRecord}s
   * @return single instance of the {@link QueryChangeStreamAction}
   */
  public synchronized QueryChangeStreamAction queryChangeStreamAction(
      ChangeStreamDao changeStreamDao,
      PartitionMetadataDao partitionMetadataDao,
      ChangeStreamRecordMapper changeStreamRecordMapper,
      DataChangeRecordAction dataChangeRecordAction,
      HeartbeatRecordAction heartbeatRecordAction,
      ChildPartitionsRecordAction childPartitionsRecordAction) {
    if (queryChangeStreamActionInstance == null) {
      queryChangeStreamActionInstance =
          new QueryChangeStreamAction(
              changeStreamDao,
              partitionMetadataDao,
              changeStreamRecordMapper,
              dataChangeRecordAction,
              heartbeatRecordAction,
              childPartitionsRecordAction);
    }
    return queryChangeStreamActionInstance;
  }
}