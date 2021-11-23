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
package org.apache.beam.sdk.io.gcp.spanner.changestreams.dao;

import com.google.cloud.spanner.DatabaseAdminClient;
import com.google.cloud.spanner.Options.RpcPriority;
import java.io.Serializable;
import org.apache.beam.sdk.io.gcp.spanner.SpannerAccessor;
import org.apache.beam.sdk.io.gcp.spanner.SpannerConfig;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.mapper.MapperFactory;

/**
 * Factory class to create data access objects to perform change stream queries and access the
 * metadata tables. The instances created are all singletons.
 */
public class DaoFactory implements Serializable {

  private static final long serialVersionUID = 7929063669009832487L;

  private static PartitionMetadataAdminDao partitionMetadataAdminDao;
  private static PartitionMetricsAdminDao partitionMetricsAdminDao;
  private static PartitionMetadataDao partitionMetadataDaoInstance;
  private static ChangeStreamDao changeStreamDaoInstance;

  private final SpannerConfig changeStreamSpannerConfig;
  private final SpannerConfig metadataSpannerConfig;

  private final String changeStreamName;
  private final String partitionMetadataTableName;
  private final String partitionMetricsTableName;
  private final MapperFactory mapperFactory;
  private final RpcPriority rpcPriority;
  private final String jobName;

  /**
   * Constructs a {@link DaoFactory} with the configuration to be used for the underlying instances.
   *
   * @param changeStreamSpannerConfig the configuration for the change streams DAO
   * @param changeStreamName the name of the change stream for the change streams DAO
   * @param metadataSpannerConfig the metadata tables configuration
   * @param partitionMetadataTableName the name of the created partition metadata table
   * @param partitionMetricsTableName the name of the created partition metrics table
   * @param mapperFactory factory class to map change streams records into the Connectors domain
   * @param rpcPriority the priority of the requests made by the DAO queries
   * @param jobName the name of the running job
   */
  public DaoFactory(
      SpannerConfig changeStreamSpannerConfig,
      String changeStreamName,
      SpannerConfig metadataSpannerConfig,
      String partitionMetadataTableName,
      String partitionMetricsTableName,
      MapperFactory mapperFactory,
      RpcPriority rpcPriority,
      String jobName) {
    this.changeStreamSpannerConfig = changeStreamSpannerConfig;
    this.changeStreamName = changeStreamName;
    this.metadataSpannerConfig = metadataSpannerConfig;
    this.partitionMetadataTableName = partitionMetadataTableName;
    this.partitionMetricsTableName = partitionMetricsTableName;
    this.mapperFactory = mapperFactory;
    this.rpcPriority = rpcPriority;
    this.jobName = jobName;
  }

  /**
   * Creates and returns a singleton DAO instance for admin operations over the partition metadata
   * table.
   *
   * <p>This method is thread safe.
   *
   * @return singleton instance of the {@link PartitionMetadataDao}
   */
  public synchronized PartitionMetadataAdminDao getPartitionMetadataAdminDao() {
    if (partitionMetadataAdminDao == null) {
      DatabaseAdminClient databaseAdminClient =
          SpannerAccessor.getOrCreate(metadataSpannerConfig).getDatabaseAdminClient();
      partitionMetadataAdminDao =
          new PartitionMetadataAdminDao(
              databaseAdminClient,
              metadataSpannerConfig.getInstanceId().get(),
              metadataSpannerConfig.getDatabaseId().get(),
              partitionMetadataTableName);
    }
    return partitionMetadataAdminDao;
  }

  /**
   * Creates and returns a singleton DAO instance for admin operations over the partition metrics
   * table.
   *
   * <p>This method is thread safe.
   *
   * @return singleton instance of the {@link PartitionMetricsAdminDao}
   */
  public synchronized PartitionMetricsAdminDao getPartitionMetricsAdminDao() {
    if (partitionMetricsAdminDao == null) {
      DatabaseAdminClient databaseAdminClient =
          SpannerAccessor.getOrCreate(metadataSpannerConfig).getDatabaseAdminClient();
      partitionMetricsAdminDao =
          new PartitionMetricsAdminDao(
              databaseAdminClient,
              metadataSpannerConfig.getInstanceId().get(),
              metadataSpannerConfig.getDatabaseId().get(),
              partitionMetricsTableName);
    }
    return partitionMetricsAdminDao;
  }

  /**
   * Creates and returns a singleton DAO instance for accessing the partition metadata table.
   *
   * <p>This method is thread safe.
   *
   * @return singleton instance of the {@link PartitionMetadataDao}
   */
  public synchronized PartitionMetadataDao getPartitionMetadataDao() {
    final SpannerAccessor spannerAccessor = SpannerAccessor.getOrCreate(metadataSpannerConfig);
    if (partitionMetadataDaoInstance == null) {
      partitionMetadataDaoInstance =
          new PartitionMetadataDao(
              this.partitionMetadataTableName,
              this.partitionMetricsTableName,
              spannerAccessor.getDatabaseClient(),
              mapperFactory.partitionMetadataMapper());
    }
    return partitionMetadataDaoInstance;
  }

  /**
   * Creates and returns a singleton DAO instance for querying a partition change stream.
   *
   * <p>This method is thread safe.
   *
   * @return singleton instance of the {@link ChangeStreamDao}
   */
  public synchronized ChangeStreamDao getChangeStreamDao() {
    final SpannerAccessor spannerAccessor = SpannerAccessor.getOrCreate(changeStreamSpannerConfig);
    if (changeStreamDaoInstance == null) {
      changeStreamDaoInstance =
          new ChangeStreamDao(
              this.changeStreamName, spannerAccessor.getDatabaseClient(), rpcPriority, jobName);
    }
    return changeStreamDaoInstance;
  }
}