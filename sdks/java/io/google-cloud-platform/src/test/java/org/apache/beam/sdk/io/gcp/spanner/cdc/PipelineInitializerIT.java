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
package org.apache.beam.sdk.io.gcp.spanner.cdc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.google.api.gax.longrunning.OperationFuture;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Database;
import com.google.cloud.spanner.DatabaseAdminClient;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerOptions;
import com.google.cloud.spanner.Statement;
import com.google.common.collect.ImmutableList;
import com.google.spanner.admin.database.v1.CreateDatabaseMetadata;
import java.util.concurrent.ExecutionException;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.io.gcp.spanner.SpannerTestUtils;
import org.apache.beam.sdk.io.gcp.spanner.SpannerTestUtils.SpannerTestPipelineOptions;
import org.apache.beam.sdk.io.gcp.spanner.cdc.dao.PartitionMetadataDao;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.TestPipeline;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class PipelineInitializerIT {

  private static final String METADATA_TABLE_NAME = "CDC_Partitions_metadata_table";
  private static final String PARENT_PARTITION_ID = "Parent0";

  private Spanner spanner;
  private DatabaseAdminClient databaseAdminClient;
  private SpannerTestPipelineOptions options;
  private String databaseName;
  private String project;

  @Before
  public void setUp() throws ExecutionException, InterruptedException {
    PipelineOptionsFactory.register(SpannerTestPipelineOptions.class);
    options = TestPipeline.testingPipelineOptions().as(SpannerTestPipelineOptions.class);

    project = options.getInstanceProjectId();
    if (project == null) {
      project = options.as(GcpOptions.class).getProject();
    }

    spanner = SpannerOptions.newBuilder().setProjectId(project).build().getService();
    databaseName = SpannerTestUtils.generateDatabaseName(options.getDatabaseIdPrefix());
    databaseAdminClient = spanner.getDatabaseAdminClient();

    // Delete database if exists.
    databaseAdminClient.dropDatabase(options.getInstanceId(), databaseName);

    // Create the test database
    OperationFuture<Database, CreateDatabaseMetadata> op = databaseAdminClient
        .createDatabase(options.getInstanceId(), databaseName, ImmutableList.of());
    op.get();
  }

  @After
  public void tearDown() {
    databaseAdminClient.dropDatabase(options.getInstanceId(), databaseName);
    spanner.close();
  }

  @Test
  public void testInitialize() {
    DatabaseId databaseId = DatabaseId.of(project, options.getInstanceId(), databaseName);
    DatabaseClient databaseClient = spanner.getDatabaseClient(databaseId);
    PartitionMetadataDao partitionMetadataDao = new PartitionMetadataDao(
        databaseClient, METADATA_TABLE_NAME);
    PipelineInitializer
        .initialize(databaseAdminClient, partitionMetadataDao, databaseId, Timestamp.MIN_VALUE,
            null);

    try (ResultSet resultSet = databaseClient.singleUse()
        .executeQuery(Statement.newBuilder("SELECT * FROM " + METADATA_TABLE_NAME).build())) {
      assertTrue(resultSet.next());
      assertEquals(PARENT_PARTITION_ID, resultSet.getCurrentRowAsStruct().getString(0));
      assertFalse(resultSet.next());
    }
  }
}
