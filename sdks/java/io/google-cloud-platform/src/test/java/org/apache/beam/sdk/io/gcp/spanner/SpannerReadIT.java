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
package org.apache.beam.sdk.io.gcp.spanner;

import com.google.api.gax.longrunning.OperationFuture;
import com.google.cloud.spanner.Database;
import com.google.cloud.spanner.DatabaseAdminClient;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerOptions;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.TimestampBound;
import com.google.spanner.admin.database.v1.CreateDatabaseMetadata;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.io.gcp.spanner.SpannerTestUtils.SpannerTestPipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** End-to-end test of Cloud Spanner Source. */
@RunWith(JUnit4.class)
public class SpannerReadIT {
  @Rule public final transient TestPipeline p = TestPipeline.create();

  private Spanner spanner;
  private DatabaseAdminClient databaseAdminClient;
  private SpannerTestPipelineOptions options;
  private String databaseName;
  private String project;

  @Before
  public void setUp() throws Exception {
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

    OperationFuture<Database, CreateDatabaseMetadata> op =
        databaseAdminClient.createDatabase(
            options.getInstanceId(),
            databaseName,
            Collections.singleton(
                "CREATE TABLE "
                    + options.getTable()
                    + " ("
                    + "  Key           INT64,"
                    + "  Value         STRING(MAX),"
                    + ") PRIMARY KEY (Key)"));
    op.get();
    makeTestData();
  }

  @Test
  public void testRead() {

    SpannerConfig spannerConfig = createSpannerConfig();

    PCollectionView<Transaction> tx =
        p.apply(
            SpannerIO.createTransaction()
                .withSpannerConfig(spannerConfig)
                .withTimestampBound(TimestampBound.strong()));

    PCollection<Struct> output =
        p.apply(
            SpannerIO.read()
                .withSpannerConfig(spannerConfig)
                .withTable(options.getTable())
                .withColumns("Key", "Value")
                .withTransaction(tx));
    PAssert.thatSingleton(output.apply("Count rows", Count.globally())).isEqualTo(5L);
    p.run();
  }

  @Test
  public void testQuery() {
    SpannerConfig spannerConfig = createSpannerConfig();

    PCollectionView<Transaction> tx =
        p.apply(
            SpannerIO.createTransaction()
                .withSpannerConfig(spannerConfig)
                .withTimestampBound(TimestampBound.strong()));

    PCollection<Struct> output =
        p.apply(
            SpannerIO.read()
                .withSpannerConfig(spannerConfig)
                .withQuery("SELECT * FROM " + options.getTable())
                .withTransaction(tx));
    PAssert.thatSingleton(output.apply("Count rows", Count.globally())).isEqualTo(5L);
    p.run();
  }

  @Test
  public void testReadAllRecordsInDb() {
    SpannerConfig spannerConfig = createSpannerConfig();

    PCollectionView<Transaction> tx =
        p.apply(
            SpannerIO.createTransaction()
                .withSpannerConfig(spannerConfig)
                .withTimestampBound(TimestampBound.strong()));

    PCollection<Struct> allRecords =
        p.apply(
                SpannerIO.read()
                    .withSpannerConfig(spannerConfig)
                    .withBatching(false)
                    .withQuery(
                        "SELECT t.table_name FROM information_schema.tables AS t WHERE t"
                            + ".table_catalog = '' AND t.table_schema = ''"))
            .apply(
                MapElements.into(TypeDescriptor.of(ReadOperation.class))
                    .via(
                        (SerializableFunction<Struct, ReadOperation>)
                            input -> {
                              String tableName = input.getString(0);
                              return ReadOperation.create().withQuery("SELECT * FROM " + tableName);
                            }))
            .apply(SpannerIO.readAll().withTransaction(tx).withSpannerConfig(spannerConfig));

    PAssert.thatSingleton(allRecords.apply("Count rows", Count.globally())).isEqualTo(5L);
    p.run();
  }

  private void makeTestData() {
    DatabaseClient databaseClient = getDatabaseClient();

    List<Mutation> mutations = new ArrayList<>();
    for (int i = 0; i < 5L; i++) {
      mutations.add(
          Mutation.newInsertOrUpdateBuilder(options.getTable())
              .set("key")
              .to(i)
              .set("value")
              .to(RandomUtils.randomAlphaNumeric(100))
              .build());
    }

    databaseClient.writeAtLeastOnce(mutations);
  }

  private SpannerConfig createSpannerConfig() {
    return SpannerConfig.create()
        .withProjectId(project)
        .withInstanceId(options.getInstanceId())
        .withDatabaseId(databaseName);
  }

  private DatabaseClient getDatabaseClient() {
    return spanner.getDatabaseClient(DatabaseId.of(project, options.getInstanceId(), databaseName));
  }

  @After
  public void tearDown() {
    databaseAdminClient.dropDatabase(options.getInstanceId(), databaseName);
    spanner.close();
  }
}
