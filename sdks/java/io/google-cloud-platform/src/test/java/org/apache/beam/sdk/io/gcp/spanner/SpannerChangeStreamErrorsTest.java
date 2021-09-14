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

import com.google.api.gax.core.NoCredentialsProvider;
import com.google.api.gax.grpc.testing.MockServiceHelper;
import com.google.api.gax.longrunning.OperationFuture;
import com.google.api.gax.longrunning.OperationTimedPollAlgorithm;
import com.google.api.gax.retrying.RetrySettings;
import com.google.api.gax.rpc.TransportChannelProvider;
import com.google.cloud.NoCredentials;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.MockDatabaseAdminServiceImpl;
import com.google.cloud.spanner.MockOperationsServiceImpl;
import com.google.cloud.spanner.MockSpannerServiceImpl;
import com.google.cloud.spanner.MockSpannerServiceImpl.SimulatedExecutionTime;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerFactory;
import com.google.cloud.spanner.SpannerOptions;
import com.google.cloud.spanner.admin.database.v1.DatabaseAdminClient;
import com.google.cloud.spanner.admin.database.v1.DatabaseAdminSettings;
import com.google.cloud.spanner.v1.SpannerSettings;
import com.google.gson.Gson;
import com.google.spanner.admin.database.v1.CreateDatabaseMetadata;
import com.google.spanner.admin.database.v1.CreateDatabaseRequest;
import com.google.spanner.admin.database.v1.Database;
import io.grpc.Status;
import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.gcp.spanner.cdc.model.Mod;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.threeten.bp.Duration;

@RunWith(JUnit4.class)
public class SpannerChangeStreamErrorsTest {

  public static final String SPANNER_HOST = "https://";
  private static final String TEST_PROJECT = "my-project";
  private static final String TEST_INSTANCE = "my-instance";
  private static final String TEST_DATABASE = "my-database";
  private static final String TEST_TABLE = "my-table";
  private static final String TEST_CHANGE_STREAM = "my-change-stream";

  @Rule public final transient TestPipeline pipeline = TestPipeline.create();
  private DatabaseAdminClient databaseAdminClient;

  private static class FakeSpannerFactory implements SpannerFactory {
    private final Spanner spanner;

    public FakeSpannerFactory(SpannerOptions spannerOptions) {
      spanner = spannerOptions.getService();
    }

    @Override
    public Spanner create(SpannerOptions spannerOptions) {
      return spanner;
    }
  }

  private Database createTestDb() throws InterruptedException, ExecutionException {
    CreateDatabaseRequest request =
        CreateDatabaseRequest.newBuilder()
            .setCreateStatement("CREATE DATABASE " + TEST_DATABASE)
            .setParent(String.format("projects/%s/instances/%s", TEST_PROJECT, TEST_INSTANCE))
            .build();
    OperationFuture<Database, CreateDatabaseMetadata> op =
        databaseAdminClient.createDatabaseOperationCallable().futureCall(request);
    return op.get();
  }

  @Test
  public void testReadSpannerChangeStream()
      throws InterruptedException, IOException, ExecutionException {
    MockSpannerServiceImpl mockSpanner = new MockSpannerServiceImpl();
    MockOperationsServiceImpl mockOperationsService = new MockOperationsServiceImpl();
    MockDatabaseAdminServiceImpl mockDatabaseAdminService =
        new MockDatabaseAdminServiceImpl(mockOperationsService);

    MockServiceHelper serviceHelper =
        new MockServiceHelper(
            SPANNER_HOST,
            Arrays.asList(mockSpanner, mockOperationsService, mockDatabaseAdminService));
    serviceHelper.start();
    serviceHelper.reset();

    mockSpanner.setExecuteSqlExecutionTime(
        SimulatedExecutionTime.ofException(Status.RESOURCE_EXHAUSTED.asRuntimeException()));
    mockSpanner.setCommitExecutionTime(
        SimulatedExecutionTime.ofException(Status.RESOURCE_EXHAUSTED.asRuntimeException()));
    mockSpanner.setBatchCreateSessionsExecutionTime(
        SimulatedExecutionTime.ofException(Status.RESOURCE_EXHAUSTED.asRuntimeException()));
    mockSpanner.setBeginTransactionExecutionTime(
        SimulatedExecutionTime.ofException(Status.RESOURCE_EXHAUSTED.asRuntimeException()));
    mockSpanner.setCreateSessionExecutionTime(
        SimulatedExecutionTime.ofException(Status.RESOURCE_EXHAUSTED.asRuntimeException()));
    mockSpanner.setExecuteBatchDmlExecutionTime(
        SimulatedExecutionTime.ofException(Status.RESOURCE_EXHAUSTED.asRuntimeException()));

    TransportChannelProvider channelProvider = serviceHelper.createChannelProvider();
    DatabaseAdminSettings.Builder settingsBuilder =
        DatabaseAdminSettings.newBuilder()
            .setTransportChannelProvider(channelProvider)
            .setCredentialsProvider(NoCredentialsProvider.create());
    settingsBuilder
        .createBackupOperationSettings()
        .setPollingAlgorithm(
            OperationTimedPollAlgorithm.create(
                RetrySettings.newBuilder()
                    .setInitialRpcTimeout(Duration.ofMillis(20L))
                    .setInitialRetryDelay(Duration.ofMillis(10L))
                    .setMaxRetryDelay(Duration.ofMillis(150L))
                    .setMaxRpcTimeout(Duration.ofMillis(150L))
                    .setMaxAttempts(10)
                    .setTotalTimeout(Duration.ofMillis(5000L))
                    .setRetryDelayMultiplier(1.3)
                    .setRpcTimeoutMultiplier(1.3)
                    .build()));
    settingsBuilder
        .createDatabaseOperationSettings()
        .setPollingAlgorithm(
            OperationTimedPollAlgorithm.create(
                RetrySettings.newBuilder()
                    .setInitialRpcTimeout(Duration.ofMillis(20L))
                    .setInitialRetryDelay(Duration.ofMillis(10L))
                    .setMaxRetryDelay(Duration.ofMillis(150L))
                    .setMaxRpcTimeout(Duration.ofMillis(150L))
                    .setMaxAttempts(10)
                    .setTotalTimeout(Duration.ofMillis(5000L))
                    .setRetryDelayMultiplier(1.3)
                    .setRpcTimeoutMultiplier(1.3)
                    .build()));
    settingsBuilder
        .restoreDatabaseOperationSettings()
        .setPollingAlgorithm(
            OperationTimedPollAlgorithm.create(
                RetrySettings.newBuilder()
                    .setInitialRpcTimeout(Duration.ofMillis(20L))
                    .setInitialRetryDelay(Duration.ofMillis(10L))
                    .setMaxRetryDelay(Duration.ofMillis(150L))
                    .setMaxRpcTimeout(Duration.ofMillis(150L))
                    .setMaxAttempts(10)
                    .setTotalTimeout(Duration.ofMillis(5000L))
                    .setRetryDelayMultiplier(1.3)
                    .setRpcTimeoutMultiplier(1.3)
                    .build()));
    databaseAdminClient = DatabaseAdminClient.create(settingsBuilder.build());

    SpannerSettings spannerSettings =
        SpannerSettings.newBuilder()
            .setTransportChannelProvider(channelProvider)
            .setCredentialsProvider(NoCredentialsProvider.create())
            .build();

    SpannerOptions spannerOptions =
        SpannerOptions.newBuilder()
            .setChannelProvider(channelProvider)
            .setCredentials(NoCredentials.getInstance())
            .build();
    FakeSpannerFactory fakeSpannerFactory = new FakeSpannerFactory(spannerOptions);
    final SpannerConfig spannerConfig =
        SpannerConfig.create()
            .withServiceFactory(fakeSpannerFactory)
            .withHost(StaticValueProvider.of(SPANNER_HOST))
            .withEmulatorHost(StaticValueProvider.of(SPANNER_HOST))
            .withProjectId(TEST_PROJECT)
            .withInstanceId(TEST_INSTANCE)
            .withDatabaseId(TEST_DATABASE);

    createTestDb();

    final Timestamp now = Timestamp.now();
    final Timestamp after3Seconds =
        Timestamp.ofTimeSecondsAndNanos(now.getSeconds() + 3, now.getNanos());
    final PCollection<String> tokens =
        pipeline
            .apply(
                SpannerIO.readChangeStream()
                    .withSpannerConfig(spannerConfig)
                    .withChangeStreamName(TEST_CHANGE_STREAM)
                    .withMetadataDatabase(TEST_DATABASE)
                    .withInclusiveStartAt(now)
                    .withInclusiveEndAt(after3Seconds))
            .apply(
                MapElements.into(TypeDescriptors.strings())
                    .via(
                        record -> {
                          final Gson gson = new Gson();
                          final Mod mod = record.getMods().get(0);
                          final Map<String, String> keys =
                              gson.fromJson(mod.getKeysJson(), Map.class);
                          final Map<String, String> newValues =
                              gson.fromJson(mod.getNewValuesJson(), Map.class);
                          return String.join(
                              ",",
                              keys.get("SingerId"),
                              newValues.get("FirstName"),
                              newValues.get("LastName"));
                        }));

    // TODO change below to expect exceptions
    PAssert.that(tokens).containsInAnyOrder("1,First Name 1,Last Name 1");

    final PipelineResult pipelineResult = pipeline.run();
    Thread.sleep(5_000);
    pipelineResult.waitUntilFinish();
  }
}
