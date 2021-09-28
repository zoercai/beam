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

import static org.hamcrest.MatcherAssert.assertThat;

import com.google.api.gax.grpc.testing.MockServiceHelper;
import com.google.api.gax.retrying.RetrySettings;
import com.google.api.gax.rpc.StatusCode.Code;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.MockDatabaseAdminServiceImpl;
import com.google.cloud.spanner.MockOperationsServiceImpl;
import com.google.cloud.spanner.MockSpannerServiceImpl;
import com.google.cloud.spanner.MockSpannerServiceImpl.SimulatedExecutionTime;
import com.google.cloud.spanner.SpannerException;
import com.google.cloud.spanner.v1.stub.SpannerStubSettings;
import com.google.spanner.v1.ExecuteSqlRequest;
import io.grpc.Status;
import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableSet;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class SpannerChangeStreamErrorsTest {

  public static final String SPANNER_HOST = "my-host";
  private static final String TEST_PROJECT = "my-project";
  private static final String TEST_INSTANCE = "my-instance";
  private static final String TEST_DATABASE = "my-database";
  private static final String TEST_CHANGE_STREAM = "my-change-stream";

  @Rule public final transient TestPipeline pipeline = TestPipeline.create();
  @Rule public transient ExpectedException thrown = ExpectedException.none();

  private MockSpannerServiceImpl mockSpannerService;
  private MockOperationsServiceImpl mockOperationsService;
  private MockDatabaseAdminServiceImpl mockDatabaseAdminService;
  private MockServiceHelper serviceHelper;
  private SpannerConfig spannerConfig;

  // ---------------- Client Library Errors ----------------

  @Test
  public void testResourceExhausted() {
    mockSpannerService.setExecuteStreamingSqlExecutionTime(
        SimulatedExecutionTime.ofStickyException(Status.RESOURCE_EXHAUSTED.asRuntimeException()));

    final Timestamp now = Timestamp.now();
    final Timestamp after3Seconds =
        Timestamp.ofTimeSecondsAndNanos(now.getSeconds() + 3, now.getNanos());
    try {
      pipeline.apply(
          SpannerIO.readChangeStream()
              .withSpannerConfig(spannerConfig)
              .withChangeStreamName(TEST_CHANGE_STREAM)
              .withMetadataDatabase(TEST_DATABASE)
              .withInclusiveStartAt(now)
              .withInclusiveEndAt(after3Seconds));
      pipeline.run().waitUntilFinish();
    } finally {
      thrown.expect(SpannerException.class);
      thrown.expectMessage("RESOURCE_EXHAUSTED");
      thrown.expectMessage("See https://cloud.google.com/spanner/quotas for more information.");
    }
  }

  @Test
  public void testDeadlineExceededRetries() {
    mockSpannerService.setExecuteStreamingSqlExecutionTime(
        SimulatedExecutionTime.ofStickyException(Status.DEADLINE_EXCEEDED.asRuntimeException()));

    final Timestamp now = Timestamp.now();
    final Timestamp after3Seconds =
        Timestamp.ofTimeSecondsAndNanos(now.getSeconds() + 3, now.getNanos());
    try {
      pipeline.apply(
          SpannerIO.readChangeStream()
              .withSpannerConfig(spannerConfig)
              .withChangeStreamName(TEST_CHANGE_STREAM)
              .withMetadataDatabase(TEST_DATABASE)
              .withInclusiveStartAt(now)
              .withInclusiveEndAt(after3Seconds));
      pipeline.run().waitUntilFinish();
    } finally {
      assertThat(
          mockSpannerService.countRequestsOfType(ExecuteSqlRequest.class), Matchers.greaterThan(1));
      thrown.expect(SpannerException.class);
      thrown.expectMessage("DEADLINE_EXCEEDED");
    }
  }

  @Test
  public void testInternalExceptionRetries() {
    mockSpannerService.setExecuteStreamingSqlExecutionTime(
        SimulatedExecutionTime.ofStickyException(Status.INTERNAL.asRuntimeException()));

    final Timestamp now = Timestamp.now();
    final Timestamp after3Seconds =
        Timestamp.ofTimeSecondsAndNanos(now.getSeconds() + 3, now.getNanos());
    try {
      pipeline.apply(
          SpannerIO.readChangeStream()
              .withSpannerConfig(spannerConfig)
              .withChangeStreamName(TEST_CHANGE_STREAM)
              .withMetadataDatabase(TEST_DATABASE)
              .withInclusiveStartAt(now)
              .withInclusiveEndAt(after3Seconds));
      pipeline.run().waitUntilFinish();
    } finally {
      assertThat(
          mockSpannerService.countRequestsOfType(ExecuteSqlRequest.class), Matchers.greaterThan(1));
      thrown.expect(SpannerException.class);
      thrown.expectMessage("INTERNAL");
    }
  }

  @Test
  public void testUnavailableExceptionRetries() {
    mockSpannerService.setExecuteStreamingSqlExecutionTime(
        SimulatedExecutionTime.ofExceptions(
            ImmutableSet.of(
                Status.UNAVAILABLE.asRuntimeException(),
                Status.RESOURCE_EXHAUSTED.asRuntimeException())));

    final Timestamp now = Timestamp.now();
    final Timestamp after3Seconds =
        Timestamp.ofTimeSecondsAndNanos(now.getSeconds() + 3, now.getNanos());
    try {
      pipeline.apply(
          SpannerIO.readChangeStream()
              .withSpannerConfig(spannerConfig)
              .withChangeStreamName(TEST_CHANGE_STREAM)
              .withMetadataDatabase(TEST_DATABASE)
              .withInclusiveStartAt(now)
              .withInclusiveEndAt(after3Seconds));
      pipeline.run().waitUntilFinish();
    } finally {
      assertThat(
          mockSpannerService.countRequestsOfType(ExecuteSqlRequest.class), Matchers.greaterThan(1));
      thrown.expect(SpannerException.class);
      thrown.expectMessage("RESOURCE_EXHAUSTED");
    }
  }

  @Test
  public void testAbortedExceptionRetries() {
    mockSpannerService.setExecuteStreamingSqlExecutionTime(
        SimulatedExecutionTime.ofStickyException(Status.ABORTED.asRuntimeException()));

    final Timestamp now = Timestamp.now();
    final Timestamp after3Seconds =
        Timestamp.ofTimeSecondsAndNanos(now.getSeconds() + 3, now.getNanos());
    try {
      pipeline.apply(
          SpannerIO.readChangeStream()
              .withSpannerConfig(spannerConfig)
              .withChangeStreamName(TEST_CHANGE_STREAM)
              .withMetadataDatabase(TEST_DATABASE)
              .withInclusiveStartAt(now)
              .withInclusiveEndAt(after3Seconds));
      pipeline.run().waitUntilFinish();
    } finally {
      assertThat(
          mockSpannerService.countRequestsOfType(ExecuteSqlRequest.class), Matchers.greaterThan(1));
      thrown.expect(SpannerException.class);
      thrown.expectMessage("ABORTED");
    }
  }

  @Test
  public void testUnknownExceptionRetries() {
    mockSpannerService.setExecuteStreamingSqlExecutionTime(
        SimulatedExecutionTime.ofStickyException(Status.UNKNOWN.asRuntimeException()));

    final Timestamp now = Timestamp.now();
    final Timestamp after3Seconds =
        Timestamp.ofTimeSecondsAndNanos(now.getSeconds() + 3, now.getNanos());
    try {
      pipeline.apply(
          SpannerIO.readChangeStream()
              .withSpannerConfig(spannerConfig)
              .withChangeStreamName(TEST_CHANGE_STREAM)
              .withMetadataDatabase(TEST_DATABASE)
              .withInclusiveStartAt(now)
              .withInclusiveEndAt(after3Seconds));
      pipeline.run().waitUntilFinish();
    } finally {
      assertThat(
          mockSpannerService.countRequestsOfType(ExecuteSqlRequest.class), Matchers.greaterThan(1));
      thrown.expect(SpannerException.class);
      thrown.expectMessage("UNKNOWN");
    }
  }

  @Before
  public void setup()
      throws InterruptedException, ExecutionException, TimeoutException, IOException {
    mockSpannerService = new MockSpannerServiceImpl();
    mockOperationsService = new MockOperationsServiceImpl();
    mockDatabaseAdminService = new MockDatabaseAdminServiceImpl(mockOperationsService);

    serviceHelper =
        new MockServiceHelper(
            SPANNER_HOST,
            Arrays.asList(mockSpannerService, mockOperationsService, mockDatabaseAdminService));
    serviceHelper.start();
    serviceHelper.reset();

    ImmutableSet<Code> defaultRetryableCodes =
        ImmutableSet.of(
            Code.DEADLINE_EXCEEDED, Code.INTERNAL, Code.UNAVAILABLE, Code.ABORTED, Code.UNKNOWN);
    SpannerStubSettings.Builder defaultSpannerStubSettingsBuilder =
        SpannerStubSettings.newBuilder();
    defaultSpannerStubSettingsBuilder
        .commitSettings()
        .setRetryableCodes(defaultRetryableCodes)
        .setRetrySettings(
            RetrySettings.newBuilder()
                .setInitialRetryDelay(org.threeten.bp.Duration.ofMillis(250))
                .setMaxRetryDelay(org.threeten.bp.Duration.ofSeconds(1))
                .setRetryDelayMultiplier(1.3)
                .setTotalTimeout(org.threeten.bp.Duration.ofSeconds(1))
                .build())
        .build();
    defaultSpannerStubSettingsBuilder
        .executeStreamingSqlSettings()
        .setRetryableCodes(defaultRetryableCodes)
        .setRetrySettings(
            RetrySettings.newBuilder()
                .setInitialRetryDelay(org.threeten.bp.Duration.ofMillis(250))
                .setMaxRetryDelay(org.threeten.bp.Duration.ofSeconds(1))
                .setRetryDelayMultiplier(1.3)
                .setTotalTimeout(org.threeten.bp.Duration.ofSeconds(1))
                .build())
        .build();
    spannerConfig =
        SpannerConfig.create()
            .withEmulatorHost(StaticValueProvider.of(SPANNER_HOST))
            .withIsLocalChannelProvider(StaticValueProvider.of(true))
            .withSpannerStubSettings(defaultSpannerStubSettingsBuilder.build())
            .withProjectId(TEST_PROJECT)
            .withInstanceId(TEST_INSTANCE)
            .withDatabaseId(TEST_DATABASE);
  }

  @After
  public void tearDown() {
    serviceHelper.stop();
  }
}
