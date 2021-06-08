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

package org.apache.beam.sdk.io.gcp.spanner.cdc;

import com.google.cloud.Timestamp;
import com.google.cloud.spanner.CommitResponse;
import com.google.cloud.spanner.TransactionContext;
import com.google.cloud.spanner.TransactionRunner;

public class TransactionRunnerStub implements TransactionRunner {

  private final TransactionContext transactionContext;

  public TransactionRunnerStub(TransactionContext transactionContext) {
    this.transactionContext = transactionContext;
  }

  @Override
  public <T> T run(TransactionCallable<T> callable) {
    try {
      return callable.run(transactionContext);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Timestamp getCommitTimestamp() {
    return null;
  }

  @Override
  public CommitResponse getCommitResponse() {
    return null;
  }

  @Override
  public TransactionRunner allowNestedTransaction() {
    return null;
  }
}
