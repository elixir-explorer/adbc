/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.arrow.adbc.core;

/**
 * A status code indicating the general category of error that occurred.
 *
 * <p>Also see the ADBC C API definition, which has similar status codes.
 */
public enum AdbcStatusCode {
  /**
   * An unknown error occurred.
   *
   * <p>May indicate client-side or database-side error.
   */
  UNKNOWN,
  /**
   * The operation is not supported.
   *
   * <p>May indicate client-side or database-side error.
   */
  NOT_IMPLEMENTED,
  /** */
  NOT_FOUND,
  ALREADY_EXISTS,
  INVALID_ARGUMENT,
  INVALID_STATE,
  INVALID_DATA,
  INTEGRITY,
  INTERNAL,
  IO,
  CANCELLED,
  TIMEOUT,
  UNAUTHENTICATED,
  UNAUTHORIZED,
  ;
}
