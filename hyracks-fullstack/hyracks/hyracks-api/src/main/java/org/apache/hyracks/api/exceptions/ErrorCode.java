/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hyracks.api.exceptions;

import java.io.File;
import java.io.InputStream;
import java.util.Map;

import org.apache.hyracks.api.util.ErrorMessageUtil;

/**
 * A registry of runtime/compile error codes
 * Error code:
 * 0 --- 9999: runtime errors
 * 10000 ---- 19999: compilation errors
 */
public class ErrorCode {
    private static final String RESOURCE_PATH = "errormsg/en.properties";
    public static final String HYRACKS = "HYR";

    // Runtime error codes.
    public static final int INVALID_OPERATOR_OPERATION = 1;
    public static final int ERROR_PROCESSING_TUPLE = 2;
    public static final int FAILURE_ON_NODE = 3;
    public static final int FILE_WITH_ABSOULTE_PATH_NOT_WITHIN_ANY_IO_DEVICE = 4;
    public static final int FULLTEXT_PHRASE_FOUND = 5;
    public static final int JOB_QUEUE_FULL = 6;
    public static final int INVALID_NETWORK_ADDRESS = 7;
    public static final int INVALID_INPUT_PARAMETER = 8;
    public static final int JOB_REQUIREMENTS_EXCEED_CAPACITY = 9;
    public static final int NO_SUCH_NODE = 10;
    public static final int CLASS_LOADING_ISSUE = 11;
    public static final int ILLEGAL_WRITE_AFTER_FLUSH_ATTEMPT = 12;
    public static final int DUPLICATE_IODEVICE = 13;
    public static final int NESTED_IODEVICES = 14;
    public static final int MORE_THAN_ONE_RESULT = 15;
    public static final int RESULT_FAILURE_EXCEPTION = 16;
    public static final int RESULT_FAILURE_NO_EXCEPTION = 17;
    public static final int INCONSISTENT_RESULT_METADATA = 18;
    public static final int CANNOT_DELETE_FILE = 19;
    public static final int NOT_A_JOBID = 20;
    public static final int ERROR_FINDING_DISTRIBUTED_JOB = 21;
    public static final int DUPLICATE_DISTRIBUTED_JOB = 22;
    public static final int DISTRIBUTED_JOB_FAILURE = 23;
    public static final int NO_RESULTSET = 24;

    // Compilation error codes.
    public static final int RULECOLLECTION_NOT_INSTANCE_OF_LIST = 10001;

    // Loads the map that maps error codes to error message templates.
    private static Map<Integer, String> errorMessageMap = null;

    private ErrorCode() {
    }

    public static String getErrorMessage(int errorCode) {
        if (errorMessageMap == null) {
            try (InputStream resourceStream = ErrorCode.class.getClassLoader().getResourceAsStream(RESOURCE_PATH)) {
                errorMessageMap = ErrorMessageUtil.loadErrorMap(resourceStream);
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }
        }
        String msg = errorMessageMap.get(errorCode);
        if (msg == null) {
            throw new IllegalStateException("Undefined error code: " + errorCode);
        }
        return msg;
    }
}
