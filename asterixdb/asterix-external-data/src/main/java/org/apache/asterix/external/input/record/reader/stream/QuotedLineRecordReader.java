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
package org.apache.asterix.external.input.record.reader.stream;

import java.io.IOException;

import org.apache.asterix.common.exceptions.ExceptionUtils;
import org.apache.asterix.external.api.AsterixInputStream;
import org.apache.asterix.external.util.ExternalDataConstants;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public class QuotedLineRecordReader extends LineRecordReader {

    private final char quote;
    private boolean prevCharEscape;
    private boolean inQuote;

    public QuotedLineRecordReader(final boolean hasHeader, final AsterixInputStream stream, final String quoteString)
            throws HyracksDataException {
        super(hasHeader, stream);
        if ((quoteString == null) || (quoteString.length() != 1)) {
            throw new HyracksDataException(ExceptionUtils.incorrectParameterMessage(
                    ExternalDataConstants.KEY_QUOTE, ExternalDataConstants.PARAMETER_OF_SIZE_ONE, quoteString));
        }
        this.quote = quoteString.charAt(0);
    }

    @Override
    public boolean hasNext() throws IOException {
        while (true) {
            if (done) {
                return false;
            }
            newlineLength = 0;
            prevCharCR = false;
            prevCharEscape = false;
            record.reset();
            int readLength = 0;
            inQuote = false;
            do {
                int startPosn = bufferPosn;
                if (bufferPosn >= bufferLength) {
                    startPosn = bufferPosn = 0;
                    bufferLength = reader.read(inputBuffer);
                    if (bufferLength <= 0) {
                        {
                            if (readLength > 0) {
                                if (inQuote) {
                                    throw new IOException("malformed input record ended inside quote");
                                }
                                record.endRecord();
                                recordNumber++;
                                return true;
                            }
                            close();
                            return false;
                        }
                    }
                }
                for (; bufferPosn < bufferLength; ++bufferPosn) {
                    if (!inQuote) {
                        if (inputBuffer[bufferPosn] == ExternalDataConstants.LF) {
                            newlineLength = (prevCharCR) ? 2 : 1;
                            ++bufferPosn;
                            break;
                        }
                        if (prevCharCR) {
                            newlineLength = 1;
                            break;
                        }
                        prevCharCR = (inputBuffer[bufferPosn] == ExternalDataConstants.CR);
                        if (inputBuffer[bufferPosn] == quote) {
                            if (!prevCharEscape) {
                                inQuote = true;
                            }
                        }
                        if (prevCharEscape) {
                            prevCharEscape = false;
                        } else {
                            prevCharEscape = inputBuffer[bufferPosn] == ExternalDataConstants.ESCAPE;
                        }
                    } else {
                        // only look for next quote
                        if (inputBuffer[bufferPosn] == quote) {
                            if (!prevCharEscape) {
                                inQuote = false;
                            }
                        }
                        prevCharEscape = inputBuffer[bufferPosn] == ExternalDataConstants.ESCAPE;
                    }
                }
                readLength = bufferPosn - startPosn;
                if (prevCharCR && newlineLength == 0) {
                    --readLength;
                }
                if (readLength > 0) {
                    record.append(inputBuffer, startPosn, readLength);
                }
            } while (newlineLength == 0);
            if (nextIsHeader) {
                nextIsHeader = false;
                continue;
            }
            recordNumber++;
            return true;
        }
    }
}
