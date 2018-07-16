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

package org.apache.hyracks.dataflow.std.sort;

import java.util.LinkedList;
import java.util.List;

import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.io.GeneratedRunFileReader;
import org.apache.hyracks.dataflow.common.io.RunFileWriter;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public abstract class AbstractSortRunGenerator implements IRunGenerator {
    protected final List<GeneratedRunFileReader> generatedRunFileReaders;

    // Temp :
    static final Logger LOGGER = LogManager.getLogger();
    protected final boolean limitMemory;
    //

    public AbstractSortRunGenerator() {
        generatedRunFileReaders = new LinkedList<>();
        this.limitMemory = true;
    }

    public AbstractSortRunGenerator(boolean limitMemory) {
        generatedRunFileReaders = new LinkedList<>();
        this.limitMemory = limitMemory;
    }

    abstract public ISorter getSorter() throws HyracksDataException;

    @Override
    public void open() throws HyracksDataException {
        // Temp :
        LOGGER.log(Level.INFO, this.hashCode() + "\t" + "open");
        //
        generatedRunFileReaders.clear();
    }

    @Override
    public void close() throws HyracksDataException {
        // Temp :
        LOGGER.log(Level.INFO, this.hashCode() + "\t" + "close");
        //
        if (getSorter().hasRemaining()) {
            if (generatedRunFileReaders.size() <= 0) {
                getSorter().sort();
            } else {
                flushFramesToRun();
            }
        }
    }

    abstract protected RunFileWriter getRunFileWriter() throws HyracksDataException;

    abstract protected IFrameWriter getFlushableFrameWriter(RunFileWriter writer) throws HyracksDataException;

    protected void flushFramesToRun() throws HyracksDataException {
        getSorter().sort();
        RunFileWriter runWriter = getRunFileWriter();
        IFrameWriter flushWriter = getFlushableFrameWriter(runWriter);
        flushWriter.open();
        try {
            getSorter().flush(flushWriter);
        } catch (Exception e) {
            flushWriter.fail();
            throw e;
        } finally {
            flushWriter.close();
        }
        // Temp :
        LOGGER.log(Level.INFO,
                this.hashCode() + "\t" + "flushFramesToRun" + "\twriter file:\t"
                        + runWriter.getFileReference().getFile().getName() + "\tbyte size:\t" + runWriter.getFileSize()
                        + "\tsize (MB):\t" + ((double) runWriter.getFileSize() / 1048576));
        //

        generatedRunFileReaders.add(runWriter.createDeleteOnCloseReader());
        getSorter().reset();
    }

    @Override
    public void fail() throws HyracksDataException {
    }

    @Override
    public List<GeneratedRunFileReader> getRuns() {
        return generatedRunFileReaders;
    }
}
