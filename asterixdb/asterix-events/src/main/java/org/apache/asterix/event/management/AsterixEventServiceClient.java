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
package org.apache.asterix.event.management;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;

import org.apache.asterix.event.driver.EventDriver;
import org.apache.asterix.event.schema.cluster.Cluster;
import org.apache.asterix.event.schema.cluster.Node;
import org.apache.asterix.event.schema.event.Events;
import org.apache.asterix.event.schema.pattern.Event;
import org.apache.asterix.event.schema.pattern.Nodeid;
import org.apache.asterix.event.schema.pattern.Pattern;
import org.apache.asterix.event.schema.pattern.Patterns;
import org.apache.asterix.event.schema.pattern.Value;
import org.apache.asterix.event.service.AsterixEventServiceUtil;
import org.apache.asterix.installer.schema.conf.Configuration;

public class AsterixEventServiceClient {

    private static final Logger LOGGER = Logger.getLogger(AsterixEventServiceClient.class.getName());

    private EventTask[] tasks;
    private boolean dryRun = false;
    private LinkedBlockingQueue<EventTaskReport> msgInbox = new LinkedBlockingQueue<EventTaskReport>();
    private AtomicInteger pendingTasks = new AtomicInteger(0);
    private final Cluster cluster;
    private IPatternListener listener;
    private IOutputHandler outputHandler;
    private Events events;
    private String eventsHomeDir;
    private Configuration configuration;

    public AsterixEventServiceClient(Configuration configuration, String eventsHomeDir, Cluster cluster,
            boolean transferArtifacts, boolean dryRun, IOutputHandler outputHandler) throws Exception {
        this.eventsHomeDir = eventsHomeDir;
        this.events = initializeEvents();
        this.cluster = cluster;
        this.dryRun = dryRun;
        this.configuration = configuration;
        this.outputHandler = outputHandler;
        if (!dryRun && transferArtifacts) {
            initializeCluster(getEventsDir());
        }
    }

    public void submit(Patterns patterns) throws Exception {
        if (patterns.getPattern().isEmpty()) {
            return;
        }
        initTasks(patterns);
        try {
            waitForCompletion();
        } catch (InterruptedException ie) {
            LOGGER.info("Interrupted exception :" + ie);
        } catch (Exception e) {
            throw e;
        }

    }

    public void submit(Patterns patterns, IPatternListener listener) throws Exception {
        this.listener = listener;
        initTasks(patterns);
    }

    private void initTasks(Patterns patterns) {
        tasks = new EventTask[patterns.getPattern().size()];
        pendingTasks.set(tasks.length);
        int index = 0;
        for (Pattern pattern : patterns.getPattern()) {
            tasks[index] = new EventTask(pattern, this);
            tasks[index].start();
            index++;
        }
    }

    public Cluster getCluster() {
        return cluster;
    }

    public boolean isDryRun() {
        return dryRun;
    }

    public Events getEvents() {
        return events;
    }

    public String getEventsDir() {
        return eventsHomeDir + File.separator + AsterixEventServiceUtil.EVENT_DIR;
    }

    public synchronized void notifyCompletion(EventTaskReport report) {

        if (report.isSuccess()) {
            if (listener != null) {
                pendingTasks.decrementAndGet();
                listener.eventCompleted(report);
                if (pendingTasks.get() == 0) {
                    listener.jobCompleted();
                }
            } else {
                try {
                    msgInbox.put(report);
                } catch (InterruptedException e) {
                }
            }
        } else {
            for (EventTask t : tasks) {
                if (t.getState() == EventTask.State.INITIALIZED || t.getState() == EventTask.State.IN_PROGRESS) {
                    t.cancel();
                }
            }
            if (listener != null) {
                listener.jobFailed(report);
            } else {
                try {
                    msgInbox.put(report);
                } catch (InterruptedException e) {
                }
            }
        }
    }

    private void waitForCompletion() throws Exception {
        while (true) {
            EventTaskReport report = msgInbox.take();
            if (report.isSuccess()) {
                if (pendingTasks.decrementAndGet() == 0) {
                    break;
                }
            } else {
                throw report.getException();
            }
        }
    }

    private void initializeCluster(String eventsDir) throws Exception {
        Patterns patterns = initPattern(eventsDir);
        submit(patterns);
    }

    private Patterns initPattern(String eventsDir) throws Exception {
        Nodeid nodeid = new Nodeid(new Value(null, EventDriver.CLIENT_NODE.getId()));
        List<Pattern> patternList = new ArrayList<Pattern>();
        String workingDir = cluster.getWorkingDir().getDir();
        String username = cluster.getUsername() == null ? System.getProperty("user.name") : cluster.getUsername();
        patternList.add(getDirectoryTransferPattern(username, eventsDir, nodeid,
                cluster.getMasterNode().getClusterIp(), workingDir));

        JAXBContext ctx = JAXBContext.newInstance(Configuration.class);
        Marshaller marshaller = ctx.createMarshaller();
        marshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, true);
        String outputPathDir = System.getProperty("java.io.tmpdir") + File.separator + "conf-"
                + System.getProperty("user.name");
        new File(outputPathDir).mkdirs();
        String outputPath = outputPathDir + File.separator + "configuration.xml";
        marshaller.marshal(configuration, new FileOutputStream(outputPath));

        patternList.add(getFileTransferPattern(username, outputPath, nodeid, cluster.getMasterNode().getClusterIp(),
                workingDir));

        if (!cluster.getWorkingDir().isNFS()) {
            for (Node node : cluster.getNode()) {
                patternList.add(getDirectoryTransferPattern(username, eventsDir, nodeid, node.getClusterIp(),
                        workingDir));

            }
        }
        return new Patterns(patternList);
    }

    private Pattern getDirectoryTransferPattern(String username, String src, Nodeid srcNode, String destNodeIp,
            String destDir) {
        String pargs = username + " " + src + " " + destNodeIp + " " + destDir;
        Event event = new Event("directory_transfer", srcNode, pargs);
        return new Pattern(null, 1, null, event);
    }

    private Pattern getFileTransferPattern(String username, String src, Nodeid srcNode, String destNodeIp,
            String destDir) {
        String pargs = username + " " + src + " " + destNodeIp + " " + destDir;
        Event event = new Event("file_transfer", srcNode, pargs);
        return new Pattern(null, 1, null, event);
    }

    public IOutputHandler getErrorHandler() {
        return outputHandler;
    }

    private Events initializeEvents() throws JAXBException, FileNotFoundException {
        File file = new File(getEventsDir() + File.separator + "events.xml");
        JAXBContext eventCtx = JAXBContext.newInstance(Events.class);
        Unmarshaller unmarshaller = eventCtx.createUnmarshaller();
        events = (Events) unmarshaller.unmarshal(file);
        return events;
    }

    public String getEventsHomeDir() {
        return eventsHomeDir;
    }

}
