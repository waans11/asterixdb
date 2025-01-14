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
package org.apache.asterix.external.input.record.reader.twitter;

import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.external.api.IExternalDataSourceFactory;
import org.apache.asterix.external.api.IRecordReader;
import org.apache.asterix.external.api.IRecordReaderFactory;
import org.apache.asterix.external.util.ExternalDataConstants;
import org.apache.asterix.external.util.TwitterUtil;
import org.apache.asterix.external.util.TwitterUtil.AuthenticationConstants;
import org.apache.asterix.external.util.TwitterUtil.SearchAPIConstants;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksAbsolutePartitionConstraint;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;

import twitter4j.FilterQuery;

public class TwitterRecordReaderFactory implements IRecordReaderFactory<String> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOGGER = Logger.getLogger(TwitterRecordReaderFactory.class.getName());

    private static final String DEFAULT_INTERVAL = "10"; // 10 seconds
    private static final int INTAKE_CARDINALITY = 1; // degree of parallelism at intake stage

    private Map<String, String> configuration;
    private transient AlgebricksAbsolutePartitionConstraint clusterLocations;

    public static boolean isTwitterPull(Map<String, String> configuration) {
        String reader = configuration.get(ExternalDataConstants.KEY_READER);
        if (reader.equals(ExternalDataConstants.READER_TWITTER_PULL)
                || reader.equals(ExternalDataConstants.READER_PULL_TWITTER)) {
            return true;
        }
        return false;
    }

    @Override
    public DataSourceType getDataSourceType() {
        return DataSourceType.RECORDS;
    }

    @Override
    public AlgebricksAbsolutePartitionConstraint getPartitionConstraint() throws AlgebricksException {
        clusterLocations = IExternalDataSourceFactory.getPartitionConstraints(clusterLocations, INTAKE_CARDINALITY);
        return clusterLocations;
    }

    @Override
    public void configure(Map<String, String> configuration) throws AsterixException {
        this.configuration = configuration;
        TwitterUtil.initializeConfigurationWithAuthInfo(configuration);
        if (!validateConfiguration(configuration)) {
            StringBuilder builder = new StringBuilder();
            builder.append("One or more parameters are missing from adapter configuration\n");
            builder.append(AuthenticationConstants.OAUTH_CONSUMER_KEY + "\n");
            builder.append(AuthenticationConstants.OAUTH_CONSUMER_SECRET + "\n");
            builder.append(AuthenticationConstants.OAUTH_ACCESS_TOKEN + "\n");
            builder.append(AuthenticationConstants.OAUTH_ACCESS_TOKEN_SECRET);
            throw new AsterixException(builder.toString());
        }

        if (configuration.get(ExternalDataConstants.KEY_READER).equals(ExternalDataConstants.READER_PULL_TWITTER)) {
            if (configuration.get(SearchAPIConstants.QUERY) == null) {
                throw new AsterixException(
                        "parameter " + SearchAPIConstants.QUERY + " not specified as part of adaptor configuration");
            }
            String interval = configuration.get(SearchAPIConstants.INTERVAL);
            if (interval != null) {
                try {
                    Integer.parseInt(interval);
                } catch (NumberFormatException nfe) {
                    throw new IllegalArgumentException(
                            "parameter " + SearchAPIConstants.INTERVAL + " is defined incorrectly, expecting a number");
                }
            } else {
                configuration.put(SearchAPIConstants.INTERVAL, DEFAULT_INTERVAL);
                if (LOGGER.isLoggable(Level.WARNING)) {
                    LOGGER.warning(" Parameter " + SearchAPIConstants.INTERVAL + " not defined, using default ("
                            + DEFAULT_INTERVAL + ")");
                }
            }
        }
    }

    @Override
    public boolean isIndexible() {
        return false;
    }

    @Override
    public IRecordReader<? extends String> createRecordReader(IHyracksTaskContext ctx, int partition)
            throws HyracksDataException {
        IRecordReader<? extends String> recordReader;
        switch (configuration.get(ExternalDataConstants.KEY_READER)) {
            case ExternalDataConstants.READER_PULL_TWITTER:
                recordReader = new TwitterPullRecordReader(TwitterUtil.getTwitterService(configuration),
                        configuration.get(SearchAPIConstants.QUERY),
                        Integer.parseInt(configuration.get(SearchAPIConstants.INTERVAL)));
                break;
            case ExternalDataConstants.READER_PUSH_TWITTER:
                FilterQuery query;
                try {
                    query = TwitterUtil.getFilterQuery(configuration);
                    recordReader = (query == null)
                            ? new TwitterPushRecordReader(TwitterUtil.getTwitterStream(configuration),
                                    TwitterUtil.getTweetListener())
                            : new TwitterPushRecordReader(TwitterUtil.getTwitterStream(configuration),
                                    TwitterUtil.getTweetListener(), query);
                } catch (AsterixException e) {
                    throw new HyracksDataException(e);
                }
                break;
            case ExternalDataConstants.READER_USER_STREAM_TWITTER:
                recordReader = new TwitterPushRecordReader(TwitterUtil.getTwitterStream(configuration),
                        TwitterUtil.getUserTweetsListener());
                break;
            default:
                throw new HyracksDataException("No Record reader found!");
        }
        return recordReader;
    }

    @Override
    public Class<? extends String> getRecordClass() {
        return String.class;
    }

    private boolean validateConfiguration(Map<String, String> configuration) {
        String consumerKey = configuration.get(AuthenticationConstants.OAUTH_CONSUMER_KEY);
        String consumerSecret = configuration.get(AuthenticationConstants.OAUTH_CONSUMER_SECRET);
        String accessToken = configuration.get(AuthenticationConstants.OAUTH_ACCESS_TOKEN);
        String tokenSecret = configuration.get(AuthenticationConstants.OAUTH_ACCESS_TOKEN_SECRET);
        if ((consumerKey == null) || (consumerSecret == null) || (accessToken == null) || (tokenSecret == null)) {
            return false;
        }
        return true;
    }

}
