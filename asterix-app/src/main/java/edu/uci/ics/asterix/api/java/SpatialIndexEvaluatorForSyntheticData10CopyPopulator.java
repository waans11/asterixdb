package edu.uci.ics.asterix.api.java;

import java.io.IOException;
import java.net.URISyntaxException;

import org.apache.http.HttpResponse;

public class SpatialIndexEvaluatorForSyntheticData10CopyPopulator {
    private static String ipAddress = "127.0.0.1";
    private static String portNum = "19002";
    private static String indexType;

    public static void main(String[] args) throws URISyntaxException, IOException {
        if (args.length < 3) {
            System.out
                    .println("Example Usage: java -jar SpatialIndexEvaluatorForSyntheticData10CopyPopulator.jar <index type> <cc ip address> <asterix api port num>");
            System.out.println("\targ0: index type - rtree, shbtree, dhbtree, or sif");
            System.out.println("\targ1: asterix cc ip address");
            System.out.println("\targ2: asterix api port number");
            System.exit(-1);
        }
        indexType = args[0];
        ipAddress = args[1];
        portNum = args[2];
        runDatasetIndexCreation();
    }

    private static void runDatasetIndexCreation() throws URISyntaxException, IOException {
        HttpResponse response;
        AsterixHttpClient ahc = new AsterixHttpClient(ipAddress, portNum);

        // ddl
        ahc.prepareDDL(getDDLAQL());
        response = ahc.execute();
        ahc.printResult(response, null);
    }

    private static String getDDLAQL() {
        StringBuilder sb = new StringBuilder();
        //create dataverse
        sb.append("drop dataverse STBench if exists; \n");
        sb.append("create dataverse STBench; \n");
        sb.append("use dataverse STBench; \n");

        //create datatype
        sb.append(" create type FsqCheckinTweetType as closed { id: int64, user_id: int64, user_followers_count: int64, ");
        sb.append(" text: string, datetime: datetime, coordinates: point, url: string? } \n");

        //create datasets
        sb.append(" create dataset FsqCheckinTweet0 (FsqCheckinTweetType) primary key id; \n");
        sb.append(" create dataset FsqCheckinTweet1 (FsqCheckinTweetType) primary key id; \n");
        sb.append(" create dataset FsqCheckinTweet2 (FsqCheckinTweetType) primary key id; \n");
        sb.append(" create dataset FsqCheckinTweet3 (FsqCheckinTweetType) primary key id; \n");
        sb.append(" create dataset FsqCheckinTweet4 (FsqCheckinTweetType) primary key id; \n");
        sb.append(" create dataset FsqCheckinTweet5 (FsqCheckinTweetType) primary key id; \n");
        sb.append(" create dataset FsqCheckinTweet6 (FsqCheckinTweetType) primary key id; \n");
        sb.append(" create dataset FsqCheckinTweet7 (FsqCheckinTweetType) primary key id; \n");
        sb.append(" create dataset FsqCheckinTweet8 (FsqCheckinTweetType) primary key id; \n");
        sb.append(" create dataset FsqCheckinTweet9 (FsqCheckinTweetType) primary key id; \n");

        //create indexes
        if (indexType.contains("rtree") || indexType.contains("dhbtree")) {
            sb.append("create index " + indexType + "CheckinCoordinate0 on FsqCheckinTweet0(coordinates) type "
                    + indexType + " ;\n");
            sb.append("create index " + indexType + "CheckinCoordinate1 on FsqCheckinTweet1(coordinates) type "
                    + indexType + " ;\n");
            sb.append("create index " + indexType + "CheckinCoordinate2 on FsqCheckinTweet2(coordinates) type "
                    + indexType + " ;\n");
            sb.append("create index " + indexType + "CheckinCoordinate3 on FsqCheckinTweet3(coordinates) type "
                    + indexType + " ;\n");
            sb.append("create index " + indexType + "CheckinCoordinate4 on FsqCheckinTweet4(coordinates) type "
                    + indexType + " ;\n");
            sb.append("create index " + indexType + "CheckinCoordinate5 on FsqCheckinTweet5(coordinates) type "
                    + indexType + " ;\n");
            sb.append("create index " + indexType + "CheckinCoordinate6 on FsqCheckinTweet6(coordinates) type "
                    + indexType + " ;\n");
            sb.append("create index " + indexType + "CheckinCoordinate7 on FsqCheckinTweet7(coordinates) type "
                    + indexType + " ;\n");
            sb.append("create index " + indexType + "CheckinCoordinate8 on FsqCheckinTweet8(coordinates) type "
                    + indexType + " ;\n");
            sb.append("create index " + indexType + "CheckinCoordinate9 on FsqCheckinTweet9(coordinates) type "
                    + indexType + " ;\n");
        } else {
            sb.append("create index " + indexType + "CheckinCoordinate0 on FsqCheckinTweet0(coordinates) type "
                    + indexType + "(-180.0,-90.0,180.0,90.0);\n");
            sb.append("create index " + indexType + "CheckinCoordinate1 on FsqCheckinTweet1(coordinates) type "
                    + indexType + "(-180.0,-90.0,180.0,90.0);\n");
            sb.append("create index " + indexType + "CheckinCoordinate2 on FsqCheckinTweet2(coordinates) type "
                    + indexType + "(-180.0,-90.0,180.0,90.0);\n");
            sb.append("create index " + indexType + "CheckinCoordinate3 on FsqCheckinTweet3(coordinates) type "
                    + indexType + "(-180.0,-90.0,180.0,90.0);\n");
            sb.append("create index " + indexType + "CheckinCoordinate4 on FsqCheckinTweet4(coordinates) type "
                    + indexType + "(-180.0,-90.0,180.0,90.0);\n");
            sb.append("create index " + indexType + "CheckinCoordinate5 on FsqCheckinTweet5(coordinates) type "
                    + indexType + "(-180.0,-90.0,180.0,90.0);\n");
            sb.append("create index " + indexType + "CheckinCoordinate6 on FsqCheckinTweet6(coordinates) type "
                    + indexType + "(-180.0,-90.0,180.0,90.0);\n");
            sb.append("create index " + indexType + "CheckinCoordinate7 on FsqCheckinTweet7(coordinates) type "
                    + indexType + "(-180.0,-90.0,180.0,90.0);\n");
            sb.append("create index " + indexType + "CheckinCoordinate8 on FsqCheckinTweet8(coordinates) type "
                    + indexType + "(-180.0,-90.0,180.0,90.0);\n");
            sb.append("create index " + indexType + "CheckinCoordinate9 on FsqCheckinTweet9(coordinates) type "
                    + indexType + "(-180.0,-90.0,180.0,90.0);\n");
        }

        //create datatype for join dataset
        sb.append(" create type FsqVenueType as closed { id: int64, name: string, phone: string?, coordinates: point, ");
        sb.append(" categories: string, checkin_count: int64?, user_count: int64?, tags: string, url: string? } \n");

        //create datasets for join dataset
        sb.append(" create dataset FsqVenue0 (FsqVenueType) primary key id; \n");
        sb.append(" create dataset FsqVenue1 (FsqVenueType) primary key id; \n");
        sb.append(" create dataset FsqVenue2 (FsqVenueType) primary key id; \n");
        sb.append(" create dataset FsqVenue3 (FsqVenueType) primary key id; \n");
        sb.append(" create dataset FsqVenue4 (FsqVenueType) primary key id; \n");
        sb.append(" create dataset FsqVenue5 (FsqVenueType) primary key id; \n");
        sb.append(" create dataset FsqVenue6 (FsqVenueType) primary key id; \n");
        sb.append(" create dataset FsqVenue7 (FsqVenueType) primary key id; \n");
        sb.append(" create dataset FsqVenue8 (FsqVenueType) primary key id; \n");
        sb.append(" create dataset FsqVenue9 (FsqVenueType) primary key id; \n");

        return sb.toString();
    }
}
