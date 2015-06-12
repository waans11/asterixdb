package edu.uci.ics.asterix.api.java;

import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URISyntaxException;

import org.apache.http.HttpResponse;

import edu.uci.ics.hyracks.api.util.StopWatch;

public class SpatialIndexEvaluatorForSyntheticDataIngestion {
    private static String ipAddress = "127.0.0.1";
    private static String portNum = "19002";
    private static String indexType;
    private static StopWatch sw = new StopWatch();
    private static String admFilePath;

    public static void main(String[] args) throws URISyntaxException, IOException {
        if (args.length < 4) {
            System.out
                    .println("Example Usage: java -jar SpatialIndexEvaluatorForSyntheticDataIngestion.jar <index type> <cc ip address> <asterix api port num> <adm file path>");
            System.out.println("\targ0: index type - rtree, shbtree, dhbtree, or sif");
            System.out.println("\targ1: asterix cc ip address");
            System.out.println("\targ2: asterix api port number");
            System.out.println("\targ3: adm file path");
            System.exit(-1);
        }
        indexType = args[0];
        ipAddress = args[1];
        portNum = args[2];
        admFilePath = args[3];
        runDatasetIndexCreation();
    }

    private static void runDatasetIndexCreation() throws URISyntaxException, IOException {
        HttpResponse response;
        AsterixHttpClient ahc = new AsterixHttpClient(ipAddress, portNum);
        StringBuilder sb = new StringBuilder();
        FileOutputStream fos = null;
        fos = ahc.openOutputFile("./" + indexType + "IngestionResult.txt");

        try {
            // ddl
            ahc.prepareDDL(getDDLAQL());
            response = ahc.execute();
            ahc.printResult(response, null);

            // start ingestion
            ahc.prepareUpdate(getIngestionAQL());
            sw.start();
            response = ahc.execute();
            sw.suspend();
            sb.append("Ingestion\t" + sw.getElapsedTime() + "\n");
            ahc.printResult(response, fos);
        } finally {
            ahc.closeOutputFile(fos);
            System.out.println(sb.toString());
        }
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
        sb.append(" create dataset FsqCheckinTweet (FsqCheckinTweetType) primary key id \n");

        //create indexes
        if (indexType.contains("rtree") || indexType.contains("dhbtree")) {
            sb.append("create index " + indexType + "CheckinCoordinate on FsqCheckinTweet(coordinates) type "
                    + indexType + " ;\n");
        } else {
            sb.append("create index " + indexType + "CheckinCoordinate on FsqCheckinTweet(coordinates) type "
                    + indexType + "(-180.0,-90.0,180.0,90.0,h,h,h,h,h);\n");
        }

        //create feed
        sb.append("create feed TweetFeed using file_feed ((\"fs\"=\"localfs\"),");
        sb.append("(\"path\"=\"" + ipAddress + ":///" + admFilePath + "\"),(\"format\"=\"adm\"),");
        sb.append("(\"type-name\"=\"FsqCheckinTweetType\"),(\"tuple-interval\"=\"0\")); \n");

        return sb.toString();
    }

    private static String getIngestionAQL() {
        StringBuilder sb = new StringBuilder();
        sb.append("use dataverse STBench;\n");
        sb.append("set wait-for-completion-feed \"true\";\n");
        sb.append("connect feed TweetFeed to dataset FsqCheckinTweet;\n");
        return sb.toString();
    }
}