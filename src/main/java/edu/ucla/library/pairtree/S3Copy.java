
package edu.ucla.library.pairtree;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import javax.inject.Inject;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;

import info.freelibrary.pairtree.PairtreeFactory;
import info.freelibrary.pairtree.PairtreeFactory.PairtreeImpl;
import info.freelibrary.pairtree.PairtreeRoot;
import info.freelibrary.util.Logger;
import info.freelibrary.util.LoggerFactory;

import io.airlift.airline.Command;
import io.airlift.airline.HelpOption;
import io.airlift.airline.Option;
import io.airlift.airline.SingleCommand;
import io.vertx.core.Vertx;

/**
 * A throw-away command line program to move S3 Pairtrees to another bucket.
 * <p>
 * To use: <pre>java -cp target/s3-pairtree-copy-0.0.1.jar edu.ucla.library.pairtree.S3Copy -h</pre>
 * </p>
 */
@Command(name = "edu.ucla.library.pairtree.S3Copy", description = "As S3 Pairtree structure copy tool")
public final class S3Copy {

    private static final Logger LOGGER = LoggerFactory.getLogger(S3Copy.class, "s3ptcopy_messages");

    @Inject
    public HelpOption myHelpOption;

    @Option(name = { "-c", "--csv" }, description = "A CSV file from a file system traversal")
    public File myCSVFile;

    @Option(name = { "-o", "--objects" },
            description = "A semicolon delimited list of objects to copy between buckets")
    public String myObjects;

    @Option(name = { "-s", "--source" }, description = "A source S3 bucket")
    public String mySource;

    @Option(name = { "-d", "--destination" }, description = "A destination S3 bucket")
    public String myDestination;

    /**
     * The main method for the reconciler program.
     *
     * @param args Arguments supplied to the program
     */
    @SuppressWarnings("uncommentedmain")
    public static void main(final String[] args) {
        final S3Copy s3Copy = SingleCommand.singleCommand(S3Copy.class).parse(args);

        if (s3Copy.myHelpOption.showHelpIfRequested()) {
            return;
        }

        s3Copy.run();
    }

    private void run() {
        Objects.requireNonNull(myObjects, LOGGER.getMessage(MessageCodes.T_001));
        Objects.requireNonNull(myCSVFile, LOGGER.getMessage(MessageCodes.T_002));
        Objects.requireNonNull(mySource, LOGGER.getMessage(MessageCodes.T_005));
        Objects.requireNonNull(myDestination, LOGGER.getMessage(MessageCodes.T_006));

        final PairtreeFactory factory = PairtreeFactory.getFactory(Vertx.factory.vertx(), PairtreeImpl.S3Bucket);
        final AmazonS3ClientBuilder builder = AmazonS3ClientBuilder.standard().withRegion(Regions.EU_WEST_1);
        final AWSCredentials creds = builder.getCredentials().getCredentials();
        final AmazonS3 s3Client = builder.withForceGlobalBucketAccessEnabled(true).build(); // we can use copyObject!
        final PairtreeRoot ptRoot = factory.getPairtree(mySource, creds.getAWSAccessKeyId(), creds.getAWSSecretKey());

        final List<String[]> fsImages = new ArrayList<>();
        final String[] objectIDs = myObjects.split(";");
        final Set<String> objects = new HashSet<>();

        try {
            final CSVParser parser = new CSVParserBuilder().withSeparator(',').withIgnoreQuotations(true).build();
            final CSVReader reader = new CSVReaderBuilder(new FileReader(myCSVFile)).withCSVParser(parser).build();

            try {
                try {
                    fsImages.addAll(reader.readAll());
                    reader.close();
                } catch (final IOException details) {
                    throw new IOException(myCSVFile.getAbsolutePath(), details);
                }

                final Iterator<String[]> iterator = fsImages.iterator();

                while (iterator.hasNext()) {
                    final String[] imageMetadata = iterator.next();

                    if (imageMetadata.length == 2) {
                        final String id = imageMetadata[0];
                        final String path = imageMetadata[1];

                        for (final String objectID : objectIDs) {
                            if (path.contains(objectID)) {
                                final String s3Path = ptRoot.getObject(id).getPath();
                                final ListObjectsRequest list = new ListObjectsRequest();
                                final ObjectListing listing;

                                list.withPrefix(s3Path).withBucketName(mySource);
                                listing = s3Client.listObjects(list);

                                for (final S3ObjectSummary object : listing.getObjectSummaries()) {
                                    final String key = object.getKey();

                                    s3Client.copyObject(mySource, key, myDestination, key);
                                }

                                LOGGER.info(MessageCodes.T_007, id, mySource, myDestination);

                                if (!objects.contains(objectID)) {
                                    LOGGER.info(MessageCodes.T_008, objectID);
                                    objects.add(objectID);
                                }
                            }
                        }
                    }
                }
            } catch (final IOException details) {
                LOGGER.error(MessageCodes.T_004, details.getMessage(), details.getCause());
            }
        } catch (final FileNotFoundException details) {
            LOGGER.error(MessageCodes.T_003, myCSVFile);
        }
    }

}
