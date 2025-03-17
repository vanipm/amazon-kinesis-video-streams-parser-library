/*
Copyright 2017-2017 Amazon.com, Inc. or its affiliates. All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License").
You may not use this file except in compliance with the License.
A copy of the License is located at

   http://aws.amazon.com/apache2.0/

or in the "license" file accompanying this file.
This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and limitations under the License.
*/

package com.amazonaws.kinesisvideo.parser.kinesis;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.kinesisvideo.parser.rekognition.pojo.RekognizedFragmentsIndex;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorFactory;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker;
import java.net.InetAddress;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class KinesisDataStreamsWorker implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(KinesisDataStreamsWorker.class);
    private static final String APPLICATION_NAME = "rekognition-kds-stream-application";
    private static final InitialPositionInStream SAMPLE_APPLICATION_INITIAL_POSITION_IN_STREAM;
    private final Regions region;
    private final AWSCredentialsProvider credentialsProvider;
    private final String kdsStreamName;
    private final RekognizedFragmentsIndex rekognizedFragmentsIndex;

    public static KinesisDataStreamsWorker create(Regions region, AWSCredentialsProvider credentialsProvider, String kdsStreamName, RekognizedFragmentsIndex rekognizedFragmentsIndex) {
        return new KinesisDataStreamsWorker(region, credentialsProvider, kdsStreamName, rekognizedFragmentsIndex);
    }

    public void run() {
        try {
            String workerId = InetAddress.getLocalHost().getCanonicalHostName() + ":" + UUID.randomUUID();
            KinesisClientLibConfiguration kinesisClientLibConfiguration = new KinesisClientLibConfiguration("rekognition-kds-stream-application", this.kdsStreamName, this.credentialsProvider, workerId);
            kinesisClientLibConfiguration.withInitialPositionInStream(SAMPLE_APPLICATION_INITIAL_POSITION_IN_STREAM).withRegionName(this.region.getName());
            IRecordProcessorFactory recordProcessorFactory = () -> new KinesisRecordProcessor(this.rekognizedFragmentsIndex, this.credentialsProvider);
            Worker worker = new Worker(recordProcessorFactory, kinesisClientLibConfiguration);
            log.debug("Running %s to process stream %s as worker %s...", new Object[]{"rekognition-kds-stream-application", this.kdsStreamName, workerId});
            System.out.printf("Running %s to process stream %s as worker %s...", "rekognition-kds-stream-application", this.kdsStreamName, workerId);
            log.debug("Starting worker thread...");
            int exitCode = 0;

            try {
                worker.run();
            } catch (Throwable t) {
                System.err.println("Caught throwable while processing data.");
                t.printStackTrace();
                exitCode = 1;
            }

            System.out.println("Exit code : " + exitCode);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    private KinesisDataStreamsWorker(Regions region, AWSCredentialsProvider credentialsProvider, String kdsStreamName, RekognizedFragmentsIndex rekognizedFragmentsIndex) {
        this.region = region;
        this.credentialsProvider = credentialsProvider;
        this.kdsStreamName = kdsStreamName;
        this.rekognizedFragmentsIndex = rekognizedFragmentsIndex;
    }

    static {
        SAMPLE_APPLICATION_INITIAL_POSITION_IN_STREAM = InitialPositionInStream.LATEST;
    }
}
