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

/*
 * Copyright 2012-2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *  http://aws.amazon.com/apache2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.kinesisvideo.parser.rekognition.pojo.DetectedFace;
import com.amazonaws.kinesisvideo.parser.rekognition.pojo.FaceSearchResponse;
import com.amazonaws.kinesisvideo.parser.rekognition.pojo.MatchedFace;
import com.amazonaws.kinesisvideo.parser.rekognition.pojo.RekognitionOutput;
import com.amazonaws.kinesisvideo.parser.rekognition.pojo.RekognizedFragmentsIndex;
import com.amazonaws.kinesisvideo.parser.rekognition.pojo.RekognizedOutput;
import com.amazonaws.kinesisvideo.parser.rekognition.pojo.RekognizedOutput.FaceSearchOutput;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.InvalidStateException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ShutdownException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ThrottlingException;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.ShutdownReason;
import com.amazonaws.services.kinesis.model.Record;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.util.List;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KinesisRecordProcessor implements IRecordProcessor {
    private static final Logger log = LoggerFactory.getLogger(KinesisRecordProcessor.class);
    private static final Log LOG = LogFactory.getLog(KinesisRecordProcessor.class);
    private String kinesisShardId;
    private static final long BACKOFF_TIME_IN_MILLIS = 3000L;
    private static final int NUM_RETRIES = 10;
    private static final String DELIMITER = "$";
    private static final long CHECKPOINT_INTERVAL_MILLIS = 1000L;
    private long nextCheckpointTimeInMillis;
    private final CharsetDecoder decoder = Charset.forName("UTF-8").newDecoder();
    private final RekognizedFragmentsIndex rekognizedFragmentsIndex;
    private StringBuilder stringBuilder = new StringBuilder();

    public KinesisRecordProcessor(RekognizedFragmentsIndex rekognizedFragmentsIndex, AWSCredentialsProvider awsCredentialsProvider) {
        this.rekognizedFragmentsIndex = rekognizedFragmentsIndex;
    }

    public void initialize(String shardId) {
        LOG.info("Initializing record processor for shard: " + shardId);
        this.kinesisShardId = shardId;
    }

    public void processRecords(List<Record> records, IRecordProcessorCheckpointer checkpointer) {
        log.info("Processing " + records.size() + " records from " + this.kinesisShardId);
        LOG.info("Processing " + records.size() + " records from " + this.kinesisShardId);
        this.processRecordsWithRetries(records);
        if (System.currentTimeMillis() > this.nextCheckpointTimeInMillis) {
            this.checkpoint(checkpointer);
            this.nextCheckpointTimeInMillis = System.currentTimeMillis() + 1000L;
        }

    }

    private void processRecordsWithRetries(List<Record> records) {
        for(Record record : records) {
            boolean processedSuccessfully = false;

            for(int i = 0; i < 10; ++i) {
                try {
                    this.processSingleRecord(record);
                    processedSuccessfully = true;
                    break;
                } catch (Throwable t) {
                    LOG.warn("Caught throwable while processing record " + record, t);

                    try {
                        Thread.sleep(3000L);
                    } catch (InterruptedException e) {
                        LOG.debug("Interrupted sleep", e);
                    }
                }
            }

            if (!processedSuccessfully) {
                LOG.error("Couldn't process record " + record + ". Skipping the record.");
            }
        }

    }

    private void processSingleRecord(Record record) {
        String data = null;
        ObjectMapper mapper = new ObjectMapper();

        try {
            ByteBuffer buffer = record.getData();
            data = new String(buffer.array(), "UTF-8");
            this.stringBuilder = this.stringBuilder.append(data).append("$");
            RekognitionOutput output = (RekognitionOutput)mapper.readValue(data, RekognitionOutput.class);
            log.debug("KDS-RecordProcessor -Rekognition output: " + output);
            String fragmentNumber = output.getInputInformation().getKinesisVideo().getFragmentNumber();
            Double frameOffsetInSeconds = output.getInputInformation().getKinesisVideo().getFrameOffsetInSeconds();
            Double serverTimestamp = output.getInputInformation().getKinesisVideo().getServerTimestamp();
            Double producerTimestamp = output.getInputInformation().getKinesisVideo().getProducerTimestamp();
            double detectedTime = output.getInputInformation().getKinesisVideo().getServerTimestamp() + output.getInputInformation().getKinesisVideo().getFrameOffsetInSeconds() * (double)1000.0F;
            RekognizedOutput rekognizedOutput = RekognizedOutput.builder().fragmentNumber(fragmentNumber).serverTimestamp(serverTimestamp).producerTimestamp(producerTimestamp).frameOffsetInSeconds(frameOffsetInSeconds).detectedTime(detectedTime).build();
            List<FaceSearchResponse> responses = output.getFaceSearchResponse();
            log.debug("KDS-RecordProcessor -FaceSearch responses: " + responses);

            for(FaceSearchResponse response : responses) {
                DetectedFace detectedFace = response.getDetectedFace();
                List<MatchedFace> matchedFaces = response.getMatchedFaces();
                RekognizedOutput.FaceSearchOutput faceSearchOutput = FaceSearchOutput.builder().detectedFace(detectedFace).matchedFaceList(matchedFaces).build();
                rekognizedOutput.addFaceSearchOutput(faceSearchOutput);
            }

            this.rekognizedFragmentsIndex.add(fragmentNumber, producerTimestamp.longValue(), serverTimestamp.longValue(), rekognizedOutput);
        } catch (NumberFormatException var19) {
            LOG.info("Record does not match sample record format. Ignoring record with data; " + data);
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        } catch (JsonParseException e) {
            e.printStackTrace();
        } catch (JsonMappingException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public void shutdown(IRecordProcessorCheckpointer checkpointer, ShutdownReason reason) {
        LOG.info("Shutting down record processor for shard: " + this.kinesisShardId);
        if (reason == ShutdownReason.TERMINATE) {
            this.checkpoint(checkpointer);
        }

    }

    private void checkpoint(IRecordProcessorCheckpointer checkpointer) {
        LOG.info("Checkpointing shard " + this.kinesisShardId);
        int i = 0;

        while(i < 10) {
            try {
                checkpointer.checkpoint();
                break;
            } catch (ShutdownException se) {
                LOG.info("Caught shutdown exception, skipping checkpoint.", se);
                break;
            } catch (ThrottlingException e) {
                if (i >= 9) {
                    LOG.error("Checkpoint failed after " + (i + 1) + "attempts.", e);
                    break;
                }

                LOG.info("Transient issue when checkpointing - attempt " + (i + 1) + " of " + 10, e);

                try {
                    Thread.sleep(3000L);
                } catch (InterruptedException ex) {
                    LOG.debug("Interrupted sleep", ex);
                }

                ++i;
            } catch (InvalidStateException e) {
                LOG.error("Cannot save checkpoint to the DynamoDB table used by the Amazon Kinesis Client Library.", e);
                break;
            }
        }

    }
}
