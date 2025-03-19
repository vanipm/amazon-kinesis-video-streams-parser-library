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

package com.amazonaws.kinesisvideo.parser.examples.lambda;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.kinesisvideo.client.KinesisVideoClient;
import com.amazonaws.kinesisvideo.client.mediasource.CameraMediaSourceConfiguration;
import com.amazonaws.kinesisvideo.common.exception.KinesisVideoException;
import com.amazonaws.kinesisvideo.java.client.KinesisVideoJavaClientFactory;
import com.amazonaws.kinesisvideo.parser.examples.BoundingBoxImagePanel;
import com.amazonaws.kinesisvideo.parser.mkv.Frame;
import com.amazonaws.kinesisvideo.parser.mkv.FrameProcessException;
import com.amazonaws.kinesisvideo.parser.rekognition.pojo.RekognizedOutput;
import com.amazonaws.kinesisvideo.parser.utilities.FragmentMetadata;
import com.amazonaws.kinesisvideo.parser.utilities.FrameVisitor;
import com.amazonaws.kinesisvideo.parser.utilities.H264FrameDecoder;
import com.amazonaws.kinesisvideo.parser.utilities.H264FrameEncoder;
import com.amazonaws.kinesisvideo.parser.utilities.MkvTrackMetadata;
import com.amazonaws.kinesisvideo.parser.utilities.ProducerStreamUtil;
import com.amazonaws.kinesisvideo.producer.StreamInfo.NalAdaptationFlags;
import com.amazonaws.regions.Regions;
import com.google.common.base.Preconditions;
import java.awt.image.BufferedImage;
import java.math.BigInteger;
import java.util.List;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class H264FrameProcessor implements FrameVisitor.FrameProcessor {
    private static final Logger log = LoggerFactory.getLogger(H264FrameProcessor.class);
    private static final int VIDEO_TRACK_NO = 1;
    private static final int MILLIS_IN_SEC = 1000;
    private static final int OFFSET_DELTA_THRESHOLD = 10;
    private final BoundingBoxImagePanel boundingBoxImagePanel = new BoundingBoxImagePanel();
    private final Regions regionName;
    private RekognizedOutput currentRekognizedOutput = null;
    private H264FrameEncoder h264Encoder;
    private H264FrameDecoder h264Decoder;
    private KVSMediaSource KVSMediaSource;
    private boolean isKVSProducerInitialized = false;
    private boolean isEncoderInitialized = false;
    private final AWSCredentialsProvider credentialsProvider;
    private final String outputKvsStreamName;
    private List<RekognizedOutput> rekognizedOutputs;
    private int frameBitRate = 1024;
    private int frameNo = 0;
    private int currentWidth = 0;
    private int currentHeight = 0;
    private long keyFrameTimecode;
    private long fragmentStartTime;

    private H264FrameProcessor(AWSCredentialsProvider credentialsProvider, String outputKvsStreamName, Regions regionName) {
        this.credentialsProvider = credentialsProvider;
        this.outputKvsStreamName = outputKvsStreamName;
        this.regionName = regionName;
        this.h264Decoder = new H264FrameDecoder();
        this.fragmentStartTime = System.currentTimeMillis();
    }

    private void initializeKinesisVideoProducer(int width, int height, byte[] cpd) {
        try {
            log.info("Initializing KVS Producer with stream name {} and region : {}", this.outputKvsStreamName, this.regionName);
            KinesisVideoClient kinesisVideoClient = KinesisVideoJavaClientFactory.createKinesisVideoClient(this.regionName, this.credentialsProvider);
            log.info("CameraMediaSourceConfiguration");
            CameraMediaSourceConfiguration configuration = (new CameraMediaSourceConfiguration.Builder()).withFrameRate(30).withRetentionPeriodInHours(1).withCameraId("/dev/video0").withIsEncoderHardwareAccelerated(false).withEncodingMimeType("video/avc").withNalAdaptationFlags(NalAdaptationFlags.NAL_ADAPTATION_ANNEXB_NALS).withIsAbsoluteTimecode(true).withEncodingBitRate(200000).withHorizontalResolution(width).withVerticalResolution(height).withCodecPrivateData(cpd).build();
            this.KVSMediaSource = new KVSMediaSource(ProducerStreamUtil.toStreamInfo(this.outputKvsStreamName, configuration));
            this.KVSMediaSource.configure(configuration);
            kinesisVideoClient.registerMediaSource(this.KVSMediaSource);
        } catch (KinesisVideoException e) {
            log.error("Exception while initialize KVS Producer !", e);
        }

    }

    public void resetEncoder() {
        if (this.isEncoderInitialized) {
            this.frameNo = 0;
            this.h264Encoder.setFrameNumber(this.frameNo);
        } else {
            throw new IllegalStateException("Encoder not initialized !");
        }
    }

    public static H264FrameProcessor create(AWSCredentialsProvider credentialsProvider, String rekognizedStreamName, Regions regionName) {
        return new H264FrameProcessor(credentialsProvider, rekognizedStreamName, regionName);
    }

    public void printCallStack() {
        StackTraceElement[] stackTrace = Thread.currentThread().getStackTrace();
        log.debug("Call stack:");

        for(StackTraceElement element : stackTrace) {
            log.debug("\tat {}", element);
        }

    }

    public void process(Frame frame, MkvTrackMetadata trackMetadata, Optional<FragmentMetadata> fragmentMetadata) throws FrameProcessException {
        log.debug("H264FrameProcessor-Processing frame: {}", frame);
        this.printCallStack();
        if (this.rekognizedOutputs != null) {
            if (frame.getTrackNumber() == 1L) {
                Preconditions.checkState(trackMetadata.getPixelWidth().isPresent() && trackMetadata.getPixelHeight().isPresent(), "Missing video resolution in track metadata !");
                Preconditions.checkState(fragmentMetadata.isPresent(), "FragmentMetadata should be present !");
                BufferedImage decodedFrame = this.h264Decoder.decodeH264Frame(frame, trackMetadata);
                int timeCode = frame.getTimeCode();
                log.debug("Decoded frame : {} with timecode : {} CachedTimeCode {} and fragment metadata : {}", new Object[]{this.frameNo, frame.getTimeCode(), timeCode, fragmentMetadata.get()});
                Optional<RekognizedOutput> rekognizedOutput = this.findRekognizedOutputForFrame(frame, fragmentMetadata);
                BufferedImage compositeFrame = this.renderFrame(decodedFrame, rekognizedOutput);
                EncodedFrame encodedH264Frame = this.encodeH264Frame(compositeFrame);
                encodedH264Frame.setTimeCode((long)frame.getTimeCode());
                encodedH264Frame.setProducerSideTimeStampMillis(((FragmentMetadata)fragmentMetadata.get()).getProducerSideTimestampMillis());
                encodedH264Frame.setServerSideTimeStampMillis(((FragmentMetadata)fragmentMetadata.get()).getServerSideTimestampMillis());
                log.debug("Encoded frame : {} with timecode : {} ProducerSideTimeStampMillis {} ServerSideTimeStampMillis {}", new Object[]{this.frameNo, encodedH264Frame.getTimeCode(), encodedH264Frame.getProducerSideTimeStampMillis(), encodedH264Frame.getServerSideTimeStampMillis()});
                log.debug("EncodedFrame: encodedH264Frame: {}", encodedH264Frame);
                this.putFrame(encodedH264Frame, ((BigInteger)trackMetadata.getPixelWidth().get()).intValue(), ((BigInteger)trackMetadata.getPixelHeight().get()).intValue(), timeCode);
                ++this.frameNo;
            } else {
                log.debug("Skipping audio frames !");
            }
        } else {
            log.warn("Rekognition output is empty");
        }

    }

    private void putFrame(EncodedFrame encodedH264Frame, int width, int height, int timeCode) {
        if (!this.isKVSProducerInitialized) {
            log.info("Initializing JNI...");
            this.initializeKinesisVideoProducer(width, height, encodedH264Frame.getCpd().array());
            this.isKVSProducerInitialized = true;
        }

        log.debug("Putting frame {} with getTimeCode {} producerSideTimeStampMillis {} serverSideTimeStampMillis {}", new Object[]{this.frameNo, encodedH264Frame.getTimeCode(), encodedH264Frame.getProducerSideTimeStampMillis(), encodedH264Frame.getServerSideTimeStampMillis()});
        this.KVSMediaSource.putFrameData(encodedH264Frame, timeCode);
        log.debug("PutFrame successful for frame no : {}", this.frameNo);
    }

    private EncodedFrame encodeH264Frame(BufferedImage bufferedImage) {
        try {
            this.initializeEncoder(bufferedImage);
            return this.h264Encoder.encodeFrame(bufferedImage);
        } catch (Exception e) {
            throw new RuntimeException("Unable to encode the bufferedImage !", e);
        }
    }

    private void initializeEncoder(BufferedImage bufferedImage) {
        if (!this.isEncoderInitialized || this.currentWidth != bufferedImage.getWidth() || this.currentHeight != bufferedImage.getHeight()) {
            this.h264Encoder = new H264FrameEncoder(bufferedImage.getWidth(), bufferedImage.getHeight(), this.frameBitRate);
            this.isEncoderInitialized = true;
            this.currentWidth = bufferedImage.getWidth();
            this.currentHeight = bufferedImage.getHeight();
        }

    }

    private Optional<RekognizedOutput> findRekognizedOutputForFrame(Frame frame, Optional<FragmentMetadata> fragmentMetadata) {
        Optional<RekognizedOutput> rekognizedOutput = Optional.empty();
        if (fragmentMetadata.isPresent()) {
            String fragmentNumber = ((FragmentMetadata)fragmentMetadata.get()).getFragmentNumberString();
            if (frame.isKeyFrame()) {
                this.keyFrameTimecode = (long)frame.getTimeCode();
                log.debug("Key frame timecode : {}", this.keyFrameTimecode);
                this.fragmentStartTime = System.currentTimeMillis();
            }

            long frameOffset = (long)frame.getTimeCode() > this.keyFrameTimecode ? (long)frame.getTimeCode() - this.keyFrameTimecode : 0L;
            log.debug("Current Fragment Number : {} Computed Frame offset : {}", fragmentNumber, frameOffset);
            if (log.isDebugEnabled()) {
                this.rekognizedOutputs.forEach((p) -> log.debug("frameOffsetInSeconds from Rekognition : {}", p.getFrameOffsetInSeconds()));
            }

            rekognizedOutput = this.rekognizedOutputs.stream().filter((p) -> this.isOffsetDeltaWithinThreshold(frameOffset, p)).findFirst();
            if (rekognizedOutput.isPresent()) {
                log.debug("Computed offset matched with retrieved offset. Delta : {}", Math.abs((double)frameOffset - ((RekognizedOutput)rekognizedOutput.get()).getFrameOffsetInSeconds() * (double)1000.0F));
                if (this.rekognizedOutputs.isEmpty()) {
                    log.debug("All frames processed for this fragment number : {}", fragmentNumber);
                }
            }
        }

        return rekognizedOutput;
    }

    private boolean isOffsetDeltaWithinThreshold(long frameOffset, RekognizedOutput output) {
        return Math.abs((double)frameOffset - output.getFrameOffsetInSeconds() * (double)1000.0F) <= (double)10.0F;
    }

    private BufferedImage renderFrame(BufferedImage bufferedImage, Optional<RekognizedOutput> rekognizedOutput) {
        if (rekognizedOutput.isPresent()) {
            log.debug("Rendering Rekognized sampled frame...");
            this.boundingBoxImagePanel.processRekognitionOutput(bufferedImage.createGraphics(), bufferedImage.getWidth(), bufferedImage.getHeight(), (RekognizedOutput)rekognizedOutput.get());
            this.currentRekognizedOutput = (RekognizedOutput)rekognizedOutput.get();
        } else if (this.currentRekognizedOutput != null) {
            log.debug("Rendering non-sampled frame with previous rekognized results...");
            this.boundingBoxImagePanel.processRekognitionOutput(bufferedImage.createGraphics(), bufferedImage.getWidth(), bufferedImage.getHeight(), this.currentRekognizedOutput);
        } else {
            log.debug("Rendering frame without any rekognized results...");
        }

        return bufferedImage;
    }

    public void setRekognizedOutputs(List<RekognizedOutput> rekognizedOutputs) {
        this.rekognizedOutputs = rekognizedOutputs;
    }

    public void setFrameBitRate(int frameBitRate) {
        this.frameBitRate = frameBitRate;
    }
}
