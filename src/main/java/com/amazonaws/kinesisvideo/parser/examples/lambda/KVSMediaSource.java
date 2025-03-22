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

import com.amazonaws.kinesisvideo.client.mediasource.CameraMediaSourceConfiguration;
import com.amazonaws.kinesisvideo.client.mediasource.MediaSourceState;
import com.amazonaws.kinesisvideo.common.exception.KinesisVideoException;
import com.amazonaws.kinesisvideo.internal.client.mediasource.MediaSource;
import com.amazonaws.kinesisvideo.internal.client.mediasource.MediaSourceConfiguration;
import com.amazonaws.kinesisvideo.internal.client.mediasource.MediaSourceSink;
import com.amazonaws.kinesisvideo.producer.KinesisVideoFrame;
import com.amazonaws.kinesisvideo.producer.StreamCallbacks;
import com.amazonaws.kinesisvideo.producer.StreamInfo;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KVSMediaSource implements MediaSource {
    private static final Logger log = LoggerFactory.getLogger(KVSMediaSource.class);
    private static final int FRAME_FLAG_KEY_FRAME = 1;
    private static final int FRAME_FLAG_NONE = 0;
    private static final long HUNDREDS_OF_NANOS_IN_MS = 10000L;
    private static final long FRAME_DURATION_20_MS = 20L;
    private CameraMediaSourceConfiguration cameraMediaSourceConfiguration;
    private MediaSourceState mediaSourceState;
    private MediaSourceSink mediaSourceSink;
    private int frameIndex;
    private final StreamInfo streamInfo;
    private long prevTimeCode = 0L;
    private long duration = 0L;
    private long totalDuration = 0L;
    private long putFrameTimeCode = 0L;
    private boolean firstFrameReset = false;
    private int frameWithinFragment = 1;

    private void putFrame(KinesisVideoFrame kinesisVideoFrame) {
        try {
            log.debug("PutFrame for frame no : {} ; kinesisVideoFrame {}", this.frameIndex, kinesisVideoFrame);
            this.mediaSourceSink.onFrame(kinesisVideoFrame);
        } catch (KinesisVideoException ex) {
            throw new RuntimeException(ex);
        }
    }

    public MediaSourceState getMediaSourceState() {
        return this.mediaSourceState;
    }

    public MediaSourceConfiguration getConfiguration() {
        return this.cameraMediaSourceConfiguration;
    }

    public StreamInfo getStreamInfo() throws KinesisVideoException {
        return this.streamInfo;
    }

    public void initialize(MediaSourceSink mediaSourceSink) {
        this.mediaSourceSink = mediaSourceSink;
    }

    public void configure(MediaSourceConfiguration configuration) {
        if (!(configuration instanceof CameraMediaSourceConfiguration)) {
            throw new IllegalStateException("Configuration must be an instance of CameraMediaSourceConfiguration");
        } else {
            this.cameraMediaSourceConfiguration = (CameraMediaSourceConfiguration)configuration;
            this.frameIndex = 0;
        }
    }

    public void start() {
        this.mediaSourceState = MediaSourceState.RUNNING;
    }

    public void putFrameData(EncodedFrame encodedFrame, int timeCode) {
        log.debug("putFrameData : {} producerSideTimeStampMillis {} serverSideTimeStampMillis {} ", new Object[]{encodedFrame, encodedFrame.getProducerSideTimeStampMillis(), encodedFrame.getServerSideTimeStampMillis()});
        int flags = encodedFrame.isKeyFrame() ? 1 : 0;
        if (encodedFrame.getByteBuffer() != null) {
            if (encodedFrame.isKeyFrame()) {
                this.duration = 5L;
                this.totalDuration = 0L;
                this.prevTimeCode = 0L;
                this.putFrameTimeCode = System.currentTimeMillis();
                this.firstFrameReset = true;
                this.frameWithinFragment = 0;
            } else {
                if (this.firstFrameReset) {
                    this.duration = encodedFrame.getTimeCode() - 5L;
                    this.firstFrameReset = false;
                } else {
                    this.duration = encodedFrame.getTimeCode() - this.prevTimeCode;
                }

                this.totalDuration += this.duration;
                if (this.totalDuration > 1998L) {
                    this.duration = 1L;
                }

                this.prevTimeCode = encodedFrame.getTimeCode();
                ++this.frameWithinFragment;
            }

            if (this.putFrameTimeCode == 0L) {
                this.putFrameTimeCode = System.currentTimeMillis();
            }

            log.debug(" frameTimeCode {} duration {} totalDurationMillis {} prevTimeCode {} currentTimeCode {}", new Object[]{encodedFrame.getTimeCode(), this.duration, this.totalDuration, this.prevTimeCode, encodedFrame.getTimeCode()});
            log.debug(" putFrameTime code {} frameWithinFragment {} ", this.putFrameTimeCode, this.frameWithinFragment);

            KinesisVideoFrame frame;
            if (encodedFrame.isKeyFrame()) {
                frame = new KinesisVideoFrame(this.frameIndex++, flags, this.putFrameTimeCode, this.putFrameTimeCode, 20L, encodedFrame.getByteBuffer());
            } else {
                frame = new KinesisVideoFrame(this.frameIndex++, flags, this.putFrameTimeCode + (long)(this.frameWithinFragment * 20), this.putFrameTimeCode + (long)(this.frameWithinFragment * 20), 200000L, encodedFrame.getByteBuffer());
            }

            if (frame.getSize() == 0) {
                return;
            }

            this.putFrame(frame);
        } else {
            log.info("Frame Data is null !");
        }

    }

    public void stop() {
        this.mediaSourceState = MediaSourceState.STOPPED;
    }

    public boolean isStopped() {
        return this.mediaSourceState == MediaSourceState.STOPPED;
    }

    public void free() {
    }

    public MediaSourceSink getMediaSourceSink() {
        return this.mediaSourceSink;
    }

    @Nullable
    public StreamCallbacks getStreamCallbacks() {
        return null;
    }

    public KVSMediaSource(StreamInfo streamInfo) {
        this.streamInfo = streamInfo;
    }
}

