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

import java.nio.ByteBuffer;

public class EncodedFrame {
    private final ByteBuffer byteBuffer;
    private final ByteBuffer cpd;
    private final boolean isKeyFrame;
    private long timeCode;
    long producerSideTimeStampMillis;
    long serverSideTimeStampMillis;

    EncodedFrame(ByteBuffer byteBuffer, ByteBuffer cpd, boolean isKeyFrame, long timeCode, long producerSideTimeStampMillis, long serverSideTimeStampMillis) {
        this.byteBuffer = byteBuffer;
        this.cpd = cpd;
        this.isKeyFrame = isKeyFrame;
        this.timeCode = timeCode;
        this.producerSideTimeStampMillis = producerSideTimeStampMillis;
        this.serverSideTimeStampMillis = serverSideTimeStampMillis;
    }

    public static EncodedFrameBuilder builder() {
        return new EncodedFrameBuilder();
    }

    public ByteBuffer getByteBuffer() {
        return this.byteBuffer;
    }

    public ByteBuffer getCpd() {
        return this.cpd;
    }

    public boolean isKeyFrame() {
        return this.isKeyFrame;
    }

    public long getTimeCode() {
        return this.timeCode;
    }

    public long getProducerSideTimeStampMillis() {
        return this.producerSideTimeStampMillis;
    }

    public long getServerSideTimeStampMillis() {
        return this.serverSideTimeStampMillis;
    }

    public void setTimeCode(long timeCode) {
        this.timeCode = timeCode;
    }

    public void setProducerSideTimeStampMillis(long producerSideTimeStampMillis) {
        this.producerSideTimeStampMillis = producerSideTimeStampMillis;
    }

    public void setServerSideTimeStampMillis(long serverSideTimeStampMillis) {
        this.serverSideTimeStampMillis = serverSideTimeStampMillis;
    }

    public static class EncodedFrameBuilder {
        private ByteBuffer byteBuffer;
        private ByteBuffer cpd;
        private boolean isKeyFrame;
        private long timeCode;
        private long producerSideTimeStampMillis;
        private long serverSideTimeStampMillis;

        EncodedFrameBuilder() {
        }

        public EncodedFrameBuilder byteBuffer(ByteBuffer byteBuffer) {
            this.byteBuffer = byteBuffer;
            return this;
        }

        public EncodedFrameBuilder cpd(ByteBuffer cpd) {
            this.cpd = cpd;
            return this;
        }

        public EncodedFrameBuilder isKeyFrame(boolean isKeyFrame) {
            this.isKeyFrame = isKeyFrame;
            return this;
        }

        public EncodedFrameBuilder timeCode(long timeCode) {
            this.timeCode = timeCode;
            return this;
        }

        public EncodedFrameBuilder producerSideTimeStampMillis(long producerSideTimeStampMillis) {
            this.producerSideTimeStampMillis = producerSideTimeStampMillis;
            return this;
        }

        public EncodedFrameBuilder serverSideTimeStampMillis(long serverSideTimeStampMillis) {
            this.serverSideTimeStampMillis = serverSideTimeStampMillis;
            return this;
        }

        public EncodedFrame build() {
            return new EncodedFrame(this.byteBuffer, this.cpd, this.isKeyFrame, this.timeCode, this.producerSideTimeStampMillis, this.serverSideTimeStampMillis);
        }

        public String toString() {
            return "EncodedFrame.EncodedFrameBuilder(byteBuffer=" + this.byteBuffer + ", cpd=" + this.cpd + ", isKeyFrame=" + this.isKeyFrame + ", timeCode=" + this.timeCode + ", producerSideTimeStampMillis=" + this.producerSideTimeStampMillis + ", serverSideTimeStampMillis=" + this.serverSideTimeStampMillis + ")";
        }
    }
}
