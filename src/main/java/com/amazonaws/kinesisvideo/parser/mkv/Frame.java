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
//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by FernFlower decompiler)
//

package com.amazonaws.kinesisvideo.parser.mkv;

import com.amazonaws.kinesisvideo.parser.ebml.EBMLUtils;
import java.nio.ByteBuffer;
import org.apache.commons.lang3.Validate;

public class Frame {
    private final long trackNumber;
    private final int timeCode;
    private final boolean keyFrame;
    private final boolean invisible;
    private final boolean discardable;
    private final Lacing lacing;
    private final ByteBuffer frameData;

    public static Frame withoutCopy(ByteBuffer simpleBlockDataBuffer) {
        FrameBuilder builder = getBuilderWithCommonParams(simpleBlockDataBuffer);
        ByteBuffer frameData = simpleBlockDataBuffer.slice();
        return builder.frameData(frameData).build();
    }

    public static Frame withCopy(ByteBuffer simpleBlockDataBuffer) {
        FrameBuilder builder = getBuilderWithCommonParams(simpleBlockDataBuffer);
        ByteBuffer frameData = ByteBuffer.allocate(simpleBlockDataBuffer.remaining());
        frameData.put(simpleBlockDataBuffer);
        frameData.flip();
        return builder.frameData(frameData).build();
    }

    private static FrameBuilder getBuilderWithCommonParams(ByteBuffer simpleBlockDataBuffer) {
        FrameBuilder builder = builder().trackNumber(EBMLUtils.readEbmlInt(simpleBlockDataBuffer)).timeCode((int)EBMLUtils.readDataSignedInteger(simpleBlockDataBuffer, 2L));
        long flag = EBMLUtils.readUnsignedIntegerSevenBytesOrLess(simpleBlockDataBuffer, 1L);
        builder.keyFrame((flag & 128L) > 0L).invisible((flag & 8L) > 0L).discardable((flag & 1L) > 0L);
        int laceValue = (int)(flag & 6L) >> 1;
        Lacing lacing = getLacing(laceValue);
        builder.lacing(lacing);
        return builder;
    }

    private static Lacing getLacing(int laceValue) {
        switch (laceValue) {
            case 0:
                return Frame.Lacing.NO;
            case 1:
                return Frame.Lacing.XIPH;
            case 2:
                return Frame.Lacing.EBML;
            case 3:
                return Frame.Lacing.FIXED_SIZE;
            default:
                Validate.isTrue(false, "Invalid value of lacing " + laceValue, new Object[0]);
                throw new IllegalArgumentException("Invalid value of lacing " + laceValue);
        }
    }

    public static FrameBuilder builder() {
        return new FrameBuilder();
    }

    public FrameBuilder toBuilder() {
        return (new FrameBuilder()).trackNumber(this.trackNumber).timeCode(this.timeCode).keyFrame(this.keyFrame).invisible(this.invisible).discardable(this.discardable).lacing(this.lacing).frameData(this.frameData);
    }

    public long getTrackNumber() {
        return this.trackNumber;
    }

    public int getTimeCode() {
        return this.timeCode;
    }

    public boolean isKeyFrame() {
        return this.keyFrame;
    }

    public boolean isInvisible() {
        return this.invisible;
    }

    public boolean isDiscardable() {
        return this.discardable;
    }

    public Lacing getLacing() {
        return this.lacing;
    }

    public ByteBuffer getFrameData() {
        return this.frameData;
    }

    private Frame(long trackNumber, int timeCode, boolean keyFrame, boolean invisible, boolean discardable, Lacing lacing, ByteBuffer frameData) {
        this.trackNumber = trackNumber;
        this.timeCode = timeCode;
        this.keyFrame = keyFrame;
        this.invisible = invisible;
        this.discardable = discardable;
        this.lacing = lacing;
        this.frameData = frameData;
    }

    public String toString() {
        return "Frame(trackNumber=" + this.getTrackNumber() + ", timeCode=" + this.getTimeCode() + ", keyFrame=" + this.isKeyFrame() + ", invisible=" + this.isInvisible() + ", discardable=" + this.isDiscardable() + ", lacing=" + this.getLacing() + ")";
    }

    public static class FrameBuilder {
        private long trackNumber;
        private int timeCode;
        private boolean keyFrame;
        private boolean invisible;
        private boolean discardable;
        private Lacing lacing;
        private ByteBuffer frameData;

        FrameBuilder() {
        }

        public FrameBuilder trackNumber(long trackNumber) {
            this.trackNumber = trackNumber;
            return this;
        }

        public FrameBuilder timeCode(int timeCode) {
            this.timeCode = timeCode;
            return this;
        }

        public FrameBuilder keyFrame(boolean keyFrame) {
            this.keyFrame = keyFrame;
            return this;
        }

        public FrameBuilder invisible(boolean invisible) {
            this.invisible = invisible;
            return this;
        }

        public FrameBuilder discardable(boolean discardable) {
            this.discardable = discardable;
            return this;
        }

        public FrameBuilder lacing(Lacing lacing) {
            this.lacing = lacing;
            return this;
        }

        public FrameBuilder frameData(ByteBuffer frameData) {
            this.frameData = frameData;
            return this;
        }

        public Frame build() {
            return new Frame(this.trackNumber, this.timeCode, this.keyFrame, this.invisible, this.discardable, this.lacing, this.frameData);
        }

        public String toString() {
            return "Frame.FrameBuilder(trackNumber=" + this.trackNumber + ", timeCode=" + this.timeCode + ", keyFrame=" + this.keyFrame + ", invisible=" + this.invisible + ", discardable=" + this.discardable + ", lacing=" + this.lacing + ", frameData=" + this.frameData + ")";
        }
    }

    public static enum Lacing {
        NO,
        XIPH,
        EBML,
        FIXED_SIZE;

        private Lacing() {
        }
    }
}

