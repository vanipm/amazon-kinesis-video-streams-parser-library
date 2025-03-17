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


package com.amazonaws.kinesisvideo.parser.examples;

import com.amazonaws.kinesisvideo.parser.rekognition.pojo.BoundingBox;
import com.amazonaws.kinesisvideo.parser.rekognition.pojo.FaceType;
import com.amazonaws.kinesisvideo.parser.rekognition.pojo.MatchedFace;
import com.amazonaws.kinesisvideo.parser.rekognition.pojo.RekognizedOutput;
import java.awt.Color;
import java.awt.Graphics;
import java.awt.Graphics2D;
import java.awt.image.BufferedImage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BoundingBoxImagePanel extends ImagePanel {
    private static final Logger log = LoggerFactory.getLogger(BoundingBoxImagePanel.class);
    private static final String DELIMITER = "-";

    public BoundingBoxImagePanel() {
    }

    public void paintComponent(Graphics g) {
        super.paintComponent(g);
    }

    public void processRekognitionOutput(Graphics2D g2, int width, int height, RekognizedOutput rekognizedOutput) {
        if (rekognizedOutput != null && rekognizedOutput.getFaceSearchOutputs() != null) {
            log.debug("Number of detected faces in a frame {}", rekognizedOutput.getFaceSearchOutputs().size());

            for(RekognizedOutput.FaceSearchOutput faceSearchOutput : rekognizedOutput.getFaceSearchOutputs()) {
                log.debug("Face search output {}", faceSearchOutput);
                FaceType detectedFaceType;
                String title;
                if (!faceSearchOutput.getMatchedFaceList().isEmpty()) {
                    MatchedFace matchedFace = (MatchedFace)faceSearchOutput.getMatchedFaceList().get(0);
                    log.debug("Matched face {}", matchedFace);
                    String externalImageId = matchedFace.getFace().getExternalImageId();
                    log.debug("External image id {}", externalImageId);
                    if (externalImageId == null) {
                        title = matchedFace.getFace().getConfidence() + "";
                        log.debug("Title {}", title);
                        detectedFaceType = FaceType.NOT_RECOGNIZED;
                        log.debug("Detected face type {}", detectedFaceType);
                    } else {
                        String[] imageIds = externalImageId.split("-");
                        if (imageIds.length > 1) {
                            title = imageIds[0];
                            log.debug("Title {}", title);
                            detectedFaceType = FaceType.fromString(imageIds[1]);
                            log.debug("Detected face type {}", detectedFaceType);
                        } else {
                            title = "No prefix";
                            log.debug("Title {}", title);
                            detectedFaceType = FaceType.NOT_RECOGNIZED;
                            log.debug("Detected face type {}", detectedFaceType);
                        }
                    }

                    log.debug("Number of matched faces for the detected face {}", faceSearchOutput.getMatchedFaceList().size());
                } else {
                    detectedFaceType = FaceType.NOT_RECOGNIZED;
                    log.debug("Detected face type {}", detectedFaceType);
                    title = "Not recognized";
                    log.debug("Detected face type {}", detectedFaceType);
                }

                log.debug("DrawFaces g2 {} width {} height {} faceSearchOutput-getBoundingBox {} title {} detectedFaceType {}", new Object[]{g2, width, height, faceSearchOutput.getDetectedFace().getBoundingBox(), title, detectedFaceType.getColor()});
                this.drawFaces(g2, width, height, faceSearchOutput.getDetectedFace().getBoundingBox(), title, detectedFaceType.getColor());
            }
        }

    }

    private void drawFaces(Graphics2D g2, int width, int height, BoundingBox boundingBox, String personName, Color color) {
        log.debug(" drawFaces width {} height {} boudingBox {} personName {} color {}", new Object[]{width, height, boundingBox, personName, color});
        Color c = g2.getColor();
        g2.setColor(color);
        this.drawBoundingBox(g2, width, height, boundingBox);
        this.drawFaceTitle(g2, width, height, boundingBox, personName);
        g2.setColor(c);
    }

    private void drawFaceTitle(Graphics2D g2, int width, int height, BoundingBox boundingBox, String personName) {
        log.debug(" drawFaceTitle width {} height {} boudingBox {} personName {}", new Object[]{width, height, boundingBox, personName});
        int left = (int)(boundingBox.getLeft() * (double)width);
        int top = (int)(boundingBox.getTop() * (double)height);
        g2.drawString(personName, left, top);
    }

    private void drawBoundingBox(Graphics2D g2, int width, int height, BoundingBox boundingBox) {
        log.debug(" drawBoundingBox width {} height {} boudingBox {}", new Object[]{width, height, boundingBox});
        int left = (int)(boundingBox.getLeft() * (double)width);
        int top = (int)(boundingBox.getTop() * (double)height);
        int bbWidth = (int)(boundingBox.getWidth() * (double)width);
        int bbHeight = (int)(boundingBox.getHeight() * (double)height);
        log.debug(" drawBoundingBox left {} top {} bbWidth {} bbHeight", new Object[]{width, bbWidth, bbHeight});
        g2.drawRect(left, top, bbWidth, bbHeight);
    }

    public void setImage(BufferedImage bufferedImage, RekognizedOutput rekognizedOutput) {
        this.image = bufferedImage;
        log.debug("Image {}", bufferedImage);
        log.debug("RekognizedOutput {}", rekognizedOutput);
        this.processRekognitionOutput(this.image.createGraphics(), this.image.getWidth(), this.image.getHeight(), rekognizedOutput);
        this.repaint();
    }
}

