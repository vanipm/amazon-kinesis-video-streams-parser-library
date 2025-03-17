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

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.kinesisvideo.parser.ebml.InputStreamParserByteSource;
import com.amazonaws.kinesisvideo.parser.mkv.MkvElementVisitException;
import com.amazonaws.kinesisvideo.parser.mkv.MkvElementVisitor;
import com.amazonaws.kinesisvideo.parser.mkv.StreamingMkvReader;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.kinesisvideo.AmazonKinesisVideo;
import com.amazonaws.services.kinesisvideo.AmazonKinesisVideoArchivedMedia;
import com.amazonaws.services.kinesisvideo.AmazonKinesisVideoArchivedMediaClient;
import com.amazonaws.services.kinesisvideo.AmazonKinesisVideoArchivedMediaClientBuilder;
import com.amazonaws.services.kinesisvideo.model.APIName;
import com.amazonaws.services.kinesisvideo.model.GetDataEndpointRequest;
import com.amazonaws.services.kinesisvideo.model.GetMediaForFragmentListRequest;
import com.amazonaws.services.kinesisvideo.model.GetMediaForFragmentListResult;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GetMediaForFragmentListWorker extends KinesisVideoCommon implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(GetMediaForFragmentListWorker.class);
    private final AmazonKinesisVideoArchivedMedia amazonKinesisVideoArchivedMedia;
    private final MkvElementVisitor elementVisitor;
    private final List<String> fragmentNumbers;

    public GetMediaForFragmentListWorker(String streamName, List<String> fragmentNumbers, AWSCredentialsProvider awsCredentialsProvider, String endPoint, Regions region, MkvElementVisitor elementVisitor) {
        super(region, awsCredentialsProvider, streamName);
        this.fragmentNumbers = fragmentNumbers;
        this.elementVisitor = elementVisitor;
        this.amazonKinesisVideoArchivedMedia = (AmazonKinesisVideoArchivedMedia)((AmazonKinesisVideoArchivedMediaClientBuilder)((AmazonKinesisVideoArchivedMediaClientBuilder)AmazonKinesisVideoArchivedMediaClient.builder().withCredentials(awsCredentialsProvider)).withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(endPoint, region.getName()))).build();
    }

    public static GetMediaForFragmentListWorker create(String streamName, List<String> fragmentNumbers, AWSCredentialsProvider awsCredentialsProvider, Regions region, AmazonKinesisVideo amazonKinesisVideo, MkvElementVisitor elementVisitor) {
        GetDataEndpointRequest request = (new GetDataEndpointRequest()).withAPIName(APIName.GET_MEDIA_FOR_FRAGMENT_LIST).withStreamName(streamName);
        String endpoint = amazonKinesisVideo.getDataEndpoint(request).getDataEndpoint();
        return new GetMediaForFragmentListWorker(streamName, fragmentNumbers, awsCredentialsProvider, endpoint, region, elementVisitor);
    }

    public void run() {
        try {
            log.info("Start GetMediaForFragmentList worker on stream {}", this.streamName);
            GetMediaForFragmentListResult result = this.amazonKinesisVideoArchivedMedia.getMediaForFragmentList((new GetMediaForFragmentListRequest()).withFragments(this.fragmentNumbers).withStreamName(this.streamName));
            log.info("GetMediaForFragmentList called on stream {} response {} requestId {}", new Object[]{this.streamName, result.getSdkHttpMetadata().getHttpStatusCode(), result.getSdkResponseMetadata().getRequestId()});
            StreamingMkvReader mkvStreamReader = StreamingMkvReader.createDefault(new InputStreamParserByteSource(result.getPayload()));
            log.info("StreamingMkvReader created for stream {} ", this.streamName);

            try {
                mkvStreamReader.apply(this.elementVisitor);
            } catch (MkvElementVisitException e) {
                log.error("Exception while accepting visitor {}", e);
            }
        } catch (Throwable t) {
            log.error("Failure in GetMediaForFragmentListWorker for streamName {} {}", this.streamName, t);
            throw t;
        } finally {
            log.info("Exiting GetMediaWorker for stream {}", this.streamName);
        }

    }
}

