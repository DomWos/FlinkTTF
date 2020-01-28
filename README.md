# FlinkTTF
Experiments with Flink Temporal Table Functions

#Results

###Flink Temporal Table Join
* The data can be received in any order on start, also it should be able to create proper results from unordered data.
* TIMESTAMPING IS DISABLED for low frequency stream, this basically means that the if the low frequency stream can be filtered out by the moving watermark.
This is shown in `DoubleTTFJoinSpecWatermarkIssueFail` class. This basically means that if we will have data from the low frequency stream arrive with timestamp lower 
than the current Watermark it will be ignored.

###Flink Delayed Broadcast
* Elements should also be allowed in any order, but this requires some tweaking
* Even though Timestamp is disabled for one stream, due to using the broadcast it seems that there is no watermark issue as described above.