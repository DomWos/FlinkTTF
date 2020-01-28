import java.util.concurrent.ConcurrentLinkedQueue

import org.apache.flink.streaming.api.functions.sink.SinkFunction

import scala.collection.mutable.ArrayBuffer

class TestSink extends SinkFunction[AllJoined]{
  @volatile
  var arrayBuffer = new ConcurrentLinkedQueue[AllJoined]()

  override def invoke(value: AllJoined): Unit = arrayBuffer.add(value)

  override def invoke(value: AllJoined, context: SinkFunction.Context[_]): Unit = arrayBuffer.add(value)
}
