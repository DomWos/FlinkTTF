package ai.humn

import java.util.concurrent.ConcurrentLinkedQueue

import bbb.avro.dto.RatesDTO
import org.apache.flink.streaming.api.functions.sink.SinkFunction

class TestSink extends SinkFunction[Seq[RatesDTO]] {
  override def invoke(value: scala.Seq[_root_.bbb.avro.dto.RatesDTO]): Unit = TestSink.values.add(value)
}

object TestSink {
  val values = new ConcurrentLinkedQueue[Seq[RatesDTO]]()
}