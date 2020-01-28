import org.apache.flink.api.common.functions.{MapFunction, RichMapFunction}
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.util.Collector

class MapVerifier extends ProcessFunction[AllJoined, AllJoined] {
  override def processElement(value: AllJoined, ctx: ProcessFunction[AllJoined, AllJoined]#Context, out: Collector[AllJoined]): Unit = {
    println("Current timestamp in Process: " + ctx.timerService().currentWatermark())
    out.collect(value)
  }
}
