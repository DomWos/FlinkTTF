package ai.humn

import ai.humn.ValueorWatermark
import bbb.avro.dto.RatesDTO
import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector

class PerKeyWatermarkAssigner(autoWatermarkInterval: Long, outOfOrderness: Long) extends KeyedProcessFunction[String ,RatesDTO, ValueorWatermark] {
  private val watermarksDescriptor =  new ValueStateDescriptor[Long]("customWatermark", TypeInformation.of(classOf[Long]))
  private lazy val maxWatermark = getRuntimeContext.getState(watermarksDescriptor)

  private val maxTimestampsDescriptor = new ValueStateDescriptor[Long]("maxTimestamp", TypeInformation.of(classOf[Long]))
  private lazy val maxTimestamp = getRuntimeContext.getState(maxTimestampsDescriptor)


  override def open(parameters: Configuration): Unit = super.open(parameters)

  override def processElement(value: RatesDTO, ctx: KeyedProcessFunction[String, RatesDTO, ValueorWatermark]#Context, out: Collector[ValueorWatermark]): Unit = {

    val maxTimestampForKey = maxTimestamp.value()
    if(maxTimestampForKey < value.getTs) {
      maxTimestamp.update(value.getTs   )
    }
    out.collect(Right(value))
    if(maxTimestampForKey == 0) {
      ctx.timerService().registerProcessingTimeTimer(ctx.timerService().currentProcessingTime() + autoWatermarkInterval)
    }
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, RatesDTO, ValueorWatermark]#OnTimerContext, out: Collector[ValueorWatermark]): Unit = {
     if(maxWatermark.value() < maxTimestamp.value()) {
       maxWatermark.update(maxTimestamp.value())
     }
    out.collect(Left((ctx.getCurrentKey, maxWatermark.value())))
    ctx.timerService().registerProcessingTimeTimer(ctx.timerService().currentProcessingTime() + autoWatermarkInterval)
  }
}
