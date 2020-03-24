package ai.humn

import ai.humn.ValueorWatermark
import bbb.avro.dto.RatesDTO
import org.apache.flink.api.common.state.{ListStateDescriptor, ValueStateDescriptor}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector

import scala.collection.JavaConverters._

class PerKeyWindowProcessFunction(windowSize: Long, windowTimeout: Long)  extends KeyedProcessFunction[String, ValueorWatermark, Seq[RatesDTO]] {
  private val windowDescriptor = new ListStateDescriptor[RatesDTO]("window",
    TypeInformation.of(classOf[RatesDTO]))
  lazy val windowElements = getRuntimeContext.getListState(windowDescriptor)
  private val firstElementDescriptor = new ValueStateDescriptor[RatesDTO]("firstElement", TypeInformation.of(classOf[RatesDTO]))
  lazy val firstWindowElement = getRuntimeContext.getState(firstElementDescriptor)
  private val scheduledTimeoutDescriptor = new ValueStateDescriptor[Long]("windowTimeout", TypeInformation.of(classOf[Long]))
  lazy val scheduledTimeout = getRuntimeContext.getState(scheduledTimeoutDescriptor)

  override def processElement(value: ValueorWatermark, ctx: KeyedProcessFunction[String, ValueorWatermark, Seq[RatesDTO]]#Context, out: Collector[Seq[RatesDTO]]): Unit = {
    value match {
      case Right(element) => {
        processRate(element, ctx.timerService().currentProcessingTime())
        if(windowTimeout > 0) {
          ctx.timerService().deleteProcessingTimeTimer(scheduledTimeout.value())
          scheduledTimeout.update(ctx.timerService().currentProcessingTime() + windowTimeout)
          ctx.timerService().registerProcessingTimeTimer(scheduledTimeout.value())
        }
      }
      case Left(watermark) => {
        val elements = processWatermark(watermark)
        if(!elements.isEmpty) {
          out.collect(elements.toSeq)
        }
      }
    }
  }

  private def processRate(element: RatesDTO, currentProcessingTime: Long) = {
    if(firstWindowElement.value() == null) {
      firstWindowElement.update(element)
    }
    windowElements.add(element)
  }

  private def processWatermark(watermarkTuple: (String, Long)) = {
      println(s"""Process Watermark ${watermarkTuple._2} for key ${watermarkTuple._1}""")
    val watermark = watermarkTuple._2
    if(firstWindowElement.value() != null && watermark - firstWindowElement.value().getTs >= windowSize) {
      val windowUpperBound = firstWindowElement.value().getTs + windowSize
      val windowLowerBound = firstWindowElement.value().getTs
      val elementsWithFlag = windowElements.get().asScala
        .map(element => (element,(element.getTs >= windowLowerBound) && element.getTs <= windowUpperBound))
      val elementsToEmit = elementsWithFlag.filter(_._2)
        .map(_._1)
      val elementsToLeave = elementsWithFlag.filter(!_._2)
        .filter(element => element._1.getTs > windowUpperBound)
        .map(_._1)

      firstWindowElement.update(elementsToLeave.minBy(_.getTs))
      windowElements.update(elementsToLeave.toList.asJava)
      elementsToEmit
    } else {
      Seq()
    }
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, ValueorWatermark, Seq[RatesDTO]]#OnTimerContext, out: Collector[Seq[RatesDTO]]): Unit = {
    if(firstWindowElement.value() != null) {
      val windowLowerBound = firstWindowElement.value().getTs
      val elementsToEmit = windowElements.get.asScala.filter(_.getTs >= windowLowerBound)
      out.collect(elementsToEmit.toSeq)
      windowElements.clear()
    }

    }
}
