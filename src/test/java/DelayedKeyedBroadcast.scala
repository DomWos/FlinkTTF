import bbb.avro.dto.{CcyIsoDTO, RatesDTO}
import org.apache.flink.api.common.state.{MapState, MapStateDescriptor, ReadOnlyBroadcastState, StateTtlConfig}
import org.apache.flink.api.common.time.Time
import org.apache.flink.api.common.typeinfo.{TypeInformation, Types}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction
import org.apache.flink.util.Collector

import scala.collection.JavaConverters.iterableAsScalaIterableConverter

class DelayedKeyedBroadcast extends KeyedBroadcastProcessFunction[String, RatesDTO, CcyIsoDTO, RatesWithCcyName] {

  val ttlConfig = StateTtlConfig
    .newBuilder(Time.minutes(2))
    .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
    .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
    .build();
  val ccyDescriptor = new MapStateDescriptor("ccyIsoCodeBroadcastState", Types.STRING, Types.POJO(classOf[CcyIsoDTO]))
  val delayedMapDescriptor = new MapStateDescriptor("ccyIsoCodeBroadcastState", Types.STRING, TypeInformation.of(classOf[List[RatesDTO]]))
  delayedMapDescriptor.enableTimeToLive(ttlConfig)
  private var delayedMap: MapState[String, List[RatesDTO]] = _
  override def open(parameters: Configuration): Unit = {
    delayedMap = getRuntimeContext.getMapState(delayedMapDescriptor)
  }

  override def processElement(in1: RatesDTO, readOnlyContext: KeyedBroadcastProcessFunction[String, RatesDTO, CcyIsoDTO, RatesWithCcyName]#ReadOnlyContext,
                              collector: Collector[RatesWithCcyName]): Unit = {
    val ccyIsoDTO: ReadOnlyBroadcastState[String, CcyIsoDTO] = readOnlyContext.getBroadcastState(ccyDescriptor)
    if( ccyIsoDTO != null ){
      if( in1 != null ) {
        val ccyCode = in1.getRatesCcyIsoCode.toString
        val ccy = ccyIsoDTO.get(ccyCode)
        if( ccy != null ) {
          collector.collect({
            val ccr = new RatesWithCcyName()
            ccr.setRatesCcyIsoCode(in1.getRatesCcyIsoCode.toString)
            ccr.setRate(in1.getRate)
            ccr.setTs(in1.getTs)
            ccr.setCcyName(ccy.getCcyIsoName.toString)
            ccr
          })
        } else {
          readOnlyContext.timerService().registerProcessingTimeTimer(System.currentTimeMillis() + 10000)
        }
      }
    }
  }

  override def processBroadcastElement(in2: CcyIsoDTO,
                                       context: KeyedBroadcastProcessFunction[String, RatesDTO, CcyIsoDTO, RatesWithCcyName]#Context,
                                       collector: Collector[RatesWithCcyName]): Unit = {
    val bcState = context.getBroadcastState(ccyDescriptor)
    bcState.put(in2.getCcyIsoCode.toString, in2)
    println(s"processBroadcastElement $in2")
  }

  override def onTimer(timestamp: Long,
                       ctx: KeyedBroadcastProcessFunction[String, RatesDTO, CcyIsoDTO, RatesWithCcyName]#OnTimerContext,
                       out: Collector[RatesWithCcyName]): Unit = {
    val broadcast = ctx.getBroadcastState(ccyDescriptor)
   val delayedToEmit = delayedMap
      .entries()
      .asScala
      .filter(element => broadcast.contains(element.getKey))
      .map(element => (element.getKey,delayedJoin(broadcast.get(element.getKey), element.getValue)))

    delayedToEmit.foreach(element => {
      delayedMap.remove(element._1)
      element._2.foreach(rateJoined => out.collect(rateJoined))
    })
  }

  private def delayedJoin(ccy: CcyIsoDTO, rates: Seq[RatesDTO]) = {
    rates.map(element => new RatesWithCcyName(ccy.getCcyIsoCode.toString, element.getRate, element.getTs, ccy.getCcyIsoName.toString))
  }
}
