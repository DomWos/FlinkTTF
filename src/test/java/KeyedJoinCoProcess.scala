import java.util

import org.apache.avro.util.Utf8
import org.apache.flink.api.common.state.{MapState, MapStateDescriptor}
import org.apache.flink.api.common.typeinfo.{TypeHint, TypeInformation}
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.co.CoProcessFunction
import org.apache.flink.util.Collector

import scala.collection.JavaConverters.asScalaBufferConverter

class KeyedJoinCoProcess extends CoProcessFunction[Fares, RatesWithCcyName, AllJoined]{
  var ratesMapStateDesciptor = new MapStateDescriptor[String, RatesWithCcyName]("temporalTableImitation", Types.STRING, Types.POJO(classOf[RatesWithCcyName]))
  var ratesMapState: MapState[String, RatesWithCcyName] = _
  var faresMapStateDescriptor = new MapStateDescriptor[String, util.List[Fares]]("temporalTableImitation2v", Types.STRING, TypeInformation.of(new TypeHint[util.List[Fares]] {}))
  var faresMapState: MapState[String, util.List[Fares]] = _
  override def open(parameters: Configuration): Unit = {
   ratesMapState = getRuntimeContext.getMapState(ratesMapStateDesciptor)
    faresMapState = getRuntimeContext.getMapState(faresMapStateDescriptor)

  }

  override def processElement1(value: Fares,
                               ctx: CoProcessFunction[Fares, RatesWithCcyName, AllJoined]#Context,
                               out: Collector[AllJoined]): Unit = {
    val isoCode = value.getFareCcyIsoCode
    if(ratesMapState.contains(isoCode)) {
      val rate = ratesMapState.get(isoCode)
      val allJoined = new AllJoined(
        value.getTs,
        value.getPrice * rate.getRate,
        new Utf8(value.getFareCcyIsoCode),
        value.getTs,
        rate.getRate,
        new Utf8(rate.getRatesCcyIsoCode),
        new Utf8(rate.getCcyName),
        rate.getRate,
        rate.getTs)
      out.collect(allJoined)
    } else {
      val list: util.List[Fares] = if(faresMapState.contains(isoCode)) faresMapState.get(isoCode) else new util.ArrayList[Fares]()
      list.add(value)
      faresMapState.put(isoCode, list)
    }
  }

  override def processElement2(value: RatesWithCcyName,
                               ctx: CoProcessFunction[Fares, RatesWithCcyName, AllJoined]#Context,
                               out: Collector[AllJoined]): Unit = {
    val isoCode = value.getRatesCcyIsoCode
    val currentRate = ratesMapState.get(isoCode)
    val currentRateTst = if(currentRate == null) Long.MinValue else currentRate.getTs
    if(currentRateTst < value.getTs) {
      ratesMapState.put(isoCode, value)
//      if(faresMapState.contains(isoCode)){
//              faresMapState.get(isoCode).asScala.filter(_.getTs <= value.getTs).map(fare => new AllJoined(
//          fare.getTs, fare.getPrice * value.getRate, new Utf8(fare.getFareCcyIsoCode), fare.getTs,
//          value.getRate, new Utf8(value.getRatesCcyIsoCode), new Utf8(value.getCcyName), value.getRate, value.getTs
//        )).foreach(element => out.collect(element))
//      }
    }
  }
}
