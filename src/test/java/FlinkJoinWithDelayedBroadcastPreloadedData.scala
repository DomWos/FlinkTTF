import java.util
import java.util.Properties

import bbb.avro.dto.{CcyIsoDTO, RatesDTO, TaxiFareDTO}
import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.apache.avro.util.Utf8
import org.apache.flink.api.common.state.MapStateDescriptor
import org.apache.flink.api.common.typeinfo.Types
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.api.scala._
import org.apache.flink.formats.avro.AvroDeserializationSchema
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, StreamExecutionEnvironment}
import org.apache.flink.table.api.TableConfig
import org.apache.flink.table.api.scala.{StreamTableEnvironment, _}
import org.apache.flink.types.Row
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.Deserializer
import org.codehaus.jackson.map.ObjectMapper
import org.junit.runner.RunWith
import org.scalatest.concurrent.Eventually
import org.scalatest.time.{Seconds, Span}
import org.scalatestplus.junit.JUnitRunner

import scala.concurrent.ExecutionContext.Implicits._
import scala.concurrent.Future
import scala.util.{Failure, Success}

@RunWith(classOf[JUnitRunner])
class FlinkJoinWithDelayedBroadcastPreloadedData extends TestUtils with Eventually {

  val kafkaProperties: Properties = new Properties()
  kafkaProperties.setProperty("bootstrap.servers", kafkaConfig.bootstrapServers)
  kafkaProperties.setProperty("group.id", "FlinkJoinProblemSpec")
  kafkaProperties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

  private implicit val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
  env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
  env.getConfig.setAutoWatermarkInterval(5000L)
  env.setParallelism(1)
  env.setMaxParallelism(1)
  env.setBufferTimeout(1000L)
  implicit val tEnv: StreamTableEnvironment = StreamTableEnvironment.create(env, new TableConfig())

  "Demo" should
    """
      Show use to Broadcast to enrich rates stream prior to joining with TaxiFares
    """.stripMargin in {

    val ccyIsoStream = makeIdlingFlinkConsumer[CcyIsoDTO](AvroDeserializationSchema.forSpecific[CcyIsoDTO](classOf[CcyIsoDTO]),
      kafkaProperties, 0L, _.getTs, ccyIsoTopicName)
    ccyIsoStream.map{
      r=>println(r.toString)
    }
    val rulDescriptor = new MapStateDescriptor("ccyIsoCodeBroadcastState", Types.VOID, Types.POJO[CcyIsoDTO](classOf[CcyIsoDTO]))
    val broadcastCcys = ccyIsoStream.broadcast(rulDescriptor)


    val ratesStream =
      makeFlinkConsumer[RatesDTO](AvroDeserializationSchema.forSpecific[RatesDTO](classOf[RatesDTO]),
        kafkaProperties,0L, _.getTs, ratesTopicName)
    ratesStream.map{
      r=>println(r.toString)
    }
    val ratesPartitionedStream: KeyedStream[RatesDTO, String] = ratesStream.keyBy( new KeySelector[RatesDTO, String](){
      override def getKey(in: RatesDTO): String = in.getRatesCcyIsoCode.toString
    })

    // connect broadcast
    val ccyMatches: DataStream[RatesWithCcyName] = ratesPartitionedStream
      .connect(broadcastCcys)
      .process( new DelayedKeyedBroadcast())
        .assignAscendingTimestamps(_.getTs)
    ccyMatches.map{
      r=> println(s"ccyMatches $r")
        r
    }

    val ratesCcyMatchTable = ccyMatches.toTable(tEnv, 'ratesCcyIsoCode, 'rate, 'ts.as('rates_ts), 'ccyName, 'ts.rowtime.as('rates_rowtime))
    tEnv.registerTable("RatesCcyMatchTable", ratesCcyMatchTable)
    val ratesTTF = ratesCcyMatchTable.createTemporalTableFunction('rates_rowtime, 'ratesCcyIsoCode)
    tEnv.registerFunction("RatesTTF", ratesTTF)

    tEnv.toAppendStream[Row](
      tEnv.sqlQuery("SELECT * FROM RatesCcyMatchTable"))
      .map(
        r=>println(s"RatesCcyMatchTable $r")
      )

    val faresStream  = makeFlinkConsumer[TaxiFareDTO](AvroDeserializationSchema.forSpecific[TaxiFareDTO](classOf[TaxiFareDTO]), kafkaProperties,0L, _.getTs, taxiFareTopicName)
    val asPojo = faresStream.map{
      r=> println(r.toString)
        val f = new Fares()
        f.setFareCcyIsoCode(r.getFareCcyIsoCode.toString)
        f.setPrice(r.getPrice)
        f.setTs(r.getTs)
        f
    }
    val faresTable = tEnv.fromDataStream(asPojo, 'fareCcyIsoCode, 'price, 'ts.as('fares_ts), 'ts.rowtime.as('fares_rowtime) )
    tEnv.registerTable("FaresTable", faresTable)

    val fareRatesJoin = tEnv.sqlQuery(
      """
        | SELECT fares_ts AS faresTst , price * rate AS convFare, fareCcyIsoCode, rates_ts AS fareRatesTs, rate AS realRate,
        | ccyName AS ccyIsoName, ratesCcyIsoCode AS ccyIsoCode, rate, rates_ts AS ccyRatesTs
        | FROM FaresTable,
        | LATERAL TABLE( RatesTTF(fares_rowtime) )
        | WHERE fareCcyIsoCode = ratesCcyIsoCode
        |""".stripMargin)

    tEnv.toAppendStream[AllJoinedString](fareRatesJoin)
      .map(
        r=>{
          println(s"fareRatesJoin : $r")
          r
        }
      ).addSink(makeProducer(OutputTopic, kafkaProperties, new AllJoinedStringSerializationSchema))
    val usd1 = makeRatesDTO("USD", rate=1.1D, ts=3000L)
    publishRatesDTO(usd1)(kafkaConfig)
    val usd2 = makeRatesDTO("USD", rate=1.7D, ts=6500L)
    publishRatesDTO(usd2)(kafkaConfig)
    val usd3 = makeRatesDTO("USD", rate=1.7D, ts=8500L)
    publishRatesDTO(usd3)(kafkaConfig)
    val usd4 = makeRatesDTO("USD", rate=1.7D, ts=20000L)
    publishRatesDTO(usd4)(kafkaConfig)
    Thread.sleep(3000L)
    val taxi2 = makeTaxiFareDTO("USD", 15D, 4000L)
    publishTaxiFareDTO(taxi2)(kafkaConfig)
    val taxi3 = makeTaxiFareDTO("USD", 25D, 5000L)
    publishTaxiFareDTO(taxi3)(kafkaConfig)
    val taxi4 = makeTaxiFareDTO("USD", 10D, 6000L)
    publishTaxiFareDTO(taxi4)(kafkaConfig)
    val taxi5 = makeTaxiFareDTO("USD", 17D, 7000L)
    publishTaxiFareDTO(taxi5)(kafkaConfig)
    val taxi6 = makeTaxiFareDTO("USD", 18D, 8000L)
    publishTaxiFareDTO(taxi6)(kafkaConfig)
    val taxi7 = makeTaxiFareDTO("USD", 23D, 9000L)
    publishTaxiFareDTO(taxi7)(kafkaConfig)
    val taxi8 = makeTaxiFareDTO("USD", 23D, 18000L)
    publishTaxiFareDTO(taxi8)(kafkaConfig)
    Thread.sleep(6000L) // This will generate timestamp, but

    val usdCcyIsoDTO = makeCcyIsoDTO("USD", "US_DOLLARS", ts= 1L)
    publishCcyIsoDTO(usdCcyIsoDTO)(kafkaConfig)
    Future {
      env.execute()
    }.onComplete {
      case Success(value) =>
      case Failure(exception) =>
        println(exception.getMessage)
        exception.printStackTrace()
        fail()
    }
    Thread.sleep(10000)


    Thread.sleep(7000L) //Watermark will be emitted here but it will be min from all streams(18000L, Long.MAX, Long.MIN)
    val nonJoinerTimeMoverTaxi2 = makeTaxiFareDTO("USD", 23D, 25000L)
    publishTaxiFareDTO(nonJoinerTimeMoverTaxi2)(kafkaConfig)
    val usd6 = makeRatesDTO("USD", rate=1.7D, ts=24000L)
    publishRatesDTO(usd6)(kafkaConfig)
    Thread.sleep(5000L)
   //Watermark will be emitted hermark will be emitted here but it will be min from all streams(18000L, Long.MAX, Long.MIN)
   val expectedResults = Seq(
     new AllJoined(taxi2.getTs, taxi2.getPrice * usd1.getRate, new Utf8(usd1.getRatesCcyIsoCode.toString), usd1.getTs, usd1.getRate, new Utf8(usd1.getRatesCcyIsoCode.toString), new Utf8(usdCcyIsoDTO.getCcyIsoName.toString), usd1.getRate, usd1.getTs),
     new AllJoined(taxi3.getTs, taxi3.getPrice * usd1.getRate, new Utf8(usd1.getRatesCcyIsoCode.toString), usd1.getTs, usd1.getRate, new Utf8(usd1.getRatesCcyIsoCode.toString), new Utf8(usdCcyIsoDTO.getCcyIsoName.toString), usd1.getRate, usd1.getTs),
     new AllJoined(taxi4.getTs, taxi4.getPrice * usd1.getRate, new Utf8(usd1.getRatesCcyIsoCode.toString), usd1.getTs, usd1.getRate, new Utf8(usd1.getRatesCcyIsoCode.toString), new Utf8(usdCcyIsoDTO.getCcyIsoName.toString), usd1.getRate, usd1.getTs),
     new AllJoined(taxi5.getTs, taxi5.getPrice * usd2.getRate, new Utf8(usd2.getRatesCcyIsoCode.toString), usd2.getTs, usd2.getRate, new Utf8(usd2.getRatesCcyIsoCode.toString), new Utf8(usdCcyIsoDTO.getCcyIsoName.toString), usd2.getRate, usd2.getTs),
     new AllJoined(taxi6.getTs, taxi6.getPrice * usd2.getRate, new Utf8(usd2.getRatesCcyIsoCode.toString), usd2.getTs, usd2.getRate, new Utf8(usd2.getRatesCcyIsoCode.toString), new Utf8(usdCcyIsoDTO.getCcyIsoName.toString), usd2.getRate, usd2.getTs),
     new AllJoined(taxi7.getTs, taxi7.getPrice * usd3.getRate, new Utf8(usd3.getRatesCcyIsoCode.toString), usd3.getTs, usd3.getRate, new Utf8(usd3.getRatesCcyIsoCode.toString), new Utf8(usdCcyIsoDTO.getCcyIsoName.toString), usd3.getRate, usd3.getTs),
     new AllJoined(taxi8.getTs, taxi8.getPrice * usd3.getRate, new Utf8(usd3.getRatesCcyIsoCode.toString), usd3.getTs, usd3.getRate, new Utf8(usd3.getRatesCcyIsoCode.toString), new Utf8(usdCcyIsoDTO.getCcyIsoName.toString), usd3.getRate, usd3.getTs)
   )
    Eventually.eventually(timeout(Span(35, Seconds)),interval(Span(3 ,Seconds))) {
      val messages = EmbeddedKafka.consumeNumberMessagesFrom(OutputTopic, 7)(EmbeddedKafkaConfig.defaultConfig, new Deserializer[AllJoined] {
        val objectMapper = new ObjectMapper()
        override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = {}

        override def deserialize(topic: String, data: Array[Byte]): AllJoined = objectMapper.readValue(data, classOf[AllJoined])

        override def close(): Unit = {}
      } )
      messages.toSet should equal(expectedResults.toSet)
    }
  }
}
