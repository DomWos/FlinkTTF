import java.util.Properties

import bbb.avro.dto.{CcyIsoDTO, RatesDTO, TaxiFareDTO}
import org.apache.flink.api.common.state.{MapStateDescriptor, ReadOnlyBroadcastState}
import org.apache.flink.api.common.typeinfo.Types
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.api.scala._
import org.apache.flink.formats.avro.AvroDeserializationSchema
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer
import org.apache.flink.table.api.TableConfig
import org.apache.flink.table.api.scala.{StreamTableEnvironment, _}
import org.apache.flink.types.Row
import org.apache.flink.util.Collector
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.junit.runner.RunWith
import org.scalatestplus.junit.JUnitRunner

import scala.concurrent.ExecutionContext.Implicits._
import scala.concurrent.Future
import scala.util.{Failure, Success}

@RunWith(classOf[JUnitRunner])
class FlinkJoinWithBroadcastRecoverySpec extends TestUtils {

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
      Show use of Broadcast to enrich rates stream prior to joining with TaxiFares
    """.stripMargin in {

    // Register a currency
    val gbpCcyIsoDTO = makeCcyIsoDTO("GBP", "POUND_STERLING", ts= 1L)
    publishCcyIsoDTO(gbpCcyIsoDTO)(kafkaConfig)

    // publish a rates
    val gbp1 = makeRatesDTO("GBP", rate=2.0D, ts=6100L)
    publishRatesDTO(gbp1)(kafkaConfig)

    // publish taxi fares
    val taxi1 = makeTaxiFareDTO("GBP", 15D, 7000L)
    publishTaxiFareDTO(taxi1)(kafkaConfig)



    val ccyIsoStream = makeFlinkConsumer[CcyIsoDTO](AvroDeserializationSchema.forSpecific[CcyIsoDTO](classOf[CcyIsoDTO]),
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
    //val ratesTable = tEnv.fromDataStream(ratesStream, 'ratesCcyIsoCode, 'rate, 'ts.as('rates_ts), 'ts.rowtime.as('rates_rowtime) )


    // connect broadcast
    val ccyMatches: DataStream[RatesWithCcyName] = ratesPartitionedStream
      .connect(broadcastCcys)
      .process( new KeyedBroadcastProcessFunction[String, RatesDTO, CcyIsoDTO, RatesWithCcyName]() {
        val ccyDescriptor = new MapStateDescriptor("ccyIsoCodeBroadcastState", Types.STRING, Types.POJO(classOf[CcyIsoDTO]))
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
      })

    ccyMatches.map{
      r=> println(s"ccyMatches $r")
    }

    val ratesCcyMatchTable = ccyMatches.toTable(tEnv, 'ratesCcyIsoCode, 'rate, 'ts.as('rates_ts), 'ccyName, 'ts.proctime.as('rates_rowtime) )
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
    val faresTable = tEnv.fromDataStream(asPojo, 'fareCcyIsoCode, 'price, 'ts.as('fares_ts), 'ts.proctime.as('fares_rowtime) )
    tEnv.registerTable("FaresTable", faresTable)

    val fareRatesJoin = tEnv.sqlQuery(
      """
        | SELECT fares_ts, price * rate AS conv_fare, fareCcyIsoCode
        | FROM FaresTable,
        | LATERAL TABLE( RatesTTF(fares_rowtime) )
        | WHERE fareCcyIsoCode = ratesCcyIsoCode
        |""".stripMargin)

    tEnv.toRetractStream[Row](fareRatesJoin)
      .map(
        r=>{
          println(s"fareRatesJoin : $r")
        }
      )

    val resultPublisher: FlinkKafkaProducer[(Boolean, Row)] = new FlinkKafkaProducer[(Boolean, Row)](resultsTopicName, new StringResultSeralizer(), kafkaProperties)
    val outStream = tEnv.toRetractStream[Row]( fareRatesJoin )
    outStream.addSink(resultPublisher)

    Future {
      env.execute()
    }.onComplete {
      case Success(value) =>
      case Failure(exception) => {
        println(exception.getMessage)
        exception.printStackTrace()
        fail()
      }
    }

    // Give flink a chance to connect everything up.
    Thread.sleep(10000L)

    val messages = getMessagesFromKafka( 5, 10 )

    messages.foreach{
      result => println(s"RESULT in kafka : $result")
    }
    messages.size shouldBe 1
    messages.head shouldBe "7000,30.0,GBP"

    // Now publish another taxi fare
    val taxi3 = makeTaxiFareDTO("GBP", 100D, 170000L)
    publishTaxiFareDTO(taxi3)(kafkaConfig)

    val messages2 = getMessagesFromKafka( 5 )

    messages2.foreach{
      result => println(s"RESULT in kafka : $result")
    }

    messages2.size shouldBe 1
    messages2.head shouldBe "170000,200.0,GBP"


    // Now move time along in taxi fare stream
    val gbp2 = makeRatesDTO("GBP", rate=10.0D, ts=25000000L)
    publishRatesDTO(gbp2)(kafkaConfig)

    val taxi8 = makeTaxiFareDTO("GBP", 100D, 25000000L)
    publishTaxiFareDTO(taxi8)(kafkaConfig)

    val messages3 = getMessagesFromKafka( 15 )

    messages3.foreach{
      result => println(s"RESULT in kafka : $result")
    }

    messages3.size shouldBe 1
    messages3.head shouldBe "25000000,1000.0,GBP"


    // Lets change the GBP rate again
    val gbp3 = makeRatesDTO("GBP", 12.34D, ts=50000000L)
    publishRatesDTO(gbp3)(kafkaConfig)

    val taxi5 = makeTaxiFareDTO("GBP", 10D, 51000000L)
    publishTaxiFareDTO(taxi5)(kafkaConfig)
    // and in rates stream

    val taxi6 = makeTaxiFareDTO("GBP", 20D, 52000000L)
    publishTaxiFareDTO(taxi6)(kafkaConfig)
    val taxi7 = makeTaxiFareDTO("GBP", 30D, 53000000L)
    publishTaxiFareDTO(taxi7)(kafkaConfig)

    val messages4 = getMessagesFromKafka( 15 )

    messages4.foreach{
      result => println(s"RESULT in kafka : $result")
    }

    messages4.size shouldBe 3
    val results = messages4.toArray
    results(0) shouldBe "51000000,123.4,GBP"
    results(1) shouldBe "52000000,246.8,GBP"
    results(2) shouldBe "53000000,370.2,GBP"


    // What happens if we publish a very old taxi fare?
    val taxiLate = makeTaxiFareDTO("USD", 10D, 1000L)
    publishTaxiFareDTO(taxiLate)(kafkaConfig)

    val messages5 = getMessagesFromKafka( 15 )

    messages5.foreach{
      result => println(s"RESULT in kafka : $result")
    }

    messages5.size shouldBe 3
    val results5 = messages4.toArray
    results5(0) shouldBe "51000000,123.4,GBP"
    results5(1) shouldBe "52000000,246.8,GBP"
    results5(2) shouldBe "53000000,370.2,GBP"

  }
}
