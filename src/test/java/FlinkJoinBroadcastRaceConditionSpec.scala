import java.util.Properties

import bbb.avro.dto.{CcyIsoDTO, RatesDTO, TaxiFareDTO}
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.api.scala._
import org.apache.flink.formats.avro.AvroDeserializationSchema
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer
import org.apache.flink.table.api.TableConfig
import org.apache.flink.table.api.scala.{StreamTableEnvironment, _}
import org.apache.flink.types.Row
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.junit.runner.RunWith
import org.scalatestplus.junit.JUnitRunner

import scala.concurrent.ExecutionContext.Implicits._
import scala.concurrent.Future
import scala.util.{Failure, Success}

@RunWith(classOf[JUnitRunner])
class FlinkJoinBroadcastRaceConditionSpec extends TestUtils {

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
  env.enableCheckpointing(1000L)
  env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
  env.getCheckpointConfig.setMinPauseBetweenCheckpoints(500)
  env.getCheckpointConfig.setCheckpointTimeout(1000)
  env.getCheckpointConfig.setMaxConcurrentCheckpoints(1)
  env.getCheckpointConfig.enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)

  implicit val tEnv: StreamTableEnvironment = StreamTableEnvironment.create(env, new TableConfig())

  override def beforeAll: Unit = {
    super.beforeAll
    // prime Kafka with some rates and fares
    // publish rates
    publishRatesDTO(makeRatesDTO("GBP", rate = 2.0D, ts = 6100L))
    // publish taxi fares
    publishTaxiFareDTO(makeTaxiFareDTO("GBP", 15D, 7000L))
  }

  "Demo" should
    """
      Show use to Broadcast to enrich rates stream prior to joining with TaxiFares
    """.stripMargin in {

    // Make a collection of CCYs as if read from a database.
    // At 100000 USDs the test fails
    val e: List[CcyIsoDTO] =
        List.fill(10)(makeCcyIsoDTO("USD", "Yankee_Dollar", ts= Long.MaxValue))
    val elements  = e ++ List(makeCcyIsoDTO("GBP", "POUND_STERLING", ts= Long.MaxValue))

    // Stream the CCYs
    val dbStreamCcys = env.fromElements( elements : _* )

    // Subscribe to CCY updates from Kinesis
    val ccyIsoStreamKafka = makeFlinkConsumer[CcyIsoDTO](AvroDeserializationSchema.forSpecific[CcyIsoDTO](classOf[CcyIsoDTO]),
      kafkaProperties, 0L, _.getTs, ccyIsoTopicName)

    val ccyIsoStream = ccyIsoStreamKafka.union(dbStreamCcys)

    val broadcastCcys = ccyIsoStream.broadcast(CcyIsoBroadcastRaceConditionAverseKeyedFunction.ccyDescriptor)

    Thread.sleep(30000L)

    val ratesStream =
      makeFlinkConsumer[RatesDTO](AvroDeserializationSchema.forSpecific[RatesDTO](classOf[RatesDTO]),
        kafkaProperties,0L, _.getTs, ratesTopicName)
    ratesStream.map{r=>println(r.toString)}

    val ratesPartitionedStream: KeyedStream[RatesDTO, String] =
      ratesStream.keyBy( new KeySelector[RatesDTO, String](){
          override def getKey(in: RatesDTO): String = in.getRatesCcyIsoCode.toString
      })

    // connect broadcast
    val ccyMatches: DataStream[RatesWithCcyName] = ratesPartitionedStream
      .connect(broadcastCcys)
      .process( new CcyIsoBroadcastRaceConditionAverseKeyedFunction )

    ccyMatches.map{ r=> println(s"ccyMatches $r")}

    val ratesCcyMatchTable = ccyMatches.toTable(tEnv, 'ratesCcyIsoCode, 'rate, 'ts.as('rates_ts), 'ccyName, 'ts.rowtime.as('rates_rowtime) )
    tEnv.registerTable("RatesCcyMatchTable", ratesCcyMatchTable)
    val ratesTTF = ratesCcyMatchTable.createTemporalTableFunction('rates_rowtime, 'ratesCcyIsoCode)
    tEnv.registerFunction("RatesTTF", ratesTTF)

    tEnv.toAppendStream[Row](tEnv.sqlQuery("SELECT * FROM RatesCcyMatchTable")).map(r=>println(s"RatesCcyMatchTable $r"))

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
        | SELECT fares_ts, price * rate AS conv_fare, fareCcyIsoCode, ccyName
        | FROM FaresTable,
        | LATERAL TABLE( RatesTTF(fares_rowtime) )
        | WHERE fareCcyIsoCode = ratesCcyIsoCode
        |""".stripMargin)

    tEnv.toRetractStream[Row](fareRatesJoin).map(r=>{println(s"fareRatesJoin : $r")})

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

    publishCcyIsoDTO( makeCcyIsoDTO("PLN", "Solidarność_Złoty", ts = Long.MaxValue))
    publishRatesDTO(makeRatesDTO("GBP", rate=3.0D, ts=170000L))
    publishTaxiFareDTO(makeTaxiFareDTO("GBP", 50D, 170000L))

    Thread.sleep(5000L)

    val messages = getMessagesFromKafka( 5, 10 )

    messages.foreach{
      result => println(s"RESULT in kafka : $result")
    }
    messages.size shouldBe 1
    val r1 = messages.toArray
    r1(0) shouldBe "7000,30.0,GBP,POUND_STERLING"

    publishRatesDTO(makeRatesDTO("GBP", rate=4.0D, ts=2170000L))
    publishTaxiFareDTO(makeTaxiFareDTO("GBP", 100D, 2170000L))

    Thread.sleep(5000L)

    val messages2 = getMessagesFromKafka( 5, 10 )
    messages2.size shouldBe 1
    messages2.head shouldBe "170000,150.0,GBP,POUND_STERLING"

  }
}
