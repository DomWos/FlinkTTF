import java.util
import java.util.Properties

import bbb.avro.dto.{CcyIsoDTO, RatesDTO, TaxiFareDTO}
import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.apache.avro.util.Utf8
import org.apache.flink.api.scala._
import org.apache.flink.formats.avro.AvroDeserializationSchema
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.TableConfig
import org.apache.flink.table.api.scala.{StreamTableEnvironment, _}
import org.apache.flink.types.Row
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.Deserializer
import org.codehaus.jackson.map.ObjectMapper
import org.junit.runner.RunWith
import org.scalatest.GivenWhenThen
import org.scalatest.concurrent.Eventually
import org.scalatest.time.{Seconds, Span}
import org.scalatestplus.junit.JUnitRunner

import scala.concurrent.ExecutionContext.Implicits._
import scala.concurrent.Future
import scala.util.{Failure, Success}

@RunWith(classOf[JUnitRunner])
class PostSQJoinTTFTest extends TestUtils with GivenWhenThen with Eventually {

  val kafkaProperties: Properties = new Properties()
  kafkaProperties.setProperty("bootstrap.servers", kafkaConfig.bootstrapServers)
  kafkaProperties.setProperty("group.id", "FlinkJoinProblemSpec")
  kafkaProperties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

  "Demo" should
    """
     show that for preloaded data it will produce proper results
    """.stripMargin in {
    implicit val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
        //Uncomment line below to make everything work
    env.setParallelism(1)
    env.getConfig.setAutoWatermarkInterval(5000L)
    implicit val tEnv: StreamTableEnvironment = StreamTableEnvironment.create(env, new TableConfig())


    Given("Running flink environment with 2 TTF Joins")
    val ccyIsoStream = makeIdlingFlinkConsumer[CcyIsoDTO](AvroDeserializationSchema.forSpecific[CcyIsoDTO](classOf[CcyIsoDTO]),
      kafkaProperties, 0L, _.getTs, ccyIsoTopicName)

    val ccyIsoTable = tEnv.fromDataStream(ccyIsoStream, 'ccyIsoCode, 'ccyIsoName, 'ts.rowtime.as('ccy_rowtime))
    tEnv.registerTable("CcyIsoTable", ccyIsoTable)
    val ccyTable = ccyIsoTable.createTemporalTableFunction('ccy_rowtime, 'ccyIsoCode)
    tEnv.registerFunction("ccyTable", ccyTable)

    val ratesStream = makeFlinkConsumer[RatesDTO](AvroDeserializationSchema.forSpecific[RatesDTO](classOf[RatesDTO]), kafkaProperties, 0L, _.getTs, ratesTopicName)
    val ratesTable = tEnv.fromDataStream(ratesStream, 'ratesCcyIsoCode, 'rate, 'ts.as('rates_ts), 'ts.rowtime.as('rates_rowtime))
    val ratesTTF = ratesTable.createTemporalTableFunction('rates_rowtime, 'ratesCcyIsoCode)
    tEnv.registerFunction("RatesTTF", ratesTTF)
    tEnv.registerTable("RatesTable", ratesTable)

    val faresStream = makeFlinkConsumer[TaxiFareDTO](AvroDeserializationSchema.forSpecific[TaxiFareDTO](classOf[TaxiFareDTO]), kafkaProperties, 0L, _.getTs, taxiFareTopicName)

    val faresTable = tEnv.fromDataStream(faresStream, 'fareCcyIsoCode, 'price, 'ts.as('faresTst), 'ts.rowtime.as('faresRowTime))
    tEnv.registerTable("FaresTable", faresTable)

    tEnv.registerFunction("faresTTF", faresTable.createTemporalTableFunction('faresRowTime, 'fareCcyIsoCode))


    val ratesCcyIsoJoin = tEnv.sqlQuery(
      """
        |  SELECT ratesCcyIsoCode, ccyIsoName as ccyName, rate, rates_ts as ts
        |  FROM RatesTable, CcyIsoTable
        |  WHERE ccyIsoCode = ratesCcyIsoCode
        |""".stripMargin)

    val stream = ratesCcyIsoJoin.toAppendStream[RatesWithCcyNameUtf]
        .assignAscendingTimestamps(_.getTs)

    stream.addSink(data => println(s"Output from SQL: ${data}"))
    tEnv.registerDataStream("ccyRatesJoin", stream, 'ratesCcyIsoCode, 'ccyName, 'ts, 'ts.rowtime.as('rates_rowtime), 'rate)
    val fareRatesJoin = tEnv.sqlQuery(
      """
        | SELECT faresTst, price * rate AS convFare, fareCcyIsoCode, ts as fareRatesTs, rate as realRate
        | FROM ccyRatesJoin,
        | LATERAL TABLE( faresTTF(rates_rowtime) )
        | WHERE fareCcyIsoCode = ratesCcyIsoCode
        |""".stripMargin)

    tEnv.toAppendStream[Row](fareRatesJoin)
        .addSink(data => System.err.println(s"""Output from Temporal Table Join: ${data}"""))


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
    Thread.sleep(5000)
    val usdCcyIsoDTO = makeCcyIsoDTO("USD", "US_DOLLARS", ts= 1L)
    publishCcyIsoDTO(usdCcyIsoDTO)(kafkaConfig)
    Thread.sleep(8000)
    val taxi2 = makeTaxiFareDTO("USD", 15D, 4000L)
    publishTaxiFareDTO(taxi2)(kafkaConfig)
    Thread.sleep(2000L)
    val taxi3 = makeTaxiFareDTO("USD", 25D, 5000L)
    publishTaxiFareDTO(taxi3)(kafkaConfig)
    Thread.sleep(2000L)

    val taxi4 = makeTaxiFareDTO("USD", 10D, 6000L)
    publishTaxiFareDTO(taxi4)(kafkaConfig)
    Thread.sleep(2000L)

    val taxi5 = makeTaxiFareDTO("USD", 17D, 7000L)
    publishTaxiFareDTO(taxi5)(kafkaConfig)
    Thread.sleep(2000L)

    val taxi6 = makeTaxiFareDTO("USD", 18D, 8000L)
    publishTaxiFareDTO(taxi6)(kafkaConfig)
    Thread.sleep(2000L)

    val taxi7 = makeTaxiFareDTO("USD", 23D, 9000L)
    publishTaxiFareDTO(taxi7)(kafkaConfig)
    Thread.sleep(2000L)

    val taxi8 = makeTaxiFareDTO("USD", 23D, 18000L)
    publishTaxiFareDTO(taxi8)(kafkaConfig)
    Thread.sleep(3000L)

    val usd1 = makeRatesDTO("USD", rate=1.1D, ts=3000L)
    publishRatesDTO(usd1)(kafkaConfig)
    Thread.sleep(2000L)

    val usd2 = makeRatesDTO("USD", rate=1.7D, ts=6500L)
    publishRatesDTO(usd2)(kafkaConfig)
    Thread.sleep(2000L)

    val usd3 = makeRatesDTO("USD", rate=1.7D, ts=8500L)
    publishRatesDTO(usd3)(kafkaConfig)
    Thread.sleep(2000L)

    val usd4 = makeRatesDTO("USD", rate=1.7D, ts=20000L)
    publishRatesDTO(usd4)(kafkaConfig)
    Thread.sleep(3000L)
    Thread.sleep(20000L)
    val nonJoinerTimeMoverTaxi2 = makeTaxiFareDTO("USD", 23D, 25000L)
    publishTaxiFareDTO(nonJoinerTimeMoverTaxi2)(kafkaConfig)
    Thread.sleep(10000L)

    Eventually.eventually(timeout(Span(35, Seconds)),interval(Span(3 ,Seconds))) {
     Thread.sleep(5000)
      throw new Exception("Just to keep spinning.")
    }
  }
}
