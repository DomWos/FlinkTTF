import java.util
import java.util.Properties

import bbb.avro.dto.{CcyIsoDTO, RatesDTO, TaxiFareDTO}
import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.apache.avro.util.Utf8
import org.apache.flink.api.common.typeinfo.SqlTimeTypeInfo
import org.apache.flink.api.java.typeutils.{GenericTypeInfo, RowTypeInfo}
import org.apache.flink.api.scala._
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.formats.avro.AvroDeserializationSchema
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaProducer}
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema
import org.apache.flink.table.api.TableConfig
import org.apache.flink.table.api.scala.{StreamTableEnvironment, table2TableConversions, _}
import org.apache.flink.types.Row
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.Deserializer
import org.codehaus.jackson.map.ObjectMapper
import org.junit.runner.RunWith
import org.scalatest.GivenWhenThen
import org.scalatest.concurrent.Eventually
import org.scalatest.concurrent.PatienceConfiguration.Timeout
import org.scalatest.time.{Seconds, Span}
import org.scalatestplus.junit.JUnitRunner

import scala.collection.JavaConverters.asScalaIteratorConverter
import scala.collection.mutable.ListBuffer
import scala.concurrent.ExecutionContext.Implicits._
import scala.concurrent.Future
import scala.util.parsing.json.JSONObject
import scala.util.{Failure, Success, Try}

@RunWith(classOf[JUnitRunner])
class DoubleTTFJoinSpecWatermarkIssueSuccess extends TestUtils with GivenWhenThen with Eventually {

  val kafkaProperties: Properties = new Properties()
  kafkaProperties.setProperty("bootstrap.servers", kafkaConfig.bootstrapServers)
  kafkaProperties.setProperty("group.id", "FlinkJoinProblemSpec")
  kafkaProperties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")


  "Demo" should
    """
      show that TTF join is subject to watermarking issue (SUCCESSFUL)
    """.stripMargin in {
    implicit val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(3)
    env.getConfig.setAutoWatermarkInterval(15000L)
    implicit val tEnv: StreamTableEnvironment = StreamTableEnvironment.create(env, new TableConfig())
    /*
    This test will work IF AND ONLY IF the ccy will arrive before the watermark as the watermarking is disabled for this stream (Can this be an issue ?)
     */
    Given("Running flink environment with 2 TTF Joins")
      val ccyIsoStream = makeIdlingFlinkConsumer[CcyIsoDTO](AvroDeserializationSchema.forSpecific[CcyIsoDTO](classOf[CcyIsoDTO]),
        kafkaProperties, 0L, _.getTs, ccyIsoTopicName)
      ccyIsoStream.map {
        r => println(r.toString)
      }
      val ccyIsoTable = tEnv.fromDataStream(ccyIsoStream, 'ccyIsoCode, 'ccyIsoName, 'ts.rowtime.as('ccy_rowtime))
      tEnv.registerTable("CcyIsoTable", ccyIsoTable)
      val ccyTable = ccyIsoTable.createTemporalTableFunction('ccy_rowtime, 'ccyIsoCode)
      tEnv.registerFunction("ccyTable", ccyTable)

      val ratesStream = makeFlinkConsumer[RatesDTO](AvroDeserializationSchema.forSpecific[RatesDTO](classOf[RatesDTO]), kafkaProperties, 0L, _.getTs, ratesTopicName)
      ratesStream.map {
        r => println(r.toString)
      }
      val ratesTable = tEnv.fromDataStream(ratesStream, 'ratesCcyIsoCode, 'rate, 'ts.as('rates_ts), 'ts.rowtime.as('rates_rowtime))
      val ratesTTF = ratesTable.createTemporalTableFunction('rates_rowtime, 'ratesCcyIsoCode)
      tEnv.registerFunction("RatesTTF", ratesTTF)
      tEnv.registerTable("RatesTable", ratesTable)

      val faresStream = makeFlinkConsumer[TaxiFareDTO](AvroDeserializationSchema.forSpecific[TaxiFareDTO](classOf[TaxiFareDTO]), kafkaProperties, 0L, _.getTs, taxiFareTopicName)
      faresStream.map {
        r => println(r.toString)
      }
      val faresTable = tEnv.fromDataStream(faresStream, 'fareCcyIsoCode, 'price, 'ts.as('faresTst), 'ts.rowtime.as('faresRowTime))
      tEnv.registerTable("FaresTable", faresTable)

      // This join get flushed for BOTH rates for some reason
      // and results in twice the number of expected results.
      val ratesCcyIsoJoin = tEnv.sqlQuery(
        """
          |  SELECT ccyIsoCode, ccyIsoName, rate, rates_ts as ccyRatesTse
          |  FROM RatesTable, LATERAL TABLE(ccyTable(rates_rowtime))
          |  WHERE ccyIsoCode = ratesCcyIsoCode
          |""".stripMargin)

      tEnv.toAppendStream[Row](ratesCcyIsoJoin)
        .map(
          r => {
            println(s"ccyRatesJoin : $r")
          }
        )

      val fareRatesJoin = tEnv.sqlQuery(
        """
          | SELECT faresTst, price * rate AS convFare, fareCcyIsoCode, rates_ts as fareRatesTs, rate as realRate
          | FROM FaresTable,
          | LATERAL TABLE( RatesTTF(faresRowTime) )
          | WHERE fareCcyIsoCode = ratesCcyIsoCode
          |""".stripMargin)

      tEnv.toAppendStream[Row](fareRatesJoin)
        .map(
          r => {
            println(s"fareRatesJoin : $r")
          }
        )

      val testSink = new TestSink

      val allJoined = fareRatesJoin
        .join(ratesCcyIsoJoin)
        .where('fareCcyIsoCode === 'ccyIsoCode && 'fareRatesTs === 'ccyRatesTs)
      tEnv.toAppendStream[AllJoined](allJoined)
        .map(
          r => {
            testSink.arrayBuffer.add(r)
            println(s"allJoined : $r")
            r
          }
        ).addSink(makeProducer(OutputTopic, kafkaProperties, new AllJoinedSerializationSchema))
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


    When("Multiple taxi fares arrrive before any of the rates or ccy")
    Thread.sleep(10000)
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
    Thread.sleep(3000L)

    val usd1 = makeRatesDTO("USD", rate=1.1D, ts=3000L)
    publishRatesDTO(usd1)(kafkaConfig)
    val usd2 = makeRatesDTO("USD", rate=1.7D, ts=6500L)
    publishRatesDTO(usd2)(kafkaConfig)
    val usd3 = makeRatesDTO("USD", rate=1.7D, ts=8500L)
    publishRatesDTO(usd3)(kafkaConfig)
    val usd4 = makeRatesDTO("USD", rate=1.7D, ts=20000L)
    publishRatesDTO(usd4)(kafkaConfig)
    Thread.sleep(3000L)
    val usdCcyIsoDTO = makeCcyIsoDTO("USD", "US_DOLLARS", ts= 1L)
    publishCcyIsoDTO(usdCcyIsoDTO)(kafkaConfig)
    val nonJoinerTimeMoverTaxi2 = makeTaxiFareDTO("USD", 23D, 25000L)
    publishTaxiFareDTO(nonJoinerTimeMoverTaxi2)(kafkaConfig)
    Thread.sleep(15000L)


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
