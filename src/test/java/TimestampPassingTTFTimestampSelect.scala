import java.util
import java.util.Properties

import bbb.avro.dto.{CcyIsoDTO, RatesDTO, TaxiFareDTO}
import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.apache.avro.util.Utf8
import org.apache.flink.api.scala._
import org.apache.flink.formats.avro.AvroDeserializationSchema
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.function.ProcessAllWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.table.api.TableConfig
import org.apache.flink.table.api.scala.{StreamTableEnvironment, _}
import org.apache.flink.types.Row
import org.apache.flink.util.Collector
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


class TimestampPassingTTFTimestampSelect extends TestUtils with GivenWhenThen  with Eventually{

  val kafkaProperties: Properties = new Properties()
  kafkaProperties.setProperty("bootstrap.servers", kafkaConfig.bootstrapServers)
  kafkaProperties.setProperty("group.id", "FlinkJoinProblemSpec")
  kafkaProperties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

  "Demo" should
    """
     show that joins are properly done if fares arrive first""".stripMargin in {
    implicit val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)
    env.getConfig.setAutoWatermarkInterval(5000L)
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
        |  SELECT ccyIsoCode, ccyIsoName, rate, rates_rowtime as ratesTs
        |  FROM RatesTable, LATERAL TABLE(ccyTable(rates_rowtime))
        |  WHERE ccyIsoCode = ratesCcyIsoCode
        |""".stripMargin)

    tEnv.toAppendStream[RatesCcyIsoJoinTimestamp](ratesCcyIsoJoin)
      .map(
        r => {
          println(s"ccyRatesJoin : $r")
          r
        }
      )
        .timeWindowAll(Time.milliseconds(7000))
        .process(new ProcessAllWindowFunction[RatesCcyIsoJoinTimestamp, String, TimeWindow] {
          override def process(context: Context, elements: Iterable[RatesCcyIsoJoinTimestamp], out: Collector[String]): Unit = {
            out.collect(elements.mkString(","))
          }
        })
        .print()
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
    Thread.sleep(7000)
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
    Thread.sleep(7000L) //Watermark will be emitted here but it will be min from all streams(18000

    Eventually.eventually(timeout(Span(35, Seconds)),interval(Span(3 ,Seconds))) {
      val messages = EmbeddedKafka.consumeNumberMessagesFrom(OutputTopic, 3)(EmbeddedKafkaConfig.defaultConfig, new Deserializer[AllJoined] {
        val objectMapper = new ObjectMapper()
        override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = {}

        override def deserialize(topic: String, data: Array[Byte]): AllJoined = objectMapper.readValue(data, classOf[AllJoined])

        override def close(): Unit = {}
      } )
      println(messages)
    }
  }


}
