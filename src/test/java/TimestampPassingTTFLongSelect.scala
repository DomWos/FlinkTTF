import java.util
import java.util.Properties

import bbb.avro.dto.{CcyIsoDTO, RatesDTO, TaxiFareDTO}
import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.apache.flink.api.scala._
import org.apache.flink.formats.avro.AvroDeserializationSchema
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.function.ProcessAllWindowFunction
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.table.api.TableConfig
import org.apache.flink.table.api.scala.{StreamTableEnvironment, _}
import org.apache.flink.util.Collector
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.Deserializer
import org.codehaus.jackson.map.ObjectMapper
import org.scalatest.GivenWhenThen
import org.scalatest.concurrent.Eventually
import org.scalatest.time.{Seconds, Span}

import scala.concurrent.ExecutionContext.Implicits._
import scala.concurrent.Future
import scala.util.{Failure, Success}

class TimestampPassingTTFLongSelect extends TestUtils with Eventually{

  val kafkaProperties: Properties = new Properties()
  kafkaProperties.setProperty("bootstrap.servers", kafkaConfig.bootstrapServers)
  kafkaProperties.setProperty("group.id", "FlinkJoinProblemSpec")
  kafkaProperties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

  "TTF" should
    """
      should emit one window when selected field is Long""".stripMargin in {
    implicit val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)
    env.getConfig.setAutoWatermarkInterval(5000L)
    implicit val tEnv: StreamTableEnvironment = StreamTableEnvironment.create(env, new TableConfig())


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


    val ratesCcyIsoJoin = tEnv.sqlQuery(
      """
        |  SELECT ccyIsoCode, ccyIsoName, rate, rates_ts as ratesLong
        |  FROM RatesTable, LATERAL TABLE(ccyTable(rates_rowtime))
        |  WHERE ccyIsoCode = ratesCcyIsoCode
        |""".stripMargin)

    val stream = tEnv.toAppendStream[RatesCcyIsoJoinLong](ratesCcyIsoJoin)

    stream.addSink(ratesJoin => println(s"""Output from Temporal Table Function :${ratesJoin}"""))

    stream.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks[RatesCcyIsoJoinLong] {
      var maxTst = 0L
      override def getCurrentWatermark: Watermark = new Watermark(maxTst-1)

      override def extractTimestamp(element: RatesCcyIsoJoinLong, previousElementTimestamp: Long): Long = {

        maxTst = Math.max(maxTst, element.getRatesLong)
        maxTst
      }
    })
        .timeWindowAll(Time.milliseconds(7000))
        .process(new ProcessAllWindowFunction[RatesCcyIsoJoinLong, String, TimeWindow] {
          override def process(context: Context, elements: Iterable[RatesCcyIsoJoinLong], out: Collector[String]): Unit = {
            out.collect(elements.mkString(","))
          }})
        .addSink(data => System.err.println(s"""Output from Window Process Function: ${data}"""))

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
    val usdCcyIsoDTO = makeCcyIsoDTO("USD", "US_DOLLARS", ts= 1L)
    publishCcyIsoDTO(usdCcyIsoDTO)(kafkaConfig)
    Thread.sleep(3000L) //Watermark will be emitted here but it will be min from all streams(18000

    val usd1 = makeRatesDTO("USD", rate=1.1D, ts=3000L)
    publishRatesDTO(usd1)(kafkaConfig)
    Thread.sleep(2000)
    val usd2 = makeRatesDTO("USD", rate=1.7D, ts=6500L)
    publishRatesDTO(usd2)(kafkaConfig)
    Thread.sleep(2000)

    val usd3 = makeRatesDTO("USD", rate=1.7D, ts=8500L)
    publishRatesDTO(usd3)(kafkaConfig)
    Thread.sleep(2000)

    val usd4 = makeRatesDTO("USD", rate=1.7D, ts=20000L)
    publishRatesDTO(usd4)(kafkaConfig)
    Thread.sleep(3000L)


    Eventually.eventually(timeout(Span(35, Seconds)),interval(Span(3 ,Seconds))) {
      Thread.sleep(1000);
      throw new Exception("Just to keep it spinning")
    }
  }


}
