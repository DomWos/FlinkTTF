package ai.humn

import java.util.Properties

import bbb.avro.dto.RatesDTO
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala._
import org.apache.flink.formats.avro.AvroDeserializationSchema
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.TableConfig
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.junit.runner.RunWith
import org.scalatest.Matchers
import org.scalatest.concurrent.Eventually
import org.scalatest.time.SpanSugar.convertLongToGrainOfTime
import org.scalatestplus.junit.JUnitRunner

import scala.collection.JavaConverters.collectionAsScalaIterableConverter
import scala.concurrent.ExecutionContext.Implicits._
import scala.concurrent.Future
import scala.util.{Failure, Success}

@RunWith(classOf[JUnitRunner])
class FlinkCustomWindowWindowTimeoutSpec extends TestUtils with Eventually with Matchers {

  val kafkaProperties: Properties = new Properties()
  kafkaProperties.setProperty("bootstrap.servers", kafkaConfig.bootstrapServers)
  kafkaProperties.setProperty("group.id", "FlinkJoinProblemSpec")
  kafkaProperties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

  private implicit val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
  env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
  env.getConfig.setAutoWatermarkInterval(5000L)
  env.setBufferTimeout(1000L)
  implicit val tEnv: StreamTableEnvironment = StreamTableEnvironment.create(env, new TableConfig())

  "Demo" should
    """
     Show the use of custom per key Windowing
    """.stripMargin in {

    val ratesStream =
      makeFlinkConsumer[RatesDTO](AvroDeserializationSchema.forSpecific[RatesDTO](classOf[RatesDTO]),
        kafkaProperties, 0L, _.getTs, ratesTopicName)
    ratesStream.map { r => println(r.toString) }

    val stream = ratesStream.keyBy(_.getRatesCcyIsoCode.toString).process(new PerKeyWatermarkAssigner(3000, 1000))(TypeInformation.of(classOf[ValueorWatermark]))
      .keyBy(d => d match {
        case Left(z) => z._1.toString
        case Right(value) => value.getRatesCcyIsoCode.toString
      })
      .process(new PerKeyWindowProcessFunction(5000, 10000L))
      .addSink(new TestSink())

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

    Thread.sleep(5000L)

    val chf = makeRatesDTO("CHF", rate = 2.0D, ts = 10000L)
    publishRatesDTO(chf)
    val gbp1 = makeRatesDTO("GBP", rate = 2.0D, ts = 6100L)
    publishRatesDTO(gbp1)
    val usd1 = makeRatesDTO("USD", rate = 2.0D, ts = 700L)
    publishRatesDTO(usd1)
    val usd2 = makeRatesDTO("USD", rate = 2.0D, ts = 900L)
    publishRatesDTO(usd2)
    val usd3 = makeRatesDTO("USD", rate = 2.0D, ts = 9000L)
    publishRatesDTO(usd3)
    Thread.sleep(7000)
    publishRatesDTO(usd2)
    val usd4 = makeRatesDTO("USD", rate = 2.0D, ts = 29000L)
    publishRatesDTO(usd4)
    val gbp4 = makeRatesDTO("GBP", rate = 2.0D, ts = 6100L)
    publishRatesDTO(gbp4)

    publishRatesDTO(makeRatesDTO("GBP", rate = 2.0D, ts = 170000L))

    Thread.sleep(5000L)

    val expectedOutput = Set(
      Seq(gbp1, gbp4),
      Seq(usd1, usd2),
      Seq(usd3),
      Seq(chf)
    )

    eventually(timeout(20 seconds),interval(40 seconds)) {
      TestSink.values.asScala.toSet shouldBe  expectedOutput
    }
  }
}
