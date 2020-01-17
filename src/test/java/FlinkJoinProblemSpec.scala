import java.util.Properties

import bbb.avro.dto.{CcyIsoDTO, RatesDTO, TaxiFareDTO}
import org.apache.flink.formats.avro.AvroDeserializationSchema
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.TableConfig
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.table.api.scala._
import org.apache.flink.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer
import org.apache.flink.types.Row
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.junit.runner.RunWith
import org.scalatestplus.junit.JUnitRunner

import scala.concurrent.Future
import scala.util.{Failure, Success}
import scala.concurrent.ExecutionContext.Implicits._

@RunWith(classOf[JUnitRunner])
class FlinkJoinProblemSpec extends TestUtils {

  val kafkaProperties: Properties = new Properties()
  kafkaProperties.setProperty("bootstrap.servers", kafkaConfig.bootstrapServers)
  kafkaProperties.setProperty("group.id", "FlinkJoinProblemSpec")
  kafkaProperties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

  private implicit val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
  env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
  env.getConfig.setAutoWatermarkInterval(5000L)
  implicit val tEnv: StreamTableEnvironment = StreamTableEnvironment.create(env, new TableConfig())

  "Demo" should
    """
      Show problems joining Rates with Taxi fares and a never changing table CcyISo
    """.stripMargin in {

    val ccyIsoStream = makeFlinkConsumer[CcyIsoDTO](AvroDeserializationSchema.forSpecific[CcyIsoDTO](classOf[CcyIsoDTO]),
      kafkaProperties, 0L, _.getTs, ccyIsoTopicName)
    ccyIsoStream.map{
      r=>println(r.toString)
    }
    val ccyIsoTable = tEnv.fromDataStream(ccyIsoStream, 'ccyIsoCode, 'ccyIsoName, 'ts.rowtime.as('ccy_rowtime) )
    tEnv.registerTable("CcyIsoTable", ccyIsoTable)


    val ratesStream  = makeFlinkConsumer[RatesDTO](AvroDeserializationSchema.forSpecific[RatesDTO](classOf[RatesDTO]), kafkaProperties,0L, _.getTs, ratesTopicName)
    ratesStream.map{
      r=>println(r.toString)
    }
    val ratesTable = tEnv.fromDataStream(ratesStream, 'ratesCcyIsoCode, 'rate, 'ts.as('rates_ts), 'ts.rowtime.as('rates_rowtime) )
    val ratesTTF = ratesTable.createTemporalTableFunction('rates_rowtime, 'ratesCcyIsoCode)
    tEnv.registerFunction("RatesTTF", ratesTTF)
    tEnv.registerTable("RatesTable", ratesTable)

    val ratesCcyIsoJoin = tEnv.sqlQuery(
      """
        |  SELECT ccyIsoCode, ccyIsoName
        |  FROM CcyIsoTable, RatesTable
        |  WHERE ccyIsoCode = ratesCcyIsoCode
        |""".stripMargin)

    tEnv.toAppendStream[Row](ratesCcyIsoJoin)
      .map(
        r=>{
          println(s"ratesCcyIsoJoin : $r")
        }
      )


    val faresStream  = makeFlinkConsumer[TaxiFareDTO](AvroDeserializationSchema.forSpecific[TaxiFareDTO](classOf[TaxiFareDTO]), kafkaProperties,0L, _.getTs, taxiFareTopicName)
    faresStream.map{
      r=>println(r.toString)
    }
    val faresTable = tEnv.fromDataStream(faresStream, 'fareCcyIsoCode, 'price, 'ts.as('fares_ts), 'ts.rowtime.as('fares_rowtime) )
    tEnv.registerTable("FaresTable", faresTable)


    val fareRatesJoin = tEnv.sqlQuery(
      """
        | SELECT fares_ts, price * rate AS conv_fare, fareCcyIsoCode
        | FROM FaresTable,
        | LATERAL TABLE( RatesTTF(fares_rowtime) )
        | WHERE fareCcyIsoCode = ratesCcyIsoCode
        |""".stripMargin)

    tEnv.toAppendStream[Row](fareRatesJoin)
      .map(
        r=>{
          println(s"fareRatesJoin : $r")
        }
      )

    val allJoined = fareRatesJoin
      .join(ratesCcyIsoJoin)
        .where('fareCcyIsoCode === 'ccyIsoCode)

    tEnv.toAppendStream[Row](allJoined)
      .map(
        r=>{
          println(s"allJoined : $r")
        }
      )

    val resultPublisher: FlinkKafkaProducer[(Boolean, Row)] = new FlinkKafkaProducer[(Boolean, Row)](resultsTopicName, new StringResultSeralizer(), kafkaProperties)
    val outStream = tEnv.toRetractStream[Row]( allJoined )
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

    // Register some currencies
    val usdCcyIsoDTO = makeCcyIsoDTO("USD", "US_DOLLARS", ts= 1L)
    publishCcyIsoDTO(usdCcyIsoDTO)(kafkaConfig)


    // now publish some rates
    val usd1 = makeRatesDTO("USD", rate=1.1D, ts=6000L)
    publishRatesDTO(usd1)(kafkaConfig)

    // publish taxi fares
    val taxi1 = makeTaxiFareDTO("USD", 15D, 11000L)
    publishTaxiFareDTO(taxi1)(kafkaConfig)

    // Now change the rate and flush
    val usd2 = makeRatesDTO("USD", 1.2D, ts=21000L)
    publishRatesDTO(usd2)(kafkaConfig)

    val taxi3 =  makeTaxiFareDTO("USD", 15D, 26000L)
    publishTaxiFareDTO(taxi3)(kafkaConfig)
    val taxi4 = makeTaxiFareDTO("USD", 20D, 31000L)
    publishTaxiFareDTO(taxi4)(kafkaConfig)

    Thread.sleep(10 * 1000L)
  }
}