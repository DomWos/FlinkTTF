import java.util.Properties

import bbb.avro.dto.{CcyIsoDTO, RatesDTO, TaxiFareDTO}
import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.apache.avro.specific.SpecificDatumWriter
import org.apache.flink.api.common.serialization.SerializationSchema
import org.apache.flink.formats.avro.AvroDeserializationSchema
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.types.Row
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.{Serializer => kSerializer}
import org.scalatest.concurrent.Eventually.{eventually, interval, timeout}
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import org.slf4j.LoggerFactory

import scala.concurrent.duration._
import scala.language.postfixOps

class TestUtils extends FlatSpec with Matchers with BeforeAndAfterAll {

        val ratesTopicName = "RateTopic"
        val ratesDtoSerializer: kSerializer[RatesDTO] = new kSerializer[RatesDTO] {
                val writer = new SpecificDatumWriter(classOf[RatesDTO])
                val serializer = new AvroSerializer()
                override def configure(map: java.util.Map[String, _], b: Boolean): Unit = {}
                override def serialize(s: String, r: RatesDTO): Array[Byte] = serializer.serializeAvro(Array(r), writer)
                override def close(): Unit = {}
        }
        def publishRatesDTO(event: RatesDTO)(implicit kafkaConfig: EmbeddedKafkaConfig): Unit = {
                val pr = new ProducerRecord[String, RatesDTO](ratesTopicName, event)
                EmbeddedKafka.publishToKafka(pr)(kafkaConfig, ratesDtoSerializer)
        }
        def makeRatesDTO(ccyIsoCode: String, rate: Double, ts: Long) ={
                val c = RatesDTO.newBuilder()
                c.setRatesCcyIsoCode(ccyIsoCode)
                c.setRate(rate)
                c.setTs(ts)
                c.build()
        }

        val taxiFareTopicName = "TaxiFareTopic"
        val taxiFareDtoSerializer: kSerializer[TaxiFareDTO] = new kSerializer[TaxiFareDTO] {
                val writer = new SpecificDatumWriter(classOf[TaxiFareDTO])
                val serializer = new AvroSerializer()
                override def configure(map: java.util.Map[String, _], b: Boolean): Unit = {}
                override def serialize(s: String, r: TaxiFareDTO): Array[Byte] = serializer.serializeAvro(Array(r), writer)
                override def close(): Unit = {}
        }
        def publishTaxiFareDTO(fare: TaxiFareDTO)(implicit kafkaConfig: EmbeddedKafkaConfig): Unit = {
                val pr = new ProducerRecord[String, TaxiFareDTO](taxiFareTopicName, fare)
                EmbeddedKafka.publishToKafka(pr)(kafkaConfig, taxiFareDtoSerializer)
        }
        def makeTaxiFareDTO(ccyIsoCode: String, price: Double, ts: Long) ={
                val c = TaxiFareDTO.newBuilder()
                c.setFareCcyIsoCode(ccyIsoCode)
                c.setPrice(price)
                c.setTs(ts)
                c.build()
        }


        val ccyIsoTopicName = "CcyIsoTopic"
        val ccyIsoFareDtoSerializer: kSerializer[CcyIsoDTO] = new kSerializer[CcyIsoDTO] {
                val writer = new SpecificDatumWriter(classOf[CcyIsoDTO])
                val serializer = new AvroSerializer()
                override def configure(map: java.util.Map[String, _], b: Boolean): Unit = {}
                override def serialize(s: String, r: CcyIsoDTO): Array[Byte] = serializer.serializeAvro(Array(r), writer)
                override def close(): Unit = {}
        }
        def publishCcyIsoDTO(ccy: CcyIsoDTO)(implicit kafkaConfig: EmbeddedKafkaConfig): Unit = {
                val pr = new ProducerRecord[String, CcyIsoDTO](ccyIsoTopicName, ccy)
                EmbeddedKafka.publishToKafka(pr)(kafkaConfig, ccyIsoFareDtoSerializer)
        }

        def makeCcyIsoDTO(ccyIsoCode: String, ccyIsoName: String, ts: Long) ={
                val c = CcyIsoDTO.newBuilder()
                c.setCcyIsoCode(ccyIsoCode)
                c.setCcyIsoName(ccyIsoName)
                c.setTs(ts)
                c.build()
        }

        implicit protected val kafkaConfig = new EmbeddedKafkaConfig {
                override def kafkaPort: Int = 6001
                override def zooKeeperPort: Int = 6000
                override def customBrokerProperties: Map[String, String] = Map.empty
                override def customProducerProperties: Map[String, String] = Map.empty
                override def customConsumerProperties: Map[String, String] = Map.empty
                override def numberOfThreads: Int = 1
                def bootstrapServers = s"localhost:${kafkaPort}"
        }

        def makeFlinkConsumer[T](deserializationSchema: AvroDeserializationSchema[T],
                                 kafkaConsumerConfig: Properties,
                                 maxOutOfOrderTime: Long,
                                 timestampExtractor: T => Long,
                                 topicName: String
                                )(implicit env: StreamExecutionEnvironment ): DataStream[T]  = {
                val rawConsumer = new FlinkKafkaConsumer[T](topicName, deserializationSchema, kafkaConsumerConfig)
                val consumer = env
                  .addSource(rawConsumer)(deserializationSchema.getProducedType)
                  .assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks[T] {
                          private val maxOutOfOrderness = maxOutOfOrderTime
                          var currentMaxTimestamp: Long = 0L
                          override def extractTimestamp(element: T, previousElementTimestamp: Long): Long = {
                                  val timestamp = timestampExtractor(element)
                                  currentMaxTimestamp = math.max(timestamp, currentMaxTimestamp)
                                  timestamp
                          }
                          override def getCurrentWatermark(): Watermark = {
                                  new Watermark(currentMaxTimestamp - maxOutOfOrderness)
                          }
                  }
                  )
                consumer
        }

        val resultsTopicName = "ResultsTopic"
        val topics = List(ratesTopicName,taxiFareTopicName, ccyIsoTopicName, resultsTopicName)

        override def beforeAll : Unit = {
                EmbeddedKafka.start()
                eventually(timeout(5.seconds), interval(1.second)){
                        assert(EmbeddedKafka.isRunning, "Kafka not ready to use")
                }
                println("Kafka is running")
                EmbeddedKafka.deleteTopics(topics)
        }

        override def afterAll = {
                EmbeddedKafka.deleteTopics(topics)
                EmbeddedKafka.stop()
        }

}



class StringResultSeralizer extends SerializationSchema[(Boolean, Row)] {

        override def serialize(br: (Boolean,Row) ): Array[Byte] = {
                val (b, row) = br
                println(s"RESULT $br")
                val vs = row.toString
                println(s"publishing $vs")
                vs.getBytes
        }
}
