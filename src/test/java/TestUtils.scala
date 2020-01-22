import java.util.Properties
import java.util.concurrent.{ConcurrentHashMap, ConcurrentLinkedQueue}

import bbb.avro.dto.{CcyIsoDTO, RatesDTO, TaxiFareDTO}
import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.apache.avro.specific.SpecificDatumWriter
import org.apache.avro.util.Utf8
import org.apache.flink.api.common.serialization.SerializationSchema
import org.apache.flink.api.common.state.{MapStateDescriptor, ReadOnlyBroadcastState}
import org.apache.flink.api.common.typeinfo.Types
import org.apache.flink.formats.avro.AvroDeserializationSchema
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.functions.co.{KeyedBroadcastProcessFunction, KeyedCoProcessFunction}
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor
import org.apache.flink.streaming.api.operators.co.{CoBroadcastWithKeyedOperator, KeyedCoProcessOperator}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.types.Row
import org.apache.flink.util.Collector
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.{Deserializer, Serializer => kSerializer}
import org.scalatest.concurrent.Eventually.{eventually, interval, timeout}
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

import scala.collection.mutable.ListBuffer
import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.Try

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
                println(s"publishRatesDTO : $event")
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
                println(s"publishTaxiFareDTO : $fare")
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
                println(s"publishCcyIsoDTO : $ccy")
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
                  .assignTimestampsAndWatermarks(
                          new AscendingTimestampExtractor[T] {
                                  override def extractAscendingTimestamp(t: T): Long = {
                                          val timestamp = timestampExtractor(t)
                                          println(s"$topicName timestamp $timestamp")
                                          timestamp
                                  }
                          }
//                          new AssignerWithPeriodicWatermarks[T] {
//                          private val maxOutOfOrderness = maxOutOfOrderTime
//                          var currentMaxTimestamp: Long = 0L
//                          override def extractTimestamp(element: T, previousElementTimestamp: Long): Long = {
//                                  val timestamp = timestampExtractor(element)
//                                  currentMaxTimestamp = math.max(timestamp, currentMaxTimestamp)
//                                  println(s"$topicName timestamp $timestamp")
//                                  timestamp
//                          }
//                          override def getCurrentWatermark: Watermark = {
//                                  val cw = currentMaxTimestamp - maxOutOfOrderness
//                                  println(s"$topicName cw $cw")
//                                  new Watermark(cw)
//                          }
//                  }
                  )
                consumer
        }

        def makeFlinkConsumerLaggingWatermarks[T](deserializationSchema: AvroDeserializationSchema[T],
                                 kafkaConsumerConfig: Properties,
                                 maxOutOfOrderTime: Long,
                                 timestampExtractor: T => Long,
                                 topicName: String
                                )(implicit env: StreamExecutionEnvironment ): DataStream[T]  = {
                val rawConsumer = new FlinkKafkaConsumer[T](topicName, deserializationSchema, kafkaConsumerConfig)
                val consumer = env
                  .addSource(rawConsumer)(deserializationSchema.getProducedType)
                    .assignTimestampsAndWatermarks(
                            new AssignerWithPeriodicWatermarks[T] {
                                      private val maxOutOfOrderness = maxOutOfOrderTime
                                      var currentMaxTimestamp: Long = 0L
                                      override def extractTimestamp(element: T, previousElementTimestamp: Long): Long = {
                                              val timestamp = timestampExtractor(element)
                                              currentMaxTimestamp = math.min(timestamp, currentMaxTimestamp)
                                              println(s"$topicName timestamp $timestamp")
                                              timestamp
                                      }
                                      override def getCurrentWatermark: Watermark = {
                                              val cw = currentMaxTimestamp - maxOutOfOrderness
                                              println(s"$topicName cw $cw")
                                              new Watermark(cw)
                                      }
                              }
                    )
                consumer
        }

        def makeFlinkConsumerNoWatermark[T](deserializationSchema: AvroDeserializationSchema[T],
                                             kafkaConsumerConfig: Properties,
                                             maxOutOfOrderTime: Long,
                                             timestampExtractor: T => Long,
                                             topicName: String
                                            )(implicit env: StreamExecutionEnvironment ): DataStream[T]  = {
                val rawConsumer = new FlinkKafkaConsumer[T](topicName, deserializationSchema, kafkaConsumerConfig)
                val consumer = env
                  .addSource(rawConsumer)(deserializationSchema.getProducedType)
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

        def getMessagesFromKafka(maxMessages: Int, timeoutSec: Int=5) = {
                val messages = ListBuffer[String]()
                Try { eventually(timeout(timeoutSec.seconds), interval(1.second)) {
                        EmbeddedKafka.consumeNumberMessagesFrom(resultsTopicName, maxMessages)(kafkaConfig,
                                new Deserializer[String] {
                                        override def configure(configs: java.util.Map[String, _], isKey: Boolean): Unit = {}

                                        override def deserialize(topic: String, data: Array[Byte]): String = {
                                                val result = new String(data)
                                                messages += result
                                                result
                                        }

                                        override def close(): Unit = {}
                                }
                        )
                }
                }
                messages
        }

}



class StringResultSeralizer extends SerializationSchema[(Boolean, Row)] {

        override def serialize(br: (Boolean,Row) ): Array[Byte] = {
                val (b, row) = br
                println(s"retract    $br")
                val vs = row.toString
                println(s"publishing $vs")
                vs.getBytes
        }
}


class CcyIsoBroadcastRaceConditionAverseKeyedFunction extends KeyedBroadcastProcessFunction[String, RatesDTO, CcyIsoDTO, RatesWithCcyName]() {

        override def processElement(rate: RatesDTO, readOnlyContext: KeyedBroadcastProcessFunction[String, RatesDTO, CcyIsoDTO, RatesWithCcyName]#ReadOnlyContext,
                                    collector: Collector[RatesWithCcyName]): Unit = {
                val ccyIsoBcast: ReadOnlyBroadcastState[String, CcyIsoDTO] = readOnlyContext.getBroadcastState(CcyIsoBroadcastRaceConditionAverseKeyedFunction.ccyDescriptor)
                if( rate != null ) {
                        val ccyCode = rate.getRatesCcyIsoCode.toString
                        val ccyIsoDto: CcyIsoDTO = ccyIsoBcast.get(ccyCode)
                        if( currencyIsoIsLate(ccyIsoDto) ) {
                                System.err.println(s"No matching ccyIsoDto iso code : $ccyCode")
                                if ( !CcyIsoBroadcastRaceConditionAverseKeyedFunction.values.containsKey(ccyCode) ) {
                                        CcyIsoBroadcastRaceConditionAverseKeyedFunction.values.put(ccyCode, new ConcurrentLinkedQueue[RatesDTO]())
                                }
                                CcyIsoBroadcastRaceConditionAverseKeyedFunction.values.get(ccyCode).add(rate)
                        } else {
                                if( thereAreBackloggedRates(ccyCode) ){
                                        val rates = CcyIsoBroadcastRaceConditionAverseKeyedFunction.values.get(ccyCode)
                                        rates.toArray.foreach {
                                                rate  => doCollect(rate.asInstanceOf[RatesDTO], ccyIsoDto, collector)
                                        }
                                        CcyIsoBroadcastRaceConditionAverseKeyedFunction.values.remove(ccyCode)
                                }
                                doCollect(rate, ccyIsoDto, collector)
                        }
                }
        }

        private def currencyIsoIsLate(ccyIsoDto: CcyIsoDTO) = ccyIsoDto == null
        private def thereAreBackloggedRates( ccyCode: String ) = CcyIsoBroadcastRaceConditionAverseKeyedFunction.values.containsKey(ccyCode)

        private def doCollect(rate: RatesDTO, ccyIsoDTO: CcyIsoDTO, collector: Collector[RatesWithCcyName]) : Unit = {
                System.err.println(s"doCollect rate: $rate, ccyIsoDTO:$ccyIsoDTO")
                collector.collect({
                        val ccr = new RatesWithCcyName()
                        ccr.setRatesCcyIsoCode(rate.getRatesCcyIsoCode.toString)
                        ccr.setRate(rate.getRate)
                        ccr.setTs(rate.getTs)
                        ccr.setCcyName(ccyIsoDTO.getCcyIsoName.toString)
                        ccr
                })
        }

        override def processBroadcastElement(in2: CcyIsoDTO,
                                             context: KeyedBroadcastProcessFunction[String, RatesDTO, CcyIsoDTO, RatesWithCcyName]#Context,
                                             collector: Collector[RatesWithCcyName]): Unit = {
                val bcState = context.getBroadcastState(CcyIsoBroadcastRaceConditionAverseKeyedFunction.ccyDescriptor)
                bcState.put(in2.getCcyIsoCode.toString, in2)
        }
}

object CcyIsoBroadcastRaceConditionAverseKeyedFunction {
        val ccyDescriptor = new MapStateDescriptor("ccyIsoCodeBroadcastState", Types.STRING, Types.POJO(classOf[CcyIsoDTO]))
        val values = new ConcurrentHashMap[String, ConcurrentLinkedQueue[RatesDTO]]()
}
