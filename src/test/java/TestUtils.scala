import java.util.Properties

import bbb.avro.dto.{CcyIsoDTO, RatesDTO, TaxiFareDTO}
import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.apache.avro.specific.SpecificDatumWriter
import org.apache.flink.api.common.serialization.SerializationSchema
import org.apache.flink.api.common.state.{MapStateDescriptor, ReadOnlyBroadcastState}
import org.apache.flink.api.common.typeinfo.Types
import org.apache.flink.formats.avro.AvroDeserializationSchema
import org.apache.flink.streaming.api.functions.{AssignerWithPeriodicWatermarks, AssignerWithPunctuatedWatermarks}
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaProducer}
import org.apache.flink.types.Row
import org.apache.flink.util.Collector
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.{Deserializer, Serializer => kSerializer}
import org.codehaus.jackson.map.ObjectMapper
import org.scalatest.concurrent.Eventually.{eventually, interval, timeout}
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll, FlatSpec, Matchers}

import scala.collection.mutable.ListBuffer
import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.Try

class TestUtils extends FlatSpec with Matchers with BeforeAndAfter {

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
                  .assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks[T] {
                          var timestamp = Long.MinValue

                          override def getCurrentWatermark: Watermark = {
                                  val watermark = if (timestamp == Long.MinValue) new Watermark(Long.MinValue) else new Watermark(timestamp-1)
//                                  println("GENERATING: " + watermark + " FOR " + topicName)
                                  watermark
                          }

                          override def extractTimestamp(element: T, previousElementTimestamp: Long): Long = {
                                  timestamp = timestampExtractor(element)
                                  timestamp
                          }
                  })
                consumer
        }

        def makeProducer[T](outTopic: String, props: Properties, serializationSchema: SerializationSchema[T]): FlinkKafkaProducer[T] = {
                new FlinkKafkaProducer[T](
                        outTopic,
                        serializationSchema,
                        props
                )
        }

        def makeIdlingFlinkConsumer[T](deserializationSchema: AvroDeserializationSchema[T],
                                 kafkaConsumerConfig: Properties,
                                 maxOutOfOrderTime: Long,
                                 timestampExtractor: T => Long,
                                 topicName: String
                                )(implicit env: StreamExecutionEnvironment ): DataStream[T]  = {
                val rawConsumer = new FlinkKafkaConsumer[T](topicName, deserializationSchema, kafkaConsumerConfig)
                val consumer = env
                  .addSource(rawConsumer)(deserializationSchema.getProducedType)
                  .assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks[T] {
                          override def getCurrentWatermark: Watermark =  {
//                                  println("MAX LONG GENERATED FOR :" + topicName)
                                  new Watermark(Long.MaxValue)
                          }

                          override def extractTimestamp(element: T, previousElementTimestamp: Long): Long = timestampExtractor(element)
                  })
                consumer
        }

        val resultsTopicName = "ResultsTopic"
        val OutputTopic = "KafkaOutputProducer"

        val topics = List(ratesTopicName,taxiFareTopicName, ccyIsoTopicName, resultsTopicName, OutputTopic)

        before {
                EmbeddedKafka.start()
                eventually(timeout(5.seconds), interval(1.second)){
                        assert(EmbeddedKafka.isRunning, "Kafka not ready to use")
                }
                println("Kafka is running")
                EmbeddedKafka.deleteTopics(topics)

        }

        after {
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


class AllJoinedSerializationSchema extends SerializationSchema[AllJoined] {
        lazy val objectMapper = new ObjectMapper()
        override def serialize(element: AllJoined): Array[Byte] = objectMapper.writer().writeValueAsBytes(element)
}

class AllJoinedStringSerializationSchema extends SerializationSchema[AllJoinedString] {
        lazy val objectMapper = new ObjectMapper()
        override def serialize(element: AllJoinedString): Array[Byte] = objectMapper.writer().writeValueAsBytes(element)
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

//class CcyIsoBroadcastKeyedFunction extends KeyedBroadcastProcessFunction[String, RatesDTO, CcyIsoDTO, RatesWithCcyName]() {
//        val ccyDescriptor = new MapStateDescriptor("ccyIsoCodeBroadcastState", Types.STRING, Types.POJO(classOf[CcyIsoDTO]))
//        override def processElement(in1: RatesDTO, readOnlyContext: KeyedBroadcastProcessFunction[String, RatesDTO, CcyIsoDTO, RatesWithCcyName]#ReadOnlyContext,
//                                    collector: Collector[RatesWithCcyName]): Unit = {
//                val ccyIsoDTO: ReadOnlyBroadcastState[String, CcyIsoDTO] = readOnlyContext.getBroadcastState(ccyDescriptor)
//                if( ccyIsoDTO != null ){
//                        if( in1 != null ) {
//                                val ccyCode = in1.getRatesCcyIsoCode.toString
//                                val ccy = ccyIsoDTO.get(ccyCode)
//                                if( ccy != null ) {
//                                        collector.collect({
//                                                val ccr = new RatesWithCcyName()
//                                                ccr.setRatesCcyIsoCode(new Utf8(in1.getRatesCcyIsoCode.toString))
//                                                ccr.setRate(in1.getRate)
//                                                ccr.setTs(in1.getTs)
//                                                ccr.setCcyName(ccy.getCcyIsoName.toString)
//                                                ccr
//                                        })
//                                }
//                        }
//                }
//        }
//        override def processBroadcastElement(in2: CcyIsoDTO,
//                                             context: KeyedBroadcastProcessFunction[String, RatesDTO, CcyIsoDTO, RatesWithCcyName]#Context,
//                                             collector: Collector[RatesWithCcyName]): Unit = {
//                val bcState = context.getBroadcastState(ccyDescriptor)
//                bcState.put(in2.getCcyIsoCode.toString, in2)
//                println(s"processBroadcastElement $in2")
//        }
//}
