import java.util.HashMap

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import kafka.serializer.{DefaultDecoder, StringDecoder}

import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.spark.storage.StorageLevel

object KafkaWordCount {
  def main(args: Array[String]) {

    val kafkaConf = Map[String, String](
      "metadata.broker.list" -> "localhost:9092",
      "zookeeper.connect" -> "localhost:2181",
      "group.id" -> "kafka-spark-streaming",
      "zookeeper.connection.timeout.ms" -> "1000")

    val sparkConf = new SparkConf().setAppName("KafkaWordCount").setMaster("local[2]")
    val ssc = new StreamingContext(sparkConf, Seconds(2))
    ssc.checkpoint("checkpoint")

    var topics = Map[String, Int](
    "avg" -> 1)
    // if you want to try the receiver-less approach, comment the below line and uncomment the next one
    val messages = KafkaUtils.createStream[String, String, StringDecoder, StringDecoder](ssc, kafkaConf, topics, StorageLevel.MEMORY_ONLY)
    //val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](<FILL IN>)

    val values = messages.map(_._2)
    val pairs = values.map(x => (x.split(",")(0), x.split(",")(1).toDouble))


    def mappingFunc(key: String, value: Option[Double], state: State[Double]): Option[(String, Double)] = {
      val sum = value.getOrElse(0d) + state.getOption.getOrElse(0d)
      val output = (key, sum)
      state.update(sum)
      Option(output)
    }


    val stateDstream = pairs.mapWithState(StateSpec.function(mappingFunc _))

    stateDstream.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
