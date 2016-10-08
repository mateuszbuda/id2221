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

    // consume "avg" topic with one partition
    var topics = Map[String, Int](
    "avg" -> 1)
    // if you want to try the receiver-less approach, comment the below line and uncomment the next one
    // we run it locally so we use memory only storage level
    val messages = KafkaUtils.createStream[String, String, StringDecoder, StringDecoder](ssc, kafkaConf, topics,
      StorageLevel.MEMORY_ONLY)
    //val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](<FILL IN>)

    val values = messages.map(_._2)
    val pairs = values.map(x => {
      val pair = x.split(",")
      (pair(0), pair(1).toDouble)
    })

    // To be able to compute the average for each key over the whole stream we need to store sum and count
    // so we use Tuple2 as a state type
    def mappingFunc(key: String, value: Option[Double], state: State[(Int, Double)]): Option[(String, Double)] = {
      val restoredState = state.getOption.getOrElse((0, 0d))
      val sum = value.getOrElse(0d) + restoredState._2
      val count = 1 + restoredState._1
      val output = (key, sum / count)
      state.update((count, sum))
      Option(output)
    }

    val stateDstream = pairs.mapWithState(StateSpec.function(mappingFunc _))

    stateDstream.stateSnapshots().mapValues(v => { v._2 / v._1 }).print(26)
    ssc.start()
    ssc.awaitTermination()
  }
}
