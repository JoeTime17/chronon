package ai.chronon.flink.source

import ai.chronon.flink.FlinkUtils
import ai.chronon.online.{TopicChecker, TopicInfo}
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.serialization.DeserializationSchema
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.kafka.clients.consumer.OffsetResetStrategy

class KafkaFlinkSource[T](props: Map[String, String],
                          deserializationSchema: DeserializationSchema[T],
                          topicInfo: TopicInfo)
    extends FlinkSource[T] {
  import KafkaFlinkSource._
  val bootstrap: String = {
    // we first check props for the bootstrap server and fallback to topicInfo if not found
    FlinkUtils
      .getProperty(KafkaBootstrap, props, topicInfo)
      .orElse(getBootstrapFromHostPort(topicInfo.params.get("host"), topicInfo.params.get("port")))
      .getOrElse(throw new IllegalArgumentException("No bootstrap servers provided"))
  }

  // we use a small scale factor as topics are often over partitioned. We can make this configurable via topicInfo
  val scaleFactor = 0.25

  implicit lazy val parallelism: Int = {
    math.ceil(TopicChecker.getPartitions(topicInfo.name, bootstrap, topicInfo.params) * scaleFactor).toInt
  }

  override def getDataStream(topic: String, groupByName: String)(env: StreamExecutionEnvironment,
                                                                 parallelism: Int): SingleOutputStreamOperator[T] = {
    // confirm the topic exists
    TopicChecker.topicShouldExist(topicInfo.name, bootstrap, topicInfo.params)

    val startingOffsets = FlinkUtils.getProperty("start_offset", props, topicInfo) match {
      case Some(timestampStr) =>
        try {
          val timestamp = timestampStr.toLong
          OffsetsInitializer.timestamp(timestamp)
        } catch {
          case _: NumberFormatException =>
            throw new IllegalArgumentException(
              s"Invalid timestamp format for start_offset: '$timestampStr'. Expected a valid long value in milliseconds.")
        }
      case None =>
        OffsetsInitializer.committedOffsets(OffsetResetStrategy.LATEST)
    }

    val kafkaSource = KafkaSource
      .builder[T]()
      .setTopics(topicInfo.name)
      .setGroupId(s"chronon-$groupByName")
      // starting offsets are configurable via start_offset (timestamp) or default to latest when starting afresh
      .setStartingOffsets(startingOffsets)
      .setValueOnlyDeserializer(deserializationSchema)
      .setBootstrapServers(bootstrap)
      .setProperties(TopicChecker.mapToJavaProperties(topicInfo.params))
      .build()

    env
      .fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), s"Kafka source: $groupByName - ${topicInfo.name}")
      .setParallelism(parallelism)
  }
}

object KafkaFlinkSource {
  val KafkaBootstrap = "bootstrap"

  def getBootstrapFromHostPort(maybeHost: Option[String], maybePort: Option[String]): Option[String] = {
    maybeHost
      .map(host => host + maybePort.map(":" + _).getOrElse(""))
  }
}
