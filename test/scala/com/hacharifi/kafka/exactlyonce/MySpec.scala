package com.hacharifi.kafka.exactlyonce

import net.manub.embeddedkafka.{Consumers, EmbeddedKafka}
import org.apache.kafka.common.serialization.{Deserializer, Serializer, StringDeserializer, StringSerializer}
import org.scalatest.concurrent.Eventually
import org.scalatest.{Matchers, WordSpec}

class MySpec extends WordSpec with EmbeddedKafka with Eventually with Matchers with Consumers {

  val TopicName = "topic-0"

  implicit val serializer: Serializer[String] = new StringSerializer
  implicit val deserializer: Deserializer[String] = new StringDeserializer

  "runs with embedded kafka" should {

    withRunningKafka {

      (1 to 100).indices foreach { indice =>
        publishToKafka[String](TopicName, "value-" + indice)
      }

      "have consume 100 messages" in {
        withStringConsumer { consumer =>
          consumeNumberMessagesFrom(TopicName, 100) should have size 100
        }
      }

    }

  }
}
