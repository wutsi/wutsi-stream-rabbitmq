package com.wutsi.stream.rabbitmq

import com.fasterxml.jackson.databind.ObjectMapper
import com.rabbitmq.client.BuiltinExchangeType
import com.rabbitmq.client.Channel
import com.wutsi.stream.Event
import com.wutsi.stream.EventHandler
import com.wutsi.stream.EventStream
import com.wutsi.stream.ObjectMapperBuilder
import org.slf4j.LoggerFactory
import java.nio.charset.Charset
import java.time.OffsetDateTime
import java.util.Timer
import java.util.TimerTask
import java.util.UUID

class RabbitMQEventStream(
    private val name: String,
    private val channel: Channel,
    private val handler: EventHandler,
    private val consumerSetupDelaySeconds: Long = 180
) : EventStream {
    companion object {
        private val LOGGER = LoggerFactory.getLogger(RabbitMQEventStream::class.java)
    }

    private val queue: String = "${name}_queue_in"
    private val topic: String = toTopicName(name)
    private val queueDLQ: String = "${name}_queue_dlq"
    private val mapper: ObjectMapper = ObjectMapperBuilder().build()

    init {
        // DLQ
        LOGGER.info("Setup DLQ")
        channel.queueDeclare(queueDLQ, true, false, false, emptyMap())

        // Queue
        LOGGER.info("Setup queue: $queue")
        channel.queueDeclare(
            queue,
            true, /* durable */
            false, /* exclusive */
            false, /* autoDelete */
            mapOf(
                "x-dead-letter-exchange" to "",
                "x-dead-letter-routing-key" to queueDLQ
            )
        )
        setupConsumer()

        // Topic
        LOGGER.info("Setup topic: $topic")
        channel.exchangeDeclare(
            topic,
            BuiltinExchangeType.FANOUT,
            true /* durable */
        )
    }

    private fun setupConsumer() {
        /*
        Wait before registering the consumer, so that the server is completely UP.
        With Spring, this caused by bug because the consumer was registered during the startup,
        then it dispatched the event using EventListener, but the event was lost because all the
        event listener was not yet setup by spring!
        */
        LOGGER.info("Will setup queue consumer in $consumerSetupDelaySeconds seconds(s)")
        val task = object : TimerTask() {
            override fun run() {
                LOGGER.info("Registering queue consumer")
                channel.basicConsume(
                    queue,
                    false, /* auto-ack */
                    RabbitMQConsumer(handler, mapper, channel)
                )
            }
        }
        Timer(queue, false).schedule(task, consumerSetupDelaySeconds * 1000)
    }

    override fun close() {
    }

    override fun enqueue(type: String, payload: Any) {
        LOGGER.info("enqueue($type, ...)")

        val event = createEvent(type, payload)
        val json: String = mapper.writeValueAsString(event)
        channel.basicPublish(
            "",
            this.queue,
            null, /* basic-properties */
            json.toByteArray(Charset.forName("utf-8"))
        )
    }

    override fun publish(type: String, payload: Any) {
        LOGGER.info("publish($type, ...)")

        val event = createEvent(type, payload)
        val json: String = mapper.writeValueAsString(event)
        channel.basicPublish(
            this.topic,
            "", /* routing-key */
            null, /* basic-properties */
            json.toByteArray(Charset.forName("utf-8"))
        )
    }

    override fun subscribeTo(source: String) {
        LOGGER.info("Subscribing: $source --> $name")
        channel.queueBind(
            queue,
            toTopicName(source),
            ""
        )
    }

    private fun createEvent(type: String, payload: Any) = Event(
        id = UUID.randomUUID().toString(),
        type = type,
        timestamp = OffsetDateTime.now(),
        payload = mapper.writeValueAsString(payload)
    )

    private fun toTopicName(name: String) = "${name}_topic_out"
}
