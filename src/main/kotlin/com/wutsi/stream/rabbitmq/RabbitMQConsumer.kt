package com.wutsi.stream.rabbitmq

import com.fasterxml.jackson.databind.ObjectMapper
import com.rabbitmq.client.AMQP.BasicProperties
import com.rabbitmq.client.Channel
import com.rabbitmq.client.DefaultConsumer
import com.rabbitmq.client.Envelope
import com.wutsi.stream.Event
import com.wutsi.stream.EventHandler
import org.slf4j.LoggerFactory

internal class RabbitMQConsumer(
    private val handler: EventHandler,
    private val mapper: ObjectMapper,
    channel: Channel
) : DefaultConsumer(channel) {
    companion object {
        private val LOGGER = LoggerFactory.getLogger(RabbitMQConsumer::class.java)
    }

    override fun handleDelivery(
        consumerTag: String,
        envelope: Envelope,
        properties: BasicProperties,
        body: ByteArray
    ) {
        LOGGER.info("handleDelivery($consumerTag, $envelope,...)")
        try {
            val event = mapper.readValue(body, Event::class.java)
            handler.onEvent(event)
            channel.basicAck(envelope.deliveryTag, false)
        } catch (ex: Exception) {
            LOGGER.error("Failed to handle the delivery - $consumerTag", ex)
            channel.basicReject(
                envelope.deliveryTag,
                false /* do not requeue - message will go to DLQ */
            )
        }
    }
}
