package com.wutsi.stream.rabbitmq

import com.nhaarman.mockitokotlin2.any
import com.nhaarman.mockitokotlin2.argumentCaptor
import com.nhaarman.mockitokotlin2.eq
import com.nhaarman.mockitokotlin2.mock
import com.nhaarman.mockitokotlin2.never
import com.nhaarman.mockitokotlin2.verify
import com.rabbitmq.client.BuiltinExchangeType
import com.rabbitmq.client.Channel
import com.wutsi.stream.Event
import com.wutsi.stream.EventHandler
import com.wutsi.stream.ObjectMapperBuilder
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals

internal class RabbitMQEventStreamTest {
    private lateinit var channel: Channel
    private lateinit var handler: EventHandler

    @BeforeEach
    fun setUp() {
        channel = mock()
        handler = mock()
    }

    @Test
    fun `setup queues and topic on initialization`() {
        RabbitMQEventStream("foo", channel, handler)

        verify(channel).queueDeclare("foo_queue_dlq", true, false, false, emptyMap())

        val params = argumentCaptor<Map<String, Any>>()
        verify(channel).queueDeclare(eq("foo_queue_in"), eq(true), eq(false), eq(false), params.capture())
        assertEquals("", params.firstValue["x-dead-letter-exchange"])
        assertEquals("foo_queue_dlq", params.firstValue["x-dead-letter-routing-key"])

        verify(channel).exchangeDeclare("foo_topic_out", BuiltinExchangeType.FANOUT, true)
    }

    @Test
    fun `queue consumer is delayed`() {
        RabbitMQEventStream("foo", channel, handler)

        verify(channel, never()).basicConsume(eq("foo_queue_in"), eq(false), any())
    }

    @Test
    fun `queue consumer is setup after a delay`() {
        RabbitMQEventStream("foo", channel, handler, 5)

        Thread.sleep(20000)
        verify(channel).basicConsume(eq("foo_queue_in"), eq(false), any())
    }

    @Test
    fun `message enqueued are pushed to the queue`() {
        val stream = RabbitMQEventStream("foo", channel, handler)
        stream.enqueue("foo", "bar")

        val json = argumentCaptor<ByteArray>()
        verify(channel).basicPublish(
            eq(""),
            eq("foo_queue_in"),
            eq(null),
            json.capture()
        )

        val event = ObjectMapperBuilder().build().readValue(json.firstValue, Event::class.java)
        assertEquals("foo", event.type)
        assertEquals("\"bar\"", event.payload)
    }

    @Test
    fun `message published are pushed to the topic`() {
        val stream = RabbitMQEventStream("foo", channel, handler)
        stream.publish("foo", "bar")

        val json = argumentCaptor<ByteArray>()
        verify(channel).basicPublish(
            eq("foo_topic_out"),
            eq(""),
            eq(null),
            json.capture()
        )

        val event = ObjectMapperBuilder().build().readValue(json.firstValue, Event::class.java)
        assertEquals("foo", event.type)
        assertEquals("\"bar\"", event.payload)
    }

    @Test
    fun `source topic bound to queue on subscribe`() {
        val stream = RabbitMQEventStream("foo", channel, handler)
        stream.subscribeTo("from")

        verify(channel).queueBind("foo_queue_in", "from_topic_out", "")
    }
}
