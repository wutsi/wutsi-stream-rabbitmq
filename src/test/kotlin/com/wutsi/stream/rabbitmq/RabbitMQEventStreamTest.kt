package com.wutsi.stream.rabbitmq

import com.nhaarman.mockitokotlin2.any
import com.nhaarman.mockitokotlin2.argumentCaptor
import com.nhaarman.mockitokotlin2.doReturn
import com.nhaarman.mockitokotlin2.eq
import com.nhaarman.mockitokotlin2.mock
import com.nhaarman.mockitokotlin2.never
import com.nhaarman.mockitokotlin2.verify
import com.nhaarman.mockitokotlin2.whenever
import com.rabbitmq.client.AMQP
import com.rabbitmq.client.AMQP.BasicProperties
import com.rabbitmq.client.BuiltinExchangeType
import com.rabbitmq.client.Channel
import com.rabbitmq.client.Envelope
import com.rabbitmq.client.GetResponse
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
        RabbitMQEventStream(
            name = "foo",
            channel = channel,
            handler = handler
        )

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
        val stream = RabbitMQEventStream("foo", channel, handler, maxRetries = 11, queueTtlSeconds = 111)
        stream.enqueue("foo", "bar")

        val json = argumentCaptor<ByteArray>()
        val properties = argumentCaptor<BasicProperties>()
        verify(channel).basicPublish(
            eq(""),
            eq("foo_queue_in"),
            properties.capture(),
            json.capture()
        )

        val event = ObjectMapperBuilder().build().readValue(json.firstValue, Event::class.java)
        assertEquals("foo", event.type)
        assertEquals("\"bar\"", event.payload)
        assertEquals(11, properties.firstValue.headers["x-max-retries"])
        assertEquals(0, properties.firstValue.headers["x-retries"])
        assertEquals("111000", properties.firstValue.expiration)
    }

    @Test
    fun `message published are pushed to the topic`() {
        val stream = RabbitMQEventStream("foo", channel, handler, maxRetries = 11, queueTtlSeconds = 111)
        stream.publish("foo", "bar")

        val json = argumentCaptor<ByteArray>()
        val properties = argumentCaptor<BasicProperties>()
        verify(channel).basicPublish(
            eq("foo_topic_out"),
            eq(""),
            properties.capture(),
            json.capture()
        )

        val event = ObjectMapperBuilder().build().readValue(json.firstValue, Event::class.java)
        assertEquals("foo", event.type)
        assertEquals("\"bar\"", event.payload)
        assertEquals(11, properties.firstValue.headers["x-max-retries"])
        assertEquals(0, properties.firstValue.headers["x-retries"])
        assertEquals("111000", properties.firstValue.expiration)
    }

    @Test
    fun `source topic bound to queue on subscribe`() {
        val stream = RabbitMQEventStream("foo", channel, handler)
        stream.subscribeTo("from")

        verify(channel).queueBind("foo_queue_in", "from_topic_out", "")
    }

    @Test
    fun `replay DLQ message`() {
        val body = "yo man".toByteArray()
        val response = GetResponse(
            Envelope(111, true, "xxx", "xxx"),
            properties(retries = 3),
            body,
            1
        )

        doReturn(response).doReturn(null).whenever(channel).basicGet(any(), any())

        val stream = RabbitMQEventStream("foo", channel, handler)
        stream.replayDlq()

        val properties = argumentCaptor<BasicProperties>()
        verify(channel).basicPublish(
            eq(""),
            eq("foo_queue_in"),
            properties.capture(),
            eq(body)
        )
        assertEquals(4, properties.firstValue.headers["x-retries"])

        verify(channel).basicAck(111, false)
    }

    @Test
    fun `do not replay DLQ message when too many retries`() {
        val body = "yo man".toByteArray()
        val response = GetResponse(
            Envelope(111, true, "xxx", "xxx"),
            properties(retries = 10, maxRetries = 10),
            body,
            1
        )

        doReturn(response).doReturn(null).whenever(channel).basicGet(any(), any())

        val stream = RabbitMQEventStream("foo", channel, handler)
        stream.replayDlq()

        verify(channel, never()).basicPublish(any(), any(), any(), any())
        verify(channel).basicReject(111, false)
    }

    private fun properties(maxRetries: Int = 10, retries: Int = 1) = AMQP.BasicProperties().builder()
        .headers(
            mapOf(
                "x-max-retries" to maxRetries,
                "x-retries" to retries
            )
        )
        .build()
}
