package com.wutsi.stream.rabbitmq

import com.fasterxml.jackson.databind.ObjectMapper
import com.nhaarman.mockitokotlin2.any
import com.nhaarman.mockitokotlin2.argumentCaptor
import com.nhaarman.mockitokotlin2.doThrow
import com.nhaarman.mockitokotlin2.mock
import com.nhaarman.mockitokotlin2.never
import com.nhaarman.mockitokotlin2.verify
import com.nhaarman.mockitokotlin2.whenever
import com.rabbitmq.client.Channel
import com.rabbitmq.client.Envelope
import com.wutsi.stream.Event
import com.wutsi.stream.EventHandler
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.nio.charset.Charset
import kotlin.test.assertEquals

internal class RabbitMQConsumerTest {
    private lateinit var handler: EventHandler
    private lateinit var channel: Channel
    private lateinit var consumer: RabbitMQConsumer

    @BeforeEach
    fun setUp() {
        handler = mock()
        channel = mock()

        consumer = RabbitMQConsumer(handler, ObjectMapper(), channel)
    }

    @Test
    fun `forward message to EventHandler on delivery`() {
        val body: String = """
            {
              "id": "123",
              "type": "Yo",
              "payload": "Man"
            }
        """.trimIndent()

        consumer.handleDelivery(
            "consumer-tag",
            Envelope(123L, true, "foo", "bar"),
            mock(),
            body.toByteArray(Charset.forName("utf-8"))
        )

        val event = argumentCaptor<Event>()
        verify(handler).onEvent(event.capture())
        assertEquals("123", event.firstValue.id)
        assertEquals("Man", event.firstValue.payload)
        assertEquals("Yo", event.firstValue.type)
    }

    @Test
    fun `don't forward message to EventHandler on delivery error`() {
        val body: String = """
            xxx
        """.trimIndent()

        consumer.handleDelivery(
            "consumer-tag",
            Envelope(123L, true, "foo", "bar"),
            mock(),
            body.toByteArray(Charset.forName("utf-8"))
        )

        verify(handler, never()).onEvent(any())
    }

    @Test
    fun `ACK on delivery`() {
        val body: String = """
            {
              "id": "123",
              "type": "Yo",
              "payload": "Man"
            }
        """.trimIndent()

        consumer.handleDelivery(
            "consumer-tag",
            Envelope(567L, true, "foo", "bar"),
            mock(),
            body.toByteArray(Charset.forName("utf-8"))
        )

        verify(channel).basicAck(567L, false)
    }

    @Test
    fun `DLQ on malformed message`() {
        val body: String = """
            xxx
        """.trimIndent()

        consumer.handleDelivery(
            "consumer-tag",
            Envelope(567L, true, "foo", "bar"),
            mock(),
            body.toByteArray(Charset.forName("utf-8"))
        )

        verify(channel).basicReject(567L, false)
    }

    @Test
    fun `NACK when error while handling message`() {
        val body: String = """
            {
              "id": "123",
              "type": "Yo",
              "payload": "Man"
            }
        """.trimIndent()

        doThrow(RuntimeException::class).whenever(handler).onEvent(any())

        consumer.handleDelivery(
            "consumer-tag",
            Envelope(567L, true, "foo", "bar"),
            mock(),
            body.toByteArray(Charset.forName("utf-8"))
        )

        verify(channel).basicReject(567L, false)
    }
}
