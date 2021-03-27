package com.wutsi.stream.rabbitmq

import com.nhaarman.mockitokotlin2.doReturn
import com.nhaarman.mockitokotlin2.doThrow
import com.nhaarman.mockitokotlin2.mock
import com.nhaarman.mockitokotlin2.whenever
import com.rabbitmq.client.Channel
import com.rabbitmq.client.Connection
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.boot.actuate.health.HealthIndicator
import org.springframework.boot.actuate.health.Status

internal class RabbitMQHealthIndicatorTest {
    lateinit var channel: Channel
    lateinit var connection: Connection
    lateinit var properties: Map<String, Any>
    lateinit var heath: HealthIndicator

    @BeforeEach
    fun setUp() {
        channel = mock()
        connection = mock()
        properties = mapOf("version" to "1.1")

        doReturn(connection).whenever(channel).connection

        heath = RabbitMQHealthIndicator(channel)
    }

    @Test
    fun up() {
        doReturn(properties).whenever(connection).serverProperties

        val result = heath.health()

        assertEquals(Status.UP, result.status)
    }

    @Test
    fun down() {
        doThrow(RuntimeException("yo")).whenever(connection).serverProperties

        val result = heath.health()

        assertEquals(Status.DOWN, result.status)
    }
}
