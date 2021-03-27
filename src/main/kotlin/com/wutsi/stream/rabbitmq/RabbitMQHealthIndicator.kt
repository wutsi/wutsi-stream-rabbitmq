package com.wutsi.stream.rabbitmq

import com.rabbitmq.client.Channel
import org.springframework.boot.actuate.health.Health
import org.springframework.boot.actuate.health.HealthIndicator

class RabbitMQHealthIndicator(
    private val channel: Channel
) : HealthIndicator {
    override fun health(): Health {
        try {
            val now = System.currentTimeMillis()
            val version = channel.connection.serverProperties.get("version").toString()
            return Health.up()
                .withDetail("version", version)
                .withDetail("durationMillis", System.currentTimeMillis() - now)
                .build()
        } catch (ex: Exception) {
            return Health.down()
                .withException(ex)
                .build()
        }
    }
}
