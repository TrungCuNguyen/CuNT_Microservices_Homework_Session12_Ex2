package com.example.emailservice;

import com.example.emailservice.consumer.EmailConsumer;
import com.example.emailservice.event.OrderCreatedEvent;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;

@SpringBootTest
@ActiveProfiles("test")
@EmbeddedKafka(partitions = 1, topics = {"order-events"}, bootstrapServersProperty = "spring.kafka.bootstrap-servers")
class EmailConsumerKafkaTest {

    @Autowired
    private EmbeddedKafkaBroker embeddedKafkaBroker;

    @MockitoSpyBean
    private EmailConsumer emailConsumer;

    @Test
    @DisplayName("Email-Service lắng nghe thành công sự kiện OrderCreatedEvent từ topic 'order-events' với group-id email-group")
    void testEmailConsumerReceivesOrderCreatedEvent() {
        // Arrange Producer
        Map<String, Object> producerProps = KafkaTestUtils.producerProps(embeddedKafkaBroker);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);

        KafkaTemplate<String, OrderCreatedEvent> kafkaTemplate =
                new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(producerProps));

        OrderCreatedEvent event = OrderCreatedEvent.builder()
                .orderId(101L)
                .orderCode("ORD-TEST-001")
                .customerEmail("customer@test.com")
                .customerName("Tran Thi B")
                .totalAmount(new BigDecimal("15000000"))
                .items(List.of(
                        OrderCreatedEvent.OrderItemEvent.builder()
                                .productId(2L)
                                .productName("Galaxy S24")
                                .quantity(1)
                                .price(new BigDecimal("15000000"))
                                .build()
                ))
                .timestamp(System.currentTimeMillis())
                .build();

        // Act: Gửi event vào topic 'order-events'
        kafkaTemplate.send("order-events", event.getOrderCode(), event);

        // Assert: Verify emailConsumer.handleOrderCreatedEvent() được gọi
        verify(emailConsumer, timeout(10000)).handleOrderCreatedEvent(any(OrderCreatedEvent.class), any(), any());
    }
}

