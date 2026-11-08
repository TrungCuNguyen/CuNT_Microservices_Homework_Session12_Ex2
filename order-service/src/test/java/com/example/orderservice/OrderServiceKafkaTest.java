package com.example.orderservice;

import com.example.orderservice.dto.request.OrderCreateRequestDTO;
import com.example.orderservice.dto.request.OrderItemRequestDTO;
import com.example.orderservice.dto.response.OrderResponseDTO;
import com.example.orderservice.event.OrderCreatedEvent;
import com.example.orderservice.repository.OrderRepository;
import com.example.orderservice.service.OrderService;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.ActiveProfiles;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
@ActiveProfiles("test")
@EmbeddedKafka(partitions = 1, topics = {"order-events"})
class OrderServiceKafkaTest {

    @Autowired
    private OrderService orderService;

    @Autowired
    private OrderRepository orderRepository;

    @Autowired
    private EmbeddedKafkaBroker embeddedKafkaBroker;

    private Consumer<String, OrderCreatedEvent> testConsumer;

    @BeforeEach
    void setUp() {
        orderRepository.deleteAll();

        Map<String, Object> consumerProps = KafkaTestUtils.consumerProps("test-order-group", "true", embeddedKafkaBroker);
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        JsonDeserializer<OrderCreatedEvent> jsonDeserializer = new JsonDeserializer<>(OrderCreatedEvent.class);
        jsonDeserializer.addTrustedPackages("*");

        testConsumer = new DefaultKafkaConsumerFactory<>(consumerProps, new StringDeserializer(), jsonDeserializer).createConsumer();
        testConsumer.subscribe(Collections.singletonList("order-events"));
    }

    @AfterEach
    void tearDown() {
        if (testConsumer != null) {
            testConsumer.close();
        }
    }

    @Test
    @DisplayName("Đặt hàng thành công: lưu DB và gửi OrderCreatedEvent vào topic 'order-events'")
    void testCreateOrderAndPublishKafkaEvent() {
        // Arrange
        OrderCreateRequestDTO request = OrderCreateRequestDTO.builder()
                .customerEmail("customer@example.com")
                .customerName("Nguyen Van A")
                .items(List.of(
                        OrderItemRequestDTO.builder()
                                .productId(1L)
                                .productName("iPhone 16 Pro Max")
                                .quantity(2)
                                .price(new BigDecimal("30000000"))
                                .build()
                ))
                .build();

        // Act
        OrderResponseDTO response = orderService.createOrder(request);

        // Assert DB
        assertNotNull(response);
        assertNotNull(response.getId());
        assertEquals("CREATED", response.getStatus());
        assertEquals(new BigDecimal("60000000"), response.getTotalAmount());
        assertTrue(orderRepository.findById(response.getId()).isPresent());

        // Assert Kafka Message
        ConsumerRecord<String, OrderCreatedEvent> record = KafkaTestUtils.getSingleRecord(testConsumer, "order-events", Duration.ofSeconds(10));
        assertNotNull(record);
        assertEquals(response.getOrderCode(), record.key());

        OrderCreatedEvent event = record.value();
        assertNotNull(event);
        assertEquals(response.getId(), event.getOrderId());
        assertEquals(response.getOrderCode(), event.getOrderCode());
        assertEquals("customer@example.com", event.getCustomerEmail());
        assertEquals(1, event.getItems().size());
        assertEquals(1L, event.getItems().get(0).getProductId());
        assertEquals(2, event.getItems().get(0).getQuantity());
    }
}

