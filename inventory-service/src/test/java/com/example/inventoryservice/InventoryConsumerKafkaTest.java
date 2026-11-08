package com.example.inventoryservice;

import com.example.inventoryservice.consumer.InventoryConsumer;
import com.example.inventoryservice.entity.ProductStock;
import com.example.inventoryservice.event.OrderCreatedEvent;
import com.example.inventoryservice.repository.ProductStockRepository;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.BeforeEach;
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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;

@SpringBootTest
@ActiveProfiles("test")
@EmbeddedKafka(partitions = 1, topics = {"order-events"}, bootstrapServersProperty = "spring.kafka.bootstrap-servers")
class InventoryConsumerKafkaTest {

    @Autowired
    private EmbeddedKafkaBroker embeddedKafkaBroker;

    @Autowired
    private ProductStockRepository productStockRepository;

    @MockitoSpyBean
    private InventoryConsumer inventoryConsumer;

    @BeforeEach
    void setUp() {
        productStockRepository.deleteAll();
        productStockRepository.save(ProductStock.builder()
                .productId(1L)
                .productName("iPhone 16 Pro Max")
                .quantity(50)
                .build());
    }

    @Test
    @DisplayName("Inventory-Service lắng nghe sự kiện OrderCreatedEvent và trừ số lượng tồn kho thành công")
    void testInventoryConsumerDeductsStock() {
        // Arrange Producer
        Map<String, Object> producerProps = KafkaTestUtils.producerProps(embeddedKafkaBroker);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);

        KafkaTemplate<String, OrderCreatedEvent> kafkaTemplate =
                new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(producerProps));

        OrderCreatedEvent event = OrderCreatedEvent.builder()
                .orderId(102L)
                .orderCode("ORD-INV-001")
                .customerEmail("customer@test.com")
                .customerName("Le Van C")
                .totalAmount(new BigDecimal("30000000"))
                .items(List.of(
                        OrderCreatedEvent.OrderItemEvent.builder()
                                .productId(1L)
                                .productName("iPhone 16 Pro Max")
                                .quantity(5) // Đặt mua 5 chiếc
                                .price(new BigDecimal("30000000"))
                                .build()
                ))
                .timestamp(System.currentTimeMillis())
                .build();

        // Act: Gửi event vào topic 'order-events'
        kafkaTemplate.send("order-events", event.getOrderCode(), event);

        // Assert: Verify inventoryConsumer.handleOrderCreatedEvent() được gọi
        verify(inventoryConsumer, timeout(10000)).handleOrderCreatedEvent(any(OrderCreatedEvent.class), any(), any());

        // Kiểm tra tồn kho đã bị trừ từ 50 xuống 45 (50 - 5 = 45)
        ProductStock updatedStock = productStockRepository.findByProductId(1L).orElseThrow();
        assertEquals(45, updatedStock.getQuantity());
    }
}
