package com.example.inventoryservice.consumer;

import com.example.inventoryservice.entity.ProductStock;
import com.example.inventoryservice.event.OrderCreatedEvent;
import com.example.inventoryservice.repository.ProductStockRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

@Component
@RequiredArgsConstructor
@Slf4j
public class InventoryConsumer {

    private final ProductStockRepository productStockRepository;

    /**
     * Lắng nghe topic order-events từ Kafka Broker.
     * Cấu hình group-id="inventory-group" (khác với email-group).
     * Thực hiện trừ kho độc lập với email-service.
     */
    @KafkaListener(
            topics = "${app.kafka.topics.order-events:order-events}",
            groupId = "${spring.kafka.consumer.group-id:inventory-group}"
    )
    @Transactional
    public void handleOrderCreatedEvent(
            @Payload OrderCreatedEvent event,
            @Header(name = KafkaHeaders.RECEIVED_KEY, required = false) String key,
            @Header(name = KafkaHeaders.RECEIVED_PARTITION, required = false) Integer partition) {

        log.info("==================================================================");
        log.info(">>> [INVENTORY SERVICE] Đã nhận sự kiện từ topic 'order-events' [Partition: {}, Key: {}]", partition, key);
        log.info(">>> [INVENTORY SERVICE] Đang xử lý trừ kho cho đơn hàng: [{}]", event.getOrderCode());

        if (event.getItems() == null || event.getItems().isEmpty()) {
            log.warn(">>> [INVENTORY SERVICE] Đơn hàng [{}] không có danh sách sản phẩm!", event.getOrderCode());
            return;
        }

        for (OrderCreatedEvent.OrderItemEvent item : event.getItems()) {
            Long productId = item.getProductId();
            int deductQty = item.getQuantity();

            ProductStock stock = productStockRepository.findByProductId(productId)
                    .orElseGet(() -> ProductStock.builder()
                            .productId(productId)
                            .productName(item.getProductName() != null ? item.getProductName() : "Product #" + productId)
                            .quantity(100) // Khởi tạo 100 nếu chưa có trong DB
                            .build());

            int oldQty = stock.getQuantity();
            int newQty = Math.max(0, oldQty - deductQty);
            stock.setQuantity(newQty);
            productStockRepository.save(stock);

            log.info(">>> [INVENTORY SERVICE] Trừ kho thành công sản phẩm [{}] ID {}: tồn cũ = {}, đã trừ = {}, tồn mới = {}",
                    stock.getProductName(), productId, oldQty, deductQty, newQty);
        }

        log.info(">>> [INVENTORY SERVICE] => Hoàn tất trừ kho cho toàn bộ đơn hàng [{}]!", event.getOrderCode());
        log.info("==================================================================");
    }
}

