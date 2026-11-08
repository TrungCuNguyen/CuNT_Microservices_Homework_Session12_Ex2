package com.example.emailservice.consumer;

import com.example.emailservice.event.OrderCreatedEvent;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class EmailConsumer {

    /**
     * Lắng nghe topic order-events từ Kafka Broker.
     * Cấu hình group-id="email-group" (khác với inventory-group).
     * Cả email-service và inventory-service cùng nhận được sự kiện để xử lý độc lập.
     */
    @KafkaListener(
            topics = "${app.kafka.topics.order-events:order-events}",
            groupId = "${spring.kafka.consumer.group-id:email-group}"
    )
    public void handleOrderCreatedEvent(
            @Payload OrderCreatedEvent event,
            @Header(name = KafkaHeaders.RECEIVED_KEY, required = false) String key,
            @Header(name = KafkaHeaders.RECEIVED_PARTITION, required = false) Integer partition) {

        log.info("==================================================================");
        log.info(">>> [EMAIL SERVICE] Đã nhận sự kiện từ topic 'order-events' [Partition: {}, Key: {}]", partition, key);
        log.info(">>> [EMAIL SERVICE] Đang gửi email xác nhận cho đơn hàng: [{}]", event.getOrderCode());
        log.info(">>> [EMAIL SERVICE] Người nhận: {} <{}>", event.getCustomerName(), event.getCustomerEmail());
        log.info(">>> [EMAIL SERVICE] Tổng số tiền thanh toán: {} VNĐ", event.getTotalAmount());

        if (event.getItems() != null) {
            log.info(">>> [EMAIL SERVICE] Chi tiết sản phẩm trong đơn:");
            event.getItems().forEach(item ->
                    log.info("    + Sản phẩm ID {}: {} | SL: {} | Đơn giá: {} VNĐ",
                            item.getProductId(), item.getProductName(), item.getQuantity(), item.getPrice())
            );
        }

        // Mô phỏng quá trình gửi email thành công
        log.info(">>> [EMAIL SERVICE] => Gửi email xác nhận đơn hàng [{}] tới {} THÀNH CÔNG!",
                event.getOrderCode(), event.getCustomerEmail());
        log.info("==================================================================");
    }
}
