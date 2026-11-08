package com.example.orderservice.service.impl;

import com.example.orderservice.dto.request.OrderCreateRequestDTO;
import com.example.orderservice.dto.request.OrderItemRequestDTO;
import com.example.orderservice.dto.response.OrderResponseDTO;
import com.example.orderservice.entity.Order;
import com.example.orderservice.entity.OrderItem;
import com.example.orderservice.event.OrderCreatedEvent;
import com.example.orderservice.repository.OrderRepository;
import com.example.orderservice.service.OrderService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

@Service
@RequiredArgsConstructor
@Slf4j
public class OrderServiceImpl implements OrderService {

    private final OrderRepository orderRepository;
    private final KafkaTemplate<String, OrderCreatedEvent> kafkaTemplate;

    @Value("${app.kafka.topics.order-events:order-events}")
    private String orderEventsTopic;

    @Override
    @Transactional
    public OrderResponseDTO createOrder(OrderCreateRequestDTO requestDTO) {
        log.info("==> [ORDER PRODUCER] Tiếp nhận yêu cầu đặt hàng từ khách hàng: {}", requestDTO.getCustomerEmail());

        // 1. Tính toán tổng tiền
        BigDecimal totalAmount = BigDecimal.ZERO;
        for (OrderItemRequestDTO item : requestDTO.getItems()) {
            BigDecimal itemTotal = item.getPrice().multiply(BigDecimal.valueOf(item.getQuantity()));
            totalAmount = totalAmount.add(itemTotal);
        }

        String orderCode = "ORD-" + UUID.randomUUID().toString().substring(0, 8).toUpperCase();

        // 2. Tạo và lưu đơn hàng vào DB
        Order order = Order.builder()
                .orderCode(orderCode)
                .customerEmail(requestDTO.getCustomerEmail())
                .customerName(requestDTO.getCustomerName())
                .totalAmount(totalAmount)
                .status("CREATED")
                .createdAt(LocalDateTime.now())
                .build();

        for (OrderItemRequestDTO itemDTO : requestDTO.getItems()) {
            OrderItem orderItem = OrderItem.builder()
                    .productId(itemDTO.getProductId())
                    .productName(itemDTO.getProductName() != null ? itemDTO.getProductName() : "Product #" + itemDTO.getProductId())
                    .quantity(itemDTO.getQuantity())
                    .price(itemDTO.getPrice())
                    .build();
            order.addItem(orderItem);
        }

        Order savedOrder = orderRepository.save(order);
        log.info("==> [DATABASE] Đã lưu thành công đơn hàng [{}] ID: {} vào Database.", savedOrder.getOrderCode(), savedOrder.getId());

        // 3. Chuẩn bị sự kiện OrderCreatedEvent
        List<OrderCreatedEvent.OrderItemEvent> itemEvents = savedOrder.getItems().stream()
                .map(item -> OrderCreatedEvent.OrderItemEvent.builder()
                        .productId(item.getProductId())
                        .productName(item.getProductName())
                        .quantity(item.getQuantity())
                        .price(item.getPrice())
                        .build())
                .collect(Collectors.toList());

        OrderCreatedEvent event = OrderCreatedEvent.builder()
                .orderId(savedOrder.getId())
                .orderCode(savedOrder.getOrderCode())
                .customerEmail(savedOrder.getCustomerEmail())
                .customerName(savedOrder.getCustomerName())
                .totalAmount(savedOrder.getTotalAmount())
                .items(itemEvents)
                .timestamp(System.currentTimeMillis())
                .build();

        // 4. Gửi sự kiện vào Kafka topic 'order-events' bất đồng bộ
        kafkaTemplate.send(orderEventsTopic, savedOrder.getOrderCode(), event);
        log.info("==> [KAFKA PRODUCER] Đã bắn sự kiện OrderCreatedEvent vào topic '{}' với Key: {}", orderEventsTopic, savedOrder.getOrderCode());
        log.info("==> [KẾT QUẢ MONG MUỐN] Luồng đặt hàng kết thúc ngay khi gửi tin vào Kafka, giúp trải nghiệm người dùng mượt mà hơn!");

        return mapToResponse(savedOrder);
    }

    @Override
    @Transactional(readOnly = true)
    public OrderResponseDTO getOrderById(Long id) {
        Order order = orderRepository.findById(id)
                .orElseThrow(() -> new RuntimeException("Không tìm thấy đơn hàng ID: " + id));
        return mapToResponse(order);
    }

    @Override
    @Transactional(readOnly = true)
    public List<OrderResponseDTO> getAllOrders() {
        return orderRepository.findAll().stream()
                .map(this::mapToResponse)
                .collect(Collectors.toList());
    }

    private OrderResponseDTO mapToResponse(Order order) {
        List<OrderResponseDTO.OrderItemResponseDTO> items = order.getItems().stream()
                .map(item -> OrderResponseDTO.OrderItemResponseDTO.builder()
                        .id(item.getId())
                        .productId(item.getProductId())
                        .productName(item.getProductName())
                        .quantity(item.getQuantity())
                        .price(item.getPrice())
                        .build())
                .collect(Collectors.toList());

        return OrderResponseDTO.builder()
                .id(order.getId())
                .orderCode(order.getOrderCode())
                .customerEmail(order.getCustomerEmail())
                .customerName(order.getCustomerName())
                .totalAmount(order.getTotalAmount())
                .status(order.getStatus())
                .items(items)
                .createdAt(order.getCreatedAt())
                .build();
    }
}
