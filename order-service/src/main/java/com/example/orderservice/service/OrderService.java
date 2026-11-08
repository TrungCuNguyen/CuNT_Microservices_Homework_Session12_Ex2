package com.example.orderservice.service;

import com.example.orderservice.dto.request.OrderCreateRequestDTO;
import com.example.orderservice.dto.response.OrderResponseDTO;

import java.util.List;

public interface OrderService {

    OrderResponseDTO createOrder(OrderCreateRequestDTO requestDTO);

    OrderResponseDTO getOrderById(Long id);

    List<OrderResponseDTO> getAllOrders();
}

