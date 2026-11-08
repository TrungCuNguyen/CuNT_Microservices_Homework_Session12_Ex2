package com.example.inventoryservice.config;

import com.example.inventoryservice.entity.ProductStock;
import com.example.inventoryservice.repository.ProductStockRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.CommandLineRunner;
import org.springframework.context.annotation.Configuration;

import java.util.List;

@Configuration
@RequiredArgsConstructor
@Slf4j
public class DataInitializer implements CommandLineRunner {

    private final ProductStockRepository productStockRepository;

    @Override
    public void run(String... args) {
        if (productStockRepository.count() == 0) {
            log.info("Khởi tạo danh sách tồn kho ban đầu cho Inventory Service...");
            productStockRepository.saveAll(List.of(
                    ProductStock.builder().productId(1L).productName("iPhone 16 Pro Max 256GB").quantity(50).build(),
                    ProductStock.builder().productId(2L).productName("Samsung Galaxy S24 Ultra").quantity(40).build(),
                    ProductStock.builder().productId(3L).productName("MacBook Pro 14 M3 Pro").quantity(25).build()
            ));
            log.info("Đã khởi tạo thành công 3 sản phẩm trong kho!");
        }
    }
}

