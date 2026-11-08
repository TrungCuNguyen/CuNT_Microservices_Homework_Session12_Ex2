package com.example.inventoryservice.controller;

import com.example.inventoryservice.entity.ProductStock;
import com.example.inventoryservice.repository.ProductStockRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/v1/inventory")
@RequiredArgsConstructor
public class InventoryController {

    private final ProductStockRepository productStockRepository;

    @GetMapping
    public ResponseEntity<List<ProductStock>> getAllStocks() {
        return ResponseEntity.ok(productStockRepository.findAll());
    }

    @GetMapping("/{productId}")
    public ResponseEntity<?> getStockByProductId(@PathVariable Long productId) {
        return productStockRepository.findByProductId(productId)
                .map(ResponseEntity::ok)
                .orElse(ResponseEntity.notFound().build());
    }
}
