package com.example.inventoryservice.repository;

import com.example.inventoryservice.entity.ProductStock;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.Optional;

@Repository
public interface ProductStockRepository extends JpaRepository<ProductStock, Long> {
    Optional<ProductStock> findByProductId(Long productId);
}

