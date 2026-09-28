package com.project.trading.order.application;

import com.project.trading.order.domain.Order;
import com.project.trading.order.domain.OrderRepository;
import com.project.trading.order.domain.OrderStatus;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.Collection;
import java.util.List;

@Service
public class OrderQueries {

    private final OrderRepository orders;

    public OrderQueries(OrderRepository orders) {
        this.orders = orders;
    }

    @Transactional(readOnly = true)
    public List<Order> recent(Collection<OrderStatus> statuses, int limit) {
        return orders.findRecent(statuses, limit);
    }
}
