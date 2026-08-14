package com.food.ordering.system.payment.service.domain.payment;

import com.food.ordering.system.payment.service.domain.entity.Payment;
import com.food.ordering.system.payment.service.domain.event.PaymentEvent;

import java.time.ZonedDateTime;
import java.util.List;

public class PaymentFailedEvent extends PaymentEvent {

    public PaymentFailedEvent(Payment payment,
                              ZonedDateTime createdAt,
                              List<String> failureMessages) {
        super(payment, createdAt, failureMessages);
    }

}
