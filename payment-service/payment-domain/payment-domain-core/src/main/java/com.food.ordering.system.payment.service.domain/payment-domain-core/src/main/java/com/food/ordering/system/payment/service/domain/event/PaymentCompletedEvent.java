package com.food.ordering.system.payment.service.domain.payment;

import com.food.ordering.system.payment.service.domain.entity.Payment;
import com.food.ordering.system.payment.service.domain.event.PaymentEvent;

import java.time.ZonedDateTime;
import java.util.Collections;

public class PaymentCompletedEvent extends PaymentEvent {

    public PaymentCompletedEvent(Payment payment,
                                 ZonedDateTime createdAt) {
        super(payment, createdAt, Collections.emptyList());
    }

}
