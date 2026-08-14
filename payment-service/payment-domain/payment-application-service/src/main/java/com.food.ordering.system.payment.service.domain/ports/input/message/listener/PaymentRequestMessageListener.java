package com.food.ordering.system.payment.service.domain.ports.input.message.listener;

import com.food.ordering.system.payment.service.domain.dto.PaymentRequest;

public interface PaymentRequestMessageListener {

    void debitPayment(PaymentRequest paymentRequest);

    void creditPayment(PaymentRequest paymentRequest);

    void transferPayment(PaymentRequest paymentRequest);
}
