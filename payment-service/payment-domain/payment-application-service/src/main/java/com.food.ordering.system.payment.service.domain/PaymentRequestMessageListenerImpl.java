package com.food.ordering.system.payment.service.domain;

import com.food.ordering.system.payment.service.domain.dto.PaymentRequest;
import com.food.ordering.system.payment.service.domain.ports.input.message.listener.PaymentRequestMessageListener;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class PaymentRequestMessageListenerImpl implements PaymentRequestMessageListener {

    private final PaymentRequestHelper paymentRequestHelper;

    public PaymentRequestMessageListenerImpl(PaymentRequestHelper paymentRequestHelper) {
        this.paymentRequestHelper = paymentRequestHelper;
    }

    @Override
    public void debitPayment(PaymentRequest paymentRequest) {
        paymentRequestHelper.debitPayment(paymentRequest);
    }

    @Override
    public void creditPayment(PaymentRequest paymentRequest) {
        paymentRequestHelper.creditPayment(paymentRequest);
    }

    @Override
    public void transferPayment(PaymentRequest paymentRequest) {
        paymentRequestHelper.transferPayment(paymentRequest);
    }

}
