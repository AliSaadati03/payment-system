Payment System — Concurrent Balance Service

A Spring Boot payment microservice that manages customer credit balances (CreditEntry) and records every balance change as an append-only CreditHistory, driven by asynchronous PaymentRequest events (Kafka) as part of a larger order-processing saga.

This README maps the implementation to the "Concurrent Balance Service" challenge and is honest about what is done, what is partial, and what is still missing.

Architecture

The project follows a layered / hexagonal structure across Gradle modules:

common — shared domain primitives (Money, BaseEntity, AggregateRoot, domain events).
payment-service/payment-domain/payment-domain-core — pure domain logic.
CreditEntry — the current balance for a customer (totalCreditAmount).
CreditHistory — an immutable ledger entry (DEBIT/CREDIT) recorded for every balance change.
Payment — the aggregate representing a single payment request (source account, destination account, amount, status).
PaymentDomainServiceImpl — the core rules: validates the amount, checks sufficient credit, mutates CreditEntry, appends a CreditHistory row, and re-derives the balance from the history as a consistency check (validateCreditHistory).
payment-domain/payment-application-service — orchestration (PaymentRequestHelper): loads the account, calls the domain service, persists results, and writes an outbox message.
payment-dataaccess — JPA entities/repositories for CreditEntry, CreditHistory, Payment, and the outbox table.
infrastructure/outbox, infrastructure/saga, infrastructure/kafka — transactional outbox and Kafka wiring so payment results are published reliably as part of a larger saga.

There is no standalone BalanceService interface with credit/debit/transfer/getBalance methods as described in the challenge. Instead, balance changes are driven by inbound PaymentRequest events and exposed through PaymentRequestHelper.debitPayment(...), .creditPayment(...), and .transferPayment(...). Functionally these cover credit/debit/transfer, but the API shape is event-driven rather than a simple synchronous interface.

Concurrency

What's implemented: each debitPayment / creditPayment operation runs inside a single Spring @Transactional boundary, so the read-modify-write on CreditEntry and the corresponding CreditHistory insert are committed atomically per request. PaymentDomainServiceImpl also recomputes the balance from the full CreditHistory list after every mutation (validateCreditHistory) as a sanity check that the running total and the ledger agree.

What's missing / a known gap: there is no explicit concurrency control on CreditEntry itself — no @Version optimistic-locking column, no pessimistic row lock, and no application-level per-account lock. Two concurrent debit requests for the same customer can both read the same CreditEntry state before either commits, which risks a lost update under the database's default isolation level. This is the single biggest gap against the challenge's concurrency requirement and the first thing I'd fix with more time (see below).

Requests for different customers naturally don't block each other, since each transaction only touches that customer's CreditEntry/CreditHistory rows.

Idempotency

What's implemented: idempotency is handled at the outbox level, keyed by sagaId rather than a generic transactionId. Before doing any balance work, debitPayment/creditPayment check OrderOutboxHelper for an already-completed outbox message for that saga id; if one exists, the handler just republishes the existing result and returns, instead of re-applying the balance change. This correctly prevents a redelivered Kafka message from double-processing.

What's missing: this only covers idempotency for the message-driven entry point, not for the BalanceService-style API the challenge describes (i.e., calling credit/debit/transfer directly and repeatedly with the same transactionId). There's also no unique constraint on (sagaId) at the outbox table level enforced concurrently — the check-then-act between the outbox lookup and the insert is not race-proof under simultaneous duplicate deliveries.

Transfer

PaymentRequestHelper.transferPayment(...) calls debitPayment(...) then creditPayment(...) sequentially. Both are @Transactional, but because this is a same-class internal method call, Spring's proxy-based @Transactional does not wrap them in independent transactions when invoked this way — in practice they run inside whatever transaction transferPayment itself is in. This means a failure between the debit and credit steps is not guaranteed to roll back cleanly the way the challenge's atomicity requirement expects, and there's no compensating action (saga step) defined specifically for a failed transfer. I don't consider this atomic yet.

Deadlock: with no explicit lock ordering or row-level locking at all, there's currently no lock-based deadlock risk — but that's a side effect of not having concurrency control, not a deliberate design choice.

Validation
Amount validation: Payment.validatePayment rejects a null or non-positive price.
Sufficient-balance validation: PaymentDomainServiceImpl.validateCreditEntry rejects a debit that exceeds the current CreditEntry balance.
Unknown account: getCreditEntry/getCreditHistory throw if no CreditEntry/CreditHistory exists for the customer.
Same-account transfer: not explicitly handled — source and destination are independent CustomerIds with no check that they differ.
Testing

No automated tests currently exist in this repository — there are no idempotency tests and no concurrency tests. This is the other major gap against the challenge and is where I'd focus next.

What I'd do with more time
Add optimistic locking (@Version) to CreditEntryEntity, retry on OptimisticLockException, or move to a pessimistic SELECT ... FOR UPDATE per account for the debit/credit path.
Introduce a real transactionId-keyed idempotency check on the balance-mutation path itself (not just at the outbox layer), backed by a unique constraint so concurrent duplicates can't both pass a check-then-act race.
Make transferPayment genuinely atomic — either a single transaction that debits and credits both CreditEntry rows with consistent lock ordering (e.g., always lock the lower account id first to avoid deadlocks), or an explicit saga with a compensating credit if the debit succeeds but the credit fails.
Add a same-account transfer guard.
Write the concurrency and idempotency tests the challenge asks for: concurrent debits on one account that should only let one succeed, concurrent transfers across multiple accounts, and repeated calls with the same transactionId/sagaId that should apply exactly once.
Build
./gradlew build

(No tests are currently defined, so ./gradlew test runs but verifies nothing.)
