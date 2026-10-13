CREATE TABLE wallets (
    id UUID PRIMARY KEY,
    player_id VARCHAR(100) NOT NULL,
    balance BIGINT NOT NULL CHECK (balance >= 0),
    currency CHAR(3) NOT NULL CHECK (currency ~ '^[A-Z]{3}$'),
    version BIGINT NOT NULL CHECK (version >= 1),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (player_id, currency),
    UNIQUE (id, currency),
    UNIQUE (id, currency, player_id)
);

CREATE TABLE wager_transactions (
    id UUID PRIMARY KEY,
    wallet_id UUID NOT NULL,
    currency CHAR(3) NOT NULL CHECK (currency ~ '^[A-Z]{3}$'),
    external_id VARCHAR(200),
    provider_id VARCHAR(200),
    idempotency_key VARCHAR(255),
    payload_hash CHAR(64),
    player_id VARCHAR(100) NOT NULL,
    round_id VARCHAR(200),
    game_id VARCHAR(200),
    reference_external_id VARCHAR(200),
    amount BIGINT NOT NULL CHECK (amount >= 0),
    tx_type VARCHAR(20) NOT NULL CHECK (tx_type IN ('OPENING', 'BET', 'WIN', 'LOSS', 'REFUND', 'ROLLBACK')),
    status VARCHAR(30) NOT NULL CHECK (status IN ('PENDING', 'PENDING_REFERENCE', 'PROCESSED', 'REJECTED')),
    failure_code VARCHAR(80),
    result_balance BIGINT NOT NULL CHECK (result_balance >= 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    next_attempt_at TIMESTAMPTZ,
    attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    FOREIGN KEY (wallet_id, currency, player_id) REFERENCES wallets(id, currency, player_id),
    UNIQUE (id, wallet_id, currency),
    UNIQUE (provider_id, external_id),
    UNIQUE (provider_id, idempotency_key),
    CHECK ((tx_type = 'LOSS' AND amount = 0) OR (tx_type <> 'LOSS' AND amount > 0)),
    CHECK ((tx_type IN ('REFUND', 'ROLLBACK')) = (reference_external_id IS NOT NULL)),
    CHECK ((status = 'REJECTED') = (failure_code IS NOT NULL)),
    CHECK ((status = 'PENDING_REFERENCE') = (next_attempt_at IS NOT NULL)),
    CHECK (
        (tx_type = 'OPENING' AND external_id IS NULL AND provider_id IS NULL AND idempotency_key IS NULL AND payload_hash IS NULL AND reference_external_id IS NULL)
        OR (tx_type <> 'OPENING' AND external_id IS NOT NULL AND provider_id IS NOT NULL AND idempotency_key IS NOT NULL AND payload_hash ~ '^[0-9a-f]{64}$')
    )
);
CREATE INDEX wager_transactions_wallet_idx ON wager_transactions(wallet_id, created_at DESC);
CREATE INDEX wager_transactions_reference_idx ON wager_transactions(status, next_attempt_at)
    WHERE status = 'PENDING_REFERENCE';
CREATE UNIQUE INDEX wager_transactions_reference_once_idx
    ON wager_transactions(provider_id, reference_external_id)
    WHERE reference_external_id IS NOT NULL AND status = 'PROCESSED';

CREATE TABLE ledger_entries (
    id UUID PRIMARY KEY,
    wallet_id UUID NOT NULL,
    currency CHAR(3) NOT NULL,
    transaction_id UUID NOT NULL UNIQUE,
    amount BIGINT NOT NULL CHECK (amount <> 0),
    balance_before BIGINT NOT NULL CHECK (balance_before >= 0),
    balance_after BIGINT NOT NULL CHECK (balance_after >= 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    FOREIGN KEY (wallet_id, currency) REFERENCES wallets(id, currency),
    FOREIGN KEY (transaction_id, wallet_id, currency) REFERENCES wager_transactions(id, wallet_id, currency),
    CHECK (balance_after::numeric = balance_before::numeric + amount::numeric)
);
CREATE INDEX ledger_entries_wallet_cursor_idx ON ledger_entries(wallet_id, created_at DESC, id DESC);

CREATE FUNCTION validate_ledger_entry() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    wager wager_transactions%ROWTYPE;
    reference wager_transactions%ROWTYPE;
    expected_amount BIGINT;
    original_amount BIGINT;
    current_balance BIGINT;
BEGIN
    SELECT * INTO wager FROM wager_transactions WHERE id = NEW.transaction_id;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'ledger transaction does not exist';
    END IF;
    IF wager.status <> 'PROCESSED' THEN
        RAISE EXCEPTION 'ledger entry requires a processed transaction';
    END IF;
    IF NEW.wallet_id <> wager.wallet_id OR NEW.currency <> wager.currency THEN
        RAISE EXCEPTION 'ledger entry scope differs from transaction';
    END IF;

    CASE wager.tx_type
        WHEN 'BET' THEN
            expected_amount := -wager.amount;
        WHEN 'WIN', 'OPENING' THEN
            expected_amount := wager.amount;
        WHEN 'REFUND' THEN
            SELECT * INTO reference
            FROM wager_transactions
            WHERE provider_id = wager.provider_id AND external_id = wager.reference_external_id
                AND status = 'PROCESSED';
            IF NOT FOUND THEN
                RAISE EXCEPTION 'refund reference does not exist';
            END IF;
            IF reference.tx_type <> 'BET' OR reference.wallet_id <> wager.wallet_id
                OR reference.player_id <> wager.player_id OR wager.amount <> reference.amount THEN
                RAISE EXCEPTION 'refund reference is invalid or outside transaction scope';
            END IF;
            expected_amount := wager.amount;
        WHEN 'ROLLBACK' THEN
            SELECT * INTO reference
            FROM wager_transactions
            WHERE provider_id = wager.provider_id AND external_id = wager.reference_external_id
                AND status = 'PROCESSED';
            IF NOT FOUND THEN
                RAISE EXCEPTION 'rollback reference does not exist';
            END IF;
            IF reference.wallet_id <> wager.wallet_id OR reference.player_id <> wager.player_id THEN
                RAISE EXCEPTION 'rollback reference is missing or outside transaction scope';
            END IF;
            SELECT amount INTO original_amount FROM ledger_entries WHERE transaction_id = reference.id;
            IF NOT FOUND THEN
                RAISE EXCEPTION 'rollback reference has no financial ledger entry';
            END IF;
            IF wager.amount <> abs(original_amount) THEN
                RAISE EXCEPTION 'rollback amount does not match reference ledger';
            END IF;
            expected_amount := -original_amount;
        ELSE
            RAISE EXCEPTION 'transaction type cannot create a ledger entry';
    END CASE;

    IF NEW.amount <> expected_amount THEN
        RAISE EXCEPTION 'ledger amount differs from transaction effect';
    END IF;
    SELECT balance INTO current_balance FROM wallets WHERE id = NEW.wallet_id;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'ledger wallet does not exist';
    END IF;
    IF NEW.balance_after <> current_balance THEN
        RAISE EXCEPTION 'ledger after-balance differs from wallet';
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER ledger_entries_validate
    BEFORE INSERT ON ledger_entries
    FOR EACH ROW EXECUTE FUNCTION validate_ledger_entry();

CREATE FUNCTION prevent_ledger_mutation() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'ledger entries are append-only';
END;
$$;
CREATE TRIGGER ledger_entries_immutable
    BEFORE UPDATE OR DELETE ON ledger_entries
    FOR EACH ROW EXECUTE FUNCTION prevent_ledger_mutation();
CREATE TRIGGER ledger_entries_no_truncate
    BEFORE TRUNCATE ON ledger_entries
    FOR EACH STATEMENT EXECUTE FUNCTION prevent_ledger_mutation();

CREATE TABLE consumer_inbox (
    message_id VARCHAR(255) PRIMARY KEY,
    payload_hash CHAR(64) NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE outbox_events (
    event_id UUID PRIMARY KEY,
    event_type VARCHAR(100) NOT NULL,
    aggregate_id UUID NOT NULL,
    correlation_id VARCHAR(255) NOT NULL,
    causation_id VARCHAR(255),
    version BIGINT NOT NULL CHECK (version >= 1),
    payload JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at TIMESTAMPTZ,
    locked_until TIMESTAMPTZ,
    attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_error TEXT
);
CREATE INDEX outbox_pending_idx ON outbox_events(next_attempt_at, created_at)
    WHERE published_at IS NULL;
