DO $$
DECLARE
    constraint_name TEXT;
BEGIN
    FOR constraint_name IN
        SELECT conname
        FROM pg_constraint
        WHERE conrelid = 'wager_transactions'::regclass
          AND contype = 'c'
          AND pg_get_constraintdef(oid) LIKE '%reference_external_id%'
    LOOP
        EXECUTE format('ALTER TABLE wager_transactions DROP CONSTRAINT %I', constraint_name);
    END LOOP;

    ALTER TABLE wager_transactions
        ADD CONSTRAINT wager_transactions_reference_check CHECK (
            (tx_type IN ('REFUND', 'ROLLBACK') AND reference_external_id IS NOT NULL)
            OR tx_type = 'WIN'
            OR (tx_type NOT IN ('REFUND', 'ROLLBACK', 'WIN') AND reference_external_id IS NULL)
        );
END $$;

ALTER TABLE consumer_inbox DROP CONSTRAINT consumer_inbox_pkey;
ALTER TABLE consumer_inbox ADD COLUMN consumer_name VARCHAR(100) CHECK (consumer_name <> '');
UPDATE consumer_inbox SET consumer_name = 'wager-transactions';
ALTER TABLE consumer_inbox ALTER COLUMN consumer_name SET NOT NULL;
ALTER TABLE consumer_inbox ADD COLUMN received_at TIMESTAMPTZ;
ALTER TABLE consumer_inbox ADD COLUMN completed_at TIMESTAMPTZ;
UPDATE consumer_inbox SET received_at = processed_at, completed_at = processed_at;
ALTER TABLE consumer_inbox
    ADD CONSTRAINT consumer_inbox_payload_hash_check CHECK (payload_hash ~ '^[0-9a-f]{64}$');
ALTER TABLE consumer_inbox ALTER COLUMN received_at SET NOT NULL;
ALTER TABLE consumer_inbox ALTER COLUMN received_at SET DEFAULT now();
ALTER TABLE consumer_inbox DROP COLUMN processed_at;
ALTER TABLE consumer_inbox
    ADD CONSTRAINT consumer_inbox_pkey PRIMARY KEY (consumer_name, message_id);

CREATE OR REPLACE FUNCTION validate_ledger_entry() RETURNS trigger LANGUAGE plpgsql AS $$
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
        WHEN 'WIN' THEN
            IF wager.reference_external_id IS NOT NULL THEN
                SELECT * INTO reference
                FROM wager_transactions
                WHERE provider_id = wager.provider_id AND external_id = wager.reference_external_id
                    AND status = 'PROCESSED';
                IF NOT FOUND OR reference.tx_type <> 'BET' OR reference.wallet_id <> wager.wallet_id
                    OR reference.player_id <> wager.player_id OR reference.round_id <> wager.round_id
                    OR reference.currency <> wager.currency THEN
                    RAISE EXCEPTION 'win reference is invalid or outside transaction scope';
                END IF;
            END IF;
            expected_amount := wager.amount;
        WHEN 'OPENING' THEN
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
                OR reference.player_id <> wager.player_id OR reference.round_id <> wager.round_id
                OR reference.currency <> wager.currency OR wager.amount <> reference.amount THEN
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
            IF reference.wallet_id <> wager.wallet_id OR reference.player_id <> wager.player_id
                OR reference.round_id <> wager.round_id OR reference.currency <> wager.currency
                OR reference.tx_type NOT IN ('BET', 'WIN', 'REFUND') THEN
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
