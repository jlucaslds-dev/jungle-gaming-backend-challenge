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

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM wager_transactions
        WHERE tx_type = 'WIN' AND reference_external_id IS NOT NULL
    ) THEN
        RAISE EXCEPTION 'cannot downgrade while referenced WIN transactions exist';
    END IF;
    IF EXISTS (
        SELECT 1
        FROM consumer_inbox
        GROUP BY message_id
        HAVING count(*) > 1
    ) THEN
        RAISE EXCEPTION 'cannot downgrade inbox while message ids are shared by multiple consumers';
    END IF;
END $$;

ALTER TABLE consumer_inbox DROP CONSTRAINT consumer_inbox_pkey;
ALTER TABLE consumer_inbox DROP CONSTRAINT consumer_inbox_payload_hash_check;
ALTER TABLE consumer_inbox ADD COLUMN processed_at TIMESTAMPTZ;
UPDATE consumer_inbox SET processed_at = COALESCE(completed_at, received_at, now());
ALTER TABLE consumer_inbox ALTER COLUMN processed_at SET NOT NULL;
ALTER TABLE consumer_inbox ALTER COLUMN processed_at SET DEFAULT now();
ALTER TABLE consumer_inbox DROP COLUMN completed_at;
ALTER TABLE consumer_inbox DROP COLUMN received_at;
ALTER TABLE consumer_inbox DROP COLUMN consumer_name;
ALTER TABLE consumer_inbox ADD CONSTRAINT consumer_inbox_pkey PRIMARY KEY (message_id);

ALTER TABLE wager_transactions DROP CONSTRAINT wager_transactions_reference_check;
ALTER TABLE wager_transactions
    ADD CONSTRAINT wager_transactions_check1 CHECK (
        (tx_type IN ('REFUND', 'ROLLBACK')) = (reference_external_id IS NOT NULL)
    );
