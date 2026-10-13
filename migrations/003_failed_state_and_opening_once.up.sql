DO $$
DECLARE
    constraint_name TEXT;
BEGIN
    SELECT conname INTO constraint_name
    FROM pg_constraint
    WHERE conrelid = 'wager_transactions'::regclass
      AND contype = 'c'
      AND pg_get_constraintdef(oid) LIKE '%status%'
      AND pg_get_constraintdef(oid) LIKE '%REJECTED%'
      AND pg_get_constraintdef(oid) LIKE '%PENDING_REFERENCE%'
    LIMIT 1;
    IF constraint_name IS NULL THEN
        RAISE EXCEPTION 'wager transaction status constraint not found';
    END IF;
    EXECUTE format('ALTER TABLE wager_transactions DROP CONSTRAINT %I', constraint_name);

    SELECT conname INTO constraint_name
    FROM pg_constraint
    WHERE conrelid = 'wager_transactions'::regclass
      AND contype = 'c'
      AND pg_get_constraintdef(oid) LIKE '%failure_code%'
      AND pg_get_constraintdef(oid) LIKE '%REJECTED%'
    LIMIT 1;
    IF constraint_name IS NULL THEN
        RAISE EXCEPTION 'wager transaction failure-code constraint not found';
    END IF;
    EXECUTE format('ALTER TABLE wager_transactions DROP CONSTRAINT %I', constraint_name);
END $$;

ALTER TABLE wager_transactions
    ADD CONSTRAINT wager_transactions_status_check
    CHECK (status IN ('PENDING', 'PENDING_REFERENCE', 'PROCESSED', 'REJECTED', 'FAILED'));
ALTER TABLE wager_transactions
    ADD CONSTRAINT wager_transactions_failure_status_check
    CHECK ((status IN ('REJECTED', 'FAILED')) = (failure_code IS NOT NULL));
ALTER TABLE wager_transactions
    ADD CONSTRAINT wager_transactions_opening_processed_check
    CHECK (tx_type <> 'OPENING' OR status = 'PROCESSED');
CREATE UNIQUE INDEX wager_transactions_opening_once_idx
    ON wager_transactions(wallet_id) WHERE tx_type = 'OPENING';