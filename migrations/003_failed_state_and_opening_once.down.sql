DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM wager_transactions WHERE status = 'FAILED') THEN
        RAISE EXCEPTION 'cannot roll back FAILED transactions without losing state';
    END IF;
END $$;

DROP INDEX wager_transactions_opening_once_idx;
ALTER TABLE wager_transactions DROP CONSTRAINT wager_transactions_opening_processed_check;
ALTER TABLE wager_transactions DROP CONSTRAINT wager_transactions_failure_status_check;
ALTER TABLE wager_transactions DROP CONSTRAINT wager_transactions_status_check;
ALTER TABLE wager_transactions
    ADD CONSTRAINT wager_transactions_status_check
    CHECK (status IN ('PENDING', 'PENDING_REFERENCE', 'PROCESSED', 'REJECTED'));
ALTER TABLE wager_transactions
    ADD CONSTRAINT wager_transactions_failure_status_check
    CHECK ((status = 'REJECTED') = (failure_code IS NOT NULL));