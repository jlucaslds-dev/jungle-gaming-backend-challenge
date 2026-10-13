DROP INDEX wager_transactions_reference_once_idx;
CREATE UNIQUE INDEX wager_transactions_reference_once_idx
    ON wager_transactions(provider_id, reference_external_id)
    WHERE tx_type IN ('REFUND', 'ROLLBACK')
        AND reference_external_id IS NOT NULL
        AND status = 'PROCESSED';

CREATE FUNCTION validate_wallet_ledger_balance() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    matching_entry_exists BOOLEAN;
BEGIN
    IF TG_OP = 'INSERT' THEN
        IF NEW.balance = 0 THEN
            RETURN NEW;
        END IF;

        SELECT EXISTS (
            SELECT 1
            FROM ledger_entries entry
            JOIN wager_transactions wager ON wager.id = entry.transaction_id
            WHERE entry.wallet_id = NEW.id
                AND entry.currency = NEW.currency
                AND entry.balance_before = 0
                AND entry.balance_after = NEW.balance
                AND entry.amount = NEW.balance
                AND entry.xmin = mod(pg_current_xact_id()::text::numeric, 4294967296)::text::xid
                AND wager.tx_type = 'OPENING'
                AND wager.status = 'PROCESSED'
        ) INTO matching_entry_exists;
    ELSE
        IF NEW.balance = OLD.balance THEN
            RETURN NEW;
        END IF;

        SELECT EXISTS (
            SELECT 1
            FROM ledger_entries entry
            JOIN wager_transactions wager ON wager.id = entry.transaction_id
            WHERE entry.wallet_id = NEW.id
                AND entry.currency = NEW.currency
                AND entry.balance_before = OLD.balance
                AND entry.balance_after = NEW.balance
                AND entry.amount = NEW.balance - OLD.balance
                AND entry.xmin = mod(pg_current_xact_id()::text::numeric, 4294967296)::text::xid
                AND wager.status = 'PROCESSED'
        ) INTO matching_entry_exists;
    END IF;

    IF NOT matching_entry_exists THEN
        RAISE EXCEPTION 'wallet balance change requires a matching processed ledger entry';
    END IF;
    RETURN NEW;
END;
$$;

CREATE CONSTRAINT TRIGGER wallets_balance_requires_ledger
    AFTER INSERT OR UPDATE ON wallets
    DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW EXECUTE FUNCTION validate_wallet_ledger_balance();
