DROP TRIGGER wallets_balance_requires_ledger ON wallets;
DROP FUNCTION validate_wallet_ledger_balance();

DROP INDEX wager_transactions_reference_once_idx;
CREATE UNIQUE INDEX wager_transactions_reference_once_idx
    ON wager_transactions(provider_id, reference_external_id)
    WHERE reference_external_id IS NOT NULL AND status = 'PROCESSED';
