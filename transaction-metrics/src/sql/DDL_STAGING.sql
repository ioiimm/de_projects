DROP TABLE IF EXISTS STV2025042911__STAGING.transactions;
CREATE TABLE IF NOT EXISTS STV2025042911__STAGING.transactions(
    operation_id VARCHAR(60) NOT NULL,
    account_number_from INT NOT NULL,
    account_number_to INT NOT NULL,
    currency_code INT NOT NULL,
    country VARCHAR(30) NOT NULL,
    status VARCHAR(30) NOT NULL,
    transaction_type VARCHAR(30) NOT NULL,
    amount INT NOT NULL,
    transaction_dt TIMESTAMP(3) NOT NULL
)
ORDER BY operation_id, transaction_dt
SEGMENTED BY HASH(operation_id, transaction_dt) ALL NODES
PARTITION BY DATE(transaction_dt::DATE);
DROP TABLE IF EXISTS STV2025042911__STAGING.currencies;
CREATE TABLE IF NOT EXISTS STV2025042911__STAGING.currencies (
	date_update TIMESTAMP(3) NOT NULL,
	currency_code INT NOT NULL,
	currency_code_with INT NOT NULL,
	currency_with_div NUMERIC(5, 3) NOT NULL
)
ORDER BY date_update, currency_code, currency_code_with
SEGMENTED BY HASH(currency_code, date_update) ALL NODES
PARTITION BY DATE(date_update::DATE);