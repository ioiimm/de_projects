MERGE INTO STV2025042911__DWH.global_metrics AS gm
USING (
	WITH accounts AS (
	SELECT
		transaction_dt::DATE AS date_update,
		account_number_from,
		currency_code AS currency_from,
		SUM(amount) AS amount,
		COUNT(*) AS cnt_transactions
	FROM STV2025042911__STAGING.transactions
	WHERE account_number_from > 0
		AND account_number_to > 0
		AND transaction_dt::DATE = CURRENT_DATE - 1
	GROUP BY transaction_dt::DATE, currency_code, account_number_from
	)
	SELECT
		a.date_update,
		a.currency_from,
		SUM(a.amount * c.currency_with_div) AS amount_total,
		SUM(a.cnt_transactions) AS cnt_transactions,
		AVG(a.cnt_transactions) AS avg_transactions_per_account,
		COUNT(DISTINCT a.account_number_from) AS cnt_accounts_make_transactions
	FROM accounts AS a
	INNER JOIN STV2025042911__STAGING.currencies AS c
		ON a.date_update = c.date_update::DATE
		AND a.currency_from = c.currency_code
	GROUP BY a.date_update, a.currency_from
	ORDER BY a.date_update, a.currency_from
	) AS info
ON gm.date_update = info.date_update
	AND gm.currency_from = info.currency_from
WHEN MATCHED THEN UPDATE
SET
	amount_total = info.amount_total,
	cnt_transactions = info.cnt_transactions,
	avg_transactions_per_account = info.avg_transactions_per_account,
	cnt_accounts_make_transactions = info.cnt_accounts_make_transactions
WHEN NOT MATCHED THEN
INSERT (
	date_update,
	currency_from,
	amount_total,
	cnt_transactions,
	avg_transactions_per_account,
	cnt_accounts_make_transactions
	)
VALUES (
	info.date_update,
	info.currency_from,
	info.amount_total,
	info.cnt_transactions,
	info.avg_transactions_per_account,
	info.cnt_accounts_make_transactions
	);