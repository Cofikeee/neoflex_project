INSERT INTO ds.md_ledger_account_s (
    chapter,
    chapter_name,
    section_number,
    section_name,
    subsection_name,
    ledger1_account,
    ledger1_account_name,
    ledger_account,
    ledger_account_name,
    characteristic,
    start_date,
    end_date
)
SELECT DISTINCT
    "CHAPTER",
    "CHAPTER_NAME",
    "SECTION_NUMBER",
    "SECTION_NAME",
    "SUBSECTION_NAME",
    "LEDGER1_ACCOUNT",
    "LEDGER1_ACCOUNT_NAME",
    "LEDGER_ACCOUNT",
    "LEDGER_ACCOUNT_NAME",
    "CHARACTERISTIC",
    "START_DATE"::DATE,
    "END_DATE"::DATE
FROM stg.md_ledger_account_s
WHERE "LEDGER_ACCOUNT" IS NOT NULL
ON CONFLICT (ledger_account, start_date) DO UPDATE
SET chapter = excluded.chapter,
    chapter_name = excluded.chapter_name,
    section_number = excluded.section_number,
    section_name = excluded.section_name,
    subsection_name = excluded.subsection_name,
    ledger1_account = excluded.ledger1_account,
    ledger1_account_name = excluded.ledger1_account_name,
    ledger_account_name = excluded.ledger_account_name,
    characteristic = excluded.characteristic,
    end_date = excluded.end_date;