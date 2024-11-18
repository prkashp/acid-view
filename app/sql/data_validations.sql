Select 
    TO_CHAR(TO_DATE(to_varchar(DATE_KEY), 'YYYYMMDD'), 'YYYY-MM-DD') as DATE,
    UPPER(CATEGORY) as TABLE_NAME, 
    CASE WHEN DESCRIPTION IN ('duplicates','dummy_validation','Import mismatch','Delete mismatch','pkey_comparison','missing_records') THEN INITCAP(DESCRIPTION)
        ELSE 'Custom' END AS CHECK_NAME, 
    PRIORITY,
    DATA_SOURCE,
    INITCAP(TABLE_SCHEMA) as TABLE_SCHEMA,
    IFF(TOTAL_DISCREPANCIES=0,0,1) AS STATUS
FROM ADMIN.DATA_VALIDATIONS
order by date_key desc limit 500000;