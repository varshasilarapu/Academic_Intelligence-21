{{ config(materialized='table') }}

SELECT 
    a.STUDENT_ID,
    a.STUDENT_NAME,
    a.ACADEMIC_SCORE,
    c.CODING_SCORE,
    -- Detection logic for academic decline
    CASE 
        WHEN a.ACADEMIC_SCORE < 60 AND c.CODING_SCORE < 50 THEN 'High Risk'
        WHEN a.ACADEMIC_SCORE < 75 THEN 'Moderate Risk'
        ELSE 'Stable'
    END AS DROPOUT_RISK_STATUS
FROM {{ source('snowflake_source', 'ACADEMIC_TABLE') }} a
JOIN {{ source('snowflake_source', 'CODING_TABLE') }} c 
ON a.STUDENT_ID = c.STUDENT_ID
