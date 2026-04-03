--- ============================================================
--- FASE 6: Introduzione a Cortex AI SQL
--- ============================================================

USE DATABASE POWERUTILITY;
USE SCHEMA PUBLIC;
USE WAREHOUSE COMPUTE_WH;

--- SENTIMENT ANALYSIS sui feedback
SELECT
    FEEDBACK_ID,
    TESTO_FEEDBACK,
    SNOWFLAKE.CORTEX.SENTIMENT(TESTO_FEEDBACK) AS SENTIMENT_SCORE
FROM FEEDBACK_CLIENTI
LIMIT 10;

--- SUMMARIZE di un documento tecnico
SELECT
    TITOLO,
    SNOWFLAKE.CORTEX.SUMMARIZE(CONTENUTO) AS RIASSUNTO
FROM DOCUMENTI_TECNICI
LIMIT 3;

--- TRANSLATE feedback in inglese
SELECT
    TESTO_FEEDBACK,
    SNOWFLAKE.CORTEX.TRANSLATE(TESTO_FEEDBACK, 'it', 'en') AS ENGLISH_TRANSLATION
FROM FEEDBACK_CLIENTI
LIMIT 5;

--- AI_COMPLETE: prompt generico per analisi feedback
SELECT SNOWFLAKE.CORTEX.COMPLETE(
    'mistral-large2',
    'Analizza questo feedback di un cliente utility e suggerisci un''azione: '
    || TESTO_FEEDBACK
) AS AZIONE_SUGGERITA
FROM FEEDBACK_CLIENTI
LIMIT 3;
