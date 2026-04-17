--- ============================================================
--- FASE 7: Notebook AISQL - Laboratorio Pratico
--- ============================================================

--- NOTA: Creare un Notebook Snowflake da Snowsight:
--- Notebooks > + Notebook > Name: A2A_AISQL_Lab
--- Database: POWERUTILITY, Schema: PUBLIC, Warehouse: COMPUTE_WH

--- ============================================================
--- CELLA 2 (SQL): Setup
--- ============================================================
USE DATABASE POWERUTILITY;
USE SCHEMA PUBLIC;
USE WAREHOUSE COMPUTE_WH;

--- ============================================================
--- CELLA 4 (SQL): Sentiment Analysis sui Feedback
--- ============================================================
SELECT
    FEEDBACK_ID,
    CANALE,
    CATEGORIA,
    SUBSTR(TESTO_FEEDBACK, 1, 80) || '...' AS TESTO_BREVE,
    ROUND(SNOWFLAKE.CORTEX.SENTIMENT(TESTO_FEEDBACK), 3) AS SENTIMENT_SCORE,
    CASE
        WHEN SNOWFLAKE.CORTEX.SENTIMENT(TESTO_FEEDBACK) > 0.3 THEN 'POSITIVO'
        WHEN SNOWFLAKE.CORTEX.SENTIMENT(TESTO_FEEDBACK) < -0.3 THEN 'NEGATIVO'
        ELSE 'NEUTRO'
    END AS SENTIMENT_LABEL
FROM FEEDBACK_CLIENTI
ORDER BY SENTIMENT_SCORE ASC
LIMIT 20;

--- ============================================================
--- CELLA 5 (SQL): Statistiche Sentiment per Categoria
--- ============================================================
SELECT
    CATEGORIA,
    COUNT(*) AS NUM_FEEDBACK,
    ROUND(AVG(SNOWFLAKE.CORTEX.SENTIMENT(TESTO_FEEDBACK)), 3) AS AVG_SENTIMENT,
    ROUND(MIN(SNOWFLAKE.CORTEX.SENTIMENT(TESTO_FEEDBACK)), 3) AS MIN_SENTIMENT,
    ROUND(MAX(SNOWFLAKE.CORTEX.SENTIMENT(TESTO_FEEDBACK)), 3) AS MAX_SENTIMENT,
    SUM(CASE WHEN SNOWFLAKE.CORTEX.SENTIMENT(TESTO_FEEDBACK) < -0.3 THEN 1 ELSE 0 END) AS NEGATIVI,
    SUM(CASE WHEN SNOWFLAKE.CORTEX.SENTIMENT(TESTO_FEEDBACK) > 0.3 THEN 1 ELSE 0 END) AS POSITIVI
FROM FEEDBACK_CLIENTI
GROUP BY CATEGORIA
ORDER BY AVG_SENTIMENT ASC;

--- ============================================================
--- CELLA 7 (SQL): Riassunto Documenti Tecnici
--- ============================================================
SELECT
    TITOLO,
    TIPO_DOCUMENTO,
    AREA,
    SNOWFLAKE.CORTEX.SUMMARIZE(CONTENUTO) AS RIASSUNTO
FROM DOCUMENTI_TECNICI
WHERE TIPO_DOCUMENTO IN ('REPORT', 'PROCEDURA')
LIMIT 5;

--- ============================================================
--- CELLA 9 (SQL): Traduzione Feedback in inglese e tedesco
--- ============================================================
SELECT
    FEEDBACK_ID,
    TESTO_FEEDBACK AS ORIGINALE_IT,
    SNOWFLAKE.CORTEX.TRANSLATE(TESTO_FEEDBACK, 'it', 'en') AS TRADUZIONE_EN,
    SNOWFLAKE.CORTEX.TRANSLATE(TESTO_FEEDBACK, 'it', 'de') AS TRADUZIONE_DE
FROM FEEDBACK_CLIENTI
LIMIT 5;

--- ============================================================
--- CELLA 11 (SQL): Analisi Avanzata con AI_COMPLETE
--- ============================================================
SELECT
    FEEDBACK_ID,
    CATEGORIA,
    TESTO_FEEDBACK,
    SNOWFLAKE.CORTEX.COMPLETE(
        'mistral-large2',
        CONCAT(
            'Sei un esperto di customer care nel settore energetico italiano. ',
            'Analizza il seguente feedback di un cliente A2A e fornisci: ',
            '1) Classificazione urgenza (CRITICA/ALTA/MEDIA/BASSA) ',
            '2) Azione suggerita in una frase. ',
            'Feedback: ', TESTO_FEEDBACK
        )
    ) AS ANALISI_AI
FROM FEEDBACK_CLIENTI
WHERE PRIORITA = 'ALTA'
LIMIT 5;

--- ============================================================
--- CELLA 12 (SQL): Generazione Report Energetico con AI_COMPLETE
--- ============================================================
WITH stats AS (
    SELECT
        ROUND(SUM(CONSUMO_KWH), 0) AS TOT_KWH,
        ROUND(SUM(CONSUMO_SMC), 0) AS TOT_SMC,
        ROUND(SUM(COSTO_TOTALE_EUR), 0) AS TOT_COSTO,
        ROUND(AVG(COSTO_TOTALE_EUR), 2) AS AVG_COSTO,
        COUNT(DISTINCT CLIENTE_ID) AS NUM_CLIENTI
    FROM CONSUMI_ENERGIA
    WHERE MESE >= DATEADD('month', -3, CURRENT_DATE())
)
SELECT SNOWFLAKE.CORTEX.COMPLETE(
    'mistral-large2',
    CONCAT(
        'Genera un breve report in italiano (max 200 parole) sui consumi energetici A2A degli ultimi 3 mesi. ',
        'Dati: Consumo totale elettricita'' = ', TOT_KWH::VARCHAR, ' kWh, ',
        'Consumo totale gas = ', TOT_SMC::VARCHAR, ' smc, ',
        'Costo totale = ', TOT_COSTO::VARCHAR, ' EUR, ',
        'Costo medio per cliente = ', AVG_COSTO::VARCHAR, ' EUR, ',
        'Clienti attivi = ', NUM_CLIENTI::VARCHAR, '. ',
        'Includi trend e raccomandazioni.'
    )
) AS REPORT_ENERGETICO
FROM stats;

--- ============================================================
--- CELLA 14 (SQL): Classificazione Automatica Feedback
--- ============================================================
SELECT
    FEEDBACK_ID,
    TESTO_FEEDBACK,
    SNOWFLAKE.CORTEX.CLASSIFY_TEXT(
        TESTO_FEEDBACK,
        ['Problema tecnico urgente', 'Richiesta informazioni', 'Reclamo commerciale', 'Complimento', 'Richiesta modifica contratto']
    ):label::VARCHAR AS CLASSIFICAZIONE,
    ROUND(SNOWFLAKE.CORTEX.CLASSIFY_TEXT(
        TESTO_FEEDBACK,
        ['Problema tecnico urgente', 'Richiesta informazioni', 'Reclamo commerciale', 'Complimento', 'Richiesta modifica contratto']
    ):score::FLOAT, 3) AS CONFIDENCE
FROM FEEDBACK_CLIENTI
LIMIT 15;

--- ============================================================
--- CELLA 16 (SQL): Estrazione Entita' dai Feedback
--- ============================================================
SELECT
    FEEDBACK_ID,
    TESTO_FEEDBACK,
    SNOWFLAKE.CORTEX.EXTRACT_ANSWER(
        TESTO_FEEDBACK,
        'Qual e'' il problema principale segnalato dal cliente?'
    ) AS PROBLEMA_PRINCIPALE,
    SNOWFLAKE.CORTEX.EXTRACT_ANSWER(
        TESTO_FEEDBACK,
        'Il cliente menziona una localita'' specifica?'
    ) AS LOCALITA_MENZIONATA
FROM FEEDBACK_CLIENTI
WHERE CATEGORIA = 'GUASTI'
LIMIT 10;
