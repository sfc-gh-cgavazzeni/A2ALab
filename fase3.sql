--- ============================================================
--- FASE 3: Setup Cortex Search (RAG su Contratti)
--- ============================================================

USE ROLE ACCOUNTADMIN;
USE DATABASE POWERUTILITY;
USE SCHEMA PUBLIC;
USE WAREHOUSE COMPUTE_WH;

--- Creare uno stage per i documenti
CREATE OR REPLACE STAGE POWERUTILITY.PUBLIC.DOCUMENTI_STAGE
    DIRECTORY = (ENABLE = TRUE)
    ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE');

--- NOTA: Caricare il PDF tramite Snowsight UI:
--- Data > POWERUTILITY > PUBLIC > Stages > DOCUMENTI_STAGE > + Files
--- Selezionare: a2a_condizioni_generali_fornitura.pdf

--- Verifica che il file sia stato caricato
LIST @POWERUTILITY.PUBLIC.DOCUMENTI_STAGE;

--- Estrarre il testo dal PDF usando AI_PARSE_DOCUMENT (test)
SELECT
    AI_PARSE_DOCUMENT(
        BUILD_SCOPED_FILE_URL(@DOCUMENTI_STAGE, 'a2a_condizioni_generali_fornitura.pdf'),
        {'mode': 'LAYOUT'}
    ):content::VARCHAR AS TESTO_COMPLETO;

--- Estrarre e suddividere il contenuto in chunk
CREATE OR REPLACE TABLE POWERUTILITY.PUBLIC.CONTRATTO_CHUNKS AS
WITH parsed AS (
    SELECT
        'a2a_condizioni_generali_fornitura.pdf' AS NOME_FILE,
        AI_PARSE_DOCUMENT(
            BUILD_SCOPED_FILE_URL(@DOCUMENTI_STAGE, 'a2a_condizioni_generali_fornitura.pdf'),
            {'mode': 'LAYOUT'}
        ):content::VARCHAR AS TESTO_COMPLETO
),
numbered AS (
    SELECT
        NOME_FILE,
        ROW_NUMBER() OVER (ORDER BY SEQ4()) AS CHUNK_ID,
        'Sezione ' || ROW_NUMBER() OVER (ORDER BY SEQ4()) AS SEZIONE,
        SUBSTR(TESTO_COMPLETO, (ROW_NUMBER() OVER (ORDER BY SEQ4()) - 1) * 2000 + 1, 2000) AS CONTENUTO,
        CEIL(ROW_NUMBER() OVER (ORDER BY SEQ4()) * 2000.0 / 3000) AS PAGINA
    FROM parsed,
    TABLE(GENERATOR(ROWCOUNT => 50))
)
SELECT
    CHUNK_ID,
    NOME_FILE,
    SEZIONE,
    CONTENUTO,
    PAGINA
FROM numbered
WHERE LENGTH(CONTENUTO) > 10;

--- Verificare i chunk creati
SELECT COUNT(*) AS NUM_CHUNKS, AVG(LENGTH(CONTENUTO)) AS AVG_LENGTH
FROM CONTRATTO_CHUNKS;

--- Creare il Cortex Search Service
CREATE OR REPLACE CORTEX SEARCH SERVICE POWERUTILITY.PUBLIC.A2A_CONTRATTO_SEARCH
  ON CONTENUTO
  ATTRIBUTES SEZIONE, NOME_FILE
  WAREHOUSE = COMPUTE_WH
  TARGET_LAG = '1 hour'
  AS (
    SELECT
        CONTENUTO,
        SEZIONE,
        NOME_FILE
    FROM POWERUTILITY.PUBLIC.CONTRATTO_CHUNKS
  );

--- Test di ricerca semantica
SELECT PARSE_JSON(
    SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
        'POWERUTILITY.PUBLIC.A2A_CONTRATTO_SEARCH',
        '{
            "query": "Quali sono le condizioni di recesso dal contratto?",
            "columns": ["CONTENUTO", "SEZIONE"],
            "limit": 3
        }'
    )
) AS RISULTATI;
