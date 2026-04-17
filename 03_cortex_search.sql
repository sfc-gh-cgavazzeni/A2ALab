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
--- Catalog > Database Explorer > POWERUTILITY > PUBLIC > Stages > DOCUMENTI_STAGE > + Files
--- Selezionare: a2a_condizioni_generali_fornitura.pdf

--- Verifica che il file sia stato caricato
LIST @POWERUTILITY.PUBLIC.DOCUMENTI_STAGE;

--- Estrarre il testo dal PDF usando AI_PARSE_DOCUMENT (test)
--- I modelli di ricerca e i LLM lavorano su testo, non su file binari come i PDF.
--- Per poter indicizzare e cercare il contenuto di un documento, dobbiamo prima
--- estrarne il testo. AI_PARSE_DOCUMENT analizza il layout del PDF e restituisce
--- il contenuto testuale strutturato, preservando l'ordine di lettura.
SELECT
    AI_PARSE_DOCUMENT(
        TO_FILE('@DOCUMENTI_STAGE', 'a2a_condizioni_generali_fornitura.pdf'),
        {'mode': 'LAYOUT'}
    ):content::VARCHAR AS TESTO_COMPLETO;

--- Estrarre e suddividere il contenuto in chunk
--- Un "chunk" e' un frammento di testo di dimensione controllata. La suddivisione
--- in chunk e' necessaria perche' i modelli di embedding hanno un limite massimo di
--- token in input, e perche' chunk piu' piccoli permettono risultati di ricerca piu'
--- precisi e pertinenti. La dimensione ideale del chunk dipende dal caso d'uso:
--- - Chunk piccoli (~500 caratteri): alta precisione, ma rischio di perdere contesto
--- - Chunk medi (~1000-2000 caratteri): buon compromesso tra precisione e contesto
--- - Chunk grandi (~3000-4000 caratteri): piu' contesto, ma meno precisione nella ricerca
--- In questo lab usiamo chunk da 2000 caratteri, un buon compromesso per documenti contrattuali.
CREATE OR REPLACE TABLE POWERUTILITY.PUBLIC.CONTRATTO_CHUNKS AS
WITH parsed AS (
    SELECT
        'a2a_condizioni_generali_fornitura.pdf' AS NOME_FILE,
        AI_PARSE_DOCUMENT(
            TO_FILE('@DOCUMENTI_STAGE', 'a2a_condizioni_generali_fornitura.pdf'),
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

--- NOTA: Questa operazione puo' richiedere fino a 10 minuti perche' AI_PARSE_DOCUMENT
--- deve analizzare il PDF pagina per pagina, estrarre il testo e ricostruire il layout.

--- Verificare i chunk creati
SELECT COUNT(*) AS NUM_CHUNKS, AVG(LENGTH(CONTENUTO)) AS AVG_LENGTH
FROM CONTRATTO_CHUNKS;

--- Creare il Cortex Search Service
--- Quando si crea un Cortex Search Service, Snowflake:
--- 1. Genera gli embedding vettoriali per ogni chunk di testo usando un modello interno
--- 2. Costruisce un indice vettoriale ottimizzato per la ricerca semantica
--- 3. Espone un endpoint API che accetta query in linguaggio naturale
--- 4. Mantiene l'indice aggiornato automaticamente in base al TARGET_LAG configurato
--- Il risultato e' un servizio di ricerca semantica pronto all'uso, integrabile
--- direttamente con Cortex Agent come strumento di retrieval (RAG).
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
