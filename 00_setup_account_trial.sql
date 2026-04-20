--- ============================================================
--- FASE 0: Creazione Account Trial Snowflake
--- ============================================================
--- Questa fase non richiede codice SQL.
--- 
--- Istruzioni:
--- 1. Aprire https://signup.snowflake.com/
--- 2. Compilare il form (Email aziendale, Company: A2A Energia)
--- 3. Selezionare: Edition = Business Critical, Cloud = AWS, Region = Europe (Frankfurt)
--- 4. Attivare l'account tramite il link ricevuto via email
--- 5. Accedere a Snowsight
--- ============================================================

CREATE OR REPLACE API INTEGRATION MY_GIT_API_INTEGRATION
    API_PROVIDER = GIT_HTTPS_API
    API_ALLOWED_PREFIXES = ('https://github.com/sfc-gh-cgavazzeni')
    ENABLED = TRUE;

