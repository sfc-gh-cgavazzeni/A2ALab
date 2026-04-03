--- ============================================================
--- FASE 2: Setup Cortex Analyst (Semantic View)
--- ============================================================

USE ROLE ACCOUNTADMIN;
USE DATABASE POWERUTILITY;
USE SCHEMA PUBLIC;

--- Creazione della Semantic View per Cortex Analyst
CREATE OR REPLACE SEMANTIC VIEW A2A_ENERGY_SEMANTIC_VIEW
AS SEMANTIC MODEL
  TABLES (
    --- TABELLA CLIENTI
    POWERUTILITY.PUBLIC.CLIENTI AS CLIENTI
      PRIMARY KEY (CLIENTE_ID)
      WITH COLUMNS (
        CLIENTE_ID
          DATA_TYPE NUMBER
          DESCRIPTION 'Identificativo univoco del cliente',
        NOME
          DATA_TYPE VARCHAR
          DESCRIPTION 'Nome del cliente',
        COGNOME
          DATA_TYPE VARCHAR
          DESCRIPTION 'Cognome del cliente',
        CITTA
          DATA_TYPE VARCHAR
          DESCRIPTION 'Citta di residenza del cliente',
        PROVINCIA
          DATA_TYPE VARCHAR
          DESCRIPTION 'Sigla provincia del cliente',
        TIPO_CLIENTE
          DATA_TYPE VARCHAR
          DESCRIPTION 'Tipologia di cliente: RESIDENZIALE, BUSINESS o CONDOMINIO',
        TIPO_CONTRATTO
          DATA_TYPE VARCHAR
          DESCRIPTION 'Tipo di contratto energetico: LUCE, GAS o DUAL (luce+gas)',
        POTENZA_IMPEGNATA_KW
          DATA_TYPE NUMBER
          DESCRIPTION 'Potenza elettrica impegnata in kilowatt',
        DATA_ATTIVAZIONE
          DATA_TYPE DATE
          DESCRIPTION 'Data di attivazione del contratto',
        STATO_CONTRATTO
          DATA_TYPE VARCHAR
          DESCRIPTION 'Stato attuale del contratto: ATTIVO, SOSPESO o CESSATO',
        SEGMENTO
          DATA_TYPE VARCHAR
          DESCRIPTION 'Segmento commerciale del cliente: PREMIUM, STANDARD o BASE'
      ),

    --- TABELLA CONSUMI
    POWERUTILITY.PUBLIC.CONSUMI_ENERGIA AS CONSUMI
      PRIMARY KEY (CONSUMO_ID)
      WITH COLUMNS (
        CONSUMO_ID
          DATA_TYPE NUMBER
          DESCRIPTION 'Identificativo univoco del record di consumo',
        CLIENTE_ID
          DATA_TYPE NUMBER
          DESCRIPTION 'Riferimento al cliente',
        MESE
          DATA_TYPE DATE
          DESCRIPTION 'Mese di riferimento del consumo',
        CONSUMO_KWH
          DATA_TYPE NUMBER
          DESCRIPTION 'Consumo di energia elettrica in kilowattora',
        CONSUMO_SMC
          DATA_TYPE NUMBER
          DESCRIPTION 'Consumo di gas naturale in standard metri cubi',
        COSTO_ENERGIA_EUR
          DATA_TYPE NUMBER
          DESCRIPTION 'Costo della componente energia in euro',
        COSTO_TRASPORTO_EUR
          DATA_TYPE NUMBER
          DESCRIPTION 'Costo del trasporto e distribuzione in euro',
        COSTO_ONERI_EUR
          DATA_TYPE NUMBER
          DESCRIPTION 'Costo degli oneri di sistema in euro',
        COSTO_TOTALE_EUR
          DATA_TYPE NUMBER
          DESCRIPTION 'Costo totale della bolletta in euro',
        FASCIA_ORARIA
          DATA_TYPE VARCHAR
          DESCRIPTION 'Fascia oraria di consumo: F1 (lun-ven 8-19), F2 (lun-ven 7-8/19-23, sab 7-23), F3 (notte e festivi)',
        FONTE_ENERGIA
          DATA_TYPE VARCHAR
          DESCRIPTION 'Fonte di approvvigionamento energetico: RETE, FOTOVOLTAICO o MISTO'
      ),

    --- TABELLA FEEDBACK
    POWERUTILITY.PUBLIC.FEEDBACK_CLIENTI AS FEEDBACK
      PRIMARY KEY (FEEDBACK_ID)
      WITH COLUMNS (
        FEEDBACK_ID
          DATA_TYPE NUMBER
          DESCRIPTION 'Identificativo univoco del feedback',
        CLIENTE_ID
          DATA_TYPE NUMBER
          DESCRIPTION 'Riferimento al cliente',
        DATA_FEEDBACK
          DATA_TYPE TIMESTAMP_NTZ
          DESCRIPTION 'Data e ora di ricezione del feedback',
        CANALE
          DATA_TYPE VARCHAR
          DESCRIPTION 'Canale di ricezione: EMAIL, TELEFONO, APP, SOCIAL o SPORTELLO',
        CATEGORIA
          DATA_TYPE VARCHAR
          DESCRIPTION 'Categoria del feedback: FATTURAZIONE, GUASTI, CONTRATTO, INFORMAZIONI o RECLAMO',
        TESTO_FEEDBACK
          DATA_TYPE VARCHAR
          DESCRIPTION 'Testo completo del feedback del cliente',
        PRIORITA
          DATA_TYPE VARCHAR
          DESCRIPTION 'Livello di priorita: ALTA, MEDIA o BASSA',
        STATO_TICKET
          DATA_TYPE VARCHAR
          DESCRIPTION 'Stato del ticket: APERTO, IN_LAVORAZIONE, RISOLTO o CHIUSO',
        TEMPO_RISOLUZIONE_ORE
          DATA_TYPE NUMBER
          DESCRIPTION 'Tempo impiegato per risolvere il ticket in ore'
      ),

    --- TABELLA IMPIANTI
    POWERUTILITY.PUBLIC.IMPIANTI AS IMPIANTI
      PRIMARY KEY (IMPIANTO_ID)
      WITH COLUMNS (
        IMPIANTO_ID
          DATA_TYPE NUMBER
          DESCRIPTION 'Identificativo univoco dell impianto',
        NOME_IMPIANTO
          DATA_TYPE VARCHAR
          DESCRIPTION 'Nome dell impianto di produzione energia',
        TIPO
          DATA_TYPE VARCHAR
          DESCRIPTION 'Tipologia impianto: TERMOELETTRICO, IDROELETTRICO, FOTOVOLTAICO, EOLICO o COGENERAZIONE',
        LOCALITA
          DATA_TYPE VARCHAR
          DESCRIPTION 'Localita dove si trova l impianto',
        REGIONE
          DATA_TYPE VARCHAR
          DESCRIPTION 'Regione italiana dell impianto',
        CAPACITA_MW
          DATA_TYPE NUMBER
          DESCRIPTION 'Capacita nominale dell impianto in megawatt',
        STATO
          DATA_TYPE VARCHAR
          DESCRIPTION 'Stato operativo: OPERATIVO, MANUTENZIONE o FERMO',
        EFFICIENZA_PERCENTUALE
          DATA_TYPE NUMBER
          DESCRIPTION 'Efficienza operativa dell impianto in percentuale',
        EMISSIONI_CO2_TON
          DATA_TYPE NUMBER
          DESCRIPTION 'Emissioni annuali di CO2 in tonnellate'
      ),

    --- TABELLA PRODUZIONE
    POWERUTILITY.PUBLIC.PRODUZIONE_ENERGIA AS PRODUZIONE
      PRIMARY KEY (PRODUZIONE_ID)
      WITH COLUMNS (
        PRODUZIONE_ID
          DATA_TYPE NUMBER
          DESCRIPTION 'Identificativo univoco del record di produzione',
        IMPIANTO_ID
          DATA_TYPE NUMBER
          DESCRIPTION 'Riferimento all impianto',
        DATA_PRODUZIONE
          DATA_TYPE DATE
          DESCRIPTION 'Data di produzione energia',
        ENERGIA_PRODOTTA_MWH
          DATA_TYPE NUMBER
          DESCRIPTION 'Energia prodotta in megawattora',
        ORE_FUNZIONAMENTO
          DATA_TYPE NUMBER
          DESCRIPTION 'Ore di funzionamento dell impianto nel giorno',
        DISPONIBILITA_PERCENTUALE
          DATA_TYPE NUMBER
          DESCRIPTION 'Percentuale di disponibilita dell impianto',
        COSTO_PRODUZIONE_EUR
          DATA_TYPE NUMBER
          DESCRIPTION 'Costo di produzione in euro',
        PREZZO_VENDITA_MWH_EUR
          DATA_TYPE NUMBER
          DESCRIPTION 'Prezzo di vendita dell energia in euro per MWh'
      )
  )

  RELATIONSHIPS (
    CONSUMI (CLIENTE_ID) REFERENCES CLIENTI (CLIENTE_ID)
      DESCRIPTION 'Ogni record di consumo appartiene ad un cliente',
    FEEDBACK (CLIENTE_ID) REFERENCES CLIENTI (CLIENTE_ID)
      DESCRIPTION 'Ogni feedback e'' associato ad un cliente',
    PRODUZIONE (IMPIANTO_ID) REFERENCES IMPIANTI (IMPIANTO_ID)
      DESCRIPTION 'Ogni record di produzione appartiene ad un impianto'
  );

--- Verifica creazione
SHOW SEMANTIC VIEWS IN SCHEMA POWERUTILITY.PUBLIC;

--- Descrivi la struttura
DESCRIBE SEMANTIC VIEW POWERUTILITY.PUBLIC.A2A_ENERGY_SEMANTIC_VIEW;
