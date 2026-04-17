--- ============================================================
--- FASE 2: Setup Cortex Analyst (Semantic View)
--- ============================================================

USE ROLE ACCOUNTADMIN;
USE DATABASE POWERUTILITY;
USE SCHEMA PUBLIC;

--- Creazione della Semantic View per Cortex Analyst
CREATE OR REPLACE SEMANTIC VIEW POWERUTILITY.PUBLIC.A2A_ENERGY_SEMANTIC_VIEW2

  TABLES (
    CLIENTI AS POWERUTILITY.PUBLIC.CLIENTI
      PRIMARY KEY (CLIENTE_ID)
      WITH SYNONYMS = ('clienti', 'anagrafica clienti', 'utenti')
      COMMENT = 'Anagrafica clienti A2A Energia con dati contrattuali e di contatto',

    CONSUMI_ENERGIA AS POWERUTILITY.PUBLIC.CONSUMI_ENERGIA
      PRIMARY KEY (CONSUMO_ID)
      WITH SYNONYMS = ('consumi', 'bollette', 'consumi mensili')
      COMMENT = 'Consumi mensili di energia elettrica e gas per cliente con dettaglio costi per fascia oraria',

    FEEDBACK_CLIENTI AS POWERUTILITY.PUBLIC.FEEDBACK_CLIENTI
      PRIMARY KEY (FEEDBACK_ID)
      WITH SYNONYMS = ('feedback', 'ticket', 'reclami', 'segnalazioni', 'assistenza')
      COMMENT = 'Ticket di assistenza e feedback dei clienti con tracciamento stato e tempi di risoluzione',

    IMPIANTI AS POWERUTILITY.PUBLIC.IMPIANTI
      PRIMARY KEY (IMPIANTO_ID)
      WITH SYNONYMS = ('impianti', 'centrali', 'centrali elettriche')
      COMMENT = 'Anagrafica impianti di produzione energia: fotovoltaico eolico idroelettrico termoelettrico cogenerazione',

    PRODUZIONE_ENERGIA AS POWERUTILITY.PUBLIC.PRODUZIONE_ENERGIA
      PRIMARY KEY (PRODUZIONE_ID)
      WITH SYNONYMS = ('produzione', 'generazione energia', 'output impianti')
      COMMENT = 'Dati giornalieri di produzione energia per impianto con costi e prezzi di vendita'
  )

  RELATIONSHIPS (
    CONSUMI_TO_CLIENTI AS
      CONSUMI_ENERGIA (CLIENTE_ID) REFERENCES CLIENTI (CLIENTE_ID),
    FEEDBACK_TO_CLIENTI AS
      FEEDBACK_CLIENTI (CLIENTE_ID) REFERENCES CLIENTI (CLIENTE_ID),
    PRODUZIONE_TO_IMPIANTI AS
      PRODUZIONE_ENERGIA (IMPIANTO_ID) REFERENCES IMPIANTI (IMPIANTO_ID)
  )

  FACTS (
    CLIENTI.POTENZA_IMPEGNATA_KW AS POTENZA_IMPEGNATA_KW
      COMMENT = 'Potenza contrattuale impegnata in kilowatt',

    CONSUMI_ENERGIA.CONSUMO_KWH AS CONSUMO_KWH
      COMMENT = 'Consumo di energia elettrica in kilowattora nel mese',
    CONSUMI_ENERGIA.CONSUMO_SMC AS CONSUMO_SMC
      COMMENT = 'Consumo di gas in standard metri cubi nel mese',
    CONSUMI_ENERGIA.COSTO_ENERGIA_EUR AS COSTO_ENERGIA_EUR
      COMMENT = 'Costo della componente energia in euro',
    CONSUMI_ENERGIA.COSTO_TRASPORTO_EUR AS COSTO_TRASPORTO_EUR
      COMMENT = 'Costo del trasporto e distribuzione in euro',
    CONSUMI_ENERGIA.COSTO_ONERI_EUR AS COSTO_ONERI_EUR
      COMMENT = 'Costo degli oneri di sistema in euro',
    CONSUMI_ENERGIA.COSTO_TOTALE_EUR AS COSTO_TOTALE_EUR
      WITH SYNONYMS = ('importo', 'spesa', 'bolletta', 'costo totale')
      COMMENT = 'Costo totale mensile in euro (energia + trasporto + oneri)',

    FEEDBACK_CLIENTI.TEMPO_RISOLUZIONE_ORE AS TEMPO_RISOLUZIONE_ORE
      WITH SYNONYMS = ('tempo risoluzione', 'SLA', 'ore risoluzione')
      COMMENT = 'Tempo impiegato per risolvere il ticket in ore',

    IMPIANTI.CAPACITA_MW AS CAPACITA_MW
      COMMENT = 'Capacita nominale dell impianto in megawatt',
    IMPIANTI.EFFICIENZA_PERCENTUALE AS EFFICIENZA_PERCENTUALE
      COMMENT = 'Efficienza operativa dell impianto in percentuale',
    IMPIANTI.EMISSIONI_CO2_TON AS EMISSIONI_CO2_TON
      WITH SYNONYMS = ('emissioni', 'CO2', 'anidride carbonica')
      COMMENT = 'Emissioni di CO2 in tonnellate',

    PRODUZIONE_ENERGIA.ENERGIA_PRODOTTA_MWH AS ENERGIA_PRODOTTA_MWH
      WITH SYNONYMS = ('produzione MWh', 'energia generata')
      COMMENT = 'Energia prodotta giornalmente in megawattora',
    PRODUZIONE_ENERGIA.ORE_FUNZIONAMENTO AS ORE_FUNZIONAMENTO
      COMMENT = 'Ore di funzionamento dell impianto nel giorno',
    PRODUZIONE_ENERGIA.DISPONIBILITA_PERCENTUALE AS DISPONIBILITA_PERCENTUALE
      COMMENT = 'Percentuale di disponibilita dell impianto nel giorno',
    PRODUZIONE_ENERGIA.COSTO_PRODUZIONE_EUR AS COSTO_PRODUZIONE_EUR
      COMMENT = 'Costo di produzione giornaliero in euro',
    PRODUZIONE_ENERGIA.PREZZO_VENDITA_MWH_EUR AS PREZZO_VENDITA_MWH_EUR
      COMMENT = 'Prezzo di vendita per MWh in euro'
  )

  DIMENSIONS (
    CLIENTI.CLIENTE_ID AS CLIENTE_ID
      COMMENT = 'Identificativo univoco del cliente',
    CLIENTI.NOME AS NOME
      WITH SYNONYMS = ('nome cliente', 'first name')
      COMMENT = 'Nome del cliente',
    CLIENTI.COGNOME AS COGNOME
      WITH SYNONYMS = ('cognome cliente', 'last name')
      COMMENT = 'Cognome del cliente',
    CLIENTI.CODICE_FISCALE AS CODICE_FISCALE
      WITH SYNONYMS = ('CF', 'codice fiscale')
      COMMENT = 'Codice fiscale del cliente',
    CLIENTI.EMAIL AS EMAIL
      COMMENT = 'Indirizzo email del cliente',
    CLIENTI.TELEFONO AS TELEFONO
      COMMENT = 'Numero di telefono del cliente',
    CLIENTI.INDIRIZZO AS INDIRIZZO
      COMMENT = 'Indirizzo di residenza del cliente',
    CLIENTI.CITTA AS CITTA
      WITH SYNONYMS = ('citta', 'comune', 'localita cliente')
      COMMENT = 'Citta di residenza del cliente',
    CLIENTI.PROVINCIA AS PROVINCIA
      COMMENT = 'Provincia del cliente (sigla)',
    CLIENTI.CAP AS CAP
      WITH SYNONYMS = ('codice postale', 'codice avviamento postale')
      COMMENT = 'Codice di avviamento postale',
    CLIENTI.TIPO_CLIENTE AS TIPO_CLIENTE
      WITH SYNONYMS = ('tipologia cliente', 'tipo utenza')
      COMMENT = 'Tipologia: RESIDENZIALE, BUSINESS, CONDOMINIO',
    CLIENTI.TIPO_CONTRATTO AS TIPO_CONTRATTO
      WITH SYNONYMS = ('tipo fornitura', 'contratto')
      COMMENT = 'Tipo di contratto: LUCE, GAS, DUAL',
    CLIENTI.STATO_CONTRATTO AS STATO_CONTRATTO
      WITH SYNONYMS = ('stato cliente', 'stato fornitura')
      COMMENT = 'Stato del contratto: ATTIVO, CESSATO, SOSPESO',
    CLIENTI.SEGMENTO AS SEGMENTO
      WITH SYNONYMS = ('segmento cliente', 'fascia')
      COMMENT = 'Segmento commerciale: BASE, STANDARD, PREMIUM',
    CLIENTI.DATA_ATTIVAZIONE AS DATA_ATTIVAZIONE
      WITH SYNONYMS = ('data registrazione', 'data iscrizione', 'data contratto')
      COMMENT = 'Data di attivazione del contratto',

    CONSUMI_ENERGIA.CONSUMO_ID AS CONSUMO_ID
      COMMENT = 'Identificativo univoco del record di consumo',
    CONSUMI_ENERGIA.MESE AS MESE
      WITH SYNONYMS = ('mese consumo', 'periodo', 'mese di riferimento')
      COMMENT = 'Mese di riferimento del consumo (primo giorno del mese)',
    CONSUMI_ENERGIA.FASCIA_ORARIA AS FASCIA_ORARIA
      WITH SYNONYMS = ('fascia', 'banda oraria', 'F1 F2 F3')
      COMMENT = 'Fascia oraria: F1 (lun-ven 8-19) F2 (lun-ven 7-8 e 19-23 sab 7-23) F3 (notte e festivi)',
    CONSUMI_ENERGIA.FONTE_ENERGIA AS FONTE_ENERGIA
      WITH SYNONYMS = ('fonte', 'tipo energia', 'sorgente')
      COMMENT = 'Fonte di energia: RETE, FOTOVOLTAICO, MISTO',
    CONSUMI_ENERGIA.POD AS POD
      WITH SYNONYMS = ('punto di prelievo', 'POD energia elettrica')
      COMMENT = 'Point of Delivery - codice punto di prelievo energia elettrica',
    CONSUMI_ENERGIA.PDR AS PDR
      WITH SYNONYMS = ('punto di riconsegna', 'PDR gas')
      COMMENT = 'Punto di Riconsegna - codice punto fornitura gas',

    FEEDBACK_CLIENTI.FEEDBACK_ID AS FEEDBACK_ID
      COMMENT = 'Identificativo univoco del ticket/feedback',
    FEEDBACK_CLIENTI.DATA_FEEDBACK AS DATA_FEEDBACK
      WITH SYNONYMS = ('data ticket', 'data segnalazione', 'data apertura')
      COMMENT = 'Data e ora di apertura del ticket',
    FEEDBACK_CLIENTI.CANALE AS CANALE
      WITH SYNONYMS = ('canale contatto', 'mezzo di contatto')
      COMMENT = 'Canale di contatto: EMAIL, TELEFONO, APP, SOCIAL, SPORTELLO',
    FEEDBACK_CLIENTI.CATEGORIA AS CATEGORIA
      WITH SYNONYMS = ('tipo problema', 'categoria ticket', 'motivo')
      COMMENT = 'Categoria del feedback: FATTURAZIONE, GUASTI, CONTRATTO, INFORMAZIONI, RECLAMO',
    FEEDBACK_CLIENTI.TESTO_FEEDBACK AS TESTO_FEEDBACK
      WITH SYNONYMS = ('descrizione', 'testo', 'messaggio')
      COMMENT = 'Testo libero del feedback del cliente',
    FEEDBACK_CLIENTI.PRIORITA AS PRIORITA
      WITH SYNONYMS = ('urgenza', 'livello priorita')
      COMMENT = 'Livello di priorita: ALTA, MEDIA, BASSA',
    FEEDBACK_CLIENTI.STATO_TICKET AS STATO_TICKET
      WITH SYNONYMS = ('stato feedback', 'stato segnalazione')
      COMMENT = 'Stato del ticket: APERTO, IN_LAVORAZIONE, RISOLTO, CHIUSO',
    FEEDBACK_CLIENTI.OPERATORE AS OPERATORE
      WITH SYNONYMS = ('agente', 'operatore call center')
      COMMENT = 'Nome dell operatore che gestisce il ticket',

    IMPIANTI.IMPIANTO_ID AS IMPIANTO_ID
      COMMENT = 'Identificativo univoco dell impianto',
    IMPIANTI.NOME_IMPIANTO AS NOME_IMPIANTO
      WITH SYNONYMS = ('nome centrale', 'impianto')
      COMMENT = 'Nome dell impianto di produzione',
    IMPIANTI.TIPO AS TIPO
      WITH SYNONYMS = ('tipo centrale', 'tecnologia', 'fonte rinnovabile')
      COMMENT = 'Tipo di impianto: FOTOVOLTAICO, EOLICO, IDROELETTRICO, TERMOELETTRICO, COGENERAZIONE',
    IMPIANTI.LOCALITA AS LOCALITA
      WITH SYNONYMS = ('ubicazione', 'dove si trova')
      COMMENT = 'Localita dove si trova l impianto',
    IMPIANTI.REGIONE AS REGIONE
      COMMENT = 'Regione in cui si trova l impianto',
    IMPIANTI.STATO_IMPIANTO AS STATO
      WITH SYNONYMS = ('stato operativo')
      COMMENT = 'Stato dell impianto: OPERATIVO, MANUTENZIONE',
    IMPIANTI.DATA_ENTRATA_SERVIZIO AS DATA_ENTRATA_SERVIZIO
      WITH SYNONYMS = ('data avvio', 'data commissioning')
      COMMENT = 'Data di entrata in servizio dell impianto',

    PRODUZIONE_ENERGIA.PRODUZIONE_ID AS PRODUZIONE_ID
      COMMENT = 'Identificativo univoco del record di produzione',
    PRODUZIONE_ENERGIA.DATA_PRODUZIONE AS DATA_PRODUZIONE
      WITH SYNONYMS = ('data generazione', 'giorno produzione')
      COMMENT = 'Data del giorno di produzione'
  )

  METRICS (
    CONSUMI_ENERGIA.CONSUMO_TOTALE_KWH AS SUM(CONSUMI_ENERGIA.CONSUMO_KWH)
      WITH SYNONYMS = ('consumo totale elettricita', 'kWh totali')
      COMMENT = 'Consumo totale di energia elettrica in kWh',
    CONSUMI_ENERGIA.CONSUMO_TOTALE_SMC AS SUM(CONSUMI_ENERGIA.CONSUMO_SMC)
      WITH SYNONYMS = ('consumo totale gas', 'smc totali')
      COMMENT = 'Consumo totale di gas in standard metri cubi',
    CONSUMI_ENERGIA.FATTURATO_TOTALE AS SUM(CONSUMI_ENERGIA.COSTO_TOTALE_EUR)
      WITH SYNONYMS = ('ricavo totale', 'fatturato', 'spesa totale')
      COMMENT = 'Fatturato totale in euro',
    CONSUMI_ENERGIA.COSTO_MEDIO_PER_KWH AS SUM(CONSUMI_ENERGIA.COSTO_ENERGIA_EUR) / NULLIF(SUM(CONSUMI_ENERGIA.CONSUMO_KWH), 0)
      WITH SYNONYMS = ('prezzo medio kWh', 'costo unitario energia')
      COMMENT = 'Costo medio per kWh di energia elettrica in euro',
    CONSUMI_ENERGIA.NUMERO_FORNITURE AS COUNT(CONSUMI_ENERGIA.CONSUMO_ID)
      COMMENT = 'Numero totale di record di consumo',
    CONSUMI_ENERGIA.CONSUMO_MEDIO_KWH AS AVG(CONSUMI_ENERGIA.CONSUMO_KWH)
      WITH SYNONYMS = ('consumo medio elettricita')
      COMMENT = 'Consumo medio mensile di energia elettrica in kWh',

    CLIENTI.NUMERO_CLIENTI AS COUNT(CLIENTI.CLIENTE_ID)
      WITH SYNONYMS = ('totale clienti', 'conteggio clienti')
      COMMENT = 'Numero totale di clienti',

    FEEDBACK_CLIENTI.NUMERO_TICKET AS COUNT(FEEDBACK_CLIENTI.FEEDBACK_ID)
      WITH SYNONYMS = ('totale ticket', 'numero segnalazioni')
      COMMENT = 'Numero totale di ticket aperti',
    FEEDBACK_CLIENTI.TEMPO_MEDIO_RISOLUZIONE AS AVG(FEEDBACK_CLIENTI.TEMPO_RISOLUZIONE_ORE)
      WITH SYNONYMS = ('SLA medio', 'tempo medio di gestione')
      COMMENT = 'Tempo medio di risoluzione ticket in ore',

    PRODUZIONE_ENERGIA.PRODUZIONE_TOTALE_MWH AS SUM(PRODUZIONE_ENERGIA.ENERGIA_PRODOTTA_MWH)
      WITH SYNONYMS = ('produzione totale', 'energia totale generata')
      COMMENT = 'Produzione totale di energia in MWh',
    PRODUZIONE_ENERGIA.COSTO_PRODUZIONE_TOTALE AS SUM(PRODUZIONE_ENERGIA.COSTO_PRODUZIONE_EUR)
      WITH SYNONYMS = ('costo totale produzione')
      COMMENT = 'Costo totale di produzione in euro',
    PRODUZIONE_ENERGIA.RICAVO_POTENZIALE AS SUM(PRODUZIONE_ENERGIA.ENERGIA_PRODOTTA_MWH * PRODUZIONE_ENERGIA.PREZZO_VENDITA_MWH_EUR)
      WITH SYNONYMS = ('ricavo produzione', 'revenue produzione')
      COMMENT = 'Ricavo potenziale dalla vendita di energia prodotta in euro',
    PRODUZIONE_ENERGIA.ORE_MEDIE_FUNZIONAMENTO AS AVG(PRODUZIONE_ENERGIA.ORE_FUNZIONAMENTO)
      COMMENT = 'Media giornaliera delle ore di funzionamento',
    PRODUZIONE_ENERGIA.DISPONIBILITA_MEDIA AS AVG(PRODUZIONE_ENERGIA.DISPONIBILITA_PERCENTUALE)
      WITH SYNONYMS = ('availability media')
      COMMENT = 'Disponibilita media percentuale degli impianti',

    IMPIANTI.CAPACITA_TOTALE_MW AS SUM(IMPIANTI.CAPACITA_MW)
      WITH SYNONYMS = ('capacita installata', 'potenza totale')
      COMMENT = 'Capacita totale installata in MW',
    IMPIANTI.NUMERO_IMPIANTI AS COUNT(IMPIANTI.IMPIANTO_ID)
      COMMENT = 'Numero totale di impianti',

    margine_produzione AS PRODUZIONE_ENERGIA.RICAVO_POTENZIALE - PRODUZIONE_ENERGIA.COSTO_PRODUZIONE_TOTALE
      WITH SYNONYMS = ('margine', 'profitto produzione')
      COMMENT = 'Margine lordo dalla produzione di energia (ricavo - costo)'
  )

  COMMENT = 'Semantic view A2A Energia - Analisi clienti, consumi, feedback, impianti e produzione energetica'

  COPY GRANTS;

--- Verifica creazione
SHOW SEMANTIC VIEWS IN SCHEMA POWERUTILITY.PUBLIC;

--- Descrivi la struttura
DESCRIBE SEMANTIC VIEW POWERUTILITY.PUBLIC.A2A_ENERGY_SEMANTIC_VIEW;