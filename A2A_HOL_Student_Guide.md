# Hands-On Lab: Snowflake AI per il Settore Energy
## A2A Energia - Guida Studente

**Durata:** 2 ore  
**Audience:** Analyst, Data Engineer, Data Scientist, Solution Architect  
**Database:** POWERUTILITY | **Schema:** PUBLIC

---

## Agenda

| Blocco | Durata | Argomento |
|--------|--------|-----------|
| 1 | 75 min | **Snowflake Intelligence** - Setup piattaforma agentica, query NLP, automazione workflow |
| 2 | 45 min | **Cortex AI SQL** - Operatori SQL nativi per task LLM, sentiment analysis, estrazione insights |

---

# FASE 0: Creazione Account Trial Snowflake

## 0.1 Registrazione

1. Aprire il browser e navigare su: **https://signup.snowflake.com/**
2. Compilare il form con i propri dati:
   - **First Name / Last Name**
   - **Email** (usare email aziendale)
   - **Company:** A2A Energia
   - **Country:** Italy
3. Cliccare **Continue**
4. Nella schermata successiva selezionare:
   - **Snowflake Edition:** `Business Critical`
   - **Cloud Provider:** `Amazon Web Services`
   - **Region:** `Europe (Frankfurt) - eu-central-1`
5. Accettare i termini e cliccare **Get Started**
6. Controllare la propria email e cliccare sul link di attivazione
7. Impostare username e password
8. Accedere a Snowsight (la web UI di Snowflake)

> **Nota:** Il trial dura 30 giorni e include $400 di crediti gratuiti. La region Frankfurt garantisce bassa latenza per utenti in Italia e compliance GDPR.

---

# FASE 1: Creazione Database e Dati Sintetici

## 1.1 Setup Iniziale

Aprire un **SQL Worksheet** in Snowsight ed eseguire il seguente script.

```sql
--------------------------------------------------------------
-- SETUP INIZIALE
--------------------------------------------------------------
USE ROLE ACCOUNTADMIN;

CREATE WAREHOUSE IF NOT EXISTS COMPUTE_WH
    WAREHOUSE_SIZE = 'XSMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE;

CREATE DATABASE IF NOT EXISTS POWERUTILITY;
USE DATABASE POWERUTILITY;
CREATE SCHEMA IF NOT EXISTS PUBLIC;
USE SCHEMA PUBLIC;
USE WAREHOUSE COMPUTE_WH;
```

## 1.2 Creazione Tabelle

```sql
--------------------------------------------------------------
-- TABELLA: CLIENTI
--------------------------------------------------------------
CREATE OR REPLACE TABLE CLIENTI (
    CLIENTE_ID NUMBER AUTOINCREMENT PRIMARY KEY,
    NOME VARCHAR(100),
    COGNOME VARCHAR(100),
    CODICE_FISCALE VARCHAR(16),
    EMAIL VARCHAR(200),
    TELEFONO VARCHAR(20),
    INDIRIZZO VARCHAR(300),
    CITTA VARCHAR(100),
    PROVINCIA VARCHAR(5),
    CAP VARCHAR(5),
    TIPO_CLIENTE VARCHAR(20),       -- RESIDENZIALE, BUSINESS, CONDOMINIO
    TIPO_CONTRATTO VARCHAR(30),     -- LUCE, GAS, DUAL
    POTENZA_IMPEGNATA_KW NUMBER(10,2),
    DATA_ATTIVAZIONE DATE,
    STATO_CONTRATTO VARCHAR(20),    -- ATTIVO, SOSPESO, CESSATO
    SEGMENTO VARCHAR(30)            -- PREMIUM, STANDARD, BASE
);

--------------------------------------------------------------
-- TABELLA: CONSUMI_ENERGIA
--------------------------------------------------------------
CREATE OR REPLACE TABLE CONSUMI_ENERGIA (
    CONSUMO_ID NUMBER AUTOINCREMENT PRIMARY KEY,
    CLIENTE_ID NUMBER REFERENCES CLIENTI(CLIENTE_ID),
    MESE DATE,
    CONSUMO_KWH NUMBER(12,2),
    CONSUMO_SMC NUMBER(12,2),       -- Standard Metri Cubi (gas)
    COSTO_ENERGIA_EUR NUMBER(12,2),
    COSTO_TRASPORTO_EUR NUMBER(12,2),
    COSTO_ONERI_EUR NUMBER(12,2),
    COSTO_TOTALE_EUR NUMBER(12,2),
    FASCIA_ORARIA VARCHAR(5),       -- F1, F2, F3
    FONTE_ENERGIA VARCHAR(20),      -- RETE, FOTOVOLTAICO, MISTO
    POD VARCHAR(20),
    PDR VARCHAR(20)
);

--------------------------------------------------------------
-- TABELLA: FEEDBACK_CLIENTI
--------------------------------------------------------------
CREATE OR REPLACE TABLE FEEDBACK_CLIENTI (
    FEEDBACK_ID NUMBER AUTOINCREMENT PRIMARY KEY,
    CLIENTE_ID NUMBER REFERENCES CLIENTI(CLIENTE_ID),
    DATA_FEEDBACK TIMESTAMP_NTZ,
    CANALE VARCHAR(30),             -- EMAIL, TELEFONO, APP, SOCIAL, SPORTELLO
    CATEGORIA VARCHAR(50),          -- FATTURAZIONE, GUASTI, CONTRATTO, INFORMAZIONI, RECLAMO
    TESTO_FEEDBACK VARCHAR(2000),
    PRIORITA VARCHAR(10),           -- ALTA, MEDIA, BASSA
    STATO_TICKET VARCHAR(20),       -- APERTO, IN_LAVORAZIONE, RISOLTO, CHIUSO
    TEMPO_RISOLUZIONE_ORE NUMBER(10,2),
    OPERATORE VARCHAR(100)
);

--------------------------------------------------------------
-- TABELLA: DOCUMENTI_TECNICI
--------------------------------------------------------------
CREATE OR REPLACE TABLE DOCUMENTI_TECNICI (
    DOCUMENTO_ID NUMBER AUTOINCREMENT PRIMARY KEY,
    TITOLO VARCHAR(300),
    TIPO_DOCUMENTO VARCHAR(50),     -- NORMATIVA, PROCEDURA, MANUALE, REPORT, CIRCOLARE
    CONTENUTO VARCHAR(5000),
    DATA_PUBBLICAZIONE DATE,
    AUTORE VARCHAR(100),
    AREA VARCHAR(50),               -- DISTRIBUZIONE, GENERAZIONE, TRADING, COMMERCIALE
    TAG VARCHAR(500)
);

--------------------------------------------------------------
-- TABELLA: IMPIANTI
--------------------------------------------------------------
CREATE OR REPLACE TABLE IMPIANTI (
    IMPIANTO_ID NUMBER AUTOINCREMENT PRIMARY KEY,
    NOME_IMPIANTO VARCHAR(200),
    TIPO VARCHAR(50),               -- TERMOELETTRICO, IDROELETTRICO, FOTOVOLTAICO, EOLICO, COGENERAZIONE
    LOCALITA VARCHAR(200),
    REGIONE VARCHAR(50),
    CAPACITA_MW NUMBER(10,2),
    STATO VARCHAR(20),              -- OPERATIVO, MANUTENZIONE, FERMO
    DATA_ENTRATA_SERVIZIO DATE,
    EFFICIENZA_PERCENTUALE NUMBER(5,2),
    EMISSIONI_CO2_TON NUMBER(12,2)
);

--------------------------------------------------------------
-- TABELLA: PRODUZIONE_ENERGIA
--------------------------------------------------------------
CREATE OR REPLACE TABLE PRODUZIONE_ENERGIA (
    PRODUZIONE_ID NUMBER AUTOINCREMENT PRIMARY KEY,
    IMPIANTO_ID NUMBER REFERENCES IMPIANTI(IMPIANTO_ID),
    DATA_PRODUZIONE DATE,
    ENERGIA_PRODOTTA_MWH NUMBER(12,2),
    ORE_FUNZIONAMENTO NUMBER(5,2),
    DISPONIBILITA_PERCENTUALE NUMBER(5,2),
    COSTO_PRODUZIONE_EUR NUMBER(12,2),
    PREZZO_VENDITA_MWH_EUR NUMBER(10,2)
);
```

## 1.3 Inserimento Dati Sintetici

```sql
--------------------------------------------------------------
-- INSERT: CLIENTI (200 record)
--------------------------------------------------------------
INSERT INTO CLIENTI (NOME, COGNOME, CODICE_FISCALE, EMAIL, TELEFONO, INDIRIZZO, CITTA, PROVINCIA, CAP, TIPO_CLIENTE, TIPO_CONTRATTO, POTENZA_IMPEGNATA_KW, DATA_ATTIVAZIONE, STATO_CONTRATTO, SEGMENTO)
SELECT
    CASE MOD(SEQ4(), 20)
        WHEN 0 THEN 'Marco' WHEN 1 THEN 'Giulia' WHEN 2 THEN 'Alessandro' WHEN 3 THEN 'Francesca'
        WHEN 4 THEN 'Luca' WHEN 5 THEN 'Sara' WHEN 6 THEN 'Andrea' WHEN 7 THEN 'Elena'
        WHEN 8 THEN 'Roberto' WHEN 9 THEN 'Chiara' WHEN 10 THEN 'Giovanni' WHEN 11 THEN 'Maria'
        WHEN 12 THEN 'Paolo' WHEN 13 THEN 'Anna' WHEN 14 THEN 'Stefano' WHEN 15 THEN 'Laura'
        WHEN 16 THEN 'Davide' WHEN 17 THEN 'Valentina' WHEN 18 THEN 'Matteo' ELSE 'Silvia'
    END,
    CASE MOD(SEQ4(), 15)
        WHEN 0 THEN 'Rossi' WHEN 1 THEN 'Bianchi' WHEN 2 THEN 'Colombo' WHEN 3 THEN 'Ferrari'
        WHEN 4 THEN 'Russo' WHEN 5 THEN 'Romano' WHEN 6 THEN 'Gallo' WHEN 7 THEN 'Conti'
        WHEN 8 THEN 'Esposito' WHEN 9 THEN 'Ricci' WHEN 10 THEN 'Bruno' WHEN 11 THEN 'Greco'
        WHEN 12 THEN 'Moretti' WHEN 13 THEN 'Barbieri' ELSE 'Lombardi'
    END,
    -- Codice Fiscale generato secondo lo standard italiano (CCCNNN AALMGG ZXXX K)
    -- Parte cognome (3 consonanti)
    CASE MOD(SEQ4(), 15)
        WHEN 0 THEN 'RSS' WHEN 1 THEN 'BNC' WHEN 2 THEN 'CLM' WHEN 3 THEN 'FRR'
        WHEN 4 THEN 'RSS' WHEN 5 THEN 'RMN' WHEN 6 THEN 'GLL' WHEN 7 THEN 'CNT'
        WHEN 8 THEN 'SPT' WHEN 9 THEN 'RCC' WHEN 10 THEN 'BRN' WHEN 11 THEN 'GRC'
        WHEN 12 THEN 'MRT' WHEN 13 THEN 'BRB' ELSE 'LMB'
    END ||
    -- Parte nome (3 consonanti/vocali)
    CASE MOD(SEQ4(), 20)
        WHEN 0 THEN 'MRC' WHEN 1 THEN 'GLI' WHEN 2 THEN 'LSN' WHEN 3 THEN 'FRN'
        WHEN 4 THEN 'LCU' WHEN 5 THEN 'SRA' WHEN 6 THEN 'NDR' WHEN 7 THEN 'LNE'
        WHEN 8 THEN 'RRT' WHEN 9 THEN 'CHR' WHEN 10 THEN 'GNN' WHEN 11 THEN 'MRA'
        WHEN 12 THEN 'PLA' WHEN 13 THEN 'NNA' WHEN 14 THEN 'SFN' WHEN 15 THEN 'LRA'
        WHEN 16 THEN 'DVD' WHEN 17 THEN 'VNT' WHEN 18 THEN 'MTT' ELSE 'SLV'
    END ||
    -- Anno nascita (2 cifre, persone tra 25 e 70 anni)
    LPAD(MOD(56 + MOD(SEQ4() * 13, 45), 100)::VARCHAR, 2, '0') ||
    -- Mese nascita (lettera codice mese italiano)
    SUBSTR('ABCDEHLMPRST', MOD(SEQ4() * 7, 12) + 1, 1) ||
    -- Giorno nascita (01-31, donne nomi dispari +40)
    LPAD((MOD(SEQ4() * 3, 28) + 1 +
        CASE WHEN MOD(SEQ4(), 20) IN (1,3,5,7,9,11,13,15,17,19) THEN 40 ELSE 0 END
    )::VARCHAR, 2, '0') ||
    -- Codice catastale comune (reale per le 8 citta')
    CASE MOD(SEQ4(), 8)
        WHEN 0 THEN 'F205' WHEN 1 THEN 'B157' WHEN 2 THEN 'A794' WHEN 3 THEN 'C933'
        WHEN 4 THEN 'D150' WHEN 5 THEN 'L682' WHEN 6 THEN 'F704' ELSE 'G388'
    END ||
    -- Carattere di controllo (simulato deterministico)
    SUBSTR('ABCDEFGHIJKLMNOPQRSTUVWXYZ', MOD(SEQ4() * 17, 26) + 1, 1),
    LOWER(
        CASE MOD(SEQ4(), 20)
            WHEN 0 THEN 'marco' WHEN 1 THEN 'giulia' WHEN 2 THEN 'alessandro' WHEN 3 THEN 'francesca'
            WHEN 4 THEN 'luca' WHEN 5 THEN 'sara' WHEN 6 THEN 'andrea' WHEN 7 THEN 'elena'
            WHEN 8 THEN 'roberto' WHEN 9 THEN 'chiara' WHEN 10 THEN 'giovanni' WHEN 11 THEN 'maria'
            WHEN 12 THEN 'paolo' WHEN 13 THEN 'anna' WHEN 14 THEN 'stefano' WHEN 15 THEN 'laura'
            WHEN 16 THEN 'davide' WHEN 17 THEN 'valentina' WHEN 18 THEN 'matteo' ELSE 'silvia'
        END || '.' ||
        CASE MOD(SEQ4(), 15)
            WHEN 0 THEN 'rossi' WHEN 1 THEN 'bianchi' WHEN 2 THEN 'colombo' WHEN 3 THEN 'ferrari'
            WHEN 4 THEN 'russo' WHEN 5 THEN 'romano' WHEN 6 THEN 'gallo' WHEN 7 THEN 'conti'
            WHEN 8 THEN 'esposito' WHEN 9 THEN 'ricci' WHEN 10 THEN 'bruno' WHEN 11 THEN 'greco'
            WHEN 12 THEN 'moretti' WHEN 13 THEN 'barbieri' ELSE 'lombardi'
        END || '@email.it'
    ),
    '+39 02 ' || LPAD(MOD(SEQ4() * 7919, 9999999)::VARCHAR, 7, '0'),
    'Via ' || CASE MOD(SEQ4(), 10)
        WHEN 0 THEN 'Roma' WHEN 1 THEN 'Milano' WHEN 2 THEN 'Garibaldi' WHEN 3 THEN 'Dante'
        WHEN 4 THEN 'Mazzini' WHEN 5 THEN 'Verdi' WHEN 6 THEN 'Cavour' WHEN 7 THEN 'Marconi'
        WHEN 8 THEN 'Leopardi' ELSE 'Volta'
    END || ', ' || (MOD(SEQ4(), 150) + 1)::VARCHAR,
    CASE MOD(SEQ4(), 8)
        WHEN 0 THEN 'Milano' WHEN 1 THEN 'Brescia' WHEN 2 THEN 'Bergamo' WHEN 3 THEN 'Como'
        WHEN 4 THEN 'Cremona' WHEN 5 THEN 'Varese' WHEN 6 THEN 'Monza' ELSE 'Pavia'
    END,
    CASE MOD(SEQ4(), 8)
        WHEN 0 THEN 'MI' WHEN 1 THEN 'BS' WHEN 2 THEN 'BG' WHEN 3 THEN 'CO'
        WHEN 4 THEN 'CR' WHEN 5 THEN 'VA' WHEN 6 THEN 'MB' ELSE 'PV'
    END,
    CASE MOD(SEQ4(), 8)
        WHEN 0 THEN '20100' WHEN 1 THEN '25100' WHEN 2 THEN '24100' WHEN 3 THEN '22100'
        WHEN 4 THEN '26100' WHEN 5 THEN '21100' WHEN 6 THEN '20900' ELSE '27100'
    END,
    CASE MOD(SEQ4(), 3) WHEN 0 THEN 'RESIDENZIALE' WHEN 1 THEN 'BUSINESS' ELSE 'CONDOMINIO' END,
    CASE MOD(SEQ4(), 3) WHEN 0 THEN 'LUCE' WHEN 1 THEN 'GAS' ELSE 'DUAL' END,
    CASE MOD(SEQ4(), 3) WHEN 0 THEN 3.0 WHEN 1 THEN 4.5 ELSE 6.0 END,
    DATEADD('day', -MOD(SEQ4() * 37, 1800), CURRENT_DATE()),
    CASE MOD(SEQ4(), 10) WHEN 0 THEN 'SOSPESO' WHEN 1 THEN 'CESSATO' ELSE 'ATTIVO' END,
    CASE MOD(SEQ4(), 3) WHEN 0 THEN 'PREMIUM' WHEN 1 THEN 'STANDARD' ELSE 'BASE' END
FROM TABLE(GENERATOR(ROWCOUNT => 200));

--------------------------------------------------------------
-- INSERT: CONSUMI_ENERGIA (2400 record - 12 mesi x 200 clienti)
--------------------------------------------------------------
INSERT INTO CONSUMI_ENERGIA (CLIENTE_ID, MESE, CONSUMO_KWH, CONSUMO_SMC, COSTO_ENERGIA_EUR, COSTO_TRASPORTO_EUR, COSTO_ONERI_EUR, COSTO_TOTALE_EUR, FASCIA_ORARIA, FONTE_ENERGIA, POD, PDR)
SELECT
    c.CLIENTE_ID,
    DATE_TRUNC('month', DATEADD('month', -m.M, CURRENT_DATE())),
    ROUND(UNIFORM(80, 600, RANDOM()) * 
        CASE WHEN m.M BETWEEN 5 AND 8 THEN 1.3    -- estate: condizionatori
             WHEN m.M BETWEEN 0 AND 2 THEN 1.4     -- inverno: riscaldamento
             ELSE 1.0 END *
        CASE c.TIPO_CLIENTE WHEN 'BUSINESS' THEN 3.0 WHEN 'CONDOMINIO' THEN 5.0 ELSE 1.0 END
    , 2),
    CASE WHEN c.TIPO_CONTRATTO IN ('GAS', 'DUAL') THEN
        ROUND(UNIFORM(20, 300, RANDOM()) * 
            CASE WHEN m.M BETWEEN 0 AND 3 THEN 2.5  -- inverno: riscaldamento gas
                 WHEN m.M BETWEEN 9 AND 11 THEN 1.8
                 ELSE 0.3 END *
            CASE c.TIPO_CLIENTE WHEN 'BUSINESS' THEN 2.5 WHEN 'CONDOMINIO' THEN 4.0 ELSE 1.0 END
        , 2)
    ELSE 0 END,
    0, 0, 0, 0,
    CASE MOD(UNIFORM(1, 100, RANDOM()), 3) WHEN 0 THEN 'F1' WHEN 1 THEN 'F2' ELSE 'F3' END,
    CASE MOD(UNIFORM(1, 100, RANDOM()), 5) WHEN 0 THEN 'FOTOVOLTAICO' WHEN 1 THEN 'MISTO' ELSE 'RETE' END,
    'IT001E' || LPAD(c.CLIENTE_ID::VARCHAR, 8, '0'),
    CASE WHEN c.TIPO_CONTRATTO IN ('GAS', 'DUAL') THEN 'IT0003' || LPAD(c.CLIENTE_ID::VARCHAR, 8, '0') ELSE NULL END
FROM CLIENTI c
CROSS JOIN (SELECT ROW_NUMBER() OVER (ORDER BY SEQ4()) - 1 AS M FROM TABLE(GENERATOR(ROWCOUNT => 12))) m;

-- Calcolo costi realistici
UPDATE CONSUMI_ENERGIA SET
    COSTO_ENERGIA_EUR = ROUND(CONSUMO_KWH * UNIFORM(0.18, 0.35, RANDOM()) + CONSUMO_SMC * UNIFORM(0.80, 1.40, RANDOM()), 2),
    COSTO_TRASPORTO_EUR = ROUND((CONSUMO_KWH * 0.03) + (CONSUMO_SMC * 0.05), 2),
    COSTO_ONERI_EUR = ROUND((CONSUMO_KWH * 0.02) + (CONSUMO_SMC * 0.03), 2);

UPDATE CONSUMI_ENERGIA SET
    COSTO_TOTALE_EUR = COSTO_ENERGIA_EUR + COSTO_TRASPORTO_EUR + COSTO_ONERI_EUR;

--------------------------------------------------------------
-- INSERT: FEEDBACK_CLIENTI (500 record)
--------------------------------------------------------------
INSERT INTO FEEDBACK_CLIENTI (CLIENTE_ID, DATA_FEEDBACK, CANALE, CATEGORIA, TESTO_FEEDBACK, PRIORITA, STATO_TICKET, TEMPO_RISOLUZIONE_ORE, OPERATORE)
SELECT
    UNIFORM(1, 200, RANDOM()),
    DATEADD('hour', -UNIFORM(1, 8760, RANDOM()), CURRENT_TIMESTAMP()),
    CASE MOD(SEQ4(), 5) WHEN 0 THEN 'EMAIL' WHEN 1 THEN 'TELEFONO' WHEN 2 THEN 'APP' WHEN 3 THEN 'SOCIAL' ELSE 'SPORTELLO' END,
    CASE MOD(SEQ4(), 5) WHEN 0 THEN 'FATTURAZIONE' WHEN 1 THEN 'GUASTI' WHEN 2 THEN 'CONTRATTO' WHEN 3 THEN 'INFORMAZIONI' ELSE 'RECLAMO' END,
    CASE MOD(SEQ4(), 25)
        WHEN 0 THEN 'Ho ricevuto una bolletta con un importo molto superiore al solito. Vorrei una verifica dei consumi reali.'
        WHEN 1 THEN 'Servizio eccellente! Il tecnico e'' stato puntuale e molto professionale nella riparazione.'
        WHEN 2 THEN 'Da tre giorni ho un''interruzione di corrente intermittente. Chiedo intervento urgente.'
        WHEN 3 THEN 'Vorrei passare ad un contratto con energia 100% rinnovabile. Quali sono le opzioni disponibili?'
        WHEN 4 THEN 'La app A2A non funziona, non riesco a vedere i miei consumi da una settimana.'
        WHEN 5 THEN 'Complimenti per il nuovo servizio di autolettura digitale, molto comodo e intuitivo.'
        WHEN 6 THEN 'Ho segnalato un guasto al contatore 15 giorni fa e nessuno e'' ancora intervenuto. Inaccettabile!'
        WHEN 7 THEN 'Vorrei informazioni sulla tariffa bioraria e se conviene nel mio caso specifico.'
        WHEN 8 THEN 'Il servizio clienti telefonico ha tempi di attesa troppo lunghi, ho aspettato 40 minuti.'
        WHEN 9 THEN 'Grazie per la pronta risoluzione del problema di fatturazione. Personale cortese e competente.'
        WHEN 10 THEN 'Richiedo la voltura del contratto per cambio di residenza. Servono documenti particolari?'
        WHEN 11 THEN 'La bolletta non e'' chiara, ci sono voci che non capisco. Potete spiegarmi il dettaglio dei costi?'
        WHEN 12 THEN 'Segnalo un calo di tensione frequente nelle ore serali nella zona di Via Garibaldi, Milano.'
        WHEN 13 THEN 'Vorrei installare un impianto fotovoltaico. A2A offre soluzioni integrate con il contratto energia?'
        WHEN 14 THEN 'Pessima esperienza allo sportello di Brescia. Operatore scortese e incompetente.'
        WHEN 15 THEN 'Ho aderito all''offerta Dual e sono molto soddisfatto. Risparmio evidente rispetto al vecchio contratto.'
        WHEN 16 THEN 'Chiedo rettifica della bolletta di gennaio: i consumi indicati sono impossibili per un appartamento di 60mq.'
        WHEN 17 THEN 'Ottimo il programma fedelta'' A2A. I punti accumulati mi hanno permesso di ottenere uno sconto interessante.'
        WHEN 18 THEN 'Problema con il pagamento RID: la banca mi segnala che il mandato non e'' stato trovato.'
        WHEN 19 THEN 'Vorrei sapere come leggere correttamente il contatore elettronico per l''autolettura.'
        WHEN 20 THEN 'Segnalo perdita di gas in Via Roma 15, Milano. Servizio emergenze contattato ma nessun intervento.'
        WHEN 21 THEN 'Mi trovo benissimo con A2A da 5 anni. Tariffe competitive e servizio affidabile.'
        WHEN 22 THEN 'Ho ricevuto una comunicazione di distacco per morosita'' ma ho pagato tutte le bollette regolarmente.'
        WHEN 23 THEN 'Richiesta di aumento potenza da 3kW a 6kW per installazione piano cottura a induzione.'
        ELSE 'Vorrei maggiori informazioni sulle agevolazioni per il bonus energia e i requisiti ISEE necessari.'
    END,
    CASE MOD(SEQ4(), 3) WHEN 0 THEN 'ALTA' WHEN 1 THEN 'MEDIA' ELSE 'BASSA' END,
    CASE MOD(SEQ4(), 4) WHEN 0 THEN 'APERTO' WHEN 1 THEN 'IN_LAVORAZIONE' WHEN 2 THEN 'RISOLTO' ELSE 'CHIUSO' END,
    ROUND(UNIFORM(0.5, 168, RANDOM()), 2),
    CASE MOD(SEQ4(), 8)
        WHEN 0 THEN 'Maria Verdi' WHEN 1 THEN 'Luca Neri' WHEN 2 THEN 'Anna Blu'
        WHEN 3 THEN 'Marco Gialli' WHEN 4 THEN 'Sara Rossi' WHEN 5 THEN 'Paolo Bianchi'
        WHEN 6 THEN 'Elena Grigi' ELSE 'Roberto Marroni'
    END
FROM TABLE(GENERATOR(ROWCOUNT => 500));

--------------------------------------------------------------
-- INSERT: DOCUMENTI_TECNICI (30 record)
--------------------------------------------------------------
INSERT INTO DOCUMENTI_TECNICI (TITOLO, TIPO_DOCUMENTO, CONTENUTO, DATA_PUBBLICAZIONE, AUTORE, AREA, TAG)
VALUES
('Procedura manutenzione reti MT/BT', 'PROCEDURA', 'Questa procedura definisce le modalita'' operative per la manutenzione ordinaria e straordinaria delle reti di media e bassa tensione. Include ispezioni visive trimestrali, prove di isolamento annuali, sostituzione componenti obsoleti e gestione delle emergenze. Tutti i lavori devono essere eseguiti in conformita'' alle norme CEI 11-27 e con personale abilitato PES/PAV. La documentazione di ogni intervento deve essere registrata nel sistema SAP PM entro 24 ore.', '2024-01-15', 'Ing. Marco Bianchi', 'DISTRIBUZIONE', 'manutenzione,rete,MT,BT,CEI'),
('Report efficienza impianti Q4 2024', 'REPORT', 'Il report analizza le prestazioni degli impianti di generazione nel quarto trimestre 2024. La disponibilita'' media si e'' attestata al 94.2%, in aumento del 2.1% rispetto al Q3. L''impianto di Cassano ha registrato il miglior indice di efficienza (97.3%). Le emissioni di CO2 si sono ridotte del 5.8% grazie all''incremento della quota rinnovabile. Le fermate programmate sono state 12, tutte completate nei tempi previsti. Si raccomanda investimento in revamping turbine per l''impianto di Monfalcone.', '2025-01-10', 'Dott. Elena Rossi', 'GENERAZIONE', 'efficienza,impianti,CO2,rinnovabili,report'),
('Normativa ARERA delibera 568/2024', 'NORMATIVA', 'Sintesi della delibera ARERA 568/2024 relativa all''aggiornamento delle condizioni economiche per il servizio di tutela dell''energia elettrica e del gas naturale. La delibera introduce nuovi criteri per il calcolo del costo della materia prima, revisione dei corrispettivi di trasporto e distribuzione, e aggiornamento degli oneri di sistema. Entrata in vigore dal 1 gennaio 2025. Impatti stimati: +3.2% per clienti domestici luce, -1.8% per gas. Necessario aggiornamento dei sistemi di billing entro il 31 dicembre.', '2024-11-20', 'Avv. Paolo Conti', 'COMMERCIALE', 'ARERA,delibera,tariffe,tutela,billing'),
('Manuale operativo smart meter 2.0', 'MANUALE', 'Guida completa per l''installazione, configurazione e manutenzione dei contatori elettronici di seconda generazione (2G). Include: specifiche tecniche del dispositivo, procedura di installazione certificata, configurazione parametri di telegestione, gestione firmware OTA, diagnostica errori e procedure di sostituzione. Il contatore 2G supporta letture ogni 15 minuti, comunicazione PLC/RF, e integrazione con sistemi MDMS. Formazione obbligatoria per tecnici: corso SM2G di 16 ore.', '2024-06-01', 'Ing. Sara Lombardi', 'DISTRIBUZIONE', 'smart meter,contatore,2G,telegestione,installazione'),
('Piano transizione energetica 2025-2030', 'REPORT', 'Il piano strategico delinea gli obiettivi di A2A per la transizione energetica nel periodo 2025-2030. Target principali: raggiungimento del 60% di produzione da fonti rinnovabili, riduzione emissioni CO2 del 40% rispetto al 2020, installazione di 500MW di nuova capacita'' fotovoltaica, sviluppo di 1000 colonnine di ricarica EV, e implementazione di soluzioni di accumulo energetico per 200MWh. Investimenti previsti: 2.8 miliardi di euro. Il piano prevede anche la chiusura progressiva degli impianti a carbone entro il 2028.', '2025-02-01', 'Ing. Giovanni Ferrari', 'GENERAZIONE', 'transizione,rinnovabili,fotovoltaico,EV,accumulo'),
('Circolare sicurezza lavori sotto tensione', 'CIRCOLARE', 'Aggiornamento delle procedure di sicurezza per lavori sotto tensione su impianti MT e BT. Recepisce le modifiche introdotte dalla norma CEI 11-27 ed.5. Principali novita'': obbligo di doppio controllo pre-intervento, nuovi DPI obbligatori (guanti classe 2 per MT), aggiornamento distanze di sicurezza, e introduzione del registro digitale degli interventi. Tutti i PES e PAV devono completare il corso di aggiornamento entro il 30 giugno 2025. Sanzioni previste per mancato adempimento.', '2025-03-01', 'Ing. Roberto Gallo', 'DISTRIBUZIONE', 'sicurezza,tensione,CEI,DPI,PES,PAV'),
('Analisi mercato energetico italiano 2024', 'REPORT', 'L''analisi del mercato energetico italiano nel 2024 evidenzia una domanda elettrica in leggera crescita (+1.2% YoY) trainata dall''elettrificazione dei trasporti e dal condizionamento estivo. Il PUN medio annuo si e'' attestato a 112 EUR/MWh, in calo del 15% rispetto al 2023. La quota rinnovabile ha raggiunto il 43% del mix energetico nazionale. Il gas naturale resta la prima fonte per generazione termoelettrica (38%). Previsione 2025: stabilizzazione prezzi con possibile aumento in caso di tensioni geopolitiche.', '2025-01-20', 'Dott. Chiara Moretti', 'TRADING', 'mercato,PUN,rinnovabili,gas,previsioni'),
('Procedura gestione reclami clienti', 'PROCEDURA', 'La procedura definisce il flusso operativo per la gestione dei reclami dei clienti in conformita'' alla delibera ARERA 413/2016. Tempi massimi di risposta: reclami scritti 30 giorni, richieste informazioni 30 giorni, rettifica fatturazione 60 giorni. Il reclamo viene classificato per tipologia (fatturazione, tecnico, commerciale, qualita'' servizio) e priorita''. L''operatore assegnato deve aggiornare lo stato del ticket ogni 48 ore. In caso di superamento dei tempi, il cliente ha diritto a indennizzo automatico. Report settimanale obbligatorio al responsabile Customer Care.', '2024-09-15', 'Dott.ssa Anna Ricci', 'COMMERCIALE', 'reclami,ARERA,customer care,indennizzo,SLA'),
('Specifica tecnica cabine secondarie', 'MANUALE', 'Specifica tecnica per la progettazione e realizzazione di cabine di trasformazione MT/BT secondarie. La cabina deve essere conforme alle norme CEI 0-16 e CEI 11-1. Componenti principali: trasformatore in resina da 250/400/630 kVA, quadro MT con sezionatore e IMS, quadro BT con interruttore generale e partenze. Dimensioni minime del locale: 4x3x3m. Ventilazione naturale con superficie minima griglie 1.5 mq. Sistema di terra con resistenza massima 1 ohm. Documentazione as-built obbligatoria.', '2024-04-10', 'Ing. Stefano Bruno', 'DISTRIBUZIONE', 'cabine,trasformatore,MT,BT,CEI'),
('Report qualita'' servizio distribuzione 2024', 'REPORT', 'Il report annuale sulla qualita'' del servizio di distribuzione evidenzia i seguenti KPI: SAIDI (durata media interruzioni) 32 minuti/cliente (-8% vs 2023), SAIFI (frequenza interruzioni) 1.8 per cliente (-12% vs 2023), tempo medio di ripristino guasti MT 85 minuti, tempo medio allacciamento nuova utenza 18 giorni lavorativi. Il miglioramento e'' attribuibile agli investimenti in automazione di rete (self-healing) e alla sostituzione di 450 km di cavi obsoleti. Target 2025: SAIDI sotto 28 minuti.', '2025-02-15', 'Ing. Laura Greco', 'DISTRIBUZIONE', 'qualita'',SAIDI,SAIFI,interruzioni,automazione');

INSERT INTO DOCUMENTI_TECNICI (TITOLO, TIPO_DOCUMENTO, CONTENUTO, DATA_PUBBLICAZIONE, AUTORE, AREA, TAG)
VALUES
('Guida integrazione sistemi accumulo', 'MANUALE', 'Manuale tecnico per l''integrazione di sistemi di accumulo a batterie (BESS) nella rete di distribuzione. Tecnologie coperte: litio-ferro-fosfato (LFP) e litio-nichel-manganese-cobalto (NMC). Dimensionamento tipico: 1-10 MWh per applicazioni di rete. Il sistema deve includere: battery management system (BMS), power conversion system (PCS), sistema SCADA per monitoraggio remoto. Requisiti di connessione alla rete: conformita'' CEI 0-21 per BT e CEI 0-16 per MT. Manutenzione preventiva semestrale obbligatoria.', '2024-08-20', 'Ing. Davide Esposito', 'DISTRIBUZIONE', 'accumulo,batterie,BESS,LFP,NMC'),
('Protocollo emergenza blackout', 'PROCEDURA', 'Protocollo operativo per la gestione delle emergenze da blackout esteso. Livelli di allerta: Giallo (perdita <10% carico), Arancione (10-30%), Rosso (>30%). Per ogni livello sono definiti: catena di comando, procedure di distacco carichi, priorita'' di ripristino (ospedali, infrastrutture critiche, residenziale, industriale), comunicazione esterna e interna. Il Centro Operativo deve essere attivato entro 15 minuti dalla segnalazione. Test annuale obbligatorio con simulazione di blackout pianificata. Coordinamento con Terna e Protezione Civile.', '2024-12-01', 'Ing. Andrea Barbieri', 'DISTRIBUZIONE', 'emergenza,blackout,ripristino,Terna,protezione civile'),
('Analisi predittiva guasti trasformatori', 'REPORT', 'Studio sull''applicazione di algoritmi di machine learning per la predizione dei guasti nei trasformatori MT/BT. Dataset: 15.000 trasformatori monitorati per 5 anni con sensori di temperatura, umidita'' e analisi olio. Il modello Random Forest ha ottenuto un''accuratezza del 87% nella predizione di guasti entro 6 mesi. Variabili piu'' significative: eta'' del trasformatore, numero di sovraccarichi, temperatura massima raggiunta, e risultati analisi DGA. Si raccomanda l''estensione del monitoraggio online a tutti i trasformatori con potenza >400kVA.', '2025-03-10', 'Dott. Matteo Conti', 'DISTRIBUZIONE', 'predittiva,ML,trasformatori,guasti,DGA'),
('Policy cybersecurity infrastrutture OT', 'NORMATIVA', 'Policy di sicurezza informatica per le infrastrutture di Operational Technology (OT) di A2A. Basata su framework IEC 62443 e Direttiva NIS2. Requisiti principali: segmentazione rete IT/OT con DMZ industriale, autenticazione multifattore per accesso remoto SCADA, patching trimestrale sistemi OT con finestra di manutenzione concordata, monitoraggio continuo anomalie di rete con SOC dedicato, incident response plan specifico per OT con RTO max 4 ore per sistemi critici. Audit annuale obbligatorio da ente terzo certificato.', '2025-01-05', 'Ing. Valentina Lombardi', 'DISTRIBUZIONE', 'cybersecurity,OT,SCADA,IEC62443,NIS2'),
('Report sostenibilita'' ambientale 2024', 'REPORT', 'Il bilancio di sostenibilita'' ambientale 2024 di A2A riporta: emissioni Scope 1 ridotte del 12% (3.2 Mt CO2eq), Scope 2 ridotte del 18%, avvio monitoraggio Scope 3. Energia prodotta da rinnovabili: 4.800 GWh (+15% vs 2023). Rifiuti avviati a recupero: 78%. Consumi idrici ridotti del 8% grazie al ricircolo nei cicli di raffreddamento. Investimenti in economia circolare: 180M EUR. Certificazioni mantenute: ISO 14001, ISO 50001, EMAS. Obiettivo net-zero al 2040 confermato con roadmap aggiornata.', '2025-03-15', 'Dott.ssa Silvia Gallo', 'GENERAZIONE', 'sostenibilita'',CO2,rinnovabili,ISO14001,net-zero');

--------------------------------------------------------------
-- INSERT: IMPIANTI (15 record)
--------------------------------------------------------------
INSERT INTO IMPIANTI (NOME_IMPIANTO, TIPO, LOCALITA, REGIONE, CAPACITA_MW, STATO, DATA_ENTRATA_SERVIZIO, EFFICIENZA_PERCENTUALE, EMISSIONI_CO2_TON)
VALUES
('Centrale Termoelettrica Cassano d''Adda', 'TERMOELETTRICO', 'Cassano d''Adda', 'Lombardia', 980, 'OPERATIVO', '1992-06-15', 58.5, 1250000),
('Centrale Termoelettrica Monfalcone', 'TERMOELETTRICO', 'Monfalcone', 'Friuli Venezia Giulia', 320, 'MANUTENZIONE', '1988-03-20', 42.3, 890000),
('Impianto Idroelettrico Cancano', 'IDROELETTRICO', 'Valdidentro', 'Lombardia', 210, 'OPERATIVO', '1956-09-01', 89.2, 0),
('Impianto Idroelettrico Ala', 'IDROELETTRICO', 'Ala', 'Trentino Alto Adige', 45, 'OPERATIVO', '1960-04-10', 91.5, 0),
('Parco Fotovoltaico Brindisi', 'FOTOVOLTAICO', 'Brindisi', 'Puglia', 85, 'OPERATIVO', '2021-11-20', 22.1, 0),
('Parco Fotovoltaico Catania', 'FOTOVOLTAICO', 'Catania', 'Sicilia', 62, 'OPERATIVO', '2022-05-15', 23.8, 0),
('Parco Eolico Trapani', 'EOLICO', 'Trapani', 'Sicilia', 48, 'OPERATIVO', '2019-08-01', 31.2, 0),
('Centrale Cogenerazione Milano', 'COGENERAZIONE', 'Milano', 'Lombardia', 420, 'OPERATIVO', '2005-02-28', 72.8, 580000),
('Parco Fotovoltaico Foggia', 'FOTOVOLTAICO', 'Foggia', 'Puglia', 35, 'OPERATIVO', '2023-03-10', 24.5, 0),
('Impianto Idroelettrico Mese', 'IDROELETTRICO', 'Mese', 'Lombardia', 160, 'OPERATIVO', '1962-07-22', 88.7, 0),
('Centrale Termoelettrica Sermide', 'TERMOELETTRICO', 'Sermide', 'Lombardia', 1140, 'OPERATIVO', '1984-11-10', 52.1, 1680000),
('Parco Eolico Matera', 'EOLICO', 'Matera', 'Basilicata', 32, 'MANUTENZIONE', '2020-06-25', 28.9, 0),
('Impianto Idroelettrico Premadio', 'IDROELETTRICO', 'Valdisotto', 'Lombardia', 75, 'OPERATIVO', '1958-12-05', 90.1, 0),
('Parco Fotovoltaico Sardegna Sud', 'FOTOVOLTAICO', 'Carbonia', 'Sardegna', 50, 'OPERATIVO', '2024-01-20', 25.2, 0),
('Termovalorizzatore Silla 2', 'COGENERAZIONE', 'Milano', 'Lombardia', 66, 'OPERATIVO', '2001-09-15', 68.4, 320000);

--------------------------------------------------------------
-- INSERT: PRODUZIONE_ENERGIA (5400 record - 12 mesi x 15 impianti x 30 giorni)
--------------------------------------------------------------
INSERT INTO PRODUZIONE_ENERGIA (IMPIANTO_ID, DATA_PRODUZIONE, ENERGIA_PRODOTTA_MWH, ORE_FUNZIONAMENTO, DISPONIBILITA_PERCENTUALE, COSTO_PRODUZIONE_EUR, PREZZO_VENDITA_MWH_EUR)
SELECT
    i.IMPIANTO_ID,
    DATEADD('day', -d.D, CURRENT_DATE()),
    ROUND(
        (i.CAPACITA_MW * UNIFORM(0.3, 0.95, RANDOM()) * 24 *
        CASE i.TIPO
            WHEN 'FOTOVOLTAICO' THEN CASE WHEN MONTH(DATEADD('day', -d.D, CURRENT_DATE())) BETWEEN 5 AND 9 THEN 0.28 ELSE 0.12 END
            WHEN 'EOLICO' THEN CASE WHEN MONTH(DATEADD('day', -d.D, CURRENT_DATE())) BETWEEN 10 AND 3 THEN 0.35 ELSE 0.22 END
            WHEN 'IDROELETTRICO' THEN CASE WHEN MONTH(DATEADD('day', -d.D, CURRENT_DATE())) BETWEEN 4 AND 7 THEN 0.65 ELSE 0.40 END
            ELSE 0.70
        END) *
        CASE i.STATO WHEN 'MANUTENZIONE' THEN 0.3 ELSE 1.0 END
    , 2),
    ROUND(UNIFORM(16, 24, RANDOM()), 2),
    ROUND(UNIFORM(85, 100, RANDOM()), 2),
    0,
    ROUND(UNIFORM(80, 150, RANDOM()), 2)
FROM IMPIANTI i
CROSS JOIN (SELECT ROW_NUMBER() OVER (ORDER BY SEQ4()) - 1 AS D FROM TABLE(GENERATOR(ROWCOUNT => 365))) d;

UPDATE PRODUZIONE_ENERGIA SET
    COSTO_PRODUZIONE_EUR = ROUND(ENERGIA_PRODOTTA_MWH * UNIFORM(25, 65, RANDOM()), 2);
```

## 1.4 Verifica Dati

```sql
SELECT 'CLIENTI' AS TABELLA, COUNT(*) AS RECORD FROM CLIENTI
UNION ALL SELECT 'CONSUMI_ENERGIA', COUNT(*) FROM CONSUMI_ENERGIA
UNION ALL SELECT 'FEEDBACK_CLIENTI', COUNT(*) FROM FEEDBACK_CLIENTI
UNION ALL SELECT 'DOCUMENTI_TECNICI', COUNT(*) FROM DOCUMENTI_TECNICI
UNION ALL SELECT 'IMPIANTI', COUNT(*) FROM IMPIANTI
UNION ALL SELECT 'PRODUZIONE_ENERGIA', COUNT(*) FROM PRODUZIONE_ENERGIA;
```

> **Risultato atteso:** CLIENTI ~200, CONSUMI_ENERGIA ~2400, FEEDBACK_CLIENTI ~500, DOCUMENTI_TECNICI ~24, IMPIANTI 15, PRODUZIONE_ENERGIA ~5475

---

# FASE 2: Setup Cortex Analyst (Semantic View)

In questa fase creeremo una **Semantic View** che permette a Snowflake Intelligence di capire la struttura dei nostri dati e rispondere a domande in linguaggio naturale.

## 2.1 Cos'e' una Semantic View

Una Semantic View e' un layer semantico dichiarativo sopra le tabelle Snowflake. Definisce:
- **Tabelle logiche** con nomi e descrizioni business-friendly
- **Dimensioni** (colonne per raggruppamento/filtro)
- **Metriche** (aggregazioni calcolate)
- **Relazioni** tra tabelle (join)

Cortex Analyst usa il semantic model per tradurre domande in linguaggio naturale in query SQL corrette.

## 2.2 Creazione del Semantic Model con Cortex Code in Snowsight

### Opzione A: Creazione tramite Snowsight UI (Semantic View Creator)

1. In Snowsight, cliccare su **Data** nel menu di sinistra
2. Navigare a **POWERUTILITY > PUBLIC**
3. Cliccare su **"+ Semantic View"** (pulsante in alto a destra)
4. Dare il nome: `A2A_ENERGY_SEMANTIC_VIEW`
5. Il Semantic View Creator si apre con un editor visuale

**Aggiungere le tabelle:**
- Cliccare **"Add Table"** e selezionare: `CLIENTI`, `CONSUMI_ENERGIA`, `FEEDBACK_CLIENTI`, `IMPIANTI`, `PRODUZIONE_ENERGIA`
- Per ogni tabella, il sistema proporra' automaticamente dimensioni e metriche

**Configurare le relazioni:**
- `CONSUMI_ENERGIA.CLIENTE_ID` → `CLIENTI.CLIENTE_ID`
- `FEEDBACK_CLIENTI.CLIENTE_ID` → `CLIENTI.CLIENTE_ID`
- `PRODUZIONE_ENERGIA.IMPIANTO_ID` → `IMPIANTI.IMPIANTO_ID`

**Verificare i campi:** Per ogni tabella, controllare che dimensioni e metriche siano corrette.

### Opzione B: Creazione tramite SQL (Raccomandata per il lab)

Eseguire il seguente script per creare la semantic view direttamente via SQL:

```sql
USE ROLE ACCOUNTADMIN;
USE DATABASE POWERUTILITY;
USE SCHEMA PUBLIC;

CREATE OR REPLACE SEMANTIC VIEW A2A_ENERGY_SEMANTIC_VIEW
AS SEMANTIC MODEL
  TABLES (
    -- TABELLA CLIENTI
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

    -- TABELLA CONSUMI
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

    -- TABELLA FEEDBACK
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

    -- TABELLA IMPIANTI
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

    -- TABELLA PRODUZIONE
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
```

## 2.3 Verifica Semantic View in Snowsight

1. Navigare in **Data > POWERUTILITY > PUBLIC**
2. Trovare la semantic view `A2A_ENERGY_SEMANTIC_VIEW`
3. Cliccare sulla view per aprire il **Semantic View Creator**
4. Verificare:
   - **Tabelle:** Tutte e 5 le tabelle sono presenti con i nomi logici corretti
   - **Colonne:** Le descrizioni sono compilate per ogni colonna
   - **Relazioni:** Le 3 relazioni (CONSUMI→CLIENTI, FEEDBACK→CLIENTI, PRODUZIONE→IMPIANTI) sono visualizzate nel diagramma
   - **Anteprima:** Cliccare su una tabella per vedere l'anteprima dei dati

> **Suggerimento:** Nel Semantic View Creator potete anche aggiungere metriche calcolate e sinonimi per migliorare la comprensione delle domande in linguaggio naturale.

## 2.4 Test Rapido della Semantic View

In un SQL Worksheet, testare:

```sql
-- Verifica che la semantic view sia stata creata
SHOW SEMANTIC VIEWS IN SCHEMA POWERUTILITY.PUBLIC;

-- Descrivi la struttura
DESCRIBE SEMANTIC VIEW POWERUTILITY.PUBLIC.A2A_ENERGY_SEMANTIC_VIEW;
```

---

# FASE 3: Setup Cortex Search (RAG su Contratti)

In questa fase configureremo **Cortex Search** per indicizzare il PDF delle Condizioni Generali di Fornitura di A2A, abilitando la ricerca semantica (RAG - Retrieval Augmented Generation).

## 3.1 Cos'e' Cortex Search

Cortex Search e' un servizio Snowflake che crea un indice di ricerca semantica sui documenti. Permette di cercare informazioni usando linguaggio naturale e restituisce i passaggi piu' rilevanti. Quando collegato a Snowflake Intelligence, abilita risposte basate su documenti non strutturati.

## 3.2 Caricare il PDF su uno Stage

```sql
-- Creare uno stage per i documenti
CREATE OR REPLACE STAGE POWERUTILITY.PUBLIC.DOCUMENTI_STAGE
    DIRECTORY = (ENABLE = TRUE)
    ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE');
```

**Caricare il file PDF:**

1. In Snowsight, navigare a **Data > POWERUTILITY > PUBLIC > Stages > DOCUMENTI_STAGE**
2. Cliccare **"+ Files"** in alto a destra
3. Selezionare il file `a2a_condizioni_generali_fornitura.pdf` dal proprio computer
4. Attendere il completamento dell'upload

In alternativa, da SQL Worksheet con SnowSQL:

```sql
-- Verifica che il file sia stato caricato
LIST @POWERUTILITY.PUBLIC.DOCUMENTI_STAGE;
```

## 3.3 Parsing del PDF con AI_PARSE_DOCUMENT

Estraiamo il testo dal PDF e lo salviamo in una tabella per l'indicizzazione.

```sql
-- Creare la tabella per il contenuto estratto dai documenti
CREATE OR REPLACE TABLE POWERUTILITY.PUBLIC.CONTRATTO_CHUNKS (
    CHUNK_ID NUMBER AUTOINCREMENT PRIMARY KEY,
    NOME_FILE VARCHAR(500),
    SEZIONE VARCHAR(500),
    CONTENUTO VARCHAR(8000),
    PAGINA NUMBER
);

-- Estrarre il testo dal PDF usando AI_PARSE_DOCUMENT
-- Prima verifichiamo il contenuto
SELECT
    AI_PARSE_DOCUMENT(
        BUILD_SCOPED_FILE_URL(@DOCUMENTI_STAGE, 'a2a_condizioni_generali_fornitura.pdf'),
        {'mode': 'LAYOUT'}
    ):content::VARCHAR AS TESTO_COMPLETO;
```

Ora creiamo i chunk per la ricerca. Per semplicita' nel lab, splittiamo il testo in parti:

```sql
-- Estrarre e suddividere il contenuto in chunk
-- Approccio: estraiamo il testo completo e lo dividiamo in sezioni logiche
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

-- Verificare i chunk creati
SELECT COUNT(*) AS NUM_CHUNKS, AVG(LENGTH(CONTENUTO)) AS AVG_LENGTH
FROM CONTRATTO_CHUNKS;
```

## 3.4 Creare il Cortex Search Service

```sql
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
```

## 3.5 Testare il Cortex Search Service

```sql
-- Test di ricerca semantica
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
```

> **Nota:** Il servizio Cortex Search potrebbe richiedere qualche minuto per l'indicizzazione iniziale.

---

# FASE 4: Configurazione Snowflake Intelligence

## 4.1 Cos'e' Snowflake Intelligence

Snowflake Intelligence e' un agente AI enterprise che combina:
- **Cortex Analyst** (query sui dati strutturati via Semantic View)
- **Cortex Search** (ricerca su documenti non strutturati)

Permette agli utenti di fare domande in linguaggio naturale e ottenere risposte che combinano dati e documenti.

## 4.2 Creare l'Agente Snowflake Intelligence

1. In Snowsight, navigare su **AI & ML > Snowflake Intelligence** (menu di sinistra)
2. Cliccare **"+ Intelligence"** in alto a destra
3. Configurare:
   - **Name:** `A2A_ENERGY_ASSISTANT`
   - **Database:** `POWERUTILITY`
   - **Schema:** `PUBLIC`
   - **Description:** `Assistente AI per l'analisi dei dati energetici A2A. Risponde a domande su clienti, consumi, impianti, produzione e contratti.`

4. **Aggiungere la Semantic View come strumento:**
   - Nella sezione **Tools**, cliccare **"+ Add Tool"**
   - Selezionare **"Analyst (Semantic View)"**
   - Scegliere `POWERUTILITY.PUBLIC.A2A_ENERGY_SEMANTIC_VIEW`
   - Description: `Interroga i dati strutturati su clienti, consumi energetici, impianti di produzione e feedback`

5. **Aggiungere il Cortex Search come strumento:**
   - Cliccare **"+ Add Tool"**
   - Selezionare **"Search Service"**
   - Scegliere `POWERUTILITY.PUBLIC.A2A_CONTRATTO_SEARCH`
   - Description: `Cerca informazioni nelle Condizioni Generali di Fornitura A2A`

6. **Configurare le istruzioni dell'agente** (opzionale ma consigliato):

Nella sezione **Instructions**, aggiungere:

```
Sei l'assistente AI di A2A Energia. Rispondi sempre in italiano.

Quando ti vengono poste domande sui dati (clienti, consumi, impianti, produzione, feedback), usa lo strumento di analisi dati per generare query SQL precise.

Quando ti vengono poste domande sul contratto o sulle condizioni di fornitura, usa lo strumento di ricerca documenti per trovare le clausole rilevanti.

Se una domanda combina dati e documentazione, usa entrambi gli strumenti per fornire una risposta completa.

Formatta i numeri con il separatore delle migliaia (.) e il separatore decimale (,) come da convenzione italiana.
Esprimi i valori monetari in EUR.
```

7. Cliccare **"Create"** per creare l'agente

In alternativa, via SQL:

```sql
CREATE OR REPLACE AGENT POWERUTILITY.PUBLIC.A2A_ENERGY_ASSISTANT
FROM SPECIFICATION $spec$
{
  "models": {
    "orchestration": "auto"
  },
  "instructions": {
    "orchestration": "Sei l'assistente AI di A2A Energia. Rispondi sempre in italiano. Quando ti vengono poste domande sui dati usa lo strumento analyst. Quando ti vengono poste domande sul contratto usa lo strumento di ricerca documenti.",
    "response": "Formatta i numeri con il separatore delle migliaia (.) e il separatore decimale (,) come da convenzione italiana. Esprimi i valori monetari in EUR."
  },
  "tools": [
    {
      "tool_spec": {
        "type": "cortex_analyst_text_to_sql",
        "name": "analisi_dati_energia",
        "description": "Interroga i dati strutturati su clienti, consumi energetici, impianti di produzione, produzione energia e feedback clienti di A2A"
      }
    },
    {
      "tool_spec": {
        "type": "cortex_search",
        "name": "ricerca_contratto",
        "description": "Cerca informazioni nelle Condizioni Generali di Fornitura A2A, clausole contrattuali, diritti e obblighi"
      }
    }
  ],
  "tool_resources": {
    "analisi_dati_energia": {
      "execution_environment": {
        "query_timeout": 299,
        "type": "warehouse",
        "warehouse": ""
      },
      "semantic_view": "POWERUTILITY.PUBLIC.A2A_ENERGY_SEMANTIC_VIEW"
    },
    "ricerca_contratto": {
      "execution_environment": {
        "query_timeout": 299,
        "type": "warehouse",
        "warehouse": ""
      },
      "search_service": "POWERUTILITY.PUBLIC.A2A_CONTRATTO_SEARCH"
    }
  }
}
$spec$;
```

## 4.3 Verificare la Creazione

```sql
SHOW AGENTS LIKE 'A2A_ENERGY_ASSISTANT' IN SCHEMA POWERUTILITY.PUBLIC;
DESCRIBE AGENT POWERUTILITY.PUBLIC.A2A_ENERGY_ASSISTANT;
```

---

# FASE 5: Test di Snowflake Intelligence

## 5.1 Accedere a Snowflake Intelligence

1. Navigare su **AI & ML > Snowflake Intelligence**
2. Cliccare su `A2A_ENERGY_ASSISTANT`
3. Si apre la chat interattiva

## 5.2 Domande di Test - Dati Strutturati

Provare le seguenti domande nella chat:

**Domande sui Clienti:**
```
Quanti clienti attivi abbiamo per tipo di contratto?
```

```
Qual e' la distribuzione dei clienti per citta' e segmento?
```

```
Quanti clienti PREMIUM abbiamo a Milano?
```

**Domande sui Consumi:**
```
Qual e' il consumo medio mensile di energia elettrica (kWh) per tipo di cliente?
```

```
Mostrami il trend dei costi totali mensili negli ultimi 6 mesi
```

```
Quali sono i 10 clienti con il consumo piu' alto nell'ultimo mese?
```

**Domande sugli Impianti e Produzione:**
```
Qual e' la capacita' totale installata per tipo di impianto?
```

```
Qual e' l'efficienza media degli impianti per tipologia?
```

```
Quanta energia e' stata prodotta nel mese scorso per ogni impianto?
```

**Domande sui Feedback:**
```
Quanti ticket aperti abbiamo per categoria?
```

```
Qual e' il tempo medio di risoluzione per priorita'?
```

## 5.3 Domande di Test - Documenti (RAG)

```
Quali sono le condizioni per il recesso dal contratto di fornitura?
```

```
Cosa prevede il contratto in caso di morosita' del cliente?
```

```
Quali sono i diritti del cliente secondo le condizioni generali di fornitura?
```

## 5.4 Domande Combinate (Dati + Documenti)

```
Abbiamo clienti con contratto sospeso? Cosa dice il contratto sulle condizioni di sospensione?
```

```
Qual e' la percentuale di reclami sulla fatturazione e cosa prevede il contratto per la gestione dei reclami?
```

## 5.5 Test Artifacts di Snowflake Intelligence

Snowflake Intelligence supporta la creazione di **Artifacts**: grafici e visualizzazioni che possono essere salvati e condivisi.

**Test creazione Artifacts:**

1. Nella chat, chiedere:
```
Crea un grafico a barre del consumo medio mensile per tipo di cliente negli ultimi 6 mesi
```

2. Quando l'agente genera il grafico:
   - Cliccare sull'icona **"Pin"** o **"Save"** sull'artifact generato
   - L'artifact viene salvato nella sezione **"Artifacts"** della conversazione

3. Provare anche:
```
Genera un grafico a torta della distribuzione dei clienti per segmento
```

```
Mostra un line chart del trend di produzione energetica mensile per tipo di impianto
```

4. **Condivisione Artifacts:**
   - Nella sezione Artifacts, cliccare su un grafico salvato
   - Cliccare **"Share"** per condividere con altri utenti
   - E' possibile anche esportare come immagine

> **Nota:** Gli Artifacts permettono di creare una libreria di visualizzazioni riutilizzabili e condivisibili, utile per report ricorrenti e dashboard ad-hoc.

---

# FASE 6: Introduzione a Cortex AI SQL

## 6.1 Cos'e' Cortex AI SQL

Cortex AI SQL e' un set di **funzioni SQL native** che integrano modelli LLM direttamente nelle query Snowflake. Permettono di eseguire task di AI/ML senza uscire dall'ambiente SQL e senza dover gestire infrastruttura esterna.

### Funzioni Principali

| Funzione | Descrizione | Output |
|----------|-------------|--------|
| `AI_COMPLETE()` | Prompt generico a un LLM | VARCHAR |
| `AI_SENTIMENT()` | Analisi del sentiment | FLOAT (-1 a 1) |
| `AI_SUMMARIZE()` | Riassunto di un testo | VARCHAR |
| `AI_TRANSLATE()` | Traduzione tra lingue | VARCHAR |
| `AI_CLASSIFY()` | Classificazione in categorie | VARCHAR |
| `AI_EXTRACT()` | Estrazione entita' strutturate | OBJECT |
| `AI_FILTER()` | Filtro booleano semantico | BOOLEAN |
| `AI_EMBED()` | Generazione embedding vettoriale | VECTOR |
| `AI_PARSE_DOCUMENT()` | Estrazione testo da PDF/immagini | OBJECT |

### Valore Aggiunto

- **Zero infrastruttura:** Tutto gira all'interno di Snowflake, nessun servizio esterno da configurare
- **Governance integrata:** I dati non lasciano mai Snowflake, RBAC applicato automaticamente
- **Scalabilita':** Processa milioni di record in batch SQL
- **Integrazione nativa:** Usabile in SELECT, WHERE, JOIN, Dynamic Tables, Streams
- **Costi prevedibili:** Basato su crediti Snowflake, nessuna API key esterna

## 6.2 Esempi Rapidi in SQL Worksheet

```sql
USE DATABASE POWERUTILITY;
USE SCHEMA PUBLIC;

-- SENTIMENT ANALYSIS sui feedback
SELECT
    FEEDBACK_ID,
    TESTO_FEEDBACK,
    SNOWFLAKE.CORTEX.SENTIMENT(TESTO_FEEDBACK) AS SENTIMENT_SCORE
FROM FEEDBACK_CLIENTI
LIMIT 10;

-- SUMMARIZE di un documento tecnico
SELECT
    TITOLO,
    SNOWFLAKE.CORTEX.SUMMARIZE(CONTENUTO) AS RIASSUNTO
FROM DOCUMENTI_TECNICI
LIMIT 3;

-- TRANSLATE feedback in inglese
SELECT
    TESTO_FEEDBACK,
    SNOWFLAKE.CORTEX.TRANSLATE(TESTO_FEEDBACK, 'it', 'en') AS ENGLISH_TRANSLATION
FROM FEEDBACK_CLIENTI
LIMIT 5;

-- AI_COMPLETE: prompt generico
SELECT SNOWFLAKE.CORTEX.COMPLETE(
    'mistral-large2',
    'Analizza questo feedback di un cliente utility e suggerisci un''azione: '
    || TESTO_FEEDBACK
) AS AZIONE_SUGGERITA
FROM FEEDBACK_CLIENTI
LIMIT 3;
```

---

# FASE 7: Notebook AISQL - Laboratorio Pratico

## 7.1 Creare un Notebook Snowflake

1. In Snowsight, navigare su **Notebooks** nel menu di sinistra
2. Cliccare **"+ Notebook"**
3. Configurare:
   - **Name:** `A2A_AISQL_Lab`
   - **Database:** `POWERUTILITY`
   - **Schema:** `PUBLIC`
   - **Warehouse:** `COMPUTE_WH`
4. Selezionare **"SQL"** come linguaggio della prima cella

## 7.2 Celle del Notebook

### Cella 1 (Markdown): Intestazione

```markdown
# Laboratorio AI SQL - A2A Energia
Esplorazione delle funzioni Cortex AI SQL sui dati energetici A2A.

**Funzioni testate:**
- SENTIMENT - Analisi del sentiment
- SUMMARIZE - Riassunto automatico
- TRANSLATE - Traduzione multilingua
- COMPLETE - Prompt LLM generico
- CLASSIFY - Classificazione automatica
- EXTRACT - Estrazione entita'
```

### Cella 2 (SQL): Setup

```sql
USE DATABASE POWERUTILITY;
USE SCHEMA PUBLIC;
USE WAREHOUSE COMPUTE_WH;
```

### Cella 3 (Markdown): Sentiment Analysis

```markdown
## 1. Sentiment Analysis
Analizziamo il sentiment dei feedback clienti per identificare clienti insoddisfatti e aree di miglioramento.
```

### Cella 4 (SQL): Sentiment sui Feedback

```sql
-- Sentiment analysis su tutti i feedback
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
```

### Cella 5 (SQL): Statistiche Sentiment per Categoria

```sql
-- Statistiche sentiment aggregate per categoria
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
```

### Cella 6 (Markdown): Summarization

```markdown
## 2. Summarization
Generiamo riassunti automatici dei documenti tecnici per una rapida consultazione.
```

### Cella 7 (SQL): Riassunto Documenti Tecnici

```sql
-- Riassunto automatico dei documenti tecnici
SELECT
    TITOLO,
    TIPO_DOCUMENTO,
    AREA,
    SNOWFLAKE.CORTEX.SUMMARIZE(CONTENUTO) AS RIASSUNTO
FROM DOCUMENTI_TECNICI
WHERE TIPO_DOCUMENTO IN ('REPORT', 'PROCEDURA')
LIMIT 5;
```

### Cella 8 (Markdown): Traduzione

```markdown
## 3. Traduzione Multilingua
Traduciamo i feedback dei clienti in inglese per report internazionali.
```

### Cella 9 (SQL): Traduzione Feedback

```sql
-- Traduzione feedback in inglese e tedesco
SELECT
    FEEDBACK_ID,
    TESTO_FEEDBACK AS ORIGINALE_IT,
    SNOWFLAKE.CORTEX.TRANSLATE(TESTO_FEEDBACK, 'it', 'en') AS TRADUZIONE_EN,
    SNOWFLAKE.CORTEX.TRANSLATE(TESTO_FEEDBACK, 'it', 'de') AS TRADUZIONE_DE
FROM FEEDBACK_CLIENTI
LIMIT 5;
```

### Cella 10 (Markdown): AI Complete

```markdown
## 4. AI Complete - Prompt Generici
Usiamo il modello LLM per analisi avanzate: categorizzazione, suggerimento azioni, generazione report.
```

### Cella 11 (SQL): Analisi Avanzata con AI_COMPLETE

```sql
-- Analisi del feedback con suggerimento di azione
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
```

### Cella 12 (SQL): Generazione Report Energetico

```sql
-- Generazione automatica di un mini-report con AI_COMPLETE
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
```

### Cella 13 (Markdown): Classify

```markdown
## 5. AI Classify - Classificazione Automatica
Classifichiamo automaticamente i feedback in categorie di urgenza.
```

### Cella 14 (SQL): Classificazione Feedback

```sql
-- Classificazione automatica dei feedback
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
```

### Cella 15 (Markdown): Extract

```markdown
## 6. AI Extract - Estrazione Entita'
Estraiamo informazioni strutturate dai testi dei feedback.
```

### Cella 16 (SQL): Estrazione Entita'

```sql
-- Estrazione entita' dai feedback
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
```

### Cella 17 (Markdown): Conclusioni

```markdown
## Conclusioni

In questo laboratorio abbiamo visto come le funzioni AI SQL di Snowflake permettono di:

- **Analizzare il sentiment** dei feedback clienti per identificare criticita'
- **Riassumere automaticamente** documenti tecnici complessi
- **Tradurre** contenuti per report multilingua
- **Generare analisi** e report con prompt LLM personalizzati
- **Classificare** automaticamente i feedback in categorie
- **Estrarre** informazioni strutturate da testi non strutturati

Tutto questo senza mai spostare i dati fuori da Snowflake e con la governance RBAC integrata.
```

---

# Appendice A: Troubleshooting

| Problema | Soluzione |
|----------|-----------|
| Semantic View non appare | Verificare di essere nel database/schema corretto. Rieseguire `SHOW SEMANTIC VIEWS` |
| Cortex Search non restituisce risultati | Attendere qualche minuto per l'indicizzazione. Verificare che i chunk contengano testo valido |
| AI_PARSE_DOCUMENT fallisce | Verificare che il file sia caricato correttamente sullo stage con `LIST @DOCUMENTI_STAGE` |
| Snowflake Intelligence non risponde | Verificare che sia Semantic View che Search Service siano attivi |
| Funzioni AI SQL lente | Ridurre il numero di record con LIMIT. Usare warehouse piu' grande per batch |
| Errore "function not found" | Usare il prefisso completo: `SNOWFLAKE.CORTEX.SENTIMENT()` |

# Appendice B: Risorse

- [Snowflake Cortex AI Functions](https://docs.snowflake.com/en/user-guide/snowflake-cortex/cortex-llm-functions)
- [Semantic Views](https://docs.snowflake.com/en/user-guide/snowflake-cortex/cortex-analyst/semantic-views)
- [Cortex Search](https://docs.snowflake.com/en/user-guide/snowflake-cortex/cortex-search)
- [Snowflake Intelligence](https://docs.snowflake.com/en/user-guide/snowflake-intelligence)

---

*Guida preparata per il Hands-On Lab A2A Energia - Snowflake AI per il Settore Energy*
