--- ============================================================
--- FASE 1: Creazione Database e Dati Sintetici
--- ============================================================

--- 1.1 Setup Iniziale
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

--- 1.2 Creazione Tabelle

--- TABELLA: CLIENTI
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
    TIPO_CLIENTE VARCHAR(20),       --- RESIDENZIALE, BUSINESS, CONDOMINIO
    TIPO_CONTRATTO VARCHAR(30),     --- LUCE, GAS, DUAL
    POTENZA_IMPEGNATA_KW NUMBER(10,2),
    DATA_ATTIVAZIONE DATE,
    STATO_CONTRATTO VARCHAR(20),    --- ATTIVO, SOSPESO, CESSATO
    SEGMENTO VARCHAR(30)            --- PREMIUM, STANDARD, BASE
);

--- TABELLA: CONSUMI_ENERGIA
CREATE OR REPLACE TABLE CONSUMI_ENERGIA (
    CONSUMO_ID NUMBER AUTOINCREMENT PRIMARY KEY,
    CLIENTE_ID NUMBER REFERENCES CLIENTI(CLIENTE_ID),
    MESE DATE,
    CONSUMO_KWH NUMBER(12,2),
    CONSUMO_SMC NUMBER(12,2),       --- Standard Metri Cubi (gas)
    COSTO_ENERGIA_EUR NUMBER(12,2),
    COSTO_TRASPORTO_EUR NUMBER(12,2),
    COSTO_ONERI_EUR NUMBER(12,2),
    COSTO_TOTALE_EUR NUMBER(12,2),
    FASCIA_ORARIA VARCHAR(5),       --- F1, F2, F3
    FONTE_ENERGIA VARCHAR(20),      --- RETE, FOTOVOLTAICO, MISTO
    POD VARCHAR(20),
    PDR VARCHAR(20)
);

--- TABELLA: FEEDBACK_CLIENTI
CREATE OR REPLACE TABLE FEEDBACK_CLIENTI (
    FEEDBACK_ID NUMBER AUTOINCREMENT PRIMARY KEY,
    CLIENTE_ID NUMBER REFERENCES CLIENTI(CLIENTE_ID),
    DATA_FEEDBACK TIMESTAMP_NTZ,
    CANALE VARCHAR(30),             --- EMAIL, TELEFONO, APP, SOCIAL, SPORTELLO
    CATEGORIA VARCHAR(50),          --- FATTURAZIONE, GUASTI, CONTRATTO, INFORMAZIONI, RECLAMO
    TESTO_FEEDBACK VARCHAR(2000),
    PRIORITA VARCHAR(10),           --- ALTA, MEDIA, BASSA
    STATO_TICKET VARCHAR(20),       --- APERTO, IN_LAVORAZIONE, RISOLTO, CHIUSO
    TEMPO_RISOLUZIONE_ORE NUMBER(10,2),
    OPERATORE VARCHAR(100)
);

--- TABELLA: DOCUMENTI_TECNICI
CREATE OR REPLACE TABLE DOCUMENTI_TECNICI (
    DOCUMENTO_ID NUMBER AUTOINCREMENT PRIMARY KEY,
    TITOLO VARCHAR(300),
    TIPO_DOCUMENTO VARCHAR(50),     --- NORMATIVA, PROCEDURA, MANUALE, REPORT, CIRCOLARE
    CONTENUTO VARCHAR(5000),
    DATA_PUBBLICAZIONE DATE,
    AUTORE VARCHAR(100),
    AREA VARCHAR(50),               --- DISTRIBUZIONE, GENERAZIONE, TRADING, COMMERCIALE
    TAG VARCHAR(500)
);

--- TABELLA: IMPIANTI
CREATE OR REPLACE TABLE IMPIANTI (
    IMPIANTO_ID NUMBER AUTOINCREMENT PRIMARY KEY,
    NOME_IMPIANTO VARCHAR(200),
    TIPO VARCHAR(50),               --- TERMOELETTRICO, IDROELETTRICO, FOTOVOLTAICO, EOLICO, COGENERAZIONE
    LOCALITA VARCHAR(200),
    REGIONE VARCHAR(50),
    CAPACITA_MW NUMBER(10,2),
    STATO VARCHAR(20),              --- OPERATIVO, MANUTENZIONE, FERMO
    DATA_ENTRATA_SERVIZIO DATE,
    EFFICIENZA_PERCENTUALE NUMBER(5,2),
    EMISSIONI_CO2_TON NUMBER(12,2)
);

--- TABELLA: PRODUZIONE_ENERGIA
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

--- 1.3 Inserimento Dati Sintetici

--- INSERT: CLIENTI (200 record)
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
    --- Codice Fiscale generato secondo lo standard italiano (CCCNNN AALMGG ZXXX K)
    --- Parte cognome (3 consonanti)
    CASE MOD(SEQ4(), 15)
        WHEN 0 THEN 'RSS' WHEN 1 THEN 'BNC' WHEN 2 THEN 'CLM' WHEN 3 THEN 'FRR'
        WHEN 4 THEN 'RSS' WHEN 5 THEN 'RMN' WHEN 6 THEN 'GLL' WHEN 7 THEN 'CNT'
        WHEN 8 THEN 'SPT' WHEN 9 THEN 'RCC' WHEN 10 THEN 'BRN' WHEN 11 THEN 'GRC'
        WHEN 12 THEN 'MRT' WHEN 13 THEN 'BRB' ELSE 'LMB'
    END ||
    --- Parte nome (3 consonanti/vocali)
    CASE MOD(SEQ4(), 20)
        WHEN 0 THEN 'MRC' WHEN 1 THEN 'GLI' WHEN 2 THEN 'LSN' WHEN 3 THEN 'FRN'
        WHEN 4 THEN 'LCU' WHEN 5 THEN 'SRA' WHEN 6 THEN 'NDR' WHEN 7 THEN 'LNE'
        WHEN 8 THEN 'RRT' WHEN 9 THEN 'CHR' WHEN 10 THEN 'GNN' WHEN 11 THEN 'MRA'
        WHEN 12 THEN 'PLA' WHEN 13 THEN 'NNA' WHEN 14 THEN 'SFN' WHEN 15 THEN 'LRA'
        WHEN 16 THEN 'DVD' WHEN 17 THEN 'VNT' WHEN 18 THEN 'MTT' ELSE 'SLV'
    END ||
    --- Anno nascita (2 cifre, persone tra 25 e 70 anni)
    LPAD(MOD(56 + MOD(SEQ4() * 13, 45), 100)::VARCHAR, 2, '0') ||
    --- Mese nascita (lettera codice mese italiano)
    SUBSTR('ABCDEHLMPRST', MOD(SEQ4() * 7, 12) + 1, 1) ||
    --- Giorno nascita (01-31, donne nomi dispari +40)
    LPAD((MOD(SEQ4() * 3, 28) + 1 +
        CASE WHEN MOD(SEQ4(), 20) IN (1,3,5,7,9,11,13,15,17,19) THEN 40 ELSE 0 END
    )::VARCHAR, 2, '0') ||
    --- Codice catastale comune (reale per le 8 citta')
    CASE MOD(SEQ4(), 8)
        WHEN 0 THEN 'F205' WHEN 1 THEN 'B157' WHEN 2 THEN 'A794' WHEN 3 THEN 'C933'
        WHEN 4 THEN 'D150' WHEN 5 THEN 'L682' WHEN 6 THEN 'F704' ELSE 'G388'
    END ||
    --- Carattere di controllo (simulato deterministico)
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

--- INSERT: CONSUMI_ENERGIA (2400 record - 12 mesi x 200 clienti)
INSERT INTO CONSUMI_ENERGIA (CLIENTE_ID, MESE, CONSUMO_KWH, CONSUMO_SMC, COSTO_ENERGIA_EUR, COSTO_TRASPORTO_EUR, COSTO_ONERI_EUR, COSTO_TOTALE_EUR, FASCIA_ORARIA, FONTE_ENERGIA, POD, PDR)
SELECT
    c.CLIENTE_ID,
    DATE_TRUNC('month', DATEADD('month', -m.M, CURRENT_DATE())),
    ROUND(UNIFORM(80, 600, RANDOM()) * 
        CASE WHEN m.M BETWEEN 5 AND 8 THEN 1.3
             WHEN m.M BETWEEN 0 AND 2 THEN 1.4
             ELSE 1.0 END *
        CASE c.TIPO_CLIENTE WHEN 'BUSINESS' THEN 3.0 WHEN 'CONDOMINIO' THEN 5.0 ELSE 1.0 END
    , 2),
    CASE WHEN c.TIPO_CONTRATTO IN ('GAS', 'DUAL') THEN
        ROUND(UNIFORM(20, 300, RANDOM()) * 
            CASE WHEN m.M BETWEEN 0 AND 3 THEN 2.5
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

--- Calcolo costi realistici
UPDATE CONSUMI_ENERGIA SET
    COSTO_ENERGIA_EUR = ROUND(CONSUMO_KWH * UNIFORM(0.18, 0.35, RANDOM()) + CONSUMO_SMC * UNIFORM(0.80, 1.40, RANDOM()), 2),
    COSTO_TRASPORTO_EUR = ROUND((CONSUMO_KWH * 0.03) + (CONSUMO_SMC * 0.05), 2),
    COSTO_ONERI_EUR = ROUND((CONSUMO_KWH * 0.02) + (CONSUMO_SMC * 0.03), 2);

UPDATE CONSUMI_ENERGIA SET
    COSTO_TOTALE_EUR = COSTO_ENERGIA_EUR + COSTO_TRASPORTO_EUR + COSTO_ONERI_EUR;

--- INSERT: FEEDBACK_CLIENTI (500 record)
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

--- INSERT: DOCUMENTI_TECNICI (30 record)
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

--- INSERT: IMPIANTI (15 record)
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

--- INSERT: PRODUZIONE_ENERGIA (5400+ record - 365 giorni x 15 impianti)
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

--- 1.4 Verifica Dati
SELECT 'CLIENTI' AS TABELLA, COUNT(*) AS RECORD FROM CLIENTI
UNION ALL SELECT 'CONSUMI_ENERGIA', COUNT(*) FROM CONSUMI_ENERGIA
UNION ALL SELECT 'FEEDBACK_CLIENTI', COUNT(*) FROM FEEDBACK_CLIENTI
UNION ALL SELECT 'DOCUMENTI_TECNICI', COUNT(*) FROM DOCUMENTI_TECNICI
UNION ALL SELECT 'IMPIANTI', COUNT(*) FROM IMPIANTI
UNION ALL SELECT 'PRODUZIONE_ENERGIA', COUNT(*) FROM PRODUZIONE_ENERGIA;
