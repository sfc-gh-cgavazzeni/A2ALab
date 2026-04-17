--- ============================================================
--- FASE 4: Configurazione Snowflake Intelligence
--- ============================================================

USE ROLE ACCOUNTADMIN;
USE DATABASE POWERUTILITY;
USE SCHEMA PUBLIC;

--- NOTA: E' possibile creare l'agente anche dalla UI Snowsight:
--- AI & ML > Snowflake Intelligence > + Intelligence

--- Creazione dell'agente Snowflake Intelligence via SQL
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

--- Verificare la creazione
SHOW AGENTS LIKE 'A2A_ENERGY_ASSISTANT' IN SCHEMA POWERUTILITY.PUBLIC;

--- Descrivere l'agente
DESCRIBE AGENT POWERUTILITY.PUBLIC.A2A_ENERGY_ASSISTANT;
