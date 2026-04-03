import os
import json
import snowflake.connector
import requests

conn = snowflake.connector.connect(connection_name=os.getenv("SNOWFLAKE_CONNECTION_NAME") or "demo_cgavazzeni")

API_ENDPOINT = "/api/v2/cortex/agent:run"
SEMANTIC_VIEW = "POWERUTILITY.PUBLIC.A2A_ENERGY_SEMANTIC_VIEW"

payload = {
    "model": "claude-3-5-sonnet",
    "messages": [
        {"role": "user", "content": [{"type": "text", "text": "Quanti clienti ci sono per tipologia?"}]}
    ],
    "tools": [
        {"tool_spec": {"type": "cortex_analyst_text_to_sql", "name": "Analisi_Dati"}}
    ],
    "tool_resources": {
        "Analisi_Dati": {
            "semantic_view": SEMANTIC_VIEW,
            "execution_environment": {"type": "warehouse", "warehouse": "COMPUTE_WH"}
        }
    },
    "response_instruction": "Rispondi in italiano."
}

print("Calling Cortex Agent API...")
print(f"Payload keys: {list(payload.keys())}")

try:
    cursor = conn.cursor()
    
    host = conn.host
    token = conn.rest.token
    
    url = f"https://{host}{API_ENDPOINT}"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Snowflake Token=\"{token}\"",
        "Accept": "application/json"  
    }
    
    resp = requests.post(url, json={**payload, "stream": False}, headers=headers, timeout=120)
    print(f"\nHTTP Status: {resp.status_code}")
    print(f"Response length: {len(resp.text)}")
    
    if resp.status_code == 200:
        data = resp.json()
        print(f"\nResponse type: {type(data)}")
        print(f"Response keys: {list(data.keys()) if isinstance(data, dict) else 'list'}")
        
        if isinstance(data, dict) and "content" in data:
            for i, item in enumerate(data["content"]):
                item_type = item.get("type", "unknown")
                print(f"\n--- Content item {i}: type={item_type}")
                if item_type == "text":
                    print(f"  text: {item.get('text', '')[:200]}")
                elif item_type == "tool_result":
                    tr = item.get("tool_result", {})
                    print(f"  tool: {tr.get('name', 'unknown')}, status: {tr.get('status', 'unknown')}")
                    for j, c in enumerate(tr.get("content", [])):
                        if c.get("type") == "json":
                            jd = c.get("json", {})
                            print(f"  json keys: {list(jd.keys())}")
                            if "sql" in jd:
                                print(f"  SQL: {jd['sql'][:200]}")
                            if "text" in jd:
                                print(f"  text: {jd['text'][:200]}")
                elif item_type == "tool_use":
                    tu = item.get("tool_use", {})
                    print(f"  tool_use: name={tu.get('name')}, type={tu.get('type')}")
                else:
                    print(f"  raw: {json.dumps(item)[:300]}")
        else:
            print(f"\nFull response (first 2000 chars):\n{json.dumps(data, indent=2)[:2000]}")
    else:
        print(f"\nError response: {resp.text[:1000]}")

except Exception as e:
    print(f"Error: {e}")
    import traceback
    traceback.print_exc()
finally:
    conn.close()
