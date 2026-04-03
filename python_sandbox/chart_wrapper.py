import json
import uuid
import base64
import io

def run(session, python_code: str, timeout_seconds: int):
    escaped_code = python_code.replace("'", "''")
    query = f"SELECT POWERUTILITY.PUBLIC.EXECUTE_PYTHON('{escaped_code}', {timeout_seconds})"
    result_raw = session.sql(query).collect()[0][0]
    result = json.loads(result_raw)

    if not result.get('success'):
        return json.dumps({"success": False, "error": result.get('error', 'Unknown error')})

    output = result.get('output', '')
    stripped = output.strip()

    if len(stripped) > 1000:
        try:
            img_bytes = base64.b64decode(stripped)
            if img_bytes[:4] == b'\x89PNG' or img_bytes[:2] == b'\xff\xd8':
                file_name = f"chart_{uuid.uuid4().hex[:12]}.png"

                session.file.put_stream(
                    input_stream=io.BytesIO(img_bytes),
                    stage_location=f"@POWERUTILITY.PUBLIC.CHART_IMAGES/{file_name}",
                    auto_compress=False,
                    overwrite=True
                )

                presigned_url_result = session.sql(
                    f"SELECT GET_PRESIGNED_URL(@POWERUTILITY.PUBLIC.CHART_IMAGES, '{file_name}', 3600)"
                ).collect()
                presigned_url = presigned_url_result[0][0]

                return json.dumps({
                    "success": True,
                    "chart_url": presigned_url,
                    "message": f"![Chart]({presigned_url})"
                })
        except Exception:
            pass

    return json.dumps({
        "success": True,
        "output": output
    })
