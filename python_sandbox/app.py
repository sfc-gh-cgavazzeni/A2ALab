#!/usr/bin/env python3
"""
Python Sandbox Service for Snowpark Container Services
Executes Python code safely and returns results as JSON
"""

import json
import sys
import traceback
import io
import contextlib
import threading
import time
from typing import Dict, Any
from flask import Flask, request, jsonify
import logging

app = Flask(__name__)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ExecutionTimeoutError(Exception):
    pass

def execute_python_code(code: str, timeout: int = 90) -> Dict[str, Any]:
    result = {
        "success": False,
        "output": "",
        "error": None,
        "execution_time": 0
    }

    start_time = time.time()

    stdout_buffer = io.StringIO()
    stderr_buffer = io.StringIO()

    execution_state = {
        "completed": False,
        "exception": None,
        "output": "",
        "error_output": ""
    }

    def execute_code():
        try:
            exec_globals = globals().copy()
            exec_globals.update({
                'json': __import__('json'),
                'math': __import__('math'),
                'datetime': __import__('datetime'),
                're': __import__('re'),
                'random': __import__('random'),
                'os': __import__('os'),
                'sys': __import__('sys'),
                'flask': __import__('flask'),
                'Flask': __import__('flask').Flask,
                'werkzeug': __import__('werkzeug'),
                'requests': __import__('requests'),
                'numpy': __import__('numpy'),
                'np': __import__('numpy'),
                'pandas': __import__('pandas'),
                'pd': __import__('pandas'),
                'scipy': __import__('scipy'),
                'matplotlib': __import__('matplotlib'),
                'plt': __import__('matplotlib.pyplot'),
                'seaborn': __import__('seaborn'),
                'sns': __import__('seaborn'),
                'sklearn': __import__('sklearn'),
                'scikit_learn': __import__('sklearn'),
                'prophet': __import__('prophet'),
                'cmdstanpy': __import__('cmdstanpy'),
            })
            exec_locals = {}

            with contextlib.redirect_stdout(stdout_buffer), \
                 contextlib.redirect_stderr(stderr_buffer):
                exec(code, exec_globals, exec_locals)

            execution_state["output"] = stdout_buffer.getvalue()
            execution_state["error_output"] = stderr_buffer.getvalue()
            execution_state["completed"] = True

        except Exception as e:
            execution_state["exception"] = e
            execution_state["output"] = stdout_buffer.getvalue()
            execution_state["error_output"] = stderr_buffer.getvalue()
            execution_state["completed"] = True

    try:
        thread = threading.Thread(target=execute_code, daemon=True)
        thread.start()
        thread.join(timeout=timeout)

        if thread.is_alive():
            result["error"] = f"Code execution timed out after {timeout} seconds"
        elif execution_state["completed"]:
            if execution_state["exception"]:
                e = execution_state["exception"]
                if isinstance(e, SyntaxError):
                    result["error"] = f"Syntax Error: {str(e)}"
                else:
                    error_details = {
                        "type": type(e).__name__,
                        "message": str(e),
                        "traceback": traceback.format_exc()
                    }
                    result["error"] = error_details
            else:
                if execution_state["error_output"]:
                    result["error"] = execution_state["error_output"]
                else:
                    result["success"] = True

            result["output"] = execution_state["output"]
        else:
            result["error"] = "Execution completed with unknown state"

    except Exception as e:
        error_details = {
            "type": type(e).__name__,
            "message": str(e),
            "traceback": traceback.format_exc()
        }
        result["error"] = error_details

    result["execution_time"] = time.time() - start_time
    return result

@app.route('/health', methods=['GET'])
def health_check():
    return jsonify({"status": "healthy", "service": "python-sandbox"})

@app.route('/execute', methods=['POST'])
def execute_code_endpoint():
    try:
        data = request.get_json()

        if not data:
            return jsonify({
                "success": False,
                "error": "Missing request body"
            }), 400

        if 'data' in data:
            return handle_snowflake_format(data)
        elif 'code' in data:
            return handle_legacy_format(data)
        else:
            return jsonify({
                "success": False,
                "error": "Invalid request format. Expected 'data' array (Snowflake format) or 'code' string (legacy format)"
            }), 400

    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        return jsonify({
            "success": False,
            "error": f"Internal server error: {str(e)}"
        }), 500

def handle_snowflake_format(data):
    try:
        rows = data.get('data', [])
        if not isinstance(rows, list):
            return jsonify({
                "success": False,
                "error": "'data' must be an array"
            }), 400

        results = []
        timeout = 600
        for row in rows:
            if not isinstance(row, list) or len(row) < 2:
                return jsonify({
                    "success": False,
                    "error": "Each row must be an array with at least [row_index, code]"
                }), 400

            row_index = row[0]
            code = row[1]

            if not isinstance(code, str):
                result = {
                    "success": False,
                    "output": "",
                    "error": "Code must be a string",
                    "execution_time": 0
                }
                results.append([row_index, result])
                continue

            logger.info(f"Executing code for row {row_index}")
            result = execute_python_code(code, timeout)
            results.append([row_index, result])

        return jsonify({"data": results})

    except Exception as e:
        logger.error(f"Error in Snowflake format handler: {str(e)}")
        return jsonify({
            "success": False,
            "error": f"Error processing Snowflake format: {str(e)}"
        }), 500

def handle_legacy_format(data):
    try:
        code = data['code']
        timeout = data.get('timeout', 30)

        if not isinstance(code, str):
            return jsonify({
                "success": False,
                "error": "'code' parameter must be a string"
            }), 400

        if not isinstance(timeout, int) or timeout <= 0 or timeout > 300:
            return jsonify({
                "success": False,
                "error": "'timeout' must be an integer between 1 and 300 seconds"
            }), 400

        logger.info(f"Executing code with timeout {timeout}s")
        result = execute_python_code(code, timeout)
        return jsonify(result)

    except Exception as e:
        logger.error(f"Error in legacy format handler: {str(e)}")
        return jsonify({
            "success": False,
            "error": f"Error processing legacy format: {str(e)}"
        }), 500

@app.route('/', methods=['GET'])
def root():
    return jsonify({
        "service": "Python Sandbox",
        "version": "1.0.0",
        "description": "Execute Python code and return results as JSON for Snowflake Service Functions",
        "endpoints": {
            "/health": "GET - Health check",
            "/execute": "POST - Execute Python code (Snowflake or legacy format)",
            "/": "GET - Service information"
        }
    })

if __name__ == '__main__':
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 8080
    app.run(host='0.0.0.0', port=port, debug=False)
