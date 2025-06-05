from llama_cpp import Llama
import os
from mcp_monkdb.mcp_server import run_select_query

llm = Llama(
    model_path="TinyLlama-1.1B.Q4_K_M.gguf",
    n_ctx=2048,
    n_threads=os.cpu_count(),
    temperature=0.7,
    top_p=0.95,
    repeat_penalty=1.1,
    verbose=False,
)

SYSTEM_PROMPT = """
You are a helpful geospatial assistant backed by MonkDB. Your job is to answer spatial questions by issuing SELECT queries via MonkDB's run_select_query tool.
Use SQL-like natural queries. Reply briefly with insights. Avoid full table dumps unless asked.
"""


def query_monkdb(sql: str) -> str:
    result = run_select_query(sql)
    if isinstance(result, dict) and result.get("status") == "error":
        return f"❌ Query failed: {result['message']}"
    if isinstance(result, list):
        return "\n".join(str(row) for row in result[:10])  # limit output
    return str(result)


def generate_response(user_input: str) -> str:
    prompt = f"""[INST] <<SYS>>{SYSTEM_PROMPT}<</SYS>>
{user_input} [/INST]"""

    response = llm(
        prompt,
        max_tokens=512,
        stop=["</s>", "[/INST]"],
        echo=False,
    )
    return response["choices"][0]["text"].strip()
