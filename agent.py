import os
import torch
import pandas as pd
from transformers import pipeline
from dotenv import load_dotenv
from mcp_monkdb.mcp_server import run_select_query

load_dotenv()

# === Load TinyLlama ===
pipe = pipeline(
    "text-generation",
    model="TinyLlama/TinyLlama-1.1B-Chat-v1.0",
    torch_dtype=torch.bfloat16,
    device_map="auto",
)

# === Updated System Prompt ===
SYSTEM_PROMPT = """
You are a sharp data analyst powered by MonkDB. When given a SQL result (in table form), your task is to extract actionable insights.

Follow these principles:
- Identify patterns, trends, clusters, outliers, comparisons, or anomalies.
- Use actual numbers and column names. Always reference real data from the table.
- DO NOT invent columns, values, or summaries beyond what's in the table.
- Avoid repeating the query. Focus on what the result implies.
- Tone: crisp, analytical, CXO-level. No generic instructions or filler language.

Return 3-5 bullet points. Each bullet must convey one key insight or takeaway.
"""

# === Helper: Tabular summary for TinyLlama ===


def generate_data_summary(df: pd.DataFrame) -> str:
    lines = []
    lines.append(f"📊 Columns: {', '.join(df.columns)}")
    lines.append(f"🧮 Rows: {len(df)}")

    for col in df.columns:
        if pd.api.types.is_numeric_dtype(df[col]):
            desc = df[col].describe()
            lines.append(
                f"📈 {col}: min={desc['min']:.2f}, max={desc['max']:.2f}, avg={desc['mean']:.2f}"
            )
        elif df[col].nunique() < 10:
            top_vals = df[col].value_counts().nlargest(3)
            formatted = ", ".join(f"{k} ({v})" for k, v in top_vals.items())
            lines.append(f"🗂️ {col}: {formatted}")
        else:
            lines.append(f"📁 {col}: {df[col].nunique()} unique values")

    return "\n".join(lines)

# === Query Runner ===


def query_monkdb(sql: str) -> tuple[str, pd.DataFrame]:
    try:
        result = run_select_query(sql)
        if isinstance(result, dict) and result.get("status") == "error":
            return f"❌ Query failed: {result['message']}", pd.DataFrame()
        if isinstance(result, list) and result:
            df = pd.DataFrame(result)
            return "", df.head(10)
        return "No results found.", pd.DataFrame()
    except Exception as e:
        return f"MCP query error: {e}", pd.DataFrame()

# === Main Response Generator ===


def generate_response(user_input: str) -> str:
    user_input = user_input.strip()

    # If it's a SQL query
    if user_input.lower().startswith("select"):
        error_msg, df = query_monkdb(user_input)
        if error_msg:
            return error_msg
        data_summary = generate_data_summary(df)
        prompt = pipe.tokenizer.apply_chat_template([
            {"role": "system", "content": SYSTEM_PROMPT.strip()},
            {"role": "user",
                "content": f"Here are the top SQL results:\n{df.to_string(index=False)}\n\nHere is a summary:\n{data_summary}\n\nSummarize or explain the insights."}
        ], tokenize=False, add_generation_prompt=True)
    else:
        prompt = pipe.tokenizer.apply_chat_template([
            {"role": "system", "content": SYSTEM_PROMPT.strip()},
            {"role": "user", "content": user_input}
        ], tokenize=False, add_generation_prompt=True)

    outputs = pipe(
        prompt,
        max_new_tokens=512,
        do_sample=True,
        temperature=0.7,
        top_p=0.95,
        eos_token_id=pipe.tokenizer.eos_token_id,
    )
    generated_text = outputs[0]["generated_text"]
    return generated_text[len(prompt):].strip()
