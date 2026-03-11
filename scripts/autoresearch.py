import ast
import os
import sys
import subprocess
import json
import time
from pathlib import Path

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# Try to use OpenAI by default, otherwise Anthropic
try:
    import openai
    HAS_OPENAI = True
except ImportError:
    HAS_OPENAI = False

try:
    from google import genai
    HAS_GEMINI = True
except ImportError:
    HAS_GEMINI = False

SYSTEM_PROMPT = """You are an elite quantitative researcher and Python developer.
Your goal is to improve the profitability of a Hyperliquid trading strategy by editing its Python code.
You will be provided with the current codebase of the strategy and the results of its most recent Walk-Forward historical backtest.

The walk-forward evaluator tests the strategy across multiple train/validation folds. 
The SCORE is the primary metric we want to maximize (it combines PnL, Profit Factor, robustness across regimes, etc).

RULES:
1. You may change parameters (lookbacks, thresholds, sizing, bps).
2. You may add new logic or modify existing logic (indicators, entry/exit conditions, state tracking).
3. Do NOT break the interface (must subclass BaseStrategy, on_tick MUST always return List[StrategyDecision], NEVER return None).
4. Return ONLY the COMPLETE new python code inside a ```python block. Do not return markdown explanations outside the block.
5. Ensure your code is syntactically correct — all parentheses, brackets, and strings must be properly closed.
6. Do NOT truncate the code. Include ALL methods and the FULL on_tick implementation.
"""

def get_llm_response(prompt: str) -> str:
    if HAS_GEMINI and (os.environ.get("GEMINI_API_KEY") or os.environ.get("GOOGLE_API_KEY")):
        api_key = os.environ.get("GEMINI_API_KEY") or os.environ.get("GOOGLE_API_KEY")
        client = genai.Client(api_key=api_key)
        response = client.models.generate_content(
            model="gemini-2.5-flash",
            contents=prompt,
            config=genai.types.GenerateContentConfig(
                system_instruction=SYSTEM_PROMPT,
                max_output_tokens=16384
            )
        )
        return response.text
    elif HAS_ANTHROPIC and os.environ.get("ANTHROPIC_API_KEY"):
        client = anthropic.Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])
        response = client.messages.create(
            model="claude-3-5-sonnet-20241022",
            max_tokens=8192,
            system=SYSTEM_PROMPT,
            messages=[{"role": "user", "content": prompt}]
        )
        return response.content[0].text
    elif HAS_OPENAI and os.environ.get("OPENAI_API_KEY"):
        client = openai.OpenAI(api_key=os.environ["OPENAI_API_KEY"])
        response = client.chat.completions.create(
            model="gpt-4o",
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": prompt}
            ]
        )
        return response.choices[0].message.content
    else:
        raise RuntimeError("No valid LLM API key or library found. Install google-genai and set GEMINI_API_KEY.")

def extract_code(llm_out: str) -> str:
    if "```python" in llm_out:
        code = llm_out.split("```python")[1].split("```")[0]
    elif "```" in llm_out:
        code = llm_out.split("```")[1].split("```")[0]
    else:
        code = llm_out
    code = code.strip() + "\n"
    # Sanitize non-ASCII characters that Gemini likes to insert
    code = code.replace("\u2014", "--")   # em-dash
    code = code.replace("\u2013", "-")    # en-dash
    code = code.replace("\u2018", "'")    # left single quote
    code = code.replace("\u2019", "'")    # right single quote
    code = code.replace("\u201c", '"')    # left double quote
    code = code.replace("\u201d", '"')    # right double quote
    code = code.replace("\u2026", "...")  # ellipsis
    # Force ASCII-safe encoding
    code = code.encode('ascii', errors='replace').decode('ascii')
    return code

def run_evaluation(strategy_name: str, dataset_path: str):
    print(f"Running evaluation for {strategy_name} on {dataset_path}...")
    cmd = [
        "python", "-m", "cli.main", "research", "run",
        "--strategy", strategy_name,
        "--dataset", dataset_path,
        "--train-size", "500",
        "--validation-size", "168",
        "--json"
    ]
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        print("Evaluation failed:")
        print(result.stderr)
        return None
        
    try:
        report = json.loads(result.stdout)
        # Find score
        strategy_result = next((s for s in report["strategy_results"] if s["strategy_id"] == strategy_name), None)
        if not strategy_result:
            return None
        
        scorecard = strategy_result.get("dataset_breakdown", [{}])[0].get("scorecard", {})
        score = scorecard.get("score", 0.0)
        return {
            "score": score,
            "report_summary": scorecard,
            "full_report": report
        }
    except Exception as e:
        print("Failed to parse output:", e)
        return None

def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--strategy", required=True, help="Strategy name (e.g. momentum_breakout)")
    parser.add_argument("--strategy-file", required=True, help="Path to strategy python file")
    parser.add_argument("--dataset", required=True, help="Path to JSONL dataset")
    parser.add_argument("--iterations", type=int, default=10, help="Number of LLM iterations")
    
    args = parser.parse_args()
    
    strategy_path = Path(args.strategy_file)
    if not strategy_path.exists():
        print(f"File {strategy_path} not found.")
        sys.exit(1)
        
    print("--- AUTORESEARCH LOOP STARTING ---")
    print(f"Target: {args.strategy}")
    print(f"Iterations: {args.iterations}")
    
    # 1. Baseline Run
    baseline = run_evaluation(args.strategy, args.dataset)
    if not baseline:
        print("Initial baseline failed. Exiting.")
        sys.exit(1)
        
    best_score = baseline["score"]
    best_code = strategy_path.read_text(encoding='utf-8')
    
    print(f"\n[BASELINE] Starting Score: {best_score:.2f}")
    
    for i in range(1, args.iterations + 1):
        print(f"\n=== Iteration {i}/{args.iterations} ===")
        # Ensure we always start from the currently accepted best code
        current_code = strategy_path.read_text(encoding='utf-8')
        
        # Build prompt
        prompt = f"""
Here is the current code for `{args.strategy}`:

```python
{current_code}
```

Here is the evaluation report from the Walk-Forward Backtester:
{json.dumps(baseline['report_summary'], indent=2)}

Please rewrite the strategy code to improve its Score and Profit Factor. Do not change the class name.
"""
        print("Generating new hypothesis with LLM...")
        try:
            llm_response = get_llm_response(prompt)
        except Exception as e:
            print(f"LLM API Error: {e}")
            continue
            
        new_code = extract_code(llm_response)
        
        if not new_code or len(new_code) < 50:
            print("Invalid code returned. Skipping.")
            continue
        
        # Validate syntax before writing to disk
        try:
            ast.parse(new_code)
        except SyntaxError as e:
            print(f"[SKIP] LLM produced invalid syntax: {e}")
            continue
            
        # Write new code
        print("Testing new strategy code...")
        strategy_path.write_text(new_code, encoding='utf-8')
        
        time.sleep(1) # buffer
        
        new_eval = run_evaluation(args.strategy, args.dataset)
        if not new_eval:
            print("[FAIL] Code crashed or evaluation failed. Reverting.")
            strategy_path.write_text(best_code, encoding='utf-8')
            continue
            
        new_score = new_eval["score"]
        
        if new_score > best_score:
            print(f"[SUCCESS] Score improved: {best_score:.2f} -> {new_score:.2f}")
            best_score = new_score
            best_code = new_code
            baseline = new_eval # Update baseline report for next iteration
        else:
            print(f"[REJECT] Score degraded: {best_score:.2f} vs {new_score:.2f}. Reverting.")
            strategy_path.write_text(best_code, encoding='utf-8')
            
    print("\n--- AUTORESEARCH COMPLETE ---")
    print(f"Final Best Score: {best_score:.2f}")
    print(f"Best code saved to {strategy_path}")

if __name__ == "__main__":
    main()
