"""统一 DeepSeek 调用：记录 token、缓存命中、延迟与成败，供中控台读取。

所有平台都从这里走模型，避免各自漏记 usage。
"""
import json
import os
import sqlite3
import time
from contextlib import closing

import requests

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB = os.environ.get("METRICS_DB") or os.path.join(ROOT, "metrics.db")
API = "https://api.deepseek.com/chat/completions"
DEFAULT_MODEL = "deepseek-chat"

SCHEMA = """
CREATE TABLE IF NOT EXISTS llm_calls(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ts INTEGER NOT NULL,
    platform TEXT,
    kind TEXT,
    model TEXT,
    prompt_tokens INTEGER DEFAULT 0,
    completion_tokens INTEGER DEFAULT 0,
    cache_hit_tokens INTEGER DEFAULT 0,
    cache_miss_tokens INTEGER DEFAULT 0,
    latency_ms INTEGER DEFAULT 0,
    ok INTEGER DEFAULT 1,
    error TEXT
);
CREATE INDEX IF NOT EXISTS idx_llm_calls_ts ON llm_calls(ts);
CREATE TABLE IF NOT EXISTS events(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ts INTEGER NOT NULL,
    platform TEXT,
    kind TEXT,
    detail TEXT,
    meta TEXT
);
CREATE INDEX IF NOT EXISTS idx_events_ts ON events(ts);
CREATE INDEX IF NOT EXISTS idx_events_platform_ts ON events(platform, ts);
"""

_ready = False


def _conn():
    c = sqlite3.connect(DB, timeout=10)
    c.execute("PRAGMA busy_timeout=5000")
    return c


def init():
    global _ready
    if _ready:
        return
    with closing(_conn()) as c:
        c.executescript(SCHEMA)
        c.commit()
    _ready = True


def _record_call(platform, kind, model, prompt, completion, hit, miss, latency, ok, error):
    try:
        init()
        with closing(_conn()) as c:
            c.execute(
                """INSERT INTO llm_calls(ts,platform,kind,model,prompt_tokens,completion_tokens,
                       cache_hit_tokens,cache_miss_tokens,latency_ms,ok,error)
                   VALUES(?,?,?,?,?,?,?,?,?,?,?)""",
                (int(time.time()), platform, kind, model, prompt, completion, hit, miss,
                 latency, 1 if ok else 0, (error or "")[:400]))
            c.commit()
    except sqlite3.Error:
        pass


def event(platform, kind, detail="", **meta):
    """写一条行为事件。埋点失败不能影响主流程。"""
    try:
        init()
        with closing(_conn()) as c:
            c.execute("INSERT INTO events(ts,platform,kind,detail,meta) VALUES(?,?,?,?,?)",
                      (int(time.time()), platform, kind, str(detail or "")[:600],
                       json.dumps(meta, ensure_ascii=False)[:1000]))
            c.commit()
    except sqlite3.Error:
        pass


def count_events(platform, kind, since):
    """数一数某个平台某类事件在 since 之后发生了多少次（用来做额度）。"""
    init()
    with closing(_conn()) as c:
        return int(c.execute(
            "SELECT COUNT(*) FROM events WHERE platform=? AND kind=? AND ts>=?",
            (platform, kind, int(since))).fetchone()[0])


def chat(platform, messages, *, model=None, temperature=0.9, max_tokens=300,
         timeout=60, kind="chat", thinking=False):
    """调用 DeepSeek，返回正文；usage 写入 metrics.db。

    deepseek-flash 默认开启思考模式，短回复场景会白烧 token 甚至把正文挤空，
    所以默认关闭；需要深想时传 thinking=True。
    """
    model = model or os.environ.get("DEEPSEEK_MODEL", "").strip() or DEFAULT_MODEL
    key = os.environ.get("DEEPSEEK_API_KEY", "").strip()
    if not key:
        _record_call(platform, kind, model, 0, 0, 0, 0, 0, False, "缺 DEEPSEEK_API_KEY")
        raise RuntimeError("缺 DEEPSEEK_API_KEY")

    payload = {"model": model, "messages": messages, "temperature": temperature,
               "max_tokens": max_tokens}
    if not thinking:
        payload["thinking"] = {"type": "disabled"}

    t0 = time.time()
    try:
        r = requests.post(API, timeout=timeout, headers={
            "Authorization": f"Bearer {key}", "Content-Type": "application/json"},
            json=payload)
        latency = int((time.time() - t0) * 1000)
        if r.status_code >= 400:
            try:
                msg = (r.json().get("error") or {}).get("message") or r.text[:200]
            except ValueError:
                msg = r.text[:200]
            _record_call(platform, kind, model, 0, 0, 0, 0, latency, False,
                         f"HTTP {r.status_code}: {msg}")
            r.raise_for_status()
        data = r.json()
        content = ((data.get("choices") or [{}])[0].get("message") or {}).get("content") or ""
        usage = data.get("usage") or {}
        prompt = int(usage.get("prompt_tokens") or 0)
        completion = int(usage.get("completion_tokens") or 0)
        hit = int(usage.get("prompt_cache_hit_tokens") or 0)
        miss = int(usage.get("prompt_cache_miss_tokens") or max(prompt - hit, 0))
        _record_call(platform, kind, model, prompt, completion, hit, miss, latency, True, "")
        return content.strip()
    except Exception as e:
        if not isinstance(e, requests.HTTPError):
            _record_call(platform, kind, model, 0, 0, 0, 0,
                         int((time.time() - t0) * 1000), False, f"{type(e).__name__}: {e}")
        raise
