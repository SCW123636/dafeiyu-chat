"""大肥鱼中控台：本地面板后端（127.0.0.1:3002）。

只读聚合 metrics.db / bili_play.db / dy_play.db，并通过子进程管理三端任务。
"""
import argparse
import ast
import json
import os
import re
import socket
import sqlite3
import subprocess
import sys
import threading
import time
import webbrowser
from collections import deque
from contextlib import closing

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

import requests
import uvicorn
from fastapi import Depends, FastAPI, HTTPException, Request
from fastapi.responses import FileResponse
from pydantic import BaseModel

from core.llm import event as log_event, init as metrics_init
from envload import load_env

load_env()
metrics_init()

PANEL_DIR = os.path.dirname(os.path.abspath(__file__))
INDEX = os.path.join(PANEL_DIR, "index.html")
LOG_DIR = os.path.join(PANEL_DIR, "logs")
DB_METRICS = os.path.join(ROOT, "metrics.db")
DB_BILI = os.path.join(ROOT, "bili_play.db")
DB_DY = os.path.join(ROOT, "dy_play.db")
QR_BILI = os.path.join(ROOT, "bili_login_qr.png")
START = time.time()
PORT = int(os.environ.get("PANEL_PORT", "3002"))
UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36")
CREATE_NO_WINDOW = 0x08000000 if os.name == "nt" else 0

os.makedirs(LOG_DIR, exist_ok=True)


def _read_dotenv():
    out = {}
    path = os.path.join(ROOT, ".env")
    try:
        with open(path, encoding="utf-8-sig") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                k, v = line.split("=", 1)
                out[k.strip()] = v.strip().strip("\"'")
    except OSError:
        pass
    return out


def _refresh_env():
    for k, v in _read_dotenv().items():
        os.environ[k] = v


# ---------------- 进程管理 ----------------

class Runner:
    def __init__(self, key, label):
        self.key = key
        self.label = label
        self.proc = None
        self.started = 0.0
        self.ended = 0.0
        self.exit_code = None
        self.kind = ""
        self.info = {}
        self.logs = deque(maxlen=4000)
        self.seq = 0
        self.lock = threading.RLock()

    def running(self):
        p = self.proc
        return p is not None and p.poll() is None

    def add(self, line):
        line = (line or "").rstrip("\r\n")
        with self.lock:
            self.seq += 1
            self.logs.append((self.seq, int(time.time()), line))
        try:
            with open(os.path.join(LOG_DIR, f"{self.key}.log"), "a", encoding="utf-8") as f:
                f.write(f"{time.strftime('%Y-%m-%d %H:%M:%S')} {line}\n")
        except OSError:
            pass

    def start(self, cmd, *, kind, info=None):
        with self.lock:
            if self.proc is not None and self.proc.poll() is None:
                raise RuntimeError(f"{self.label}正在运行")
            _refresh_env()
            env = os.environ.copy()
            env.update({"PYTHONIOENCODING": "utf-8", "PYTHONUTF8": "1",
                        "PYTHONUNBUFFERED": "1"})
            try:
                p = subprocess.Popen(
                    cmd, cwd=ROOT, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                    text=True, encoding="utf-8", errors="replace", bufsize=1,
                    env=env, creationflags=CREATE_NO_WINDOW)
            except Exception as e:
                self.add(f"[启动失败] {type(e).__name__}: {e}")
                raise
            self.proc = p
            self.started = time.time()
            self.ended = 0.0
            self.exit_code = None
            self.kind = kind
            self.info = info or {}
        self.add(f"[启动] {subprocess.list2cmdline(cmd)}  pid={p.pid}")
        threading.Thread(target=self._pump, args=(p,), daemon=True).start()
        return p.pid

    def _pump(self, proc):
        try:
            for line in proc.stdout:
                self.add(line)
        except Exception as e:
            self.add(f"[读取日志失败] {type(e).__name__}: {e}")
        rc = proc.wait()
        with self.lock:
            self.exit_code = rc
            self.ended = time.time()
        self.add(f"[进程退出] code={rc}")

    def stop(self):
        with self.lock:
            p = self.proc
            if p is None or p.poll() is not None:
                return False
            self.add("[停止] 发送终止信号")
            try:
                p.terminate()
            except Exception:
                return False
        try:
            p.wait(timeout=5)
        except subprocess.TimeoutExpired:
            try:
                p.kill()
            except Exception:
                pass
        return True

    def state(self):
        p = self.proc
        running = p is not None and p.poll() is None
        with self.lock:
            recent = [x for _, _, x in list(self.logs)[-150:]]
        headline = ""
        for line in reversed(recent):
            if any(k in line for k in ("大肥鱼 进入", "NapCat 已连接", "二维码已保存",
                                       "登录成功", "收工", "进程退出")):
                headline = line.strip()
                break
        return {
            "key": self.key,
            "label": self.label,
            "running": running,
            "kind": self.kind,
            "pid": p.pid if p else None,
            "started": int(self.started) if self.started else 0,
            "ended": int(self.ended) if self.ended else 0,
            "elapsed": int(time.time() - self.started) if running else 0,
            "exit_code": self.exit_code,
            "info": self.info,
            "headline": headline,
        }

    def logs_since(self, since):
        with self.lock:
            seq = self.seq
            lines = [{"seq": s, "ts": t, "text": x}
                     for s, t, x in self.logs if s > since]
        return {"seq": seq, "lines": lines}


RUNNERS = {
    "bili": Runner("bili", "B站任务"),
    "dy": Runner("dy", "抖音任务"),
    "qq": Runner("qq", "QQ 任务"),
    "desk": Runner("desk", "运营台"),
    "login_bili": Runner("login_bili", "B站登录"),
    "login_dy": Runner("login_dy", "抖音登录"),
    "check_dy": Runner("check_dy", "抖音检测"),
}


def _python(*args):
    return [sys.executable, "-u", *args]


def _port_open(port):
    try:
        with socket.create_connection(("127.0.0.1", port), timeout=0.35):
            return True
    except OSError:
        return False


_PID_CACHE = {"ts": 0.0, "port": 0, "pid": None}


def _port_pid(port):
    if _PID_CACHE["port"] == port and time.time() - _PID_CACHE["ts"] < 5:
        return _PID_CACHE["pid"]
    pid = None
    try:
        out = subprocess.run(["netstat", "-ano", "-p", "TCP"], capture_output=True,
                             text=True, timeout=5).stdout
        for line in out.splitlines():
            parts = line.split()
            if (len(parts) >= 5 and parts[1].endswith(f":{port}")
                    and parts[3].upper() == "LISTENING"):
                pid = int(parts[4])
                break
    except Exception:
        pass
    _PID_CACHE.update({"ts": time.time(), "port": port, "pid": pid})
    return pid


def _pid_cmdline(pid):
    try:
        script = ("[Console]::OutputEncoding=[Text.Encoding]::UTF8; "
                  f"(Get-CimInstance Win32_Process -Filter \"ProcessId={pid}\").CommandLine")
        out = subprocess.run(["powershell", "-NoProfile", "-Command", script],
                             capture_output=True, text=True, encoding="utf-8",
                             errors="replace", timeout=8).stdout
        return (out or "").strip()
    except Exception:
        return ""


def _qq_port():
    try:
        return int(os.environ.get("QQ_WS_PORT", "3001") or 3001)
    except ValueError:
        return 3001


# ---------------- 数据库 ----------------

def _q(path, sql, args=()):
    if not os.path.exists(path):
        return []
    try:
        c = sqlite3.connect(path, timeout=3)
        c.row_factory = sqlite3.Row
        c.execute("PRAGMA query_only=ON")
        try:
            return [dict(r) for r in c.execute(sql, args).fetchall()]
        finally:
            c.close()
    except sqlite3.Error:
        return []


def _one(path, sql, args=(), default=None):
    rows = _q(path, sql, args)
    return rows[0] if rows else default


def _today():
    lt = time.localtime()
    return int(time.mktime((lt.tm_year, lt.tm_mon, lt.tm_mday, 0, 0, 0, 0, 0, -1)))


def _meta(row):
    try:
        return json.loads(row.get("meta") or "{}")
    except (ValueError, AttributeError):
        return {}


# ---------------- 账号与配置 ----------------

_NAV = {"ts": 0.0, "data": None, "busy": False}
_NAV_LOCK = threading.Lock()


def _refresh_nav():
    with _NAV_LOCK:
        if _NAV["busy"]:
            return
        _NAV["busy"] = True
    try:
        _refresh_env()
        env = _read_dotenv()
        if not env.get("BILI_SESSDATA"):
            data = {"logged_in": False, "verified": False, "uname": "", "level": None,
                    "uid": env.get("BILI_UID", ""), "reason": "未配置 SESSDATA"}
        else:
            try:
                s = requests.Session()
                s.headers.update({"User-Agent": UA, "Referer": "https://www.bilibili.com/"})
                for key, name in (("BILI_SESSDATA", "SESSDATA"), ("BILI_JCT", "bili_jct"),
                                  ("BILI_UID", "DedeUserID")):
                    if env.get(key):
                        s.cookies.set(name, env[key], domain=".bilibili.com")
                j = s.get("https://api.bilibili.com/x/web-interface/nav", timeout=6).json()
                d = j.get("data") or {}
                data = {
                    "logged_in": bool(d.get("isLogin")),
                    "verified": True,
                    "uname": d.get("uname") or "",
                    "level": (d.get("level_info") or {}).get("current_level"),
                    "uid": str(d.get("mid") or env.get("BILI_UID") or ""),
                    "reason": "" if d.get("isLogin") else "登录态失效",
                }
            except Exception as e:
                data = {"logged_in": bool(env.get("BILI_SESSDATA")), "verified": False,
                        "uname": "", "level": None, "uid": env.get("BILI_UID", ""),
                        "reason": f"未联网校验（{type(e).__name__}）"}
        with _NAV_LOCK:
            _NAV["ts"] = time.time()
            _NAV["data"] = data
    finally:
        with _NAV_LOCK:
            _NAV["busy"] = False


def _nav_state():
    with _NAV_LOCK:
        ts = _NAV["ts"]
        data = _NAV["data"]
        busy = _NAV["busy"]
    if data is None:
        if not busy:
            threading.Thread(target=_refresh_nav, daemon=True).start()
        return {"logged_in": False, "verified": False, "uname": "", "level": None,
                "uid": "", "reason": "校验中"}
    if time.time() - ts > 45 and not busy:
        threading.Thread(target=_refresh_nav, daemon=True).start()
    return data


def _dy_account():
    state = os.path.join(ROOT, "dy_state.json")
    logged, method, ts = False, "none", 0
    try:
        with open(state, encoding="utf-8") as f:
            j = json.load(f)
        cookies = j.get("cookies") or []
        logged = any(c.get("name") == "sessionid" and c.get("value") for c in cookies)
        method = "state"
        ts = int(os.path.getmtime(state))
    except (OSError, ValueError):
        pass
    if not logged:
        for rel in ("dy_profile/Default/Network/Cookies", "dy_profile/Default/Cookies"):
            path = os.path.join(ROOT, rel)
            if not os.path.exists(path):
                continue
            try:
                c = sqlite3.connect(path, timeout=2)
                c.execute("PRAGMA query_only=ON")
                n = c.execute("SELECT COUNT(*) FROM cookies WHERE host_key LIKE '%douyin%' "
                              "AND name='sessionid' AND length(encrypted_value)>0").fetchone()[0]
                c.close()
                logged = n > 0
                method = "cookies"
                ts = int(os.path.getmtime(path))
                break
            except sqlite3.Error:
                continue
    return {"logged_in": bool(logged), "method": method, "ts": ts}


def _qq_state():
    port = _qq_port()
    opened = _port_open(port)
    runner = RUNNERS["qq"]
    st = runner.state()
    connected = False
    if st["running"]:
        with runner.lock:
            for _, _, line in reversed(runner.logs):
                if "NapCat 已连接" in line:
                    connected = True
                    break
                if "NapCat 断开" in line:
                    break
    return {
        "port": port,
        "port_open": opened,
        "external": opened and not st["running"],
        "external_pid": _port_pid(port) if opened and not st["running"] else None,
        "napcat_connected": connected if st["running"] else None,
        "runner": st,
    }


def _ast_value(path, name, default=None):
    try:
        with open(path, encoding="utf-8") as f:
            tree = ast.parse(f.read())
        for node in tree.body:
            if isinstance(node, ast.Assign):
                for target in node.targets:
                    if isinstance(target, ast.Name) and target.id == name:
                        return ast.literal_eval(node.value)
    except (OSError, SyntaxError, ValueError):
        pass
    return default


RISK = {
    "bili": {
        "min_gap": _ast_value(os.path.join(ROOT, "bili_play.py"), "MIN_GAP", 90),
        "max_per_hour": _ast_value(os.path.join(ROOT, "bili_play.py"), "MAX_PER_HOUR", 12),
        "max_per_run": _ast_value(os.path.join(ROOT, "bili_play.py"), "MAX_PER_RUN", 20),
        "min_new": _ast_value(os.path.join(ROOT, "bili_play.py"), "MIN_NEW_MSGS", 4),
    },
    "dy": {
        "min_gap": _ast_value(os.path.join(ROOT, "dy_agent.py"), "MIN_GAP", 120),
        "max_per_hour": _ast_value(os.path.join(ROOT, "dy_agent.py"), "MAX_PER_HOUR", 8),
        "max_per_run": _ast_value(os.path.join(ROOT, "dy_agent.py"), "MAX_PER_RUN", 10),
        "min_new": _ast_value(os.path.join(ROOT, "dy_agent.py"), "MIN_NEW_MSGS", 5),
    },
}

MODEL = os.environ.get("DEEPSEEK_MODEL", "").strip() or "deepseek-chat"
PERSONA = {
    "qq": {
        "text": os.environ.get("QQ_PERSONA") or _ast_value(
            os.path.join(ROOT, "qq_agent.py"), "DEFAULT_PERSONA", ""),
        "model": MODEL, "temperature": 0.9, "context": 8,
    },
    "bili": {
        "text": _ast_value(os.path.join(ROOT, "bili_play.py"), "SYSTEM", ""),
        "model": MODEL, "temperature": 0.9, "context": 12,
    },
    "dy": {
        "text": _ast_value(os.path.join(ROOT, "dy_agent.py"), "SYSTEM", ""),
        "model": MODEL, "temperature": 0.9, "context": 30,
    },
}


# ---------------- 统计 ----------------

def _said_summary(path, today):
    today_row = _one(path, "SELECT COUNT(*) total, "
                          "SUM(CASE WHEN code=0 THEN 1 ELSE 0 END) ok, "
                          "SUM(CASE WHEN code=-1 THEN 1 ELSE 0 END) dry "
                          "FROM said WHERE ts>=?", (today,)) or {}
    hour_row = _one(path, "SELECT COUNT(*) n FROM said WHERE code=0 AND ts>=?",
                    (int(time.time()) - 3600,)) or {}
    last = _one(path, "SELECT text,code,ts FROM said ORDER BY id DESC LIMIT 1")
    return {
        "today_said": int(today_row.get("ok") or 0),
        "today_dry": int(today_row.get("dry") or 0),
        "hour_said": int(hour_row.get("n") or 0),
        "last": last,
    }


def _text_metrics(texts):
    texts = [t for t in texts if t]
    if not texts:
        return {"count": 0, "dup": 0.0, "avg_len": 0.0}
    return {
        "count": len(texts),
        "dup": 1 - len(set(texts)) / len(texts),
        "avg_len": sum(len(t) for t in texts) / len(texts),
    }


def _quality(platform, said_db, today):
    counts = {r["kind"]: r["n"] for r in _q(
        DB_METRICS, "SELECT kind, COUNT(*) n FROM events WHERE platform=? AND ts>=? "
                    "GROUP BY kind", (platform, today))}
    real = int(counts.get("say") or 0)
    dry = int(counts.get("say_dry") or 0)
    skip = int(counts.get("skip") or 0)
    sticker_rate = None
    if platform == "qq":
        rows = _q(DB_METRICS, "SELECT detail, meta FROM events WHERE platform='qq' "
                              "AND ts>=? AND kind IN ('say','say_dry') "
                              "ORDER BY id DESC LIMIT 300", (today,))
        texts = [r["detail"] for r in rows]
        sticker_rate = (sum(1 for r in rows if _meta(r).get("sticker")) / len(rows)
                        if rows else None)
    else:
        rows = _q(said_db, "SELECT text FROM said WHERE ts>=? ORDER BY id DESC LIMIT 300",
                  (today,))
        texts = [r["text"] for r in rows]
    base = _text_metrics(texts)
    json_fail = _one(DB_METRICS, "SELECT COUNT(*) n FROM events WHERE platform=? AND ts>=? "
                                 "AND (detail LIKE '%JSON%' OR detail LIKE '%非 JSON%')",
                     (platform, today)) or {}
    return {
        "real": real,
        "dry": dry,
        "skip": skip,
        "speak_rate": (real / (real + skip)) if (real + skip) else None,
        "sticker_rate": sticker_rate,
        "dup": base["dup"],
        "avg_len": base["avg_len"],
        "samples": base["count"],
        "json_fail": int(json_fail.get("n") or 0),
    }


def _series():
    rows = _q(DB_METRICS, "SELECT CAST(ts/3600 AS INTEGER)*3600 h, "
                          "SUM(prompt_tokens+completion_tokens) tok, COUNT(*) n "
                          "FROM llm_calls WHERE ts>=? GROUP BY h",
              (int(time.time()) - 23 * 3600,))
    found = {int(r["h"]): r for r in rows}
    now_h = int(time.time() // 3600) * 3600
    out = []
    for i in range(24):
        h = now_h - (23 - i) * 3600
        r = found.get(h) or {}
        out.append({"label": time.strftime("%H", time.localtime(h)),
                    "tokens": int(r.get("tok") or 0),
                    "calls": int(r.get("n") or 0)})
    return out


def _task_label(runner, info):
    st = runner.state()
    if st["running"]:
        if runner.key in ("login_bili", "login_dy"):
            return "登录中"
        if runner.key == "check_dy":
            return "检测登录态"
        if runner.key == "desk":
            return "生成草稿"
        mode = "仅观察" if (info or {}).get("dry_run") else "真发"
        return f"{mode} · {info.get('minutes', '?')} 分钟"
    if st["ended"]:
        return f"上次退出 code={st['exit_code']}"
    return "待机"


def _bili_stats(today):
    heard = _one(DB_BILI, "SELECT COUNT(*) n FROM heard WHERE ts>=?", (today,)) or {}
    said = _said_summary(DB_BILI, today)
    return {
        "account": _nav_state(),
        "runner": RUNNERS["bili"].state(),
        "task": _task_label(RUNNERS["bili"], RUNNERS["bili"].info),
        "today_heard": int(heard.get("n") or 0),
        "quota": RISK["bili"]["max_per_hour"],
        "risk": RISK["bili"],
        **said,
    }


def _dy_stats(today):
    heard = _one(DB_DY, "SELECT COUNT(*) n FROM heard WHERE ts>=?", (today,)) or {}
    said = _said_summary(DB_DY, today)
    return {
        "account": _dy_account(),
        "runner": RUNNERS["dy"].state(),
        "task": _task_label(RUNNERS["dy"], RUNNERS["dy"].info),
        "today_heard": int(heard.get("n") or 0),
        "quota": RISK["dy"]["max_per_hour"],
        "risk": RISK["dy"],
        **said,
    }


def _qq_stats(today):
    state = _qq_state()
    heard = _one(DB_METRICS, "SELECT COUNT(*) n FROM events WHERE platform='qq' "
                             "AND kind='heard' AND ts>=?", (today,)) or {}
    said = _one(DB_METRICS, "SELECT COUNT(*) n FROM events WHERE platform='qq' "
                            "AND kind='say' AND ts>=?", (today,)) or {}
    last = _one(DB_METRICS, "SELECT detail, ts FROM events WHERE platform='qq' "
                            "AND kind IN ('say','say_dry') ORDER BY id DESC LIMIT 1")
    return {
        **state,
        "task": _task_label(RUNNERS["qq"], RUNNERS["qq"].info),
        "today_heard": int(heard.get("n") or 0),
        "today_said": int(said.get("n") or 0),
        "today_dry": 0,
        "hour_said": 0,
        "quota": None,
        "last": ({"text": last["detail"], "ts": last["ts"], "code": 0}
                 if last else None),
    }


def _state_payload():
    today = _today()
    kpi_row = _one(DB_METRICS, "SELECT COUNT(*) calls, "
                              "COALESCE(SUM(prompt_tokens),0) prompt, "
                              "COALESCE(SUM(completion_tokens),0) completion, "
                              "COALESCE(SUM(cache_hit_tokens),0) hit, "
                              "COALESCE(SUM(cache_miss_tokens),0) miss, "
                              "COALESCE(AVG(latency_ms),0) latency, "
                              "COALESCE(SUM(CASE WHEN ok=0 THEN 1 ELSE 0 END),0) errors "
                              "FROM llm_calls WHERE ts>=?", (today,)) or {}
    price_hit = float(os.environ.get("DS_PRICE_HIT", "0.2") or 0.2)
    price_miss = float(os.environ.get("DS_PRICE_MISS", "2.0") or 2.0)
    price_out = float(os.environ.get("DS_PRICE_OUT", "3.0") or 3.0)
    cost = (int(kpi_row.get("hit") or 0) * price_hit
            + int(kpi_row.get("miss") or 0) * price_miss
            + int(kpi_row.get("completion") or 0) * price_out) / 1_000_000

    bili = _bili_stats(today)
    dy = _dy_stats(today)
    qq = _qq_stats(today)
    today_say = bili["today_said"] + dy["today_said"] + qq["today_said"]
    today_heard = bili["today_heard"] + dy["today_heard"] + qq["today_heard"]

    login_bili = RUNNERS["login_bili"]
    qr_ts = int(os.path.getmtime(QR_BILI)) if os.path.exists(QR_BILI) else 0
    qr_visible = bool(qr_ts and login_bili.started and qr_ts >= login_bili.started - 5
                      and not (login_bili.exit_code == 0 and login_bili.ended > qr_ts))
    if (login_bili.exit_code == 0 and login_bili.ended
            and _NAV["ts"] < login_bili.ended):
        threading.Thread(target=_refresh_nav, daemon=True).start()

    events = _q(DB_METRICS, "SELECT id, ts, platform, kind, detail, meta FROM events "
                            "ORDER BY id DESC LIMIT 90")
    for e in events:
        e["meta"] = _meta(e)

    drafts = _q(DB_BILI, "SELECT id, ts, title, body, tags, status, dyn_id "
                         "FROM drafts ORDER BY id DESC LIMIT 20")

    return {
        "now": int(time.time()),
        "server": {"port": PORT, "pid": os.getpid(), "uptime": int(time.time() - START)},
        "kpi": {
            "tokens": int(kpi_row.get("prompt") or 0) + int(kpi_row.get("completion") or 0),
            "prompt": int(kpi_row.get("prompt") or 0),
            "completion": int(kpi_row.get("completion") or 0),
            "cache_hit": int(kpi_row.get("hit") or 0),
            "calls": int(kpi_row.get("calls") or 0),
            "latency": int(kpi_row.get("latency") or 0),
            "errors": int(kpi_row.get("errors") or 0),
            "error_rate": (int(kpi_row.get("errors") or 0) / int(kpi_row.get("calls") or 1)
                           if int(kpi_row.get("calls") or 0) else 0),
            "cost": cost,
            "today_say": today_say,
            "today_heard": today_heard,
        },
        "series": _series(),
        "platforms": {"bili": bili, "dy": dy, "qq": qq},
        "quality": {
            "bili": _quality("bili", DB_BILI, today),
            "dy": _quality("dy", DB_DY, today),
            "qq": _quality("qq", DB_METRICS, today),
        },
        "events": events,
        "drafts": drafts,
        "persona": PERSONA,
        "risk": RISK,
        "desk_runner": RUNNERS["desk"].state(),
        "login": {
            "bili_qr": qr_ts if qr_visible else 0,
            "bili_runner": login_bili.state(),
            "dy_runner": RUNNERS["login_dy"].state(),
            "check_dy_runner": RUNNERS["check_dy"].state(),
        },
    }


# ---------------- API ----------------

app = FastAPI(title="大肥鱼中控台", docs_url=None, redoc_url=None)


def check_origin(request: Request):
    origin = request.headers.get("origin")
    if not origin:
        return
    host = request.headers.get("host", "")
    if origin.rstrip("/") not in {f"http://{host}", f"https://{host}"}:
        raise HTTPException(status_code=403, detail="跨站请求被拒绝")


class BiliRun(BaseModel):
    room: str = ""
    minutes: int = 10
    dry_run: bool = True
    confirm: bool = False


class DyRun(BaseModel):
    room: str
    minutes: int = 10
    dry_run: bool = True
    confirm: bool = False


class QqRun(BaseModel):
    dry_run: bool = True
    confirm: bool = False


class GenReq(BaseModel):
    n: int = 2


class ConfirmReq(BaseModel):
    confirm: bool = False


@app.get("/")
def index():
    resp = FileResponse(INDEX, media_type="text/html; charset=utf-8")
    resp.headers["Cache-Control"] = "no-store"
    return resp


@app.get("/avatar")
def avatar():
    path = os.path.join(ROOT, "assets", "dafeiyu", "avatar_deepseek_pet.png")
    if not os.path.exists(path):
        raise HTTPException(404, "没有头像文件")
    return FileResponse(path, media_type="image/png")


@app.get("/api/qr/bili")
def qr_bili():
    if not os.path.exists(QR_BILI):
        raise HTTPException(404, "还没有二维码")
    resp = FileResponse(QR_BILI, media_type="image/png")
    resp.headers["Cache-Control"] = "no-store"
    return resp


@app.get("/api/state")
def state():
    return _state_payload()


@app.get("/api/logs")
def logs(key: str, since: int = 0):
    runner = RUNNERS.get(key)
    if not runner:
        raise HTTPException(404, "没有这个日志通道")
    return runner.logs_since(max(0, since))


def _room(value):
    value = (value or "").strip()
    if value and not re.fullmatch(r"\d{5,20}", value):
        raise HTTPException(400, "房间号必须是数字")
    return value


def _minutes(value):
    if not 1 <= int(value) <= 180:
        raise HTTPException(400, "时长需在 1-180 分钟")
    return int(value)


@app.post("/api/run/bili")
def run_bili(p: BiliRun, _: None = Depends(check_origin)):
    if RUNNERS["bili"].running():
        raise HTTPException(409, "B站任务正在运行")
    if RUNNERS["login_bili"].running():
        raise HTTPException(409, "B站正在登录，先等登录结束")
    room = _room(p.room)
    minutes = _minutes(p.minutes)
    if not p.dry_run and not p.confirm:
        raise HTTPException(400, "真发需要二次确认")
    cmd = _python("bili_play.py", "--minutes", str(minutes))
    if room:
        cmd += ["--room", room]
    if p.dry_run:
        cmd.append("--dry-run")
    RUNNERS["bili"].start(cmd, kind="run", info={
        "room": room or "自动", "minutes": minutes, "dry_run": p.dry_run})
    log_event("desk", "start", f"B站任务 {minutes} 分钟"
              f"{'（仅观察）' if p.dry_run else '（真发）'}", room=room or "auto")
    return {"ok": True, "runner": RUNNERS["bili"].state()}


@app.post("/api/run/dy")
def run_dy(p: DyRun, _: None = Depends(check_origin)):
    if RUNNERS["dy"].running():
        raise HTTPException(409, "抖音任务正在运行")
    if RUNNERS["login_dy"].running() or RUNNERS["check_dy"].running():
        raise HTTPException(409, "抖音登录/检测正在进行")
    room = _room(p.room)
    if not room:
        raise HTTPException(400, "抖音必须填房间号")
    minutes = _minutes(p.minutes)
    if not p.dry_run and not p.confirm:
        raise HTTPException(400, "真发需要二次确认")
    cmd = _python("dy_agent.py", "--room", room, "--minutes", str(minutes))
    if p.dry_run:
        cmd.append("--dry-run")
    RUNNERS["dy"].start(cmd, kind="run", info={
        "room": room, "minutes": minutes, "dry_run": p.dry_run})
    log_event("desk", "start", f"抖音任务 {minutes} 分钟"
              f"{'（仅观察）' if p.dry_run else '（真发）'}", room=room)
    return {"ok": True, "runner": RUNNERS["dy"].state()}


def _start_qq(dry_run):
    port = _qq_port()
    if RUNNERS["qq"].running():
        raise HTTPException(409, "QQ 任务已在面板中运行")
    if _port_open(port):
        raise HTTPException(409, f"端口 {port} 已被外部进程占用，先接管或手动停止")
    cmd = _python("qq_agent.py")
    if dry_run:
        cmd.append("--dry-run")
    RUNNERS["qq"].start(cmd, kind="run", info={"dry_run": dry_run})
    log_event("desk", "start", "QQ 任务" + ("（仅观察）" if dry_run else "（真发）"))
    return {"ok": True, "runner": RUNNERS["qq"].state()}


@app.post("/api/run/qq")
def run_qq(p: QqRun, _: None = Depends(check_origin)):
    if not p.dry_run and not p.confirm:
        raise HTTPException(400, "真发需要二次确认")
    return _start_qq(p.dry_run)


@app.post("/api/qq/takeover")
def qq_takeover(p: ConfirmReq, _: None = Depends(check_origin)):
    if not p.confirm:
        raise HTTPException(400, "接管需要确认")
    if RUNNERS["qq"].running():
        raise HTTPException(409, "QQ 任务已经在面板里运行")
    port = _qq_port()
    if not _port_open(port):
        raise HTTPException(400, f"端口 {port} 没有进程")
    pid = _port_pid(port)
    if not pid:
        raise HTTPException(400, "找不到占用端口的进程")
    cmdline = _pid_cmdline(pid)
    if "qq_agent.py" not in cmdline:
        raise HTTPException(409, "端口上的进程不是 qq_agent.py，拒绝接管")
    try:
        subprocess.run(["taskkill", "/PID", str(pid), "/F"], capture_output=True,
                       text=True, timeout=10)
    except Exception as e:
        raise HTTPException(500, f"停止外部进程失败: {e}")
    deadline = time.time() + 5
    while time.time() < deadline and _port_open(port):
        time.sleep(0.2)
    if _port_open(port):
        raise HTTPException(500, "端口仍被占用，接管失败")
    log_event("desk", "start", f"接管 QQ：终止外部进程 pid={pid}")
    return _start_qq(False)


@app.post("/api/stop/{key}")
def stop(key: str, _: None = Depends(check_origin)):
    runner = RUNNERS.get(key)
    if not runner:
        raise HTTPException(404, "没有这个任务")
    if key == "qq" and not runner.running() and _port_open(_qq_port()):
        raise HTTPException(409, "QQ 是外部进程，面板无法停止；可用「接管」")
    stopped = runner.stop()
    if stopped:
        log_event("desk", "stop", f"停止 {runner.label}")
    return {"ok": stopped, "runner": runner.state()}


@app.post("/api/login/bili")
def login_bili(_: None = Depends(check_origin)):
    if RUNNERS["bili"].running():
        raise HTTPException(409, "B站任务正在跑，先停止再登录")
    if RUNNERS["login_bili"].running():
        raise HTTPException(409, "登录流程已经在跑")
    RUNNERS["login_bili"].start(_python("bili_login.py"), kind="login")
    return {"ok": True, "runner": RUNNERS["login_bili"].state()}


@app.post("/api/login/dy")
def login_dy(_: None = Depends(check_origin)):
    if RUNNERS["dy"].running():
        raise HTTPException(409, "抖音任务正在跑，先停止再登录")
    if RUNNERS["login_dy"].running():
        raise HTTPException(409, "抖音登录已经在跑")
    RUNNERS["login_dy"].start(_python("dy_login.py"), kind="login")
    return {"ok": True, "runner": RUNNERS["login_dy"].state()}


@app.post("/api/check/dy")
def check_dy(_: None = Depends(check_origin)):
    if RUNNERS["dy"].running():
        raise HTTPException(409, "抖音任务正在跑，先停止再检测")
    if RUNNERS["check_dy"].running():
        raise HTTPException(409, "检测已经在跑")
    RUNNERS["check_dy"].start(_python("dy_login.py", "--check"), kind="check")
    return {"ok": True, "runner": RUNNERS["check_dy"].state()}


@app.post("/api/drafts/gen")
def drafts_gen(p: GenReq, _: None = Depends(check_origin)):
    if RUNNERS["desk"].running():
        raise HTTPException(409, "运营台正在生成")
    n = max(1, min(int(p.n), 5))
    RUNNERS["desk"].start(_python("bili_desk.py", "gen", "--n", str(n)), kind="desk")
    return {"ok": True, "runner": RUNNERS["desk"].state()}


@app.post("/api/drafts/{draft_id}/publish")
def drafts_publish(draft_id: int, p: ConfirmReq, _: None = Depends(check_origin)):
    if not p.confirm:
        raise HTTPException(400, "发布需要确认")
    row = _one(DB_BILI, "SELECT id,title,body,tags,status FROM drafts WHERE id=?", (draft_id,))
    if not row:
        raise HTTPException(404, "没有这条草稿")
    if row["status"] == "published":
        raise HTTPException(409, "这条草稿已经发布")
    _refresh_env()
    if not os.environ.get("BILI_JCT"):
        raise HTTPException(400, "缺 BILI_JCT，先扫码登录")
    text = row["body"] + (("\n" + row["tags"]) if row["tags"] else "")
    try:
        from bili_live import new_session
        from bili_post import post_dynamic
        http_code, raw = post_dynamic(new_session(), text)
    except Exception as e:
        raise HTTPException(500, f"发布失败: {type(e).__name__}: {e}")
    try:
        j = json.loads(raw)
    except ValueError:
        j = {}
    code = j.get("code")
    dyn_id = str((j.get("data") or {}).get("dyn_id_str") or "")
    status = "published" if code == 0 else f"blocked:{code}"
    try:
        c = sqlite3.connect(DB_BILI, timeout=5)
        c.execute("UPDATE drafts SET status=?, dyn_id=? WHERE id=?", (status, dyn_id, draft_id))
        c.commit()
        c.close()
    except sqlite3.Error:
        pass
    log_event("desk", "say" if code == 0 else "error",
              f"发布草稿 #{draft_id}" + ("成功" if code == 0 else f" 被拦 code={code}"),
              dyn_id=dyn_id, http=http_code)
    return {"ok": code == 0, "code": code, "http": http_code,
            "dyn_id": dyn_id, "status": status, "raw": raw[:300]}


def main():
    global PORT
    ap = argparse.ArgumentParser()
    ap.add_argument("--port", type=int, default=PORT)
    ap.add_argument("--no-open", action="store_true")
    a = ap.parse_args()
    PORT = a.port
    threading.Thread(target=_refresh_nav, daemon=True).start()
    if not a.no_open:
        threading.Timer(1.0, lambda: webbrowser.open(f"http://127.0.0.1:{PORT}/")).start()
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    print(f"大肥鱼中控台: http://127.0.0.1:{PORT}/")
    try:
        uvicorn.run(app, host="127.0.0.1", port=PORT, log_level="warning",
                    access_log=False)
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
