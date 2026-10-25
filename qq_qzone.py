"""QQ空间：借 NapCat 已登录的 QQ 会话读好友动态、发自己的说说。

依赖 NapCat 的 OneBot HTTP（127.0.0.1:3000，token 与 QQ_WS_TOKEN 相同）。
只做文本说说；图片上传未接。
"""
import argparse
import html as htmllib
import os
import re
import sys
import time

import requests

from core.llm import event as log_event
from envload import load_env

load_env()

API = os.environ.get("QQ_ONEBOT_HTTP", "http://127.0.0.1:3000").rstrip("/")
TOKEN = os.environ.get("QQ_WS_TOKEN", "")
UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36")
QZ = "https://user.qzone.qq.com"

_ITEM = re.compile(r"\{ver:'1',appid:")
_FIELD = re.compile(r"(\w+):'((?:[^'\\]|\\.)*)'")
_ESCAPE = re.compile(r"\\x([0-9a-fA-F]{2})|\\u([0-9a-fA-F]{4})|\\(.)")


class QZoneError(RuntimeError):
    pass


def _action(action, **params):
    if not TOKEN:
        raise QZoneError("缺 QQ_WS_TOKEN")
    try:
        r = requests.post(f"{API}/{action}", json=params, timeout=20,
                          headers={"Authorization": f"Bearer {TOKEN}"})
    except requests.RequestException as e:
        raise QZoneError(f"连不上 NapCat HTTP（{API}）：{e}") from e
    if r.status_code >= 400:
        raise QZoneError(f"NapCat HTTP {r.status_code}")
    j = r.json()
    if j.get("retcode") != 0:
        raise QZoneError(f"{action} 失败：{j.get('message') or j.get('wording')}")
    return j.get("data") or {}


def _unescape(s):
    def sub(m):
        if m.group(1):
            return chr(int(m.group(1), 16))
        if m.group(2):
            return chr(int(m.group(2), 16))
        ch = m.group(3)
        return {"n": "\n", "t": "\t", "r": "\r"}.get(ch, ch)
    return _ESCAPE.sub(sub, s)


def _strip(s):
    s = re.sub(r"<br\s*/?>", "\n", s)
    s = re.sub(r"<[^>]+>", "", s)
    s = htmllib.unescape(s)
    return re.sub(r"[ \t\u3000]+", " ", s).strip()


def _gtk(p_skey):
    h = 5381
    for ch in p_skey:
        h += (h << 5) + ord(ch)
    return h & 0x7FFFFFFF


def _creds():
    data = _action("get_cookies", domain="user.qzone.qq.com")
    ck = {}
    for part in (data.get("cookies") or "").split(";"):
        if "=" in part:
            k, v = part.strip().split("=", 1)
            ck[k] = v
    if not ck.get("p_skey"):
        raise QZoneError("NapCat 没给到 qzone cookie，可能 QQ 没登录")
    uin = (ck.get("p_uin") or "").lstrip("o")
    if not uin:
        uin = str(_action("get_login_info").get("user_id") or "")
    s = requests.Session()
    s.cookies.update(ck)
    s.headers.update({"User-Agent": UA, "Referer": f"{QZ}/{uin}"})
    return s, uin, _gtk(ck["p_skey"])


def friend_feed(limit=10):
    s, uin, t = _creds()
    r = s.get(f"{QZ}/proxy/domain/ic2.qzone.qq.com/cgi-bin/feeds/feeds3_html_more",
              params={"uin": uin, "scope": 0, "view": 1, "flag": 1, "appid": 311,
                      "pagenum": 1, "fupdate": 1, "type": 0, "g_tk": t,
                      "format": "json", "qzreferrer": f"{QZ}/{uin}"}, timeout=20)
    if r.status_code >= 400:
        raise QZoneError(f"好友动态 HTTP {r.status_code}")
    raw = r.text
    if not raw.lstrip().startswith("{"):
        raise QZoneError("好友动态返回异常，可能 cookie 失效")
    out = []
    for part in _ITEM.split(raw)[1:limit + 1]:
        f = {k: _unescape(v) for k, v in _FIELD.findall(part)}
        h = f.get("html", "")
        if f.get("opuin") == uin:
            m = re.search(r'<div class="f-info[^"]*"[^>]*>(.*?)</div>', h, re.S)
        else:
            m = re.search(r'<div class="txt-box[^"]*"[^>]*>(.*?)</div>', h, re.S)
        text = _strip(m.group(1)) if m else ""
        imgs = [u for u in re.findall(r'<img[^>]+src="([^"]+)"', h)
                if "qlogo" not in u and ("qpic.cn" in u or "photo.qq.com" in u)][:3]
        video = 'f-video-wrap' in h or "photovideo.photo.qq.com" in h
        out.append({"author": f.get("nickname") or "", "uin": f.get("opuin") or "",
                    "time": f.get("feedstime") or "", "text": text,
                    "images": imgs, "video": video, "tid": f.get("key") or ""})
    return out


def _when(v):
    s = str(v or "")
    if s.isdigit():
        return time.strftime("%m月%d日 %H:%M", time.localtime(int(s)))
    return s


def own_posts(limit=10):
    s, uin, t = _creds()
    r = s.get(f"{QZ}/proxy/domain/taotao.qq.com/cgi-bin/emotion_cgi_msglist_v6",
              params={"uin": uin, "ftype": 0, "sort": 0, "pos": 0, "num": limit,
                      "g_tk": t, "code_version": 1, "format": "json",
                      "qzreferrer": f"{QZ}/{uin}"}, timeout=20)
    if r.status_code >= 400:
        raise QZoneError(f"我的说说 HTTP {r.status_code}")
    try:
        j = r.json()
    except ValueError as e:
        raise QZoneError("我的说说返回异常，可能 cookie 失效") from e
    if j.get("code") != 0:
        raise QZoneError(f"我的说说 code={j.get('code')} {j.get('message') or ''}".strip())
    out = []
    for m in j.get("msglist") or []:
        out.append({"tid": m.get("tid") or "", "time": _when(m.get("created_time")),
                    "text": (m.get("content") or "").strip(),
                    "images": [p.get("url2") or p.get("url1")
                               for p in (m.get("pic") or [])][:3]})
    return out


def _qzonetoken(s, uin):
    try:
        page = s.get(f"{QZ}/{uin}/infocenter", timeout=20).text
    except requests.RequestException:
        return ""
    m = re.search(r'\{ try\{return "(.*?)";\}', page)
    return m.group(1) if m else ""


def publish(text, *, dry_run=True):
    text = (text or "").strip()
    if not text:
        raise QZoneError("说说内容是空的")
    if len(text) > 1000:
        raise QZoneError("说说超过 1000 字，先截短")
    s, uin, t = _creds()
    if dry_run:
        return {"dry_run": True, "uin": uin, "text": text}
    url = (f"{QZ}/proxy/domain/taotao.qzone.qq.com/cgi-bin/"
           f"emotion_cgi_publish_v6?g_tk={t}")
    data = {
        "syn_tweet_verson": "1", "paramstr": "1", "pic_template": "",
        "richtype": "", "richval": "", "special_url": "", "subrichtype": "",
        "who": "1", "con": text, "feedversion": "1", "ver": "1",
        "ugc_right": "1", "to_sign": "0", "hostuin": uin,
        "code_version": "1", "format": "json",
        "qzreferrer": f"{QZ}/{uin}",
    }
    token = _qzonetoken(s, uin)
    if token:
        url += f"&qzonetoken={token}"
    r = s.post(url, data=data, timeout=30, headers={
        "Origin": QZ, "Referer": f"{QZ}/{uin}",
        "Content-Type": "application/x-www-form-urlencoded;charset=UTF-8"})
    if r.status_code >= 400:
        raise QZoneError(f"发说说 HTTP {r.status_code}")
    try:
        j = r.json()
    except ValueError as e:
        raise QZoneError(f"发说说返回异常：{r.text[:120]!r}") from e
    if j.get("code") not in (0, "0"):
        raise QZoneError(f"发说说失败 code={j.get('code')} {j.get('message') or ''}".strip())
    try:
        log_event("qq", "qzone_post", text[:120], tid=j.get("tid") or "")
    except Exception:
        pass
    return {"dry_run": False, "tid": j.get("tid") or "", "uin": uin, "text": text}


def delete(tid, *, dry_run=True):
    tid = (tid or "").strip()
    if not tid:
        raise QZoneError("缺 tid")
    s, uin, t = _creds()
    if dry_run:
        return {"dry_run": True, "tid": tid}
    r = s.post(f"{QZ}/proxy/domain/taotao.qzone.qq.com/cgi-bin/emotion_cgi_delete_v6"
               f"?g_tk={t}",
               data={"hostuin": uin, "tid": tid, "t1_source": "1",
                     "code_version": "1", "format": "fs",
                     "qzreferrer": f"{QZ}/{uin}"},
               timeout=30, headers={"Origin": QZ, "Referer": f"{QZ}/{uin}"})
    if r.status_code >= 400:
        raise QZoneError(f"删说说 HTTP {r.status_code}")
    try:
        log_event("qq", "qzone_delete", tid)
    except Exception:
        pass
    return {"dry_run": False, "tid": tid, "raw": r.text[:120]}


def format_items(items, width=160):
    if not items:
        return "（没有内容）"
    lines = []
    for it in items:
        who = it.get("author") or "我"
        when = it.get("time") or ""
        txt = (it.get("text") or "（无文字）").replace("\n", " / ")
        tag = " [视频]" if it.get("video") else (f" [{len(it['images'])}图]"
                                                if it.get("images") else "")
        lines.append(f"- {who} {when}：{txt[:width]}{tag}")
    return "\n".join(lines)


def main():
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    ap = argparse.ArgumentParser(description="QQ空间动态读写")
    sub = ap.add_subparsers(dest="cmd", required=True)
    p = sub.add_parser("feed", help="好友动态")
    p.add_argument("-n", type=int, default=10)
    p = sub.add_parser("own", help="自己的说说")
    p.add_argument("-n", type=int, default=10)
    p = sub.add_parser("post", help="发文字说说")
    p.add_argument("text")
    p.add_argument("--yes", action="store_true", help="真的发出去")
    p = sub.add_parser("delete", help="删自己的说说")
    p.add_argument("tid")
    p.add_argument("--yes", action="store_true", help="真的删")
    a = ap.parse_args()

    try:
        if a.cmd == "feed":
            print(format_items(friend_feed(a.n)))
        elif a.cmd == "own":
            print(format_items(own_posts(a.n)))
        elif a.cmd == "post":
            r = publish(a.text, dry_run=not a.yes)
            if r["dry_run"]:
                print(f"[dry-run] 会以 {r['uin']} 的 QQ 发说说：{r['text']}")
                print("确认无误后加 --yes 真发。")
            else:
                print(f"已发布 tid={r['tid'] or '?'}  {time.strftime('%H:%M:%S')}")
        elif a.cmd == "delete":
            r = delete(a.tid, dry_run=not a.yes)
            print(("[dry-run] 会删除 " if r["dry_run"] else "已删除 ") + a.tid)
    except QZoneError as e:
        print(f"失败：{e}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
