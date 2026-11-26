import json
import requests
import asyncio
import os
import re
import sqlite3
import hashlib
import uuid
import subprocess
import threading
import imageio_ffmpeg
import shutil
import traceback
import time
import hmac
import secrets
import boto3
from botocore.config import Config as BotoConfig
from datetime import datetime, timedelta
from contextlib import asynccontextmanager
from fastapi import (
    FastAPI,
    WebSocket,
    WebSocketDisconnect,
    HTTPException,
    Header,
    Response,
    UploadFile,
    File,
    Request,
    Form,
)
from pydantic import BaseModel
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse, FileResponse
from urllib.parse import quote
from typing import Optional
import sys
from google.oauth2 import id_token
from google.auth.transport import requests as google_requests

# ================= .env 加载（密钥统一出口） =================
# 密钥统一放工作区根目录 .env，这里启动时读进来。
# 优先用 python-dotenv（若装了）；没装则用下面的自研解析器兜底（无第三方依赖）。
# override=True 很重要：start_all.bat 过去把 R2 密钥 set 进了环境变量，
# 若不让 .env 覆盖，轮换后旧密钥仍会顶着用，等于没轮换。
# .env 查询顺序：server.py 同级目录 -> 当前工作目录。


# ================= 控制台输出编码 =================
# start_all.bat 里是 chcp 65001（UTF-8 码页），但 Python 写控制台时不一定用它：
# TTY 直写会调 Unicode API 没问题，被重定向/管道时则按系统 ANSI(GBK) 编码，
# 于是中文日志在 cmd 窗口里是乱码。这里统一按 UTF-8 写，与 chcp 65001 对齐。
for _stream_name in ("stdout", "stderr"):
    _stream = getattr(sys, _stream_name, None)
    if _stream is not None and hasattr(_stream, "reconfigure"):
        try:
            _stream.reconfigure(encoding="utf-8", errors="replace")
        except Exception:
            pass


def _load_env_file():
    env_path = None
    for cand in (
        os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env"),
        os.path.join(os.getcwd(), ".env"),
    ):
        if os.path.isfile(cand):
            env_path = cand
            break
    if not env_path:
        print("[ENV] 未找到 .env（密钥将回退到环境变量 / 源码内默认值）")
        return None

    loaded = 0
    try:
        with open(env_path, "r", encoding="utf-8-sig") as f:
            lines = f.readlines()
    except Exception as e:
        print(f"[ENV] 读取 .env 失败：{e}")
        return None

    for raw in lines:
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        if line.startswith("export "):
            line = line[7:].lstrip()
        key, _, value = line.partition("=")
        key = key.strip()
        value = value.strip()
        # 去成对引号
        if len(value) >= 2 and value[0] == value[-1] and value[0] in ("'", '"'):
            value = value[1:-1]
        if not key:
            continue
        os.environ[key] = value
        loaded += 1

    print(f"[ENV] 已加载 {env_path}（{loaded} 项，override=on）")
    return env_path


try:
    from dotenv import load_dotenv as _load_dotenv

    if not _load_dotenv(
        os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env"),
        override=True,
    ):
        _load_env_file()
except ImportError:
    _load_env_file()
except Exception as e:
    print(f"[ENV] python-dotenv 加载异常，改用内置解析器：{e}")
    _load_env_file()

# ================= R2 配置 =================
R2_ACCOUNT_ID = os.environ.get("R2_ACCOUNT_ID", "9c3418b33aedb07f029780068383a182")
R2_ACCESS_KEY_ID = os.environ.get("R2_ACCESS_KEY_ID", "")
R2_SECRET_ACCESS_KEY = os.environ.get("R2_SECRET_ACCESS_KEY", "")
R2_BUCKET = os.environ.get("R2_BUCKET", "cccccxai-works")
R2_ENDPOINT = os.environ.get(
    "R2_ENDPOINT", f"https://{R2_ACCOUNT_ID}.r2.cloudflarestorage.com"
)
R2_PUBLIC_BASE = os.environ.get(
    "R2_PUBLIC_BASE", "https://pub-430a478066314d8c8514443f6b5c11ee.r2.dev"
).rstrip("/")

_r2_client = None


def _mask_secret(value):
    """只暴露前 8 位用于核对，不打印完整密钥。"""
    if not value:
        return "<未设置>"
    if len(value) <= 8:
        return value[:2] + "***"
    return value[:8] + "..." + f"(len={len(value)})"


def get_r2_client():
    global _r2_client
    if _r2_client is None:
        # ---- 启动日志（任务 7）：R2 出问题先看这里 ----
        print(f"[R2] endpoint={R2_ENDPOINT}")
        print(f"[R2] bucket={R2_BUCKET}, access_key={_mask_secret(R2_ACCESS_KEY_ID)}")
        if not R2_ACCESS_KEY_ID or not R2_SECRET_ACCESS_KEY:
            raise HTTPException(
                status_code=500,
                detail="R2 凭证未配置（请设置 R2_ACCESS_KEY_ID / R2_SECRET_ACCESS_KEY 环境变量）",
            )
        _r2_client = boto3.client(
            "s3",
            endpoint_url=R2_ENDPOINT,
            aws_access_key_id=R2_ACCESS_KEY_ID,
            aws_secret_access_key=R2_SECRET_ACCESS_KEY,
            config=BotoConfig(
                signature_version="s3v4",
                s3={"addressing_style": "path"},
            ),
            region_name="auto",
        )
    return _r2_client


# ================= 配置区域 =================
COMFYUI_SERVER = "http://127.0.0.1:8188"
TRANSLATE_JSON_PATH = r"E:\WBE下载目录\xai中英文翻译API.json"

# ================= DeepSeek 配置 =================
DEEPSEEK_API_KEY = os.environ.get("DEEPSEEK_API_KEY", "")
ADMIN_EMAIL = "2012967093@qq.com"

WORKS_DIR = r"E:\WBE下载目录\works"
CHAT_IMAGES_DIR = os.path.join(WORKS_DIR, "chat_images")
AVATARS_DIR = r"E:\WBE下载目录\avatars"
AUDIO_UPLOAD_DIR = r"E:\WBE下载目录\audio_uploads"
COMFYUI_DIR = r"F:\ComfyUI_windows_portable_nvidia\ComfyUI_windows_portable\ComfyUI"

WORKS_MAX_IMAGE_BYTES = 10 * 1024 * 1024
WORKS_MAX_VIDEO_BYTES = 500 * 1024 * 1024
WORKS_MAX_PER_USER = 50
WORKS_RETENTION_DAYS = 3
WORKS_CLEANUP_INTERVAL = 3600
WORKS_AUTO_CLEANUP = False

AVATAR_MAX_BYTES = 2 * 1024 * 1024
ALLOWED_AVATAR_EXTS = {".jpg", ".jpeg", ".png", ".webp"}

DB_PATH = os.path.join(os.path.dirname(__file__), "users.db")
FFMPEG_EXE = imageio_ffmpeg.get_ffmpeg_exe()

SIGNUP_CREDITS = 10000


def is_user_vip(user_id: int):
    """返回 (是否有效 VIP, plan, expires_at)。

    只有 users.vip_plan 有值、且 vip_expires_at 未过期才算有效 ——
    "到期自动失效"靠这里判断，不需要定时任务去改状态。
    """
    try:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute(
            "SELECT vip_plan, vip_expires_at FROM users WHERE id = ?", (user_id,)
        )
        row = c.fetchone()
        conn.close()
    except sqlite3.OperationalError:
        return False, "none", None
    if not row:
        return False, "none", None
    plan, expires = row[0] or "none", row[1]
    if plan == "none" or not expires:
        return False, plan, expires
    try:
        exp_dt = datetime.fromisoformat(expires)
    except (TypeError, ValueError):
        return False, plan, expires
    return (exp_dt > datetime.now()), plan, expires

GOOGLE_CLIENT_ID = (
    "681548989889-l9858vhp3h0lka2n4fd9a6q7f5k7kbt7.apps.googleusercontent.com"
)

RESEND_API_KEY = os.environ.get("RESEND_API_KEY", "")
RESEND_FROM_EMAIL = "noreply@cccccxai.com"
SMTP_FROM_NAME = "DM 造梦空间"
SITE_URL = "https://www.cccccxai.com"
RESET_TOKEN_EXPIRE_MINUTES = 30
VERIFY_TOKEN_EXPIRE_HOURS = 24


# ================= 启动环境自检（任务 7） =================
# 目的：R2 / DeepSeek / Resend 出问题时，看启动日志就知道环境变量对不对，
# 不用去访问 /api/debug/r2（那个接口还要管理员 token）。
# 只打印"是否配置 + 前 8 位指纹"，不打印完整密钥（_mask_secret 定义在文件上方）。
def log_env_fingerprint():
    print("=" * 64)
    print("[ENV-CHECK] 启动环境自检")
    print("=" * 64)
    print(f"[R2] endpoint={R2_ENDPOINT}")
    print(f"[R2] bucket={R2_BUCKET}")
    print(f"[R2] account_id={R2_ACCOUNT_ID}")
    print(f"[R2] access_key={_mask_secret(R2_ACCESS_KEY_ID)}")
    print(f"[R2] secret_key={'<未设置>' if not R2_SECRET_ACCESS_KEY else '(已设置)'}")
    print(f"[R2] public_base={R2_PUBLIC_BASE}")
    print(f"[DeepSeek] api_key={_mask_secret(DEEPSEEK_API_KEY)}")
    print(f"[Resend]   api_key={_mask_secret(RESEND_API_KEY)}")
    print(f"[Resend]   from={RESEND_FROM_EMAIL}")
    print(f"[Site]     url={SITE_URL}")
    missing = []
    for name, val in (
        ("R2_ACCESS_KEY_ID", R2_ACCESS_KEY_ID),
        ("R2_SECRET_ACCESS_KEY", R2_SECRET_ACCESS_KEY),
        ("DEEPSEEK_API_KEY", DEEPSEEK_API_KEY),
        ("RESEND_API_KEY", RESEND_API_KEY),
    ):
        if not val:
            missing.append(name)
    if missing:
        print(f"[ENV-CHECK] [WARN] 以下变量为空，相关功能会失败：{', '.join(missing)}")
    else:
        print("[ENV-CHECK] OK：4 个关键密钥均已加载")
    print(f"[ENV-CHECK] 诊断接口：GET /api/debug/r2 （需管理员 token，匿名返回 401）")
    print("=" * 64)


active_tasks = {}
_last_unload_time = time.time()


def _unload_comfy_models():
    try:
        r = requests.post(
            f"{COMFYUI_SERVER}/free",
            json={"unload_models": True, "free_memory": True},
            timeout=10,
        )
        if r.status_code == 200:
            print("[FREE] OK 已请求 ComfyUI 卸载模型")
        else:
            print(f"[FREE] 返回 {r.status_code}")
    except Exception as e:
        print(f"[FREE] 卸载失败: {e}")


async def task_timeout_monitor():
    global _last_unload_time
    while True:
        await asyncio.sleep(30)
        now = time.time()
        for prompt_id, info in list(active_tasks.items()):
            elapsed = now - info["start_time"]
            if elapsed > info["timeout"]:
                print(f"[TIMEOUT] 任务 {prompt_id} 超时 {elapsed:.0f} 秒")
                try:
                    requests.post(f"{COMFYUI_SERVER}/interrupt", timeout=5)
                    time.sleep(1)
                    requests.post(
                        f"{COMFYUI_SERVER}/queue", json={"clear": True}, timeout=5
                    )
                except Exception as e:
                    print(f"[TIMEOUT] 取消任务失败: {e}")
                cost = info.get("cost", 0)
                if cost > 0:
                    try:
                        conn = sqlite3.connect(DB_PATH)
                        c = conn.cursor()
                        c.execute(
                            "UPDATE users SET credits = credits + ? WHERE id = ?",
                            (cost, info["user_id"]),
                        )
                        conn.commit()
                        conn.close()
                    except Exception as e:
                        print(f"[TIMEOUT] 退还积分失败: {e}")
                active_tasks.pop(prompt_id, None)
        for prompt_id, info in list(active_tasks.items()):
            if now - info["start_time"] > 7200:
                active_tasks.pop(prompt_id, None)
        if len(active_tasks) == 0 and (now - _last_unload_time) > 300:
            threading.Thread(target=_unload_comfy_models, daemon=True).start()
            _last_unload_time = now


async def works_cleanup_monitor():
    print(f"[CLEANUP] 作品自动清理：{'开启' if WORKS_AUTO_CLEANUP else '关闭'}")
    while True:
        if WORKS_AUTO_CLEANUP:
            try:
                cutoff = (
                    datetime.now() - timedelta(days=WORKS_RETENTION_DAYS)
                ).isoformat()
                conn = sqlite3.connect(DB_PATH)
                c = conn.cursor()
                c.execute(
                    "SELECT id, file_name FROM user_works WHERE created_at < ?",
                    (cutoff,),
                )
                to_delete = c.fetchall()
                if to_delete:
                    deleted_files = 0
                    for work_id, file_name in to_delete:
                        try:
                            fp = os.path.join(WORKS_DIR, file_name)
                            if os.path.exists(fp):
                                os.remove(fp)
                                deleted_files += 1
                        except Exception as e:
                            print(f"[CLEANUP] 删文件失败 {file_name}: {e}")
                        c.execute(
                            "DELETE FROM user_work_likes WHERE work_id = ?", (work_id,)
                        )
                        c.execute("DELETE FROM user_works WHERE id = ?", (work_id,))
                    conn.commit()
                    print(
                        f"[CLEANUP] 清理了 {len(to_delete)} 条过期作品，删除 {deleted_files} 个文件"
                    )
                conn.close()
            except Exception as e:
                print(f"[CLEANUP] 作品清理出错: {e}")

        try:
            cache_cutoff = time.time() - 7 * 86400
            for cache_dir in [
                os.path.join(WORKS_DIR, ".video_cache"),
                os.path.join(WORKS_DIR, ".image_cache"),
            ]:
                if not os.path.isdir(cache_dir):
                    continue
                count = 0
                for fn in os.listdir(cache_dir):
                    fp = os.path.join(cache_dir, fn)
                    try:
                        if os.path.isfile(fp) and os.path.getmtime(fp) < cache_cutoff:
                            os.remove(fp)
                            count += 1
                    except:
                        pass
                if count > 0:
                    print(f"[CLEANUP] 清理缓存目录 {cache_dir}：{count} 个文件")
        except Exception as e:
            print(f"[CLEANUP] 缓存清理出错: {e}")

        await asyncio.sleep(WORKS_CLEANUP_INTERVAL)


@asynccontextmanager
async def lifespan(app: FastAPI):
    log_env_fingerprint()
    # 启动时就把 R2 客户端建好：一是立刻打印 [R2] 日志，
    # 二是凭证错误在启动阶段就暴露，而不是等用户上传作品时才 500。
    try:
        get_r2_client()
        print("[R2] 客户端初始化成功")
    except Exception as e:
        print(f"[R2] [WARN] 客户端初始化失败：{e}")
    asyncio.create_task(task_timeout_monitor())
    asyncio.create_task(works_cleanup_monitor())
    yield


# 关掉自动生成的 API 文档（/docs、/redoc、/openapi.json）。
# 原因：公网域名 api.cccccxai.com 会把根路径暴露出去，/docs 实测匿名 200 可访问，
# 等于把全部接口结构（含参数名）送给扫描器。三个都要设 None ——
# FastAPI 在 openapi_url 为 None 时会连 /openapi.json 一起关掉。
# 需要临时看文档时：本地起一个只绑 loopback 的副本，或临时改回默认值。
app = FastAPI(lifespan=lifespan, docs_url=None, redoc_url=None, openapi_url=None)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.middleware("http")
async def force_cors_headers(request, call_next):
    if request.method == "OPTIONS":
        from fastapi.responses import Response as FastAPIResponse

        return FastAPIResponse(
            status_code=200,
            headers={
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Methods": "GET, POST, PUT, DELETE, OPTIONS, PATCH, HEAD",
                "Access-Control-Allow-Headers": "Authorization, Content-Type, Accept",
                "Access-Control-Max-Age": "600",
            },
        )
    response = await call_next(request)
    response.headers["Access-Control-Allow-Origin"] = "*"
    response.headers["Access-Control-Allow-Methods"] = (
        "GET, POST, PUT, DELETE, OPTIONS, PATCH, HEAD"
    )
    response.headers["Access-Control-Allow-Headers"] = (
        "Authorization, Content-Type, Accept"
    )
    return response


def init_db():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("""CREATE TABLE IF NOT EXISTS users (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        email TEXT UNIQUE NOT NULL,
        password_hash TEXT NOT NULL,
        credits INTEGER DEFAULT 10000,
        created_at TEXT NOT NULL)""")
    c.execute("""CREATE TABLE IF NOT EXISTS sessions (
        token TEXT PRIMARY KEY,
        user_id INTEGER NOT NULL,
        created_at TEXT NOT NULL)""")
    c.execute("""CREATE TABLE IF NOT EXISTS tasks (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER NOT NULL,
        prompt TEXT,
        created_at TEXT NOT NULL)""")
    c.execute("""CREATE TABLE IF NOT EXISTS orders (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER NOT NULL,
        order_id TEXT UNIQUE,
        price_id TEXT,
        amount_cents INTEGER,
        credits INTEGER,
        status TEXT DEFAULT 'pending',
        created_at TEXT NOT NULL,
        paid_at TEXT)""")
    c.execute("""CREATE TABLE IF NOT EXISTS chat_messages (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER NOT NULL,
        role TEXT NOT NULL,
        content TEXT NOT NULL,
        audio_url TEXT,
        created_at TEXT NOT NULL)""")
    c.execute("""CREATE TABLE IF NOT EXISTS task_results (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER NOT NULL,
        prompt_id TEXT UNIQUE,
        task_type TEXT,
        prompt TEXT,
        result_url TEXT,
        created_at TEXT NOT NULL,
        updated_at TEXT)""")
    c.execute("""CREATE TABLE IF NOT EXISTS password_resets (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER NOT NULL,
        token TEXT UNIQUE NOT NULL,
        expires_at TEXT NOT NULL,
        used INTEGER DEFAULT 0,
        created_at TEXT NOT NULL)""")
    c.execute("""CREATE TABLE IF NOT EXISTS email_verifications (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER NOT NULL,
        token TEXT UNIQUE NOT NULL,
        expires_at TEXT NOT NULL,
        used INTEGER DEFAULT 0,
        created_at TEXT NOT NULL)""")
    c.execute("""CREATE TABLE IF NOT EXISTS user_works (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER NOT NULL,
        type TEXT NOT NULL,
        title TEXT,
        file_name TEXT NOT NULL,
        file_url TEXT NOT NULL,
        likes INTEGER DEFAULT 0,
        created_at TEXT NOT NULL)""")
    c.execute("""CREATE TABLE IF NOT EXISTS user_work_likes (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        work_id INTEGER NOT NULL,
        user_id INTEGER NOT NULL,
        created_at TEXT NOT NULL,
        UNIQUE(work_id, user_id))""")
    c.execute("""CREATE TABLE IF NOT EXISTS ideas (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER NOT NULL,
        content TEXT NOT NULL,
        color TEXT DEFAULT '',
        created_at TEXT NOT NULL,
        updated_at TEXT)""")
    c.execute("""CREATE TABLE IF NOT EXISTS idea_likes (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        idea_id INTEGER NOT NULL,
        user_id INTEGER NOT NULL,
        created_at TEXT NOT NULL,
        UNIQUE(idea_id, user_id))""")

    migrations = [
        ("users", "is_verified", "INTEGER DEFAULT 0"),
        ("users", "daily_count", "INTEGER DEFAULT 0"),
        ("users", "daily_reset_date", "TEXT DEFAULT ''"),
        ("users", "avatar_url", "TEXT"),
        ("user_works", "cover_url", "TEXT"),
        ("user_works", "avatar_url", "TEXT"),
        ("users", "daily_deepseek_count", "INTEGER DEFAULT 0"),
        ("users", "daily_deepseek_date", "TEXT DEFAULT ''"),
        ("chat_messages", "model", "TEXT DEFAULT 'local'"),
        ("task_results", "result_filename", "TEXT"),
        # 2026-10-08 VIP / 充值：users 加 VIP 身份，orders 加收款核对字段
        ("users", "vip_plan", "TEXT DEFAULT 'none'"),
        ("users", "vip_expires_at", "TEXT"),
        ("orders", "pay_note", "TEXT"),
        ("orders", "confirmed_at", "TEXT"),
        # 2026-10-08 提示词反推：它的结果是【文本】不是媒体文件，
        # 不能塞进 result_url（前端会把 URL 当图片去加载）→ 单独一列。
        ("task_results", "result_text", "TEXT"),
        # 2026-11-01 音乐作品：风格 / 歌词 / 时长
        ("user_works", "style", "TEXT"),
        ("user_works", "lyrics", "TEXT"),
        ("user_works", "duration", "REAL"),
    ]

    for table, col, coltype in migrations:
        try:
            c.execute(f"ALTER TABLE {table} ADD COLUMN {col} {coltype}")
        except sqlite3.OperationalError:
            pass

    c.execute("CREATE INDEX IF NOT EXISTS idx_tasks_user_id ON tasks(user_id)")
    c.execute(
        "CREATE INDEX IF NOT EXISTS idx_task_results_user_id ON task_results(user_id)"
    )
    c.execute(
        "CREATE INDEX IF NOT EXISTS idx_task_results_prompt_id ON task_results(prompt_id)"
    )
    c.execute(
        "CREATE INDEX IF NOT EXISTS idx_task_results_result_filename ON task_results(result_filename)"
    )
    c.execute("CREATE INDEX IF NOT EXISTS idx_sessions_token ON sessions(token)")
    c.execute(
        "CREATE INDEX IF NOT EXISTS idx_chat_messages_user_id ON chat_messages(user_id)"
    )
    c.execute(
        "CREATE INDEX IF NOT EXISTS idx_user_works_type_id ON user_works(type, id DESC)"
    )
    c.execute(
        "CREATE INDEX IF NOT EXISTS idx_user_work_likes_user ON user_work_likes(user_id)"
    )
    c.execute(
        "CREATE INDEX IF NOT EXISTS idx_email_verifications_token ON email_verifications(token)"
    )
    c.execute(
        "CREATE INDEX IF NOT EXISTS idx_password_resets_token ON password_resets(token)"
    )
    c.execute("CREATE INDEX IF NOT EXISTS idx_ideas_user_id ON ideas(user_id)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_idea_likes_user ON idea_likes(user_id)")

    try:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        cutoff = (datetime.now() - timedelta(days=30)).isoformat()
        c.execute("DELETE FROM tasks WHERE created_at < ?", (cutoff,))
        c.execute("DELETE FROM task_results WHERE created_at < ?", (cutoff,))
        conn.commit()
        c.execute("VACUUM")
        conn.commit()
        conn.close()
        print("[DB] 旧数据清理完成")
    except Exception as e:
        print(f"[DB] 清理失败: {e}")


init_db()
os.makedirs(WORKS_DIR, exist_ok=True)
os.makedirs(AVATARS_DIR, exist_ok=True)
os.makedirs(AUDIO_UPLOAD_DIR, exist_ok=True)
os.makedirs(CHAT_IMAGES_DIR, exist_ok=True)


def hash_password(password: str) -> str:
    return hashlib.sha256(password.encode("utf-8")).hexdigest()


def require_admin(authorization):
    if not authorization:
        raise HTTPException(status_code=401, detail="请先登录")
    token = authorization.replace("Bearer ", "")
    user_id = get_user_by_token(token)
    if not user_id:
        raise HTTPException(status_code=401, detail="登录已过期")
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("SELECT email FROM users WHERE id = ?", (user_id,))
    row = c.fetchone()
    conn.close()
    if not row or row[0] != ADMIN_EMAIL:
        raise HTTPException(status_code=403, detail="没有管理员权限")
    return user_id


def get_user_by_token(token: str):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("SELECT user_id FROM sessions WHERE token = ?", (token,))
    row = c.fetchone()
    conn.close()
    return row[0] if row else None


def require_user(authorization):
    if not authorization:
        raise HTTPException(status_code=401, detail="请先登录")
    token = authorization.replace("Bearer ", "")
    user_id = get_user_by_token(token)
    if not user_id:
        raise HTTPException(status_code=401, detail="登录已过期")
    return user_id


def get_user_credits(user_id: int):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("SELECT credits FROM users WHERE id = ?", (user_id,))
    row = c.fetchone()
    conn.close()
    return row[0] if row else 0


def _mask_email(email: str) -> str:
    if not email:
        return "匿名"
    if "@" not in email:
        return email[:3] + "***"
    name, domain = email.split("@", 1)
    if len(name) <= 2:
        masked = name[0] + "***"
    else:
        masked = name[:2] + "***"
    return masked


def _ext_of(filename: str) -> str:
    if "." not in filename:
        return ""
    return "." + filename.rsplit(".", 1)[1].lower()


def _send_email_sync(to_email: str, subject: str, html_content: str):
    resp = requests.post(
        "https://api.resend.com/emails",
        headers={
            "Authorization": f"Bearer {RESEND_API_KEY}",
            "Content-Type": "application/json",
        },
        json={
            "from": f"{SMTP_FROM_NAME} <{RESEND_FROM_EMAIL}>",
            "to": [to_email],
            "subject": subject,
            "html": html_content,
        },
        timeout=15,
    )
    if resp.status_code not in (200, 201):
        raise Exception(f"Resend 发送失败: {resp.status_code} {resp.text}")


async def send_email(to_email: str, subject: str, html_content: str):
    await asyncio.to_thread(_send_email_sync, to_email, subject, html_content)


# ================= 邮件模板（深色卡片风格，两处邮件共用） =================
# 样式对齐用户提供的参考稿：深色圆角卡片 + 绿色标题 + 绿色按钮 + 分隔线 + 中英双语页脚。
# 只用一个函数，避免"验证邮箱"和"重置密码"两封信风格不一致。
# ⚠️ 邮件客户端兼容：全部用内联样式；不依赖外部 CSS/JS；正文用 <p> 与 <strong>/<br>，不用 <ul>。
def build_action_email(
    title: str,
    greeting: str,
    message: str,
    button_text: str,
    action_url: str,
    note: str,
    footer_en: str,
    footer_extra: str = "",
) -> str:
    return f"""\
<div style="background:#0a0a0a;padding:40px 24px;font-family:'Segoe UI',Arial,'Microsoft YaHei',sans-serif;text-align:center;">
  <div style="max-width:600px;margin:0 auto;background:#0a0a0a;border-radius:16px;padding:44px 36px;">
    <h1 style="color:#22c55e;font-size:24px;font-weight:700;margin:0 0 30px;letter-spacing:.5px;line-height:1.35;">{title}</h1>
    <p style="color:#e8e8e8;font-size:16px;line-height:1.9;margin:0 0 18px;">{greeting}</p>
    <p style="color:#e8e8e8;font-size:16px;line-height:1.9;margin:0 0 34px;">{message}</p>
    <p style="margin:0 0 34px;">
      <a href="{action_url}" style="display:inline-block;padding:16px 52px;background:#22c55e;color:#0a0a0a;text-decoration:none;border-radius:10px;font-weight:700;font-size:16px;">{button_text}</a>
    </p>
    <p style="color:#9ca3af;font-size:14px;line-height:1.9;margin:0 0 30px;">{note}</p>
    <hr style="border:none;border-top:1px solid #2a2a2a;margin:0 0 26px;" />
    <p style="color:#e8e8e8;font-size:14px;line-height:1.9;margin:0 0 12px;">{footer_en}</p>
    {footer_extra}
  </div>
</div>"""


# ================= Pydantic 模型 =================
class VideoRequest(BaseModel):
    prompt: str
    duration: float = 10.0
    megapixels: float = 0.9
    fast_mode: bool = True


class RegisterRequest(BaseModel):
    email: str
    password: str


class LoginRequest(BaseModel):
    email: str
    password: str


class ChatRequest(BaseModel):
    message: str
    voice: str = "voices_examples/male/male_02.wav"
    image: Optional[str] = None


class MultimodalRequest(BaseModel):
    prompt: str
    duration: float = 10.0
    megapixels: float = 0.4
    fast_mode: bool = True
    ref_image_0: str = ""
    ref_image_1: str = ""


class I2VRequest(BaseModel):
    prompt: str
    duration: float = 5.0
    megapixels: float = 0.4
    turbo_mode: bool = True
    first_frame: str = ""


class GoogleAuthRequest(BaseModel):
    credential: str


class ForgotPasswordRequest(BaseModel):
    email: str


class ResetPasswordRequest(BaseModel):
    token: str
    new_password: str


class ReversePromptRequest(BaseModel):
    image: str = ""


class SingleEditRequest(BaseModel):
    prompt: str = ""
    image: str = ""
    turbo_mode: bool = False


class MultiEditRequest(BaseModel):
    prompt: str = ""
    ref_image_0: str = ""
    ref_image_1: str = ""
    turbo_mode: bool = False


class FourViewRequest(BaseModel):
    image: str = ""
    append: str = "保持人物身份一致性"


class WorkLikeRequest(BaseModel):
    work_id: int


class VoiceCloneRequest(BaseModel):
    text: str
    reference_audio: str
    speed: float = 1.0
    seed: int = 14


class MusicRequest(BaseModel):
    caption: str = ""
    lyrics: str = ""
    duration: float = 120.0
    cfg_scale: float = 1.7
    top_k: int = 50
    seed: int = 14087943957953


class MotionTransferRequest(BaseModel):
    source_image: str = ""
    driving_video: str = ""
    mode: str = "motion"


class IdeaCreateRequest(BaseModel):
    content: str
    color: str = ""


class IdeaIdRequest(BaseModel):
    idea_id: int


class OllamaChatRequest(BaseModel):
    message: str = ""
    image: Optional[str] = None


class R2PresignRequest(BaseModel):
    filename: str
    content_type: str = "application/octet-stream"
    type: str = "image"
    is_cover: bool = False  # ★ 新增


class WorksRegisterRequest(BaseModel):
    type: str
    title: str = ""
    key: str
    public_url: str
    size: int = 0
    cover_url: str = ""  # ★ 新增
    # 2026-11-01 音乐作品：歌名走 title，这里补 风格 / 歌词 / 时长
    style: str = ""
    lyrics: str = ""
    duration: float = 0.0


# ================= 用户接口 =================
@app.post("/api/register")
async def register(req: RegisterRequest):
    if not req.email or not req.password:
        raise HTTPException(status_code=400, detail="邮箱和密码不能为空")
    if len(req.password) < 6:
        raise HTTPException(status_code=400, detail="密码至少6位")
    if not re.match(r"^[^@\s]+@[^@\s]+\.[^@\s]+$", req.email):
        raise HTTPException(status_code=400, detail="邮箱格式不正确")
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("SELECT id, is_verified FROM users WHERE email = ?", (req.email,))
    row = c.fetchone()
    if row:
        user_id, is_verified = row
        if is_verified:
            conn.close()
            raise HTTPException(status_code=400, detail="该邮箱已注册")
        c.execute(
            "UPDATE users SET password_hash = ? WHERE id = ?",
            (hash_password(req.password), user_id),
        )
    else:
        try:
            c.execute(
                "INSERT INTO users (email, password_hash, credits, created_at, is_verified) VALUES (?, ?, ?, ?, 0)",
                (req.email, hash_password(req.password), 0, datetime.now().isoformat()),
            )
            conn.commit()
            user_id = c.lastrowid
        except sqlite3.IntegrityError:
            conn.close()
            raise HTTPException(status_code=400, detail="该邮箱已注册")
    token = secrets.token_urlsafe(32)
    expires_at = (
        datetime.now() + timedelta(hours=VERIFY_TOKEN_EXPIRE_HOURS)
    ).isoformat()
    c.execute(
        "UPDATE email_verifications SET used = 1 WHERE user_id = ? AND used = 0",
        (user_id,),
    )
    c.execute(
        "INSERT INTO email_verifications (user_id, token, expires_at, used, created_at) VALUES (?, ?, ?, 0, ?)",
        (user_id, token, expires_at, datetime.now().isoformat()),
    )
    conn.commit()
    conn.close()
    verify_url = f"{SITE_URL}/?verify_token={token}"
    html = build_action_email(
        title="🔒 验证邮箱 / Verify Email",
        greeting="您好，",
        message=(
            "感谢注册 <strong>DM 造梦空间</strong>。"
            '点击验证后送 <strong style="color:#22c55e;">10,000 积分</strong>。'
        ),
        button_text="点击验证邮箱",
        action_url=verify_url,
        note="如果非本人操作，请忽略此邮件。",
        footer_en="Thanks for signing up for DM 造梦空间. Verify your email to get 10,000 credits.",
        footer_extra=(
            '<p style="color:#e8e8e8;font-size:13px;line-height:1.7;margin:0;">'
            "If you didn't request this, please ignore this email.</p>"
        ),
    )
    try:
        await send_email(req.email, "验证邮箱 - DM 造梦空间", html)
    except Exception as e:
        print(f"[MAIL] 发送失败: {e}")
        raise HTTPException(status_code=500, detail="邮件发送失败")
    return {"status": "ok", "message": "验证邮件已发送，请查收邮箱"}


@app.get("/api/verify_email")
async def verify_email(token: str):
    token = (token or "").strip()
    if not token:
        raise HTTPException(status_code=400, detail="缺少验证令牌")
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute(
        "SELECT id, user_id, expires_at, used FROM email_verifications WHERE token = ?",
        (token,),
    )
    row = c.fetchone()
    if not row:
        conn.close()
        raise HTTPException(status_code=400, detail="验证链接无效")
    verify_id, user_id, expires_at, used = row
    if used:
        conn.close()
        raise HTTPException(status_code=400, detail="验证链接已使用")
    try:
        if datetime.fromisoformat(expires_at) < datetime.now():
            conn.close()
            raise HTTPException(status_code=400, detail="验证链接已过期")
    except ValueError:
        conn.close()
        raise HTTPException(status_code=400, detail="验证链接无效")
    c.execute("SELECT is_verified FROM users WHERE id = ?", (user_id,))
    user_row = c.fetchone()
    if not user_row:
        conn.close()
        raise HTTPException(status_code=400, detail="账号不存在")
    if user_row[0] == 0:
        c.execute(
            "UPDATE users SET is_verified = 1, credits = credits + ? WHERE id = ?",
            (SIGNUP_CREDITS, user_id),
        )
    c.execute("UPDATE email_verifications SET used = 1 WHERE id = ?", (verify_id,))
    conn.commit()
    conn.close()
    return {"status": "ok", "message": "邮箱验证成功，请登录"}


@app.post("/api/login")
async def login(req: LoginRequest):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute(
        "SELECT id, password_hash, is_verified FROM users WHERE email = ?", (req.email,)
    )
    row = c.fetchone()
    if not row or row[1] != hash_password(req.password):
        conn.close()
        raise HTTPException(status_code=401, detail="邮箱或密码错误")
    user_id, _, is_verified = row
    if is_verified == 0:
        conn.close()
        raise HTTPException(status_code=403, detail="请先验证邮箱后再登录")
    token = str(uuid.uuid4())
    c.execute(
        "INSERT INTO sessions (token, user_id, created_at) VALUES (?, ?, ?)",
        (token, user_id, datetime.now().isoformat()),
    )
    conn.commit()
    conn.close()
    return {"status": "ok", "token": token, "email": req.email}


@app.post("/api/auth/google")
async def google_login(req: GoogleAuthRequest):
    try:
        idinfo = id_token.verify_oauth2_token(
            req.credential, google_requests.Request(), GOOGLE_CLIENT_ID
        )
        email = idinfo.get("email")
        google_sub = idinfo.get("sub")
        if not email:
            raise HTTPException(status_code=400, detail="无法从 Google 获取邮箱")
    except ValueError:
        raise HTTPException(status_code=401, detail="Google 登录凭证无效")
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("SELECT id FROM users WHERE email = ?", (email,))
    row = c.fetchone()
    if row:
        user_id = row[0]
    else:
        c.execute(
            "INSERT INTO users (email, password_hash, credits, created_at, is_verified) VALUES (?, ?, ?, ?, 1)",
            (email, f"google_{google_sub}", SIGNUP_CREDITS, datetime.now().isoformat()),
        )
        conn.commit()
        user_id = c.lastrowid
    token = str(uuid.uuid4())
    c.execute(
        "INSERT INTO sessions (token, user_id, created_at) VALUES (?, ?, ?)",
        (token, user_id, datetime.now().isoformat()),
    )
    conn.commit()
    conn.close()
    return {
        "status": "ok",
        "token": token,
        "email": email,
        "credits": get_user_credits(user_id),
    }


@app.post("/api/forgot_password")
async def forgot_password(req: ForgotPasswordRequest):
    email = (req.email or "").strip()
    if not email:
        raise HTTPException(status_code=400, detail="请输入邮箱")
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("SELECT id FROM users WHERE email = ?", (email,))
    row = c.fetchone()
    if row:
        c.execute(
            "SELECT created_at FROM password_resets WHERE user_id = ? ORDER BY id DESC LIMIT 1",
            (row[0],),
        )
        last = c.fetchone()
        if last:
            try:
                last_time = datetime.fromisoformat(last[0])
                if (datetime.now() - last_time).total_seconds() < 60:
                    conn.close()
                    raise HTTPException(
                        status_code=429, detail="请求过于频繁，请 60 秒后再试"
                    )
            except ValueError:
                pass
    if not row:
        conn.close()
        return {"status": "ok", "message": "如果该邮箱已注册，重置邮件已发送"}
    user_id = row[0]
    token = secrets.token_urlsafe(32)
    expires_at = (
        datetime.now() + timedelta(minutes=RESET_TOKEN_EXPIRE_MINUTES)
    ).isoformat()
    c.execute(
        "UPDATE password_resets SET used = 1 WHERE user_id = ? AND used = 0", (user_id,)
    )
    c.execute(
        "INSERT INTO password_resets (user_id, token, expires_at, used, created_at) VALUES (?, ?, ?, 0, ?)",
        (user_id, token, expires_at, datetime.now().isoformat()),
    )
    conn.commit()
    conn.close()
    reset_url = f"{SITE_URL}/?reset_token={token}"
    html = build_action_email(
        title="🔒 重置密码 / Reset Password",
        greeting="您好，",
        message="我们收到了您重置 <strong>DM 造梦空间</strong> 账号密码的请求。",
        button_text="点击重置密码",
        action_url=reset_url,
        note=f"此链接 {RESET_TOKEN_EXPIRE_MINUTES} 分钟内有效。如果非本人操作，请忽略此邮件。",
        footer_en="We received a request to reset your DM 造梦空间 password.",
        footer_extra=(
            f'<p style="color:#e8e8e8;font-size:13px;line-height:1.7;margin:0;">'
            f"This link expires in {RESET_TOKEN_EXPIRE_MINUTES} minutes. "
            f"If you didn't request this, please ignore this email.</p>"
        ),
    )
    try:
        await send_email(email, "重置密码 - DM 造梦空间", html)
    except Exception as e:
        raise HTTPException(status_code=500, detail="邮件发送失败")
    return {"status": "ok", "message": "重置邮件已发送，请查收"}


@app.post("/api/reset_password")
async def reset_password(req: ResetPasswordRequest):
    token = (req.token or "").strip()
    new_password = req.new_password or ""
    if not token:
        raise HTTPException(status_code=400, detail="缺少重置凭证")
    if len(new_password) < 6:
        raise HTTPException(status_code=400, detail="密码至少6位")
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute(
        "SELECT id, user_id, expires_at, used FROM password_resets WHERE token = ?",
        (token,),
    )
    row = c.fetchone()
    if not row:
        conn.close()
        raise HTTPException(status_code=400, detail="重置链接无效")
    reset_id, user_id, expires_at, used = row
    if used:
        conn.close()
        raise HTTPException(status_code=400, detail="重置链接已使用")
    try:
        if datetime.fromisoformat(expires_at) < datetime.now():
            conn.close()
            raise HTTPException(status_code=400, detail="重置链接已过期")
    except ValueError:
        conn.close()
        raise HTTPException(status_code=400, detail="重置链接无效")
    c.execute(
        "UPDATE users SET password_hash = ? WHERE id = ?",
        (hash_password(new_password), user_id),
    )
    c.execute("UPDATE password_resets SET used = 1 WHERE id = ?", (reset_id,))
    c.execute("DELETE FROM sessions WHERE user_id = ?", (user_id,))
    conn.commit()
    conn.close()
    return {"status": "ok", "message": "密码重置成功，请用新密码登录"}


@app.get("/api/me")
async def get_me(authorization: Optional[str] = Header(None)):
    user_id = require_user(authorization)
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("SELECT email, credits, avatar_url FROM users WHERE id = ?", (user_id,))
    row = c.fetchone()
    conn.close()
    if not row:
        raise HTTPException(status_code=404, detail="用户不存在")
    email, credits, avatar_url = row
    vip_ok, vip_plan, vip_expires = is_user_vip(user_id)
    return {
        "email": email,
        "credits": credits or 0,
        "avatar_url": avatar_url,
        "masked_email": _mask_email(email),
        "is_admin": email == ADMIN_EMAIL,
        "is_vip": vip_ok,
        "vip_plan": vip_plan if vip_ok else "none",
        "vip_expires_at": vip_expires,
    }


# ===== 充值 / VIP（2026-10-08）=====


@app.post("/api/upload_avatar")
async def upload_avatar(
    file: UploadFile = File(...), authorization: Optional[str] = Header(None)
):
    user_id = require_user(authorization)
    ext = _ext_of(file.filename or "")
    if ext not in ALLOWED_AVATAR_EXTS:
        raise HTTPException(status_code=400, detail=f"不支持的图片格式：{ext}")
    contents = await file.read()
    if len(contents) > AVATAR_MAX_BYTES:
        raise HTTPException(status_code=413, detail="头像不能超过 2 MB")
    os.makedirs(AVATARS_DIR, exist_ok=True)
    file_name = f"{uuid.uuid4().hex}{ext}"
    file_path = os.path.join(AVATARS_DIR, file_name)
    with open(file_path, "wb") as f:
        f.write(contents)
    avatar_url = f"/api/avatars/{file_name}"
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("UPDATE users SET avatar_url = ? WHERE id = ?", (avatar_url, user_id))
    conn.commit()
    conn.close()
    return {"status": "ok", "avatar_url": avatar_url}


@app.get("/api/avatars/{filename}")
async def get_avatar(filename: str):
    if not re.match(r"^[a-f0-9]{32}\.[a-zA-Z0-9]+$", filename):
        raise HTTPException(status_code=400, detail="文件名非法")
    file_path = os.path.join(AVATARS_DIR, filename)
    if not os.path.exists(file_path):
        raise HTTPException(status_code=404, detail="头像不存在")
    ext = _ext_of(filename)
    media_type = "image/jpeg"
    if ext == ".png":
        media_type = "image/png"
    elif ext == ".webp":
        media_type = "image/webp"
    return FileResponse(
        file_path,
        media_type=media_type,
        headers={"Cache-Control": "public, max-age=86400"},
    )


@app.get("/api/debug/r2")
async def debug_r2(authorization: str = Header(None)):
    require_admin(authorization)
    import os as _os

    return {
        "R2_ENDPOINT": _os.environ.get("R2_ENDPOINT", "<未设置>"),
        "R2_ACCOUNT_ID": _os.environ.get("R2_ACCOUNT_ID", "<未设置>"),
        "R2_ACCESS_KEY_ID_set": bool(_os.environ.get("R2_ACCESS_KEY_ID")),
        "R2_SECRET_ACCESS_KEY_set": bool(_os.environ.get("R2_SECRET_ACCESS_KEY")),
        "R2_BUCKET": _os.environ.get("R2_BUCKET", "<未设置>"),
        "R2_PUBLIC_BASE": _os.environ.get("R2_PUBLIC_BASE", "<未设置>"),
    }


@app.get("/api/stats")
async def stats(authorization: str = Header(None)):
    require_admin(authorization)
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("SELECT COUNT(*) FROM users")
    total_users = c.fetchone()[0]
    c.execute("SELECT COUNT(*) FROM tasks")
    total_tasks = c.fetchone()[0]
    conn.close()
    return {
        "total_users": total_users,
        "total_tasks": total_tasks,
        "active_tasks": len(active_tasks),
    }


@app.post("/api/upload")
async def upload_file(
    file: UploadFile = File(...), authorization: Optional[str] = Header(None)
):
    user_id = require_user(authorization)
    upload_dir = os.path.join(COMFYUI_DIR, "input")
    os.makedirs(upload_dir, exist_ok=True)
    safe_name = f"user_{uuid.uuid4().hex[:8]}_{file.filename}"
    file_path = os.path.join(upload_dir, safe_name)
    with open(file_path, "wb") as f:
        shutil.copyfileobj(file.file, f)
    return {"filename": safe_name}


@app.post("/api/upload_video")
async def upload_video(
    file: UploadFile = File(...), authorization: Optional[str] = Header(None)
):
    user_id = require_user(authorization)
    ext = _ext_of(file.filename or "")
    if ext not in {".mp4", ".webm", ".mov", ".mkv", ".avi", ".m4v"}:
        raise HTTPException(status_code=400, detail=f"不支持的视频格式：{ext}")
    if file.size and file.size > 3 * 1024 * 1024:
        raise HTTPException(status_code=413, detail="视频不能超过 3 MB")
    upload_dir = os.path.join(COMFYUI_DIR, "input")
    os.makedirs(upload_dir, exist_ok=True)
    safe_name = f"motion_{uuid.uuid4().hex[:8]}{ext}"
    contents = await file.read()
    loop = asyncio.get_event_loop()

    def _write():
        with open(os.path.join(upload_dir, safe_name), "wb") as f:
            f.write(contents)

    await loop.run_in_executor(None, _write)
    return {"status": "ok", "filename": safe_name}


# 注意：反推是 async 路由，且内部是同步轮询（sleep 会阻塞事件循环）——
# 这是既有实现，本次不改动它，避免顺手动到别人的核心流程。


# ================= 创意留言 =================
@app.get("/api/ideas/list")
async def ideas_list(authorization: Optional[str] = Header(None), limit: int = 100):
    current_user_id = None
    if authorization:
        tk = authorization.replace("Bearer ", "")
        current_user_id = get_user_by_token(tk)
    limit = max(1, min(200, limit))
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute(
        """SELECT i.id, i.user_id, i.content, i.color, i.created_at, i.updated_at,
                        u.email, u.avatar_url
                 FROM ideas i LEFT JOIN users u ON u.id = i.user_id
                 ORDER BY i.id DESC LIMIT ?""",
        (limit,),
    )
    rows = c.fetchall()
    liked_ids = set()
    like_counts = {}
    if rows:
        idea_ids = [r[0] for r in rows]
        ph = ",".join(["?"] * len(idea_ids))
        c.execute(
            f"SELECT idea_id, COUNT(*) FROM idea_likes WHERE idea_id IN ({ph}) GROUP BY idea_id",
            idea_ids,
        )
        for r in c.fetchall():
            like_counts[r[0]] = r[1]
        if current_user_id:
            c.execute(
                f"SELECT idea_id FROM idea_likes WHERE user_id = ? AND idea_id IN ({ph})",
                (current_user_id, *idea_ids),
            )
            liked_ids = {r[0] for r in c.fetchall()}
    conn.close()
    return {
        "ideas": [
            {
                "id": r[0],
                "content": r[2] or "",
                "color": r[3] or "",
                "created_at": r[4],
                "updated_at": r[5],
                "author": _mask_email(r[6]),
                "avatar_url": r[7],
                "is_mine": (current_user_id is not None and r[1] == current_user_id),
                "likes": like_counts.get(r[0], 0),
                "liked_by_me": r[0] in liked_ids,
            }
            for r in rows
        ]
    }


@app.post("/api/ideas/create")
async def ideas_create(
    req: IdeaCreateRequest, authorization: Optional[str] = Header(None)
):
    import html as _html

    user_id = require_user(authorization)
    content = (req.content or "").strip()
    if not content:
        raise HTTPException(status_code=400, detail="内容不能为空")
    if len(content) > 5000:
        raise HTTPException(status_code=400, detail="内容太长（最多 5000 字）")
    # ★ XSS 防护：后端转义 < > & " ' 等，即使前端漏了也不怕
    content = _html.escape(content)
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    now = datetime.now().isoformat()
    c.execute(
        "INSERT INTO ideas (user_id, content, color, created_at, updated_at) VALUES (?, ?, ?, ?, ?)",
        (user_id, content, req.color or "", now, now),
    )
    conn.commit()
    idea_id = c.lastrowid
    conn.close()
    return {"status": "ok", "id": idea_id}


@app.post("/api/ideas/like")
async def ideas_like(req: IdeaIdRequest, authorization: Optional[str] = Header(None)):
    user_id = require_user(authorization)
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("SELECT id FROM ideas WHERE id = ?", (req.idea_id,))
    if not c.fetchone():
        conn.close()
        raise HTTPException(status_code=404, detail="记录不存在")
    c.execute(
        "SELECT id FROM idea_likes WHERE idea_id = ? AND user_id = ?",
        (req.idea_id, user_id),
    )
    existed = c.fetchone()
    if existed:
        c.execute(
            "DELETE FROM idea_likes WHERE idea_id = ? AND user_id = ?",
            (req.idea_id, user_id),
        )
        liked = False
    else:
        c.execute(
            "INSERT INTO idea_likes (idea_id, user_id, created_at) VALUES (?, ?, ?)",
            (req.idea_id, user_id, datetime.now().isoformat()),
        )
        liked = True
    conn.commit()
    c.execute("SELECT COUNT(*) FROM idea_likes WHERE idea_id = ?", (req.idea_id,))
    new_likes = c.fetchone()[0]
    conn.close()
    return {"likes": new_likes, "liked": liked}


@app.delete("/api/ideas/{idea_id}")
async def ideas_delete(idea_id: int, authorization: Optional[str] = Header(None)):
    user_id = require_user(authorization)
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("SELECT user_id FROM ideas WHERE id = ?", (idea_id,))
    row = c.fetchone()
    if not row:
        conn.close()
        raise HTTPException(status_code=404, detail="记录不存在")
    if row[0] != user_id:
        conn.close()
        raise HTTPException(status_code=403, detail="只能删除自己的记录")
    c.execute("DELETE FROM idea_likes WHERE idea_id = ?", (idea_id,))
    c.execute("DELETE FROM ideas WHERE id = ?", (idea_id,))
    conn.commit()
    conn.close()
    return {"status": "ok", "message": "已删除"}


# ================= 作品区 =================
ALLOWED_IMAGE_EXTS = {".jpg", ".jpeg", ".png", ".gif", ".webp"}
ALLOWED_VIDEO_EXTS = {".mp4", ".webm", ".mov", ".m4v"}
ALLOWED_AUDIO_EXTS = {".mp3", ".m4a", ".aac", ".wav", ".ogg", ".opus", ".flac"}


def _make_video_cover(video_path: str) -> str:
    try:
        cover_name = f"{uuid.uuid4().hex}_cover.jpg"
        cover_path = os.path.join(WORKS_DIR, cover_name)
        subprocess.run(
            [
                FFMPEG_EXE,
                "-y",
                "-ss",
                "00:00:01",
                "-i",
                video_path,
                "-vframes",
                "1",
                "-vf",
                "scale=480:-1",
                cover_path,
            ],
            capture_output=True,
            timeout=30,
        )
        if os.path.exists(cover_path):
            return f"/api/works/file/{cover_name}"
    except Exception as e:
        print(f"[COVER] 抽帧失败: {e}")
    return None


@app.post("/api/r2/presign")
async def r2_presign(
    req: R2PresignRequest, authorization: Optional[str] = Header(None)
):
    user_id = require_user(authorization)

    # ===== 扩展名校验 =====
    ext = _ext_of(req.filename or "")
    if req.is_cover:
        # 封面固定 jpg，跳过校验
        pass
    elif req.type == "image":
        if ext not in ALLOWED_IMAGE_EXTS:
            raise HTTPException(status_code=400, detail=f"不支持的图片格式：{ext}")
    elif req.type == "audio":
        if ext not in ALLOWED_AUDIO_EXTS:
            raise HTTPException(status_code=400, detail=f"不支持的音频格式：{ext}")
    else:
        if ext not in ALLOWED_VIDEO_EXTS:
            raise HTTPException(status_code=400, detail=f"不支持的视频格式：{ext}")

    # ===== key 生成 =====
    if req.is_cover:
        key = f"works/covers/{uuid.uuid4().hex}.jpg"
    else:
        key = f"works/{uuid.uuid4().hex}{ext}"

    try:
        client = get_r2_client()
        upload_url = client.generate_presigned_url(
            "put_object",
            Params={
                "Bucket": R2_BUCKET,
                "Key": key,
                "ContentType": req.content_type or "application/octet-stream",
            },
            ExpiresIn=600,
        )
    except HTTPException:
        raise
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"生成 presign 失败: {e}")

    return {
        "upload_url": upload_url,
        "key": key,
        "public_url": f"{R2_PUBLIC_BASE}/{key}",
        "expires_in": 600,
    }


@app.post("/api/works/register")
async def works_register(
    req: WorksRegisterRequest, authorization: Optional[str] = Header(None)
):
    user_id = require_user(authorization)

    if req.type not in ("image", "video", "audio"):
        raise HTTPException(status_code=400, detail="type 错误")
    if not req.key.startswith("works/"):
        raise HTTPException(status_code=400, detail="key 非法")
    if not req.public_url.startswith(R2_PUBLIC_BASE):
        raise HTTPException(status_code=400, detail="public_url 非法")

    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("SELECT COUNT(*) FROM user_works WHERE user_id = ?", (user_id,))
    count = c.fetchone()[0]
    if count >= WORKS_MAX_PER_USER:
        conn.close()
        raise HTTPException(
            status_code=400, detail=f"最多上传 {WORKS_MAX_PER_USER} 个作品"
        )

    c.execute("SELECT avatar_url FROM users WHERE id = ?", (user_id,))
    row = c.fetchone()
    user_avatar = row[0] if row else None

    file_name = req.key
    file_url = req.public_url
    cover_url = None
    cover_url = req.cover_url or None  # ★ 用前端传来的

    c.execute(
        """INSERT INTO user_works
           (user_id, type, title, file_name, file_url, cover_url, avatar_url, likes, created_at,
            style, lyrics, duration)
           VALUES (?, ?, ?, ?, ?, ?, ?, 0, ?, ?, ?, ?)""",
        (
            user_id,
            req.type,
            req.title or "",
            file_name,
            file_url,
            cover_url,
            user_avatar,
            datetime.now().isoformat(),
            (req.style or "")[:60],
            (req.lyrics or "")[:8000],
            float(req.duration or 0),
        ),
    )
    conn.commit()
    work_id = c.lastrowid
    conn.close()

    return {
        "status": "ok",
        "work": {
            "id": work_id,
            "type": req.type,
            "title": req.title or "",
            "file_url": file_url,
            "cover_url": cover_url,
            "likes": 0,
            "liked_by_me": False,
            "is_mine": True,
            "created_at": datetime.now().isoformat(),
        },
    }


@app.post("/api/works/upload")
async def works_upload(
    file: UploadFile = File(...),
    type: str = Form("image"),
    title: str = Form(""),
    authorization: Optional[str] = Header(None),
):
    user_id = require_user(authorization)
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("SELECT COUNT(*) FROM user_works WHERE user_id = ?", (user_id,))
    count = c.fetchone()[0]
    conn.close()
    if count >= WORKS_MAX_PER_USER:
        raise HTTPException(
            status_code=400, detail=f"最多上传 {WORKS_MAX_PER_USER} 个作品"
        )
    ext = _ext_of(file.filename or "")
    if type == "image":
        if ext not in ALLOWED_IMAGE_EXTS:
            raise HTTPException(status_code=400, detail=f"不支持的图片格式：{ext}")
        max_size = WORKS_MAX_IMAGE_BYTES
    else:
        if ext not in ALLOWED_VIDEO_EXTS:
            raise HTTPException(status_code=400, detail=f"不支持的视频格式：{ext}")
        max_size = WORKS_MAX_VIDEO_BYTES
    contents = await file.read()
    if len(contents) > max_size:
        limit_mb = max_size // (1024 * 1024)
        raise HTTPException(status_code=413, detail=f"文件超过 {limit_mb} MB 限制")
    os.makedirs(WORKS_DIR, exist_ok=True)
    file_name = f"{uuid.uuid4().hex}{ext}"
    file_path = os.path.join(WORKS_DIR, file_name)
    with open(file_path, "wb") as f:
        f.write(contents)
    file_url = f"/api/works/file/{file_name}"
    cover_url = None
    if type == "video":
        cover_url = _make_video_cover(file_path)
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("SELECT avatar_url FROM users WHERE id = ?", (user_id,))
    row = c.fetchone()
    user_avatar = row[0] if row else None
    c.execute(
        """INSERT INTO user_works (user_id, type, title, file_name, file_url, cover_url, avatar_url, likes, created_at)
                 VALUES (?, ?, ?, ?, ?, ?, ?, 0, ?)""",
        (
            user_id,
            type,
            title or "",
            file_name,
            file_url,
            cover_url,
            user_avatar,
            datetime.now().isoformat(),
        ),
    )
    conn.commit()
    work_id = c.lastrowid
    conn.close()
    return {
        "status": "ok",
        "work": {
            "id": work_id,
            "type": type,
            "title": title or "",
            "file_url": file_url,
            "cover_url": cover_url,
            "likes": 0,
            "liked_by_me": False,
            "is_mine": True,
            "created_at": datetime.now().isoformat(),
        },
    }


@app.get("/api/works/list")
async def works_list(
    type: str = "image",
    limit: int = 24,
    offset: int = 0,
    sort: str = "latest",
    authorization: Optional[str] = Header(None),
):
    if type not in ("image", "video", "audio"):
        raise HTTPException(status_code=400, detail="类型错误")
    # 2026-10-08：首页作品区改成"每页 24 个（4 排 × 6 列）× 最多 10 页"，
    # 所以需要 offset，并且上限从 24 放宽到 48（留点余量，前端仍按 24 用）。
    try:
        limit = max(1, min(int(limit), 48))
    except (TypeError, ValueError):
        limit = 24
    try:
        offset = max(0, int(offset))
    except (TypeError, ValueError):
        offset = 0
    current_user_id = None
    if authorization:
        tk = authorization.replace("Bearer ", "")
        current_user_id = get_user_by_token(tk)
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    order_by = "w.likes DESC, w.id DESC" if sort == "hot" else "w.id DESC"
    total = c.execute(
        "SELECT COUNT(*) FROM user_works WHERE type = ?", (type,)
    ).fetchone()[0]
    c.execute(
        f"""SELECT w.id, w.user_id, w.type, w.title, w.file_url, w.cover_url, w.avatar_url, w.likes, w.created_at,
                        u.email, u.avatar_url, w.style, w.lyrics, w.duration
                 FROM user_works w LEFT JOIN users u ON u.id = w.user_id
                 WHERE w.type = ? ORDER BY {order_by} LIMIT ? OFFSET ?""",
        (type, limit, offset),
    )
    rows = c.fetchall()
    liked_ids = set()
    if current_user_id and rows:
        work_ids = [r[0] for r in rows]
        placeholders = ",".join(["?"] * len(work_ids))
        c.execute(
            f"SELECT work_id FROM user_work_likes WHERE user_id = ? AND work_id IN ({placeholders})",
            (current_user_id, *work_ids),
        )
        liked_ids = {r[0] for r in c.fetchall()}
    conn.close()
    works = []
    for r in rows:
        works.append(
            {
                "id": r[0],
                "type": r[2],
                "title": r[3] or "",
                "file_url": r[4],
                "cover_url": r[5],
                "avatar_url": r[10] or r[6],
                "likes": r[7] or 0,
                "liked_by_me": r[0] in liked_ids,
                "is_mine": (current_user_id is not None and r[1] == current_user_id),
                "author": _mask_email(r[9]),
                "created_at": r[8],
                "style": (r[11] or "") if len(r) > 11 else "",
                "lyrics": (r[12] or "") if len(r) > 12 else "",
                "duration": (r[13] or 0) if len(r) > 13 else 0,
            }
        )
    return {
        "works": works,
        # 分页信息（2026-10-08 新增）：前端据此算总页数、禁用/启用翻页按钮
        "total": total,
        "offset": offset,
        "limit": limit,
        "has_more": (offset + len(rows)) < total,
    }


@app.post("/api/works/like")
async def works_like(req: WorkLikeRequest, authorization: Optional[str] = Header(None)):
    user_id = require_user(authorization)
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("SELECT id FROM user_works WHERE id = ?", (req.work_id,))
    if not c.fetchone():
        conn.close()
        raise HTTPException(status_code=404, detail="作品不存在")
    c.execute(
        "SELECT id FROM user_work_likes WHERE work_id = ? AND user_id = ?",
        (req.work_id, user_id),
    )
    existed = c.fetchone()
    if existed:
        c.execute(
            "DELETE FROM user_work_likes WHERE work_id = ? AND user_id = ?",
            (req.work_id, user_id),
        )
        c.execute(
            "UPDATE user_works SET likes = MAX(likes - 1, 0) WHERE id = ?",
            (req.work_id,),
        )
        liked = False
    else:
        c.execute(
            "INSERT INTO user_work_likes (work_id, user_id, created_at) VALUES (?, ?, ?)",
            (req.work_id, user_id, datetime.now().isoformat()),
        )
        c.execute(
            "UPDATE user_works SET likes = likes + 1 WHERE id = ?", (req.work_id,)
        )
        liked = True
    conn.commit()
    c.execute("SELECT likes FROM user_works WHERE id = ?", (req.work_id,))
    new_likes = c.fetchone()[0]
    conn.close()
    return {"likes": new_likes, "liked": liked}


@app.delete("/api/works/{work_id}")
async def works_delete(work_id: int, authorization: Optional[str] = Header(None)):
    user_id = require_user(authorization)
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute(
        "SELECT user_id, file_name, cover_url FROM user_works WHERE id = ?", (work_id,)
    )
    row = c.fetchone()
    if not row:
        conn.close()
        raise HTTPException(status_code=404, detail="作品不存在")
    owner_id, file_name, cover_url = row
    if owner_id != user_id:
        conn.close()
        raise HTTPException(status_code=403, detail="只能删除自己的作品")

    if file_name and file_name.startswith("works/"):
        try:
            client = get_r2_client()
            client.delete_object(Bucket=R2_BUCKET, Key=file_name)
            if cover_url and cover_url.startswith("works/"):
                client.delete_object(Bucket=R2_BUCKET, Key=cover_url)
        except Exception as e:
            print(f"[DELETE_WORK] R2 删除失败: {e}")
    else:
        try:
            fp = os.path.join(WORKS_DIR, file_name)
            if os.path.exists(fp):
                os.remove(fp)
            if cover_url:
                cover_name = cover_url.rsplit("/", 1)[-1]
                cp = os.path.join(WORKS_DIR, cover_name)
                if os.path.exists(cp):
                    os.remove(cp)
        except Exception as e:
            print(f"[DELETE_WORK] 删文件失败: {e}")

    c.execute("DELETE FROM user_work_likes WHERE work_id = ?", (work_id,))
    c.execute("DELETE FROM user_works WHERE id = ?", (work_id,))
    conn.commit()
    conn.close()
    return {"status": "ok", "message": "已删除"}


@app.get("/api/works/file/{filename}")
async def works_file(filename: str):
    if not re.match(r"^[a-f0-9]{32}(_cover)?\.[a-zA-Z0-9]+$", filename):
        raise HTTPException(status_code=400, detail="文件名非法")
    file_path = os.path.join(WORKS_DIR, filename)
    if not os.path.exists(file_path):
        raise HTTPException(status_code=404, detail="文件不存在")
    ext = _ext_of(filename)
    media_type = {
        ".jpg": "image/jpeg",
        ".jpeg": "image/jpeg",
        ".png": "image/png",
        ".gif": "image/gif",
        ".webp": "image/webp",
        ".mp4": "video/mp4",
        ".webm": "video/webm",
        ".mov": "video/quicktime",
        ".m4v": "video/x-m4v",
    }.get(ext, "application/octet-stream")
    return FileResponse(
        file_path,
        media_type=media_type,
        headers={
            "Cache-Control": "public, max-age=604800, immutable",
            "Accept-Ranges": "bytes",
        },
    )


# ================= 聊天历史 =================


@app.get("/api/task_results")
async def get_task_results(
    authorization: Optional[str] = Header(None),
    task_type: str = "",
    limit: int = 20,
    offset: int = 0,
):
    """生成记录查询。task_type 支持逗号分隔多类型；返回 total / has_more 供前端分页。

    2026-10-08：新增 offset/total/has_more 与 result_filename（前端历史面板 + 按需下载用）。
    注意保留期逻辑不在此处（见 init_db 的 30 天清理），本次未改动。
    """
    user_id = require_user(authorization)
    # 收敛参数边界，防止 limit 传 0/负数或过大拖慢查询
    try:
        limit = max(1, min(int(limit), 100))
    except (TypeError, ValueError):
        limit = 20
    try:
        offset = max(0, int(offset))
    except (TypeError, ValueError):
        offset = 0
    types = [t.strip() for t in (task_type or "").split(",") if t.strip()]

    if types:
        ph = ",".join("?" * len(types))
        where = f"user_id = ? AND task_type IN ({ph})"
        args = [user_id] + types
    else:
        where = "user_id = ?"
        args = [user_id]

    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    # result_filename / result_text 是后加的列，老库可能没有 → 取不到就退化成 NULL
    try:
        total = c.execute(
            f"SELECT COUNT(*) FROM task_results WHERE {where}", args
        ).fetchone()[0]
        rows = c.execute(
            f"""SELECT task_type, prompt, result_url, created_at, result_filename, result_text
                    FROM task_results WHERE {where}
                    ORDER BY id DESC LIMIT ? OFFSET ?""",
            args + [limit, offset],
        ).fetchall()
    except sqlite3.OperationalError:
        total = c.execute(
            f"SELECT COUNT(*) FROM task_results WHERE {where}", args
        ).fetchone()[0]
        rows = c.execute(
            f"""SELECT task_type, prompt, result_url, created_at, result_filename, NULL
                    FROM task_results WHERE {where}
                    ORDER BY id DESC LIMIT ? OFFSET ?""",
            args + [limit, offset],
        ).fetchall()
    conn.close()

    return {
        "results": [
            {
                "task_type": r[0],
                "prompt": r[1],
                "result_url": r[2],
                "created_at": r[3],
                "result_filename": r[4],
                "result_text": r[5],
            }
            for r in rows
        ],
        "total": total,
        "offset": offset,
        "limit": limit,
        "has_more": (offset + len(rows)) < total,
    }


# ================= ComfyUI 代理（E 方案）=================
VIDEO_CACHE_DIR = os.path.join(WORKS_DIR, ".video_cache")
os.makedirs(VIDEO_CACHE_DIR, exist_ok=True)


IMAGE_CACHE_DIR = os.path.join(WORKS_DIR, ".image_cache")
os.makedirs(IMAGE_CACHE_DIR, exist_ok=True)


# ================= WebSocket：任务完成 =================
@app.websocket("/ws/{prompt_id}")
async def websocket_endpoint(websocket: WebSocket, prompt_id: str):
    await websocket.accept()
    try:
        while True:
            history = requests.get(f"{COMFYUI_SERVER}/history/{prompt_id}").json()
            if prompt_id in history:
                outputs = history[prompt_id]["outputs"]
                result_url = None
                result_kind = None
                result_filename = None

                for node_id, output in outputs.items():
                    if not isinstance(output, dict):
                        continue
                    for key in ["gifs", "videos", "video", "animated", "result"]:
                        if key in output and len(output[key]) > 0:
                            item = output[key][0]
                            if isinstance(item, dict) and "filename" in item:
                                fn = item["filename"]
                                sf = item.get("subfolder", "")
                                tp = item.get("type", "output")
                                result_url = f"https://api.cccccxai.com/api/video?filename={fn}&subfolder={sf}&type={tp}"
                                result_kind = "video"
                                result_filename = fn
                                print(
                                    f"[WS] 找到视频: node={node_id}, key={key}, file={fn}, subfolder={sf}"
                                )
                                break
                    if result_url:
                        break

                if not result_url:
                    for node_id, output in outputs.items():
                        if not isinstance(output, dict):
                            continue
                        for key, val in output.items():
                            if not isinstance(val, list):
                                continue
                            for item in val:
                                if isinstance(item, dict) and "filename" in item:
                                    fn = item["filename"]
                                    tp = item.get("type", "output")
                                    if tp == "input":
                                        continue
                                    if fn.lower().endswith(
                                        (".mp4", ".webm", ".mov", ".mkv", ".avi")
                                    ):
                                        sf = item.get("subfolder", "")
                                        result_url = f"https://api.cccccxai.com/api/video?filename={fn}&subfolder={sf}&type={tp}"
                                        result_kind = "video"
                                        result_filename = fn
                                        print(
                                            f"[WS] 兜底找到视频: node={node_id}, key={key}, file={fn}, subfolder={sf}"
                                        )
                                        break
                            if result_url:
                                break
                        if result_url:
                            break

                if not result_url:
                    for node_id, output in outputs.items():
                        if not isinstance(output, dict):
                            continue
                        for key in ["audio", "audios"]:
                            if key in output and len(output[key]) > 0:
                                info = output[key][0]
                                if isinstance(info, dict) and "filename" in info:
                                    fn = info["filename"]
                                    result_url = (
                                        f"https://api.cccccxai.com/api/audio/file/{fn}"
                                    )
                                    result_kind = "audio"
                                    result_filename = fn
                                    break
                        if result_url:
                            break

                all_images = []
                all_image_filenames = []
                for node_id, output in outputs.items():
                    if not isinstance(output, dict):
                        continue
                    if "images" in output and len(output["images"]) > 0:
                        for img in output["images"]:
                            if isinstance(img, dict) and "filename" in img:
                                all_images.append(
                                    f"https://api.cccccxai.com/api/image?filename={img['filename']}&subfolder={img.get('subfolder', '')}&type={img.get('type', 'output')}"
                                )
                                all_image_filenames.append(img["filename"])

                if not result_url and all_images:
                    result_url = all_images[0]
                    result_kind = "image"
                    result_filename = (
                        all_image_filenames[0] if all_image_filenames else None
                    )

                if not result_url:
                    print(f"[WS] {prompt_id} 没找到任何输出，outputs 结构：")
                    for nid, out in outputs.items():
                        if isinstance(out, dict):
                            print(f"[WS]   node {nid}: keys={list(out.keys())}")

                info = active_tasks.get(prompt_id, {})
                info_user_id = info.get("user_id")
                info_task_type = info.get("task_type", "t2v")
                info_prompt = info.get("prompt", "")
                active_tasks.pop(prompt_id, None)

                if result_url and info_user_id:
                    try:
                        conn = sqlite3.connect(DB_PATH)
                        c = conn.cursor()
                        c.execute(
                            """INSERT OR REPLACE INTO task_results
                                     (user_id, prompt_id, task_type, prompt, result_url, result_filename, created_at, updated_at)
                                     VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                            (
                                info_user_id,
                                prompt_id,
                                info_task_type,
                                info_prompt,
                                result_url,
                                result_filename,
                                datetime.now().isoformat(),
                                datetime.now().isoformat(),
                            ),
                        )
                        conn.commit()
                        conn.close()
                    except Exception as e:
                        print(f"[WS] 写 task_results 失败: {e}")

                await websocket.send_json(
                    {
                        "status": "completed",
                        "kind": result_kind,
                        "url": result_url,
                        "video_url": result_url if result_kind == "video" else None,
                        "image_url": result_url if result_kind == "image" else None,
                        "audio_url": result_url if result_kind == "audio" else None,
                        "images": all_images,
                    }
                )
                threading.Thread(target=_unload_comfy_models, daemon=True).start()
                break
            else:
                await websocket.send_json({"status": "generating"})
            await asyncio.sleep(3)
    except WebSocketDisconnect:
        pass
    except Exception as e:
        try:
            await websocket.send_json({"status": "error", "message": str(e)})
        except:
            pass
    finally:
        try:
            await websocket.close()
        except:
            pass


@app.websocket("/ws_chat/{prompt_id}")
async def websocket_chat(websocket: WebSocket, prompt_id: str):
    await websocket.accept()
    try:
        while True:
            history = requests.get(f"{COMFYUI_SERVER}/history/{prompt_id}").json()
            if prompt_id in history:
                outputs = history[prompt_id]["outputs"]
                reply_text = None
                audio_url = None
                audio_filename = None
                if "81" in outputs and "text" in outputs["81"]:
                    reply_text = (
                        outputs["81"]["text"][0]
                        if isinstance(outputs["81"]["text"], list)
                        else outputs["81"]["text"]
                    )
                if "206" in outputs:
                    out206 = outputs["206"]
                    for key in ["audio", "audios"]:
                        if key in out206 and len(out206[key]) > 0:
                            info = out206[key][0]
                            if isinstance(info, dict) and "filename" in info:
                                fn = info["filename"]
                                audio_url = f"https://api.cccccxai.com/api/audio?filename={fn}&subfolder={info.get('subfolder', '')}&type={info.get('type', 'output')}"
                                audio_filename = fn
                                break
                if not audio_url:
                    for node_id, output in outputs.items():
                        for key in ["audio", "audios"]:
                            if key in output and len(output[key]) > 0:
                                info = output[key][0]
                                if isinstance(info, dict) and "filename" in info:
                                    fn = info["filename"]
                                    audio_url = f"https://api.cccccxai.com/api/audio?filename={fn}&subfolder={info.get('subfolder', '')}&type={info.get('type', 'output')}"
                                    audio_filename = fn
                                    break
                        if audio_url:
                            break
                info = active_tasks.get(prompt_id, {})
                info_user_id = info.get("user_id")
                active_tasks.pop(prompt_id, None)
                if info_user_id and (reply_text or audio_url):
                    try:
                        conn = sqlite3.connect(DB_PATH)
                        c = conn.cursor()
                        c.execute(
                            """INSERT INTO chat_messages (user_id, role, content, audio_url, created_at)
                                     VALUES (?, 'ai', ?, ?, ?)""",
                            (
                                info_user_id,
                                reply_text or "",
                                audio_url,
                                datetime.now().isoformat(),
                            ),
                        )
                        if audio_filename:
                            try:
                                c.execute(
                                    """INSERT OR REPLACE INTO task_results
                                       (user_id, prompt_id, task_type, prompt, result_url, result_filename, created_at, updated_at)
                                       VALUES (?, ?, 'chat_tts', '', ?, ?, ?, ?)""",
                                    (
                                        info_user_id,
                                        prompt_id,
                                        audio_url,
                                        audio_filename,
                                        datetime.now().isoformat(),
                                        datetime.now().isoformat(),
                                    ),
                                )
                            except Exception as e:
                                print(f"[CHAT] 写 task_results 失败: {e}")
                        conn.commit()
                        conn.close()
                    except Exception as e:
                        print(f"[CHAT] 写库失败: {e}")
                await websocket.send_json(
                    {
                        "status": "completed",
                        "reply_text": reply_text,
                        "audio_url": audio_url,
                    }
                )
                threading.Thread(target=_unload_comfy_models, daemon=True).start()
                break
            else:
                await websocket.send_json({"status": "generating"})
            await asyncio.sleep(3)
    except WebSocketDisconnect:
        pass
    except Exception as e:
        try:
            await websocket.send_json({"status": "error", "message": str(e)})
        except:
            pass
    finally:
        try:
            await websocket.close()
        except:
            pass


# ================= 音乐公共曲库 =================
MUSIC_LIBRARY_DIR = os.path.join(WORKS_DIR, "music")
os.makedirs(MUSIC_LIBRARY_DIR, exist_ok=True)


def _ensure_music_tables():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("""CREATE TABLE IF NOT EXISTS music_library (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER NOT NULL,
        title TEXT DEFAULT '',
        file_name TEXT NOT NULL,
        file_url TEXT NOT NULL,
        duration REAL DEFAULT 0,
        plays INTEGER DEFAULT 0,
        created_at TEXT NOT NULL)""")
    c.execute("""CREATE TABLE IF NOT EXISTS music_likes (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        music_id INTEGER NOT NULL,
        user_id INTEGER NOT NULL,
        created_at TEXT NOT NULL,
        UNIQUE(music_id, user_id))""")
    c.execute(
        "CREATE INDEX IF NOT EXISTS idx_music_library_id ON music_library(id DESC)"
    )
    c.execute("CREATE INDEX IF NOT EXISTS idx_music_likes_user ON music_likes(user_id)")
    conn.commit()
    conn.close()


_ensure_music_tables()


@app.get("/api/music/library")
async def music_library_list(
    authorization: Optional[str] = Header(None), limit: int = 100, random: int = 0
):
    current_user_id = None
    if authorization:
        tk = authorization.replace("Bearer ", "")
        current_user_id = get_user_by_token(tk)
    limit = max(1, min(200, limit))
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    order_by = "RANDOM()" if random else "m.id DESC"
    c.execute(
        f"""SELECT m.id, m.user_id, m.title, m.file_url, m.plays, m.created_at,
                        u.email, u.avatar_url
                 FROM music_library m LEFT JOIN users u ON u.id = m.user_id
                 ORDER BY {order_by} LIMIT ?""",
        (limit,),
    )
    rows = c.fetchall()
    like_counts = {}
    liked_ids = set()
    if rows:
        ids = [r[0] for r in rows]
        ph = ",".join(["?"] * len(ids))
        c.execute(
            f"SELECT music_id, COUNT(*) FROM music_likes WHERE music_id IN ({ph}) GROUP BY music_id",
            ids,
        )
        for r in c.fetchall():
            like_counts[r[0]] = r[1]
        if current_user_id:
            c.execute(
                f"SELECT music_id FROM music_likes WHERE user_id = ? AND music_id IN ({ph})",
                (current_user_id, *ids),
            )
            liked_ids = {r[0] for r in c.fetchall()}
    conn.close()
    return {
        "musics": [
            {
                "id": r[0],
                "title": r[2] or "未命名",
                "file_url": r[3],
                "plays": r[4] or 0,
                "created_at": r[5],
                "author": _mask_email(r[6]),
                "avatar_url": r[7],
                "is_mine": (current_user_id is not None and r[1] == current_user_id),
                "likes": like_counts.get(r[0], 0),
                "liked_by_me": r[0] in liked_ids,
            }
            for r in rows
        ]
    }


@app.post("/api/music/like")
async def music_like(req: dict, authorization: Optional[str] = Header(None)):
    user_id = require_user(authorization)
    mid = req.get("music_id")
    if not mid:
        raise HTTPException(status_code=400, detail="缺少 music_id")
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("SELECT id FROM music_library WHERE id = ?", (mid,))
    if not c.fetchone():
        conn.close()
        raise HTTPException(status_code=404, detail="音乐不存在")
    c.execute(
        "SELECT id FROM music_likes WHERE music_id = ? AND user_id = ?", (mid, user_id)
    )
    if c.fetchone():
        c.execute(
            "DELETE FROM music_likes WHERE music_id = ? AND user_id = ?", (mid, user_id)
        )
        liked = False
    else:
        c.execute(
            "INSERT INTO music_likes (music_id, user_id, created_at) VALUES (?, ?, ?)",
            (mid, user_id, datetime.now().isoformat()),
        )
        liked = True
    conn.commit()
    c.execute("SELECT COUNT(*) FROM music_likes WHERE music_id = ?", (mid,))
    n = c.fetchone()[0]
    conn.close()
    return {"likes": n, "liked": liked}


@app.post("/api/music/play")
async def music_play(req: dict):
    mid = req.get("music_id")
    if not mid:
        raise HTTPException(status_code=400, detail="缺少 music_id")
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("UPDATE music_library SET plays = plays + 1 WHERE id = ?", (mid,))
    conn.commit()
    c.execute("SELECT plays FROM music_library WHERE id = ?", (mid,))
    row = c.fetchone()
    conn.close()
    return {"plays": row[0] if row else 0}


@app.delete("/api/music/{music_id}")
async def music_delete(music_id: int, authorization: Optional[str] = Header(None)):
    user_id = require_user(authorization)
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("SELECT user_id, file_name FROM music_library WHERE id = ?", (music_id,))
    row = c.fetchone()
    if not row:
        conn.close()
        raise HTTPException(status_code=404, detail="音乐不存在")
    if row[0] != user_id:
        conn.close()
        raise HTTPException(status_code=403, detail="只能删除自己发布的")
    try:
        fp = os.path.join(MUSIC_LIBRARY_DIR, row[1])
        if os.path.isfile(fp):
            os.remove(fp)
    except Exception as e:
        print(f"[MUSIC] 删文件失败: {e}")
    c.execute("DELETE FROM music_likes WHERE music_id = ?", (music_id,))
    c.execute("DELETE FROM music_library WHERE id = ?", (music_id,))
    conn.commit()
    conn.close()
    return {"status": "ok"}


@app.get("/api/music/file/{filename}")
async def music_file(filename: str, dl: int = 0):
    if ".." in filename or "/" in filename or "\\" in filename:
        raise HTTPException(status_code=400, detail="文件名非法")
    fp = os.path.join(MUSIC_LIBRARY_DIR, filename)
    if not os.path.isfile(fp):
        raise HTTPException(status_code=404, detail="文件不存在")
    headers = {"Cache-Control": "public, max-age=604800", "Accept-Ranges": "bytes"}
    if dl:
        headers["Content-Disposition"] = f'attachment; filename="{filename}"'
    return FileResponse(fp, media_type="audio/mpeg", headers=headers)



# ================= 游戏托管：下载（需登录）=================
GAME_DOWNLOAD_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "game-portal", "downloads")
GAME_DOWNLOADS = {
    "posthuman": os.path.join(GAME_DOWNLOAD_DIR, "PostHumanEra-Windows.zip"),
}


@app.get("/api/game/download/{slug}")
def game_download(slug: str, authorization: Optional[str] = Header(None)):
    if not authorization:
        raise HTTPException(401, "请先登录")
    tk = authorization.replace("Bearer ", "")
    if not get_user_by_token(tk):
        raise HTTPException(401, "登录已过期")
    fp = GAME_DOWNLOADS.get(slug)
    if not fp or not os.path.exists(fp):
        raise HTTPException(404, "游戏包不存在")
    return FileResponse(fp, filename=os.path.basename(fp), media_type="application/zip")


# ================= 游戏托管：静态挂载（必须放在 catch-all 之前）=================
_GAME_ROOT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "game")
os.makedirs(_GAME_ROOT, exist_ok=True)
try:
    from starlette.staticfiles import StaticFiles as _StaticFiles

    class _GameStatic(_StaticFiles):
        """Godot Web 包专用：修 MIME + 给大文件长缓存（2026-10-27）。"""

        async def get_response(self, path, scope):
            resp = await super().get_response(path, scope)
            low = str(path).lower()
            if low.endswith(".wasm"):
                resp.headers["Content-Type"] = "application/wasm"
            elif low.endswith(".pck"):
                resp.headers["Content-Type"] = "application/octet-stream"
            if low.endswith((".wasm", ".pck", ".png", ".jpg", ".jpeg", ".js", ".worklet.js")):
                resp.headers["Cache-Control"] = "public, max-age=31536000, immutable"
            elif low.endswith((".html", "")):
                resp.headers["Cache-Control"] = "no-cache"
            return resp

    app.mount("/game", _GameStatic(directory=_GAME_ROOT, html=True), name="gameportal")
    print("[game] mounted /game ->", _GAME_ROOT, "(wasm MIME + long cache)")
except Exception as _e:
    print("[game] mount failed:", _e)


# ================= 下载目录（APK 安装包，2026-10-27）=================
_DOWNLOAD_ROOT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "download")
os.makedirs(_DOWNLOAD_ROOT, exist_ok=True)
try:
    class _DownloadStatic(_StaticFiles):
        async def get_response(self, path, scope):
            resp = await super().get_response(path, scope)
            if str(path).lower().endswith(".apk"):
                resp.headers["Content-Type"] = "application/vnd.android.package-archive"
            resp.headers["Cache-Control"] = "no-cache"
            return resp

    app.mount("/download", _DownloadStatic(directory=_DOWNLOAD_ROOT, html=False), name="downloads")
    print("[download] mounted /download ->", _DOWNLOAD_ROOT)
except Exception as _e:
    print("[download] mount failed:", _e)


# ================= 静态文件 =================
_STATIC_DIR = os.path.dirname(os.path.abspath(__file__))


@app.get("/")
async def serve_index():
    index_path = os.path.join(_STATIC_DIR, "index.html")
    if os.path.isfile(index_path):
        return FileResponse(index_path, media_type="text/html")
    return {"detail": "index.html not found"}


# 只允许这些扩展名通过静态兜底（其余一律 404）
_ALLOWED_STATIC_EXT = {
    ".html", ".css", ".js", ".mjs", ".json",
    ".png", ".jpg", ".jpeg", ".gif", ".webp", ".svg", ".ico",
    ".woff", ".woff2", ".ttf",
    ".mp4", ".webm", ".mp3",
}


# ================= SEO：robots.txt + sitemap.xml（2026-10-27 新增）=================
# 背景：主站原来 /robots.txt 与 /sitemap.xml 都是 404（搜索引擎没有任何入口）。
# ⚠️ 公网 /robots.txt 目前被 Cloudflare 的「托管 robots.txt」盖住（2.6 KB 的
#    content-signals 文件），要让我们这份生效，需在 CF 面板关掉该功能；
#    /sitemap.xml 不受影响，CF 会直接回源。
# 只收录确认可访问的公开页面（2026-10-27 实测全部 200）。
_SEO_SITE = "https://www.cccccxai.com"
_SEO_PAGES = [
    ("/", "1.0", "daily"),
    ("/game/posthuman/", "0.9", "weekly"),
    ("/credits.html", "0.4", "monthly"),
    ("/pricing.html", "0.4", "monthly"),
    ("/privacy.html", "0.3", "yearly"),
    ("/terms.html", "0.3", "yearly"),
    ("/refund.html", "0.3", "yearly"),
]


@app.get("/robots.txt", include_in_schema=False)
async def robots_txt():
    from fastapi.responses import PlainTextResponse
    body = ("User-agent: *\n"
            "Allow: /\n"
            "Disallow: /api/\n"
            "\n"
            "Sitemap: %s/sitemap.xml\n" % _SEO_SITE)
    return PlainTextResponse(body, headers={"Cache-Control": "public, max-age=3600"})


@app.get("/sitemap.xml", include_in_schema=False)
async def sitemap_xml():
    from fastapi.responses import Response
    from datetime import datetime as _dt
    _today = _dt.now().strftime("%Y-%m-%d")
    _items = "".join(
        "  <url><loc>%s%s</loc><lastmod>%s</lastmod>"
        "<changefreq>%s</changefreq><priority>%s</priority></url>\n"
        % (_SEO_SITE, _p, _today, _freq, _prio)
        for _p, _prio, _freq in _SEO_PAGES
    )
    _xml = ('<?xml version="1.0" encoding="UTF-8"?>\n'
            '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">\n'
            + _items + '</urlset>\n')
    return Response(content=_xml, media_type="application/xml",
                    headers={"Cache-Control": "public, max-age=3600"})


@app.get("/{filename:path}")
async def serve_static(filename: str):
    if ".." in filename or filename.startswith("/"):
        raise HTTPException(status_code=400, detail="非法路径")
    # 🔒 2026-10-27 安全修复：兜底路由原来会把 .env / users.db / server.py / 日志 / *.md
    # 直接吐给公网（实测 https://站点/.env 返回 200）。改成扩展名白名单 + 拒绝以 . 或 _
    # 开头的路径段（挡掉 _backups、_tmp_upload*、.env.local、__pycache__）。
    _segs = [s for s in filename.replace("\\", "/").split("/") if s]
    if any(s.startswith(".") or s.startswith("_") for s in _segs):
        raise HTTPException(status_code=404, detail="文件不存在")
    if _ext_of(filename).lower() not in _ALLOWED_STATIC_EXT:
        raise HTTPException(status_code=404, detail="文件不存在")
    file_path = os.path.join(_STATIC_DIR, filename)
    if not os.path.isfile(file_path):
        raise HTTPException(status_code=404, detail="文件不存在")
    ext = _ext_of(filename)
    media_type = {
        ".html": "text/html",
        ".css": "text/css",
        ".js": "application/javascript",
        ".png": "image/png",
        ".jpg": "image/jpeg",
        ".jpeg": "image/jpeg",
        ".gif": "image/gif",
        ".webp": "image/webp",
        ".svg": "image/svg+xml",
        ".ico": "image/x-icon",
        ".json": "application/json",
        ".wasm": "application/wasm",
        ".pck": "application/octet-stream",
        ".data": "application/octet-stream",
        ".woff": "font/woff",
        ".woff2": "font/woff2",
    }.get(ext, "application/octet-stream")
    return FileResponse(file_path, media_type=media_type)


# ================= 临时上传通道（2026-10-26，仅本次传包用，用完可删）=================
_TMP_UPLOAD_DIR = r"F:\_inbox"
_TMP_TOKEN_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "_tmp_upload_token.txt")


def _tmp_token():
    try:
        with open(_TMP_TOKEN_FILE, "r", encoding="utf-8") as f:
            return f.read().strip()
    except Exception:
        return ""


@app.post("/api/tmp/upload")
async def tmp_upload(request: Request, filename: str = "upload.bin", t: str = ""):
    """一次性 token + 单文件上传，落到 F:\\_inbox。用完删 token 文件即失效。"""
    expect = _tmp_token()
    if not expect or t != expect:
        raise HTTPException(status_code=403, detail="token 无效或已使用")
    safe = re.sub(r"[^A-Za-z0-9._-]", "_", os.path.basename(filename))[:120] or "upload.bin"
    os.makedirs(_TMP_UPLOAD_DIR, exist_ok=True)
    dst = os.path.join(_TMP_UPLOAD_DIR, safe)
    total = 0
    with open(dst, "wb") as f:
        async for chunk in request.stream():
            if chunk:
                total += len(chunk)
                if total > 800 * 1024 * 1024:
                    f.close()
                    try:
                        os.remove(dst)
                    except Exception:
                        pass
                    raise HTTPException(status_code=413, detail="文件过大")
                f.write(chunk)
    # 单次有效
    try:
        os.remove(_TMP_TOKEN_FILE)
    except Exception:
        pass
    return {"ok": True, "saved": dst, "bytes": total}


# ================= 分片上传 =================
CHUNK_UPLOAD_DIR = os.path.join(COMFYUI_DIR, "input", ".chunks")
os.makedirs(CHUNK_UPLOAD_DIR, exist_ok=True)


WORKS_CHUNK_DIR = os.path.join(WORKS_DIR, ".chunks")
os.makedirs(WORKS_CHUNK_DIR, exist_ok=True)
WORKS_CHUNK_MAX_BYTES = 2 * 1024 * 1024


@app.post("/api/works/upload_chunk")
async def works_upload_chunk(
    chunk: UploadFile = File(...),
    upload_id: str = Form(...),
    chunk_index: int = Form(...),
    total_chunks: int = Form(...),
    filename: str = Form(...),
    type: str = Form("video"),
    title: str = Form(""),
    authorization: Optional[str] = Header(None),
):
    user_id = require_user(authorization)

    ext = _ext_of(filename or "")
    if ext not in ALLOWED_VIDEO_EXTS:
        raise HTTPException(status_code=400, detail=f"不支持的视频格式：{ext}")

    contents = await chunk.read()
    if len(contents) > WORKS_CHUNK_MAX_BYTES:
        raise HTTPException(
            status_code=413,
            detail=f"单片不能超过 {WORKS_CHUNK_MAX_BYTES // 1024 // 1024} MB",
        )

    safe_uid = re.sub(r"[^a-f0-9]", "", upload_id.lower())
    if len(safe_uid) < 8:
        raise HTTPException(status_code=400, detail="upload_id 非法")

    part_dir = os.path.join(WORKS_CHUNK_DIR, safe_uid)
    os.makedirs(part_dir, exist_ok=True)

    part_path = os.path.join(part_dir, f"{chunk_index:06d}.part")
    loop = asyncio.get_event_loop()

    def _write():
        with open(part_path, "wb") as f:
            f.write(contents)

    await loop.run_in_executor(None, _write)
    return {"status": "ok", "chunk_index": chunk_index}


@app.post("/api/works/upload_merge")
async def works_upload_merge(req: dict, authorization: Optional[str] = Header(None)):
    user_id = require_user(authorization)

    upload_id = (req.get("upload_id") or "").strip()
    total_chunks = int(req.get("total_chunks") or 0)
    filename = (req.get("filename") or "").strip()
    title = (req.get("title") or "").strip()
    type_ = (req.get("type") or "video").strip()

    if not upload_id or total_chunks <= 0 or not filename:
        raise HTTPException(status_code=400, detail="参数不完整")

    safe_uid = re.sub(r"[^a-f0-9]", "", upload_id.lower())
    if len(safe_uid) < 8:
        raise HTTPException(status_code=400, detail="upload_id 非法")

    part_dir = os.path.join(WORKS_CHUNK_DIR, safe_uid)
    if not os.path.isdir(part_dir):
        raise HTTPException(status_code=404, detail="未找到上传会话")

    parts = []
    for i in range(total_chunks):
        p = os.path.join(part_dir, f"{i:06d}.part")
        if not os.path.isfile(p):
            raise HTTPException(status_code=400, detail=f"缺少第 {i} 个分片")
        parts.append(p)

    ext = _ext_of(filename or "")
    if ext not in ALLOWED_VIDEO_EXTS:
        raise HTTPException(status_code=400, detail=f"不支持的视频格式：{ext}")

    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("SELECT COUNT(*) FROM user_works WHERE user_id = ?", (user_id,))
    count = c.fetchone()[0]
    if count >= WORKS_MAX_PER_USER:
        conn.close()
        raise HTTPException(
            status_code=400, detail=f"最多上传 {WORKS_MAX_PER_USER} 个作品"
        )

    file_name = f"{uuid.uuid4().hex}{ext}"
    final_path = os.path.join(WORKS_DIR, file_name)

    loop = asyncio.get_event_loop()

    def _merge():
        with open(final_path, "wb") as out:
            for p in parts:
                with open(p, "rb") as f:
                    shutil.copyfileobj(f, out)

    try:
        await loop.run_in_executor(None, _merge)
    except Exception as e:
        conn.close()
        raise HTTPException(status_code=500, detail=f"合并失败: {e}")
    finally:
        try:
            shutil.rmtree(part_dir, ignore_errors=True)
        except Exception:
            pass

    cover_url = _make_video_cover(final_path)
    file_url = f"/api/works/file/{file_name}"

    c.execute("SELECT avatar_url FROM users WHERE id = ?", (user_id,))
    row = c.fetchone()
    user_avatar = row[0] if row else None

    c.execute(
        """INSERT INTO user_works (user_id, type, title, file_name, file_url, cover_url, avatar_url, likes, created_at)
                 VALUES (?, ?, ?, ?, ?, ?, ?, 0, ?)""",
        (
            user_id,
            type_,
            title or "",
            file_name,
            file_url,
            cover_url,
            user_avatar,
            datetime.now().isoformat(),
        ),
    )
    conn.commit()
    work_id = c.lastrowid
    conn.close()

    return {
        "status": "ok",
        "work": {
            "id": work_id,
            "type": type_,
            "title": title or "",
            "file_url": file_url,
            "cover_url": cover_url,
            "likes": 0,
            "liked_by_me": False,
            "is_mine": True,
            "created_at": datetime.now().isoformat(),
        },
    }




# ================= 管理员 =================
@app.delete("/api/admin/work/{work_id}")
async def admin_delete_work(work_id: int, authorization: str = Header(None)):
    require_admin(authorization)
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("SELECT file_name, cover_url FROM user_works WHERE id = ?", (work_id,))
    row = c.fetchone()
    if not row:
        conn.close()
        raise HTTPException(status_code=404, detail="作品不存在")
    file_name, cover_url = row

    if file_name and file_name.startswith("works/"):
        try:
            client = get_r2_client()
            client.delete_object(Bucket=R2_BUCKET, Key=file_name)
            if cover_url and cover_url.startswith("works/"):
                client.delete_object(Bucket=R2_BUCKET, Key=cover_url)
        except Exception as e:
            print(f"[ADMIN] R2 删除失败: {e}")
    else:
        try:
            fp = os.path.join(WORKS_DIR, file_name)
            if os.path.exists(fp):
                os.remove(fp)
            if cover_url:
                cn = cover_url.rsplit("/", 1)[-1]
                cp = os.path.join(WORKS_DIR, cn)
                if os.path.exists(cp):
                    os.remove(cp)
        except Exception as e:
            print(f"[ADMIN] 删文件失败: {e}")

    c.execute("DELETE FROM user_work_likes WHERE work_id = ?", (work_id,))
    c.execute("DELETE FROM user_works WHERE id = ?", (work_id,))
    conn.commit()
    conn.close()
    return {"status": "ok", "message": "已删除"}


@app.delete("/api/admin/idea/{idea_id}")
async def admin_delete_idea(idea_id: int, authorization: str = Header(None)):
    require_admin(authorization)
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("DELETE FROM idea_likes WHERE idea_id = ?", (idea_id,))
    c.execute("DELETE FROM ideas WHERE id = ?", (idea_id,))
    conn.commit()
    conn.close()
    return {"status": "ok", "message": "已删除"}


if __name__ == "__main__":
    import uvicorn

    # 绑 127.0.0.1：不对局域网/公网直接暴露。
    # 公网入口是本机的 Cloudflare Tunnel（cloudflared 在 LocalSystem 上跑，
    # 走 loopback 连进来），tailnet 入口是 tailscale serve —— 两者都不受影响。
    # 改了这里必须重启后端才生效（见 AGENTS.md「服务重启铁律」）。
    uvicorn.run(app, host="127.0.0.1", port=8000)
