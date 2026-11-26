"""
VIP / 积分 / 卡密兑换 —— 第三站(520xai) 与 新站(dmllm) 共用模块（2026-10-30）

设计来源：主站 `CREDITS-VIP-DESIGN.md`（2026-10-08 上线、2026-10-26 随"生成能力"下架）。
本模块实现的是该文档 §11.2 推荐的 **方案 B：卡密/兑换码**——
即「独角数卡卖出卡密 → 用户在本站输入卡密 → 立即加积分/开 VIP」，
不含方案 A 的人工转账收银台（那套的行前缀/状态机思路见文档，需要时再恢复）。

设计要点（照抄主站踩过的坑）：
  1. 卡密熵要够、去掉易混字符（用户可能要手打）→ 见 CODE_ALPHABET
  2. 兑换必须**一次性 + 事务**（防并发双花）
  3. 权益只在后端算，前端传的只当"意向"
  4. VIP **续期叠加**：还在有效期内则从原到期日往后加
  5. 一切发放都写 `credit_logs`，可追溯
用法（站点 server.py 里）：
    import vip_credits
    vip_credits.ensure_schema(DB_PATH)          # 建表/补列，幂等
    app.include_router(vip_credits.build_router(DB_PATH, get_user_by_token))
"""

import os
import re
import secrets
import sqlite3
import time
from datetime import datetime, timedelta
from typing import Callable, Optional

from fastapi import APIRouter, Header, HTTPException, Request
from pydantic import BaseModel

# ============================ 配置（改价只改这里） ============================
CREDITS_PER_YUAN = 5000          # 1 元 = 5000 积分（与主站一致）
VIP_PLANS = {
    "month":   {"name": "月卡", "name_en": "Monthly",   "price_yuan": 35,  "days": 30,  "bonus": 35000},
    "quarter": {"name": "季卡", "name_en": "Quarterly", "price_yuan": 75,  "days": 90,  "bonus": 75000},
    "year":    {"name": "年卡", "name_en": "Yearly",    "price_yuan": 275, "days": 365, "bonus": 275000},
}
# 积分充值档位（仅用于 /api/config 展示与后台批量发卡，实际金额以订单/卡密为准）
CREDIT_PACKS = [
    {"price_yuan": 5,   "credits": 5 * CREDITS_PER_YUAN},
    {"price_yuan": 10,  "credits": 10 * CREDITS_PER_YUAN},
    {"price_yuan": 30,  "credits": 30 * CREDITS_PER_YUAN},
    {"price_yuan": 50,  "credits": 50 * CREDITS_PER_YUAN},
    {"price_yuan": 100, "credits": 100 * CREDITS_PER_YUAN},
]
PAYMENT_NOTICE = ("购买后你会在订单页/邮件收到一串卡密，回到本站「兑换卡密」粘贴即可即时到账。")

# 卡密字符集：去掉 0 O 1 I L S 等易混字符，用户可能要手打
CODE_ALPHABET = "23456789ABCDEFGHJKMNPQRTUVWXYZ"
CODE_GROUPS = 3           # 3 组 × 4 位 = 12 位随机（约 28^12 ≈ 1.4e17）
CODE_RE = re.compile(r"^[A-Z0-9]{2,6}(-[A-Z0-9]{4}){2,5}$")
_CODE_PREFIX = os.getenv("VIP_CODE_PREFIX", "VC").upper()

# 兑换限速：每用户 10 分钟内最多 10 次（防爆破）
_REDEEM_MAX = 10
_REDEEM_WINDOW = 600
_redeem_hits: dict = {}


def _now() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _conn(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path, timeout=10)
    conn.row_factory = sqlite3.Row
    return conn


def _admin_emails() -> set:
    """管理员：环境变量 ADMIN_EMAILS（逗号分隔），兼容主站旧变量 ADMIN_EMAIL。"""
    raw = os.getenv("ADMIN_EMAILS") or os.getenv("ADMIN_EMAIL") or ""
    return {e.strip().lower() for e in raw.split(",") if e.strip()}


# ==================== 输入上限（防越界/防负数发放） ====================
MAX_CREDITS_PER_CARD = 10_000_000     # 单张卡最多发多少积分
MAX_VIP_DAYS = 3650                   # 单张卡最多给多少天
MAX_CODES_PER_CALL = 500
# 兑换限速表上限：超过就清理过期条目，避免被刷爆内存
_REDEEM_ENTRIES_SOFT_LIMIT = 2000


# ============================ 建表 / 迁移（幂等） ============================
def ensure_schema(db_path: str) -> None:
    conn = _conn(db_path)
    try:
        c = conn.cursor()
        # users 补 VIP 列（老库没有）
        cols = {r[1] for r in c.execute("PRAGMA table_info(users)")}
        if "vip_plan" not in cols:
            c.execute("ALTER TABLE users ADD COLUMN vip_plan TEXT DEFAULT 'none'")
        if "vip_expires_at" not in cols:
            c.execute("ALTER TABLE users ADD COLUMN vip_expires_at TEXT")
        # 充值订单（与主站同结构，便于将来接正规支付时复用）
        c.execute("""
            CREATE TABLE IF NOT EXISTS orders (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                order_id TEXT UNIQUE,
                price_id TEXT,
                amount_cents INTEGER,
                credits INTEGER,
                status TEXT DEFAULT 'pending',
                created_at TEXT NOT NULL,
                paid_at TEXT,
                pay_note TEXT,
                confirmed_at TEXT
            )
        """)
        # 卡密（本模块核心）
        c.execute("""
            CREATE TABLE IF NOT EXISTS redeem_codes (
                code TEXT PRIMARY KEY,
                kind TEXT NOT NULL,
                credits INTEGER DEFAULT 0,
                vip_plan TEXT,
                days INTEGER DEFAULT 0,
                batch TEXT,
                used_by INTEGER,
                used_at TEXT,
                created_at TEXT NOT NULL,
                note TEXT
            )
        """)
        c.execute("CREATE INDEX IF NOT EXISTS idx_redeem_codes_batch ON redeem_codes(batch)")
        # 权益流水（可追溯：谁、何时、发了什么）
        c.execute("""
            CREATE TABLE IF NOT EXISTS credit_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                delta INTEGER NOT NULL DEFAULT 0,
                kind TEXT,
                ref TEXT,
                vip_plan TEXT,
                vip_expires_at TEXT,
                created_at TEXT NOT NULL
            )
        """)
        c.execute("CREATE INDEX IF NOT EXISTS idx_credit_logs_user ON credit_logs(user_id)")
        conn.commit()
    finally:
        conn.close()


# ============================ 权益计算 / 判定 ============================
def _parse_dt(s: Optional[str]) -> Optional[datetime]:
    if not s:
        return None
    try:
        return datetime.fromisoformat(s)
    except Exception:
        return None


def is_vip(db_path: str, user_id: int) -> bool:
    """VIP 是否有效：vip_plan 有值且未过期。异常一律返回 False（安全侧）。"""
    try:
        conn = _conn(db_path)
        try:
            row = conn.execute(
                "SELECT vip_plan, vip_expires_at FROM users WHERE id = ?", (user_id,)
            ).fetchone()
        finally:
            conn.close()
        if not row:
            return False
        plan = (row["vip_plan"] or "none").lower()
        exp = _parse_dt(row["vip_expires_at"])
        return plan != "none" and exp is not None and exp > datetime.now()
    except Exception:
        return False


def vip_state(db_path: str, user_id: int) -> dict:
    conn = _conn(db_path)
    try:
        row = conn.execute(
            "SELECT credits, vip_plan, vip_expires_at FROM users WHERE id = ?", (user_id,)
        ).fetchone()
    finally:
        conn.close()
    if not row:
        return {"credits": 0, "is_vip": False, "vip_plan": "none", "vip_expires_at": None}
    plan = (row["vip_plan"] or "none").lower()
    exp = _parse_dt(row["vip_expires_at"])
    ok = plan != "none" and exp is not None and exp > datetime.now()
    return {
        "credits": row["credits"] or 0,
        "is_vip": ok,
        "vip_plan": plan if ok else "none",
        "vip_expires_at": row["vip_expires_at"] if ok else None,
    }


def grant(conn: sqlite3.Connection, user_id: int, *, credits: int = 0,
          vip_plan: Optional[str] = None, days: int = 0,
          kind: str = "redeem", ref: str = "") -> dict:
    """发放权益（调用方负责事务/commit）。VIP 有效期内续期叠加。

    ⚠️ 防御：credits / days 一律取非负并封顶 —— 负数会变成"扣积分/让 VIP 永不过期"。
    """
    credits = max(0, min(int(credits or 0), MAX_CREDITS_PER_CARD))
    days = max(0, min(int(days or 0), MAX_VIP_DAYS))
    if vip_plan and vip_plan.lower() not in VIP_PLANS:
        raise ValueError("非法 VIP 档位: %r" % vip_plan)
    c = conn.cursor()
    if credits:
        c.execute("UPDATE users SET credits = COALESCE(credits,0) + ? WHERE id = ?", (credits, user_id))
    new_exp = None
    if vip_plan and days:
        row = c.execute("SELECT vip_expires_at FROM users WHERE id = ?", (user_id,)).fetchone()
        cur = _parse_dt(row["vip_expires_at"]) if row else None
        base = cur if (cur and cur > datetime.now()) else datetime.now()
        new_exp = (base + timedelta(days=days)).isoformat(timespec="seconds")
        c.execute("UPDATE users SET vip_plan = ?, vip_expires_at = ? WHERE id = ?",
                  (vip_plan, new_exp, user_id))
    c.execute(
        "INSERT INTO credit_logs (user_id, delta, kind, ref, vip_plan, vip_expires_at, created_at)"
        " VALUES (?,?,?,?,?,?,?)",
        (user_id, credits, kind, ref[:200], vip_plan, new_exp, _now()),
    )
    return {"credits_delta": credits, "vip_plan": vip_plan, "vip_expires_at": new_exp}


# ============================ 卡密生成 ============================
def gen_code() -> str:
    groups = ["".join(secrets.choice(CODE_ALPHABET) for _ in range(4))
              for _ in range(CODE_GROUPS)]
    return _CODE_PREFIX + "-" + "-".join(groups)


def normalize_code(raw: str) -> str:
    """容错：去空格、转大写、去掉中文全角连字符。"""
    s = (raw or "").strip().upper().replace(" ", "").replace("－", "-").replace("—", "-")
    return s


# ============================ 请求体 ============================
class RedeemReq(BaseModel):
    code: str


class GenCodesReq(BaseModel):
    count: int = 10
    kind: str = "vip"                    # vip | credits
    plan: Optional[str] = None           # kind=vip 时必填：month/quarter/year
    days: Optional[int] = None           # 覆盖档位天数（可选）
    credits: Optional[int] = None        # kind=credits: 发放积分；kind=vip: 附赠积分
    batch: Optional[str] = None
    note: Optional[str] = None


class GrantVipReq(BaseModel):
    email: Optional[str] = None
    user_id: Optional[int] = None
    plan: str = "month"
    days: Optional[int] = None


# ============================ 路由 ============================
def build_router(db_path: str,
                 get_user_by_token: Callable[[str], Optional[int]],
                 prefix_token_getter: Optional[Callable[[Request], Optional[int]]] = None) -> APIRouter:
    """挂到站点 FastAPI app 上。get_user_by_token 由站点提供（读自己的 sessions 表）。"""
    r = APIRouter()
    admins = _admin_emails()

    def _uid(authorization: Optional[str]) -> int:
        if not authorization:
            raise HTTPException(401, "请先登录")
        uid = get_user_by_token(authorization.replace("Bearer ", "").strip())
        if not uid:
            raise HTTPException(401, "登录已过期")
        return int(uid)

    def _email_of(uid: int) -> str:
        conn = _conn(db_path)
        try:
            row = conn.execute("SELECT email FROM users WHERE id = ?", (uid,)).fetchone()
        finally:
            conn.close()
        return (row["email"] or "").lower() if row else ""

    def _require_admin(authorization: Optional[str]) -> int:
        uid = _uid(authorization)
        if not admins:
            raise HTTPException(403, "未配置管理员（ADMIN_EMAILS），后台功能关闭")
        if _email_of(uid) not in admins:
            raise HTTPException(403, "无管理员权限")
        return uid

    @r.get("/api/vip/config")
    async def vip_config():
        """公开：价格/档位（前端渲染用，改价不用动前端）。"""
        return {
            "credits_per_yuan": CREDITS_PER_YUAN,
            "credit_packs": CREDIT_PACKS,
            "vip_plans": [
                {"key": k, "days": v["days"],
                 "credits": v["price_yuan"] * CREDITS_PER_YUAN + v["bonus"], **v}
                for k, v in VIP_PLANS.items()
            ],
            "payment_notice": PAYMENT_NOTICE,
            "mode": "redeem_code",     # 当前只上卡密兑换
        }

    @r.get("/api/vip/status")
    async def vip_status(authorization: Optional[str] = Header(None)):
        uid = _uid(authorization)
        st = vip_state(db_path, uid)
        st["is_admin"] = _email_of(uid) in admins if admins else False
        return st

    @r.post("/api/redeem")
    async def redeem(req: RedeemReq, authorization: Optional[str] = Header(None)):
        uid = _uid(authorization)
        # 限速（防爆破）+ 防止字典无限膨胀
        now_ts = time.time()
        if len(_redeem_hits) > _REDEEM_ENTRIES_SOFT_LIMIT:
            for _k in [k for k, v in list(_redeem_hits.items())
                       if not [t for t in v if now_ts - t < _REDEEM_WINDOW]]:
                _redeem_hits.pop(_k, None)
        hits = [t for t in _redeem_hits.get(uid, []) if now_ts - t < _REDEEM_WINDOW]
        if len(hits) >= _REDEEM_MAX:
            raise HTTPException(429, "尝试次数过多，请 10 分钟后再试")
        hits.append(now_ts)
        _redeem_hits[uid] = hits

        code = normalize_code(req.code)
        if not CODE_RE.match(code):
            raise HTTPException(400, "卡密格式不对，请检查后重试")

        conn = _conn(db_path)
        try:
            c = conn.cursor()
            c.execute("BEGIN IMMEDIATE")
            row = c.execute(
                "SELECT code, kind, credits, vip_plan, days, used_at FROM redeem_codes WHERE code = ?",
                (code,),
            ).fetchone()
            if not row:
                conn.rollback()
                raise HTTPException(404, "卡密不存在")
            if row["used_at"]:
                conn.rollback()
                raise HTTPException(409, "这张卡密已被使用")
            # 一次性核销：带 used_at IS NULL 条件，rowcount==1 才算抢到
            c.execute("UPDATE redeem_codes SET used_by = ?, used_at = ? WHERE code = ? AND used_at IS NULL",
                      (uid, _now(), code))
            if c.rowcount != 1:
                conn.rollback()
                raise HTTPException(409, "这张卡密刚被使用，请换一张")
            info = grant(
                conn, uid,
                credits=int(row["credits"] or 0),
                vip_plan=(row["vip_plan"] or None),
                days=int(row["days"] or 0),
                kind="redeem", ref=code,
            )
            conn.commit()
        except HTTPException:
            raise
        except Exception as e:
            conn.rollback()
            raise HTTPException(500, f"兑换失败：{e}")
        finally:
            conn.close()

        st = vip_state(db_path, uid)
        return {"ok": True, "code": code, **st, **info}

    # ---------------- 管理员 ----------------
    @r.post("/api/admin/codes/generate")
    async def admin_gen_codes(req: GenCodesReq, authorization: Optional[str] = Header(None)):
        _require_admin(authorization)
        n = max(1, min(500, int(req.count)))
        kind = (req.kind or "vip").lower()
        if kind not in ("vip", "credits"):
            raise HTTPException(400, "kind 只能是 vip 或 credits")
        plan, days, bonus = None, 0, int(req.credits or 0)
        if kind == "vip":
            plan = (req.plan or "month").lower()
            if plan not in VIP_PLANS:
                raise HTTPException(400, f"plan 必须是 {list(VIP_PLANS)}")
            days = int(req.days or VIP_PLANS[plan]["days"])
            # ⚠️ 不传 credits 时按主站设计发「金额×5000 + bonus」：
            #    月卡 35 元 = 175,000 + 35,000 = 210,000 积分（CREDITS-VIP-DESIGN.md §3）
            if req.credits is None:
                bonus = VIP_PLANS[plan]["price_yuan"] * CREDITS_PER_YUAN + VIP_PLANS[plan]["bonus"]
        else:
            days = 0
            if bonus <= 0:
                raise HTTPException(400, "kind=credits 时必须给 credits（发放积分数）")
        batch = req.batch or datetime.now().strftime("B%Y%m%d%H%M%S")
        conn = _conn(db_path)
        made = []
        try:
            c = conn.cursor()
            for _ in range(n):
                for _try in range(5):
                    code = gen_code()
                    try:
                        c.execute(
                            "INSERT INTO redeem_codes (code, kind, credits, vip_plan, days, batch, created_at, note)"
                            " VALUES (?,?,?,?,?,?,?,?)",
                            (code, kind, bonus, plan, days, batch, _now(), (req.note or "")[:200]),
                        )
                        made.append(code)
                        break
                    except sqlite3.IntegrityError:
                        continue
            conn.commit()
        finally:
            conn.close()
        return {"ok": True, "batch": batch, "count": len(made),
                "kind": kind, "vip_plan": plan, "days": days, "credits": bonus,
                "codes": made}

    @r.get("/api/admin/codes")
    async def admin_list_codes(authorization: Optional[str] = Header(None),
                               batch: Optional[str] = None, limit: int = 200):
        _require_admin(authorization)
        limit = max(1, min(1000, limit))
        conn = _conn(db_path)
        try:
            if batch:
                rows = conn.execute(
                    "SELECT code, kind, credits, vip_plan, days, batch, used_by, used_at, created_at"
                    " FROM redeem_codes WHERE batch = ? ORDER BY created_at DESC LIMIT ?", (batch, limit)
                ).fetchall()
            else:
                rows = conn.execute(
                    "SELECT code, kind, credits, vip_plan, days, batch, used_by, used_at, created_at"
                    " FROM redeem_codes ORDER BY created_at DESC LIMIT ?", (limit,)
                ).fetchall()
            stat = conn.execute(
                "SELECT COUNT(*), SUM(CASE WHEN used_at IS NOT NULL THEN 1 ELSE 0 END) FROM redeem_codes"
            ).fetchone()
        finally:
            conn.close()
        return {"total": stat[0] or 0, "used": stat[1] or 0,
                "codes": [dict(x) for x in rows]}

    @r.post("/api/admin/vip/grant")
    async def admin_grant_vip(req: GrantVipReq, authorization: Optional[str] = Header(None)):
        """手工开通/赠送（运营后门，不走卡密）。"""
        _require_admin(authorization)
        plan = (req.plan or "month").lower()
        if plan not in VIP_PLANS:
            raise HTTPException(400, f"plan 必须是 {list(VIP_PLANS)}")
        days = int(req.days or VIP_PLANS[plan]["days"])
        conn = _conn(db_path)
        try:
            c = conn.cursor()
            if req.user_id:
                target = int(req.user_id)
            elif req.email:
                row = c.execute("SELECT id FROM users WHERE lower(email) = ?",
                                ((req.email or "").strip().lower(),)).fetchone()
                if not row:
                    raise HTTPException(404, "邮箱对应用户不存在")
                target = int(row["id"])
            else:
                raise HTTPException(400, "请给 user_id 或 email")
            c.execute("BEGIN IMMEDIATE")
            info = grant(conn, target, vip_plan=plan, days=days, kind="admin", ref="grant")
            conn.commit()
        finally:
            conn.close()
        return {"ok": True, "user_id": target, **info, **vip_state(db_path, target)}

    return r
