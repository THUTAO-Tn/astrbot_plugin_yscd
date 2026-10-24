"""AstrBot 插件：原神（国服）兑换码查询 + 主动推送。

功能：
1. `/yscd`            列出当前有效的国服兑换码（首次使用会自动订阅推送）
2. `/yscd sub`        订阅新兑换码推送
3. `/yscd unsub`      取消订阅
4. `/yscd refresh`    立即刷新数据源
5. `/yscd help`       查看帮助

数据源（均只取国服）：
- 米游社官方公告接口（api-takumi.miyoushe.com），解析官方前瞻直播/活动公告中的兑换码及有效期
- 社区聚合接口（api.ennead.cc），BetterGI 等工具常用的兑换码聚合源，用于补全兑换码与奖励信息

作者: DSH
"""

from __future__ import annotations

import asyncio
import json
import os
import re
import time
from dataclasses import dataclass, field
from datetime import date, datetime
from html import unescape
from pathlib import Path
from typing import Any

import httpx

from astrbot.api import AstrBotConfig, logger
from astrbot.api.event import AstrMessageEvent, MessageChain, filter
from astrbot.api.star import Context, Star, register

PLUGIN_NAME = "astrbot_plugin_yscd"

# AstrBot 的 data 目录（插件持久化数据必须放在 data 下，避免更新插件时丢失）
try:  # pragma: no cover - 依赖 AstrBot 运行时
    from astrbot.core.utils.astrbot_path import get_astrbot_data_path

    _DATA_ROOT = Path(get_astrbot_data_path())
except Exception:  # pragma: no cover
    _DATA_ROOT = Path("data")

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
)

ENNEAD_URL = "https://api.ennead.cc/mihoyo/genshin/codes"
MIYOUSHE_NEWS_URL = "https://api-takumi.miyoushe.com/post/wapi/getNewsList"
MIYOUSHE_DETAIL_URL = "https://api-takumi.miyoushe.com/post/wapi/getPostFull"
GIFT_URL = "https://ys.mihoyo.com/main/gift"

# 兑换码本体：大写字母 + 数字，长度 8~14（原神码基本都是这个形态）
CODE_PATTERN = re.compile(r"(?<![A-Za-z0-9])([A-Z0-9]{8,14})(?![A-Za-z0-9])")
# 公告中的有效期，如「有效期至 2026/01/01 12:00」「截止时间：2026年1月1日」
EXPIRE_PATTERN = re.compile(
    r"(?:有效期|截止|失效|过期)[^\d\n]{0,12}(\d{4})\s*[-/.年]\s*(\d{1,2})"
    r"\s*[-/.月]\s*(\d{1,2})"
)
CODE_STOPWORDS = {
    "UID",
    "HTTP",
    "HTTPS",
    "HTML",
    "GENSHIN",
    "MIYOUSHE",
    "MIHOYO",
    "HOYOVERSE",
    "CN",
    "GPS",
    "IOS",
    "ANDROID",
}

# 常见奖励名英文 -> 中文（聚合源返回英文）
REWARD_ZH = {
    "primogem": "原石",
    "primogems": "原石",
    "mora": "摩拉",
    "hero's wit": "大英雄的经验",
    "heros wit": "大英雄的经验",
    "adventurer's experience": "冒险家的经验",
    "adventurers experience": "冒险家的经验",
    "wanderer's advice": "流浪者的经验",
    "mystic enhancement ore": "精锻用魔矿",
    "fine enhancement ore": "精锻用良矿",
    "enhancement ore": "精锻用矿石",
    "fragile resin": "脆弱树脂",
    "acquaint fate": "相遇之缘",
    "intertwined fate": "纠缠之缘",
    "crafting bench": "合成台",
}


# --------------------------------------------------------------------------- #
# 数据结构
# --------------------------------------------------------------------------- #
@dataclass
class GenshinCode:
    """一条兑换码。"""

    code: str
    name: str = ""
    expire: date | None = None
    rewards: list[str] = field(default_factory=list)
    source: str = ""
    first_seen: float = field(default_factory=time.time)

    def to_dict(self) -> dict[str, Any]:
        return {
            "code": self.code,
            "name": self.name,
            "expire": self.expire.isoformat() if self.expire else "",
            "rewards": self.rewards,
            "source": self.source,
            "first_seen": self.first_seen,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "GenshinCode":
        expire = None
        raw = str(data.get("expire") or "")
        if raw:
            try:
                expire = date.fromisoformat(raw)
            except ValueError:
                expire = None
        return cls(
            code=str(data.get("code", "")),
            name=str(data.get("name", "")),
            expire=expire,
            rewards=list(data.get("rewards") or []),
            source=str(data.get("source", "")),
            first_seen=float(data.get("first_seen") or time.time()),
        )

    def is_expired(self, today: date | None = None) -> bool:
        if not self.expire:
            return False
        return self.expire < (today or date.today())

    def expire_text(self) -> str:
        if not self.expire:
            return "未知（以官方公告为准，请尽快兑换）"
        d = self.expire
        return f"{d.year}-{d.month}-{d.day}"


# --------------------------------------------------------------------------- #
# 插件主体
# --------------------------------------------------------------------------- #
@register(
    PLUGIN_NAME,
    "DSH",
    "原神（国服）兑换码查询与新码主动推送：/yscd 查看兑换码并自动订阅推送",
    "1.0.0",
)
class GenshinCodesPlugin(Star):
    def __init__(self, context: Context, config: AstrBotConfig | None = None):
        super().__init__(context)
        self.config = config or {}
        self.data_dir = _DATA_ROOT / "plugin_data" / PLUGIN_NAME
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.state_path = self.data_dir / "state.json"

        self.state: dict[str, Any] = self._load_state()
        self._client: httpx.AsyncClient | None = None
        self._poll_task: asyncio.Task | None = None
        self._refresh_lock = asyncio.Lock()

    # ------------------------------------------------------------------ #
    # 生命周期
    # ------------------------------------------------------------------ #
    async def initialize(self) -> None:
        """插件加载后启动后台轮询任务。"""
        self._client = self._build_client()
        if self._cfg_bool("enable_auto_push", True):
            self._poll_task = asyncio.create_task(
                self._poll_loop(), name="yscd-poller"
            )
            logger.info(f"[{PLUGIN_NAME}] 后台兑换码轮询已启动")

    async def terminate(self) -> None:
        """插件卸载/停用时清理资源。"""
        if self._poll_task and not self._poll_task.done():
            self._poll_task.cancel()
            try:
                await self._poll_task
            except (asyncio.CancelledError, Exception):  # noqa: BLE001
                pass
        self._poll_task = None
        if self._client is not None:
            try:
                await self._client.aclose()
            except Exception:  # noqa: BLE001
                pass
            self._client = None
        self._save_state()

    # ------------------------------------------------------------------ #
    # 指令
    # ------------------------------------------------------------------ #
    @filter.command("yscd")
    async def yscd(self, event: AstrMessageEvent):
        """原神（国服）兑换码查询 / 订阅 / 退订。"""
        arg = self._parse_arg(event)
        try:
            if arg in ("help", "帮助", "-h", "--help"):
                yield event.plain_result(self._help_text())
                return

            if arg in ("sub", "订阅", "关注"):
                self._subscribe(event)
                yield event.plain_result("✅ 已订阅原神兑换码推送，出现新码会自动发到这里。")
                return

            if arg in ("unsub", "退订", "取消订阅"):
                removed = self._unsubscribe(event)
                yield event.plain_result(
                    "✅ 已取消订阅，不再主动推送。" if removed else "ℹ️ 当前会话本来就没有订阅。"
                )
                return

            if arg in ("refresh", "刷新", "-r"):
                codes = await self._refresh(notify=True)
                if not codes:
                    yield event.plain_result("⚠️ 数据源暂时不可用，请稍后再试。")
                    return
                yield event.plain_result(
                    self._format_list(codes, subscribed=self._subscribe(event))
                )
                return

            # 默认：查询兑换码，并（按配置）自动订阅
            codes = await self._get_codes()
            if not codes:
                yield event.plain_result(
                    "⚠️ 暂时没有获取到有效的国服兑换码（数据源可能波动），请稍后再试。"
                )
                return
            subscribed = self._subscribe(event) if self._cfg_bool(
                "auto_subscribe", True
            ) else self._is_subscribed(event)
            yield event.plain_result(self._format_list(codes, subscribed=subscribed))
        except Exception as exc:  # noqa: BLE001
            logger.exception(f"[{PLUGIN_NAME}] 处理 /yscd 失败: {exc}")
            yield event.plain_result("❌ 查询出错了，请稍后再试或联系管理员查看日志。")

    # ------------------------------------------------------------------ #
    # 数据获取
    # ------------------------------------------------------------------ #
    def _build_client(self) -> httpx.AsyncClient:
        headers = {
            "User-Agent": USER_AGENT,
            "Accept": "application/json, text/plain, */*",
        }
        kwargs: dict[str, Any] = {
            "timeout": httpx.Timeout(self._cfg_int("http_timeout", 15)),
            "follow_redirects": True,
            "headers": headers,
        }
        proxy = str(self.config.get("proxy", "") or "").strip()
        if proxy:
            try:  # httpx >= 0.26
                return httpx.AsyncClient(proxy=proxy, **kwargs)
            except TypeError:  # httpx <= 0.25
                return httpx.AsyncClient(proxies=proxy, **kwargs)
        return httpx.AsyncClient(**kwargs)

    async def _get_codes(self, max_age_seconds: int | None = None) -> list[GenshinCode]:
        """取当前有效兑换码；超过 max_age 会先刷新一次。"""
        max_age = (
            max_age_seconds
            if max_age_seconds is not None
            else self._cfg_int("cache_ttl_minutes", 10) * 60
        )
        last = float(self.state.get("last_refresh", 0) or 0)
        if time.time() - last > max_age:
            codes = await self._refresh(notify=False)
            if codes:
                return codes
        return self._stored_codes()

    async def _refresh(self, notify: bool = True) -> list[GenshinCode]:
        """拉取所有数据源，合并入库，并按需推送新码。"""
        async with self._refresh_lock:
            client = self._client or self._build_client()
            self._client = client

            ennead_codes: list[GenshinCode] = []
            miyoushe_codes: list[GenshinCode] = []
            miyoushe_expire: dict[str, date] = {}

            if self._cfg_bool("enable_ennead_source", True):
                try:
                    ennead_codes = await self._fetch_ennead(client)
                except Exception as exc:  # noqa: BLE001
                    logger.warning(f"[{PLUGIN_NAME}] 聚合源拉取失败: {exc}")

            if self._cfg_bool("enable_miyoushe_source", True):
                try:
                    miyoushe_codes, miyoushe_expire = await self._fetch_miyoushe(client)
                except Exception as exc:  # noqa: BLE001
                    logger.warning(f"[{PLUGIN_NAME}] 米游社公告拉取失败: {exc}")

            merged = self._merge(ennead_codes, miyoushe_codes, miyoushe_expire)
            if not merged and not self._stored_codes():
                logger.warning(f"[{PLUGIN_NAME}] 所有数据源均未返回数据")
                return []

            self._upsert(merged)
            self.state["last_refresh"] = time.time()
            first_run = not self.state.get("initialized", False)
            if first_run:
                # 首次运行只入库，把历史兑换码标记为已推送，避免装好就刷屏
                self._mark_all_pushed()
                self.state["initialized"] = True

            pending = self._pending_codes()
            if notify and pending:
                await self._broadcast(pending)
            else:
                self._save_state()
            return self._stored_codes()

    async def _fetch_ennead(self, client: httpx.AsyncClient) -> list[GenshinCode]:
        resp = await client.get(ENNEAD_URL)
        resp.raise_for_status()
        payload = resp.json()
        out: list[GenshinCode] = []
        for item in payload.get("active") or []:
            raw_code = str(item.get("code") or "").strip().upper()
            if not raw_code:
                continue
            rewards = [
                self._reward_to_zh(str(r)) for r in (item.get("rewards") or []) if r
            ]
            out.append(
                GenshinCode(
                    code=raw_code,
                    name=self._make_name(rewards, fallback="兑换码"),
                    rewards=rewards,
                    source="聚合源",
                )
            )
        logger.debug(f"[{PLUGIN_NAME}] 聚合源返回 {len(out)} 个有效兑换码")
        return out

    async def _fetch_miyoushe(
        self, client: httpx.AsyncClient
    ) -> tuple[list[GenshinCode], dict[str, date]]:
        """抓取米游社官方公告，解析兑换码与有效期。"""
        params = {
            "gids": 2,
            "page_size": 20,
            "type": 1,
            "client_type": 4,
            "app_version": "2.70.1",
        }
        resp = await client.get(MIYOUSHE_NEWS_URL, params=params)
        resp.raise_for_status()
        data = resp.json().get("data") or {}
        posts = data.get("list") or []

        keywords = ("兑换码", "前瞻", "直播", "礼包")
        targets: list[dict[str, Any]] = []
        for item in posts:
            post = item.get("post") or {}
            subject = str(post.get("subject") or "")
            if not subject or not any(k in subject for k in keywords):
                continue
            targets.append(post)
            if len(targets) >= 3:
                break

        codes: list[GenshinCode] = []
        expires: dict[str, date] = {}
        for post in targets:
            post_id = post.get("post_id")
            if not post_id:
                continue
            detail = await self._fetch_miyoushe_detail(client, str(post_id))
            if not detail:
                continue
            text, subject = detail
            found = self._parse_codes_from_text(text)
            expire = self._parse_expire(text)
            logger.debug(
                f"[{PLUGIN_NAME}] 公告《{subject[:20]}》解析到 {len(found)} 个码，有效期 {expire}"
            )
            for idx, raw in enumerate(found, start=1):
                if raw in expires:
                    continue
                expires[raw] = expire  # type: ignore[assignment]
                label = self._short_subject(subject)
                if len(found) > 1:
                    label = f"{label} #{idx}"
                codes.append(
                    GenshinCode(
                        code=raw,
                        name=self._make_name([], fallback=label),
                        source="米游社",
                        expire=expire,
                    )
                )
        return codes, expires

    async def _fetch_miyoushe_detail(
        self, client: httpx.AsyncClient, post_id: str
    ) -> tuple[str, str] | None:
        params = {"post_id": post_id, "read": 1}
        resp = await client.get(MIYOUSHE_DETAIL_URL, params=params)
        resp.raise_for_status()
        body = resp.json().get("data") or {}
        post = body.get("post") or {}
        inner = post.get("post") or post  # 兼容不同返回层级
        content = str(
            inner.get("content") or inner.get("structured_content") or ""
        )
        subject = str(inner.get("subject") or "")
        if not content:
            return None
        return self._html_to_text(content), subject

    def _merge(
        self,
        ennead_codes: list[GenshinCode],
        miyoushe_codes: list[GenshinCode],
        miyoushe_expire: dict[str, date],
    ) -> list[GenshinCode]:
        """合并多源结果，优先使用米游社公告里的有效期。"""
        merged: dict[str, GenshinCode] = {}
        for c in ennead_codes:
            merged[c.code] = c
        for c in miyoushe_codes:
            if c.code in merged:
                target = merged[c.code]
                if not target.expire and c.expire:
                    target.expire = c.expire
                if not target.name or target.name == "兑换码":
                    target.name = c.name
                target.source = f"{target.source}+米游社"
            else:
                merged[c.code] = c
        for code, expire in miyoushe_expire.items():
            if expire and code in merged and not merged[code].expire:
                merged[code].expire = expire

        codes = list(merged.values())
        codes.sort(
            key=lambda c: (c.expire is None, c.expire or date.max, c.code)
        )
        return codes

    def _upsert(self, codes: list[GenshinCode]) -> list[GenshinCode]:
        """把最新结果写入 state，返回本次新出现的兑换码。"""
        store: dict[str, Any] = self.state.setdefault("codes", {})
        now = time.time()
        new_codes: list[GenshinCode] = []
        for c in codes:
            old = store.get(c.code)
            if old is None:
                store[c.code] = {**c.to_dict(), "first_seen": now, "pushed": False}
                new_codes.append(c)
                continue
            # 保留首次发现时间、推送状态；数据源临时失败时不要把已知信息清空
            data = c.to_dict()
            c.first_seen = float(old.get("first_seen") or now)
            data["first_seen"] = c.first_seen
            if not data.get("expire") and old.get("expire"):
                data["expire"] = old["expire"]
                c.expire = GenshinCode.from_dict(old).expire
            if not data.get("name") and old.get("name"):
                data["name"] = old["name"]
            data["pushed"] = bool(old.get("pushed", False))
            old.update(data)
        return new_codes

    def _mark_all_pushed(self) -> None:
        """把当前所有兑换码标记为已推送（用于首次初始化）。"""
        for entry in (self.state.get("codes") or {}).values():
            entry["pushed"] = True

    def _pending_codes(self) -> list[GenshinCode]:
        """入库了但还没推送过的新兑换码（过期的直接跳过）。"""
        store: dict[str, Any] = self.state.get("codes") or {}
        pending = [
            GenshinCode.from_dict(v)
            for v in store.values()
            if v.get("code") and not v.get("pushed")
        ]
        pending = [c for c in pending if not c.is_expired()]
        pending.sort(key=lambda c: c.expire or date.max)
        return pending

    def _stored_codes(self) -> list[GenshinCode]:
        store: dict[str, Any] = self.state.get("codes") or {}
        codes = [GenshinCode.from_dict(v) for v in store.values() if v.get("code")]
        codes = [c for c in codes if not c.is_expired()]
        codes.sort(
            key=lambda c: (
                c.expire is None,
                c.expire or date.max,
                c.first_seen,
                c.code,
            )
        )
        return codes

    # ------------------------------------------------------------------ #
    # 解析工具
    # ------------------------------------------------------------------ #
    @staticmethod
    def _html_to_text(html: str) -> str:
        text = re.sub(r"<br\s*/?>", "\n", html, flags=re.I)
        text = re.sub(r"</(p|div|li|h\d)>", "\n", text, flags=re.I)
        text = re.sub(r"<[^>]+>", "", text)
        text = unescape(text)
        text = text.replace("\u00a0", " ")
        text = re.sub(r"[ \t]+", " ", text)
        text = re.sub(r"\n{2,}", "\n", text)
        return text.strip()

    @staticmethod
    def _parse_codes_from_text(text: str) -> list[str]:
        """只解析「兑换码」关键词附近的候选码，降低误判。"""
        candidates: list[str] = []
        segments: list[str] = []
        for match in re.finditer(r"兑换码|礼包码|CDK", text):
            segments.append(text[match.end() : match.end() + 400])
        if not segments:
            return []
        for seg in segments:
            for token in CODE_PATTERN.findall(seg):
                if token in CODE_STOPWORDS:
                    continue
                digits = sum(ch.isdigit() for ch in token)
                letters = sum(ch.isalpha() for ch in token)
                if digits < 1 or letters < 2:
                    continue
                if token not in candidates:
                    candidates.append(token)
            if len(candidates) >= 10:
                break
        return candidates

    @staticmethod
    def _parse_expire(text: str) -> date | None:
        match = EXPIRE_PATTERN.search(text)
        if not match:
            return None
        try:
            return date(int(match.group(1)), int(match.group(2)), int(match.group(3)))
        except ValueError:
            return None

    @staticmethod
    def _reward_to_zh(raw: str) -> str:
        text = raw.strip()
        # 规范化数量分隔符：只处理「×」和单独出现的 x（如 Hero's Wit x5），
        # 避免把 Experience 里的字母 x 当成乘号
        text = re.sub(r"\s*×\s*", "×", text)
        text = re.sub(r"\s+x\s*(?=\d)", "×", text)
        text = re.sub(r"(?<=\d),(?=\d{3})", "", text)  # 10,000 -> 10000
        lower = text.lower()
        for en, zh in REWARD_ZH.items():
            if en in lower:
                amount = text.split("×")[-1].strip() if "×" in text else ""
                return f"{zh}×{amount}" if amount else zh
        return text

    @staticmethod
    def _make_name(rewards: list[str], fallback: str = "兑换码") -> str:
        cleaned = [r for r in rewards if r]
        if not cleaned:
            return fallback
        return "、".join(cleaned[:2])

    @staticmethod
    def _short_subject(subject: str) -> str:
        subject = re.sub(r"[《》「」【】]", "", subject).strip()
        return subject[:16] or "官方公告兑换码"

    # ------------------------------------------------------------------ #
    # 订阅与推送
    # ------------------------------------------------------------------ #
    def _session_key(self, event: AstrMessageEvent) -> str:
        return getattr(event, "unified_msg_origin", "") or event.get_session_id()

    def _is_group(self, event: AstrMessageEvent) -> bool:
        try:
            group_id = event.get_group_id()
        except Exception:  # noqa: BLE001
            group_id = None
        if group_id:
            return True
        try:
            return bool(event.message_obj.group_id)
        except Exception:  # noqa: BLE001
            return False

    def _is_subscribed(self, event: AstrMessageEvent) -> bool:
        return self._session_key(event) in (self.state.get("subs") or {})

    def _subscribe(self, event: AstrMessageEvent) -> bool:
        key = self._session_key(event)
        if not key:
            return False
        subs: dict[str, Any] = self.state.setdefault("subs", {})
        record = subs.get(key) or {}
        record.update(
            {
                "ts": time.time(),
                "group": self._is_group(event),
                "platform": event.get_platform_name(),
                "fail": int(record.get("fail", 0)),
            }
        )
        subs[key] = record
        self._save_state()
        return True

    def _unsubscribe(self, event: AstrMessageEvent) -> bool:
        key = self._session_key(event)
        subs: dict[str, Any] = self.state.setdefault("subs", {})
        if key in subs:
            subs.pop(key, None)
            self._save_state()
            return True
        return False

    async def _poll_loop(self) -> None:
        """后台轮询：定期刷新，新码出现时主动推送。"""
        interval = max(5, self._cfg_int("fetch_interval_minutes", 30)) * 60
        await asyncio.sleep(5)  # 等 AstrBot 平台加载完成
        while True:
            try:
                await self._refresh(notify=True)
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001
                logger.exception(f"[{PLUGIN_NAME}] 轮询失败: {exc}")
            await asyncio.sleep(interval)

    async def _broadcast(self, new_codes: list[GenshinCode]) -> None:
        subs: dict[str, Any] = self.state.get("subs") or {}
        if not subs:
            logger.info(f"[{PLUGIN_NAME}] 发现 {len(new_codes)} 个新兑换码，但暂无订阅者")
            return

        limit = max(1, self._cfg_int("max_push_codes", 5))
        show = new_codes[:limit]
        blocks = [self._format_block(c) for c in show]
        header = f"🎁 原神新兑换码（{len(new_codes)} 个）\n"
        footer = "\n\n发送 /yscd 查看全部兑换码 · 发送 /yscd unsub 退订"
        extra = "" if len(new_codes) <= limit else "\n（仅推送前几个，其余请发送 /yscd 查看）"
        message = header + "\n" + "\n\n".join(blocks) + extra + footer

        dead: list[str] = []
        for key, record in list(subs.items()):
            try:
                await self.context.send_message(key, MessageChain().message(message))
                record["fail"] = 0
                record["last_push"] = time.time()
            except Exception as exc:  # noqa: BLE001
                record["fail"] = int(record.get("fail", 0)) + 1
                logger.warning(
                    f"[{PLUGIN_NAME}] 推送到 {key} 失败({record['fail']}): {exc}"
                )
                if record["fail"] >= 3:
                    dead.append(key)
            await asyncio.sleep(1.2)  # 降低触发平台频率限制的概率

        for key in dead:
            subs.pop(key, None)
            logger.warning(f"[{PLUGIN_NAME}] 连续推送失败，已移除订阅: {key}")

        for c in new_codes:
            entry = (self.state.get("codes") or {}).get(c.code)
            if entry is not None:
                entry["pushed"] = True
                entry["pushed_at"] = time.time()
        self._save_state()

    # ------------------------------------------------------------------ #
    # 文案
    # ------------------------------------------------------------------ #
    def _format_block(self, code: GenshinCode) -> str:
        """【兑换码名称】 / 过期日期 / 兑换码本体"""
        name = code.name or "兑换码"
        body = code.code
        if self._cfg_bool("use_code_block", False):
            body = f"\n```\n{body}\n```"
        return f"【{name}】\n过期日期：{code.expire_text()}\n兑换码：{body}"

    def _format_list(self, codes: list[GenshinCode], subscribed: bool) -> str:
        if not codes:
            return "⚠️ 暂时没有获取到有效的国服兑换码，请稍后再试。"
        limit = max(1, self._cfg_int("max_codes_display", 12))
        shown = codes[:limit]
        updated = datetime.fromtimestamp(
            float(self.state.get("last_refresh") or time.time())
        ).strftime("%Y-%m-%d %H:%M")
        head = f"🎁 原神国服兑换码 · 共 {len(codes)} 个有效\n🕒 更新于 {updated}\n"
        body = "\n\n".join(self._format_block(c) for c in shown)
        foot = (
            f"\n\n💡 复制「兑换码：」后面的内容，到 {GIFT_URL} 或游戏内兑换"
            f"\n🔔 {'已订阅新兑换码推送，' if subscribed else '发送 /yscd sub 可订阅推送，'}"
            "发送 /yscd unsub 退订"
        )
        if len(codes) > limit:
            foot += f"\n（仅展示前 {limit} 个，共 {len(codes)} 个有效）"
        return head + "\n" + body + foot

    def _help_text(self) -> str:
        return (
            "🎫 原神（国服）兑换码\n"
            "/yscd            查看当前有效兑换码（并自动订阅推送）\n"
            "/yscd sub        订阅新兑换码推送\n"
            "/yscd unsub      取消订阅\n"
            "/yscd refresh    立即刷新数据源\n"
            "/yscd help       查看本帮助\n"
            f"\n兑换地址：{GIFT_URL}"
        )

    @staticmethod
    def _parse_arg(event: AstrMessageEvent) -> str:
        text = (getattr(event, "message_str", "") or "").strip()
        parts = text.split(None, 1)
        return parts[1].strip().lower() if len(parts) > 1 else ""

    # ------------------------------------------------------------------ #
    # 配置 / 持久化
    # ------------------------------------------------------------------ #
    def _cfg_bool(self, key: str, default: bool) -> bool:
        value = self.config.get(key, default)
        if isinstance(value, str):
            return value.strip().lower() in ("1", "true", "yes", "on")
        return bool(value)

    def _cfg_int(self, key: str, default: int) -> int:
        try:
            return int(self.config.get(key, default))
        except (TypeError, ValueError):
            return default

    def _load_state(self) -> dict[str, Any]:
        if self.state_path.exists():
            try:
                with self.state_path.open("r", encoding="utf-8") as f:
                    data = json.load(f)
                if isinstance(data, dict):
                    data.setdefault("codes", {})
                    data.setdefault("subs", {})
                    return data
            except Exception as exc:  # noqa: BLE001
                logger.warning(f"[{PLUGIN_NAME}] 读取状态文件失败: {exc}")
        return {"codes": {}, "subs": {}, "last_refresh": 0, "initialized": False}

    def _save_state(self) -> None:
        try:
            tmp = self.state_path.with_suffix(".json.tmp")
            with tmp.open("w", encoding="utf-8") as f:
                json.dump(self.state, f, ensure_ascii=False, indent=2)
            os.replace(tmp, self.state_path)
        except Exception as exc:  # noqa: BLE001
            logger.warning(f"[{PLUGIN_NAME}] 保存状态文件失败: {exc}")
