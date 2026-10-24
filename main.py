"""AstrBot 插件：原神（国服）兑换码查询 + 新码主动推送。

功能：
1. `/yscd`          列出当前有效的国服兑换码（首次使用会自动订阅推送）
2. `/yscd sub`      订阅新兑换码推送
3. `/yscd unsub`    取消订阅
4. `/yscd refresh`  立即刷新数据源
5. `/yscd help`     查看帮助

数据源（只取国服）：
1. BetterGI 兑换码仓库 codes.json
   https://cnb.cool/bettergi/genshin-redeem-code/-/git/raw/main/codes.json
   该文件同时收录国服与国际服条目，国际服条目标题带「[国际服]」前缀
   （HourofRuin / ExpertMitya / PhotoTime 这类英文码国服不可用），插件会过滤掉。
2. 米游社官方前瞻直播接口（miyolive），作为前瞻码的补充源：
   getForumPostList 找前瞻帖 → 取 act_id → miyolive/index 取 code_ver →
   refreshCode 取 code_list（含代码与奖励）。

国服兑换码现在多是中文短语（7.2 为「坏灭肇始之时 / 天才少年米提亚登场 /
千星奇域一起拍拍拍」），插件按原样展示，方便直接复制。

作者: DSH
"""

from __future__ import annotations

import asyncio
import json
import os
import re
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
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

USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"
GIFT_URL = "https://ys.mihoyo.com/main/gift"

# BGI 兑换码仓库（国服 + 国际服混排，需要按标题过滤）
BGI_CODES_URL = "https://cnb.cool/bettergi/genshin-redeem-code/-/git/raw/main/codes.json"
# 米游社前瞻直播
LIVE_POST_LIST_URL = "https://bbs-api.miyoushe.com/post/wapi/getForumPostList"
LIVE_INDEX_URL = "https://api-takumi.mihoyo.com/event/miyolive/index"
LIVE_CODE_URL = "https://api-takumi-static.mihoyo.com/event/miyolive/refreshCode"

# 条目 / 标题里出现这些词说明是国际服，直接丢弃
INTL_MARKERS = ("国际服", "[国际]", "国际", "overseas", "global", "os服")

DATE_PATTERN = re.compile(r"(\d{4})\s*[-/.年]\s*(\d{1,2})\s*[-/.月]\s*(\d{1,2})")


# --------------------------------------------------------------------------- #
# 数据结构
# --------------------------------------------------------------------------- #
@dataclass
class RedeemCode:
    """一条兑换码（同一批公告共享组名 / 有效期 / 奖励）。"""

    code: str
    group: str = ""
    expire: date | None = None
    reward: str = ""
    source: str = ""
    first_seen: float = 0.0

    def to_dict(self) -> dict[str, Any]:
        return {
            "code": self.code,
            "group": self.group,
            "expire": self.expire.isoformat() if self.expire else "",
            "reward": self.reward,
            "source": self.source,
            "first_seen": self.first_seen,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "RedeemCode":
        expire = None
        raw = str(data.get("expire") or "")
        if raw:
            try:
                expire = date.fromisoformat(raw)
            except ValueError:
                expire = None
        return cls(
            code=str(data.get("code") or ""),
            group=str(data.get("group") or ""),
            expire=expire,
            reward=str(data.get("reward") or ""),
            source=str(data.get("source") or ""),
            first_seen=float(data.get("first_seen") or 0),
        )

    def is_expired(self, today: date | None = None) -> bool:
        return bool(self.expire) and self.expire < (today or date.today())

    def expire_text(self) -> str:
        if not self.expire:
            return "官方未注明"
        return f"{self.expire.year:04d}-{self.expire.month:02d}-{self.expire.day:02d}"


# --------------------------------------------------------------------------- #
# 插件主体
# --------------------------------------------------------------------------- #
@register(
    PLUGIN_NAME,
    "DSH",
    "原神（国服）兑换码：/yscd 查询并自动订阅新码推送，只取国服数据",
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
        self._client = self._build_client()
        if self._cfg_bool("enable_auto_push", True):
            self._poll_task = asyncio.create_task(
                self._poll_loop(), name="yscd-poller"
            )
            logger.info(f"[{PLUGIN_NAME}] 后台兑换码轮询已启动")

    async def terminate(self) -> None:
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
                self._subscribe(event)
                yield event.plain_result(self._render_list(codes))
                return

            codes = await self._get_codes()
            if not codes:
                yield event.plain_result(
                    "⚠️ 暂时没有获取到有效的国服兑换码（数据源可能波动），请稍后再试。"
                )
                return
            if self._cfg_bool("auto_subscribe", True):
                self._subscribe(event)
            yield event.plain_result(self._render_list(codes))
        except Exception as exc:  # noqa: BLE001
            logger.exception(f"[{PLUGIN_NAME}] 处理 /yscd 失败: {exc}")
            yield event.plain_result("❌ 查询出错了，请稍后再试或联系管理员查看日志。")

    # ------------------------------------------------------------------ #
    # 网络
    # ------------------------------------------------------------------ #
    def _build_client(self) -> httpx.AsyncClient:
        kwargs: dict[str, Any] = {
            "timeout": httpx.Timeout(self._cfg_int("http_timeout", 15)),
            "follow_redirects": True,
            "headers": {"User-Agent": USER_AGENT, "Accept": "application/json, */*"},
        }
        proxy = str(self.config.get("proxy", "") or "").strip()
        if proxy:
            try:  # httpx >= 0.26
                return httpx.AsyncClient(proxy=proxy, **kwargs)
            except TypeError:  # httpx <= 0.25
                return httpx.AsyncClient(proxies=proxy, **kwargs)
        return httpx.AsyncClient(**kwargs)

    async def _get_json(
        self, client: httpx.AsyncClient, url: str, **kwargs: Any
    ) -> Any:
        resp = await client.get(url, **kwargs)
        resp.raise_for_status()
        return resp.json()

    # ------------------------------------------------------------------ #
    # 数据源 1：BGI 兑换码仓库
    # ------------------------------------------------------------------ #
    async def _fetch_bgi_groups(
        self, client: httpx.AsyncClient
    ) -> tuple[list[dict[str, Any]], int]:
        data = await self._get_json(client, BGI_CODES_URL)
        if not isinstance(data, list):
            raise ValueError("BGI 接口返回格式不是数组")

        groups: list[dict[str, Any]] = []
        expired = 0
        today = date.today()
        for item in data:
            if not isinstance(item, dict):
                continue
            title = str(item.get("title") or "").strip()
            codes = [str(c).strip() for c in (item.get("codes") or []) if str(c).strip()]
            if not codes:
                continue
            if self._is_international(title, codes):
                logger.debug(f"[{PLUGIN_NAME}] 跳过国际服条目：{title}")
                continue
            expire = self._parse_date(str(item.get("valid") or ""))
            if expire and expire < today:
                expired += 1
                continue
            groups.append(
                {
                    "title": self._normalize_title(title),
                    "expire": expire,
                    "codes": codes,
                    "rewards": self._split_rewards(str(item.get("content") or "")),
                    "source": "BetterGI（BGI）",
                }
            )
        logger.info(f"[{PLUGIN_NAME}] BGI 国服有效兑换码分组 {len(groups)} 个")
        return groups, expired

    @staticmethod
    def _is_international(title: str, codes: list[str]) -> bool:
        low = title.lower()
        if any(marker in low for marker in INTL_MARKERS):
            return True
        # 兜底：整组全是纯英文码且标题没有国服特征时，按国际服处理
        if codes and all(re.fullmatch(r"[A-Za-z]+", c) for c in codes):
            if not any(k in title for k in ("国服", "中文")):
                return True
        return False

    @staticmethod
    def _normalize_title(title: str) -> str:
        if "前瞻" in title or "直播" in title:
            return "官方前瞻直播兑换码"
        return title or "兑换码"

    @staticmethod
    def _split_rewards(content: str) -> list[str]:
        content = content.strip()
        if not content:
            return []
        parts = re.split(r"\s*[+；;]\s*", content)
        return [p.strip() for p in parts if p.strip()]

    # ------------------------------------------------------------------ #
    # 数据源 2：米游社官方前瞻直播（miyolive）
    # ------------------------------------------------------------------ #
    async def _fetch_live_group(
        self, client: httpx.AsyncClient
    ) -> dict[str, Any] | None:
        act_id = await self._find_live_act_id(client)
        if not act_id:
            logger.debug(f"[{PLUGIN_NAME}] 未找到当前前瞻直播 act_id")
            return None

        headers = {
            "x-rpc-act_id": act_id,
            "Referer": "https://www.miyoushe.com/ys/",
            "User-Agent": USER_AGENT,
        }
        index = await self._get_json(client, LIVE_INDEX_URL, headers=headers)
        if index.get("retcode") != 0:
            logger.debug(f"[{PLUGIN_NAME}] miyolive/index retcode={index.get('retcode')}")
            return None
        live = (index.get("data") or {}).get("live") or {}
        start_text = str(live.get("start") or "").strip()
        start_ms = self._parse_datetime(start_text)
        if not start_ms:
            return None
        if time.time() * 1000 - start_ms > 7 * 24 * 3600 * 1000:
            logger.debug(f"[{PLUGIN_NAME}] 当前前瞻直播已结束超过 7 天")
            return None
        code_ver = str(live.get("code_ver") or "").strip()
        if not code_ver:
            return None

        payload = await self._get_json(
            client,
            LIVE_CODE_URL,
            params={"version": code_ver, "time": int(time.time() * 1000)},
            headers=headers,
        )
        if payload.get("retcode") != 0:
            logger.debug(f"[{PLUGIN_NAME}] refreshCode retcode={payload.get('retcode')}")
            return None

        now = time.time()
        codes: list[str] = []
        rewards: list[str] = []
        for item in (payload.get("data") or {}).get("code_list") or []:
            code = str(item.get("code") or "").strip()
            if not code:
                continue
            try:
                if float(item.get("to_get_time") or 0) > now + 120:
                    continue
            except (TypeError, ValueError):
                pass
            codes.append(code)
            reward = self._strip_html(str(item.get("title") or ""))
            if reward:
                rewards.append(reward)
        if not codes:
            logger.debug(f"[{PLUGIN_NAME}] 官方直播兑换码尚未生成")
            return None

        expire = datetime.fromtimestamp(start_ms / 1000).date() + timedelta(days=3)
        logger.info(f"[{PLUGIN_NAME}] 米游社直播源解析到 {len(codes)} 个前瞻兑换码")
        return {
            "title": "官方前瞻直播兑换码",
            "expire": expire,
            "codes": codes,
            "rewards": rewards,
            "source": "米游社官方前瞻直播",
        }

    async def _find_live_act_id(self, client: httpx.AsyncClient) -> str:
        headers = {"Referer": "https://www.miyoushe.com/ys/", "User-Agent": USER_AGENT}
        seen: set[str] = set()
        posts: list[dict[str, Any]] = []
        for sort_type in (2, 1):
            try:
                payload = await self._get_json(
                    client,
                    LIVE_POST_LIST_URL,
                    params={
                        "forum_id": 28,
                        "is_good": "false",
                        "is_hot": "false",
                        "page_size": 20,
                        "sort_type": sort_type,
                    },
                    headers=headers,
                )
            except Exception as exc:  # noqa: BLE001
                logger.debug(f"[{PLUGIN_NAME}] 前瞻帖列表失败: {exc}")
                continue
            for row in (payload.get("data") or {}).get("list") or []:
                post = row.get("post") or {}
                post_id = str(post.get("post_id") or "")
                if not post_id or post_id in seen:
                    continue
                seen.add(post_id)
                posts.append(post)

        for post in posts:
            subject = str(post.get("subject") or "")
            if not ("前瞻" in subject or "直播" in subject):
                continue
            text = f"{post.get('structured_content') or ''}\n{post.get('content') or ''}"
            match = re.search(r"act_id=([A-Za-z0-9]+)", text)
            if match:
                return match.group(1)
        return ""

    # ------------------------------------------------------------------ #
    # 抓取 / 入库 / 推送
    # ------------------------------------------------------------------ #
    async def _get_codes(self, max_age_seconds: int | None = None) -> list[RedeemCode]:
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

    async def _refresh(self, notify: bool = True) -> list[RedeemCode]:
        async with self._refresh_lock:
            client = self._client or self._build_client()
            self._client = client

            groups: list[dict[str, Any]] = []
            expired_hidden = 0

            if self._cfg_bool("enable_bgi_source", True):
                try:
                    bgi_groups, expired_hidden = await self._fetch_bgi_groups(client)
                    groups.extend(bgi_groups)
                except Exception as exc:  # noqa: BLE001
                    logger.warning(f"[{PLUGIN_NAME}] BGI 兑换码源失败: {exc}")

            if self._cfg_bool("enable_live_source", True):
                try:
                    live = await self._fetch_live_group(client)
                    if live:
                        existing = {c for g in groups for c in g["codes"]}
                        extra = [c for c in live["codes"] if c not in existing]
                        if extra:
                            # 奖励要和留下的码对齐，避免错位/重复
                            rewards = [
                                live["rewards"][i]
                                for i, c in enumerate(live["codes"])
                                if c in extra and i < len(live.get("rewards") or [])
                            ]
                            groups.append({**live, "codes": extra, "rewards": rewards})
                except Exception as exc:  # noqa: BLE001
                    logger.debug(f"[{PLUGIN_NAME}] 米游社直播源失败: {exc}")

            groups.extend(self._parse_manual_groups())

            if not groups and not self._stored_codes():
                logger.warning(f"[{PLUGIN_NAME}] 所有数据源都没有拿到兑换码")
                return []

            self._sync(groups)
            self.state["last_refresh"] = time.time()
            self.state["expired_hidden"] = expired_hidden + self._expired_stored_count()
            if not self.state.get("initialized", False):
                self._mark_all_pushed()  # 首次运行只入库，不刷屏历史码
                self.state["initialized"] = True

            pending = self._pending_codes()
            if notify and pending:
                await self._broadcast(pending)
            else:
                self._save_state()
            return self._stored_codes()

    def _sync(self, groups: list[dict[str, Any]]) -> None:
        """把本轮结果同步进状态：新码入库，消失的码（源里没了）清掉。"""
        store: dict[str, Any] = self.state.setdefault("codes", {})
        now = time.time()
        fresh: dict[str, dict[str, Any]] = {}
        for group in groups:
            codes: list[str] = group.get("codes") or []
            rewards: list[str] = group.get("rewards") or []
            for i, code in enumerate(codes):
                entry = {
                    "code": code,
                    "group": group.get("title") or "",
                    "expire": (
                        group["expire"].isoformat() if group.get("expire") else ""
                    ),
                    "reward": rewards[i] if i < len(rewards) else "",
                    "source": group.get("source") or "",
                }
                fresh[code] = entry

        for code, entry in fresh.items():
            old = store.get(code)
            if old is None:
                store[code] = {**entry, "first_seen": now, "pushed": False}
                continue
            if not entry["expire"] and old.get("expire"):
                entry["expire"] = old["expire"]
            if not entry["reward"] and old.get("reward"):
                entry["reward"] = old["reward"]
            entry["pushed"] = bool(old.get("pushed", False))
            entry["first_seen"] = old.get("first_seen", now)
            store[code] = entry

        # 数据源里已经不再出现的码（下线/换新）不要继续展示
        for code in [c for c in store if c not in fresh]:
            store.pop(code, None)

    # ------------------------------------------------------------------ #
    # 状态读写
    # ------------------------------------------------------------------ #
    def _mark_all_pushed(self) -> None:
        for entry in (self.state.get("codes") or {}).values():
            entry["pushed"] = True

    def _pending_codes(self) -> list[RedeemCode]:
        pending = [
            RedeemCode.from_dict(v)
            for v in (self.state.get("codes") or {}).values()
            if v.get("code") and not v.get("pushed")
        ]
        return [c for c in pending if not c.is_expired()]

    def _stored_codes(self) -> list[RedeemCode]:
        codes = [
            RedeemCode.from_dict(v)
            for v in (self.state.get("codes") or {}).values()
            if v.get("code")
        ]
        return [c for c in codes if not c.is_expired()]

    def _expired_stored_count(self) -> int:
        today = date.today()
        return sum(
            1
            for v in (self.state.get("codes") or {}).values()
            if v.get("expire") and date.fromisoformat(v["expire"]) < today
        )

    # ------------------------------------------------------------------ #
    # 推送
    # ------------------------------------------------------------------ #
    async def _broadcast(self, new_codes: list[RedeemCode]) -> None:
        subs: dict[str, Any] = self.state.get("subs") or {}
        if subs:
            limit = max(1, self._cfg_int("max_push_codes", 6))
            message = self._render_list(new_codes[:limit], header="原神新兑换码")
            dead: list[str] = []
            for key, record in list(subs.items()):
                try:
                    await self.context.send_message(
                        key, MessageChain().message(message)
                    )
                    record["fail"] = 0
                    record["last_push"] = time.time()
                except Exception as exc:  # noqa: BLE001
                    record["fail"] = int(record.get("fail", 0)) + 1
                    logger.warning(
                        f"[{PLUGIN_NAME}] 推送到 {key} 失败({record['fail']}): {exc}"
                    )
                    if record["fail"] >= 3:
                        dead.append(key)
                await asyncio.sleep(1.2)
            for key in dead:
                subs.pop(key, None)
                logger.warning(f"[{PLUGIN_NAME}] 连续推送失败，已移除订阅: {key}")
        else:
            logger.info(f"[{PLUGIN_NAME}] 新增 {len(new_codes)} 个兑换码，但暂无订阅者")

        for c in new_codes:
            entry = (self.state.get("codes") or {}).get(c.code)
            if entry is not None:
                entry["pushed"] = True
                entry["pushed_at"] = time.time()
        self._save_state()

    # ------------------------------------------------------------------ #
    # 文案（严格按需求排版）
    # ------------------------------------------------------------------ #
    def _render_group(self, codes: list[RedeemCode]) -> str:
        first = codes[0]
        lines = [f"【{first.group or '兑换码'}】", f"有效期至：{first.expire_text()}"]
        rewards = list(dict.fromkeys(c.reward for c in codes if c.reward))
        if rewards:
            lines.append("奖励：" + "；".join(rewards))
        if len(codes) == 1:
            lines.append("兑换码：")
            lines.append(first.code)
        else:
            for idx, c in enumerate(codes, start=1):
                lines.append(f"兑换码 {idx}：")
                lines.append(c.code)
        return "\n".join(lines)

    def _render_list(self, codes: list[RedeemCode], header: str = "原神兑换码") -> str:
        if not codes:
            return "⚠️ 暂时没有获取到有效的国服兑换码，请稍后再试。"

        grouped: dict[str, list[RedeemCode]] = {}
        for c in codes:
            grouped.setdefault(c.group or "兑换码", []).append(c)
        groups = list(grouped.values())

        parts = [header]
        if header == "原神兑换码":
            parts.append(f"缓存更新：{self._update_text()}")
        parts.extend(self._render_group(g) for g in groups)

        sources = "、".join(dict.fromkeys(c.source for c in codes if c.source))
        parts.append("")
        parts.append(f"数据来源：{sources or 'BetterGI（BGI）'}")
        parts.append(f"已隐藏 {self._hidden_count()} 条过期兑换码。")
        return "\n".join(parts)

    def _update_text(self) -> str:
        ts = float(self.state.get("last_refresh") or 0)
        when = datetime.fromtimestamp(ts) if ts else datetime.now()
        return when.strftime("%Y-%m-%d %H:%M:%S")

    def _hidden_count(self) -> int:
        return int(self.state.get("expired_hidden") or 0)

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
    # 手工补录 / 工具函数
    # ------------------------------------------------------------------ #
    def _parse_manual_groups(self) -> list[dict[str, Any]]:
        """配置里的补录兑换码，每行：标题|有效期(YYYY-MM-DD)|兑换码|奖励(可选)。"""
        raw = str(self.config.get("manual_codes", "") or "").strip()
        if not raw:
            return []
        groups: dict[str, dict[str, Any]] = {}
        for line in raw.splitlines():
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            parts = [p.strip() for p in line.split("|")]
            if len(parts) < 3:
                continue
            title, expire_raw, code = parts[0], parts[1], parts[2]
            reward = parts[3] if len(parts) > 3 else ""
            group = groups.setdefault(
                title,
                {
                    "title": title,
                    "expire": self._parse_date(expire_raw),
                    "codes": [],
                    "rewards": [],
                    "source": "手动配置",
                },
            )
            group["codes"].append(code)
            if reward:
                group["rewards"].append(reward)
        return list(groups.values())

    @staticmethod
    def _parse_date(text: str) -> date | None:
        match = DATE_PATTERN.search(text or "")
        if not match:
            return None
        try:
            return date(int(match.group(1)), int(match.group(2)), int(match.group(3)))
        except ValueError:
            return None

    @staticmethod
    def _parse_datetime(text: str) -> int:
        """把米游社返回的「2026-10-23 20:00:00」（东八区）转成毫秒时间戳。"""
        match = re.match(
            r"(\d{4})-(\d{2})-(\d{2})[ T](\d{2}):(\d{2})(?::(\d{2}))?", text
        )
        if not match:
            return 0
        try:
            dt = datetime(
                int(match.group(1)),
                int(match.group(2)),
                int(match.group(3)),
                int(match.group(4)),
                int(match.group(5)),
                int(match.group(6) or 0),
                tzinfo=timezone(timedelta(hours=8)),
            )
        except ValueError:
            return 0
        return int(dt.timestamp() * 1000)

    @staticmethod
    def _strip_html(value: str) -> str:
        text = re.sub(r"<br\s*/?>", "\n", value)
        text = re.sub(r"<[^>]+>", "", text)
        return text.replace("&nbsp;", " ").strip()

    # ------------------------------------------------------------------ #
    # 订阅
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

    # ------------------------------------------------------------------ #
    # 轮询 / 配置 / 持久化
    # ------------------------------------------------------------------ #
    async def _poll_loop(self) -> None:
        interval = max(5, self._cfg_int("fetch_interval_minutes", 30)) * 60
        await asyncio.sleep(5)  # 等平台加载完成
        while True:
            try:
                await self._refresh(notify=True)
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001
                logger.exception(f"[{PLUGIN_NAME}] 轮询失败: {exc}")
            await asyncio.sleep(interval)

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
