"""AstrBot 插件：原神（国服）兑换码查询 + 新码主动推送。

功能：
1. `/yscd`          列出当前有效的国服兑换码（首次使用会自动订阅推送）
2. `/yscd sub`      订阅新兑换码推送
3. `/yscd unsub`    取消订阅
4. `/yscd refresh`  立即刷新数据源
5. `/yscd help`     查看帮助

数据源（只取国服）：
- 米游社国服官方公告 / 前瞻直播资讯（api-takumi.miyoushe.com）。
  国服兑换码现在很多是中文短语（如「千星奇域一起拍拍拍」），国际服的英文码
  （hourofruin / expertmitya / phototime 这类）国服不可用，插件不会采用。
- 管理员可在配置里补录兑换码（公告没写或解析不到时兜底）。

作者: DSH
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
import random
import re
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta
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

MIYOUSHE_NEWS_URL = "https://api-takumi.miyoushe.com/post/wapi/getNewsList"
MIYOUSHE_DETAIL_URL = "https://api-takumi.miyoushe.com/post/wapi/getPostFull"
# 米游社 web 端 DS 签名盐（公开固定值）
MIYOUSHE_SALT = "xV8v4Qu54lUKrEYFZkJhB8cuOh9Asafs"
GIFT_URL = "https://ys.mihoyo.com/main/gift"

# 公告标题里出现这些词时优先抓详情
POST_KEYWORDS = ("兑换码", "前瞻", "直播", "礼包", "福利", "庆典", "活动")
# 前瞻直播的三条兑换码奖励是多年不变的固定组合
FORESHADOW_REWARDS = [
    "原石*100 精锻用魔矿*10",
    "原石*100 大英雄的经验*5",
    "原石*100 摩拉*50000",
]

EXPIRE_PATTERN = re.compile(
    r"(?:有效期|截止|失效|过期)[^0-9\n]{0,10}(\d{4})\s*[-/.年]\s*(\d{1,2})"
    r"\s*[-/.月]\s*(\d{1,2})"
)
CODE_CHARS = re.compile(r"^[A-Za-z0-9\u4e00-\u9fff·\-]+$")
CODE_LABEL = re.compile(r"兑换码|礼包码|CDK", re.I)
BAD_PREFIX = (
    "有效期",
    "奖励",
    "兑换",
    "点击",
    "前往",
    "领取",
    "活动",
    "复制",
    "本次",
    "版本",
    "原神",
    "米游社",
    "旅行者",
    "亲爱的",
)
# 候选码里出现这些词多半是正文句子，不是兑换码
BAD_CONTAINS = (
    "兑换",
    "领取",
    "如下",
    "点击",
    "前往",
    "有效期",
    "奖励",
    "活动",
    "复制",
    "查看",
    "版本",
    "旅行者",
)


# --------------------------------------------------------------------------- #
# 数据结构
# --------------------------------------------------------------------------- #
@dataclass
class RedeemCode:
    """一条兑换码（同一批公告共享组名 / 有效期）。"""

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
            return "未知"
        return f"{self.expire.year:04d}-{self.expire.month:02d}-{self.expire.day:02d}"


# --------------------------------------------------------------------------- #
# 插件主体
# --------------------------------------------------------------------------- #
@register(
    PLUGIN_NAME,
    "DSH",
    "原神（国服）兑换码：/yscd 查询并自动订阅新码推送，只取米游社国服公告",
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
    # 网络与数据源
    # ------------------------------------------------------------------ #
    def _build_client(self) -> httpx.AsyncClient:
        kwargs: dict[str, Any] = {
            "timeout": httpx.Timeout(self._cfg_int("http_timeout", 15)),
            "follow_redirects": True,
            "headers": {
                "User-Agent": USER_AGENT,
                "Accept": "application/json, text/plain, */*",
            },
        }
        proxy = str(self.config.get("proxy", "") or "").strip()
        if proxy:
            try:  # httpx >= 0.26
                return httpx.AsyncClient(proxy=proxy, **kwargs)
            except TypeError:  # httpx <= 0.25
                return httpx.AsyncClient(proxies=proxy, **kwargs)
        return httpx.AsyncClient(**kwargs)

    @staticmethod
    def _ds_headers(query: str) -> dict[str, str]:
        """米游社 web 端 DS 签名；部分接口不带 DS 会返回 403。"""
        t = str(int(time.time()))
        r = str(random.randint(100000, 200000))
        raw = f"salt={MIYOUSHE_SALT}&t={t}&r={r}&b=&q={query}"
        ds = hashlib.md5(raw.encode("utf-8")).hexdigest()
        return {
            "DS": f"{t},{r},{ds}",
            "x-rpc-app_version": "2.71.1",
            "x-rpc-client_type": "5",
            "x-rpc-language": "zh-cn",
            "Referer": "https://app.mihoyo.com",
            "Origin": "https://app.mihoyo.com",
        }

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
        """拉取数据源 → 入库 → 按需推送新码。"""
        async with self._refresh_lock:
            client = self._client or self._build_client()
            self._client = client

            groups: list[dict[str, Any]] = []
            if self._cfg_bool("enable_miyoushe_source", True):
                try:
                    groups = await self._fetch_miyoushe_groups(client)
                except Exception as exc:  # noqa: BLE001
                    logger.warning(f"[{PLUGIN_NAME}] 米游社公告拉取失败: {exc}")
            groups.extend(self._parse_manual_groups())

            if not groups and not self._stored_codes():
                logger.warning(f"[{PLUGIN_NAME}] 所有数据源都没有拿到兑换码")
                return []

            self._upsert(groups)
            self.state["last_refresh"] = time.time()
            if not self.state.get("initialized", False):
                self._mark_all_pushed()  # 首次运行只入库，不刷屏历史码
                self.state["initialized"] = True

            pending = self._pending_codes()
            if notify and pending:
                await self._broadcast(pending)
            else:
                self._save_state()
            return self._stored_codes()

    async def _fetch_miyoushe_groups(
        self, client: httpx.AsyncClient
    ) -> list[dict[str, Any]]:
        matched: list[dict[str, Any]] = []
        others: list[dict[str, Any]] = []
        seen_ids: set[str] = set()
        for news_type in (1, 2, 3):  # 1=活动公告 2=游戏公告 3=资讯
            try:
                resp = await client.get(
                    MIYOUSHE_NEWS_URL,
                    params={"gids": 2, "page_size": 20, "type": news_type},
                )
                resp.raise_for_status()
                payload = resp.json()
            except Exception as exc:  # noqa: BLE001
                logger.debug(f"[{PLUGIN_NAME}] 米游社列表 type={news_type} 失败: {exc}")
                continue
            for item in (payload.get("data") or {}).get("list") or []:
                post = item.get("post") or {}
                post_id = str(post.get("post_id") or "")
                subject = str(post.get("subject") or "")
                if not post_id or not subject or post_id in seen_ids:
                    continue
                seen_ids.add(post_id)
                info = {"id": post_id, "subject": subject}
                if any(k in subject for k in POST_KEYWORDS):
                    matched.append(info)
                else:
                    others.append(info)

        # 有关键词的优先；再用最近公告补位，避免标题没写「兑换码」的活动被漏掉
        scan_limit = max(1, self._cfg_int("scan_posts_per_round", 6))
        posts = (matched + others)[:scan_limit]
        groups: list[dict[str, Any]] = []
        for post in posts:
            try:
                text = await self._fetch_post_detail(client, post["id"])
            except Exception as exc:  # noqa: BLE001
                logger.debug(f"[{PLUGIN_NAME}] 公告 {post['id']} 详情失败: {exc}")
                continue
            if not text:
                continue
            codes = self._extract_codes(text)
            if not codes:
                logger.debug(
                    f"[{PLUGIN_NAME}] 公告《{post['subject'][:16]}》未解析到兑换码"
                )
                continue
            expire = self._extract_expire(text)
            title = self._group_title(post["subject"])
            rewards = self._extract_rewards(text, len(codes))
            if (
                not rewards
                and title == "官方前瞻直播兑换码"
                and len(codes) == len(FORESHADOW_REWARDS)
                and self._cfg_bool("foreshadow_reward_template", True)
            ):
                rewards = list(FORESHADOW_REWARDS)
            groups.append(
                {
                    "title": title,
                    "expire": expire,
                    "codes": codes,
                    "rewards": rewards,
                    "source": "米游社官方公告",
                }
            )
            logger.info(
                f"[{PLUGIN_NAME}] 公告《{title}》解析到 {len(codes)} 个兑换码"
            )
        return groups

    async def _fetch_post_detail(
        self, client: httpx.AsyncClient, post_id: str
    ) -> str | None:
        params = {"post_id": post_id, "read": 1}
        query = "&".join(f"{k}={params[k]}" for k in sorted(params))
        # 先直接请求；被风控（403）时再带 DS 签名重试
        resp = await client.get(MIYOUSHE_DETAIL_URL, params=params)
        if resp.status_code in (401, 403):
            resp = await client.get(
                MIYOUSHE_DETAIL_URL, params=params, headers=self._ds_headers(query)
            )
        if resp.status_code in (401, 403):
            logger.debug(
                f"[{PLUGIN_NAME}] 公告 {post_id} 被风控拒绝（401/403），"
                "如持续出现请检查网络环境或改用代理"
            )
            return None
        resp.raise_for_status()
        body = resp.json().get("data") or {}
        post = body.get("post") or {}
        inner = post.get("post") or post
        content = str(inner.get("content") or inner.get("structured_content") or "")
        if not content:
            return None
        return self._html_to_text(content)

    # ------------------------------------------------------------------ #
    # 文本解析
    # ------------------------------------------------------------------ #
    @staticmethod
    def _html_to_text(html: str) -> str:
        text = re.sub(r"<br\s*/?>", "\n", html, flags=re.I)
        text = re.sub(r"</(p|div|li|h\d|tr)>", "\n", text, flags=re.I)
        text = re.sub(r"<[^>]+>", "", text)
        text = unescape(text)
        text = text.replace("\u00a0", " ").replace("\u3000", " ")
        text = re.sub(r"[ \t]+", " ", text)
        text = re.sub(r"\n{2,}", "\n", text)
        return text.strip()

    @staticmethod
    def _looks_like_code(token: str) -> bool:
        token = token.strip().strip("：:·.。")
        if not token or len(token) < 2 or len(token) > 24:
            return False
        if token.startswith(BAD_PREFIX) or token.startswith("※"):
            return False
        if any(word in token for word in BAD_CONTAINS):
            return False
        if token.isdigit():
            return False
        if not CODE_CHARS.fullmatch(token):
            return False
        # 纯英文数字的旧式兑换码至少要 6 位且含字母，避免误判
        if re.fullmatch(r"[A-Za-z0-9\-]+", token):
            return bool(re.search(r"[A-Za-z]", token)) and len(token) >= 6
        return True

    def _extract_codes(self, text: str) -> list[str]:
        """从公告正文提取兑换码：支持中文短语码（7.x 国服）与英文码（旧版）。"""
        found: list[str] = []

        def add(token: str) -> None:
            token = token.strip().strip("：:·.。，,；;")
            if token and token not in found and self._looks_like_code(token):
                found.append(token)

        lines = [ln.strip() for ln in text.split("\n")]
        for idx, line in enumerate(lines):
            if not CODE_LABEL.search(line):
                continue
            # 行内写法：兑换码1：XXXX / 兑换码：XXXX、YYYY
            rest = CODE_LABEL.sub("", line)
            rest = re.sub(r"^\s*[0-9一二三四五六七八九十]+\s*[：:、.]?", "", rest)
            rest = rest.lstrip("：:，,、 ")
            for part in re.split(r"[、，,；;/\s]+", rest):
                add(part)

            # 后续若干行是纯兑换码
            blank_run = 0
            for nxt in lines[idx + 1 : idx + 7]:
                if not nxt:
                    blank_run += 1
                    if blank_run >= 2:
                        break
                    continue
                if CODE_LABEL.search(nxt) or re.search(r"有效期|奖励|截止|失效|※|【", nxt):
                    break
                if self._looks_like_code(nxt):
                    add(nxt)
                    blank_run = 0
                else:
                    break
            if len(found) >= 8:
                break
        return found[:8]

    @staticmethod
    def _extract_expire(text: str) -> date | None:
        match = EXPIRE_PATTERN.search(text)
        if not match:
            return None
        try:
            return date(int(match.group(1)), int(match.group(2)), int(match.group(3)))
        except ValueError:
            return None

    @staticmethod
    def _extract_rewards(text: str, count: int) -> list[str]:
        rewards: list[str] = []
        for line in text.split("\n"):
            line = line.strip()
            if not line.startswith(("奖励", "活动奖励", "礼包内容")):
                continue
            body = re.sub(r"^(奖励|活动奖励|礼包内容)\s*[：:]?", "", line)
            for part in re.split(r"[；;、]", body):
                part = part.strip()
                if part and part not in rewards:
                    rewards.append(part)
            if rewards:
                break
        return rewards[: max(count, 1)]

    @staticmethod
    def _group_title(subject: str) -> str:
        if "前瞻" in subject or "直播" in subject:
            return "官方前瞻直播兑换码"
        # 优先取书名号/引号里的活动名，如「旧友的祝福」网页活动 → 旧友的祝福
        match = re.search(r"[「《【]([^」》】]{2,20})[」》】]", subject)
        if match:
            return match.group(1)
        title = re.sub(r"[《》「」【】]", "", subject).strip()
        title = re.sub(r"^\d+(\.\d+)?版本", "", title)
        return (title or "官方公告兑换码")[:18]

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
            expire = None
            try:
                expire = date.fromisoformat(expire_raw)
            except ValueError:
                expire = None
            group = groups.setdefault(
                title,
                {
                    "title": title,
                    "expire": expire,
                    "codes": [],
                    "rewards": [],
                    "source": "手动配置",
                },
            )
            group["codes"].append(code)
            if reward:
                group["rewards"].append(reward)
        return list(groups.values())

    # ------------------------------------------------------------------ #
    # 入库 / 查询 / 推送
    # ------------------------------------------------------------------ #
    def _upsert(self, groups: list[dict[str, Any]]) -> None:
        store: dict[str, Any] = self.state.setdefault("codes", {})
        now = time.time()
        for group in groups:
            codes: list[str] = group.get("codes") or []
            rewards: list[str] = group.get("rewards") or []
            for i, code in enumerate(codes):
                reward = rewards[i] if i < len(rewards) else ""
                entry = {
                    "code": code,
                    "group": group.get("title") or "",
                    "expire": (
                        group["expire"].isoformat() if group.get("expire") else ""
                    ),
                    "reward": reward,
                    "source": group.get("source") or "",
                }
                old = store.get(code)
                if old is None:
                    store[code] = {**entry, "first_seen": now, "pushed": False}
                    continue
                if not entry["expire"] and old.get("expire"):
                    entry["expire"] = old["expire"]
                if not entry["reward"] and old.get("reward"):
                    entry["reward"] = old["reward"]
                old.update(entry)
                old.setdefault("first_seen", now)
        self._prune()

    def _prune(self) -> None:
        """清掉过期超过 30 天的记录，避免状态文件无限增长。"""
        store: dict[str, Any] = self.state.get("codes") or {}
        cutoff = date.today() - timedelta(days=30)
        for key in [
            k
            for k, v in store.items()
            if v.get("expire") and date.fromisoformat(v["expire"]) < cutoff
        ]:
            store.pop(key, None)

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

    async def _broadcast(self, new_codes: list[RedeemCode]) -> None:
        subs: dict[str, Any] = self.state.get("subs") or {}
        if subs:
            limit = max(1, self._cfg_int("max_push_codes", 6))
            message = self._render_list(new_codes[:limit], header="原神兑换码（新）")
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
        rewards = [c.reward for c in codes if c.reward]
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

    def _render_list(
        self, codes: list[RedeemCode], header: str = "原神兑换码"
    ) -> str:
        if not codes:
            return "⚠️ 暂时没有获取到有效的国服兑换码，请稍后再试。"

        # 按公告分组，按有效期排序
        grouped: dict[str, list[RedeemCode]] = {}
        for c in codes:
            grouped.setdefault(c.group or "兑换码", []).append(c)
        groups = list(grouped.values())
        groups.sort(key=lambda g: g[0].expire or date.max)

        updated = datetime.fromtimestamp(
            float(self.state.get("last_refresh") or time.time())
        ).strftime("%Y-%m-%d %H:%M:%S")
        parts = [header]
        if header == "原神兑换码":
            parts.append(f"缓存更新：{updated}")
        parts.extend(self._render_group(g) for g in groups)

        sources = (
            "、".join(dict.fromkeys(c.source for c in codes if c.source))
            or "米游社官方公告"
        )
        parts.append("")
        parts.append(f"数据来源：{sources}")
        parts.append(f"已隐藏 {self._expired_count()} 条过期兑换码。")
        return "\n".join(parts)

    def _expired_count(self) -> int:
        today = date.today()
        return sum(
            1
            for v in (self.state.get("codes") or {}).values()
            if v.get("expire") and date.fromisoformat(v["expire"]) < today
        )

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
