# 原神兑换码（astrbot_plugin_yscd）

AstrBot 插件：查询**原神国服**兑换码，出现新兑换码时主动推送给用过 `/yscd` 的会话。

> 国服兑换码现在很多是**中文短语**（7.2 是「坏灭肇始之时 / 天才少年米提亚登场 / 千星奇域一起拍拍拍」）。
> 国际服的英文码（`HourofRuin` / `ExpertMitya` / `PhotoTime`）**国服不可用**，插件会过滤掉。

- 指令：`/yscd`
- 数据源：BetterGI 兑换码仓库（国服条目）+ 米游社官方前瞻直播接口
- 推送：后台定时轮询，发现新码自动发到订阅过的私聊/群聊，**纯文本、不 @ 任何人**

---

## 一、输出格式

```
原神兑换码
缓存更新：2026-10-24 15:11:42
【旧友的祝福】
有效期至：2026-10-31
兑换码：
最好的伙伴
【官方前瞻直播兑换码】
有效期至：2026-10-26
奖励：原石*100 精锻用魔矿*10；原石*100 大英雄的经验*5；原石*100 摩拉*50000
兑换码 1：
坏灭肇始之时
兑换码 2：
天才少年米提亚登场
兑换码 3：
千星奇域一起拍拍拍

数据来源：BetterGI（BGI）、米游社官方前瞻直播
已隐藏 2 条过期兑换码。
```

- 一组一块：`【组名】` → `有效期至：` → （有奖励才显示）`奖励：` → `兑换码：`（单条）或 `兑换码 1/2/3：`（多条）
- 前瞻类条目组名统一为 `官方前瞻直播兑换码`
- 新码推送同排版，首行是 `原神新兑换码`，不带缓存时间

---

## 二、安装

`astrbot_plugin_yscd-v1.0.0` 整个文件夹放进 `AstrBot/data/plugins/`，再到 WebUI「插件」页刷新/重载。

```
astrbot_plugin_yscd-v1.0.0/
├── main.py
├── metadata.yaml
├── _conf_schema.json
├── requirements.txt
└── README.md
```

依赖 `httpx`（AstrBot 自带）。`metadata.yaml` 的 `repo` 是占位地址，发布前改成你自己的仓库。

---

## 三、指令

| 指令 | 作用 |
| --- | --- |
| `/yscd` | 查看有效兑换码；同时（默认）自动订阅新码推送 |
| `/yscd sub` | 订阅推送 |
| `/yscd unsub` | 取消订阅 |
| `/yscd refresh` | 立即刷新数据源再输出 |
| `/yscd help` | 帮助 |

兑换地址：<https://ys.mihoyo.com/main/gift>

---

## 四、配置

| 配置 | 默认 | 说明 |
| --- | --- | --- |
| `enable_bgi_source` | 开 | BGI 兑换码仓库（推荐，含国服+国际服，插件自动过滤国际服） |
| `enable_live_source` | 开 | 米游社官方前瞻直播（miyolive），前瞻码补充源 |
| `enable_auto_push` | 开 | 后台轮询 + 新码推送 |
| `auto_subscribe` | 开 | 发过 `/yscd` 就自动订阅（群聊直接发群里，不 @ 所有人） |
| `manual_codes` | 空 | 手动补录，每行 `标题\|有效期\|兑换码\|奖励` |
| `fetch_interval_minutes` | 30 | 轮询间隔 |
| `cache_ttl_minutes` | 10 | 查询缓存时间 |
| `max_push_codes` | 6 | 单次推送最多几条 |
| `http_timeout` | 15 | 请求超时（秒） |
| `proxy` | 空 | HTTP 代理 |

`manual_codes` 示例：

```
官方前瞻直播兑换码|2026-10-26|千星奇域一起拍拍拍|原石*100 精锻用魔矿*10
```

---

## 五、实现说明

### 数据源 1：BetterGI 兑换码仓库

`https://cnb.cool/bettergi/genshin-redeem-code/-/git/raw/main/codes.json`

返回数组，每项形如：

```json
{
  "title": "原神7.2前瞻",
  "content": "原石*100 精锻用魔矿*10 + 原石*100 大英雄的经验*5 + 原石*100 摩拉*50000",
  "time": "2026-10-23 19:30",
  "codes": ["坏灭肇始之时", "天才少年米提亚登场", "千星奇域一起拍拍拍"],
  "valid": "2026-10-26"
}
```

处理：

- **过滤国际服**：标题含 `[国际服]` / `国际` / `overseas` / `global` 的条目直接丢弃；标题没写国服但整组都是纯英文单词的也按国际服处理；
- **过滤过期**：`valid` 早于今天的条目只计数、不展示（对应「已隐藏 N 条」）；
- `content` 按 ` + ` / `；` 拆成奖励列表，`valid` 作为有效期。

### 数据源 2：米游社官方前瞻直播（miyolive）

1. `bbs-api.miyoushe.com/post/wapi/getForumPostList`（forum_id=28）拉官方帖，找标题含「前瞻 / 直播」的帖子，从其 `structured_content` 里正则取 `act_id`；
2. `api-takumi.mihoyo.com/event/miyolive/index`（header `x-rpc-act_id`）拿 `live.start` 与 `code_ver`；
3. `api-takumi-static.mihoyo.com/event/miyolive/refreshCode?version=<code_ver>` 拿 `code_list`（`code` / `title`（奖励）/ `to_get_time`）；
4. 有效期按「开播时间 + 3 天」计算，奖励取 `title`。

这个源只补 BGI 里没有的码（按码去重），失败不影响主源。

### 去重与状态

- 状态文件 `data/plugin_data/astrbot_plugin_yscd/state.json`，记录每个码的组名/有效期/奖励/是否已推送/首次出现时间；
- 每轮刷新会**用最新结果覆盖**：数据源里已经不存在的码（下线、被替换）会从状态里删掉，所以升级后不会再残留旧版本的国际服英文码；
- 首次加载只入库不推送，之后新增的才推；
- 推送纯文本、无 `At`/`AtAll`；群聊触发过一次 `/yscd` 后就一直推这个群；连续失败 3 次的会话自动移除订阅。

---

## 六、排错

| 现象 | 检查 |
| --- | --- |
| 还是英文码 | 旧状态文件残留：停用插件删除 `data/plugin_data/astrbot_plugin_yscd/` 后重载（本版会自动清掉不在源里的码） |
| 查不到码 | 日志 `BGI 兑换码源失败` → 网络/代理问题；`米游社直播源失败` 只影响前瞻补充 |
| 前瞻码没出 | 直播后 `to_get_time` 未到、或 BGI 仓库还没更新，等下一轮轮询 |
| 想手工补 | 用 `manual_codes`，格式见上 |

- 只处理国服，不请求任何国际服接口；
- 兑换码有时效，收到推送请尽快兑换。
