---
title: 幸运抽奖系统
emoji: 🎡
colorFrom: purple
colorTo: pink
sdk: docker
app_port: 7860
pinned: false
---

# 幸运抽奖系统

跨设备的在线幸运抽奖：参与者输入名字加入，主办方通过 PIN 登录后台，支持转盘 / 随机开奖、多奖项同抽、每人唯一 CDK、中奖冷却、定时开奖、实时同步与 Excel/TXT 导出。

- 参与者入口：`/`
- 主办方入口：`/admin`
- 健康检查：`/healthz`

主办方 PIN 通过 Space Secret `ADMIN_PIN` 注入（默认 `888888`）。

> 免费 Space 的磁盘是临时的，重启后会清空数据。请在活动结束后立即导出中奖记录归档。
