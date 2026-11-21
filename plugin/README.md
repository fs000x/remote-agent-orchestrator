# rao — Remote Agent Orchestrator (ZCode 插件入口)

前置步骤（插件安装前完成一次）：

    pipx install rao
    rao setup          # 检测本机 Agent 并注入 MCP+Skill；配置远程设备

安装本插件后，ZCode 会话中即可使用 6 个编排工具
（devices_list / remote_dispatch / remote_status / remote_steer / remote_cancel / remote_collect）。
