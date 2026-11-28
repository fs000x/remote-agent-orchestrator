import { defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    include: ["tests/**/*.test.ts"],
    testTimeout: 30_000,
    hookTimeout: 30_000,
    // 真机用例由环境变量/可达性门禁自行 skip，不在配置层隐藏失败
  },
});
