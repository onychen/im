// benchmark/ws-conn-test.js — k6 WebSocket 并发连接数压测
//
// 用法：
//   k6 run --env VUS=50 ws-conn-test.js
//   k6 run --env VUS=1000 ws-conn-test.js
//   k6 run --env VUS=10000 ws-conn-test.js

import ws from "k6/ws";
import { check } from "k6";
import { SharedArray } from "k6/data";

const tokens = new SharedArray("tokens", function () {
  return open("tokens.csv")
    .split("\n")
    .slice(1)
    .filter((line) => line.trim() !== "")
    .map((line) => {
      const [userID, token] = line.split(",");
      return token;
    });
});

const VUS = parseInt(__ENV.VUS) || 100;

export const options = {
  scenarios: {
    connect: {
      executor: "per-vu-iterations",
      vus: VUS,
      iterations: 1,            // 每个 VU 只跑 1 次
      maxDuration: `${Math.ceil(VUS / 100) + 30}s`,
    },
  },
  thresholds: {
    "checks{check:connected}": ["rate>0.99"],
    "ws_connecting": ["p(95)<500"],
  },
};

export default function () {
  const idx = (__VU - 1) % tokens.length;
  const token = tokens[idx];
  if (!token) return;

  const url = `ws://localhost:8080/ws?token=${token}`;

  const res = ws.connect(url, null, function (socket) {
    socket.on("open", () => {
      // 连接成功后立即关闭，让 iteration 结束
      socket.close();
    });
    socket.on("close", () => {});
  });

  check(res, {
    connected: (r) => r && r.status === 101,
  });
}
