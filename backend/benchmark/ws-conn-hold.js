// benchmark/ws-conn-hold.js — k6 WebSocket 长连接保持压测
//
// 用法：
//   k6 run --env VUS=10000 --env HOLD=60 ws-conn-hold.js
//
// 每个 VU 建连后保持 HOLD 秒，验证服务端维持大量长连接的能力。
// 压测期间用 pprof 观察 goroutine 数和内存：
//   go tool pprof http://localhost:6060/debug/pprof/goroutine
//   go tool pprof http://localhost:6060/debug/pprof/heap

import ws from "k6/ws";
import { sleep } from "k6";
import { SharedArray } from "k6/data";

const tokens = new SharedArray("tokens", function () {
  return open("tokens.csv")
    .split("\n")
    .slice(1)
    .filter((l) => l.trim())
    .map((l) => l.split(",")[1]);
});

const VUS = parseInt(__ENV.VUS) || 100;
const HOLD = parseInt(__ENV.HOLD) || 60;

export const options = {
  scenarios: {
    hold: {
      executor: "per-vu-iterations",
      vus: VUS,
      iterations: 1,
      maxDuration: `${HOLD + 30}s`,
      gracefulStop: "5s",
    },
  },
};

export default function () {
  const idx = (__VU - 1) % tokens.length;
  const token = tokens[idx];
  if (!token) return;

  const url = `ws://localhost:8080/ws?token=${token}`;

  const res = ws.connect(url, null, function (socket) {
    socket.on("open", () => {});
    socket.on("close", () => {});
    socket.on("error", () => {});
  });

  if (res.status !== 101) {
    console.error(`VU ${__VU}: 建连失败 status=${res.status}`);
    return;
  }

  // 保持连接 HOLD 秒，期间不关闭 socket
  sleep(HOLD);
}
