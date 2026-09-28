// benchmark/ws-debug.js — 最小诊断脚本
import ws from "k6/ws";

const tokens = open("tokens.csv")
  .split("\n")
  .slice(1)
  .filter((l) => l.trim())
  .map((l) => {
    const parts = l.split(",");
    return parts[1]; // token
  });

export default function () {
  const token = tokens[0];
  console.log(`Token prefix: ${token.substring(0, 40)}...`);

  const url = `ws://localhost:8080/ws?token=${token}`;

  const res = ws.connect(url, null, function (socket) {
    socket.on("open", () => {
      console.log("✅ OPEN — 连接成功！");
    });
    socket.on("close", (code) => {
      console.log(`❌ CLOSE — code=${code}`);
    });
    socket.on("error", (e) => {
      console.log(`⚠️  ERROR — ${e ? JSON.stringify(e) : "unknown"}`);
    });
    socket.on("message", (msg) => {
      console.log(`📩 MSG: ${msg}`);
    });
    // k6 v2 的 setTimeout 签名可能已变化，先不加
  });

  console.log(`→ connect 返回 status=${res.status} error="${res.error}"`);
}
