// Exercise the running server's atomic Redis -> Kafka -> MySQL message path.
// Usage: node scripts/smoke.mjs [http://127.0.0.1:18080]
const base = process.argv[2] ?? "http://127.0.0.1:18080";
const suffix = `${Date.now()}_${Math.floor(Math.random() * 100000)}`;
const password = "smoke_pass_1234";

async function api(method, path, body, token, expected = 200) {
  const response = await fetch(`${base}${path}`, {
    method,
    headers: {
      ...(body ? { "content-type": "application/json" } : {}),
      ...(token ? { authorization: `Bearer ${token}` } : {}),
    },
    body: body ? JSON.stringify(body) : undefined,
    signal: AbortSignal.timeout(5000),
  });
  const result = await response.json();
  if (response.status !== expected || result.code !== 0) {
    throw new Error(`${method} ${path}: HTTP ${response.status}, code=${result.code}, message=${result.message}`);
  }
  return result.data;
}

function openSocket(token) {
  return new Promise((resolve, reject) => {
    const url = new URL(base.replace(/^http/, "ws") + "/ws");
    url.searchParams.set("token", token);
    const socket = new WebSocket(url);
    const timer = setTimeout(() => reject(new Error("WebSocket open timed out")), 5000);
    socket.addEventListener("open", () => { clearTimeout(timer); resolve(socket); }, { once: true });
    socket.addEventListener("error", () => { clearTimeout(timer); reject(new Error("WebSocket open failed")); }, { once: true });
  });
}

function waitForType(socket, type, timeout = 10000) {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => {
      socket.removeEventListener("message", onMessage);
      reject(new Error(`WebSocket ${type} timed out`));
    }, timeout);
    function onMessage(event) {
      const message = JSON.parse(event.data);
      if (message.type !== type) return;
      clearTimeout(timer);
      socket.removeEventListener("message", onMessage);
      resolve(message.data);
    }
    socket.addEventListener("message", onMessage);
  });
}

let senderSocket;
let receiverSocket;
try {
  const sender = `smoke_sender_${suffix}`;
  const receiver = `smoke_receiver_${suffix}`;
  await api("POST", "/api/v1/auth/register", { username: sender, password }, undefined, 201);
  const receiverUser = await api("POST", "/api/v1/auth/register", { username: receiver, password }, undefined, 201);
  const senderAuth = await api("POST", "/api/v1/auth/login", { username: sender, password });
  const receiverAuth = await api("POST", "/api/v1/auth/login", { username: receiver, password });
  const request = await api("POST", "/api/v1/friend/request", { to_user_id: receiverUser.user_id }, senderAuth.access_token, 201);
  await api("POST", "/api/v1/friend/accept", { request_id: request.request_id }, receiverAuth.access_token);

  senderSocket = await openSocket(senderAuth.access_token);
  receiverSocket = await openSocket(receiverAuth.access_token);
  const clientMsgId = `smoke_${suffix}`;
  const ack = waitForType(senderSocket, "serverAck");
  const delivery = waitForType(receiverSocket, "msg");
  senderSocket.send(JSON.stringify({
    type: "msg",
    data: {
      msgId: clientMsgId,
      convType: 1,
      toId: receiverUser.user_id,
      msgType: 1,
      content: clientMsgId,
      timestamp: Math.floor(Date.now() / 1000),
    },
  }));
  const [ackData, deliveredData] = await Promise.all([ack, delivery]);
  if (!ackData.serverMsgId || deliveredData.content !== clientMsgId) {
    throw new Error("message acknowledgment or delivery did not match the sent message");
  }

  let archived = false;
  for (let attempt = 0; attempt < 20; attempt++) {
    const result = await api("GET", `/api/v1/msg/search?q=${encodeURIComponent(clientMsgId)}`, undefined, senderAuth.access_token);
    archived = (result.items ?? []).some((item) => item.content === clientMsgId);
    if (archived) break;
    await new Promise((resolve) => setTimeout(resolve, 500));
  }
  if (!archived) throw new Error("message was delivered but not archived in MySQL");
  console.log("PASS: register, login, friendship, WebSocket delivery/ack, MySQL archive");
} finally {
  senderSocket?.close();
  receiverSocket?.close();
}
