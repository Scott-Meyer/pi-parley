// Scripted OpenAI-compatible model for the OMP check. It decides each reply from
// the conversation it is sent, never from request counts.
import http from "node:http";
import { appendFileSync } from "node:fs";

const port = Number(process.env.PORT);
const log = (value) => process.env.REQUEST_LOG && appendFileSync(process.env.REQUEST_LOG, `${JSON.stringify(value)}\n`);
const textOf = (message) => typeof message.content === "string" ? message.content
  : Array.isArray(message.content) ? message.content.map((part) => part.text ?? "").join("\n") : "";

function reply(messages) {
  const all = messages.map(textOf).join("\n");
  const last = messages.at(-1);
  const toolCall = (name, args) => ({ tool: { name, args } });
  if (last?.role === "tool") return { text: `done: ${textOf(last).slice(0, 80).replace(/\s+/g, " ")}` };
  const lastUser = [...messages].reverse().find((message) => message.role === "user");
  const latest = lastUser ? textOf(lastUser) : "";
  if (latest.includes("OMP-B register")) return toolCall("parley", { action: "status", profile: { name: "omp-b", description: "Answering the OMP compatibility check" } });
  if (latest.includes("OMP-A ask")) return toolCall("parley", { action: "ask", to: "omp-b", message: "OMP-PING-4417 what is the fixture word?", blocking: false });
  // Check the answer first: a reply's header quotes the question it answers.
  if (latest.includes("OMP-PONG-4417")) return { text: "received the answer" };
  if (latest.includes("OMP-PING-4417")) {
    const ref = latest.match(/Message: (#\d+)/)?.[1];
    return ref ? toolCall("parley", { action: "reply", replyTo: ref, message: "OMP-PONG-4417 the word is lantern" }) : { text: "no message reference seen" };
  }
  return { text: `nothing scripted for: ${latest.slice(0, 60)}` };
}

http.createServer(async (req, res) => {
  if (req.method === "GET") { res.writeHead(200, { "content-type": "application/json" }); res.end(JSON.stringify({ object: "list", data: [{ id: "compat-model", object: "model" }] })); return; }
  let body = ""; for await (const chunk of req) body += chunk;
  const request = JSON.parse(body || "{}");
  const decision = reply(request.messages ?? []);
  log({ at: new Date().toISOString(), messages: (request.messages ?? []).map((m) => ({ role: m.role, text: textOf(m).slice(0, 4000), tool_calls: m.tool_calls })), decision });
  const id = `c${Date.now()}`;
  const chunk = (delta, finish = null) => ({ id, object: "chat.completion.chunk", created: Math.floor(Date.now() / 1000), model: "compat-model", choices: [{ index: 0, delta, finish_reason: finish }] });
  res.writeHead(200, { "content-type": "text/event-stream", "cache-control": "no-cache" });
  const events = decision.tool
    ? [chunk({ role: "assistant", tool_calls: [{ index: 0, id: `call_${id}`, type: "function", function: { name: decision.tool.name, arguments: JSON.stringify(decision.tool.args) } }] }), chunk({}, "tool_calls")]
    : [chunk({ role: "assistant", content: decision.text }), chunk({}, "stop")];
  for (const event of events) res.write(`data: ${JSON.stringify(event)}\n\n`);
  res.end("data: [DONE]\n\n");
}).listen(port, "127.0.0.1", () => console.log("fake-openai-ready"));
