import { createRequire as __parleyCreateRequire } from "node:module"; const require = __parleyCreateRequire(import.meta.url);
var __defProp = Object.defineProperty;
var __getOwnPropNames = Object.getOwnPropertyNames;
var __esm = (fn, res, err) => function __init() {
  if (err) throw err[0];
  try {
    return fn && (res = (0, fn[__getOwnPropNames(fn)[0]])(fn = 0)), res;
  } catch (e) {
    throw err = [e], e;
  }
};
var __export = (target2, all) => {
  for (var name in all)
    __defProp(target2, name, { get: all[name], enumerable: true });
};

// broker/bundle.ts
import { existsSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
function bundledScript(name) {
  if (!bundledBuild) return void 0;
  const path2 = join(dirname(fileURLToPath(import.meta.url)), `${name}.mjs`);
  return existsSync(path2) ? path2 : void 0;
}
function nodeCommand(execPath = process.execPath) {
  const name = execPath.split(/[\\/]/).pop() ?? "";
  return /^node(js)?(\.exe)?$/i.test(name) ? execPath : "node";
}
var bundledBuild;
var init_bundle = __esm({
  "broker/bundle.ts"() {
    bundledBuild = true ? JSON.parse('{"packageVersion":"1.6.0","sourceId":"2a7065408bfd38e34b57cfd4944b46f6c05f275657423bf9f8e1f2a822fd90cb"}') : void 0;
  }
});

// broker/paths.ts
import { createHash } from "crypto";
import { chmodSync, mkdirSync, readFileSync } from "fs";
import { isAbsolute, join as join2, resolve, win32 } from "path";
import { homedir } from "os";
function getBrokerPipeName(agentDir) {
  let normalized = win32.normalize(agentDir).toLowerCase();
  const rootLength = win32.parse(normalized).root.length;
  while (normalized.length > rootLength && normalized.endsWith("\\")) {
    normalized = normalized.slice(0, -1);
  }
  const digest = createHash("sha256").update("pi-parley-agent-dir\0", "utf8").update(normalized, "utf8").digest("hex");
  return `pi-parley-${digest}`;
}
function getAgentDirPath(env = process.env, homeDir = homedir(), cwd = process.cwd()) {
  const configured = env.PI_CODING_AGENT_DIR?.trim();
  if (!configured) {
    return join2(homeDir, ".pi/agent");
  }
  return isAbsolute(configured) ? configured : resolve(cwd, configured);
}
function getParleyDirPath(agentDir = getAgentDirPath()) {
  return join2(agentDir, "parley");
}
function shouldUseTcpTransport(env = process.env, platform = process.platform) {
  const transport = env.PI_PARLEY_TRANSPORT?.trim().toLowerCase();
  if (transport === "tcp") return true;
  if (transport === "socket") return false;
  const tcpOptIn = env.PI_PARLEY_TCP?.trim().toLowerCase();
  if (tcpOptIn === "1" || tcpOptIn === "true") return true;
  if (tcpOptIn === "0" || tcpOptIn === "false") return false;
  return platform === "win32";
}
function getBrokerPortFilePath(parleyDir = getParleyDirPath()) {
  return join2(parleyDir, "broker.port.json");
}
function getBrokerSocketPath(platform = process.platform, agentDir = getAgentDirPath()) {
  if (platform === "win32") {
    return `\\\\.\\pipe\\${getBrokerPipeName(agentDir)}`;
  }
  return join2(getParleyDirPath(agentDir), "broker.sock");
}
function readBrokerTcpEndpoint(parleyDir = getParleyDirPath()) {
  const endpointFile = getBrokerPortFilePath(parleyDir);
  const parsed = JSON.parse(readFileSync(endpointFile, "utf-8"));
  if (typeof parsed !== "object" || parsed === null || Array.isArray(parsed)) {
    throw new Error(`Invalid parley TCP endpoint at ${endpointFile}: expected a JSON object`);
  }
  const endpoint = parsed;
  if (endpoint.transport !== "tcp" || endpoint.host !== PARLEY_TCP_HOST || typeof endpoint.port !== "number" || !Number.isSafeInteger(endpoint.port) || endpoint.port <= 0 || endpoint.port > 65535 || typeof endpoint.stateId !== "string" || endpoint.stateId.length === 0) {
    throw new Error(`Invalid parley TCP endpoint at ${endpointFile}`);
  }
  return Object.freeze({ transport: "tcp", host: endpoint.host, port: endpoint.port, stateId: endpoint.stateId });
}
function getBrokerConnectTarget(platform = process.platform, env = process.env, parleyDir = getParleyDirPath(getAgentDirPath(env))) {
  return shouldUseTcpTransport(env, platform) ? readBrokerTcpEndpoint(parleyDir) : getBrokerSocketPath(platform, getAgentDirPath(env));
}
function ensureParleyRuntimeDir(parleyDir = getParleyDirPath(), platform = process.platform) {
  mkdirSync(parleyDir, { recursive: true, mode: PARLEY_DIR_MODE });
  if (platform !== "win32") {
    chmodSync(parleyDir, PARLEY_DIR_MODE);
  }
}
function restrictParleyRuntimeFile(filePath, platform = process.platform) {
  if (platform !== "win32") {
    chmodSync(filePath, PARLEY_RUNTIME_FILE_MODE);
  }
}
var PARLEY_DIR_MODE, PARLEY_RUNTIME_FILE_MODE, PARLEY_TCP_HOST, PARLEY_PROTOCOL_NAME, PARLEY_PROTOCOL_VERSION;
var init_paths = __esm({
  "broker/paths.ts"() {
    PARLEY_DIR_MODE = 448;
    PARLEY_RUNTIME_FILE_MODE = 384;
    PARLEY_TCP_HOST = "127.0.0.1";
    PARLEY_PROTOCOL_NAME = "pi-parley";
    PARLEY_PROTOCOL_VERSION = 1;
  }
});

// broker/framing.ts
function writeMessage(socket, msg) {
  const json = JSON.stringify(msg);
  const payloadLength = Buffer.byteLength(json, "utf-8");
  const frame = Buffer.allocUnsafe(4 + payloadLength);
  frame.writeUInt32BE(payloadLength, 0);
  frame.write(json, 4, payloadLength, "utf-8");
  socket.write(frame);
}
function createMessageReader(onMessage, onError, maxFrameBytes = MAX_FRAME_BYTES) {
  const header = Buffer.allocUnsafe(4);
  let headerBytes = 0;
  let payload = null;
  let payloadBytes = 0;
  let payloadLength = 0;
  function reportMessage(framePayload) {
    let msg;
    try {
      msg = JSON.parse(framePayload.toString("utf-8"));
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      onError(new Error(`Failed to parse parley message: ${message}`, { cause: error }));
      return false;
    }
    try {
      onMessage(msg);
      return true;
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      onError(new Error(`Failed to handle parley message: ${message}`, { cause: error }));
      return false;
    }
  }
  return (data) => {
    let offset = 0;
    while (offset < data.length) {
      if (headerBytes < 4) {
        const bytes2 = Math.min(4 - headerBytes, data.length - offset);
        data.copy(header, headerBytes, offset, offset + bytes2);
        headerBytes += bytes2;
        offset += bytes2;
        if (headerBytes < 4) {
          return;
        }
        payloadLength = header.readUInt32BE(0);
        if (payloadLength > maxFrameBytes) {
          headerBytes = 0;
          onError(new Error(`Parley frame length ${payloadLength} exceeds maximum ${maxFrameBytes} bytes`));
          return;
        }
      }
      if (payloadBytes === 0 && data.length - offset >= payloadLength) {
        const framePayload2 = data.subarray(offset, offset + payloadLength);
        offset += payloadLength;
        headerBytes = 0;
        payload = null;
        payloadLength = 0;
        if (!reportMessage(framePayload2)) {
          return;
        }
        continue;
      }
      if (payload === null || payload.length !== payloadLength) {
        payload = Buffer.allocUnsafe(payloadLength);
      }
      const bytes = Math.min(payloadLength - payloadBytes, data.length - offset);
      data.copy(payload, payloadBytes, offset, offset + bytes);
      payloadBytes += bytes;
      offset += bytes;
      if (payloadBytes < payloadLength) {
        return;
      }
      const framePayload = payload;
      headerBytes = 0;
      payload = null;
      payloadBytes = 0;
      payloadLength = 0;
      if (!reportMessage(framePayload)) {
        return;
      }
    }
  };
}
var MAX_FRAME_BYTES;
var init_framing = __esm({
  "broker/framing.ts"() {
    MAX_FRAME_BYTES = 1024 * 1024;
  }
});

// hosts/control.ts
var control_exports = {};
__export(control_exports, {
  RELEASE_EVENT: () => RELEASE_EVENT,
  askSiblingToRelease: () => askSiblingToRelease,
  sendHook: () => sendHook,
  serveControl: () => serveControl,
  takeEarlySessionId: () => takeEarlySessionId
});
import { execFileSync } from "node:child_process";
import { existsSync as existsSync2, mkdirSync as mkdirSync2, readdirSync, readFileSync as readFileSync2, rmSync, writeFileSync } from "node:fs";
import net from "node:net";
import { join as join3 } from "node:path";
function hostsDir(env = process.env) {
  return join3(getParleyDirPath(env.PI_CODING_AGENT_DIR?.trim() || void 0), "hosts");
}
function parentDir(hostPid, env) {
  return join3(hostsDir(env), String(hostPid));
}
function serveControl(hostPid, cwd, handle) {
  const dir = parentDir(hostPid);
  mkdirSync2(dir, { recursive: true, mode: 448 });
  const socketPath = join3(dir, `${process.pid}.sock`);
  const notePath = join3(dir, `${process.pid}.json`);
  rmSync(socketPath, { force: true });
  const note = { pid: process.pid, cwd, startedAt: Date.now() };
  const writeNote = () => writeFileSync(notePath, JSON.stringify(note), { mode: 384 });
  writeNote();
  const server = net.createServer((socket) => {
    socket.on("error", () => {
    });
    socket.on("data", createMessageReader((value) => {
      void handle(value).then(
        (response) => {
          try {
            writeMessage(socket, response);
          } catch {
          }
          socket.end();
        },
        () => socket.end()
      );
    }, () => socket.destroy()));
  });
  server.listen(socketPath);
  server.unref();
  return {
    claim(sessionId) {
      if (note.sessionId !== sessionId) {
        note.sessionId = sessionId;
        writeNote();
      }
    },
    unclaim() {
      if (note.sessionId !== void 0) {
        delete note.sessionId;
        note.released = true;
        writeNote();
      }
    },
    close() {
      server.close();
      rmSync(socketPath, { force: true });
      rmSync(notePath, { force: true });
    }
  };
}
async function askSiblingToRelease(hostPid, sessionId, timeoutMs = 5e3) {
  const dir = parentDir(hostPid);
  let sibling;
  try {
    for (const name of readdirSync(dir)) {
      if (!name.endsWith(".json")) continue;
      try {
        const note = JSON.parse(readFileSync2(join3(dir, name), "utf8"));
        if (note.pid !== process.pid && note.sessionId === sessionId && alive(note.pid)) sibling = note;
      } catch {
      }
    }
  } catch {
    return void 0;
  }
  if (!sibling) return void 0;
  return new Promise((resolve4) => {
    const socket = net.connect(join3(dir, `${sibling.pid}.sock`));
    const done = (value) => {
      clearTimeout(timer);
      socket.destroy();
      resolve4(value);
    };
    const timer = setTimeout(() => done(), timeoutMs);
    socket.on("connect", () => writeMessage(socket, { event: RELEASE_EVENT, payload: { session_id: sessionId } }));
    socket.on("data", createMessageReader((value) => done(value), () => done()));
    socket.on("error", () => done());
    socket.on("close", () => done());
  });
}
function ancestors() {
  const chain = [];
  let pid = process.ppid;
  for (let depth = 0; depth < 8 && pid > 1; depth++) {
    chain.push(pid);
    try {
      pid = Number(execFileSync("ps", ["-o", "ppid=", "-p", String(pid)], { encoding: "utf8", timeout: 2e3 }).trim());
    } catch {
      break;
    }
  }
  return chain;
}
function alive(pid) {
  try {
    process.kill(pid, 0);
    return true;
  } catch {
    return false;
  }
}
function chooseServer(dir, sessionId, cwd) {
  const notes = [];
  for (const name of readdirSync(dir)) {
    if (!name.endsWith(".json")) continue;
    try {
      const note = JSON.parse(readFileSync2(join3(dir, name), "utf8"));
      if (alive(note.pid)) notes.push(note);
      else {
        rmSync(join3(dir, name), { force: true });
        rmSync(join3(dir, `${note.pid}.sock`), { force: true });
      }
    } catch {
    }
  }
  if (sessionId) {
    const claimed = notes.find((note) => note.sessionId === sessionId);
    if (claimed) return claimed;
  }
  const unclaimed = notes.filter((note) => !note.sessionId && !note.released).sort((a, b) => a.startedAt - b.startedAt);
  return unclaimed.find((note) => cwd && samePath(note.cwd, cwd)) ?? (notes.length === 1 ? notes[0] : unclaimed.length === 1 ? unclaimed[0] : void 0);
}
function samePath(a, b) {
  const strip = (p) => p.replace(/^\/private(?=\/)/, "").replace(/\/+$/, "");
  return strip(a) === strip(b);
}
async function sendHook(request, timeoutMs = 5e3) {
  const sessionId = typeof request.payload.session_id === "string" && request.payload.session_id ? request.payload.session_id : void 0;
  const cwd = typeof request.payload.cwd === "string" ? request.payload.cwd : process.cwd();
  const dir = ancestors().map((pid) => parentDir(pid)).find((candidate) => existsSync2(candidate));
  const server = dir ? chooseServer(dir, sessionId, cwd) : void 0;
  if (!dir || !server) {
    if (sessionId) {
      try {
        const early = parentDir(process.ppid);
        mkdirSync2(early, { recursive: true, mode: 448 });
        writeFileSync(join3(early, "early-session"), JSON.stringify({ sessionId, cwd }), { mode: 384 });
      } catch {
      }
    }
    return void 0;
  }
  return new Promise((resolve4) => {
    const socket = net.connect(join3(dir, `${server.pid}.sock`));
    const done = (value) => {
      clearTimeout(timer);
      socket.destroy();
      resolve4(value);
    };
    const timer = setTimeout(() => done(), timeoutMs);
    socket.on("connect", () => writeMessage(socket, request));
    socket.on("data", createMessageReader((value) => done(value), () => done()));
    socket.on("error", () => done());
    socket.on("close", () => done());
  });
}
function takeEarlySessionId(hostPid, cwd) {
  const file = join3(parentDir(hostPid), "early-session");
  try {
    const early = JSON.parse(readFileSync2(file, "utf8"));
    if (!early.sessionId || early.cwd && !samePath(early.cwd, cwd)) return void 0;
    rmSync(file, { force: true });
    return early.sessionId;
  } catch {
    return void 0;
  }
}
var RELEASE_EVENT;
var init_control = __esm({
  "hosts/control.ts"() {
    init_paths();
    init_framing();
    RELEASE_EVENT = "parley:release";
  }
});

// node_modules/typebox/build/system/memory/metrics.mjs
var Metrics;
var init_metrics = __esm({
  "node_modules/typebox/build/system/memory/metrics.mjs"() {
    Metrics = {
      assign: 0,
      create: 0,
      clone: 0,
      discard: 0,
      update: 0
    };
  }
});

// node_modules/typebox/build/guard/string.mjs
function IsBetween(value, min, max) {
  return value >= min && value <= max;
}
function IsZeroWidthJoiner(value) {
  return value === 8205;
}
function IsHighSurrogate(value) {
  return IsBetween(value, 55296, 56319);
}
function IsRegionalIndicator(value) {
  return IsBetween(value, 127462, 127487);
}
function IsVariationSelector(value) {
  return IsBetween(value, 65024, 65039);
}
function IsCombiningMark(value) {
  return IsBetween(value, 768, 879) || IsBetween(value, 6832, 6911) || IsBetween(value, 7616, 7679) || IsBetween(value, 65056, 65071);
}
function CodePointLength(value) {
  return value > 65535 ? 2 : 1;
}
function ConsumeModifiers(value, index) {
  while (index < value.length) {
    const point = value.codePointAt(index);
    if (IsCombiningMark(point) || IsVariationSelector(point)) {
      index += CodePointLength(point);
    } else {
      break;
    }
  }
  return index;
}
function NextGraphemeClusterIndex(value, clusterStart) {
  const startCP = value.codePointAt(clusterStart);
  let clusterEnd = clusterStart + CodePointLength(startCP);
  clusterEnd = ConsumeModifiers(value, clusterEnd);
  while (clusterEnd < value.length - 1 && IsZeroWidthJoiner(value.codePointAt(clusterEnd))) {
    const nextCP = value.codePointAt(clusterEnd + 1);
    clusterEnd += 1 + CodePointLength(nextCP);
    clusterEnd = ConsumeModifiers(value, clusterEnd);
  }
  if (IsRegionalIndicator(startCP) && clusterEnd < value.length && IsRegionalIndicator(value.codePointAt(clusterEnd))) {
    clusterEnd += CodePointLength(value.codePointAt(clusterEnd));
  }
  return clusterEnd;
}
function IsGraphemeCodePoint(value) {
  return value >= 768 && // above special range
  (IsHighSurrogate(value) || IsCombiningMark(value) || IsVariationSelector(value) || IsZeroWidthJoiner(value));
}
function GraphemeCount(value) {
  let count = 0;
  let index = 0;
  while (index < value.length) {
    index = NextGraphemeClusterIndex(value, index);
    count++;
  }
  return count;
}
function IsMinLengthSegmented(value, minLength) {
  let count = 0;
  let index = 0;
  while (index < value.length) {
    index = NextGraphemeClusterIndex(value, index);
    if (++count >= minLength)
      return true;
  }
  return false;
}
function IsMaxLengthSegmented(value, maxLength) {
  let count = 0;
  let index = 0;
  while (index < value.length) {
    index = NextGraphemeClusterIndex(value, index);
    if (++count > maxLength)
      return false;
  }
  return true;
}
function IsMinLength(value, minLength) {
  if (minLength === 0)
    return true;
  if (value.length < minLength)
    return false;
  let index = 0;
  while (true) {
    if (IsGraphemeCodePoint(value.charCodeAt(index))) {
      return IsMinLengthSegmented(value, minLength);
    }
    if (++index >= minLength)
      return true;
  }
}
function IsMaxLength(value, maxLength) {
  if (value.length <= maxLength)
    return true;
  let index = 0;
  while (true) {
    if (IsGraphemeCodePoint(value.charCodeAt(index))) {
      return IsMaxLengthSegmented(value, maxLength);
    }
    if (++index > maxLength)
      return false;
  }
}
var init_string = __esm({
  "node_modules/typebox/build/guard/string.mjs"() {
  }
});

// node_modules/typebox/build/guard/guard.mjs
var guard_exports = {};
__export(guard_exports, {
  Counted: () => Counted,
  Entries: () => Entries,
  EntriesRegExp: () => EntriesRegExp,
  Every: () => Every,
  EveryAll: () => EveryAll,
  GraphemeCount: () => GraphemeCount2,
  HasPropertyKey: () => HasPropertyKey,
  IsArray: () => IsArray,
  IsBigInt: () => IsBigInt,
  IsBoolean: () => IsBoolean,
  IsClassInstance: () => IsClassInstance,
  IsConstructor: () => IsConstructor,
  IsDeepEqual: () => IsDeepEqual,
  IsEqual: () => IsEqual,
  IsFunction: () => IsFunction,
  IsGreaterEqualThan: () => IsGreaterEqualThan,
  IsGreaterThan: () => IsGreaterThan,
  IsInteger: () => IsInteger,
  IsLessEqualThan: () => IsLessEqualThan,
  IsLessThan: () => IsLessThan,
  IsMaxLength: () => IsMaxLength2,
  IsMinLength: () => IsMinLength2,
  IsMultipleOf: () => IsMultipleOf,
  IsNull: () => IsNull,
  IsNumber: () => IsNumber,
  IsObject: () => IsObject,
  IsObjectNotArray: () => IsObjectNotArray,
  IsString: () => IsString,
  IsSymbol: () => IsSymbol,
  IsUndefined: () => IsUndefined,
  IsUnsafePropertyKey: () => IsUnsafePropertyKey,
  IsValueLike: () => IsValueLike,
  Keys: () => Keys,
  ShiftLeft: () => ShiftLeft,
  Some: () => Some,
  SomeAll: () => SomeAll,
  Symbols: () => Symbols,
  Values: () => Values
});
function IsArray(value) {
  return Array.isArray(value);
}
function IsBigInt(value) {
  return IsEqual(typeof value, "bigint");
}
function IsBoolean(value) {
  return IsEqual(typeof value, "boolean");
}
function IsConstructor(value) {
  if (IsUndefined(value) || !IsFunction(value))
    return false;
  const result = Function.prototype.toString.call(value);
  if (/^class\s/.test(result))
    return true;
  if (/\[native code\]/.test(result))
    return true;
  return false;
}
function IsFunction(value) {
  return IsEqual(typeof value, "function");
}
function IsInteger(value) {
  return Number.isInteger(value);
}
function IsNull(value) {
  return IsEqual(value, null);
}
function IsNumber(value) {
  return Number.isFinite(value);
}
function IsObjectNotArray(value) {
  return IsObject(value) && !IsArray(value);
}
function IsObject(value) {
  return IsEqual(typeof value, "object") && !IsNull(value);
}
function IsString(value) {
  return IsEqual(typeof value, "string");
}
function IsSymbol(value) {
  return IsEqual(typeof value, "symbol");
}
function IsUndefined(value) {
  return IsEqual(value, void 0);
}
function IsEqual(left, right) {
  return left === right;
}
function IsGreaterThan(left, right) {
  return left > right;
}
function IsLessThan(left, right) {
  return left < right;
}
function IsLessEqualThan(left, right) {
  return left <= right;
}
function IsGreaterEqualThan(left, right) {
  return left >= right;
}
function IsMultipleOf(dividend, divisor) {
  if (IsBigInt(dividend) || IsBigInt(divisor)) {
    return BigInt(dividend) % BigInt(divisor) === 0n;
  }
  const tolerance = 1e-10;
  if (!IsNumber(dividend))
    return true;
  if (IsInteger(dividend) && 1 / divisor % 1 === 0)
    return true;
  const mod = dividend % divisor;
  return Math.min(Math.abs(mod), Math.abs(mod - divisor), Math.abs(mod + divisor)) < tolerance;
}
function IsClassInstance(value) {
  if (!IsObject(value))
    return false;
  const proto = globalThis.Object.getPrototypeOf(value);
  if (IsNull(proto))
    return false;
  return IsEqual(typeof proto.constructor, "function") && !(IsEqual(proto.constructor, globalThis.Object) || IsEqual(proto.constructor.name, "Object"));
}
function IsValueLike(value) {
  return IsBigInt(value) || IsBoolean(value) || IsNull(value) || IsNumber(value) || IsString(value) || IsUndefined(value);
}
function GraphemeCount2(value) {
  return GraphemeCount(value);
}
function IsMaxLength2(value, length) {
  return IsMaxLength(value, length);
}
function IsMinLength2(value, length) {
  return IsMinLength(value, length);
}
function Every(value, offset, callback) {
  return value.every((item, index) => index < offset || callback(item, index));
}
function EveryAll(value, offset, callback) {
  let result = true;
  value.forEach((item, index) => {
    if (index >= offset && !callback(item, index))
      result = false;
  });
  return result;
}
function Some(value, callback) {
  return value.some((value2, index) => callback(value2, index));
}
function SomeAll(value, callback) {
  let result = false;
  value.forEach((item, index) => {
    if (callback(item, index))
      result = true;
  });
  return result;
}
function Counted(value, callback) {
  return value.reduce((result, value2, index) => callback(value2, index) ? ++result : result, 0);
}
function ShiftLeft(array, true_, false_) {
  return IsEqual(array.length, 0) ? false_() : true_(array[0], array.slice(1));
}
function IsUnsafePropertyKey(key) {
  return IsEqual(key, "__proto__") || IsEqual(key, "constructor") || IsEqual(key, "prototype");
}
function HasPropertyKey(value, key) {
  return IsUnsafePropertyKey(key) ? Object.prototype.hasOwnProperty.call(value, key) : key in value;
}
function EntriesRegExp(value) {
  return Keys(value).map((key) => [new RegExp(`^${key}$`), value[key]]);
}
function Entries(value) {
  return Object.entries(value);
}
function Keys(value) {
  return Object.getOwnPropertyNames(value);
}
function Symbols(value) {
  return Object.getOwnPropertySymbols(value);
}
function Values(value) {
  return Object.values(value);
}
function DeepEqualObject(left, right) {
  if (!IsObject(right))
    return false;
  const keys = Keys(left);
  return IsEqual(keys.length, Keys(right).length) && keys.every((key) => IsDeepEqual(left[key], right[key]));
}
function DeepEqualArray(left, right) {
  return IsArray(right) && IsEqual(left.length, right.length) && left.every((_, index) => IsDeepEqual(left[index], right[index]));
}
function IsDeepEqual(left, right) {
  return IsArray(left) ? DeepEqualArray(left, right) : IsObject(left) ? DeepEqualObject(left, right) : IsEqual(left, right);
}
var init_guard = __esm({
  "node_modules/typebox/build/guard/guard.mjs"() {
    init_string();
  }
});

// node_modules/typebox/build/guard/emit.mjs
var init_emit = __esm({
  "node_modules/typebox/build/guard/emit.mjs"() {
    init_guard();
  }
});

// node_modules/typebox/build/guard/globals.mjs
var globals_exports = {};
__export(globals_exports, {
  IsBigInt64Array: () => IsBigInt64Array,
  IsBigUint64Array: () => IsBigUint64Array,
  IsBoolean: () => IsBoolean2,
  IsDate: () => IsDate,
  IsFloat32Array: () => IsFloat32Array,
  IsFloat64Array: () => IsFloat64Array,
  IsInt16Array: () => IsInt16Array,
  IsInt32Array: () => IsInt32Array,
  IsInt8Array: () => IsInt8Array,
  IsMap: () => IsMap,
  IsNumber: () => IsNumber2,
  IsRegExp: () => IsRegExp,
  IsSet: () => IsSet,
  IsString: () => IsString2,
  IsTypeArray: () => IsTypeArray,
  IsUint16Array: () => IsUint16Array,
  IsUint32Array: () => IsUint32Array,
  IsUint8Array: () => IsUint8Array,
  IsUint8ClampedArray: () => IsUint8ClampedArray
});
function IsBoolean2(value) {
  return value instanceof Boolean;
}
function IsNumber2(value) {
  return value instanceof Number;
}
function IsString2(value) {
  return value instanceof String;
}
function IsTypeArray(value) {
  return globalThis.ArrayBuffer.isView(value);
}
function IsInt8Array(value) {
  return value instanceof globalThis.Int8Array;
}
function IsUint8Array(value) {
  return value instanceof globalThis.Uint8Array;
}
function IsUint8ClampedArray(value) {
  return value instanceof globalThis.Uint8ClampedArray;
}
function IsInt16Array(value) {
  return value instanceof globalThis.Int16Array;
}
function IsUint16Array(value) {
  return value instanceof globalThis.Uint16Array;
}
function IsInt32Array(value) {
  return value instanceof globalThis.Int32Array;
}
function IsUint32Array(value) {
  return value instanceof globalThis.Uint32Array;
}
function IsFloat32Array(value) {
  return value instanceof globalThis.Float32Array;
}
function IsFloat64Array(value) {
  return value instanceof globalThis.Float64Array;
}
function IsBigInt64Array(value) {
  return value instanceof globalThis.BigInt64Array;
}
function IsBigUint64Array(value) {
  return value instanceof globalThis.BigUint64Array;
}
function IsRegExp(value) {
  return value instanceof globalThis.RegExp;
}
function IsDate(value) {
  return value instanceof globalThis.Date;
}
function IsSet(value) {
  return value instanceof globalThis.Set;
}
function IsMap(value) {
  return value instanceof globalThis.Map;
}
var init_globals = __esm({
  "node_modules/typebox/build/guard/globals.mjs"() {
  }
});

// node_modules/typebox/build/guard/native.mjs
var init_native = __esm({
  "node_modules/typebox/build/guard/native.mjs"() {
    init_guard();
  }
});

// node_modules/typebox/build/guard/index.mjs
var init_guard2 = __esm({
  "node_modules/typebox/build/guard/index.mjs"() {
    init_emit();
    init_globals();
    init_native();
    init_guard();
    init_guard();
  }
});

// node_modules/typebox/build/system/settings/settings.mjs
var settings_exports = {};
__export(settings_exports, {
  Get: () => Get,
  Reset: () => Reset,
  Set: () => Set2
});
function Reset() {
  settings.immutableTypes = false;
  settings.maxErrors = 8;
  settings.maxInstantiationCount = 128;
  settings.useAcceleration = true;
  settings.exactOptionalPropertyTypes = false;
  settings.enumerableKind = false;
  settings.correctiveParse = false;
  settings.unionPrioritySort = true;
}
function Set2(options2) {
  for (const key of guard_exports.Keys(options2)) {
    const value = options2[key];
    if (value !== void 0) {
      Object.defineProperty(settings, key, { value });
    }
  }
}
function Get() {
  return settings;
}
var settings;
var init_settings = __esm({
  "node_modules/typebox/build/system/settings/settings.mjs"() {
    init_guard2();
    settings = {
      immutableTypes: false,
      maxErrors: 8,
      maxInstantiationCount: 128,
      useAcceleration: true,
      exactOptionalPropertyTypes: false,
      enumerableKind: false,
      correctiveParse: false,
      unionPrioritySort: true
    };
  }
});

// node_modules/typebox/build/system/settings/index.mjs
var init_settings2 = __esm({
  "node_modules/typebox/build/system/settings/index.mjs"() {
    init_settings();
  }
});

// node_modules/typebox/build/system/memory/freeze.mjs
function Freeze(value) {
  return settings_exports.Get().immutableTypes ? Object.freeze(value) : value;
}
var init_freeze = __esm({
  "node_modules/typebox/build/system/memory/freeze.mjs"() {
    init_settings2();
  }
});

// node_modules/typebox/build/system/memory/assign.mjs
function Assign(left, right) {
  Metrics.assign += 1;
  return Freeze({ ...left, ...right });
}
var init_assign = __esm({
  "node_modules/typebox/build/system/memory/assign.mjs"() {
    init_metrics();
    init_freeze();
  }
});

// node_modules/typebox/build/system/memory/clone.mjs
function FromClassInstance(value) {
  return value;
}
function IsSchemaObject(value) {
  return guard_exports.HasPropertyKey(value, "~kind") || guard_exports.HasPropertyKey(value, "~unsafe");
}
function FromSchemaObject(value) {
  const result = {};
  for (const key of guard_exports.Keys(value)) {
    if (guard_exports.IsUnsafePropertyKey(key))
      continue;
    const descriptor = Object.getOwnPropertyDescriptor(value, key);
    descriptor.value = FromValue(descriptor.value);
    if (guard_exports.IsEqual(descriptor.enumerable, true)) {
      result[key] = descriptor.value;
    } else {
      Object.defineProperty(result, key, descriptor);
    }
  }
  return result;
}
function FromPlainObject(value) {
  const result = {};
  for (const key of guard_exports.Keys(value)) {
    if (guard_exports.IsUnsafePropertyKey(key))
      continue;
    result[key] = FromValue(value[key]);
  }
  for (const key of guard_exports.Symbols(value)) {
    result[key] = FromValue(value[key]);
  }
  return result;
}
function FromObject(value) {
  return guard_exports.IsClassInstance(value) ? FromClassInstance(value) : IsSchemaObject(value) ? FromSchemaObject(value) : FromPlainObject(value);
}
function FromArray(value) {
  return value.map((element) => FromValue(element));
}
function FromTypedArray(value) {
  return value.slice();
}
function FromRegExp(value) {
  return new RegExp(value.source, value.flags);
}
function FromMap(value) {
  return new Map(FromValue([...value.entries()]));
}
function FromSet(value) {
  return new Set(FromValue([...value.values()]));
}
function FromValue(value) {
  return globals_exports.IsTypeArray(value) ? FromTypedArray(value) : globals_exports.IsRegExp(value) ? FromRegExp(value) : globals_exports.IsMap(value) ? FromMap(value) : globals_exports.IsSet(value) ? FromSet(value) : guard_exports.IsArray(value) ? FromArray(value) : guard_exports.IsObject(value) ? FromObject(value) : value;
}
function Clone(value) {
  Metrics.clone += 1;
  return FromValue(value);
}
var init_clone = __esm({
  "node_modules/typebox/build/system/memory/clone.mjs"() {
    init_guard2();
    init_metrics();
  }
});

// node_modules/typebox/build/system/memory/create.mjs
function MergeHidden(left, right) {
  for (const key of Object.keys(right)) {
    Object.defineProperty(left, key, {
      configurable: true,
      writable: true,
      enumerable: false,
      value: right[key]
    });
  }
  return left;
}
function Merge(left, right) {
  return { ...left, ...right };
}
function Create(hidden, enumerable, options2 = {}) {
  Metrics.create += 1;
  const withOptions = Merge(enumerable, options2);
  const withHidden = settings_exports.Get().enumerableKind ? Merge(withOptions, hidden) : MergeHidden(withOptions, hidden);
  return Freeze(withHidden);
}
var init_create = __esm({
  "node_modules/typebox/build/system/memory/create.mjs"() {
    init_settings2();
    init_metrics();
    init_freeze();
  }
});

// node_modules/typebox/build/system/memory/discard.mjs
function Discard(value, propertyKeys) {
  Metrics.discard += 1;
  const result = {};
  for (const key of guard_exports.Keys(value)) {
    if (propertyKeys.includes(key))
      continue;
    const descriptor = Object.getOwnPropertyDescriptor(value, key);
    descriptor.value = Clone(descriptor.value);
    Object.defineProperty(result, key, descriptor);
  }
  return Freeze(result);
}
var init_discard = __esm({
  "node_modules/typebox/build/system/memory/discard.mjs"() {
    init_guard2();
    init_metrics();
    init_freeze();
    init_clone();
  }
});

// node_modules/typebox/build/system/memory/update.mjs
function Update(current, hidden, enumerable) {
  Metrics.update += 1;
  const settings2 = settings_exports.Get();
  const result = Clone(current);
  for (const key of Object.keys(hidden)) {
    Object.defineProperty(result, key, {
      configurable: true,
      writable: true,
      enumerable: settings2.enumerableKind,
      value: hidden[key]
    });
  }
  for (const key of Object.keys(enumerable)) {
    Object.defineProperty(result, key, {
      configurable: true,
      enumerable: true,
      writable: true,
      value: enumerable[key]
    });
  }
  return Freeze(result);
}
var init_update = __esm({
  "node_modules/typebox/build/system/memory/update.mjs"() {
    init_settings2();
    init_metrics();
    init_freeze();
    init_clone();
  }
});

// node_modules/typebox/build/system/memory/memory.mjs
var memory_exports = {};
__export(memory_exports, {
  Assign: () => Assign,
  Clone: () => Clone,
  Create: () => Create,
  Discard: () => Discard,
  Metrics: () => Metrics,
  Update: () => Update
});
var init_memory = __esm({
  "node_modules/typebox/build/system/memory/memory.mjs"() {
    init_assign();
    init_clone();
    init_create();
    init_discard();
    init_metrics();
    init_update();
  }
});

// node_modules/typebox/build/system/memory/index.mjs
var init_memory2 = __esm({
  "node_modules/typebox/build/system/memory/index.mjs"() {
    init_memory();
  }
});

// node_modules/typebox/build/type/types/schema.mjs
function IsKind(value, kind) {
  return guard_exports.IsObject(value) && guard_exports.HasPropertyKey(value, "~kind") && guard_exports.IsEqual(value["~kind"], kind);
}
function IsSchema(value) {
  return guard_exports.IsObject(value);
}
var init_schema = __esm({
  "node_modules/typebox/build/type/types/schema.mjs"() {
    init_guard2();
  }
});

// node_modules/typebox/build/type/types/deferred.mjs
function Deferred(action, parameters, options2) {
  return memory_exports.Create({ "~kind": "Deferred" }, { type: "deferred", action, parameters, options: options2 }, {});
}
function IsDeferred(value) {
  return IsKind(value, "Deferred");
}
var init_deferred = __esm({
  "node_modules/typebox/build/type/types/deferred.mjs"() {
    init_memory2();
    init_schema();
  }
});

// node_modules/typebox/build/type/engine/readonly/instantiate_add.mjs
function AddReadonlyOperation(type) {
  return memory_exports.Update(type, { "~readonly": true }, {});
}
function AddReadonlyAction(type, options2) {
  const result = memory_exports.Update(AddReadonlyOperation(type), {}, options2);
  return result;
}
function AddReadonlyInstantiate(context, state, type, options2) {
  const instantiatedType = InstantiateType(context, state, type);
  return AddReadonlyAction(instantiatedType, options2);
}
var init_instantiate_add = __esm({
  "node_modules/typebox/build/type/engine/readonly/instantiate_add.mjs"() {
    init_memory2();
    init_instantiate27();
  }
});

// node_modules/typebox/build/type/engine/optional/instantiate_add.mjs
function AddOptionalOperation(type) {
  return memory_exports.Update(type, { "~optional": true }, {});
}
function AddOptionalAction(type, options2) {
  const result = memory_exports.Update(AddOptionalOperation(type), {}, options2);
  return result;
}
function AddOptionalInstantiate(context, state, type, options2) {
  const instantiatedType = InstantiateType(context, state, type);
  return AddOptionalAction(instantiatedType, options2);
}
var init_instantiate_add2 = __esm({
  "node_modules/typebox/build/type/engine/optional/instantiate_add.mjs"() {
    init_memory2();
    init_instantiate27();
  }
});

// node_modules/typebox/build/type/types/array.mjs
function _Array_(items, options2) {
  return memory_exports.Create({ "~kind": "Array" }, { type: "array", items }, options2);
}
function IsArray2(value) {
  return IsKind(value, "Array");
}
function ArrayOptions(type) {
  return memory_exports.Discard(type, ["~kind", "type", "items"]);
}
var init_array = __esm({
  "node_modules/typebox/build/type/types/array.mjs"() {
    init_memory2();
    init_schema();
  }
});

// node_modules/typebox/build/type/types/constructor.mjs
function Constructor(parameters, instanceType, options2 = {}) {
  return memory_exports.Create({ "~kind": "Constructor" }, { type: "constructor", parameters, instanceType }, options2);
}
function IsConstructor2(value) {
  return IsKind(value, "Constructor");
}
function ConstructorOptions(type) {
  return memory_exports.Discard(type, ["~kind", "type", "parameters", "instanceType"]);
}
var init_constructor = __esm({
  "node_modules/typebox/build/type/types/constructor.mjs"() {
    init_memory2();
    init_schema();
  }
});

// node_modules/typebox/build/type/types/function.mjs
function _Function_(parameters, returnType, options2 = {}) {
  return memory_exports.Create({ ["~kind"]: "Function" }, { type: "function", parameters, returnType }, options2);
}
function IsFunction2(value) {
  return IsKind(value, "Function");
}
function FunctionOptions(type) {
  return memory_exports.Discard(type, ["~kind", "type", "parameters", "returnType"]);
}
var init_function = __esm({
  "node_modules/typebox/build/type/types/function.mjs"() {
    init_memory2();
    init_schema();
  }
});

// node_modules/typebox/build/type/types/ref.mjs
function Ref(ref, options2) {
  return memory_exports.Create({ ["~kind"]: "Ref" }, { $ref: ref }, options2);
}
function IsRef(value) {
  return IsKind(value, "Ref");
}
var init_ref = __esm({
  "node_modules/typebox/build/type/types/ref.mjs"() {
    init_memory2();
    init_schema();
  }
});

// node_modules/typebox/build/type/types/generic.mjs
function Generic(parameters, expression) {
  return memory_exports.Create({ "~kind": "Generic" }, { type: "generic", parameters, expression });
}
function IsGeneric(value) {
  return IsKind(value, "Generic");
}
var init_generic = __esm({
  "node_modules/typebox/build/type/types/generic.mjs"() {
    init_memory2();
    init_schema();
  }
});

// node_modules/typebox/build/type/types/any.mjs
function Any(options2) {
  return memory_exports.Create({ ["~kind"]: "Any" }, {}, options2);
}
function IsAny(value) {
  return IsKind(value, "Any");
}
var init_any = __esm({
  "node_modules/typebox/build/type/types/any.mjs"() {
    init_memory2();
    init_schema();
  }
});

// node_modules/typebox/build/type/types/never.mjs
function Never(options2) {
  return memory_exports.Create({ "~kind": "Never" }, { not: {} }, options2);
}
function IsNever(value) {
  return IsKind(value, "Never");
}
var NeverPattern;
var init_never = __esm({
  "node_modules/typebox/build/type/types/never.mjs"() {
    init_memory2();
    init_schema();
    NeverPattern = "(?!)";
  }
});

// node_modules/typebox/build/type/action/_add_optional.mjs
function AddOptionalDeferred(type, options2 = {}) {
  return Deferred("AddOptional", [type], options2);
}
function AddOptional(type, options2 = {}) {
  return AddOptionalAction(type, options2);
}
var init_add_optional = __esm({
  "node_modules/typebox/build/type/action/_add_optional.mjs"() {
    init_deferred();
    init_instantiate_add2();
  }
});

// node_modules/typebox/build/type/types/_optional.mjs
function Optional(type) {
  return AddOptional(type);
}
function IsOptional(value) {
  return IsSchema(value) && guard_exports.HasPropertyKey(value, "~optional");
}
var init_optional = __esm({
  "node_modules/typebox/build/type/types/_optional.mjs"() {
    init_guard2();
    init_schema();
    init_add_optional();
  }
});

// node_modules/typebox/build/type/types/properties.mjs
function RequiredArray(properties) {
  return guard_exports.Keys(properties).filter((key) => !IsOptional(properties[key]));
}
function PropertyKeys(properties) {
  return guard_exports.Keys(properties);
}
function PropertyValues(properties) {
  return guard_exports.Values(properties);
}
var init_properties = __esm({
  "node_modules/typebox/build/type/types/properties.mjs"() {
    init_guard2();
    init_optional();
  }
});

// node_modules/typebox/build/type/types/object.mjs
function _Object_(properties, options2 = {}) {
  const requiredKeys = RequiredArray(properties);
  const required = requiredKeys.length > 0 ? { required: requiredKeys } : {};
  return memory_exports.Create({ "~kind": "Object" }, { type: "object", ...required, properties }, options2);
}
function IsObject2(value) {
  return IsKind(value, "Object");
}
function ObjectOptions(type) {
  return memory_exports.Discard(type, ["~kind", "type", "properties", "required"]);
}
var init_object = __esm({
  "node_modules/typebox/build/type/types/object.mjs"() {
    init_memory2();
    init_schema();
    init_properties();
  }
});

// node_modules/typebox/build/type/types/unknown.mjs
function Unknown(options2) {
  return memory_exports.Create({ ["~kind"]: "Unknown" }, {}, options2);
}
function IsUnknown(value) {
  return IsKind(value, "Unknown");
}
var init_unknown = __esm({
  "node_modules/typebox/build/type/types/unknown.mjs"() {
    init_memory2();
    init_schema();
  }
});

// node_modules/typebox/build/type/types/cyclic.mjs
function Cyclic($defs, $ref, options2) {
  const defs = guard_exports.Keys($defs).reduce((result, key) => {
    return { ...result, [key]: memory_exports.Update($defs[key], {}, { $id: key }) };
  }, {});
  return memory_exports.Create({ ["~kind"]: "Cyclic" }, { $defs: defs, $ref }, options2);
}
function IsCyclic(value) {
  return IsKind(value, "Cyclic");
}
var init_cyclic = __esm({
  "node_modules/typebox/build/type/types/cyclic.mjs"() {
    init_guard2();
    init_memory2();
    init_schema();
  }
});

// node_modules/typebox/build/type/types/unsafe.mjs
function Unsafe(schema) {
  return memory_exports.Update(schema, { ["~unsafe"]: null }, {});
}
function IsUnsafe(value) {
  return guard_exports.IsObjectNotArray(value) && guard_exports.HasPropertyKey(value, "~unsafe") && guard_exports.IsNull(value["~unsafe"]);
}
var init_unsafe = __esm({
  "node_modules/typebox/build/type/types/unsafe.mjs"() {
    init_guard2();
    init_memory2();
  }
});

// node_modules/typebox/build/system/arguments/arguments.mjs
var arguments_exports = {};
__export(arguments_exports, {
  Match: () => Match
});
function Match(args, match) {
  return match[args.length]?.(...args) ?? (() => {
    throw Error("Invalid Arguments");
  })();
}
var init_arguments = __esm({
  "node_modules/typebox/build/system/arguments/arguments.mjs"() {
  }
});

// node_modules/typebox/build/system/arguments/index.mjs
var init_arguments2 = __esm({
  "node_modules/typebox/build/system/arguments/index.mjs"() {
    init_arguments();
  }
});

// node_modules/typebox/build/type/types/infer.mjs
function Infer(...args) {
  const [name, extends_] = arguments_exports.Match(args, {
    2: (name2, extends_2) => [name2, extends_2, extends_2],
    1: (name2) => [name2, Unknown(), Unknown()]
  });
  return memory_exports.Create({ ["~kind"]: "Infer" }, { type: "infer", name, extends: extends_ }, {});
}
function IsInfer(value) {
  return IsKind(value, "Infer");
}
var init_infer = __esm({
  "node_modules/typebox/build/type/types/infer.mjs"() {
    init_arguments2();
    init_memory2();
    init_schema();
    init_unknown();
  }
});

// node_modules/typebox/build/type/types/dependent.mjs
function Dependent(if_, then_, else_, options2 = {}) {
  return memory_exports.Create({ "~kind": "Dependent" }, { if: if_, then: then_, else: else_ }, options2);
}
function IsDependent(value) {
  return IsKind(value, "Dependent");
}
function DependentOptions(type) {
  return memory_exports.Discard(type, ["~kind", "if", "then", "else"]);
}
var init_dependent = __esm({
  "node_modules/typebox/build/type/types/dependent.mjs"() {
    init_memory2();
    init_schema();
  }
});

// node_modules/typebox/build/type/engine/enum/typescript_enum_to_enum_values.mjs
function IsTypeScriptEnumLike(value) {
  return guard_exports.IsObjectNotArray(value);
}
function TypeScriptEnumToEnumValues(type) {
  const keys = guard_exports.Keys(type).filter((key) => isNaN(key));
  return keys.reduce((result, key) => [...result, type[key]], []);
}
var init_typescript_enum_to_enum_values = __esm({
  "node_modules/typebox/build/type/engine/enum/typescript_enum_to_enum_values.mjs"() {
    init_guard2();
  }
});

// node_modules/typebox/build/type/types/enum.mjs
function IsEnumValue(value) {
  return guard_exports.IsString(value) || guard_exports.IsNumber(value);
}
function Enum(value, options2) {
  const values = IsTypeScriptEnumLike(value) ? TypeScriptEnumToEnumValues(value) : value;
  return memory_exports.Create({ "~kind": "Enum" }, { enum: values }, options2);
}
function IsEnum(value) {
  return IsKind(value, "Enum");
}
var init_enum = __esm({
  "node_modules/typebox/build/type/types/enum.mjs"() {
    init_guard2();
    init_memory2();
    init_schema();
    init_typescript_enum_to_enum_values();
    init_typescript_enum_to_enum_values();
  }
});

// node_modules/typebox/build/type/types/intersect.mjs
function Intersect(types, options2 = {}) {
  return memory_exports.Create({ "~kind": "Intersect" }, { allOf: types }, options2);
}
function IsIntersect(value) {
  return IsKind(value, "Intersect");
}
function IntersectOptions(type) {
  return memory_exports.Discard(type, ["~kind", "allOf"]);
}
var init_intersect = __esm({
  "node_modules/typebox/build/type/types/intersect.mjs"() {
    init_memory2();
    init_schema();
  }
});

// node_modules/typebox/build/system/environment/evaluate.mjs
var init_evaluate = __esm({
  "node_modules/typebox/build/system/environment/evaluate.mjs"() {
    init_settings2();
    init_guard2();
  }
});

// node_modules/typebox/build/system/environment/environment.mjs
var init_environment = __esm({
  "node_modules/typebox/build/system/environment/environment.mjs"() {
    init_evaluate();
  }
});

// node_modules/typebox/build/system/environment/index.mjs
var init_environment2 = __esm({
  "node_modules/typebox/build/system/environment/index.mjs"() {
    init_environment();
  }
});

// node_modules/typebox/build/system/unreachable/unreachable.mjs
function Unreachable() {
  throw new Error("Unreachable");
}
var init_unreachable = __esm({
  "node_modules/typebox/build/system/unreachable/unreachable.mjs"() {
  }
});

// node_modules/typebox/build/system/unreachable/index.mjs
var init_unreachable2 = __esm({
  "node_modules/typebox/build/system/unreachable/index.mjs"() {
    init_unreachable();
  }
});

// node_modules/typebox/build/system/hashing/hash.mjs
var ByteMarker, Accumulator, Prime, Size, Bytes, F64, F64In, F64Out, encoder;
var init_hash = __esm({
  "node_modules/typebox/build/system/hashing/hash.mjs"() {
    init_unreachable2();
    init_guard2();
    (function(ByteMarker2) {
      ByteMarker2[ByteMarker2["Array"] = 0] = "Array";
      ByteMarker2[ByteMarker2["BigInt"] = 1] = "BigInt";
      ByteMarker2[ByteMarker2["Boolean"] = 2] = "Boolean";
      ByteMarker2[ByteMarker2["Date"] = 3] = "Date";
      ByteMarker2[ByteMarker2["Constructor"] = 4] = "Constructor";
      ByteMarker2[ByteMarker2["Function"] = 5] = "Function";
      ByteMarker2[ByteMarker2["Null"] = 6] = "Null";
      ByteMarker2[ByteMarker2["Number"] = 7] = "Number";
      ByteMarker2[ByteMarker2["Object"] = 8] = "Object";
      ByteMarker2[ByteMarker2["RegExp"] = 9] = "RegExp";
      ByteMarker2[ByteMarker2["String"] = 10] = "String";
      ByteMarker2[ByteMarker2["Symbol"] = 11] = "Symbol";
      ByteMarker2[ByteMarker2["TypeArray"] = 12] = "TypeArray";
      ByteMarker2[ByteMarker2["Undefined"] = 13] = "Undefined";
    })(ByteMarker || (ByteMarker = {}));
    Accumulator = BigInt("14695981039346656037");
    [Prime, Size] = [BigInt("1099511628211"), BigInt(
      "18446744073709551616"
      /* 2 ^ 64 */
    )];
    Bytes = Array.from({ length: 256 }).map((_, i) => BigInt(i));
    F64 = new Float64Array(1);
    F64In = new DataView(F64.buffer);
    F64Out = new Uint8Array(F64.buffer);
    encoder = new TextEncoder();
  }
});

// node_modules/typebox/build/system/hashing/index.mjs
var init_hashing = __esm({
  "node_modules/typebox/build/system/hashing/index.mjs"() {
    init_hash();
  }
});

// node_modules/typebox/build/system/locale/en_US.mjs
var init_en_US = __esm({
  "node_modules/typebox/build/system/locale/en_US.mjs"() {
  }
});

// node_modules/typebox/build/system/locale/_config.mjs
var init_config = __esm({
  "node_modules/typebox/build/system/locale/_config.mjs"() {
    init_en_US();
  }
});

// node_modules/typebox/build/system/locale/ar_001.mjs
var init_ar_001 = __esm({
  "node_modules/typebox/build/system/locale/ar_001.mjs"() {
  }
});

// node_modules/typebox/build/system/locale/bn_BD.mjs
var init_bn_BD = __esm({
  "node_modules/typebox/build/system/locale/bn_BD.mjs"() {
  }
});

// node_modules/typebox/build/system/locale/cs_CZ.mjs
var init_cs_CZ = __esm({
  "node_modules/typebox/build/system/locale/cs_CZ.mjs"() {
  }
});

// node_modules/typebox/build/system/locale/de_DE.mjs
var init_de_DE = __esm({
  "node_modules/typebox/build/system/locale/de_DE.mjs"() {
  }
});

// node_modules/typebox/build/system/locale/el_GR.mjs
var init_el_GR = __esm({
  "node_modules/typebox/build/system/locale/el_GR.mjs"() {
  }
});

// node_modules/typebox/build/system/locale/es_419.mjs
var init_es_419 = __esm({
  "node_modules/typebox/build/system/locale/es_419.mjs"() {
  }
});

// node_modules/typebox/build/system/locale/es_AR.mjs
var init_es_AR = __esm({
  "node_modules/typebox/build/system/locale/es_AR.mjs"() {
  }
});

// node_modules/typebox/build/system/locale/es_ES.mjs
var init_es_ES = __esm({
  "node_modules/typebox/build/system/locale/es_ES.mjs"() {
  }
});

// node_modules/typebox/build/system/locale/es_MX.mjs
var init_es_MX = __esm({
  "node_modules/typebox/build/system/locale/es_MX.mjs"() {
  }
});

// node_modules/typebox/build/system/locale/fa_IR.mjs
var init_fa_IR = __esm({
  "node_modules/typebox/build/system/locale/fa_IR.mjs"() {
  }
});

// node_modules/typebox/build/system/locale/fil_PH.mjs
var init_fil_PH = __esm({
  "node_modules/typebox/build/system/locale/fil_PH.mjs"() {
  }
});

// node_modules/typebox/build/system/locale/fr_CA.mjs
var init_fr_CA = __esm({
  "node_modules/typebox/build/system/locale/fr_CA.mjs"() {
  }
});

// node_modules/typebox/build/system/locale/fr_FR.mjs
var init_fr_FR = __esm({
  "node_modules/typebox/build/system/locale/fr_FR.mjs"() {
  }
});

// node_modules/typebox/build/system/locale/ha_NG.mjs
var init_ha_NG = __esm({
  "node_modules/typebox/build/system/locale/ha_NG.mjs"() {
  }
});

// node_modules/typebox/build/system/locale/hi_IN.mjs
var init_hi_IN = __esm({
  "node_modules/typebox/build/system/locale/hi_IN.mjs"() {
  }
});

// node_modules/typebox/build/system/locale/hu_HU.mjs
var init_hu_HU = __esm({
  "node_modules/typebox/build/system/locale/hu_HU.mjs"() {
  }
});

// node_modules/typebox/build/system/locale/id_ID.mjs
var init_id_ID = __esm({
  "node_modules/typebox/build/system/locale/id_ID.mjs"() {
  }
});

// node_modules/typebox/build/system/locale/it_IT.mjs
var init_it_IT = __esm({
  "node_modules/typebox/build/system/locale/it_IT.mjs"() {
  }
});

// node_modules/typebox/build/system/locale/ja_JP.mjs
var init_ja_JP = __esm({
  "node_modules/typebox/build/system/locale/ja_JP.mjs"() {
  }
});

// node_modules/typebox/build/system/locale/ko_KR.mjs
var init_ko_KR = __esm({
  "node_modules/typebox/build/system/locale/ko_KR.mjs"() {
  }
});

// node_modules/typebox/build/system/locale/ms_MY.mjs
var init_ms_MY = __esm({
  "node_modules/typebox/build/system/locale/ms_MY.mjs"() {
  }
});

// node_modules/typebox/build/system/locale/nl_NL.mjs
var init_nl_NL = __esm({
  "node_modules/typebox/build/system/locale/nl_NL.mjs"() {
  }
});

// node_modules/typebox/build/system/locale/pl_PL.mjs
var init_pl_PL = __esm({
  "node_modules/typebox/build/system/locale/pl_PL.mjs"() {
  }
});

// node_modules/typebox/build/system/locale/pt_BR.mjs
var init_pt_BR = __esm({
  "node_modules/typebox/build/system/locale/pt_BR.mjs"() {
  }
});

// node_modules/typebox/build/system/locale/pt_PT.mjs
var init_pt_PT = __esm({
  "node_modules/typebox/build/system/locale/pt_PT.mjs"() {
  }
});

// node_modules/typebox/build/system/locale/ro_RO.mjs
var init_ro_RO = __esm({
  "node_modules/typebox/build/system/locale/ro_RO.mjs"() {
  }
});

// node_modules/typebox/build/system/locale/ru_RU.mjs
var init_ru_RU = __esm({
  "node_modules/typebox/build/system/locale/ru_RU.mjs"() {
  }
});

// node_modules/typebox/build/system/locale/sv_SE.mjs
var init_sv_SE = __esm({
  "node_modules/typebox/build/system/locale/sv_SE.mjs"() {
  }
});

// node_modules/typebox/build/system/locale/sw_TZ.mjs
var init_sw_TZ = __esm({
  "node_modules/typebox/build/system/locale/sw_TZ.mjs"() {
  }
});

// node_modules/typebox/build/system/locale/th_TH.mjs
var init_th_TH = __esm({
  "node_modules/typebox/build/system/locale/th_TH.mjs"() {
  }
});

// node_modules/typebox/build/system/locale/tr_TR.mjs
var init_tr_TR = __esm({
  "node_modules/typebox/build/system/locale/tr_TR.mjs"() {
  }
});

// node_modules/typebox/build/system/locale/uk_UA.mjs
var init_uk_UA = __esm({
  "node_modules/typebox/build/system/locale/uk_UA.mjs"() {
  }
});

// node_modules/typebox/build/system/locale/ur_PK.mjs
var init_ur_PK = __esm({
  "node_modules/typebox/build/system/locale/ur_PK.mjs"() {
  }
});

// node_modules/typebox/build/system/locale/vi_VN.mjs
var init_vi_VN = __esm({
  "node_modules/typebox/build/system/locale/vi_VN.mjs"() {
  }
});

// node_modules/typebox/build/system/locale/yo_NG.mjs
var init_yo_NG = __esm({
  "node_modules/typebox/build/system/locale/yo_NG.mjs"() {
  }
});

// node_modules/typebox/build/system/locale/zh_Hans.mjs
var init_zh_Hans = __esm({
  "node_modules/typebox/build/system/locale/zh_Hans.mjs"() {
  }
});

// node_modules/typebox/build/system/locale/zh_Hant.mjs
var init_zh_Hant = __esm({
  "node_modules/typebox/build/system/locale/zh_Hant.mjs"() {
  }
});

// node_modules/typebox/build/system/locale/_locale.mjs
var init_locale = __esm({
  "node_modules/typebox/build/system/locale/_locale.mjs"() {
    init_config();
    init_ar_001();
    init_bn_BD();
    init_cs_CZ();
    init_de_DE();
    init_el_GR();
    init_en_US();
    init_es_419();
    init_es_AR();
    init_es_ES();
    init_es_MX();
    init_fa_IR();
    init_fil_PH();
    init_fr_CA();
    init_fr_CA();
    init_fr_FR();
    init_ha_NG();
    init_hi_IN();
    init_hu_HU();
    init_id_ID();
    init_it_IT();
    init_ja_JP();
    init_ko_KR();
    init_ms_MY();
    init_nl_NL();
    init_pl_PL();
    init_pt_BR();
    init_pt_PT();
    init_ro_RO();
    init_ru_RU();
    init_sv_SE();
    init_sw_TZ();
    init_th_TH();
    init_tr_TR();
    init_uk_UA();
    init_ur_PK();
    init_vi_VN();
    init_yo_NG();
    init_zh_Hans();
    init_zh_Hant();
  }
});

// node_modules/typebox/build/system/locale/index.mjs
var init_locale2 = __esm({
  "node_modules/typebox/build/system/locale/index.mjs"() {
    init_locale();
  }
});

// node_modules/typebox/build/system/system.mjs
var init_system = __esm({
  "node_modules/typebox/build/system/system.mjs"() {
    init_arguments2();
    init_environment2();
    init_hashing();
    init_locale2();
    init_memory2();
    init_settings2();
  }
});

// node_modules/typebox/build/system/index.mjs
var init_system2 = __esm({
  "node_modules/typebox/build/system/index.mjs"() {
    init_system();
    init_system();
    init_system();
  }
});

// node_modules/typebox/build/type/types/_codec.mjs
function Codec(type) {
  return new DecodeBuilder(type);
}
function Decode(type, callback) {
  return Codec(type).Decode(callback).Encode(() => {
    throw Error("Encode not implemented");
  });
}
function Encode(type, callback) {
  return Codec(type).Decode(() => {
    throw Error("Decode not implemented");
  }).Encode(callback);
}
function IsCodec(value) {
  return IsSchema(value) && guard_exports.HasPropertyKey(value, "~codec") && guard_exports.IsObject(value["~codec"]) && guard_exports.HasPropertyKey(value["~codec"], "encode") && guard_exports.HasPropertyKey(value["~codec"], "decode");
}
var EncodeBuilder, DecodeBuilder;
var init_codec = __esm({
  "node_modules/typebox/build/type/types/_codec.mjs"() {
    init_system2();
    init_guard2();
    init_schema();
    EncodeBuilder = class {
      constructor(type, decode) {
        this.type = type;
        this.decode = decode;
      }
      Encode(callback) {
        const type = this.type;
        const decode = IsCodec(type) ? (value) => this.decode(type["~codec"].decode(value)) : this.decode;
        const encode = IsCodec(type) ? (value) => type["~codec"].encode(callback(value)) : callback;
        const codec = { decode, encode };
        return memory_exports.Update(this.type, { "~codec": codec }, {});
      }
    };
    DecodeBuilder = class {
      constructor(type) {
        this.type = type;
      }
      Decode(callback) {
        return new EncodeBuilder(this.type, callback);
      }
    };
  }
});

// node_modules/typebox/build/type/types/_immutable.mjs
function Immutable(type) {
  return AddImmutable(type);
}
function IsImmutable(value) {
  return IsSchema(value) && guard_exports.HasPropertyKey(value, "~immutable");
}
var init_immutable = __esm({
  "node_modules/typebox/build/type/types/_immutable.mjs"() {
    init_guard2();
    init_schema();
    init_add_immutable();
  }
});

// node_modules/typebox/build/type/action/_add_readonly.mjs
function AddReadonlyDeferred(type, options2 = {}) {
  return Deferred("AddReadonly", [type], options2);
}
function AddReadonly(type, options2 = {}) {
  return AddReadonlyAction(type, options2);
}
var init_add_readonly = __esm({
  "node_modules/typebox/build/type/action/_add_readonly.mjs"() {
    init_deferred();
    init_instantiate_add();
  }
});

// node_modules/typebox/build/type/types/_readonly.mjs
function Readonly(type) {
  return AddReadonly(type);
}
function IsReadonly(value) {
  return IsSchema(value) && guard_exports.HasPropertyKey(value, "~readonly");
}
var init_readonly = __esm({
  "node_modules/typebox/build/type/types/_readonly.mjs"() {
    init_guard2();
    init_schema();
    init_add_readonly();
  }
});

// node_modules/typebox/build/type/types/_refine.mjs
function RefineAdd(type, refinement) {
  const refinements = IsRefine(type) ? [...type["~refine"], refinement] : [refinement];
  return memory_exports.Update(type, { "~refine": refinements }, {});
}
function Refine(...args) {
  const [type, check, error] = arguments_exports.Match(args, {
    3: (type2, check2, error2) => [type2, check2, error2],
    2: (type2, check2) => [type2, check2, () => "Refine Error"]
  });
  return RefineAdd(type, { check, error });
}
function IsRefinement(value) {
  return guard_exports.IsObjectNotArray(value) && guard_exports.HasPropertyKey(value, "check") && guard_exports.HasPropertyKey(value, "error") && guard_exports.IsFunction(value.check) && guard_exports.IsFunction(value.error);
}
function IsRefine(value) {
  return IsSchema(value) && guard_exports.HasPropertyKey(value, "~refine") && guard_exports.IsArray(value["~refine"]) && guard_exports.Every(value["~refine"], 0, (value2) => IsRefinement(value2));
}
var init_refine = __esm({
  "node_modules/typebox/build/type/types/_refine.mjs"() {
    init_arguments2();
    init_memory2();
    init_guard2();
    init_schema();
  }
});

// node_modules/typebox/build/type/types/bigint.mjs
function BigInt2(options2) {
  return memory_exports.Create({ "~kind": "BigInt" }, { type: "bigint" }, options2);
}
function IsBigInt2(value) {
  return IsKind(value, "BigInt");
}
var BigIntPattern;
var init_bigint = __esm({
  "node_modules/typebox/build/type/types/bigint.mjs"() {
    init_memory2();
    init_schema();
    BigIntPattern = "-?(?:0|[1-9][0-9]*)n";
  }
});

// node_modules/typebox/build/type/types/boolean.mjs
function Boolean2(options2) {
  return memory_exports.Create({ "~kind": "Boolean" }, { type: "boolean" }, options2);
}
function IsBoolean3(value) {
  return IsKind(value, "Boolean");
}
var init_boolean = __esm({
  "node_modules/typebox/build/type/types/boolean.mjs"() {
    init_memory2();
    init_schema();
  }
});

// node_modules/typebox/build/type/types/identifier.mjs
function Identifier(name) {
  return memory_exports.Create({ "~kind": "Identifier" }, { name });
}
function IsIdentifier(value) {
  return IsKind(value, "Identifier");
}
var init_identifier = __esm({
  "node_modules/typebox/build/type/types/identifier.mjs"() {
    init_memory2();
    init_schema();
  }
});

// node_modules/typebox/build/type/types/integer.mjs
function Integer(options2) {
  return memory_exports.Create({ "~kind": "Integer" }, { type: "integer" }, options2);
}
function IsInteger2(value) {
  return IsKind(value, "Integer");
}
var IntegerPattern;
var init_integer = __esm({
  "node_modules/typebox/build/type/types/integer.mjs"() {
    init_memory2();
    init_schema();
    IntegerPattern = "-?(?:0|[1-9][0-9]*)";
  }
});

// node_modules/typebox/build/type/types/literal.mjs
function LiteralTypeName(value) {
  return guard_exports.IsBigInt(value) ? "bigint" : guard_exports.IsBoolean(value) ? "boolean" : guard_exports.IsNumber(value) ? "number" : guard_exports.IsString(value) ? "string" : (() => {
    throw new InvalidLiteralValue(value);
  })();
}
function Literal(value, options2) {
  return memory_exports.Create({ "~kind": "Literal" }, { type: LiteralTypeName(value), const: value }, options2);
}
function IsLiteralValue(value) {
  return guard_exports.IsBigInt(value) || guard_exports.IsBoolean(value) || guard_exports.IsNumber(value) || guard_exports.IsString(value);
}
function IsLiteralNumber(value) {
  return IsLiteral(value) && guard_exports.IsNumber(value.const);
}
function IsLiteralString(value) {
  return IsLiteral(value) && guard_exports.IsString(value.const);
}
function IsLiteral(value) {
  return IsKind(value, "Literal");
}
var InvalidLiteralValue;
var init_literal = __esm({
  "node_modules/typebox/build/type/types/literal.mjs"() {
    init_memory2();
    init_guard2();
    init_schema();
    InvalidLiteralValue = class extends Error {
      constructor(value) {
        super(`Invalid Literal value`);
        Object.defineProperty(this, "cause", {
          value: { value },
          writable: false,
          configurable: false,
          enumerable: false
        });
      }
    };
  }
});

// node_modules/typebox/build/type/types/null.mjs
function Null(options2) {
  return memory_exports.Create({ "~kind": "Null" }, { type: "null" }, options2);
}
function IsNull2(value) {
  return IsKind(value, "Null");
}
var init_null = __esm({
  "node_modules/typebox/build/type/types/null.mjs"() {
    init_memory2();
    init_schema();
  }
});

// node_modules/typebox/build/type/types/number.mjs
function Number2(options2) {
  return memory_exports.Create({ "~kind": "Number" }, { type: "number" }, options2);
}
function IsNumber3(value) {
  return IsKind(value, "Number");
}
var NumberPattern;
var init_number = __esm({
  "node_modules/typebox/build/type/types/number.mjs"() {
    init_memory2();
    init_schema();
    NumberPattern = "-?(?:0|[1-9][0-9]*)(?:\\.[0-9]+)?";
  }
});

// node_modules/typebox/build/type/types/symbol.mjs
function Symbol2(options2) {
  return memory_exports.Create({ "~kind": "Symbol" }, { type: "symbol" }, options2);
}
function IsSymbol2(value) {
  return IsKind(value, "Symbol");
}
var init_symbol = __esm({
  "node_modules/typebox/build/type/types/symbol.mjs"() {
    init_memory2();
    init_schema();
  }
});

// node_modules/typebox/build/type/types/parameter.mjs
function Parameter(...args) {
  const [name, extends_, equals] = arguments_exports.Match(args, {
    3: (name2, extends_2, equals2) => [name2, extends_2, equals2],
    2: (name2, extends_2) => [name2, extends_2, extends_2],
    1: (name2) => [name2, Unknown(), Unknown()]
  });
  return memory_exports.Create({ "~kind": "Parameter" }, { name, extends: extends_, equals }, {});
}
function IsParameter(value) {
  return IsKind(value, "Parameter");
}
var init_parameter = __esm({
  "node_modules/typebox/build/type/types/parameter.mjs"() {
    init_arguments2();
    init_memory2();
    init_schema();
    init_unknown();
  }
});

// node_modules/typebox/build/type/types/string.mjs
function String2(options2) {
  return memory_exports.Create({ "~kind": "String" }, { type: "string" }, options2);
}
function IsString3(value) {
  return IsKind(value, "String");
}
var StringPattern;
var init_string2 = __esm({
  "node_modules/typebox/build/type/types/string.mjs"() {
    init_memory2();
    init_schema();
    StringPattern = ".*";
  }
});

// node_modules/typebox/build/type/types/union.mjs
function Union(anyOf, options2 = {}) {
  return memory_exports.Create({ "~kind": "Union" }, { anyOf }, options2);
}
function IsUnion(value) {
  return IsKind(value, "Union");
}
function UnionOptions(type) {
  return memory_exports.Discard(type, ["~kind", "anyOf"]);
}
var init_union = __esm({
  "node_modules/typebox/build/type/types/union.mjs"() {
    init_memory2();
    init_schema();
  }
});

// node_modules/typebox/build/type/engine/patterns/pattern.mjs
function ParsePatternIntoTypes(pattern) {
  const parsed = Pattern(pattern);
  const result = guard_exports.IsEqual(parsed.length, 2) ? parsed[0] : [];
  return result;
}
var init_pattern = __esm({
  "node_modules/typebox/build/type/engine/patterns/pattern.mjs"() {
    init_guard2();
    init_parser();
  }
});

// node_modules/typebox/build/type/engine/template_literal/is_finite.mjs
function FromLiteral(_value) {
  return true;
}
function FromTypesReduce(types) {
  return guard_exports.ShiftLeft(types, (left, right) => FromType(left) ? FromTypesReduce(right) : false, () => true);
}
function FromTypes(types) {
  const result = guard_exports.IsEqual(types.length, 0) ? false : FromTypesReduce(types);
  return result;
}
function FromType(type) {
  return IsUnion(type) ? FromTypes(type.anyOf) : IsLiteral(type) ? FromLiteral(type.const) : false;
}
function IsTemplateLiteralFinite(types) {
  const result = FromTypes(types);
  return result;
}
var init_is_finite = __esm({
  "node_modules/typebox/build/type/engine/template_literal/is_finite.mjs"() {
    init_guard2();
    init_literal();
    init_union();
  }
});

// node_modules/typebox/build/type/engine/template_literal/create.mjs
function TemplateLiteralCreate(pattern) {
  return memory_exports.Create({ ["~kind"]: "TemplateLiteral" }, { type: "string", pattern }, {});
}
var init_create2 = __esm({
  "node_modules/typebox/build/type/engine/template_literal/create.mjs"() {
    init_memory2();
  }
});

// node_modules/typebox/build/type/engine/template_literal/decode.mjs
function FromLiteralPush(variants, value, result = []) {
  return guard_exports.ShiftLeft(variants, (left, right) => FromLiteralPush(right, value, [...result, `${left}${value}`]), () => result);
}
function FromLiteral2(variants, value) {
  return guard_exports.IsEqual(variants.length, 0) ? [`${value}`] : FromLiteralPush(variants, value);
}
function FromUnion(variants, types, result = []) {
  return guard_exports.ShiftLeft(types, (left, right) => FromUnion(variants, right, [...result, ...FromType2(variants, left)]), () => result);
}
function FromType2(variants, type) {
  const result = IsUnion(type) ? FromUnion(variants, type.anyOf) : IsLiteral(type) ? FromLiteral2(variants, type.const) : Unreachable();
  return result;
}
function DecodeFromSpan(variants, types) {
  return guard_exports.ShiftLeft(types, (left, right) => DecodeFromSpan(FromType2(variants, left), right), () => variants);
}
function VariantsToLiterals(variants) {
  return variants.map((variant) => Literal(variant));
}
function DecodeTypesAsUnion(types) {
  const variants = DecodeFromSpan([], types);
  const literals = VariantsToLiterals(variants);
  const result = Union(literals);
  return result;
}
function DecodeTypes(types) {
  return guard_exports.IsEqual(types.length, 0) ? Unreachable() : (
    // Literal('') :
    guard_exports.IsEqual(types.length, 1) && IsLiteral(types[0]) ? types[0] : DecodeTypesAsUnion(types)
  );
}
function TemplateLiteralDecodeUnsafe(pattern) {
  const types = ParsePatternIntoTypes(pattern);
  const result = guard_exports.IsEqual(types.length, 0) ? String2() : IsTemplateLiteralFinite(types) ? DecodeTypes(types) : TemplateLiteralCreate(pattern);
  return result;
}
function TemplateLiteralDecode(pattern) {
  const decoded = TemplateLiteralDecodeUnsafe(pattern);
  const result = IsTemplateLiteral(decoded) ? String2() : decoded;
  return result;
}
var init_decode = __esm({
  "node_modules/typebox/build/type/engine/template_literal/decode.mjs"() {
    init_guard2();
    init_unreachable2();
    init_literal();
    init_string2();
    init_template_literal();
    init_union();
    init_pattern();
    init_is_finite();
    init_create2();
  }
});

// node_modules/typebox/build/type/engine/record/record_create.mjs
function CreateRecord(key, value) {
  const type = "object";
  const patternProperties = { [key]: value };
  return memory_exports.Create({ ["~kind"]: "Record" }, { type, patternProperties });
}
var init_record_create = __esm({
  "node_modules/typebox/build/type/engine/record/record_create.mjs"() {
    init_memory2();
  }
});

// node_modules/typebox/build/type/engine/record/from_key_any.mjs
function FromAnyKey(value) {
  return CreateRecord(StringKey, value);
}
var init_from_key_any = __esm({
  "node_modules/typebox/build/type/engine/record/from_key_any.mjs"() {
    init_record();
    init_record_create();
  }
});

// node_modules/typebox/build/type/engine/record/from_key_boolean.mjs
function FromBooleanKey(value) {
  return _Object_({ true: value, false: value });
}
var init_from_key_boolean = __esm({
  "node_modules/typebox/build/type/engine/record/from_key_boolean.mjs"() {
    init_object();
  }
});

// node_modules/typebox/build/type/types/tuple.mjs
function Tuple(types, options2 = {}) {
  const [items, minItems, additionalItems] = [types, types.length, false];
  return memory_exports.Create({ ["~kind"]: "Tuple" }, { type: "array", additionalItems, items, minItems }, options2);
}
function IsTuple(value) {
  return IsKind(value, "Tuple");
}
function TupleOptions(type) {
  return memory_exports.Discard(type, ["~kind", "type", "items", "minItems", "additionalItems"]);
}
var init_tuple = __esm({
  "node_modules/typebox/build/type/types/tuple.mjs"() {
    init_memory2();
    init_schema();
  }
});

// node_modules/typebox/build/type/engine/readonly/instantiate_remove.mjs
function RemoveReadonlyOperation(type) {
  return memory_exports.Discard(type, ["~readonly"]);
}
function RemoveReadonlyAction(type, options2) {
  const result = memory_exports.Update(RemoveReadonlyOperation(type), {}, options2);
  return result;
}
function RemoveReadonlyInstantiate(context, state, type, options2) {
  const instantiatedType = InstantiateType(context, state, type);
  return RemoveReadonlyAction(instantiatedType, options2);
}
var init_instantiate_remove = __esm({
  "node_modules/typebox/build/type/engine/readonly/instantiate_remove.mjs"() {
    init_memory2();
    init_instantiate27();
  }
});

// node_modules/typebox/build/type/action/_remove_readonly.mjs
function RemoveReadonlyDeferred(type, options2 = {}) {
  return Deferred("RemoveReadonly", [type], options2);
}
function RemoveReadonly(type, options2 = {}) {
  return RemoveReadonlyAction(type, options2);
}
var init_remove_readonly = __esm({
  "node_modules/typebox/build/type/action/_remove_readonly.mjs"() {
    init_deferred();
    init_instantiate_remove();
  }
});

// node_modules/typebox/build/type/engine/optional/instantiate_remove.mjs
function RemoveOptionalOperation(type) {
  return memory_exports.Discard(type, ["~optional"]);
}
function RemoveOptionalAction(type, options2) {
  const result = memory_exports.Update(RemoveOptionalOperation(type), {}, options2);
  return result;
}
function RemoveOptionalInstantiate(context, state, type, options2) {
  const instantiatedType = InstantiateType(context, state, type);
  return RemoveOptionalAction(instantiatedType, options2);
}
var init_instantiate_remove2 = __esm({
  "node_modules/typebox/build/type/engine/optional/instantiate_remove.mjs"() {
    init_memory2();
    init_instantiate27();
  }
});

// node_modules/typebox/build/type/action/_remove_optional.mjs
function RemoveOptionalDeferred(type, options2 = {}) {
  return Deferred("RemoveOptional", [type], options2);
}
function RemoveOptional(type, options2 = {}) {
  return RemoveOptionalAction(type, options2);
}
var init_remove_optional = __esm({
  "node_modules/typebox/build/type/action/_remove_optional.mjs"() {
    init_deferred();
    init_instantiate_remove2();
  }
});

// node_modules/typebox/build/type/engine/tuple/to_object.mjs
function TupleElementsToProperties(types) {
  const result = types.reduceRight((result2, right, index) => {
    return { [index]: right, ...result2 };
  }, {});
  return result;
}
function TupleToObject(type) {
  const properties = TupleElementsToProperties(type.items);
  const result = _Object_(properties);
  return result;
}
var init_to_object = __esm({
  "node_modules/typebox/build/type/engine/tuple/to_object.mjs"() {
    init_object();
  }
});

// node_modules/typebox/build/type/engine/evaluate/composite.mjs
function CanComposite(type) {
  return IsObject2(type) || IsTuple(type);
}
function IsReadonlyProperty(left, right) {
  return IsReadonly(left) ? IsReadonly(right) ? true : false : false;
}
function IsOptionalProperty(left, right) {
  return IsOptional(left) ? IsOptional(right) ? true : false : false;
}
function CompositeProperty(left, right) {
  const isReadonly = IsReadonlyProperty(left, right);
  const isOptional = IsOptionalProperty(left, right);
  const evaluated = EvaluateIntersect([left, right]);
  const property = RemoveReadonly(RemoveOptional(evaluated));
  return isReadonly && isOptional ? AddReadonly(AddOptional(property)) : isReadonly && !isOptional ? AddReadonly(property) : !isReadonly && isOptional ? AddOptional(property) : property;
}
function CompositePropertyKey(left, right, key) {
  return key in left ? key in right ? CompositeProperty(left[key], right[key]) : left[key] : key in right ? right[key] : Never();
}
function CompositeProperties(left, right) {
  const keys = /* @__PURE__ */ new Set([...guard_exports.Keys(left), ...guard_exports.Keys(right)]);
  const result = [...keys].reduce((result2, key) => {
    return { ...result2, [key]: CompositePropertyKey(left, right, key) };
  }, {});
  return result;
}
function GetProperties(type) {
  const result = IsObject2(type) ? type.properties : IsTuple(type) ? TupleElementsToProperties(type.items) : {};
  return result;
}
function Composite(left, right) {
  const leftProperties = GetProperties(left);
  const rightProperties = GetProperties(right);
  const properties = CompositeProperties(leftProperties, rightProperties);
  const result = _Object_(properties);
  return result;
}
var init_composite = __esm({
  "node_modules/typebox/build/type/engine/evaluate/composite.mjs"() {
    init_guard2();
    init_readonly();
    init_optional();
    init_object();
    init_never();
    init_tuple();
    init_add_readonly();
    init_add_optional();
    init_remove_readonly();
    init_remove_optional();
    init_to_object();
    init_evaluate2();
  }
});

// node_modules/typebox/build/type/engine/evaluate/narrow.mjs
function NarrowCompareRule(left, right) {
  const result = Compare(left, right);
  return guard_exports.IsEqual(result, CompareResultLeftInside) ? left : guard_exports.IsEqual(result, CompareResultRightInside) ? right : guard_exports.IsEqual(result, CompareResultEqual) ? right : Never();
}
function NarrowCompositeRule(left, right) {
  const canCompositeLeft = CanComposite(left);
  const canCompositeRight = CanComposite(right);
  return canCompositeLeft && canCompositeRight ? Composite(left, right) : canCompositeLeft && !canCompositeRight ? left : !canCompositeLeft && canCompositeRight ? right : NarrowCompareRule(left, right);
}
function Narrow(left, right) {
  return IsNever(left) ? left : IsAny(left) ? left : IsUnknown(left) ? right : IsNever(right) ? right : IsAny(right) ? right : IsUnknown(right) ? left : NarrowCompositeRule(left, right);
}
var init_narrow = __esm({
  "node_modules/typebox/build/type/engine/evaluate/narrow.mjs"() {
    init_guard2();
    init_any();
    init_never();
    init_unknown();
    init_compare();
    init_composite();
  }
});

// node_modules/typebox/build/type/engine/evaluate/distribute.mjs
function ShouldEvaluate(left, right) {
  const result = IsUnion(left) || IsUnion(right);
  return result;
}
function DistributeOperation(left, right) {
  const evaluatedLeft = EvaluateType(left);
  const evaluatedRight = EvaluateType(right);
  const shouldEvaluate = ShouldEvaluate(evaluatedLeft, evaluatedRight);
  const result = shouldEvaluate ? EvaluateIntersect([evaluatedLeft, evaluatedRight]) : Narrow(evaluatedLeft, evaluatedRight);
  return result;
}
function DistributeType(type, types, result = []) {
  return guard_exports.ShiftLeft(types, (left, right) => DistributeType(type, right, [...result, DistributeOperation(left, type)]), () => guard_exports.IsEqual(result.length, 0) ? [type] : result);
}
function DistributeUnion(types, distribution, result = []) {
  return guard_exports.ShiftLeft(types, (left, right) => DistributeUnion(right, distribution, [...result, ...Distribute([left], distribution)]), () => result);
}
function Distribute(types, result = []) {
  return guard_exports.ShiftLeft(types, (left, right) => IsUnion(left) ? Distribute(right, DistributeUnion(left.anyOf, result)) : Distribute(right, DistributeType(left, result)), () => result);
}
var init_distribute = __esm({
  "node_modules/typebox/build/type/engine/evaluate/distribute.mjs"() {
    init_guard2();
    init_union();
    init_narrow();
    init_evaluate2();
    init_evaluate2();
  }
});

// node_modules/typebox/build/type/engine/exclude/operation.mjs
function ExcludeType(left, right) {
  const check = Extends({}, left, right);
  const result = result_exports.IsExtendsTrueLike(check) ? [] : [left];
  return result;
}
function ExcludeUnion(left, right, result = []) {
  return guard_exports.ShiftLeft(left, (head, tail) => ExcludeUnion(tail, right, [...result, ...ExcludeType(head, right)]), () => result);
}
function ExcludeOperation(left, right) {
  const evaluated = EvaluateType(left);
  const canonical = IsUnion(evaluated) ? evaluated.anyOf : [evaluated];
  const remaining = ExcludeUnion(canonical, right);
  const result = EvaluateUnion(remaining);
  return result;
}
var init_operation = __esm({
  "node_modules/typebox/build/type/engine/exclude/operation.mjs"() {
    init_guard2();
    init_union();
    init_extends3();
    init_evaluate2();
    init_evaluate2();
  }
});

// node_modules/typebox/build/type/engine/evaluate/evaluate.mjs
function EvaluateDependent(if_, then_, else_) {
  const intersected = EvaluateIntersect([if_, then_]);
  const excluded = ExcludeOperation(else_, if_);
  const result = EvaluateUnion([intersected, excluded]);
  return result;
}
function EvaluateEnum(values, result = []) {
  return guard_exports.ShiftLeft(values, (left, right) => EvaluateEnum(right, [...result, Literal(left)]), () => EvaluateUnion(result));
}
function EvaluateIntersect(types) {
  const distribution = Distribute(types);
  const broadend = Broaden(distribution);
  const result = EvaluateUnion(broadend);
  return result;
}
function EvaluateTemplateLiteral(pattern) {
  const evaluated = TemplateLiteralDecode(pattern);
  const result = EvaluateType(evaluated);
  return result;
}
function EvaluateUnion(types) {
  const broadend = Broaden(types);
  const result = EvaluateUnionFast(broadend);
  return result;
}
function EvaluateType(type) {
  const result = IsDependent(type) ? EvaluateDependent(type.if, type.then, type.else) : IsEnum(type) ? EvaluateEnum(type.enum) : IsIntersect(type) ? EvaluateIntersect(type.allOf) : IsTemplateLiteral(type) ? EvaluateTemplateLiteral(type.pattern) : IsUnion(type) ? EvaluateUnion(type.anyOf) : type;
  return result;
}
function EvaluateUnionFast(types) {
  const result = guard_exports.IsEqual(types.length, 1) ? types[0] : guard_exports.IsEqual(types.length, 0) ? Never() : Union(types);
  return result;
}
var init_evaluate2 = __esm({
  "node_modules/typebox/build/type/engine/evaluate/evaluate.mjs"() {
    init_guard2();
    init_dependent();
    init_enum();
    init_intersect();
    init_literal();
    init_never();
    init_template_literal();
    init_union();
    init_distribute();
    init_broaden();
    init_operation();
    init_decode();
  }
});

// node_modules/typebox/build/type/engine/record/from_key_enum.mjs
function FromEnumKey(values, value) {
  const unionKey = EvaluateEnum(values);
  const result = FromKey(unionKey, value);
  return result;
}
var init_from_key_enum = __esm({
  "node_modules/typebox/build/type/engine/record/from_key_enum.mjs"() {
    init_from_key();
    init_evaluate2();
  }
});

// node_modules/typebox/build/type/engine/record/from_key_integer.mjs
function FromIntegerKey(_key, value) {
  const result = CreateRecord(IntegerKey, value);
  return result;
}
var init_from_key_integer = __esm({
  "node_modules/typebox/build/type/engine/record/from_key_integer.mjs"() {
    init_record();
    init_record_create();
  }
});

// node_modules/typebox/build/type/engine/record/from_key_intersect.mjs
function FromIntersectKey(types, value) {
  const evaluatedKey = EvaluateIntersect(types);
  const result = FromKey(evaluatedKey, value);
  return result;
}
var init_from_key_intersect = __esm({
  "node_modules/typebox/build/type/engine/record/from_key_intersect.mjs"() {
    init_evaluate2();
    init_from_key();
  }
});

// node_modules/typebox/build/type/engine/record/from_key_literal.mjs
function FromLiteralKey(key, value) {
  return guard_exports.IsString(key) || guard_exports.IsNumber(key) ? _Object_({ [key]: value }) : guard_exports.IsEqual(key, false) ? _Object_({ false: value }) : guard_exports.IsEqual(key, true) ? _Object_({ true: value }) : _Object_({});
}
var init_from_key_literal = __esm({
  "node_modules/typebox/build/type/engine/record/from_key_literal.mjs"() {
    init_guard2();
    init_object();
  }
});

// node_modules/typebox/build/type/engine/record/from_key_number.mjs
function FromNumberKey(_key, value) {
  const result = CreateRecord(NumberKey, value);
  return result;
}
var init_from_key_number = __esm({
  "node_modules/typebox/build/type/engine/record/from_key_number.mjs"() {
    init_record();
    init_record_create();
  }
});

// node_modules/typebox/build/type/engine/record/from_key_string.mjs
function FromStringKey(key, value) {
  return guard_exports.HasPropertyKey(key, "pattern") && (guard_exports.IsString(key.pattern) || key.pattern instanceof RegExp) ? CreateRecord(key.pattern.toString(), value) : CreateRecord(StringKey, value);
}
var init_from_key_string = __esm({
  "node_modules/typebox/build/type/engine/record/from_key_string.mjs"() {
    init_guard2();
    init_record();
    init_record_create();
  }
});

// node_modules/typebox/build/type/engine/record/from_key_template_literal.mjs
function FromTemplateKey(pattern, value) {
  const types = ParsePatternIntoTypes(pattern);
  const finite = IsTemplateLiteralFinite(types);
  const result = finite ? FromKey(EvaluateTemplateLiteral(pattern), value) : CreateRecord(pattern, value);
  return result;
}
var init_from_key_template_literal = __esm({
  "node_modules/typebox/build/type/engine/record/from_key_template_literal.mjs"() {
    init_from_key();
    init_pattern();
    init_is_finite();
    init_evaluate2();
    init_record_create();
  }
});

// node_modules/typebox/build/type/engine/evaluate/flatten.mjs
function FlattenType(type) {
  const result = IsUnion(type) ? Flatten(type.anyOf) : [type];
  return result;
}
function Flatten(types, result = []) {
  return guard_exports.ShiftLeft(types, (left, right) => Flatten(right, [...result, ...FlattenType(left)]), () => result);
}
var init_flatten = __esm({
  "node_modules/typebox/build/type/engine/evaluate/flatten.mjs"() {
    init_guard2();
    init_union();
  }
});

// node_modules/typebox/build/type/engine/record/from_key_union.mjs
function StringOrNumberCheck(types) {
  return types.some((type) => IsString3(type) || IsNumber3(type) || IsInteger2(type));
}
function TryBuildRecord(types, value) {
  return guard_exports.IsEqual(StringOrNumberCheck(types), true) ? CreateRecord(StringKey, value) : void 0;
}
function CreateProperties(types, value) {
  return types.reduce((result, left) => {
    return IsLiteral(left) && (guard_exports.IsString(left.const) || guard_exports.IsNumber(left.const)) ? { ...result, [left.const]: value } : result;
  }, {});
}
function CreateObject(types, value) {
  const properties = CreateProperties(types, value);
  const result = _Object_(properties);
  return result;
}
function FromUnionKey(types, value) {
  const flattened = Flatten(types);
  const record = TryBuildRecord(flattened, value);
  return IsSchema(record) ? record : CreateObject(flattened, value);
}
var init_from_key_union = __esm({
  "node_modules/typebox/build/type/engine/record/from_key_union.mjs"() {
    init_guard2();
    init_schema();
    init_literal();
    init_number();
    init_integer();
    init_object();
    init_string2();
    init_record();
    init_flatten();
    init_record_create();
  }
});

// node_modules/typebox/build/type/engine/record/from_key.mjs
function FromKey(key, value) {
  const result = IsAny(key) ? FromAnyKey(value) : IsBoolean3(key) ? FromBooleanKey(value) : IsEnum(key) ? FromEnumKey(key.enum, value) : IsInteger2(key) ? FromIntegerKey(key, value) : IsIntersect(key) ? FromIntersectKey(key.allOf, value) : IsLiteral(key) ? FromLiteralKey(key.const, value) : IsNumber3(key) ? FromNumberKey(key, value) : IsUnion(key) ? FromUnionKey(key.anyOf, value) : IsString3(key) ? FromStringKey(key, value) : IsTemplateLiteral(key) ? FromTemplateKey(key.pattern, value) : _Object_({});
  return result;
}
var init_from_key = __esm({
  "node_modules/typebox/build/type/engine/record/from_key.mjs"() {
    init_any();
    init_boolean();
    init_enum();
    init_intersect();
    init_integer();
    init_literal();
    init_number();
    init_object();
    init_string2();
    init_template_literal();
    init_union();
    init_from_key_any();
    init_from_key_boolean();
    init_from_key_enum();
    init_from_key_integer();
    init_from_key_intersect();
    init_from_key_literal();
    init_from_key_number();
    init_from_key_string();
    init_from_key_template_literal();
    init_from_key_union();
  }
});

// node_modules/typebox/build/type/engine/record/instantiate.mjs
function RecordAction(key, value, options2) {
  const result = CanInstantiate([key]) ? memory_exports.Update(FromKey(key, value), {}, options2) : RecordDeferred(key, value, options2);
  return result;
}
function RecordInstantiate(context, state, key, value, options2) {
  const instantiatedKey = InstantiateType(context, state, key);
  const instantiatedValue = InstantiateType(context, state, value);
  return RecordAction(instantiatedKey, instantiatedValue, options2);
}
var init_instantiate = __esm({
  "node_modules/typebox/build/type/engine/record/instantiate.mjs"() {
    init_memory2();
    init_record();
    init_from_key();
    init_instantiate27();
  }
});

// node_modules/typebox/build/type/types/record.mjs
function RecordDeferred(key, value, options2 = {}) {
  return Deferred("Record", [key, value], options2);
}
function Record(key, value, options2 = {}) {
  return RecordAction(key, value, options2);
}
function RecordFromPattern(pattern, value) {
  return CreateRecord(pattern, value);
}
function RecordPatternToType(pattern) {
  const result = guard_exports.IsEqual(pattern, StringKey) ? String2() : guard_exports.IsEqual(pattern, IntegerKey) ? Integer() : guard_exports.IsEqual(pattern, NumberKey) ? Number2() : TemplateLiteralDecodeUnsafe(pattern);
  return result;
}
function RecordPattern(type) {
  return guard_exports.Keys(type.patternProperties)[0];
}
function RecordKey(type) {
  const pattern = RecordPattern(type);
  const result = RecordPatternToType(pattern);
  return result;
}
function RecordValue(type) {
  return type.patternProperties[RecordPattern(type)];
}
function IsRecord(value) {
  return IsKind(value, "Record");
}
var IntegerKey, NumberKey, StringKey;
var init_record = __esm({
  "node_modules/typebox/build/type/types/record.mjs"() {
    init_memory2();
    init_guard2();
    init_schema();
    init_integer();
    init_number();
    init_string2();
    init_deferred();
    init_decode();
    init_record_create();
    init_instantiate();
    IntegerKey = `^${IntegerPattern}$`;
    NumberKey = `^${NumberPattern}$`;
    StringKey = `^${StringPattern}$`;
  }
});

// node_modules/typebox/build/type/types/rest.mjs
function Rest(type) {
  return memory_exports.Create({ "~kind": "Rest" }, { type: "rest", items: type }, {});
}
function IsRest(value) {
  return IsKind(value, "Rest");
}
var init_rest = __esm({
  "node_modules/typebox/build/type/types/rest.mjs"() {
    init_memory2();
    init_schema();
  }
});

// node_modules/typebox/build/type/types/static.mjs
var init_static = __esm({
  "node_modules/typebox/build/type/types/static.mjs"() {
  }
});

// node_modules/typebox/build/type/types/this.mjs
function This(options2) {
  return memory_exports.Create({ ["~kind"]: "This" }, { $ref: "#" }, options2);
}
function IsThis(value) {
  return IsKind(value, "This");
}
var init_this = __esm({
  "node_modules/typebox/build/type/types/this.mjs"() {
    init_memory2();
    init_schema();
  }
});

// node_modules/typebox/build/type/types/undefined.mjs
function Undefined(options2) {
  return memory_exports.Create({ "~kind": "Undefined" }, { type: "undefined" }, options2);
}
function IsUndefined2(value) {
  return IsKind(value, "Undefined");
}
var init_undefined = __esm({
  "node_modules/typebox/build/type/types/undefined.mjs"() {
    init_memory2();
    init_schema();
  }
});

// node_modules/typebox/build/type/types/void.mjs
function Void(options2) {
  return memory_exports.Create({ "~kind": "Void" }, { type: "void" }, options2);
}
function IsVoid(value) {
  return IsKind(value, "Void");
}
var init_void = __esm({
  "node_modules/typebox/build/type/types/void.mjs"() {
    init_memory2();
    init_schema();
  }
});

// node_modules/typebox/build/type/types/index.mjs
var init_types = __esm({
  "node_modules/typebox/build/type/types/index.mjs"() {
    init_codec();
    init_immutable();
    init_optional();
    init_readonly();
    init_refine();
    init_any();
    init_array();
    init_bigint();
    init_boolean();
    init_call();
    init_constructor();
    init_cyclic();
    init_deferred();
    init_enum();
    init_function();
    init_generic();
    init_identifier();
    init_dependent();
    init_infer();
    init_integer();
    init_intersect();
    init_literal();
    init_never();
    init_null();
    init_number();
    init_unknown();
    init_symbol();
    init_object();
    init_parameter();
    init_properties();
    init_record();
    init_ref();
    init_rest();
    init_schema();
    init_static();
    init_string2();
    init_symbol();
    init_template_literal();
    init_this();
    init_tuple();
    init_undefined();
    init_union();
    init_unknown();
    init_unsafe();
    init_void();
  }
});

// node_modules/typebox/build/type/script/mapping.mjs
function IntrinsicOrCall(ref, parameters) {
  return guard_exports.IsEqual(ref, "Array") ? _Array_(parameters[0]) : guard_exports.IsEqual(ref, "Capitalize") ? CapitalizeDeferred(parameters[0]) : guard_exports.IsEqual(ref, "ConstructorParameters") ? ConstructorParametersDeferred(parameters[0]) : guard_exports.IsEqual(ref, "Evaluate") ? EvaluateDeferred(parameters[0]) : guard_exports.IsEqual(ref, "Exclude") ? ExcludeDeferred(parameters[0], parameters[1]) : guard_exports.IsEqual(ref, "Extract") ? ExtractDeferred(parameters[0], parameters[1]) : guard_exports.IsEqual(ref, "Index") ? IndexDeferred(parameters[0], parameters[1]) : guard_exports.IsEqual(ref, "InstanceType") ? InstanceTypeDeferred(parameters[0]) : guard_exports.IsEqual(ref, "Lowercase") ? LowercaseDeferred(parameters[0]) : guard_exports.IsEqual(ref, "NonNullable") ? NonNullableDeferred(parameters[0]) : guard_exports.IsEqual(ref, "Omit") ? OmitDeferred(parameters[0], parameters[1]) : guard_exports.IsEqual(ref, "Parameters") ? ParametersDeferred(parameters[0]) : guard_exports.IsEqual(ref, "Partial") ? PartialDeferred(parameters[0]) : guard_exports.IsEqual(ref, "Pick") ? PickDeferred(parameters[0], parameters[1]) : guard_exports.IsEqual(ref, "Readonly") ? ReadonlyObjectDeferred(parameters[0]) : guard_exports.IsEqual(ref, "KeyOf") ? KeyOfDeferred(parameters[0]) : guard_exports.IsEqual(ref, "Record") ? RecordDeferred(parameters[0], parameters[1]) : guard_exports.IsEqual(ref, "Required") ? RequiredDeferred(parameters[0]) : guard_exports.IsEqual(ref, "ReturnType") ? ReturnTypeDeferred(parameters[0]) : guard_exports.IsEqual(ref, "Uncapitalize") ? UncapitalizeDeferred(parameters[0]) : guard_exports.IsEqual(ref, "Uppercase") ? UppercaseDeferred(parameters[0]) : CallConstruct(Ref(ref), parameters);
}
function Unreachable2() {
  throw Error("Unreachable");
}
function DelimitedDecode(input, result = []) {
  return guard_exports.ShiftLeft(input, (left, right) => DelimitedDecode(right, [...result, left[1]]), () => result);
}
function Delimited(input) {
  return guard_exports.IsEqual(input.length, 3) ? [input[0], ...DelimitedDecode(input[1])] : [];
}
function GenericParameterExtendsEqualsMapping(input) {
  return Parameter(input[0], input[2], input[4]);
}
function GenericParameterExtendsMapping(input) {
  return Parameter(input[0], input[2], input[2]);
}
function GenericParameterEqualsMapping(input) {
  return Parameter(input[0], Unknown(), input[2]);
}
function GenericParameterIdentifierMapping(input) {
  return Parameter(input, Unknown(), Unknown());
}
function GenericParameterMapping(input) {
  return input;
}
function GenericParameterListMapping(input) {
  return Delimited(input);
}
function GenericParametersMapping(input) {
  return input[1];
}
function GenericCallArgumentListMapping(input) {
  return Delimited(input);
}
function GenericCallArgumentsMapping(input) {
  return input[1];
}
function GenericCallMapping(input) {
  return IntrinsicOrCall(input[0], input[1]);
}
function OptionalSemiColonMapping(input) {
  return null;
}
function KeywordStringMapping(input) {
  return String2();
}
function KeywordNumberMapping(input) {
  return Number2();
}
function KeywordBooleanMapping(input) {
  return Boolean2();
}
function KeywordUndefinedMapping(input) {
  return Undefined();
}
function KeywordNullMapping(input) {
  return Null();
}
function KeywordIntegerMapping(input) {
  return Integer();
}
function KeywordBigIntMapping(input) {
  return BigInt2();
}
function KeywordUnknownMapping(input) {
  return Unknown();
}
function KeywordAnyMapping(input) {
  return Any();
}
function KeywordObjectMapping(input) {
  return _Object_({});
}
function KeywordNeverMapping(input) {
  return Never();
}
function KeywordSymbolMapping(input) {
  return Symbol2();
}
function KeywordVoidMapping(input) {
  return Void();
}
function KeywordThisMapping(input) {
  return This();
}
function LiteralBigIntMapping(input) {
  return Literal(BigInt(input));
}
function LiteralBooleanMapping(input) {
  return Literal(guard_exports.IsEqual(input, "true"));
}
function LiteralNumberMapping(input) {
  return Literal(parseFloat(input));
}
function LiteralStringMapping(input) {
  return Literal(input);
}
function TemplateInterpolateMapping(input) {
  return input[1];
}
function TemplateSpanMapping(input) {
  return Literal(input);
}
function TemplateBodyMapping(input) {
  return guard_exports.IsEqual(input.length, 3) ? [input[0], input[1], ...input[2]] : [input[0]];
}
function TemplateLiteralTypesMapping(input) {
  return input[1];
}
function TemplateLiteralMapping(input) {
  return TemplateLiteralDeferred(input);
}
function DependentMapping(input) {
  return guard_exports.IsEqual(input.length, 6) ? Dependent(input[1], input[3], input[5]) : Dependent(input[1], input[3], Unknown());
}
function KeyOfMapping(input) {
  return input.length > 0;
}
function IndexArrayMapping(input) {
  return input.reduce((result, current) => {
    return guard_exports.IsEqual(current.length, 3) ? [...result, [current[1]]] : [...result, []];
  }, []);
}
function ExtendsMapping(input) {
  return guard_exports.IsEqual(input.length, 6) ? [input[1], input[3], input[5]] : [];
}
function BaseMapping(input) {
  return guard_exports.IsArray(input) && guard_exports.IsEqual(input.length, 3) ? input[1] : input;
}
function WithMapping(input) {
  return guard_exports.IsEqual(input.length, 2) ? input[1] : [];
}
function FactorIndexArray(Type2, indexArray) {
  return indexArray.reduce((result, left) => {
    const _left = left;
    return guard_exports.IsEqual(_left.length, 1) ? IndexDeferred(result, _left[0]) : guard_exports.IsEqual(_left.length, 0) ? _Array_(result) : Unreachable2();
  }, Type2);
}
function FactorExtends(type, extend) {
  return guard_exports.IsEqual(extend.length, 3) ? ConditionalDeferred(type, extend[0], extend[1], extend[2]) : type;
}
function FactorWith(type, withClause) {
  return guard_exports.IsArray(withClause) && guard_exports.IsEqual(withClause.length, 0) ? type : WithDeferred(type, withClause);
}
function FactorMapping(input) {
  const [keyOf, type, indexArray, extend, withClause] = input;
  return FactorWith(keyOf ? FactorExtends(KeyOfDeferred(FactorIndexArray(type, indexArray)), extend) : FactorExtends(FactorIndexArray(type, indexArray), extend), withClause);
}
function ExprBinaryMapping(left, rest2) {
  return guard_exports.IsEqual(rest2.length, 3) ? (() => {
    const [operator, right, next] = rest2;
    const Schema = ExprBinaryMapping(right, next);
    if (guard_exports.IsEqual(operator, "&")) {
      return IsIntersect(Schema) ? Intersect([left, ...Schema.allOf]) : Intersect([left, Schema]);
    }
    if (guard_exports.IsEqual(operator, "|")) {
      return IsUnion(Schema) ? Union([left, ...Schema.anyOf]) : Union([left, Schema]);
    }
    Unreachable2();
  })() : left;
}
function ExprTermTailMapping(input) {
  return input;
}
function ExprTermMapping(input) {
  const [left, rest2] = input;
  return ExprBinaryMapping(left, rest2);
}
function ExprTailMapping(input) {
  return input;
}
function ExprMapping(input) {
  const [left, rest2] = input;
  return ExprBinaryMapping(left, rest2);
}
function ExprReadonlyMapping(input) {
  return AddImmutableDeferred(input[1]);
}
function ExprPipeMapping(input) {
  return input[1];
}
function GenericTypeMapping(input) {
  return Generic(input[0], input[2]);
}
function InferTypeMapping(input) {
  return guard_exports.IsEqual(input.length, 4) ? Infer(input[1], input[3]) : guard_exports.IsEqual(input.length, 2) ? Infer(input[1], Unknown()) : Unreachable2();
}
function TypeMapping(input) {
  return input;
}
function PropertyKeyNumberMapping(input) {
  return `${input}`;
}
function PropertyKeyIdentMapping(input) {
  return input;
}
function PropertyKeyQuotedMapping(input) {
  return input;
}
function PropertyKeyIndexMapping(input) {
  return IsInteger2(input[3]) ? IntegerKey : IsNumber3(input[3]) ? NumberKey : IsSymbol2(input[3]) ? StringKey : IsString3(input[3]) ? StringKey : Unreachable2();
}
function PropertyKeyMapping(input) {
  return input;
}
function ReadonlyMapping(input) {
  return input.length > 0;
}
function OptionalMapping(input) {
  return input.length > 0;
}
function PropertyMapping(input) {
  const [isReadonly, key, isOptional, _colon, type] = input;
  return {
    [key]: isReadonly && isOptional ? AddReadonlyDeferred(AddOptionalDeferred(type)) : isReadonly && !isOptional ? AddReadonlyDeferred(type) : !isReadonly && isOptional ? AddOptionalDeferred(type) : type
  };
}
function PropertyDelimiterMapping(input) {
  return input;
}
function PropertyListMapping(input) {
  return Delimited(input);
}
function PropertiesReduce(propertyList) {
  return propertyList.reduce((result, left) => {
    const isPatternProperties = guard_exports.HasPropertyKey(left, IntegerKey) || guard_exports.HasPropertyKey(left, NumberKey) || guard_exports.HasPropertyKey(left, StringKey);
    return isPatternProperties ? [result[0], memory_exports.Assign(result[1], left)] : [memory_exports.Assign(result[0], left), result[1]];
  }, [{}, {}]);
}
function PropertiesMapping(input) {
  return PropertiesReduce(input[1]);
}
function _Object_Mapping(input) {
  const [properties, patternProperties] = input;
  const options2 = guard_exports.IsEqual(guard_exports.Keys(patternProperties).length, 0) ? {} : { patternProperties };
  return _Object_(properties, options2);
}
function ElementNamedMapping(input) {
  return guard_exports.IsEqual(input.length, 5) ? AddReadonlyDeferred(AddOptionalDeferred(input[4])) : guard_exports.IsEqual(input.length, 3) ? input[2] : guard_exports.IsEqual(input.length, 4) ? guard_exports.IsEqual(input[2], "readonly") ? AddReadonlyDeferred(input[3]) : AddOptionalDeferred(input[3]) : Unreachable2();
}
function ElementBaseMapping(input) {
  if (!guard_exports.IsArray(input) || !guard_exports.IsEqual(input.length, 3))
    return input;
  const [isReadonly, type, isOptional] = input;
  return isReadonly && isOptional ? AddReadonlyDeferred(AddOptionalDeferred(type)) : isReadonly && !isOptional ? AddReadonlyDeferred(type) : !isReadonly && isOptional ? AddOptionalDeferred(type) : type;
}
function ElementMapping(input) {
  return guard_exports.IsEqual(input.length, 2) ? Rest(input[1]) : guard_exports.IsEqual(input.length, 1) ? input[0] : Unreachable2();
}
function ElementListMapping(input) {
  return Delimited(input);
}
function _Tuple_Mapping(input) {
  return Tuple(input[1]);
}
function ParameterReadonlyOptionalMapping(input) {
  return AddReadonlyDeferred(AddOptionalDeferred(input[4]));
}
function ParameterReadonlyMapping(input) {
  return AddReadonlyDeferred(input[3]);
}
function ParameterOptionalMapping(input) {
  return AddOptionalDeferred(input[3]);
}
function ParameterTypeMapping(input) {
  return input[2];
}
function ParameterBaseMapping(input) {
  return input;
}
function ParameterMapping(input) {
  return guard_exports.IsEqual(input.length, 2) ? Rest(input[1]) : guard_exports.IsEqual(input.length, 1) ? input[0] : Unreachable2();
}
function ParameterListMapping(input) {
  return Delimited(input);
}
function _Function_Mapping(input) {
  return _Function_(input[1], input[4]);
}
function _Constructor_Mapping(input) {
  return Constructor(input[2], input[5]);
}
function ApplyReadonly(state, type) {
  return guard_exports.IsEqual(state, "remove") ? RemoveReadonlyDeferred(type) : guard_exports.IsEqual(state, "add") ? AddReadonlyDeferred(type) : type;
}
function MappedReadonlyMapping(input) {
  return guard_exports.IsEqual(input.length, 2) && guard_exports.IsEqual(input[0], "-") ? "remove" : guard_exports.IsEqual(input.length, 2) && guard_exports.IsEqual(input[0], "+") ? "add" : guard_exports.IsEqual(input.length, 1) ? "add" : "none";
}
function ApplyOptional(state, type) {
  return guard_exports.IsEqual(state, "remove") ? RemoveOptionalDeferred(type) : guard_exports.IsEqual(state, "add") ? AddOptionalDeferred(type) : type;
}
function MappedOptionalMapping(input) {
  return guard_exports.IsEqual(input.length, 2) && guard_exports.IsEqual(input[0], "-") ? "remove" : guard_exports.IsEqual(input.length, 2) && guard_exports.IsEqual(input[0], "+") ? "add" : guard_exports.IsEqual(input.length, 1) ? "add" : "none";
}
function MappedAsMapping(input) {
  return guard_exports.IsEqual(input.length, 2) ? [input[1]] : [];
}
function _Mapped_Mapping(input) {
  return guard_exports.IsArray(input[6]) && guard_exports.IsEqual(input[6].length, 1) ? MappedDeferred(Identifier(input[3]), input[5], input[6][0], ApplyReadonly(input[1], ApplyOptional(input[8], input[10]))) : MappedDeferred(Identifier(input[3]), input[5], Ref(input[3]), ApplyReadonly(input[1], ApplyOptional(input[8], input[10])));
}
function ReferenceMapping(input) {
  return Ref(input);
}
function WithBigIntMapping(input) {
  return BigInt(input);
}
function WithNumberMapping(input) {
  return parseFloat(input);
}
function WithBooleanMapping(input) {
  return guard_exports.IsEqual(input, "true");
}
function WithStringMapping(input) {
  return input;
}
function WithNullMapping(input) {
  return null;
}
function WithUndefinedMapping(input) {
  return void 0;
}
function WithPropertyMapping(input) {
  return { [input[0]]: input[2] };
}
function WithPropertyListMapping(input) {
  return Delimited(input);
}
function WithObjectMappingReduce(propertyList) {
  return propertyList.reduce((result, left) => {
    return memory_exports.Assign(result, left);
  }, {});
}
function WithObjectMapping(input) {
  return WithObjectMappingReduce(input[1]);
}
function WithElementListMapping(input) {
  return Delimited(input);
}
function WithArrayMapping(input) {
  return input[1];
}
function WithValueMapping(input) {
  return input;
}
function PatternBigIntMapping(input) {
  return BigInt2();
}
function PatternStringMapping(input) {
  return String2();
}
function PatternNumberMapping(input) {
  return Number2();
}
function PatternIntegerMapping(input) {
  return Integer();
}
function PatternNeverMapping(input) {
  return Never();
}
function PatternTextMapping(input) {
  return Literal(input);
}
function PatternBaseMapping(input) {
  return input;
}
function PatternGroupMapping(input) {
  return Union(input[1]);
}
function PatternUnionMapping(input) {
  return input.length === 3 ? [...input[0], ...input[2]] : input.length === 1 ? [...input[0]] : [];
}
function PatternTermMapping(input) {
  return [input[0], ...input[1]];
}
function PatternBodyMapping(input) {
  return input;
}
function PatternMapping(input) {
  return input[1];
}
function InterfaceDeclarationHeritageListMapping(input) {
  return Delimited(input);
}
function InterfaceDeclarationHeritageMapping(input) {
  return guard_exports.IsEqual(input.length, 2) ? input[1] : [];
}
function InterfaceDeclarationGenericMapping(input) {
  const parameters = input[2];
  const heritage = input[3];
  const [properties, patternProperties] = input[4];
  const options2 = guard_exports.IsEqual(guard_exports.Keys(patternProperties).length, 0) ? {} : { patternProperties };
  return { [input[1]]: Generic(parameters, InterfaceDeferred(heritage, properties, options2)) };
}
function InterfaceDeclarationMapping(input) {
  const heritage = input[2];
  const [properties, patternProperties] = input[3];
  const options2 = guard_exports.IsEqual(guard_exports.Keys(patternProperties).length, 0) ? {} : { patternProperties };
  return { [input[1]]: InterfaceDeferred(heritage, properties, options2) };
}
function TypeAliasDeclarationGenericMapping(input) {
  return { [input[1]]: Generic(input[2], input[4]) };
}
function TypeAliasDeclarationMapping(input) {
  return { [input[1]]: input[3] };
}
function ExportKeywordMapping(input) {
  return null;
}
function ModuleDeclarationDelimiterMapping(input) {
  return input;
}
function ModuleDeclarationListMapping(input) {
  return Delimited(input);
}
function ModuleDeclarationMapping(input) {
  return input[1];
}
function ModuleMapping(input) {
  const [moduleDeclaration, moduleDeclarationList] = [input[0], input[1]];
  return ModuleDeferred(memory_exports.Assign(moduleDeclaration, PropertiesReduce(moduleDeclarationList)[0]));
}
function ScriptMapping(input) {
  return input;
}
var init_mapping = __esm({
  "node_modules/typebox/build/type/script/mapping.mjs"() {
    init_memory2();
    init_guard2();
    init_types();
    init_action();
  }
});

// node_modules/typebox/build/type/script/token/internal/guard.mjs
var init_guard3 = __esm({
  "node_modules/typebox/build/type/script/token/internal/guard.mjs"() {
    init_guard();
  }
});

// node_modules/typebox/build/type/script/token/internal/match.mjs
function IsMatch(value) {
  return IsEqual(value.length, 2);
}
function Match2(input, ok, fail) {
  return IsMatch(input) ? ok(input[0], input[1]) : fail();
}
var init_match = __esm({
  "node_modules/typebox/build/type/script/token/internal/match.mjs"() {
    init_guard3();
  }
});

// node_modules/typebox/build/type/script/token/internal/take.mjs
function TakeVariant(variant, input) {
  return IsEqual(input.indexOf(variant), 0) ? [variant, input.slice(variant.length)] : [];
}
function Take(variants, input) {
  for (let i = 0; i < variants.length; i++) {
    const result = TakeVariant(variants[i], input);
    if (IsMatch(result))
      return result;
  }
  return [];
}
var init_take = __esm({
  "node_modules/typebox/build/type/script/token/internal/take.mjs"() {
    init_match();
    init_guard3();
  }
});

// node_modules/typebox/build/type/script/token/internal/char.mjs
function Range(start, end) {
  return Array.from({ length: end - start + 1 }, (_, i) => String.fromCharCode(start + i));
}
var Alpha, Zero, NonZero, Digit, WhiteSpace, NewLine, UnderScore, Dot, DollarSign, Hyphen;
var init_char = __esm({
  "node_modules/typebox/build/type/script/token/internal/char.mjs"() {
    Alpha = [
      ...Range(97, 122),
      // Lowercase
      ...Range(65, 90)
      // Uppercase
    ];
    Zero = "0";
    NonZero = Range(49, 57);
    Digit = [Zero, ...NonZero];
    WhiteSpace = " ";
    NewLine = "\n";
    UnderScore = "_";
    Dot = ".";
    DollarSign = "$";
    Hyphen = "-";
  }
});

// node_modules/typebox/build/type/script/token/internal/trim.mjs
function DiscardMultilineComment(input) {
  const index = input.indexOf(CloseComment);
  const result = IsEqual(index, -1) ? "" : input.slice(index + 2);
  return result;
}
function DiscardLineComment(input) {
  const index = input.indexOf(NewLine);
  const result = IsEqual(index, -1) ? "" : input.slice(index);
  return result;
}
function TrimStartUntilNewline(input) {
  return input.replace(/^[ \t\r\f\v]+/, "");
}
function TrimWhitespace(input) {
  const trimmed = TrimStartUntilNewline(input);
  return trimmed.startsWith(OpenComment) ? TrimWhitespace(DiscardMultilineComment(trimmed.slice(2))) : trimmed.startsWith(LineComment) ? TrimWhitespace(DiscardLineComment(trimmed.slice(2))) : trimmed;
}
function Trim(input) {
  const trimmed = input.trimStart();
  return trimmed.startsWith(OpenComment) ? Trim(DiscardMultilineComment(trimmed.slice(2))) : trimmed.startsWith(LineComment) ? Trim(DiscardLineComment(trimmed.slice(2))) : trimmed;
}
var LineComment, OpenComment, CloseComment;
var init_trim = __esm({
  "node_modules/typebox/build/type/script/token/internal/trim.mjs"() {
    init_guard3();
    init_char();
    LineComment = "//";
    OpenComment = "/*";
    CloseComment = "*/";
  }
});

// node_modules/typebox/build/type/script/token/internal/optional.mjs
function Optional2(value, input) {
  return Match2(Take([value], input), (Optional4, Rest2) => [Optional4, Rest2], () => ["", input]);
}
var init_optional2 = __esm({
  "node_modules/typebox/build/type/script/token/internal/optional.mjs"() {
    init_match();
    init_take();
  }
});

// node_modules/typebox/build/type/script/token/internal/many.mjs
function IsDiscard(discard, input) {
  return discard.includes(input);
}
function Many(allowed, discard, input, result = "") {
  return Match2(Take(allowed, input), (Char, Rest2) => IsDiscard(discard, Char) ? Many(allowed, discard, Rest2, result) : Many(allowed, discard, Rest2, `${result}${Char}`), () => [result, input]);
}
var init_many = __esm({
  "node_modules/typebox/build/type/script/token/internal/many.mjs"() {
    init_match();
    init_take();
  }
});

// node_modules/typebox/build/type/script/token/unsigned_integer.mjs
function TakeNonZero(input) {
  return Take(NonZero, input);
}
function TakeDigits(input) {
  return Many(AllowedDigits, [UnderScore], input);
}
function TakeUnsignedInteger(input) {
  return Match2(Take([Zero], input), (Zero2, ZeroRest) => [Zero2, ZeroRest], () => Match2(
    TakeNonZero(input),
    (NonZero2, NonZeroRest) => Match2(TakeDigits(NonZeroRest), (Digits, DigitsRest) => [`${NonZero2}${Digits}`, DigitsRest], () => []),
    // fail: did not match Digits
    () => []
  ));
}
function UnsignedInteger(input) {
  return TakeUnsignedInteger(Trim(input));
}
var AllowedDigits;
var init_unsigned_integer = __esm({
  "node_modules/typebox/build/type/script/token/unsigned_integer.mjs"() {
    init_match();
    init_trim();
    init_take();
    init_many();
    init_char();
    init_char();
    init_char();
    init_char();
    AllowedDigits = [...Digit, UnderScore];
  }
});

// node_modules/typebox/build/type/script/token/integer.mjs
function TakeSign(input) {
  return Optional2(Hyphen, input);
}
function TakeSignedInteger(input) {
  return Match2(
    TakeSign(input),
    (Sign, SignRest) => Match2(UnsignedInteger(SignRest), (UnsignedInteger2, UnsignedIntegerRest) => [`${Sign}${UnsignedInteger2}`, UnsignedIntegerRest], () => []),
    // fail: did not match unsigned integer
    () => []
  );
}
function Integer2(input) {
  return TakeSignedInteger(Trim(input));
}
var init_integer2 = __esm({
  "node_modules/typebox/build/type/script/token/integer.mjs"() {
    init_match();
    init_trim();
    init_optional2();
    init_char();
    init_unsigned_integer();
  }
});

// node_modules/typebox/build/type/script/token/bigint.mjs
function TakeBigInt(input) {
  return Match2(
    Integer2(input),
    (Integer3, IntegerRest) => Match2(Take(["n"], IntegerRest), (_N, NRest) => [`${Integer3}`, NRest], () => []),
    // fail: did not match 'n'
    () => []
  );
}
function BigInt3(input) {
  return TakeBigInt(input);
}
var init_bigint2 = __esm({
  "node_modules/typebox/build/type/script/token/bigint.mjs"() {
    init_match();
    init_take();
    init_integer2();
  }
});

// node_modules/typebox/build/type/script/token/const.mjs
function TakeConst(const_, input) {
  return Take([const_], input);
}
function Const(const_, input) {
  return IsEqual(const_, "") ? ["", input] : const_.startsWith(NewLine) ? TakeConst(const_, TrimWhitespace(input)) : const_.startsWith(WhiteSpace) ? TakeConst(const_, input) : TakeConst(const_, Trim(input));
}
var init_const = __esm({
  "node_modules/typebox/build/type/script/token/const.mjs"() {
    init_guard3();
    init_trim();
    init_trim();
    init_take();
    init_char();
    init_char();
  }
});

// node_modules/typebox/build/type/script/token/ident.mjs
function TakeInitial(input) {
  return Take(Initial, input);
}
function TakeRemaining(input, result = "") {
  return Match2(Take(Remaining, input), (Remaining2, RemainingRest) => TakeRemaining(RemainingRest, `${result}${Remaining2}`), () => [result, input]);
}
function TakeIdent(input) {
  return Match2(
    TakeInitial(input),
    (Initial2, InitialRest) => Match2(TakeRemaining(InitialRest), (Remaining2, RemainingRest) => [`${Initial2}${Remaining2}`, RemainingRest], () => []),
    // fail: did not match Remaining
    () => []
  );
}
function Ident(input) {
  return TakeIdent(Trim(input));
}
var Initial, Remaining;
var init_ident = __esm({
  "node_modules/typebox/build/type/script/token/ident.mjs"() {
    init_match();
    init_trim();
    init_take();
    init_char();
    init_char();
    init_char();
    init_char();
    Initial = [...Alpha, UnderScore, DollarSign];
    Remaining = [...Initial, ...Digit];
  }
});

// node_modules/typebox/build/type/script/token/unsigned_number.mjs
function IsLeadingDot(input) {
  return IsMatch(Take([Dot], input));
}
function TakeFractional(input) {
  return Match2(Many(AllowedDigits2, [UnderScore], input), (Digits, DigitsRest) => IsEqual(Digits, "") ? [] : [Digits, DigitsRest], () => []);
}
function LeadingDot(input) {
  return Match2(
    Take([Dot], input),
    (Dot2, DotRest) => Match2(TakeFractional(DotRest), (Fractional, FractionalRest) => [`0${Dot2}${Fractional}`, FractionalRest], () => []),
    // fail: did not match Fractional
    () => []
  );
}
function LeadingInteger(input) {
  return Match2(
    UnsignedInteger(input),
    (Integer3, IntegerRest) => Match2(
      Take([Dot], IntegerRest),
      (Dot2, DotRest) => Match2(TakeFractional(DotRest), (Fractional, FractionalRest) => [`${Integer3}${Dot2}${Fractional}`, FractionalRest], () => [`${Integer3}`, DotRest]),
      // fail: did not match Fractional, use Integer
      () => [`${Integer3}`, IntegerRest]
    ),
    // fail: did not match Dot, use Integer
    () => []
  );
}
function TakeUnsignedNumber(input) {
  return IsLeadingDot(input) ? LeadingDot(input) : LeadingInteger(input);
}
function UnsignedNumber(input) {
  return TakeUnsignedNumber(Trim(input));
}
var AllowedDigits2;
var init_unsigned_number = __esm({
  "node_modules/typebox/build/type/script/token/unsigned_number.mjs"() {
    init_guard3();
    init_match();
    init_trim();
    init_take();
    init_many();
    init_char();
    init_char();
    init_unsigned_integer();
    AllowedDigits2 = [...Digit, UnderScore];
  }
});

// node_modules/typebox/build/type/script/token/number.mjs
function TakeSign2(input) {
  return Optional2(Hyphen, input);
}
function TakeSignedNumber(input) {
  return Match2(
    TakeSign2(input),
    (Sign, SignRest) => Match2(UnsignedNumber(SignRest), (UnsignedInteger2, UnsignedIntegerRest) => [`${Sign}${UnsignedInteger2}`, UnsignedIntegerRest], () => []),
    // fail: did not match unsigned integer
    () => []
  );
}
function Number3(input) {
  return TakeSignedNumber(Trim(input));
}
var init_number2 = __esm({
  "node_modules/typebox/build/type/script/token/number.mjs"() {
    init_match();
    init_trim();
    init_optional2();
    init_char();
    init_unsigned_number();
  }
});

// node_modules/typebox/build/type/script/token/rest.mjs
var init_rest2 = __esm({
  "node_modules/typebox/build/type/script/token/rest.mjs"() {
    init_guard3();
  }
});

// node_modules/typebox/build/type/script/token/until.mjs
function TakeOne(input) {
  const result = IsEqual(input, "") ? [] : [input.slice(0, 1), input.slice(1)];
  return result;
}
function IsInputMatchSentinal(end, input) {
  return ShiftLeft(end, (left, right) => input.startsWith(left) ? true : IsInputMatchSentinal(right, input), () => false);
}
function Until(end, input, result = "") {
  return Match2(
    TakeOne(input),
    (One, Rest2) => IsInputMatchSentinal(end, input) ? [result, input] : Until(end, Rest2, `${result}${One}`),
    () => []
  );
}
var init_until = __esm({
  "node_modules/typebox/build/type/script/token/until.mjs"() {
    init_match();
    init_guard3();
  }
});

// node_modules/typebox/build/type/script/token/span.mjs
function MultiLine(start, end, input) {
  return Match2(
    Take([start], input),
    (_, Rest2) => Match2(
      Until([end], Rest2),
      (Until2, UntilRest) => Match2(Take([end], UntilRest), (_2, Rest3) => [`${Until2}`, Rest3], () => []),
      // fail: did not match End
      () => []
    ),
    // fail: did not match Until
    () => []
  );
}
function SingleLine(start, end, input) {
  return Match2(
    Take([start], input),
    (_, Rest2) => Match2(
      Until([NewLine, end], Rest2),
      (Until2, UntilRest) => Match2(Take([end], UntilRest), (_2, EndRest) => [`${Until2}`, EndRest], () => []),
      // fail: did not match End
      () => []
    ),
    // fail: did not match Until
    () => []
  );
}
function Span(start, end, multiLine, input) {
  return multiLine ? MultiLine(start, end, Trim(input)) : SingleLine(start, end, Trim(input));
}
var init_span = __esm({
  "node_modules/typebox/build/type/script/token/span.mjs"() {
    init_match();
    init_trim();
    init_char();
    init_take();
    init_until();
  }
});

// node_modules/typebox/build/type/script/token/string.mjs
function TakeInitial2(quotes, input) {
  return Take(quotes, input);
}
function TakeSpan(quote2, input) {
  return Span(quote2, quote2, false, input);
}
function TakeString(quotes, input) {
  return Match2(TakeInitial2(quotes, input), (Initial2, InitialRest) => TakeSpan(Initial2, `${Initial2}${InitialRest}`), () => []);
}
function String3(quotes, input) {
  return TakeString(quotes, Trim(input));
}
var init_string3 = __esm({
  "node_modules/typebox/build/type/script/token/string.mjs"() {
    init_match();
    init_take();
    init_trim();
    init_span();
  }
});

// node_modules/typebox/build/type/script/token/until_1.mjs
function Until_1(end, input) {
  return Match2(Until(end, input), (Until2, UntilRest) => IsEqual(Until2, "") ? [] : [Until2, UntilRest], () => []);
}
var init_until_1 = __esm({
  "node_modules/typebox/build/type/script/token/until_1.mjs"() {
    init_guard3();
    init_match();
    init_until();
  }
});

// node_modules/typebox/build/type/script/token/index.mjs
var init_token = __esm({
  "node_modules/typebox/build/type/script/token/index.mjs"() {
    init_bigint2();
    init_const();
    init_ident();
    init_integer2();
    init_number2();
    init_rest2();
    init_span();
    init_string3();
    init_unsigned_integer();
    init_unsigned_number();
    init_until_1();
    init_until();
  }
});

// node_modules/typebox/build/type/script/parser.mjs
var If, GenericParameterExtendsEquals, GenericParameterExtends, GenericParameterEquals, GenericParameterIdentifier, GenericParameter, GenericParameterList_0, GenericParameterList, GenericParameters, GenericCallArgumentList_0, GenericCallArgumentList, GenericCallArguments, GenericCall, OptionalSemiColon, KeywordString, KeywordNumber, KeywordBoolean, KeywordUndefined, KeywordNull, KeywordInteger, KeywordBigInt, KeywordUnknown, KeywordAny, KeywordObject, KeywordNever, KeywordSymbol, KeywordVoid, KeywordThis, TemplateInterpolate, TemplateSpan, TemplateBody, TemplateLiteralTypes, TemplateLiteral, Dependent2, LiteralBigInt, LiteralBoolean, LiteralNumber, LiteralString, KeyOf, IndexArray_0, IndexArray, Extends2, Base, With, Factor, ExprTermTail, ExprTerm, ExprTail, Expr, ExprReadonly, ExprPipe, GenericType, InferType, Type, PropertyKeyNumber, PropertyKeyIdent, PropertyKeyQuoted, PropertyKeyIndex, PropertyKey, Readonly2, Optional3, Property, PropertyDelimiter, PropertyList_0, PropertyList, Properties, _Object_2, ElementNamed, ElementBase, Element, ElementList_0, ElementList, _Tuple_, ParameterReadonlyOptional, ParameterReadonly, ParameterOptional, ParameterType, ParameterBase, Parameter2, ParameterList_0, ParameterList, _Function_2, _Constructor_, MappedReadonly, MappedOptional, MappedAs, _Mapped_, Reference, WithBigInt, WithNumber, WithBoolean, WithString, WithNull, WithUndefined, WithProperty, WithPropertyList_0, WithPropertyList, WithObject, WithElementList_0, WithElementList, WithArray, WithValue, PatternBigInt, PatternString, PatternNumber, PatternInteger, PatternNever, PatternText, PatternBase, PatternGroup, PatternUnion, PatternTerm, PatternBody, Pattern, InterfaceDeclarationHeritageList_0, InterfaceDeclarationHeritageList, InterfaceDeclarationHeritage, InterfaceDeclarationGeneric, InterfaceDeclaration, TypeAliasDeclarationGeneric, TypeAliasDeclaration, ExportKeyword, ModuleDeclarationDelimiter, ModuleDeclarationList_0, ModuleDeclarationList, ModuleDeclaration, Module, Script;
var init_parser = __esm({
  "node_modules/typebox/build/type/script/parser.mjs"() {
    init_mapping();
    init_token();
    If = (result, left, right = () => []) => result.length === 2 ? left(result) : right();
    GenericParameterExtendsEquals = (input) => If(If(Ident(input), ([_0, input2]) => If(Const("extends", input2), ([_1, input3]) => If(Type(input3), ([_2, input4]) => If(Const("=", input4), ([_3, input5]) => If(Type(input5), ([_4, input6]) => [[_0, _1, _2, _3, _4], input6]))))), ([_0, input2]) => [GenericParameterExtendsEqualsMapping(_0), input2]);
    GenericParameterExtends = (input) => If(If(Ident(input), ([_0, input2]) => If(Const("extends", input2), ([_1, input3]) => If(Type(input3), ([_2, input4]) => [[_0, _1, _2], input4]))), ([_0, input2]) => [GenericParameterExtendsMapping(_0), input2]);
    GenericParameterEquals = (input) => If(If(Ident(input), ([_0, input2]) => If(Const("=", input2), ([_1, input3]) => If(Type(input3), ([_2, input4]) => [[_0, _1, _2], input4]))), ([_0, input2]) => [GenericParameterEqualsMapping(_0), input2]);
    GenericParameterIdentifier = (input) => If(Ident(input), ([_0, input2]) => [GenericParameterIdentifierMapping(_0), input2]);
    GenericParameter = (input) => If(If(GenericParameterExtendsEquals(input), ([_0, input2]) => [_0, input2], () => If(GenericParameterExtends(input), ([_0, input2]) => [_0, input2], () => If(GenericParameterEquals(input), ([_0, input2]) => [_0, input2], () => If(GenericParameterIdentifier(input), ([_0, input2]) => [_0, input2], () => [])))), ([_0, input2]) => [GenericParameterMapping(_0), input2]);
    GenericParameterList_0 = (input, result = []) => If(If(Const(",", input), ([_0, input2]) => If(GenericParameter(input2), ([_1, input3]) => [[_0, _1], input3])), ([_0, input2]) => GenericParameterList_0(input2, [...result, _0]), () => [result, input]);
    GenericParameterList = (input) => If(If(If(GenericParameter(input), ([_0, input2]) => If(GenericParameterList_0(input2), ([_1, input3]) => If(If(Const(",", input3), ([_02, input4]) => [[_02], input4], () => [[], input3]), ([_2, input4]) => [[_0, _1, _2], input4]))), ([_0, input2]) => [_0, input2], () => If([[], input], ([_0, input2]) => [_0, input2], () => [])), ([_0, input2]) => [GenericParameterListMapping(_0), input2]);
    GenericParameters = (input) => If(If(Const("<", input), ([_0, input2]) => If(GenericParameterList(input2), ([_1, input3]) => If(Const(">", input3), ([_2, input4]) => [[_0, _1, _2], input4]))), ([_0, input2]) => [GenericParametersMapping(_0), input2]);
    GenericCallArgumentList_0 = (input, result = []) => If(If(Const(",", input), ([_0, input2]) => If(Type(input2), ([_1, input3]) => [[_0, _1], input3])), ([_0, input2]) => GenericCallArgumentList_0(input2, [...result, _0]), () => [result, input]);
    GenericCallArgumentList = (input) => If(If(If(Type(input), ([_0, input2]) => If(GenericCallArgumentList_0(input2), ([_1, input3]) => If(If(Const(",", input3), ([_02, input4]) => [[_02], input4], () => [[], input3]), ([_2, input4]) => [[_0, _1, _2], input4]))), ([_0, input2]) => [_0, input2], () => If([[], input], ([_0, input2]) => [_0, input2], () => [])), ([_0, input2]) => [GenericCallArgumentListMapping(_0), input2]);
    GenericCallArguments = (input) => If(If(Const("<", input), ([_0, input2]) => If(GenericCallArgumentList(input2), ([_1, input3]) => If(Const(">", input3), ([_2, input4]) => [[_0, _1, _2], input4]))), ([_0, input2]) => [GenericCallArgumentsMapping(_0), input2]);
    GenericCall = (input) => If(If(Ident(input), ([_0, input2]) => If(GenericCallArguments(input2), ([_1, input3]) => [[_0, _1], input3])), ([_0, input2]) => [GenericCallMapping(_0), input2]);
    OptionalSemiColon = (input) => If(If(If(Const(";", input), ([_0, input2]) => [[_0], input2]), ([_0, input2]) => [_0, input2], () => If([[], input], ([_0, input2]) => [_0, input2], () => [])), ([_0, input2]) => [OptionalSemiColonMapping(_0), input2]);
    KeywordString = (input) => If(Const("string", input), ([_0, input2]) => [KeywordStringMapping(_0), input2]);
    KeywordNumber = (input) => If(Const("number", input), ([_0, input2]) => [KeywordNumberMapping(_0), input2]);
    KeywordBoolean = (input) => If(Const("boolean", input), ([_0, input2]) => [KeywordBooleanMapping(_0), input2]);
    KeywordUndefined = (input) => If(Const("undefined", input), ([_0, input2]) => [KeywordUndefinedMapping(_0), input2]);
    KeywordNull = (input) => If(Const("null", input), ([_0, input2]) => [KeywordNullMapping(_0), input2]);
    KeywordInteger = (input) => If(Const("integer", input), ([_0, input2]) => [KeywordIntegerMapping(_0), input2]);
    KeywordBigInt = (input) => If(Const("bigint", input), ([_0, input2]) => [KeywordBigIntMapping(_0), input2]);
    KeywordUnknown = (input) => If(Const("unknown", input), ([_0, input2]) => [KeywordUnknownMapping(_0), input2]);
    KeywordAny = (input) => If(Const("any", input), ([_0, input2]) => [KeywordAnyMapping(_0), input2]);
    KeywordObject = (input) => If(Const("object", input), ([_0, input2]) => [KeywordObjectMapping(_0), input2]);
    KeywordNever = (input) => If(Const("never", input), ([_0, input2]) => [KeywordNeverMapping(_0), input2]);
    KeywordSymbol = (input) => If(Const("symbol", input), ([_0, input2]) => [KeywordSymbolMapping(_0), input2]);
    KeywordVoid = (input) => If(Const("void", input), ([_0, input2]) => [KeywordVoidMapping(_0), input2]);
    KeywordThis = (input) => If(Const("this", input), ([_0, input2]) => [KeywordThisMapping(_0), input2]);
    TemplateInterpolate = (input) => If(If(Const("${", input), ([_0, input2]) => If(Type(input2), ([_1, input3]) => If(Const("}", input3), ([_2, input4]) => [[_0, _1, _2], input4]))), ([_0, input2]) => [TemplateInterpolateMapping(_0), input2]);
    TemplateSpan = (input) => If(Until(["${", "`"], input), ([_0, input2]) => [TemplateSpanMapping(_0), input2]);
    TemplateBody = (input) => If(If(If(TemplateSpan(input), ([_0, input2]) => If(TemplateInterpolate(input2), ([_1, input3]) => If(TemplateBody(input3), ([_2, input4]) => [[_0, _1, _2], input4]))), ([_0, input2]) => [_0, input2], () => If(If(TemplateSpan(input), ([_0, input2]) => [[_0], input2]), ([_0, input2]) => [_0, input2], () => If(If(TemplateSpan(input), ([_0, input2]) => [[_0], input2]), ([_0, input2]) => [_0, input2], () => []))), ([_0, input2]) => [TemplateBodyMapping(_0), input2]);
    TemplateLiteralTypes = (input) => If(If(Const("`", input), ([_0, input2]) => If(TemplateBody(input2), ([_1, input3]) => If(Const("`", input3), ([_2, input4]) => [[_0, _1, _2], input4]))), ([_0, input2]) => [TemplateLiteralTypesMapping(_0), input2]);
    TemplateLiteral = (input) => If(TemplateLiteralTypes(input), ([_0, input2]) => [TemplateLiteralMapping(_0), input2]);
    Dependent2 = (input) => If(If(If(Const("if", input), ([_0, input2]) => If(Type(input2), ([_1, input3]) => If(Const("then", input3), ([_2, input4]) => If(Type(input4), ([_3, input5]) => If(Const("else", input5), ([_4, input6]) => If(Type(input6), ([_5, input7]) => [[_0, _1, _2, _3, _4, _5], input7])))))), ([_0, input2]) => [_0, input2], () => If(If(Const("if", input), ([_0, input2]) => If(Type(input2), ([_1, input3]) => If(Const("then", input3), ([_2, input4]) => If(Type(input4), ([_3, input5]) => [[_0, _1, _2, _3], input5])))), ([_0, input2]) => [_0, input2], () => [])), ([_0, input2]) => [DependentMapping(_0), input2]);
    LiteralBigInt = (input) => If(BigInt3(input), ([_0, input2]) => [LiteralBigIntMapping(_0), input2]);
    LiteralBoolean = (input) => If(If(Const("true", input), ([_0, input2]) => [_0, input2], () => If(Const("false", input), ([_0, input2]) => [_0, input2], () => [])), ([_0, input2]) => [LiteralBooleanMapping(_0), input2]);
    LiteralNumber = (input) => If(Number3(input), ([_0, input2]) => [LiteralNumberMapping(_0), input2]);
    LiteralString = (input) => If(String3(["'", '"'], input), ([_0, input2]) => [LiteralStringMapping(_0), input2]);
    KeyOf = (input) => If(If(If(Const("keyof", input), ([_0, input2]) => [[_0], input2]), ([_0, input2]) => [_0, input2], () => If([[], input], ([_0, input2]) => [_0, input2], () => [])), ([_0, input2]) => [KeyOfMapping(_0), input2]);
    IndexArray_0 = (input, result = []) => If(If(If(Const("[", input), ([_0, input2]) => If(Type(input2), ([_1, input3]) => If(Const("]", input3), ([_2, input4]) => [[_0, _1, _2], input4]))), ([_0, input2]) => [_0, input2], () => If(If(Const("[", input), ([_0, input2]) => If(Const("]", input2), ([_1, input3]) => [[_0, _1], input3])), ([_0, input2]) => [_0, input2], () => [])), ([_0, input2]) => IndexArray_0(input2, [...result, _0]), () => [result, input]);
    IndexArray = (input) => If(IndexArray_0(input), ([_0, input2]) => [IndexArrayMapping(_0), input2]);
    Extends2 = (input) => If(If(If(Const("extends", input), ([_0, input2]) => If(Type(input2), ([_1, input3]) => If(Const("?", input3), ([_2, input4]) => If(Type(input4), ([_3, input5]) => If(Const(":", input5), ([_4, input6]) => If(Type(input6), ([_5, input7]) => [[_0, _1, _2, _3, _4, _5], input7])))))), ([_0, input2]) => [_0, input2], () => If([[], input], ([_0, input2]) => [_0, input2], () => [])), ([_0, input2]) => [ExtendsMapping(_0), input2]);
    Base = (input) => If(If(If(Const("(", input), ([_0, input2]) => If(Type(input2), ([_1, input3]) => If(Const(")", input3), ([_2, input4]) => [[_0, _1, _2], input4]))), ([_0, input2]) => [_0, input2], () => If(KeywordString(input), ([_0, input2]) => [_0, input2], () => If(KeywordNumber(input), ([_0, input2]) => [_0, input2], () => If(KeywordBoolean(input), ([_0, input2]) => [_0, input2], () => If(KeywordUndefined(input), ([_0, input2]) => [_0, input2], () => If(KeywordNull(input), ([_0, input2]) => [_0, input2], () => If(KeywordInteger(input), ([_0, input2]) => [_0, input2], () => If(KeywordBigInt(input), ([_0, input2]) => [_0, input2], () => If(KeywordUnknown(input), ([_0, input2]) => [_0, input2], () => If(KeywordAny(input), ([_0, input2]) => [_0, input2], () => If(KeywordObject(input), ([_0, input2]) => [_0, input2], () => If(KeywordNever(input), ([_0, input2]) => [_0, input2], () => If(KeywordSymbol(input), ([_0, input2]) => [_0, input2], () => If(KeywordVoid(input), ([_0, input2]) => [_0, input2], () => If(KeywordThis(input), ([_0, input2]) => [_0, input2], () => If(LiteralBigInt(input), ([_0, input2]) => [_0, input2], () => If(LiteralBoolean(input), ([_0, input2]) => [_0, input2], () => If(LiteralNumber(input), ([_0, input2]) => [_0, input2], () => If(LiteralString(input), ([_0, input2]) => [_0, input2], () => If(TemplateLiteral(input), ([_0, input2]) => [_0, input2], () => If(Dependent2(input), ([_0, input2]) => [_0, input2], () => If(_Object_2(input), ([_0, input2]) => [_0, input2], () => If(_Tuple_(input), ([_0, input2]) => [_0, input2], () => If(_Constructor_(input), ([_0, input2]) => [_0, input2], () => If(_Function_2(input), ([_0, input2]) => [_0, input2], () => If(_Mapped_(input), ([_0, input2]) => [_0, input2], () => If(GenericCall(input), ([_0, input2]) => [_0, input2], () => If(Reference(input), ([_0, input2]) => [_0, input2], () => [])))))))))))))))))))))))))))), ([_0, input2]) => [BaseMapping(_0), input2]);
    With = (input) => If(If(If(Const("with", input), ([_0, input2]) => If(WithObject(input2), ([_1, input3]) => [[_0, _1], input3])), ([_0, input2]) => [_0, input2], () => If([[], input], ([_0, input2]) => [_0, input2], () => [])), ([_0, input2]) => [WithMapping(_0), input2]);
    Factor = (input) => If(If(KeyOf(input), ([_0, input2]) => If(Base(input2), ([_1, input3]) => If(IndexArray(input3), ([_2, input4]) => If(Extends2(input4), ([_3, input5]) => If(With(input5), ([_4, input6]) => [[_0, _1, _2, _3, _4], input6]))))), ([_0, input2]) => [FactorMapping(_0), input2]);
    ExprTermTail = (input) => If(If(If(Const("&", input), ([_0, input2]) => If(Factor(input2), ([_1, input3]) => If(ExprTermTail(input3), ([_2, input4]) => [[_0, _1, _2], input4]))), ([_0, input2]) => [_0, input2], () => If([[], input], ([_0, input2]) => [_0, input2], () => [])), ([_0, input2]) => [ExprTermTailMapping(_0), input2]);
    ExprTerm = (input) => If(If(Factor(input), ([_0, input2]) => If(ExprTermTail(input2), ([_1, input3]) => [[_0, _1], input3])), ([_0, input2]) => [ExprTermMapping(_0), input2]);
    ExprTail = (input) => If(If(If(Const("|", input), ([_0, input2]) => If(ExprTerm(input2), ([_1, input3]) => If(ExprTail(input3), ([_2, input4]) => [[_0, _1, _2], input4]))), ([_0, input2]) => [_0, input2], () => If([[], input], ([_0, input2]) => [_0, input2], () => [])), ([_0, input2]) => [ExprTailMapping(_0), input2]);
    Expr = (input) => If(If(ExprTerm(input), ([_0, input2]) => If(ExprTail(input2), ([_1, input3]) => [[_0, _1], input3])), ([_0, input2]) => [ExprMapping(_0), input2]);
    ExprReadonly = (input) => If(If(Const("readonly", input), ([_0, input2]) => If(Expr(input2), ([_1, input3]) => [[_0, _1], input3])), ([_0, input2]) => [ExprReadonlyMapping(_0), input2]);
    ExprPipe = (input) => If(If(Const("|", input), ([_0, input2]) => If(Expr(input2), ([_1, input3]) => [[_0, _1], input3])), ([_0, input2]) => [ExprPipeMapping(_0), input2]);
    GenericType = (input) => If(If(GenericParameters(input), ([_0, input2]) => If(Const("=", input2), ([_1, input3]) => If(Type(input3), ([_2, input4]) => [[_0, _1, _2], input4]))), ([_0, input2]) => [GenericTypeMapping(_0), input2]);
    InferType = (input) => If(If(If(Const("infer", input), ([_0, input2]) => If(Ident(input2), ([_1, input3]) => If(Const("extends", input3), ([_2, input4]) => If(Expr(input4), ([_3, input5]) => [[_0, _1, _2, _3], input5])))), ([_0, input2]) => [_0, input2], () => If(If(Const("infer", input), ([_0, input2]) => If(Ident(input2), ([_1, input3]) => [[_0, _1], input3])), ([_0, input2]) => [_0, input2], () => [])), ([_0, input2]) => [InferTypeMapping(_0), input2]);
    Type = (input) => If(If(InferType(input), ([_0, input2]) => [_0, input2], () => If(ExprPipe(input), ([_0, input2]) => [_0, input2], () => If(ExprReadonly(input), ([_0, input2]) => [_0, input2], () => If(Expr(input), ([_0, input2]) => [_0, input2], () => [])))), ([_0, input2]) => [TypeMapping(_0), input2]);
    PropertyKeyNumber = (input) => If(Number3(input), ([_0, input2]) => [PropertyKeyNumberMapping(_0), input2]);
    PropertyKeyIdent = (input) => If(Ident(input), ([_0, input2]) => [PropertyKeyIdentMapping(_0), input2]);
    PropertyKeyQuoted = (input) => If(String3(["'", '"'], input), ([_0, input2]) => [PropertyKeyQuotedMapping(_0), input2]);
    PropertyKeyIndex = (input) => If(If(Const("[", input), ([_0, input2]) => If(Ident(input2), ([_1, input3]) => If(Const(":", input3), ([_2, input4]) => If(If(KeywordInteger(input4), ([_02, input5]) => [_02, input5], () => If(KeywordNumber(input4), ([_02, input5]) => [_02, input5], () => If(KeywordString(input4), ([_02, input5]) => [_02, input5], () => If(KeywordSymbol(input4), ([_02, input5]) => [_02, input5], () => [])))), ([_3, input5]) => If(Const("]", input5), ([_4, input6]) => [[_0, _1, _2, _3, _4], input6]))))), ([_0, input2]) => [PropertyKeyIndexMapping(_0), input2]);
    PropertyKey = (input) => If(If(PropertyKeyNumber(input), ([_0, input2]) => [_0, input2], () => If(PropertyKeyIdent(input), ([_0, input2]) => [_0, input2], () => If(PropertyKeyQuoted(input), ([_0, input2]) => [_0, input2], () => If(PropertyKeyIndex(input), ([_0, input2]) => [_0, input2], () => [])))), ([_0, input2]) => [PropertyKeyMapping(_0), input2]);
    Readonly2 = (input) => If(If(If(Const("readonly", input), ([_0, input2]) => [[_0], input2]), ([_0, input2]) => [_0, input2], () => If([[], input], ([_0, input2]) => [_0, input2], () => [])), ([_0, input2]) => [ReadonlyMapping(_0), input2]);
    Optional3 = (input) => If(If(If(Const("?", input), ([_0, input2]) => [[_0], input2]), ([_0, input2]) => [_0, input2], () => If([[], input], ([_0, input2]) => [_0, input2], () => [])), ([_0, input2]) => [OptionalMapping(_0), input2]);
    Property = (input) => If(If(Readonly2(input), ([_0, input2]) => If(PropertyKey(input2), ([_1, input3]) => If(Optional3(input3), ([_2, input4]) => If(Const(":", input4), ([_3, input5]) => If(Type(input5), ([_4, input6]) => [[_0, _1, _2, _3, _4], input6]))))), ([_0, input2]) => [PropertyMapping(_0), input2]);
    PropertyDelimiter = (input) => If(If(If(Const(",", input), ([_0, input2]) => If(Const("\n", input2), ([_1, input3]) => [[_0, _1], input3])), ([_0, input2]) => [_0, input2], () => If(If(Const(";", input), ([_0, input2]) => If(Const("\n", input2), ([_1, input3]) => [[_0, _1], input3])), ([_0, input2]) => [_0, input2], () => If(If(Const(",", input), ([_0, input2]) => [[_0], input2]), ([_0, input2]) => [_0, input2], () => If(If(Const(";", input), ([_0, input2]) => [[_0], input2]), ([_0, input2]) => [_0, input2], () => If(If(Const("\n", input), ([_0, input2]) => [[_0], input2]), ([_0, input2]) => [_0, input2], () => []))))), ([_0, input2]) => [PropertyDelimiterMapping(_0), input2]);
    PropertyList_0 = (input, result = []) => If(If(PropertyDelimiter(input), ([_0, input2]) => If(Property(input2), ([_1, input3]) => [[_0, _1], input3])), ([_0, input2]) => PropertyList_0(input2, [...result, _0]), () => [result, input]);
    PropertyList = (input) => If(If(If(Property(input), ([_0, input2]) => If(PropertyList_0(input2), ([_1, input3]) => If(If(PropertyDelimiter(input3), ([_02, input4]) => [[_02], input4], () => [[], input3]), ([_2, input4]) => [[_0, _1, _2], input4]))), ([_0, input2]) => [_0, input2], () => If([[], input], ([_0, input2]) => [_0, input2], () => [])), ([_0, input2]) => [PropertyListMapping(_0), input2]);
    Properties = (input) => If(If(Const("{", input), ([_0, input2]) => If(PropertyList(input2), ([_1, input3]) => If(Const("}", input3), ([_2, input4]) => [[_0, _1, _2], input4]))), ([_0, input2]) => [PropertiesMapping(_0), input2]);
    _Object_2 = (input) => If(Properties(input), ([_0, input2]) => [_Object_Mapping(_0), input2]);
    ElementNamed = (input) => If(If(If(Ident(input), ([_0, input2]) => If(Const("?", input2), ([_1, input3]) => If(Const(":", input3), ([_2, input4]) => If(Const("readonly", input4), ([_3, input5]) => If(Type(input5), ([_4, input6]) => [[_0, _1, _2, _3, _4], input6]))))), ([_0, input2]) => [_0, input2], () => If(If(Ident(input), ([_0, input2]) => If(Const(":", input2), ([_1, input3]) => If(Const("readonly", input3), ([_2, input4]) => If(Type(input4), ([_3, input5]) => [[_0, _1, _2, _3], input5])))), ([_0, input2]) => [_0, input2], () => If(If(Ident(input), ([_0, input2]) => If(Const("?", input2), ([_1, input3]) => If(Const(":", input3), ([_2, input4]) => If(Type(input4), ([_3, input5]) => [[_0, _1, _2, _3], input5])))), ([_0, input2]) => [_0, input2], () => If(If(Ident(input), ([_0, input2]) => If(Const(":", input2), ([_1, input3]) => If(Type(input3), ([_2, input4]) => [[_0, _1, _2], input4]))), ([_0, input2]) => [_0, input2], () => [])))), ([_0, input2]) => [ElementNamedMapping(_0), input2]);
    ElementBase = (input) => If(If(ElementNamed(input), ([_0, input2]) => [_0, input2], () => If(If(Readonly2(input), ([_0, input2]) => If(Type(input2), ([_1, input3]) => If(Optional3(input3), ([_2, input4]) => [[_0, _1, _2], input4]))), ([_0, input2]) => [_0, input2], () => [])), ([_0, input2]) => [ElementBaseMapping(_0), input2]);
    Element = (input) => If(If(If(Const("...", input), ([_0, input2]) => If(ElementBase(input2), ([_1, input3]) => [[_0, _1], input3])), ([_0, input2]) => [_0, input2], () => If(If(ElementBase(input), ([_0, input2]) => [[_0], input2]), ([_0, input2]) => [_0, input2], () => [])), ([_0, input2]) => [ElementMapping(_0), input2]);
    ElementList_0 = (input, result = []) => If(If(Const(",", input), ([_0, input2]) => If(Element(input2), ([_1, input3]) => [[_0, _1], input3])), ([_0, input2]) => ElementList_0(input2, [...result, _0]), () => [result, input]);
    ElementList = (input) => If(If(If(Element(input), ([_0, input2]) => If(ElementList_0(input2), ([_1, input3]) => If(If(Const(",", input3), ([_02, input4]) => [[_02], input4], () => [[], input3]), ([_2, input4]) => [[_0, _1, _2], input4]))), ([_0, input2]) => [_0, input2], () => If([[], input], ([_0, input2]) => [_0, input2], () => [])), ([_0, input2]) => [ElementListMapping(_0), input2]);
    _Tuple_ = (input) => If(If(Const("[", input), ([_0, input2]) => If(ElementList(input2), ([_1, input3]) => If(Const("]", input3), ([_2, input4]) => [[_0, _1, _2], input4]))), ([_0, input2]) => [_Tuple_Mapping(_0), input2]);
    ParameterReadonlyOptional = (input) => If(If(Ident(input), ([_0, input2]) => If(Const("?", input2), ([_1, input3]) => If(Const(":", input3), ([_2, input4]) => If(Const("readonly", input4), ([_3, input5]) => If(Type(input5), ([_4, input6]) => [[_0, _1, _2, _3, _4], input6]))))), ([_0, input2]) => [ParameterReadonlyOptionalMapping(_0), input2]);
    ParameterReadonly = (input) => If(If(Ident(input), ([_0, input2]) => If(Const(":", input2), ([_1, input3]) => If(Const("readonly", input3), ([_2, input4]) => If(Type(input4), ([_3, input5]) => [[_0, _1, _2, _3], input5])))), ([_0, input2]) => [ParameterReadonlyMapping(_0), input2]);
    ParameterOptional = (input) => If(If(Ident(input), ([_0, input2]) => If(Const("?", input2), ([_1, input3]) => If(Const(":", input3), ([_2, input4]) => If(Type(input4), ([_3, input5]) => [[_0, _1, _2, _3], input5])))), ([_0, input2]) => [ParameterOptionalMapping(_0), input2]);
    ParameterType = (input) => If(If(Ident(input), ([_0, input2]) => If(Const(":", input2), ([_1, input3]) => If(Type(input3), ([_2, input4]) => [[_0, _1, _2], input4]))), ([_0, input2]) => [ParameterTypeMapping(_0), input2]);
    ParameterBase = (input) => If(If(ParameterReadonlyOptional(input), ([_0, input2]) => [_0, input2], () => If(ParameterReadonly(input), ([_0, input2]) => [_0, input2], () => If(ParameterOptional(input), ([_0, input2]) => [_0, input2], () => If(ParameterType(input), ([_0, input2]) => [_0, input2], () => [])))), ([_0, input2]) => [ParameterBaseMapping(_0), input2]);
    Parameter2 = (input) => If(If(If(Const("...", input), ([_0, input2]) => If(ParameterBase(input2), ([_1, input3]) => [[_0, _1], input3])), ([_0, input2]) => [_0, input2], () => If(If(ParameterBase(input), ([_0, input2]) => [[_0], input2]), ([_0, input2]) => [_0, input2], () => [])), ([_0, input2]) => [ParameterMapping(_0), input2]);
    ParameterList_0 = (input, result = []) => If(If(Const(",", input), ([_0, input2]) => If(Parameter2(input2), ([_1, input3]) => [[_0, _1], input3])), ([_0, input2]) => ParameterList_0(input2, [...result, _0]), () => [result, input]);
    ParameterList = (input) => If(If(If(Parameter2(input), ([_0, input2]) => If(ParameterList_0(input2), ([_1, input3]) => If(If(Const(",", input3), ([_02, input4]) => [[_02], input4], () => [[], input3]), ([_2, input4]) => [[_0, _1, _2], input4]))), ([_0, input2]) => [_0, input2], () => If([[], input], ([_0, input2]) => [_0, input2], () => [])), ([_0, input2]) => [ParameterListMapping(_0), input2]);
    _Function_2 = (input) => If(If(Const("(", input), ([_0, input2]) => If(ParameterList(input2), ([_1, input3]) => If(Const(")", input3), ([_2, input4]) => If(Const("=>", input4), ([_3, input5]) => If(Type(input5), ([_4, input6]) => [[_0, _1, _2, _3, _4], input6]))))), ([_0, input2]) => [_Function_Mapping(_0), input2]);
    _Constructor_ = (input) => If(If(Const("new", input), ([_0, input2]) => If(Const("(", input2), ([_1, input3]) => If(ParameterList(input3), ([_2, input4]) => If(Const(")", input4), ([_3, input5]) => If(Const("=>", input5), ([_4, input6]) => If(Type(input6), ([_5, input7]) => [[_0, _1, _2, _3, _4, _5], input7])))))), ([_0, input2]) => [_Constructor_Mapping(_0), input2]);
    MappedReadonly = (input) => If(If(If(Const("+", input), ([_0, input2]) => If(Const("readonly", input2), ([_1, input3]) => [[_0, _1], input3])), ([_0, input2]) => [_0, input2], () => If(If(Const("-", input), ([_0, input2]) => If(Const("readonly", input2), ([_1, input3]) => [[_0, _1], input3])), ([_0, input2]) => [_0, input2], () => If(If(Const("readonly", input), ([_0, input2]) => [[_0], input2]), ([_0, input2]) => [_0, input2], () => If([[], input], ([_0, input2]) => [_0, input2], () => [])))), ([_0, input2]) => [MappedReadonlyMapping(_0), input2]);
    MappedOptional = (input) => If(If(If(Const("+", input), ([_0, input2]) => If(Const("?", input2), ([_1, input3]) => [[_0, _1], input3])), ([_0, input2]) => [_0, input2], () => If(If(Const("-", input), ([_0, input2]) => If(Const("?", input2), ([_1, input3]) => [[_0, _1], input3])), ([_0, input2]) => [_0, input2], () => If(If(Const("?", input), ([_0, input2]) => [[_0], input2]), ([_0, input2]) => [_0, input2], () => If([[], input], ([_0, input2]) => [_0, input2], () => [])))), ([_0, input2]) => [MappedOptionalMapping(_0), input2]);
    MappedAs = (input) => If(If(If(Const("as", input), ([_0, input2]) => If(Type(input2), ([_1, input3]) => [[_0, _1], input3])), ([_0, input2]) => [_0, input2], () => If([[], input], ([_0, input2]) => [_0, input2], () => [])), ([_0, input2]) => [MappedAsMapping(_0), input2]);
    _Mapped_ = (input) => If(If(Const("{", input), ([_0, input2]) => If(MappedReadonly(input2), ([_1, input3]) => If(Const("[", input3), ([_2, input4]) => If(Ident(input4), ([_3, input5]) => If(Const("in", input5), ([_4, input6]) => If(Type(input6), ([_5, input7]) => If(MappedAs(input7), ([_6, input8]) => If(Const("]", input8), ([_7, input9]) => If(MappedOptional(input9), ([_8, input10]) => If(Const(":", input10), ([_9, input11]) => If(Type(input11), ([_10, input12]) => If(OptionalSemiColon(input12), ([_11, input13]) => If(Const("}", input13), ([_12, input14]) => [[_0, _1, _2, _3, _4, _5, _6, _7, _8, _9, _10, _11, _12], input14]))))))))))))), ([_0, input2]) => [_Mapped_Mapping(_0), input2]);
    Reference = (input) => If(Ident(input), ([_0, input2]) => [ReferenceMapping(_0), input2]);
    WithBigInt = (input) => If(BigInt3(input), ([_0, input2]) => [WithBigIntMapping(_0), input2]);
    WithNumber = (input) => If(Number3(input), ([_0, input2]) => [WithNumberMapping(_0), input2]);
    WithBoolean = (input) => If(If(Const("true", input), ([_0, input2]) => [_0, input2], () => If(Const("false", input), ([_0, input2]) => [_0, input2], () => [])), ([_0, input2]) => [WithBooleanMapping(_0), input2]);
    WithString = (input) => If(String3(['"', "'"], input), ([_0, input2]) => [WithStringMapping(_0), input2]);
    WithNull = (input) => If(Const("null", input), ([_0, input2]) => [WithNullMapping(_0), input2]);
    WithUndefined = (input) => If(Const("undefined", input), ([_0, input2]) => [WithUndefinedMapping(_0), input2]);
    WithProperty = (input) => If(If(PropertyKey(input), ([_0, input2]) => If(Const(":", input2), ([_1, input3]) => If(WithValue(input3), ([_2, input4]) => [[_0, _1, _2], input4]))), ([_0, input2]) => [WithPropertyMapping(_0), input2]);
    WithPropertyList_0 = (input, result = []) => If(If(PropertyDelimiter(input), ([_0, input2]) => If(WithProperty(input2), ([_1, input3]) => [[_0, _1], input3])), ([_0, input2]) => WithPropertyList_0(input2, [...result, _0]), () => [result, input]);
    WithPropertyList = (input) => If(If(If(WithProperty(input), ([_0, input2]) => If(WithPropertyList_0(input2), ([_1, input3]) => If(If(PropertyDelimiter(input3), ([_02, input4]) => [[_02], input4], () => [[], input3]), ([_2, input4]) => [[_0, _1, _2], input4]))), ([_0, input2]) => [_0, input2], () => If([[], input], ([_0, input2]) => [_0, input2], () => [])), ([_0, input2]) => [WithPropertyListMapping(_0), input2]);
    WithObject = (input) => If(If(Const("{", input), ([_0, input2]) => If(WithPropertyList(input2), ([_1, input3]) => If(Const("}", input3), ([_2, input4]) => [[_0, _1, _2], input4]))), ([_0, input2]) => [WithObjectMapping(_0), input2]);
    WithElementList_0 = (input, result = []) => If(If(Const(",", input), ([_0, input2]) => If(WithValue(input2), ([_1, input3]) => [[_0, _1], input3])), ([_0, input2]) => WithElementList_0(input2, [...result, _0]), () => [result, input]);
    WithElementList = (input) => If(If(If(WithValue(input), ([_0, input2]) => If(WithElementList_0(input2), ([_1, input3]) => If(If(Const(",", input3), ([_02, input4]) => [[_02], input4], () => [[], input3]), ([_2, input4]) => [[_0, _1, _2], input4]))), ([_0, input2]) => [_0, input2], () => If([[], input], ([_0, input2]) => [_0, input2], () => [])), ([_0, input2]) => [WithElementListMapping(_0), input2]);
    WithArray = (input) => If(If(Const("[", input), ([_0, input2]) => If(WithElementList(input2), ([_1, input3]) => If(Const("]", input3), ([_2, input4]) => [[_0, _1, _2], input4]))), ([_0, input2]) => [WithArrayMapping(_0), input2]);
    WithValue = (input) => If(If(WithBigInt(input), ([_0, input2]) => [_0, input2], () => If(WithNumber(input), ([_0, input2]) => [_0, input2], () => If(WithBoolean(input), ([_0, input2]) => [_0, input2], () => If(WithString(input), ([_0, input2]) => [_0, input2], () => If(WithNull(input), ([_0, input2]) => [_0, input2], () => If(WithUndefined(input), ([_0, input2]) => [_0, input2], () => If(WithObject(input), ([_0, input2]) => [_0, input2], () => If(WithArray(input), ([_0, input2]) => [_0, input2], () => [])))))))), ([_0, input2]) => [WithValueMapping(_0), input2]);
    PatternBigInt = (input) => If(Const("-?(?:0|[1-9][0-9]*)n", input), ([_0, input2]) => [PatternBigIntMapping(_0), input2]);
    PatternString = (input) => If(Const(".*", input), ([_0, input2]) => [PatternStringMapping(_0), input2]);
    PatternNumber = (input) => If(Const("-?(?:0|[1-9][0-9]*)(?:\\.[0-9]+)?", input), ([_0, input2]) => [PatternNumberMapping(_0), input2]);
    PatternInteger = (input) => If(Const("-?(?:0|[1-9][0-9]*)", input), ([_0, input2]) => [PatternIntegerMapping(_0), input2]);
    PatternNever = (input) => If(Const("(?!)", input), ([_0, input2]) => [PatternNeverMapping(_0), input2]);
    PatternText = (input) => If(Until_1(["-?(?:0|[1-9][0-9]*)n", ".*", "-?(?:0|[1-9][0-9]*)(?:\\.[0-9]+)?", "-?(?:0|[1-9][0-9]*)", "(?!)", "(", ")", "$", "|"], input), ([_0, input2]) => [PatternTextMapping(_0), input2]);
    PatternBase = (input) => If(If(PatternBigInt(input), ([_0, input2]) => [_0, input2], () => If(PatternString(input), ([_0, input2]) => [_0, input2], () => If(PatternNumber(input), ([_0, input2]) => [_0, input2], () => If(PatternInteger(input), ([_0, input2]) => [_0, input2], () => If(PatternNever(input), ([_0, input2]) => [_0, input2], () => If(PatternGroup(input), ([_0, input2]) => [_0, input2], () => If(PatternText(input), ([_0, input2]) => [_0, input2], () => []))))))), ([_0, input2]) => [PatternBaseMapping(_0), input2]);
    PatternGroup = (input) => If(If(Const("(", input), ([_0, input2]) => If(PatternBody(input2), ([_1, input3]) => If(Const(")", input3), ([_2, input4]) => [[_0, _1, _2], input4]))), ([_0, input2]) => [PatternGroupMapping(_0), input2]);
    PatternUnion = (input) => If(If(If(PatternTerm(input), ([_0, input2]) => If(Const("|", input2), ([_1, input3]) => If(PatternUnion(input3), ([_2, input4]) => [[_0, _1, _2], input4]))), ([_0, input2]) => [_0, input2], () => If(If(PatternTerm(input), ([_0, input2]) => [[_0], input2]), ([_0, input2]) => [_0, input2], () => If([[], input], ([_0, input2]) => [_0, input2], () => []))), ([_0, input2]) => [PatternUnionMapping(_0), input2]);
    PatternTerm = (input) => If(If(PatternBase(input), ([_0, input2]) => If(PatternBody(input2), ([_1, input3]) => [[_0, _1], input3])), ([_0, input2]) => [PatternTermMapping(_0), input2]);
    PatternBody = (input) => If(If(PatternUnion(input), ([_0, input2]) => [_0, input2], () => If(PatternTerm(input), ([_0, input2]) => [_0, input2], () => [])), ([_0, input2]) => [PatternBodyMapping(_0), input2]);
    Pattern = (input) => If(If(Const("^", input), ([_0, input2]) => If(PatternBody(input2), ([_1, input3]) => If(Const("$", input3), ([_2, input4]) => [[_0, _1, _2], input4]))), ([_0, input2]) => [PatternMapping(_0), input2]);
    InterfaceDeclarationHeritageList_0 = (input, result = []) => If(If(Const(",", input), ([_0, input2]) => If(Type(input2), ([_1, input3]) => [[_0, _1], input3])), ([_0, input2]) => InterfaceDeclarationHeritageList_0(input2, [...result, _0]), () => [result, input]);
    InterfaceDeclarationHeritageList = (input) => If(If(If(Type(input), ([_0, input2]) => If(InterfaceDeclarationHeritageList_0(input2), ([_1, input3]) => If(If(Const(",", input3), ([_02, input4]) => [[_02], input4], () => [[], input3]), ([_2, input4]) => [[_0, _1, _2], input4]))), ([_0, input2]) => [_0, input2], () => If([[], input], ([_0, input2]) => [_0, input2], () => [])), ([_0, input2]) => [InterfaceDeclarationHeritageListMapping(_0), input2]);
    InterfaceDeclarationHeritage = (input) => If(If(If(Const("extends", input), ([_0, input2]) => If(InterfaceDeclarationHeritageList(input2), ([_1, input3]) => [[_0, _1], input3])), ([_0, input2]) => [_0, input2], () => If([[], input], ([_0, input2]) => [_0, input2], () => [])), ([_0, input2]) => [InterfaceDeclarationHeritageMapping(_0), input2]);
    InterfaceDeclarationGeneric = (input) => If(If(Const("interface", input), ([_0, input2]) => If(Ident(input2), ([_1, input3]) => If(GenericParameters(input3), ([_2, input4]) => If(InterfaceDeclarationHeritage(input4), ([_3, input5]) => If(Properties(input5), ([_4, input6]) => [[_0, _1, _2, _3, _4], input6]))))), ([_0, input2]) => [InterfaceDeclarationGenericMapping(_0), input2]);
    InterfaceDeclaration = (input) => If(If(Const("interface", input), ([_0, input2]) => If(Ident(input2), ([_1, input3]) => If(InterfaceDeclarationHeritage(input3), ([_2, input4]) => If(Properties(input4), ([_3, input5]) => [[_0, _1, _2, _3], input5])))), ([_0, input2]) => [InterfaceDeclarationMapping(_0), input2]);
    TypeAliasDeclarationGeneric = (input) => If(If(Const("type", input), ([_0, input2]) => If(Ident(input2), ([_1, input3]) => If(GenericParameters(input3), ([_2, input4]) => If(Const("=", input4), ([_3, input5]) => If(Type(input5), ([_4, input6]) => [[_0, _1, _2, _3, _4], input6]))))), ([_0, input2]) => [TypeAliasDeclarationGenericMapping(_0), input2]);
    TypeAliasDeclaration = (input) => If(If(Const("type", input), ([_0, input2]) => If(Ident(input2), ([_1, input3]) => If(Const("=", input3), ([_2, input4]) => If(Type(input4), ([_3, input5]) => [[_0, _1, _2, _3], input5])))), ([_0, input2]) => [TypeAliasDeclarationMapping(_0), input2]);
    ExportKeyword = (input) => If(If(If(Const("export", input), ([_0, input2]) => [[_0], input2]), ([_0, input2]) => [_0, input2], () => If([[], input], ([_0, input2]) => [_0, input2], () => [])), ([_0, input2]) => [ExportKeywordMapping(_0), input2]);
    ModuleDeclarationDelimiter = (input) => If(If(If(Const(";", input), ([_0, input2]) => If(Const("\n", input2), ([_1, input3]) => [[_0, _1], input3])), ([_0, input2]) => [_0, input2], () => If(If(Const(";", input), ([_0, input2]) => [[_0], input2]), ([_0, input2]) => [_0, input2], () => If(If(Const("\n", input), ([_0, input2]) => [[_0], input2]), ([_0, input2]) => [_0, input2], () => []))), ([_0, input2]) => [ModuleDeclarationDelimiterMapping(_0), input2]);
    ModuleDeclarationList_0 = (input, result = []) => If(If(ModuleDeclarationDelimiter(input), ([_0, input2]) => If(ModuleDeclaration(input2), ([_1, input3]) => [[_0, _1], input3])), ([_0, input2]) => ModuleDeclarationList_0(input2, [...result, _0]), () => [result, input]);
    ModuleDeclarationList = (input) => If(If(If(ModuleDeclaration(input), ([_0, input2]) => If(ModuleDeclarationList_0(input2), ([_1, input3]) => If(If(ModuleDeclarationDelimiter(input3), ([_02, input4]) => [[_02], input4], () => [[], input3]), ([_2, input4]) => [[_0, _1, _2], input4]))), ([_0, input2]) => [_0, input2], () => If([[], input], ([_0, input2]) => [_0, input2], () => [])), ([_0, input2]) => [ModuleDeclarationListMapping(_0), input2]);
    ModuleDeclaration = (input) => If(If(ExportKeyword(input), ([_0, input2]) => If(If(InterfaceDeclarationGeneric(input2), ([_02, input3]) => [_02, input3], () => If(InterfaceDeclaration(input2), ([_02, input3]) => [_02, input3], () => If(TypeAliasDeclarationGeneric(input2), ([_02, input3]) => [_02, input3], () => If(TypeAliasDeclaration(input2), ([_02, input3]) => [_02, input3], () => [])))), ([_1, input3]) => If(OptionalSemiColon(input3), ([_2, input4]) => [[_0, _1, _2], input4]))), ([_0, input2]) => [ModuleDeclarationMapping(_0), input2]);
    Module = (input) => If(If(ModuleDeclaration(input), ([_0, input2]) => If(ModuleDeclarationList(input2), ([_1, input3]) => [[_0, _1], input3])), ([_0, input2]) => [ModuleMapping(_0), input2]);
    Script = (input) => If(If(Module(input), ([_0, input2]) => [_0, input2], () => If(GenericType(input), ([_0, input2]) => [_0, input2], () => If(Type(input), ([_0, input2]) => [_0, input2], () => []))), ([_0, input2]) => [ScriptMapping(_0), input2]);
  }
});

// node_modules/typebox/build/type/engine/patterns/template.mjs
function ParseTemplateIntoTypes(template) {
  const parsed = TemplateLiteralTypes(`\`${template}\``);
  const result = guard_exports.IsEqual(parsed.length, 2) ? parsed[0] : Unreachable();
  return result;
}
var init_template = __esm({
  "node_modules/typebox/build/type/engine/patterns/template.mjs"() {
    init_unreachable2();
    init_guard2();
    init_parser();
  }
});

// node_modules/typebox/build/type/engine/template_literal/encode.mjs
function JoinString(input) {
  return input.join("|");
}
function UnwrapTemplateLiteralPattern(pattern) {
  return pattern.slice(1, pattern.length - 1);
}
function EncodeLiteral(value, right, pattern) {
  return EncodeTypes(right, `${pattern}${value}`);
}
function EncodeBigInt(right, pattern) {
  return EncodeTypes(right, `${pattern}${BigIntPattern}`);
}
function EncodeInteger(right, pattern) {
  return EncodeTypes(right, `${pattern}${IntegerPattern}`);
}
function EncodeNumber(right, pattern) {
  return EncodeTypes(right, `${pattern}${NumberPattern}`);
}
function EncodeBoolean(right, pattern) {
  return EncodeType(Union([Literal("false"), Literal("true")]), right, pattern);
}
function EncodeString(right, pattern) {
  return EncodeTypes(right, `${pattern}${StringPattern}`);
}
function EncodeTemplateLiteral(templatePattern, right, pattern) {
  return EncodeTypes(right, `${pattern}${UnwrapTemplateLiteralPattern(templatePattern)}`);
}
function EncodeTemplateLiteralDeferred(types, right, pattern) {
  const templateLiteral = TemplateLiteralAction(types, {});
  const result = EncodeType(templateLiteral, right, pattern);
  return result;
}
function EncodeEnum(values, right, pattern) {
  const evaluated = EvaluateEnum(values);
  return EncodeType(evaluated, right, pattern);
}
function EncodeUnion(types, right, pattern, result = []) {
  return guard_exports.ShiftLeft(types, (head, tail) => EncodeUnion(tail, right, pattern, [...result, EncodeType(head, [], "")]), () => EncodeTypes(right, `${pattern}(${JoinString(result)})`));
}
function EncodeType(type, right, pattern) {
  return IsEnum(type) ? EncodeEnum(type.enum, right, pattern) : IsInteger2(type) ? EncodeInteger(right, pattern) : IsLiteral(type) ? EncodeLiteral(type.const, right, pattern) : IsBigInt2(type) ? EncodeBigInt(right, pattern) : IsBoolean3(type) ? EncodeBoolean(right, pattern) : IsNumber3(type) ? EncodeNumber(right, pattern) : IsString3(type) ? EncodeString(right, pattern) : IsTemplateLiteral(type) ? EncodeTemplateLiteral(type.pattern, right, pattern) : IsTemplateLiteralDeferred(type) ? EncodeTemplateLiteralDeferred(type.parameters[0], right, pattern) : IsUnion(type) ? EncodeUnion(type.anyOf, right, pattern) : NeverPattern;
}
function EncodeTypes(types, pattern) {
  return guard_exports.ShiftLeft(types, (left, right) => EncodeType(left, right, pattern), () => pattern);
}
function EncodePattern(types) {
  const encoded = EncodeTypes(types, "");
  const result = `^${encoded}$`;
  return result;
}
function TemplateLiteralEncode(types) {
  const pattern = EncodePattern(types);
  const result = TemplateLiteralCreate(pattern);
  return result;
}
var init_encode = __esm({
  "node_modules/typebox/build/type/engine/template_literal/encode.mjs"() {
    init_guard2();
    init_enum();
    init_literal();
    init_union();
    init_template_literal();
    init_bigint();
    init_string2();
    init_number();
    init_integer();
    init_boolean();
    init_never();
    init_create2();
    init_evaluate2();
    init_instantiate2();
  }
});

// node_modules/typebox/build/type/engine/template_literal/instantiate.mjs
function TemplateLiteralAction(types, options2) {
  const result = CanInstantiate(types) ? memory_exports.Update(TemplateLiteralEncode(types), {}, options2) : TemplateLiteralDeferred(types, options2);
  return result;
}
function TemplateLiteralInstantiate(context, state, types, options2) {
  const instantiatedTypes = InstantiateTypes(context, state, types);
  return TemplateLiteralAction(instantiatedTypes, options2);
}
var init_instantiate2 = __esm({
  "node_modules/typebox/build/type/engine/template_literal/instantiate.mjs"() {
    init_memory2();
    init_template_literal();
    init_encode();
    init_instantiate27();
  }
});

// node_modules/typebox/build/type/types/template_literal.mjs
function TemplateLiteralDeferred(types, options2 = {}) {
  return Deferred("TemplateLiteral", [types], options2);
}
function IsTemplateLiteralDeferred(value) {
  return IsSchema(value) && guard_exports.HasPropertyKey(value, "action") && guard_exports.IsEqual(value.action, "TemplateLiteral");
}
function TemplateLiteralFromTypes(types) {
  return TemplateLiteralAction(types, {});
}
function TemplateLiteralFromString(template) {
  const types = ParseTemplateIntoTypes(template);
  return TemplateLiteralFromTypes(types);
}
function TemplateLiteral2(input, options2 = {}) {
  const type = guard_exports.IsString(input) ? TemplateLiteralFromString(input) : TemplateLiteralFromTypes(input);
  return memory_exports.Update(type, {}, options2);
}
function IsTemplateLiteral(value) {
  return IsKind(value, "TemplateLiteral");
}
var init_template_literal = __esm({
  "node_modules/typebox/build/type/types/template_literal.mjs"() {
    init_system();
    init_guard2();
    init_schema();
    init_deferred();
    init_template();
    init_instantiate2();
  }
});

// node_modules/typebox/build/type/extends/result.mjs
var result_exports = {};
__export(result_exports, {
  ExtendsFalse: () => ExtendsFalse,
  ExtendsTrue: () => ExtendsTrue,
  ExtendsUnion: () => ExtendsUnion,
  IsExtendsFalse: () => IsExtendsFalse,
  IsExtendsTrue: () => IsExtendsTrue,
  IsExtendsTrueLike: () => IsExtendsTrueLike,
  IsExtendsUnion: () => IsExtendsUnion,
  Match: () => Match3
});
function ExtendsUnion(inferred) {
  return memory_exports.Create({ ["~kind"]: "ExtendsUnion" }, { inferred });
}
function IsExtendsUnion(value) {
  return guard_exports.IsObject(value) && guard_exports.HasPropertyKey(value, "~kind") && guard_exports.HasPropertyKey(value, "inferred") && guard_exports.IsEqual(value["~kind"], "ExtendsUnion") && guard_exports.IsObject(value.inferred);
}
function ExtendsTrue(inferred) {
  return memory_exports.Create({ ["~kind"]: "ExtendsTrue" }, { inferred });
}
function IsExtendsTrue(value) {
  return guard_exports.IsObject(value) && guard_exports.HasPropertyKey(value, "~kind") && guard_exports.HasPropertyKey(value, "inferred") && guard_exports.IsEqual(value["~kind"], "ExtendsTrue") && guard_exports.IsObject(value.inferred);
}
function ExtendsFalse() {
  return memory_exports.Create({ ["~kind"]: "ExtendsFalse" }, {});
}
function IsExtendsFalse(value) {
  return guard_exports.IsObject(value) && guard_exports.HasPropertyKey(value, "~kind") && guard_exports.IsEqual(value["~kind"], "ExtendsFalse");
}
function IsExtendsTrueLike(value) {
  return IsExtendsUnion(value) || IsExtendsTrue(value);
}
function Match3(result, true_, false_) {
  return IsExtendsTrueLike(result) ? true_(result.inferred) : false_();
}
var init_result = __esm({
  "node_modules/typebox/build/type/extends/result.mjs"() {
    init_guard2();
    init_memory2();
  }
});

// node_modules/typebox/build/type/extends/extends_right.mjs
function ExtendsRightInfer(inferred, name, left, right) {
  return Match3(ExtendsLeft(inferred, left, right), (checkInferred) => ExtendsTrue(memory_exports.Assign(memory_exports.Assign(inferred, checkInferred), { [name]: left })), () => ExtendsFalse());
}
function ExtendsRightAny(inferred, _left) {
  return ExtendsTrue(inferred);
}
function ExtendsRightDependent(inferred, left, if_, then_, else_) {
  return Match3(ExtendsLeft(inferred, left, if_), (inferred2) => Match3(ExtendsLeft(inferred2, left, then_), (inferred3) => ExtendsTrue(inferred3), () => ExtendsFalse()), () => Match3(ExtendsLeft(inferred, left, else_), (inferred2) => ExtendsTrue(inferred2), () => ExtendsFalse()));
}
function ExtendsRightEnum(inferred, left, right) {
  const evaluated = EvaluateEnum(right);
  return ExtendsLeft(inferred, left, evaluated);
}
function ExtendsRightIntersect(inferred, left, right) {
  return guard_exports.ShiftLeft(right, (head, tail) => Match3(ExtendsLeft(inferred, left, head), (inferred2) => ExtendsRightIntersect(inferred2, left, tail), () => ExtendsFalse()), () => ExtendsTrue(inferred));
}
function ExtendsRightTemplateLiteral(inferred, left, right) {
  const evaluated = EvaluateTemplateLiteral(right);
  return ExtendsLeft(inferred, left, evaluated);
}
function ExtendsRightUnion(inferred, left, right) {
  return guard_exports.ShiftLeft(right, (head, tail) => Match3(ExtendsLeft(inferred, left, head), (inferred2) => ExtendsTrue(inferred2), () => ExtendsRightUnion(inferred, left, tail)), () => ExtendsFalse());
}
function ExtendsRight(inferred, left, right) {
  return IsAny(right) ? ExtendsRightAny(inferred, left) : IsDependent(right) ? ExtendsRightDependent(inferred, left, right.if, right.then, right.else) : IsEnum(right) ? ExtendsRightEnum(inferred, left, right.enum) : IsInfer(right) ? ExtendsRightInfer(inferred, right.name, left, right.extends) : IsIntersect(right) ? ExtendsRightIntersect(inferred, left, right.allOf) : IsTemplateLiteral(right) ? ExtendsRightTemplateLiteral(inferred, left, right.pattern) : IsUnion(right) ? ExtendsRightUnion(inferred, left, right.anyOf) : IsUnknown(right) ? ExtendsTrue(inferred) : ExtendsFalse();
}
var init_extends_right = __esm({
  "node_modules/typebox/build/type/extends/extends_right.mjs"() {
    init_guard2();
    init_memory2();
    init_any();
    init_dependent();
    init_enum();
    init_infer();
    init_intersect();
    init_template_literal();
    init_union();
    init_unknown();
    init_extends_left();
    init_result();
    init_evaluate2();
    init_evaluate2();
  }
});

// node_modules/typebox/build/type/extends/any.mjs
function ExtendsAny(inferred, left, right) {
  return IsInfer(right) ? ExtendsRight(inferred, left, right) : IsAny(right) ? ExtendsTrue(inferred) : IsUnknown(right) ? ExtendsTrue(inferred) : ExtendsUnion(inferred);
}
var init_any2 = __esm({
  "node_modules/typebox/build/type/extends/any.mjs"() {
    init_infer();
    init_any();
    init_unknown();
    init_extends_right();
    init_result();
  }
});

// node_modules/typebox/build/type/extends/array.mjs
function ExtendsImmutable(left, right) {
  const isImmutableLeft = IsImmutable(left);
  const isImmutableRight = IsImmutable(right);
  return isImmutableLeft && isImmutableRight ? true : !isImmutableLeft && isImmutableRight ? true : isImmutableLeft && !isImmutableRight ? false : true;
}
function ExtendsArray(inferred, arrayLeft, left, right) {
  return IsArray2(right) ? ExtendsImmutable(arrayLeft, right) ? ExtendsLeft(inferred, left, right.items) : ExtendsFalse() : ExtendsRight(inferred, arrayLeft, right);
}
var init_array2 = __esm({
  "node_modules/typebox/build/type/extends/array.mjs"() {
    init_array();
    init_immutable();
    init_extends_right();
    init_extends_left();
    init_result();
  }
});

// node_modules/typebox/build/type/extends/bigint.mjs
function ExtendsBigInt(inferred, left, right) {
  return IsBigInt2(right) ? ExtendsTrue(inferred) : ExtendsRight(inferred, left, right);
}
var init_bigint3 = __esm({
  "node_modules/typebox/build/type/extends/bigint.mjs"() {
    init_bigint();
    init_extends_right();
    init_result();
  }
});

// node_modules/typebox/build/type/extends/boolean.mjs
function ExtendsBoolean(inferred, left, right) {
  return IsBoolean3(right) ? ExtendsTrue(inferred) : ExtendsRight(inferred, left, right);
}
var init_boolean2 = __esm({
  "node_modules/typebox/build/type/extends/boolean.mjs"() {
    init_boolean();
    init_extends_right();
    init_result();
  }
});

// node_modules/typebox/build/type/extends/parameters.mjs
function ParameterCompare(inferred, left, leftRest, right, rightRest) {
  const checkLeft = IsInfer(right) ? left : right;
  const checkRight = IsInfer(right) ? right : left;
  const isLeftOptional = IsOptional(left);
  const isRightOptional = IsOptional(right);
  return !isLeftOptional && isRightOptional ? ExtendsFalse() : Match3(ExtendsLeft(inferred, checkLeft, checkRight), (inferred2) => ExtendsParameters(inferred2, leftRest, rightRest), () => ExtendsFalse());
}
function ParameterRight(inferred, left, leftRest, rightRest) {
  return guard_exports.ShiftLeft(rightRest, (head, tail) => ParameterCompare(inferred, left, leftRest, head, tail), () => IsOptional(left) ? ExtendsTrue(inferred) : ExtendsFalse());
}
function ParametersLeft(inferred, left, rightRest) {
  return guard_exports.ShiftLeft(left, (head, tail) => ParameterRight(inferred, head, tail, rightRest), () => ExtendsTrue(inferred));
}
function ExtendsParameters(inferred, left, right) {
  return ParametersLeft(inferred, left, right);
}
var init_parameters = __esm({
  "node_modules/typebox/build/type/extends/parameters.mjs"() {
    init_guard2();
    init_infer();
    init_optional();
    init_extends_left();
    init_result();
  }
});

// node_modules/typebox/build/type/extends/return_type.mjs
function ExtendsReturnType(inferred, left, right) {
  return IsVoid(right) ? ExtendsTrue(inferred) : ExtendsLeft(inferred, left, right);
}
var init_return_type = __esm({
  "node_modules/typebox/build/type/extends/return_type.mjs"() {
    init_void();
    init_extends_left();
    init_result();
  }
});

// node_modules/typebox/build/type/extends/constructor.mjs
function ExtendsConstructor(inferred, parameters, returnType, right) {
  return IsAny(right) ? ExtendsTrue(inferred) : IsUnknown(right) ? ExtendsTrue(inferred) : IsConstructor2(right) ? Match3(ExtendsParameters(inferred, parameters, right["parameters"]), (inferred2) => ExtendsReturnType(inferred2, returnType, right["instanceType"]), () => ExtendsFalse()) : ExtendsFalse();
}
var init_constructor2 = __esm({
  "node_modules/typebox/build/type/extends/constructor.mjs"() {
    init_any();
    init_constructor();
    init_unknown();
    init_result();
    init_parameters();
    init_return_type();
  }
});

// node_modules/typebox/build/type/extends/dependent.mjs
function ExtendsDependent(inferred, if_, then_, else_, right) {
  return Match3(ExtendsLeft(inferred, if_, right), () => ExtendsLeft(inferred, then_, right), () => ExtendsLeft(inferred, else_, right));
}
var init_dependent2 = __esm({
  "node_modules/typebox/build/type/extends/dependent.mjs"() {
    init_extends_left();
    init_result();
  }
});

// node_modules/typebox/build/type/extends/enum.mjs
function ExtendsEnum(inferred, left, right) {
  const evaluated = EvaluateEnum(left);
  return ExtendsLeft(inferred, evaluated, right);
}
var init_enum2 = __esm({
  "node_modules/typebox/build/type/extends/enum.mjs"() {
    init_extends_left();
    init_evaluate2();
  }
});

// node_modules/typebox/build/type/extends/function.mjs
function ExtendsFunction(inferred, parameters, returnType, right) {
  return IsAny(right) ? ExtendsTrue(inferred) : IsUnknown(right) ? ExtendsTrue(inferred) : IsFunction2(right) ? Match3(ExtendsParameters(inferred, parameters, right["parameters"]), (inferred2) => ExtendsReturnType(inferred2, returnType, right["returnType"]), () => ExtendsFalse()) : ExtendsFalse();
}
var init_function2 = __esm({
  "node_modules/typebox/build/type/extends/function.mjs"() {
    init_any();
    init_function();
    init_unknown();
    init_result();
    init_parameters();
    init_return_type();
  }
});

// node_modules/typebox/build/type/extends/integer.mjs
function ExtendsInteger(inferred, left, right) {
  return IsInteger2(right) ? ExtendsTrue(inferred) : IsNumber3(right) ? ExtendsTrue(inferred) : ExtendsRight(inferred, left, right);
}
var init_integer3 = __esm({
  "node_modules/typebox/build/type/extends/integer.mjs"() {
    init_integer();
    init_number();
    init_extends_right();
    init_result();
  }
});

// node_modules/typebox/build/type/extends/intersect.mjs
function ExtendsIntersect(inferred, left, right) {
  const evaluated = EvaluateIntersect(left);
  return ExtendsLeft(inferred, evaluated, right);
}
var init_intersect2 = __esm({
  "node_modules/typebox/build/type/extends/intersect.mjs"() {
    init_extends_left();
    init_evaluate3();
  }
});

// node_modules/typebox/build/type/extends/literal.mjs
function ExtendsLiteralValue(inferred, left, right) {
  return left === right ? ExtendsTrue(inferred) : ExtendsFalse();
}
function ExtendsLiteralBigInt(inferred, left, right) {
  return IsLiteral(right) ? ExtendsLiteralValue(inferred, left, right.const) : IsBigInt2(right) ? ExtendsTrue(inferred) : ExtendsRight(inferred, Literal(left), right);
}
function ExtendsLiteralBoolean(inferred, left, right) {
  return IsLiteral(right) ? ExtendsLiteralValue(inferred, left, right.const) : IsBoolean3(right) ? ExtendsTrue(inferred) : ExtendsRight(inferred, Literal(left), right);
}
function ExtendsLiteralNumber(inferred, left, right) {
  return IsLiteral(right) ? ExtendsLiteralValue(inferred, left, right.const) : IsNumber3(right) ? ExtendsTrue(inferred) : ExtendsRight(inferred, Literal(left), right);
}
function ExtendsLiteralString(inferred, left, right) {
  return IsLiteral(right) ? ExtendsLiteralValue(inferred, left, right.const) : IsString3(right) ? ExtendsTrue(inferred) : ExtendsRight(inferred, Literal(left), right);
}
function ExtendsLiteral(inferred, left, right) {
  return guard_exports.IsBigInt(left.const) ? ExtendsLiteralBigInt(inferred, left.const, right) : guard_exports.IsBoolean(left.const) ? ExtendsLiteralBoolean(inferred, left.const, right) : guard_exports.IsNumber(left.const) ? ExtendsLiteralNumber(inferred, left.const, right) : guard_exports.IsString(left.const) ? ExtendsLiteralString(inferred, left.const, right) : Unreachable();
}
var init_literal2 = __esm({
  "node_modules/typebox/build/type/extends/literal.mjs"() {
    init_guard2();
    init_unreachable();
    init_literal();
    init_bigint();
    init_boolean();
    init_number();
    init_string2();
    init_extends_right();
    init_result();
  }
});

// node_modules/typebox/build/type/extends/never.mjs
function ExtendsNever(inferred, left, right) {
  return IsInfer(right) ? ExtendsRight(inferred, left, right) : ExtendsTrue(inferred);
}
var init_never2 = __esm({
  "node_modules/typebox/build/type/extends/never.mjs"() {
    init_infer();
    init_extends_right();
    init_result();
  }
});

// node_modules/typebox/build/type/extends/null.mjs
function ExtendsNull(inferred, left, right) {
  return IsNull2(right) ? ExtendsTrue(inferred) : ExtendsRight(inferred, left, right);
}
var init_null2 = __esm({
  "node_modules/typebox/build/type/extends/null.mjs"() {
    init_null();
    init_extends_right();
    init_result();
  }
});

// node_modules/typebox/build/type/extends/number.mjs
function ExtendsNumber(inferred, left, right) {
  return IsNumber3(right) ? ExtendsTrue(inferred) : ExtendsRight(inferred, left, right);
}
var init_number3 = __esm({
  "node_modules/typebox/build/type/extends/number.mjs"() {
    init_number();
    init_extends_right();
    init_result();
  }
});

// node_modules/typebox/build/type/extends/object.mjs
function ExtendsPropertyOptional(inferred, left, right) {
  return IsOptional(left) ? IsOptional(right) ? ExtendsTrue(inferred) : ExtendsFalse() : ExtendsTrue(inferred);
}
function ExtendsProperty(inferred, left, right) {
  return (
    // Right TInfer<TNever> is TExtendsFalse
    IsInfer(right) && IsNever(right.extends) ? ExtendsFalse() : Match3(ExtendsLeft(inferred, left, right), (inferred2) => ExtendsPropertyOptional(inferred2, left, right), () => ExtendsFalse())
  );
}
function ExtractInferredProperties(keys, properties) {
  return keys.reduce((result, key) => {
    return key in properties ? IsExtendsTrueLike(properties[key]) ? { ...result, ...properties[key].inferred } : Unreachable() : Unreachable();
  }, {});
}
function ExtendsPropertiesComparer(inferred, left, right) {
  const properties = {};
  for (const rightKey of guard_exports.Keys(right)) {
    properties[rightKey] = rightKey in left ? ExtendsProperty({}, left[rightKey], right[rightKey]) : IsOptional(right[rightKey]) ? IsInfer(right[rightKey]) ? ExtendsTrue(memory_exports.Assign(inferred, { [right[rightKey].name]: right[rightKey].extends })) : ExtendsTrue(inferred) : ExtendsFalse();
  }
  const checked = guard_exports.Values(properties).every((result) => IsExtendsTrueLike(result));
  const extracted = checked ? ExtractInferredProperties(guard_exports.Keys(properties), properties) : {};
  return checked ? ExtendsTrue(extracted) : ExtendsFalse();
}
function ExtendsProperties(inferred, left, right) {
  const compared = ExtendsPropertiesComparer(inferred, left, right);
  return IsExtendsTrueLike(compared) ? ExtendsTrue(memory_exports.Assign(inferred, compared.inferred)) : ExtendsFalse();
}
function ExtendsObjectToObject(inferred, left, right) {
  return ExtendsProperties(inferred, left, right);
}
function RecordMergeInferred(left, right) {
  return guard_exports.Keys(right).reduce((result, key) => {
    return {
      ...result,
      [key]: guard_exports.HasPropertyKey(left, key) ? IsUnion(result[key]) ? Union([...result[key].anyOf, right[key]]) : Union([left[key], right[key]]) : right[key]
    };
  }, left);
}
function ExtendsRecordComparer(properties, keys, type, result) {
  return guard_exports.ShiftLeft(keys, (left, right) => Match3(ExtendsLeft({}, properties[left], type), (inferred) => ExtendsRecordComparer(properties, right, type, RecordMergeInferred(result, inferred)), () => ExtendsFalse()), () => ExtendsTrue(result));
}
function ExtendsObjectToRecord(inferred, properties, _pattern, value) {
  const keys = guard_exports.Keys(properties);
  const result = ExtendsRecordComparer(properties, keys, value, inferred);
  return result;
}
function ExtendsObject(inferred, left, right) {
  return IsRecord(right) ? ExtendsObjectToRecord(inferred, left, RecordPattern(right), RecordValue(right)) : IsObject2(right) ? ExtendsObjectToObject(inferred, left, right.properties) : ExtendsRight(inferred, _Object_(left), right);
}
var init_object2 = __esm({
  "node_modules/typebox/build/type/extends/object.mjs"() {
    init_unreachable2();
    init_memory2();
    init_guard2();
    init_optional();
    init_infer();
    init_never();
    init_object();
    init_record();
    init_union();
    init_extends_left();
    init_extends_right();
    init_result();
  }
});

// node_modules/typebox/build/type/extends/record.mjs
function FromObject2(inferred, properties) {
  return guard_exports.IsEqual(guard_exports.Keys(properties).length, 0) ? ExtendsTrue(inferred) : ExtendsFalse();
}
function FromRecord(inferred, _leftKey, leftValue, _rightKey, rightValue) {
  return ExtendsLeft(inferred, leftValue, rightValue);
}
function ExtendsRecord(inferred, leftPattern, leftValue, right) {
  return IsRecord(right) ? FromRecord(inferred, RecordPatternToType(leftPattern), leftValue, RecordPatternToType(RecordPattern(right)), RecordValue(right)) : IsObject2(right) ? FromObject2(inferred, right.properties) : IsAny(right) ? ExtendsTrue(inferred) : IsUnknown(right) ? ExtendsTrue(inferred) : ExtendsFalse();
}
var init_record2 = __esm({
  "node_modules/typebox/build/type/extends/record.mjs"() {
    init_guard2();
    init_any();
    init_unknown();
    init_object();
    init_record();
    init_extends_left();
    init_result();
  }
});

// node_modules/typebox/build/type/extends/string.mjs
function ExtendsString(inferred, left, right) {
  return IsString3(right) ? ExtendsTrue(inferred) : ExtendsRight(inferred, left, right);
}
var init_string4 = __esm({
  "node_modules/typebox/build/type/extends/string.mjs"() {
    init_string2();
    init_extends_right();
    init_result();
  }
});

// node_modules/typebox/build/type/extends/symbol.mjs
function ExtendsSymbol(inferred, left, right) {
  return IsSymbol2(right) ? ExtendsTrue(inferred) : ExtendsRight(inferred, left, right);
}
var init_symbol2 = __esm({
  "node_modules/typebox/build/type/extends/symbol.mjs"() {
    init_symbol();
    init_extends_right();
    init_result();
  }
});

// node_modules/typebox/build/type/extends/template_literal.mjs
function ExtendsTemplateLiteral(inferred, left, right) {
  const evaluated = EvaluateTemplateLiteral(left);
  return ExtendsLeft(inferred, evaluated, right);
}
var init_template_literal2 = __esm({
  "node_modules/typebox/build/type/extends/template_literal.mjs"() {
    init_extends_left();
    init_evaluate2();
  }
});

// node_modules/typebox/build/type/extends/inference.mjs
function Inferrable(name, type) {
  return memory_exports.Create({ "~kind": "Inferrable" }, { name, type }, {});
}
function IsInferable(value) {
  return guard_exports.IsObject(value) && guard_exports.HasPropertyKey(value, "~kind") && guard_exports.HasPropertyKey(value, "name") && guard_exports.HasPropertyKey(value, "type") && guard_exports.IsEqual(value["~kind"], "Inferrable") && guard_exports.IsString(value.name) && guard_exports.IsObject(value.type);
}
function TryRestInferable(type) {
  return IsRest(type) ? IsInfer(type.items) ? IsArray2(type.items.extends) ? Inferrable(type.items.name, type.items.extends.items) : IsUnknown(type.items.extends) ? Inferrable(type.items.name, type.items.extends) : void 0 : Unreachable() : void 0;
}
function TryInferable(type) {
  return IsInfer(type) ? Inferrable(type.name, type.extends) : void 0;
}
function TryInferResults(rest2, right, result = []) {
  return guard_exports.ShiftLeft(rest2, (head, tail) => Match3(ExtendsLeft({}, head, right), () => TryInferResults(tail, right, [...result, head]), () => void 0), () => result);
}
function InferTupleResult(inferred, name, left, right) {
  const results = TryInferResults(left, right);
  return guard_exports.IsArray(results) ? ExtendsTrue(memory_exports.Assign(inferred, { [name]: Tuple(results) })) : ExtendsFalse();
}
function InferUnionResult(inferred, name, left, right) {
  const results = TryInferResults(left, right);
  return guard_exports.IsArray(results) ? ExtendsTrue(memory_exports.Assign(inferred, { [name]: Union(results) })) : ExtendsFalse();
}
var init_inference = __esm({
  "node_modules/typebox/build/type/extends/inference.mjs"() {
    init_unreachable2();
    init_memory2();
    init_guard2();
    init_array();
    init_unknown();
    init_tuple();
    init_extends_left();
    init_union();
    init_infer();
    init_rest();
    init_result();
  }
});

// node_modules/typebox/build/type/extends/tuple.mjs
function Reverse(types) {
  return [...types].reverse();
}
function ApplyReverse(types, reversed) {
  return reversed ? Reverse(types) : types;
}
function Reversed(types) {
  const first = types.length > 0 ? types[0] : void 0;
  const inferrable = IsSchema(first) ? TryRestInferable(first) : void 0;
  return IsSchema(inferrable);
}
function ElementsCompare(inferred, reversed, left, leftRest, right, rightRest) {
  return Match3(ExtendsLeft(inferred, left, right), (checkInferred) => Elements(checkInferred, reversed, leftRest, rightRest), () => ExtendsFalse());
}
function ElementsLeft(inferred, reversed, leftRest, right, rightRest) {
  const inferable = TryRestInferable(right);
  return (
    // Rest Inferrable Right Means we delegate to TInferTupleResult to Generate a Result
    IsInferable(inferable) ? InferTupleResult(inferred, inferable["name"], ApplyReverse(leftRest, reversed), inferable["type"]) : guard_exports.ShiftLeft(leftRest, (head, tail) => ElementsCompare(inferred, reversed, head, tail, right, rightRest), () => ExtendsFalse())
  );
}
function ElementsRight(inferred, reversed, leftRest, rightRest) {
  return guard_exports.ShiftLeft(rightRest, (head, tail) => ElementsLeft(inferred, reversed, leftRest, head, tail), () => guard_exports.IsEqual(leftRest.length, 0) ? ExtendsTrue(inferred) : ExtendsFalse());
}
function Elements(inferred, reversed, leftRest, rightRest) {
  return ElementsRight(inferred, reversed, leftRest, rightRest);
}
function ExtendsTupleToTuple(inferred, left, right) {
  const instantiatedRight = InstantiateElements(inferred, State([], []), right);
  const reversed = Reversed(instantiatedRight);
  return Elements(inferred, reversed, ApplyReverse(left, reversed), ApplyReverse(instantiatedRight, reversed));
}
function ExtendsTupleToArray(inferred, left, right) {
  const inferrable = TryInferable(right);
  return IsInferable(inferrable) ? InferUnionResult(inferred, inferrable["name"], left, inferrable["type"]) : guard_exports.ShiftLeft(left, (head, tail) => Match3(ExtendsLeft(inferred, head, right), (inferred2) => ExtendsTupleToArray(inferred2, tail, right), () => ExtendsFalse()), () => ExtendsTrue(inferred));
}
function ExtendsTuple(inferred, left, right) {
  const instantiatedLeft = InstantiateElements(inferred, State([], []), left);
  return IsTuple(right) ? ExtendsTupleToTuple(inferred, instantiatedLeft, right.items) : IsArray2(right) ? ExtendsTupleToArray(inferred, instantiatedLeft, right.items) : ExtendsRight(inferred, Tuple(instantiatedLeft), right);
}
var init_tuple2 = __esm({
  "node_modules/typebox/build/type/extends/tuple.mjs"() {
    init_guard2();
    init_schema();
    init_array();
    init_tuple();
    init_extends_left();
    init_extends_right();
    init_result();
    init_instantiate27();
    init_instantiate27();
    init_inference();
  }
});

// node_modules/typebox/build/type/extends/undefined.mjs
function ExtendsUndefined(inferred, left, right) {
  return IsVoid(right) ? ExtendsTrue(inferred) : IsUndefined2(right) ? ExtendsTrue(inferred) : ExtendsRight(inferred, left, right);
}
var init_undefined2 = __esm({
  "node_modules/typebox/build/type/extends/undefined.mjs"() {
    init_undefined();
    init_void();
    init_extends_right();
    init_result();
  }
});

// node_modules/typebox/build/type/extends/union.mjs
function ExtendsUnionSome(inferred, type, unionTypes) {
  return guard_exports.ShiftLeft(unionTypes, (head, tail) => Match3(ExtendsLeft(inferred, type, head), (inferred2) => ExtendsTrue(inferred2), () => ExtendsUnionSome(inferred, type, tail)), () => ExtendsFalse());
}
function ExtendsUnionLeft(inferred, left, right) {
  return guard_exports.ShiftLeft(left, (head, tail) => Match3(ExtendsUnionSome(inferred, head, right), (inferred2) => ExtendsUnionLeft(inferred2, tail, right), () => ExtendsFalse()), () => ExtendsTrue(inferred));
}
function ExtendsUnion2(inferred, left, right) {
  const inferrable = TryInferable(right);
  return IsInferable(inferrable) ? InferUnionResult(inferred, inferrable.name, left, inferrable.type) : IsUnion(right) ? ExtendsUnionLeft(inferred, left, right.anyOf) : ExtendsUnionLeft(inferred, left, [right]);
}
var init_union2 = __esm({
  "node_modules/typebox/build/type/extends/union.mjs"() {
    init_guard2();
    init_union();
    init_extends_left();
    init_result();
    init_inference();
  }
});

// node_modules/typebox/build/type/extends/unknown.mjs
function ExtendsUnknown(inferred, left, right) {
  return IsInfer(right) ? ExtendsRight(inferred, left, right) : IsAny(right) ? ExtendsTrue(inferred) : IsUnknown(right) ? ExtendsTrue(inferred) : ExtendsFalse();
}
var init_unknown2 = __esm({
  "node_modules/typebox/build/type/extends/unknown.mjs"() {
    init_any();
    init_unknown();
    init_infer();
    init_extends_right();
    init_result();
  }
});

// node_modules/typebox/build/type/extends/void.mjs
function ExtendsVoid(inferred, left, right) {
  return IsVoid(right) ? ExtendsTrue(inferred) : ExtendsRight(inferred, left, right);
}
var init_void2 = __esm({
  "node_modules/typebox/build/type/extends/void.mjs"() {
    init_void();
    init_extends_right();
    init_result();
  }
});

// node_modules/typebox/build/type/extends/extends_left.mjs
function ExtendsLeft(inferred, left, right) {
  return IsAny(left) ? ExtendsAny(inferred, left, right) : IsArray2(left) ? ExtendsArray(inferred, left, left.items, right) : IsBigInt2(left) ? ExtendsBigInt(inferred, left, right) : IsBoolean3(left) ? ExtendsBoolean(inferred, left, right) : IsConstructor2(left) ? ExtendsConstructor(inferred, left.parameters, left.instanceType, right) : IsDependent(left) ? ExtendsDependent(inferred, left.if, left.then, left.else, right) : IsEnum(left) ? ExtendsEnum(inferred, left.enum, right) : IsFunction2(left) ? ExtendsFunction(inferred, left.parameters, left.returnType, right) : IsInteger2(left) ? ExtendsInteger(inferred, left, right) : IsIntersect(left) ? ExtendsIntersect(inferred, left.allOf, right) : IsLiteral(left) ? ExtendsLiteral(inferred, left, right) : IsNever(left) ? ExtendsNever(inferred, left, right) : IsNull2(left) ? ExtendsNull(inferred, left, right) : IsNumber3(left) ? ExtendsNumber(inferred, left, right) : IsObject2(left) ? ExtendsObject(inferred, left.properties, right) : IsRecord(left) ? ExtendsRecord(inferred, RecordPattern(left), RecordValue(left), right) : IsString3(left) ? ExtendsString(inferred, left, right) : IsSymbol2(left) ? ExtendsSymbol(inferred, left, right) : IsTemplateLiteral(left) ? ExtendsTemplateLiteral(inferred, left.pattern, right) : IsTuple(left) ? ExtendsTuple(inferred, left.items, right) : IsUndefined2(left) ? ExtendsUndefined(inferred, left, right) : IsUnion(left) ? ExtendsUnion2(inferred, left.anyOf, right) : IsUnknown(left) ? ExtendsUnknown(inferred, left, right) : IsVoid(left) ? ExtendsVoid(inferred, left, right) : ExtendsFalse();
}
var init_extends_left = __esm({
  "node_modules/typebox/build/type/extends/extends_left.mjs"() {
    init_any2();
    init_array2();
    init_bigint3();
    init_boolean2();
    init_constructor2();
    init_dependent2();
    init_enum2();
    init_function2();
    init_integer3();
    init_intersect2();
    init_literal2();
    init_never2();
    init_null2();
    init_number3();
    init_object2();
    init_record2();
    init_string4();
    init_symbol2();
    init_template_literal2();
    init_tuple2();
    init_undefined2();
    init_union2();
    init_unknown2();
    init_void2();
    init_any();
    init_array();
    init_bigint();
    init_boolean();
    init_constructor();
    init_dependent();
    init_enum();
    init_function();
    init_integer();
    init_intersect();
    init_literal();
    init_never();
    init_null();
    init_number();
    init_object();
    init_record();
    init_string2();
    init_symbol();
    init_template_literal();
    init_tuple();
    init_undefined();
    init_unknown();
    init_union();
    init_void();
    init_result();
  }
});

// node_modules/typebox/build/type/engine/interface/instantiate.mjs
function InterfaceOperation(heritage, properties) {
  const result = EvaluateIntersect([...heritage, _Object_(properties)]);
  return result;
}
function InterfaceAction(heritage, properties, options2) {
  const result = CanInstantiate(heritage) ? memory_exports.Update(InterfaceOperation(heritage, properties), {}, options2) : InterfaceDeferred(heritage, properties, options2);
  return result;
}
function InterfaceInstantiate(context, state, heritage, properties, options2) {
  const instantiatedHeritage = InstantiateTypes(context, state, heritage);
  const instantiatedProperties = InstantiateProperties(context, state, properties);
  return InterfaceAction(instantiatedHeritage, instantiatedProperties, options2);
}
var init_instantiate3 = __esm({
  "node_modules/typebox/build/type/engine/interface/instantiate.mjs"() {
    init_memory2();
    init_object();
    init_evaluate2();
    init_action();
    init_instantiate27();
    init_instantiate27();
    init_instantiate27();
  }
});

// node_modules/typebox/build/type/action/interface.mjs
function InterfaceDeferred(heritage, properties, options2 = {}) {
  return Deferred("Interface", [heritage, properties], options2);
}
function IsInterfaceDeferred(value) {
  return IsSchema(value) && guard_exports.HasPropertyKey(value, "action") && guard_exports.IsEqual(value.action, "Interface");
}
function Interface(heritage, properties, options2 = {}) {
  return InterfaceAction(heritage, properties, options2);
}
var init_interface = __esm({
  "node_modules/typebox/build/type/action/interface.mjs"() {
    init_guard2();
    init_schema();
    init_deferred();
    init_instantiate3();
  }
});

// node_modules/typebox/build/type/engine/cyclic/check.mjs
function FromRef(stack, context, ref) {
  return stack.includes(ref) ? true : FromType3([...stack, ref], context, context[ref]);
}
function FromProperties(stack, context, properties) {
  const types = PropertyValues(properties);
  return FromTypes2(stack, context, types);
}
function FromTypes2(stack, context, types) {
  return guard_exports.ShiftLeft(types, (left, right) => FromType3(stack, context, left) ? true : FromTypes2(stack, context, right), () => false);
}
function FromType3(stack, context, type) {
  return IsRef(type) ? FromRef(stack, context, type.$ref) : IsArray2(type) ? FromType3(stack, context, type.items) : IsConstructor2(type) ? FromTypes2(stack, context, [...type.parameters, type.instanceType]) : IsFunction2(type) ? FromTypes2(stack, context, [...type.parameters, type.returnType]) : IsInterfaceDeferred(type) ? FromProperties(stack, context, type.parameters[1]) : IsIntersect(type) ? FromTypes2(stack, context, type.allOf) : IsObject2(type) ? FromProperties(stack, context, type.properties) : IsUnion(type) ? FromTypes2(stack, context, type.anyOf) : IsTuple(type) ? FromTypes2(stack, context, type.items) : IsRecord(type) ? FromType3(stack, context, RecordValue(type)) : false;
}
function CyclicCheck(stack, context, type) {
  const result = FromType3(stack, context, type);
  return result;
}
var init_check = __esm({
  "node_modules/typebox/build/type/engine/cyclic/check.mjs"() {
    init_guard2();
    init_array();
    init_constructor();
    init_function();
    init_intersect();
    init_object();
    init_properties();
    init_record();
    init_tuple();
    init_union();
    init_ref();
    init_interface();
  }
});

// node_modules/typebox/build/type/engine/cyclic/candidates.mjs
function ResolveCandidateKeys(context, keys) {
  return keys.reduce((result, left) => {
    return CyclicCheck([left], context, context[left]) ? [...result, left] : result;
  }, []);
}
function CyclicCandidates(context) {
  const keys = PropertyKeys(context);
  const result = ResolveCandidateKeys(context, keys);
  return result;
}
var init_candidates = __esm({
  "node_modules/typebox/build/type/engine/cyclic/candidates.mjs"() {
    init_properties();
    init_check();
  }
});

// node_modules/typebox/build/type/engine/cyclic/dependencies.mjs
function FromRef2(context, ref, result) {
  return result.includes(ref) ? result : ref in context ? FromType4(context, context[ref], [...result, ref]) : Unreachable();
}
function FromProperties2(context, properties, result) {
  const types = PropertyValues(properties);
  return FromTypes3(context, types, result);
}
function FromTypes3(context, types, result) {
  return types.reduce((result2, left) => {
    return FromType4(context, left, result2);
  }, result);
}
function FromType4(context, type, result) {
  return IsRef(type) ? FromRef2(context, type.$ref, result) : IsArray2(type) ? FromType4(context, type.items, result) : IsConstructor2(type) ? FromTypes3(context, [...type.parameters, type.instanceType], result) : IsFunction2(type) ? FromTypes3(context, [...type.parameters, type.returnType], result) : IsInterfaceDeferred(type) ? FromProperties2(context, type.parameters[1], result) : IsIntersect(type) ? FromTypes3(context, type.allOf, result) : IsObject2(type) ? FromProperties2(context, type.properties, result) : IsUnion(type) ? FromTypes3(context, type.anyOf, result) : IsTuple(type) ? FromTypes3(context, type.items, result) : IsRecord(type) ? FromType4(context, RecordValue(type), result) : result;
}
function CyclicDependencies(context, key, type) {
  const result = FromType4(context, type, [key]);
  return result;
}
var init_dependencies = __esm({
  "node_modules/typebox/build/type/engine/cyclic/dependencies.mjs"() {
    init_unreachable2();
    init_array();
    init_constructor();
    init_function();
    init_intersect();
    init_object();
    init_properties();
    init_record();
    init_tuple();
    init_union();
    init_ref();
    init_interface();
  }
});

// node_modules/typebox/build/type/engine/cyclic/extends.mjs
function FromRef3(_ref) {
  return Any();
}
function FromProperties3(properties) {
  return guard_exports.Keys(properties).reduce((result, key) => {
    return { ...result, [key]: FromType5(properties[key]) };
  }, {});
}
function FromTypes4(types) {
  return types.reduce((result, left) => {
    return [...result, FromType5(left)];
  }, []);
}
function FromType5(type) {
  return IsRef(type) ? FromRef3(type.$ref) : IsArray2(type) ? _Array_(FromType5(type.items), ArrayOptions(type)) : IsConstructor2(type) ? Constructor(FromTypes4(type.parameters), FromType5(type.instanceType)) : IsFunction2(type) ? _Function_(FromTypes4(type.parameters), FromType5(type.returnType)) : IsIntersect(type) ? Intersect(FromTypes4(type.allOf)) : IsObject2(type) ? _Object_(FromProperties3(type.properties)) : IsRecord(type) ? Record(RecordKey(type), FromType5(RecordValue(type))) : IsUnion(type) ? Union(FromTypes4(type.anyOf)) : IsTuple(type) ? Tuple(FromTypes4(type.items)) : type;
}
function CyclicAnyFromParameters(defs, ref) {
  return ref in defs ? FromType5(defs[ref]) : Unknown();
}
function CyclicExtends(type) {
  return CyclicAnyFromParameters(type.$defs, type.$ref);
}
var init_extends = __esm({
  "node_modules/typebox/build/type/engine/cyclic/extends.mjs"() {
    init_guard2();
    init_any();
    init_array();
    init_constructor();
    init_function();
    init_intersect();
    init_object();
    init_record();
    init_ref();
    init_tuple();
    init_union();
    init_unknown();
  }
});

// node_modules/typebox/build/type/engine/cyclic/instantiate.mjs
function CyclicInterface(context, heritage, properties) {
  const instantiatedHeritage = InstantiateTypes(context, State([], []), heritage);
  const instantiatedProperties = InstantiateProperties({}, State([], []), properties);
  const evaluatedInterface = EvaluateIntersect([...instantiatedHeritage, _Object_(instantiatedProperties)]);
  return evaluatedInterface;
}
function CyclicDefinitions(context, dependencies) {
  const keys = guard_exports.Keys(context).filter((key) => dependencies.includes(key));
  return keys.reduce((result, key) => {
    const type = context[key];
    const instantiatedType = IsInterfaceDeferred(type) ? CyclicInterface(context, type.parameters[0], type.parameters[1]) : type;
    return { ...result, [key]: instantiatedType };
  }, {});
}
function InstantiateCyclic(context, ref, type) {
  const dependencies = CyclicDependencies(context, ref, type);
  const definitions = CyclicDefinitions(context, dependencies);
  const result = Cyclic(definitions, ref);
  return result;
}
var init_instantiate4 = __esm({
  "node_modules/typebox/build/type/engine/cyclic/instantiate.mjs"() {
    init_guard2();
    init_cyclic();
    init_object();
    init_dependencies();
    init_action();
    init_instantiate27();
    init_instantiate27();
    init_instantiate27();
    init_evaluate2();
  }
});

// node_modules/typebox/build/type/engine/cyclic/target.mjs
function Resolve(defs, ref) {
  return ref in defs ? IsRef(defs[ref]) ? Resolve(defs, defs[ref].$ref) : defs[ref] : Never();
}
function CyclicTarget(defs, ref) {
  const result = Resolve(defs, ref);
  return result;
}
var init_target = __esm({
  "node_modules/typebox/build/type/engine/cyclic/target.mjs"() {
    init_never();
    init_ref();
  }
});

// node_modules/typebox/build/type/engine/cyclic/index.mjs
var init_cyclic2 = __esm({
  "node_modules/typebox/build/type/engine/cyclic/index.mjs"() {
    init_candidates();
    init_check();
    init_dependencies();
    init_extends();
    init_instantiate4();
    init_target();
  }
});

// node_modules/typebox/build/type/extends/extends.mjs
function Canonical(type) {
  return IsCyclic(type) ? CyclicExtends(type) : IsUnsafe(type) ? Unknown() : type;
}
function Extends(inferred, left, right) {
  const canonicalLeft = Canonical(left);
  const canonicalRight = Canonical(right);
  return ExtendsLeft(inferred, canonicalLeft, canonicalRight);
}
var init_extends2 = __esm({
  "node_modules/typebox/build/type/extends/extends.mjs"() {
    init_cyclic();
    init_unknown();
    init_unsafe();
    init_extends_left();
    init_cyclic2();
  }
});

// node_modules/typebox/build/type/extends/index.mjs
var init_extends3 = __esm({
  "node_modules/typebox/build/type/extends/index.mjs"() {
    init_extends2();
    init_result();
  }
});

// node_modules/typebox/build/type/engine/evaluate/compare.mjs
function Compare(left, right) {
  const extendsCheck = [Extends({}, left, right), Extends({}, right, left)];
  return result_exports.IsExtendsTrueLike(extendsCheck[0]) && result_exports.IsExtendsTrueLike(extendsCheck[1]) ? CompareResultEqual : result_exports.IsExtendsTrueLike(extendsCheck[0]) && result_exports.IsExtendsFalse(extendsCheck[1]) ? CompareResultLeftInside : result_exports.IsExtendsFalse(extendsCheck[0]) && result_exports.IsExtendsTrueLike(extendsCheck[1]) ? CompareResultRightInside : CompareResultDisjoint;
}
var CompareResultEqual, CompareResultDisjoint, CompareResultLeftInside, CompareResultRightInside;
var init_compare = __esm({
  "node_modules/typebox/build/type/engine/evaluate/compare.mjs"() {
    init_extends3();
    CompareResultEqual = 0;
    CompareResultDisjoint = 1;
    CompareResultLeftInside = 2;
    CompareResultRightInside = 3;
  }
});

// node_modules/typebox/build/type/engine/evaluate/broaden.mjs
function BroadenFilter(type, types, result = [], all = types) {
  return guard_exports.ShiftLeft(types, (left, right) => {
    const compare = Compare(type, left);
    return guard_exports.IsEqual(compare, CompareResultLeftInside) || guard_exports.IsEqual(compare, CompareResultEqual) ? all : guard_exports.IsEqual(compare, CompareResultDisjoint) ? BroadenFilter(type, right, [...result, left], all) : BroadenFilter(type, right, result, all);
  }, () => [...result, type]);
}
function BroadenType(type, types, result) {
  const evaluated = EvaluateType(type);
  return IsAny(evaluated) ? [evaluated] : (
    // terminate (always the most broad)
    IsUnknown(evaluated) ? [evaluated] : (
      // terminate (always the most broad)
      IsNever(evaluated) ? BroadenTypes(types, result) : (
        // ignored: never is dropped
        IsObject2(evaluated) ? BroadenTypes(types, [...result, evaluated]) : (
          // objects are always considered (too expensive to compare)
          BroadenTypes(types, BroadenFilter(evaluated, result))
        )
      )
    )
  );
}
function BroadenTypes(types, result = []) {
  return guard_exports.ShiftLeft(types, (left, right) => BroadenType(left, right, result), () => result);
}
function Broaden(types) {
  const broadened = BroadenTypes(types);
  const flattened = Flatten(broadened);
  return flattened;
}
var init_broaden = __esm({
  "node_modules/typebox/build/type/engine/evaluate/broaden.mjs"() {
    init_guard2();
    init_any();
    init_never();
    init_object();
    init_unknown();
    init_compare();
    init_flatten();
    init_evaluate2();
  }
});

// node_modules/typebox/build/type/engine/evaluate/instantiate.mjs
function EvaluateAction(type, options2) {
  const result = memory_exports.Update(EvaluateType(type), {}, options2);
  return result;
}
function EvaluateInstantiate(context, state, type, options2) {
  const instantiatedType = InstantiateType(context, state, type);
  return EvaluateAction(instantiatedType, options2);
}
var init_instantiate5 = __esm({
  "node_modules/typebox/build/type/engine/evaluate/instantiate.mjs"() {
    init_memory2();
    init_instantiate27();
    init_evaluate2();
  }
});

// node_modules/typebox/build/type/engine/evaluate/index.mjs
var init_evaluate3 = __esm({
  "node_modules/typebox/build/type/engine/evaluate/index.mjs"() {
    init_broaden();
    init_compare();
    init_composite();
    init_distribute();
    init_evaluate2();
    init_flatten();
    init_instantiate5();
    init_narrow();
  }
});

// node_modules/typebox/build/type/engine/call/distribute_arguments.mjs
function CollectDistributionNames(expression, result = []) {
  return (
    // Conditional
    IsDeferred(expression) && guard_exports.IsEqual(expression.action, "Conditional") ? IsRef(expression.parameters[0]) ? CollectDistributionNames(expression.parameters[2], CollectDistributionNames(expression.parameters[3], [...result, expression.parameters[0]["$ref"]])) : CollectDistributionNames(expression.parameters[2], CollectDistributionNames(expression.parameters[3], result)) : IsDeferred(expression) && guard_exports.IsEqual(expression.action, "Mapped") ? IsDeferred(expression.parameters[1]) && guard_exports.IsEqual(expression.parameters[1].action, "KeyOf") && IsRef(expression.parameters[1].parameters[0]) ? [...result, expression.parameters[1].parameters[0]["$ref"]] : result : result
  );
}
function BuildDistributionArray(parameters, names) {
  return parameters.reduce((result, left) => [...result, names.includes(left.name)], []);
}
function ZipDistributionArray(arguments_, distributionArray, result = []) {
  return guard_exports.ShiftLeft(arguments_, (argumentLeft, argumentRight) => guard_exports.ShiftLeft(distributionArray, (booleanLeft, booleanRight) => ZipDistributionArray(argumentRight, booleanRight, [...result, [booleanLeft, argumentLeft]]), () => result), () => result);
}
function CanonicalArgument(type) {
  return IsTemplateLiteral(type) ? EvaluateTemplateLiteral(type.pattern) : IsEnum(type) ? EvaluateEnum(type.enum) : type;
}
function Expand(type) {
  const canonicalArgument = CanonicalArgument(type);
  return IsUnion(canonicalArgument) ? [...canonicalArgument.anyOf] : [canonicalArgument];
}
function Append(current, type) {
  return current.reduce((result, left) => [...result, [...left, type]], []);
}
function Cross(current, variants) {
  return variants.reduce((result, left) => {
    return [...result, ...Append(current, left)];
  }, []);
}
function Distribute2(zipped) {
  return zipped.reduce((result, left) => {
    return guard_exports.IsEqual(left[0], true) ? Cross(result, Expand(left[1])) : Cross(result, [left[1]]);
  }, [[]]);
}
function DistributeArguments(parameters, arguments_, expression) {
  const distributionNames = CollectDistributionNames(expression);
  const distributionArray = BuildDistributionArray(parameters, distributionNames);
  const zippedArguments = ZipDistributionArray(arguments_, distributionArray);
  return IsDeferred(expression) && guard_exports.IsEqual(expression.action, "Conditional") ? Distribute2(zippedArguments) : IsDeferred(expression) && guard_exports.IsEqual(expression.action, "Mapped") ? Distribute2(zippedArguments) : [arguments_];
}
var init_distribute_arguments = __esm({
  "node_modules/typebox/build/type/engine/call/distribute_arguments.mjs"() {
    init_guard2();
    init_union();
    init_deferred();
    init_enum();
    init_template_literal();
    init_ref();
    init_evaluate2();
    init_evaluate2();
  }
});

// node_modules/typebox/build/type/engine/call/resolve_target.mjs
function FromNotResolvable() {
  return ["(not-resolvable)", Never()];
}
function FromNotGeneric() {
  return ["(not-generic)", Never()];
}
function FromGeneric(name, parameters, expression) {
  return [name, Generic(parameters, expression)];
}
function FromRef4(context, ref, arguments_) {
  return ref in context ? FromType6(context, ref, context[ref], arguments_) : FromNotResolvable();
}
function FromType6(context, name, target2, arguments_) {
  return IsGeneric(target2) ? FromGeneric(name, target2.parameters, target2.expression) : IsRef(target2) ? FromRef4(context, target2.$ref, arguments_) : FromNotGeneric();
}
function ResolveTarget(context, target2, arguments_) {
  return FromType6(context, "(anonymous)", target2, arguments_);
}
var init_resolve_target = __esm({
  "node_modules/typebox/build/type/engine/call/resolve_target.mjs"() {
    init_generic();
    init_ref();
    init_never();
  }
});

// node_modules/typebox/build/type/engine/call/resolve_arguments.mjs
function AssertArgumentExtends(name, type, extends_) {
  if (IsInfer(type) || IsCall(type) || result_exports.IsExtendsTrueLike(Extends({}, type, extends_)))
    return;
  const cause = { parameter: name, expect: extends_, actual: type };
  throw new Error(`Argument for parameter ${name} does not satisfy constraint`, { cause });
}
function BindArgument(context, state, name, extends_, type) {
  const instantiatedArgument = InstantiateType(context, state, type);
  AssertArgumentExtends(name, instantiatedArgument, extends_);
  return memory_exports.Assign(context, { [name]: instantiatedArgument });
}
function BindArguments(context, state, parameterLeft, parameterRight, arguments_) {
  const instantiatedExtends = InstantiateType(context, state, parameterLeft.extends);
  const instantiatedEquals = InstantiateType(context, state, parameterLeft.equals);
  return guard_exports.ShiftLeft(arguments_, (left, right) => BindParameters(BindArgument(context, state, parameterLeft["name"], instantiatedExtends, left), state, parameterRight, right), () => BindParameters(BindArgument(context, state, parameterLeft["name"], instantiatedExtends, instantiatedEquals), state, parameterRight, []));
}
function BindParameters(context, state, parameters, arguments_) {
  return guard_exports.ShiftLeft(parameters, (left, right) => BindArguments(context, state, left, right, arguments_), () => context);
}
function ResolveArgumentsContext(context, state, parameters, arguments_) {
  return BindParameters(context, state, parameters, arguments_);
}
var init_resolve_arguments = __esm({
  "node_modules/typebox/build/type/engine/call/resolve_arguments.mjs"() {
    init_guard2();
    init_memory2();
    init_instantiate27();
    init_extends3();
    init_infer();
    init_call();
  }
});

// node_modules/typebox/build/type/engine/call/instantiate.mjs
function InstantiationAssert() {
  if (guard_exports.IsLessThan(instantiationCount, settings_exports.Get().maxInstantiationCount))
    return;
  throw Error("Type instantiation is excessively deep and possibly infinite");
}
function InstantiationIncrement() {
  InstantiationAssert();
  instantiationCount++;
  instantiationDepth++;
}
function InstantiationDecrement() {
  instantiationDepth--;
  if (guard_exports.IsEqual(instantiationDepth, 0))
    instantiationCount = 0;
}
function Peek(state) {
  const result = guard_exports.IsGreaterThan(state.callstack.length, 0) ? state.callstack[state.callstack.length - 1] : "";
  return result;
}
function IsTailCall(state, name) {
  const result = guard_exports.IsEqual(Peek(state), name);
  return result;
}
function CallDispatch(context, state, target2, parameters, expression, arguments_) {
  InstantiationIncrement();
  try {
    const argumentsContext = ResolveArgumentsContext(context, state, parameters, arguments_);
    const returnType = InstantiateType(argumentsContext, State([...state["callstack"], target2["$ref"]], state["visited"]), expression);
    return InstantiateType(argumentsContext, State([], []), returnType);
  } finally {
    InstantiationDecrement();
  }
}
function CallDistributed(context, state, target2, parameters, expression, distributedArguments) {
  return distributedArguments.reduce((result, arguments_) => {
    const returnType = CallDispatch(context, state, target2, parameters, expression, arguments_);
    return [...result, returnType];
  }, []);
}
function CallImmediate(context, state, target2, parameters, expression, arguments_) {
  const distributedArguments = DistributeArguments(parameters, arguments_, expression);
  const returnTypes = CallDistributed(context, state, target2, parameters, expression, distributedArguments);
  const result = guard_exports.IsEqual(returnTypes.length, 1) ? returnTypes[0] : EvaluateUnion(returnTypes);
  return result;
}
function CallInstantiate(context, state, target2, arguments_) {
  const instantiatedArguments = InstantiateTypes(context, state, arguments_);
  const resolved = ResolveTarget(context, target2, arguments_);
  const name = resolved[0];
  const type = resolved[1];
  const result = IsGeneric(type) ? IsTailCall(state, name) ? CallConstruct(Ref(name), instantiatedArguments) : CallImmediate(context, state, Ref(name), type.parameters, type.expression, instantiatedArguments) : CallConstruct(target2, instantiatedArguments);
  return result;
}
var instantiationDepth, instantiationCount;
var init_instantiate6 = __esm({
  "node_modules/typebox/build/type/engine/call/instantiate.mjs"() {
    init_settings2();
    init_guard2();
    init_call();
    init_ref();
    init_generic();
    init_evaluate3();
    init_instantiate27();
    init_instantiate27();
    init_instantiate27();
    init_distribute_arguments();
    init_resolve_target();
    init_resolve_arguments();
    instantiationDepth = 0;
    instantiationCount = 0;
  }
});

// node_modules/typebox/build/type/types/call.mjs
function CallConstruct(target2, arguments_) {
  return memory_exports.Create({ ["~kind"]: "Call" }, { type: "call", target: target2, arguments: arguments_ }, {});
}
function Call(target2, arguments_) {
  return CallInstantiate({}, State([], []), target2, arguments_);
}
function IsCall(value) {
  return IsKind(value, "Call");
}
var init_call = __esm({
  "node_modules/typebox/build/type/types/call.mjs"() {
    init_memory2();
    init_schema();
    init_instantiate6();
    init_instantiate27();
  }
});

// node_modules/typebox/build/type/engine/immutable/instantiate_remove.mjs
function RemoveImmutableOperation(type) {
  return memory_exports.Discard(type, ["~immutable"]);
}
function RemoveImmutableAction(type, options2) {
  const result = memory_exports.Update(RemoveImmutableOperation(type), {}, options2);
  return result;
}
function RemoveImmutableInstantiate(context, state, type, options2) {
  const instantiatedType = InstantiateType(context, state, type);
  return RemoveImmutableAction(instantiatedType, options2);
}
var init_instantiate_remove3 = __esm({
  "node_modules/typebox/build/type/engine/immutable/instantiate_remove.mjs"() {
    init_memory2();
    init_instantiate27();
  }
});

// node_modules/typebox/build/type/engine/intrinsics/mapping.mjs
function ApplyMapping(mapping, value) {
  return mapping(value);
}
var init_mapping2 = __esm({
  "node_modules/typebox/build/type/engine/intrinsics/mapping.mjs"() {
  }
});

// node_modules/typebox/build/type/engine/intrinsics/from_literal.mjs
function FromLiteral3(mapping, value) {
  return guard_exports.IsString(value) ? Literal(ApplyMapping(mapping, value)) : Literal(value);
}
var init_from_literal = __esm({
  "node_modules/typebox/build/type/engine/intrinsics/from_literal.mjs"() {
    init_guard2();
    init_literal();
    init_mapping2();
  }
});

// node_modules/typebox/build/type/engine/intrinsics/from_template_literal.mjs
function FromTemplateLiteral(mapping, pattern) {
  const evaluated = EvaluateTemplateLiteral(pattern);
  const result = FromType7(mapping, evaluated);
  return result;
}
var init_from_template_literal = __esm({
  "node_modules/typebox/build/type/engine/intrinsics/from_template_literal.mjs"() {
    init_from_type();
    init_evaluate3();
  }
});

// node_modules/typebox/build/type/engine/intrinsics/from_union.mjs
function FromUnion2(mapping, types) {
  const result = types.map((type) => FromType7(mapping, type));
  return Union(result);
}
var init_from_union = __esm({
  "node_modules/typebox/build/type/engine/intrinsics/from_union.mjs"() {
    init_union();
    init_from_type();
  }
});

// node_modules/typebox/build/type/engine/intrinsics/from_type.mjs
function FromType7(mapping, type) {
  return IsLiteral(type) ? FromLiteral3(mapping, type.const) : IsTemplateLiteral(type) ? FromTemplateLiteral(mapping, type.pattern) : IsUnion(type) ? FromUnion2(mapping, type.anyOf) : type;
}
var init_from_type = __esm({
  "node_modules/typebox/build/type/engine/intrinsics/from_type.mjs"() {
    init_literal();
    init_template_literal();
    init_union();
    init_from_literal();
    init_from_template_literal();
    init_from_union();
  }
});

// node_modules/typebox/build/type/action/capitalize.mjs
function CapitalizeDeferred(type, options2 = {}) {
  return Deferred("Capitalize", [type], options2);
}
function Capitalize(type, options2 = {}) {
  return CapitalizeAction(type, options2);
}
var init_capitalize = __esm({
  "node_modules/typebox/build/type/action/capitalize.mjs"() {
    init_deferred();
    init_instantiate7();
  }
});

// node_modules/typebox/build/type/action/lowercase.mjs
function LowercaseDeferred(type, options2 = {}) {
  return Deferred("Lowercase", [type], options2);
}
function Lowercase(type, options2 = {}) {
  return LowercaseAction(type, options2);
}
var init_lowercase = __esm({
  "node_modules/typebox/build/type/action/lowercase.mjs"() {
    init_deferred();
    init_instantiate7();
  }
});

// node_modules/typebox/build/type/action/uncapitalize.mjs
function UncapitalizeDeferred(type, options2 = {}) {
  return Deferred("Uncapitalize", [type], options2);
}
function Uncapitalize(type, options2 = {}) {
  return UncapitalizeAction(type, options2);
}
var init_uncapitalize = __esm({
  "node_modules/typebox/build/type/action/uncapitalize.mjs"() {
    init_deferred();
    init_instantiate7();
  }
});

// node_modules/typebox/build/type/action/uppercase.mjs
function UppercaseDeferred(type, options2 = {}) {
  return Deferred("Uppercase", [type], options2);
}
function Uppercase(type, options2 = {}) {
  return UppercaseAction(type, options2);
}
var init_uppercase = __esm({
  "node_modules/typebox/build/type/action/uppercase.mjs"() {
    init_deferred();
    init_instantiate7();
  }
});

// node_modules/typebox/build/type/engine/intrinsics/instantiate.mjs
function CapitalizeAction(type, options2) {
  const result = CanInstantiate([type]) ? memory_exports.Update(FromType7(CapitalizeMapping, type), {}, options2) : CapitalizeDeferred(type, options2);
  return result;
}
function LowercaseAction(type, options2) {
  const result = CanInstantiate([type]) ? memory_exports.Update(FromType7(LowercaseMapping, type), {}, options2) : LowercaseDeferred(type, options2);
  return result;
}
function UncapitalizeAction(type, options2) {
  const result = CanInstantiate([type]) ? memory_exports.Update(FromType7(UncapitalizeMapping, type), {}, options2) : UncapitalizeDeferred(type, options2);
  return result;
}
function UppercaseAction(type, options2) {
  const result = CanInstantiate([type]) ? memory_exports.Update(FromType7(UppercaseMapping, type), {}, options2) : UppercaseDeferred(type, options2);
  return result;
}
function CapitalizeInstantiate(context, state, type, options2) {
  const instantiatedType = InstantiateType(context, state, type);
  return CapitalizeAction(instantiatedType, options2);
}
function LowercaseInstantiate(context, state, type, options2) {
  const instantiatedType = InstantiateType(context, state, type);
  return LowercaseAction(instantiatedType, options2);
}
function UncapitalizeInstantiate(context, state, type, options2) {
  const instantiatedType = InstantiateType(context, state, type);
  return UncapitalizeAction(instantiatedType, options2);
}
function UppercaseInstantiate(context, state, type, options2) {
  const instantiatedType = InstantiateType(context, state, type);
  return UppercaseAction(instantiatedType, options2);
}
var CapitalizeMapping, LowercaseMapping, UncapitalizeMapping, UppercaseMapping;
var init_instantiate7 = __esm({
  "node_modules/typebox/build/type/engine/intrinsics/instantiate.mjs"() {
    init_memory2();
    init_from_type();
    init_instantiate27();
    init_capitalize();
    init_lowercase();
    init_uncapitalize();
    init_uppercase();
    CapitalizeMapping = (input) => input[0].toUpperCase() + input.slice(1);
    LowercaseMapping = (input) => input.toLowerCase();
    UncapitalizeMapping = (input) => input[0].toLowerCase() + input.slice(1);
    UppercaseMapping = (input) => input.toUpperCase();
  }
});

// node_modules/typebox/build/type/action/conditional.mjs
function ConditionalDeferred(left, right, true_, false_, options2 = {}) {
  return Deferred("Conditional", [left, right, true_, false_], options2);
}
function Conditional(left, right, true_, false_, options2 = {}) {
  return ConditionalAction({}, State([], []), left, right, true_, false_, options2);
}
var init_conditional = __esm({
  "node_modules/typebox/build/type/action/conditional.mjs"() {
    init_deferred();
    init_instantiate8();
    init_instantiate27();
  }
});

// node_modules/typebox/build/type/engine/conditional/instantiate.mjs
function ConditionalOperation(context, state, left, right, true_, false_) {
  const extendsResult = Extends(context, left, right);
  return result_exports.IsExtendsUnion(extendsResult) ? Union([InstantiateType(extendsResult.inferred, state, true_), InstantiateType(context, state, false_)]) : result_exports.IsExtendsTrue(extendsResult) ? InstantiateType(extendsResult.inferred, state, true_) : InstantiateType(context, state, false_);
}
function ConditionalAction(context, state, left, right, true_, false_, options2) {
  const result = CanInstantiate([left, right]) ? memory_exports.Update(ConditionalOperation(context, state, left, right, true_, false_), {}, options2) : ConditionalDeferred(left, right, true_, false_, options2);
  return result;
}
function ConditionalInstantiate(context, state, left, right, true_, false_, options2) {
  const instantiatedLeft = InstantiateType(context, state, left);
  const instantiatedRight = InstantiateType(context, state, right);
  return ConditionalAction(context, state, instantiatedLeft, instantiatedRight, true_, false_, options2);
}
var init_instantiate8 = __esm({
  "node_modules/typebox/build/type/engine/conditional/instantiate.mjs"() {
    init_memory2();
    init_union();
    init_extends3();
    init_instantiate27();
    init_conditional();
  }
});

// node_modules/typebox/build/type/engine/conditional/index.mjs
var init_conditional2 = __esm({
  "node_modules/typebox/build/type/engine/conditional/index.mjs"() {
    init_instantiate8();
  }
});

// node_modules/typebox/build/type/action/constructor_parameters.mjs
function ConstructorParametersDeferred(type, options2 = {}) {
  return Deferred("ConstructorParameters", [type], options2);
}
function ConstructorParameters(type, options2 = {}) {
  return ConstructorParametersAction(type, options2);
}
var init_constructor_parameters = __esm({
  "node_modules/typebox/build/type/action/constructor_parameters.mjs"() {
    init_deferred();
    init_instantiate9();
  }
});

// node_modules/typebox/build/type/engine/constructor_parameters/instantiate.mjs
function ConstructorParametersOperation(type) {
  const parameters = IsConstructor2(type) ? type["parameters"] : [];
  const instantiatedParameters = InstantiateElements({}, State([], []), parameters);
  const result = Tuple(instantiatedParameters);
  return result;
}
function ConstructorParametersAction(type, options2) {
  const result = CanInstantiate([type]) ? memory_exports.Update(ConstructorParametersOperation(type), {}, options2) : ConstructorParametersDeferred(type, options2);
  return result;
}
function ConstructorParametersInstantiate(context, state, type, options2) {
  const instantiatedType = InstantiateType(context, state, type);
  return ConstructorParametersAction(instantiatedType, options2);
}
var init_instantiate9 = __esm({
  "node_modules/typebox/build/type/engine/constructor_parameters/instantiate.mjs"() {
    init_memory2();
    init_constructor();
    init_tuple();
    init_constructor_parameters();
    init_instantiate27();
    init_instantiate27();
    init_instantiate27();
  }
});

// node_modules/typebox/build/type/action/exclude.mjs
function ExcludeDeferred(left, right, options2 = {}) {
  return Deferred("Exclude", [left, right], options2);
}
function Exclude(left, right, options2 = {}) {
  return ExcludeAction(left, right, options2);
}
var init_exclude = __esm({
  "node_modules/typebox/build/type/action/exclude.mjs"() {
    init_deferred();
    init_instantiate10();
  }
});

// node_modules/typebox/build/type/engine/exclude/instantiate.mjs
function ExcludeAction(left, right, options2) {
  const result = CanInstantiate([left, right]) ? memory_exports.Update(ExcludeOperation(left, right), {}, options2) : ExcludeDeferred(left, right, options2);
  return result;
}
function ExcludeInstantiate(context, state, left, right, options2) {
  const instantiatedLeft = InstantiateType(context, state, left);
  const instantiatedRight = InstantiateType(context, state, right);
  return ExcludeAction(instantiatedLeft, instantiatedRight, options2);
}
var init_instantiate10 = __esm({
  "node_modules/typebox/build/type/engine/exclude/instantiate.mjs"() {
    init_memory2();
    init_instantiate27();
    init_exclude();
    init_operation();
  }
});

// node_modules/typebox/build/type/action/extract.mjs
function ExtractDeferred(left, right, options2 = {}) {
  return Deferred("Extract", [left, right], options2);
}
function Extract(left, right, options2 = {}) {
  return ExtractAction(left, right, options2);
}
var init_extract = __esm({
  "node_modules/typebox/build/type/action/extract.mjs"() {
    init_deferred();
    init_instantiate11();
  }
});

// node_modules/typebox/build/type/engine/extract/operation.mjs
function ExtractType(left, right) {
  const check = Extends({}, left, right);
  const result = result_exports.IsExtendsTrueLike(check) ? [left] : [];
  return result;
}
function ExtractUnion(left, right, result = []) {
  return guard_exports.ShiftLeft(left, (head, tail) => ExtractUnion(tail, right, [...result, ...ExtractType(head, right)]), () => result);
}
function ExtractOperation(left, right) {
  const evaluated = EvaluateType(left);
  const canonical = IsUnion(evaluated) ? evaluated.anyOf : [evaluated];
  const remaining = ExtractUnion(canonical, right);
  const result = EvaluateUnion(remaining);
  return result;
}
var init_operation2 = __esm({
  "node_modules/typebox/build/type/engine/extract/operation.mjs"() {
    init_guard2();
    init_union();
    init_extends3();
    init_evaluate2();
    init_evaluate2();
  }
});

// node_modules/typebox/build/type/engine/extract/instantiate.mjs
function ExtractAction(left, right, options2) {
  const result = CanInstantiate([left, right]) ? memory_exports.Update(ExtractOperation(left, right), {}, options2) : ExtractDeferred(left, right, options2);
  return result;
}
function ExtractInstantiate(context, state, left, right, options2) {
  const instantiatedLeft = InstantiateType(context, state, left);
  const instantiatedRight = InstantiateType(context, state, right);
  return ExtractAction(instantiatedLeft, instantiatedRight, options2);
}
var init_instantiate11 = __esm({
  "node_modules/typebox/build/type/engine/extract/instantiate.mjs"() {
    init_memory2();
    init_instantiate27();
    init_extract();
    init_operation2();
  }
});

// node_modules/typebox/build/type/engine/helpers/keys_to_indexer.mjs
function KeysToLiterals(keys) {
  return keys.reduce((result, left) => {
    return IsLiteralValue(left) ? [...result, Literal(left)] : result;
  }, []);
}
function KeysToIndexer(keys) {
  const literals = KeysToLiterals(keys);
  const result = Union(literals);
  return result;
}
var init_keys_to_indexer = __esm({
  "node_modules/typebox/build/type/engine/helpers/keys_to_indexer.mjs"() {
    init_literal();
    init_union();
  }
});

// node_modules/typebox/build/type/action/indexed.mjs
function IndexDeferred(type, indexer, options2 = {}) {
  return Deferred("Index", [type, indexer], options2);
}
function Index(type, indexer_or_keys, options2 = {}) {
  const indexer = guard_exports.IsArray(indexer_or_keys) ? KeysToIndexer(indexer_or_keys) : indexer_or_keys;
  return IndexAction(type, indexer, options2);
}
var init_indexed = __esm({
  "node_modules/typebox/build/type/action/indexed.mjs"() {
    init_guard2();
    init_deferred();
    init_keys_to_indexer();
    init_instantiate12();
  }
});

// node_modules/typebox/build/type/engine/object/from_cyclic.mjs
function FromCyclic(defs, ref) {
  const target2 = CyclicTarget(defs, ref);
  const result = FromType8(target2);
  return result;
}
var init_from_cyclic = __esm({
  "node_modules/typebox/build/type/engine/object/from_cyclic.mjs"() {
    init_from_type2();
    init_target();
  }
});

// node_modules/typebox/build/type/engine/object/from_dependent.mjs
function FromDependent(if_, then_, else_) {
  const evaluated = EvaluateDependent(if_, then_, else_);
  const result = FromType8(evaluated);
  return result;
}
var init_from_dependent = __esm({
  "node_modules/typebox/build/type/engine/object/from_dependent.mjs"() {
    init_from_type2();
    init_evaluate2();
  }
});

// node_modules/typebox/build/type/engine/object/from_intersect.mjs
function CollapseIntersectProperties(left, right) {
  const leftKeys = guard_exports.Keys(left).filter((key) => !guard_exports.HasPropertyKey(right, key));
  const rightKeys = guard_exports.Keys(right).filter((key) => !guard_exports.HasPropertyKey(left, key));
  const sharedKeys = guard_exports.Keys(left).filter((key) => guard_exports.HasPropertyKey(right, key));
  const leftProperties = leftKeys.reduce((result, key) => ({ ...result, [key]: left[key] }), {});
  const rightProperties = rightKeys.reduce((result, key) => ({ ...result, [key]: right[key] }), {});
  const sharedProperties = sharedKeys.reduce((result, key) => ({ ...result, [key]: EvaluateIntersect([left[key], right[key]]) }), {});
  const unique = memory_exports.Assign(leftProperties, rightProperties);
  const shared = memory_exports.Assign(unique, sharedProperties);
  return shared;
}
function FromIntersect(types) {
  return types.reduce((result, left) => {
    return CollapseIntersectProperties(result, FromType8(left));
  }, {});
}
var init_from_intersect = __esm({
  "node_modules/typebox/build/type/engine/object/from_intersect.mjs"() {
    init_memory2();
    init_guard2();
    init_from_type2();
    init_evaluate2();
  }
});

// node_modules/typebox/build/type/engine/object/from_object.mjs
function FromObject3(properties) {
  return properties;
}
var init_from_object = __esm({
  "node_modules/typebox/build/type/engine/object/from_object.mjs"() {
  }
});

// node_modules/typebox/build/type/engine/object/from_tuple.mjs
function FromTuple(types) {
  const object = TupleToObject(Tuple(types));
  const result = FromType8(object);
  return result;
}
var init_from_tuple = __esm({
  "node_modules/typebox/build/type/engine/object/from_tuple.mjs"() {
    init_tuple();
    init_to_object();
    init_from_type2();
  }
});

// node_modules/typebox/build/type/engine/object/from_union.mjs
function CollapseUnionProperties(left, right) {
  const sharedKeys = guard_exports.Keys(left).filter((key) => key in right);
  const result = sharedKeys.reduce((result2, key) => {
    return { ...result2, [key]: EvaluateUnion([left[key], right[key]]) };
  }, {});
  return result;
}
function ReduceVariants(types, result) {
  return guard_exports.ShiftLeft(types, (left, right) => ReduceVariants(right, CollapseUnionProperties(result, FromType8(left))), () => result);
}
function FromUnion3(types) {
  return guard_exports.ShiftLeft(types, (left, right) => ReduceVariants(right, FromType8(left)), () => Unreachable());
}
var init_from_union2 = __esm({
  "node_modules/typebox/build/type/engine/object/from_union.mjs"() {
    init_guard2();
    init_unreachable2();
    init_evaluate2();
    init_from_type2();
  }
});

// node_modules/typebox/build/type/engine/object/from_type.mjs
function FromType8(type) {
  return IsCyclic(type) ? FromCyclic(type.$defs, type.$ref) : IsDependent(type) ? FromDependent(type.if, type.then, type.else) : IsIntersect(type) ? FromIntersect(type.allOf) : IsUnion(type) ? FromUnion3(type.anyOf) : IsTuple(type) ? FromTuple(type.items) : IsObject2(type) ? FromObject3(type.properties) : {};
}
var init_from_type2 = __esm({
  "node_modules/typebox/build/type/engine/object/from_type.mjs"() {
    init_cyclic();
    init_dependent();
    init_intersect();
    init_object();
    init_tuple();
    init_union();
    init_from_cyclic();
    init_from_dependent();
    init_from_intersect();
    init_from_object();
    init_from_tuple();
    init_from_union2();
  }
});

// node_modules/typebox/build/type/engine/object/collapse.mjs
function CollapseToObject(type) {
  const properties = FromType8(type);
  const result = _Object_(properties);
  return result;
}
var init_collapse = __esm({
  "node_modules/typebox/build/type/engine/object/collapse.mjs"() {
    init_object();
    init_from_type2();
  }
});

// node_modules/typebox/build/type/engine/object/index.mjs
var init_object3 = __esm({
  "node_modules/typebox/build/type/engine/object/index.mjs"() {
    init_collapse();
  }
});

// node_modules/typebox/build/type/engine/helpers/keys.mjs
function ConvertToIntegerKey(value) {
  const normal = `${value}`;
  return integerKeyPattern.test(normal) ? parseInt(normal) : value;
}
var integerKeyPattern;
var init_keys = __esm({
  "node_modules/typebox/build/type/engine/helpers/keys.mjs"() {
    integerKeyPattern = new RegExp("^(?:0|[1-9][0-9]*)$");
  }
});

// node_modules/typebox/build/type/engine/indexed/from_array.mjs
function NormalizeLiteral(value) {
  return Literal(ConvertToIntegerKey(value));
}
function NormalizeIndexerTypes(types) {
  return types.map((type) => NormalizeIndexer(type));
}
function NormalizeIndexer(type) {
  return IsIntersect(type) ? Intersect(NormalizeIndexerTypes(type.allOf)) : IsUnion(type) ? Union(NormalizeIndexerTypes(type.anyOf)) : IsLiteral(type) ? NormalizeLiteral(type.const) : type;
}
function FromArray2(type, indexer) {
  const normalizedIndexer = NormalizeIndexer(indexer);
  const check = Extends({}, normalizedIndexer, Number2());
  const result = (
    // indexer
    result_exports.IsExtendsTrueLike(check) ? type : IsLiteral(indexer) && guard_exports.IsEqual(indexer.const, "length") ? Number2() : Never()
  );
  return result;
}
var init_from_array = __esm({
  "node_modules/typebox/build/type/engine/indexed/from_array.mjs"() {
    init_guard2();
    init_intersect();
    init_union();
    init_literal();
    init_number();
    init_never();
    init_extends3();
    init_keys();
  }
});

// node_modules/typebox/build/type/engine/indexable/from_cyclic.mjs
function FromCyclic2(defs, ref) {
  const target2 = CyclicTarget(defs, ref);
  const result = FromType9(target2);
  return result;
}
var init_from_cyclic2 = __esm({
  "node_modules/typebox/build/type/engine/indexable/from_cyclic.mjs"() {
    init_from_type3();
    init_target();
  }
});

// node_modules/typebox/build/type/engine/indexable/from_dependent.mjs
function FromDependent2(if_, then_, else_) {
  const evaluated = EvaluateDependent(if_, then_, else_);
  const result = FromType9(evaluated);
  return result;
}
var init_from_dependent2 = __esm({
  "node_modules/typebox/build/type/engine/indexable/from_dependent.mjs"() {
    init_from_type3();
    init_evaluate2();
  }
});

// node_modules/typebox/build/type/engine/indexable/from_enum.mjs
function FromEnum(values) {
  const evaluated = EvaluateEnum(values);
  const result = FromType9(evaluated);
  return result;
}
var init_from_enum = __esm({
  "node_modules/typebox/build/type/engine/indexable/from_enum.mjs"() {
    init_from_type3();
    init_evaluate2();
  }
});

// node_modules/typebox/build/type/engine/indexable/from_intersect.mjs
function FromIntersect2(types) {
  const evaluated = EvaluateIntersect(types);
  const result = FromType9(evaluated);
  return result;
}
var init_from_intersect2 = __esm({
  "node_modules/typebox/build/type/engine/indexable/from_intersect.mjs"() {
    init_evaluate2();
    init_from_type3();
  }
});

// node_modules/typebox/build/type/engine/indexable/from_literal.mjs
function FromLiteral4(value) {
  const result = [`${value}`];
  return result;
}
var init_from_literal2 = __esm({
  "node_modules/typebox/build/type/engine/indexable/from_literal.mjs"() {
  }
});

// node_modules/typebox/build/type/engine/indexable/from_template_literal.mjs
function FromTemplateLiteral2(pattern) {
  const evaluated = EvaluateTemplateLiteral(pattern);
  const result = FromType9(evaluated);
  return result;
}
var init_from_template_literal2 = __esm({
  "node_modules/typebox/build/type/engine/indexable/from_template_literal.mjs"() {
    init_from_type3();
    init_evaluate2();
  }
});

// node_modules/typebox/build/type/engine/indexable/from_union.mjs
function FromUnion4(types) {
  return types.reduce((result, left) => {
    return [...result, ...FromType9(left)];
  }, []);
}
var init_from_union3 = __esm({
  "node_modules/typebox/build/type/engine/indexable/from_union.mjs"() {
    init_from_type3();
  }
});

// node_modules/typebox/build/type/engine/indexable/from_type.mjs
function FromType9(type) {
  return IsCyclic(type) ? FromCyclic2(type.$defs, type.$ref) : IsDependent(type) ? FromDependent2(type.if, type.then, type.else) : IsEnum(type) ? FromEnum(type.enum) : IsIntersect(type) ? FromIntersect2(type.allOf) : IsLiteral(type) ? FromLiteral4(type.const) : IsTemplateLiteral(type) ? FromTemplateLiteral2(type.pattern) : IsUnion(type) ? FromUnion4(type.anyOf) : [];
}
var init_from_type3 = __esm({
  "node_modules/typebox/build/type/engine/indexable/from_type.mjs"() {
    init_cyclic();
    init_dependent();
    init_enum();
    init_intersect();
    init_literal();
    init_template_literal();
    init_union();
    init_from_cyclic2();
    init_from_dependent2();
    init_from_enum();
    init_from_intersect2();
    init_from_literal2();
    init_from_template_literal2();
    init_from_union3();
  }
});

// node_modules/typebox/build/type/engine/indexable/to_indexable_keys.mjs
function ToIndexableKeys(type) {
  const result = FromType9(type);
  return result;
}
var init_to_indexable_keys = __esm({
  "node_modules/typebox/build/type/engine/indexable/to_indexable_keys.mjs"() {
    init_from_type3();
  }
});

// node_modules/typebox/build/type/engine/this/expand_this.mjs
function FromTypes5(properties, types) {
  return types.map((type) => FromType10(properties, type));
}
function FromType10(properties, type) {
  return IsArray2(type) ? _Array_(FromType10(properties, type.items)) : IsConstructor2(type) ? Constructor(FromTypes5(properties, type.parameters), FromType10(properties, type.instanceType)) : IsFunction2(type) ? _Function_(FromTypes5(properties, type.parameters), FromType10(properties, type.returnType)) : IsTuple(type) ? Tuple(FromTypes5(properties, type.items)) : IsUnion(type) ? Union(FromTypes5(properties, type.anyOf)) : IsIntersect(type) ? Intersect(FromTypes5(properties, type.allOf)) : IsThis(type) ? _Object_(properties) : type;
}
function ExpandThis(properties, type) {
  const result = FromType10(properties, type);
  return result;
}
var init_expand_this = __esm({
  "node_modules/typebox/build/type/engine/this/expand_this.mjs"() {
    init_array();
    init_constructor();
    init_function();
    init_intersect();
    init_object();
    init_tuple();
    init_this();
    init_union();
  }
});

// node_modules/typebox/build/type/engine/indexed/from_object.mjs
function IndexProperty(properties, key) {
  const selectedType = key in properties ? properties[key] : Never();
  const result = ExpandThis(properties, selectedType);
  return result;
}
function IndexProperties(properties, keys) {
  return keys.reduce((result, left) => {
    return [...result, IndexProperty(properties, left)];
  }, []);
}
function FromIndexer(properties, indexer) {
  const keys = ToIndexableKeys(indexer);
  const variants = IndexProperties(properties, keys);
  const result = EvaluateUnion(variants);
  return result;
}
function NumericKeys(keys) {
  const result = keys.filter((key) => NumericKeyPattern.test(key));
  return result;
}
function FromIndexerNumber(properties) {
  const keys = PropertyKeys(properties);
  const numericKeys = NumericKeys(keys);
  const variants = IndexProperties(properties, numericKeys);
  const result = EvaluateUnion(variants);
  return result;
}
function FromObject4(properties, indexer) {
  const result = IsNumber3(indexer) ? FromIndexerNumber(properties) : FromIndexer(properties, indexer);
  return result;
}
var NumericKeyPattern;
var init_from_object2 = __esm({
  "node_modules/typebox/build/type/engine/indexed/from_object.mjs"() {
    init_number();
    init_never();
    init_properties();
    init_evaluate2();
    init_to_indexable_keys();
    init_record();
    init_expand_this();
    NumericKeyPattern = new RegExp(IntegerKey);
  }
});

// node_modules/typebox/build/type/engine/indexed/array_indexer.mjs
function ConvertLiteral(value) {
  return Literal(ConvertToIntegerKey(value));
}
function ArrayIndexerTypes(types) {
  return types.map((type) => FormatArrayIndexer(type));
}
function FormatArrayIndexer(type) {
  return IsIntersect(type) ? Intersect(ArrayIndexerTypes(type.allOf)) : IsUnion(type) ? Union(ArrayIndexerTypes(type.anyOf)) : IsLiteral(type) ? ConvertLiteral(type.const) : type;
}
var init_array_indexer = __esm({
  "node_modules/typebox/build/type/engine/indexed/array_indexer.mjs"() {
    init_union();
    init_intersect();
    init_literal();
    init_keys();
  }
});

// node_modules/typebox/build/type/engine/indexed/from_tuple.mjs
function IndexElementsWithIndexer(types, indexer) {
  return types.reduceRight((result, right, index) => {
    const check = Extends({}, Literal(index), indexer);
    return result_exports.IsExtendsTrueLike(check) ? [right, ...result] : result;
  }, []);
}
function FromTupleWithIndexer(types, indexer) {
  const formattedArrayIndexer = FormatArrayIndexer(indexer);
  const elements = IndexElementsWithIndexer(types, formattedArrayIndexer);
  return EvaluateUnionFast(elements);
}
function FromTupleWithoutIndexer(types) {
  return EvaluateUnionFast(types);
}
function FromTuple2(types, indexer) {
  return (
    // length (intrinsic)
    IsLiteral(indexer) && guard_exports.IsEqual(indexer.const, "length") ? Literal(types.length) : IsNumber3(indexer) || IsInteger2(indexer) ? FromTupleWithoutIndexer(types) : FromTupleWithIndexer(types, indexer)
  );
}
var init_from_tuple2 = __esm({
  "node_modules/typebox/build/type/engine/indexed/from_tuple.mjs"() {
    init_guard2();
    init_literal();
    init_number();
    init_integer();
    init_evaluate2();
    init_extends3();
    init_array_indexer();
  }
});

// node_modules/typebox/build/type/engine/indexed/from_type.mjs
function FromType11(type, indexer) {
  return IsArray2(type) ? FromArray2(type.items, indexer) : IsObject2(type) ? FromObject4(type.properties, indexer) : IsTuple(type) ? FromTuple2(type.items, indexer) : Never();
}
var init_from_type4 = __esm({
  "node_modules/typebox/build/type/engine/indexed/from_type.mjs"() {
    init_array();
    init_never();
    init_object();
    init_tuple();
    init_from_array();
    init_from_object2();
    init_from_tuple2();
  }
});

// node_modules/typebox/build/type/engine/indexed/instantiate.mjs
function NormalizeType(type) {
  const result = IsCyclic(type) || IsDependent(type) || IsIntersect(type) || IsUnion(type) ? CollapseToObject(type) : type;
  return result;
}
function IndexAction(type, indexer, options2) {
  const result = CanInstantiate([type, indexer]) ? memory_exports.Update(FromType11(NormalizeType(type), indexer), {}, options2) : IndexDeferred(type, indexer, options2);
  return result;
}
function IndexInstantiate(context, state, type, indexer, options2) {
  const instantiatedType = InstantiateType(context, state, type);
  const instantiatedIndexer = InstantiateType(context, state, indexer);
  return IndexAction(instantiatedType, instantiatedIndexer, options2);
}
var init_instantiate12 = __esm({
  "node_modules/typebox/build/type/engine/indexed/instantiate.mjs"() {
    init_memory2();
    init_cyclic();
    init_dependent();
    init_intersect();
    init_union();
    init_instantiate27();
    init_indexed();
    init_object3();
    init_from_type4();
  }
});

// node_modules/typebox/build/type/action/instance_type.mjs
function InstanceTypeDeferred(type, options2 = {}) {
  return Deferred("InstanceType", [type], options2);
}
function InstanceType(type, options2 = {}) {
  return InstanceTypeAction(type, options2);
}
var init_instance_type = __esm({
  "node_modules/typebox/build/type/action/instance_type.mjs"() {
    init_deferred();
    init_instantiate13();
  }
});

// node_modules/typebox/build/type/engine/instance_type/instantiate.mjs
function InstanceTypeOperation(type) {
  return IsConstructor2(type) ? type["instanceType"] : Never();
}
function InstanceTypeAction(type, options2) {
  const result = CanInstantiate([type]) ? memory_exports.Update(InstanceTypeOperation(type), {}, options2) : InstanceTypeDeferred(type, options2);
  return result;
}
function InstanceTypeInstantiate(context, state, type, options2 = {}) {
  const instantiatedType = InstantiateType(context, state, type);
  return InstanceTypeAction(instantiatedType, options2);
}
var init_instantiate13 = __esm({
  "node_modules/typebox/build/type/engine/instance_type/instantiate.mjs"() {
    init_memory2();
    init_constructor();
    init_never();
    init_instance_type();
    init_instantiate27();
  }
});

// node_modules/typebox/build/type/action/keyof.mjs
function KeyOfDeferred(type, options2 = {}) {
  return Deferred("KeyOf", [type], options2);
}
function KeyOf2(type, options2 = {}) {
  return KeyOfAction(type, options2);
}
var init_keyof = __esm({
  "node_modules/typebox/build/type/action/keyof.mjs"() {
    init_deferred();
    init_instantiate14();
  }
});

// node_modules/typebox/build/type/engine/keyof/from_any.mjs
function FromAny() {
  return Union([Number2(), String2(), Symbol2()]);
}
var init_from_any = __esm({
  "node_modules/typebox/build/type/engine/keyof/from_any.mjs"() {
    init_number();
    init_string2();
    init_symbol();
    init_union();
  }
});

// node_modules/typebox/build/type/engine/keyof/from_array.mjs
function FromArray3(_type) {
  return Number2();
}
var init_from_array2 = __esm({
  "node_modules/typebox/build/type/engine/keyof/from_array.mjs"() {
    init_number();
  }
});

// node_modules/typebox/build/type/engine/keyof/from_object.mjs
function FromPropertyKeys(keys) {
  const result = keys.reduce((result2, left) => {
    return IsLiteralValue(left) ? [...result2, Literal(ConvertToIntegerKey(left))] : Unreachable();
  }, []);
  return result;
}
function FromObject5(properties) {
  const propertyKeys = guard_exports.Keys(properties);
  const variants = FromPropertyKeys(propertyKeys);
  const result = EvaluateUnionFast(variants);
  return result;
}
var init_from_object3 = __esm({
  "node_modules/typebox/build/type/engine/keyof/from_object.mjs"() {
    init_unreachable2();
    init_guard2();
    init_literal();
    init_keys();
    init_evaluate2();
  }
});

// node_modules/typebox/build/type/engine/keyof/from_record.mjs
function FromRecord2(type) {
  return RecordKey(type);
}
var init_from_record = __esm({
  "node_modules/typebox/build/type/engine/keyof/from_record.mjs"() {
    init_record();
  }
});

// node_modules/typebox/build/type/engine/keyof/from_tuple.mjs
function FromTuple3(types) {
  const result = types.map((_, index) => Literal(index));
  return EvaluateUnionFast(result);
}
var init_from_tuple3 = __esm({
  "node_modules/typebox/build/type/engine/keyof/from_tuple.mjs"() {
    init_literal();
    init_evaluate2();
  }
});

// node_modules/typebox/build/type/engine/keyof/from_type.mjs
function FromType12(type) {
  return IsAny(type) ? FromAny() : IsArray2(type) ? FromArray3(type.items) : IsObject2(type) ? FromObject5(type.properties) : IsRecord(type) ? FromRecord2(type) : IsTuple(type) ? FromTuple3(type.items) : Never();
}
var init_from_type5 = __esm({
  "node_modules/typebox/build/type/engine/keyof/from_type.mjs"() {
    init_any();
    init_array();
    init_never();
    init_object();
    init_record();
    init_tuple();
    init_from_any();
    init_from_array2();
    init_from_object3();
    init_from_record();
    init_from_tuple3();
  }
});

// node_modules/typebox/build/type/engine/keyof/instantiate.mjs
function NormalizeType2(type) {
  const result = IsCyclic(type) || IsDependent(type) || IsIntersect(type) || IsUnion(type) ? CollapseToObject(type) : type;
  return result;
}
function KeyOfAction(type, options2) {
  return CanInstantiate([type]) ? memory_exports.Update(FromType12(NormalizeType2(type)), {}, options2) : KeyOfDeferred(type, options2);
}
function KeyOfInstantiate(context, state, type, options2) {
  const instantiatedType = InstantiateType(context, state, type);
  return KeyOfAction(instantiatedType, options2);
}
var init_instantiate14 = __esm({
  "node_modules/typebox/build/type/engine/keyof/instantiate.mjs"() {
    init_memory2();
    init_cyclic();
    init_dependent();
    init_intersect();
    init_union();
    init_keyof();
    init_instantiate27();
    init_object3();
    init_from_type5();
  }
});

// node_modules/typebox/build/type/action/mapped.mjs
function MappedDeferred(identifier, type, as, property, options2 = {}) {
  return Deferred("Mapped", [identifier, type, as, property], options2);
}
function Mapped(identifier, type, as, property, options2 = {}) {
  return MappedAction({}, State([], []), identifier, type, as, property, options2);
}
var init_mapped = __esm({
  "node_modules/typebox/build/type/action/mapped.mjs"() {
    init_deferred();
    init_instantiate15();
    init_instantiate27();
  }
});

// node_modules/typebox/build/type/engine/mapped/mapped_variants.mjs
function FromTemplateLiteral3(pattern) {
  const evaluated = EvaluateTemplateLiteral(pattern);
  const result = FromType13(evaluated);
  return result;
}
function FromUnion5(types) {
  return types.reduce((result, left) => {
    return [...result, ...FromType13(left)];
  }, []);
}
function FromEnum2(values) {
  const evaluated = EvaluateEnum(values);
  const result = FromType13(evaluated);
  return result;
}
function FromLiteral5(value) {
  const result = guard_exports.IsNumber(value) ? [Literal(`${value}`)] : [Literal(value)];
  return result;
}
function FromType13(type) {
  const result = IsEnum(type) ? FromEnum2(type.enum) : IsLiteral(type) ? FromLiteral5(type.const) : IsTemplateLiteral(type) ? FromTemplateLiteral3(type.pattern) : IsUnion(type) ? FromUnion5(type.anyOf) : [type];
  return result;
}
function MappedVariants(type) {
  const result = FromType13(type);
  return result;
}
var init_mapped_variants = __esm({
  "node_modules/typebox/build/type/engine/mapped/mapped_variants.mjs"() {
    init_guard2();
    init_literal();
    init_enum();
    init_template_literal();
    init_union();
    init_evaluate2();
    init_evaluate2();
  }
});

// node_modules/typebox/build/type/engine/mapped/mapped_operation.mjs
function CanonicalAs(instantiatedAs) {
  const result = IsTemplateLiteral(instantiatedAs) ? EvaluateTemplateLiteral(instantiatedAs.pattern) : instantiatedAs;
  return result;
}
function MappedVariant(context, state, identifier, variant, as, property) {
  const variantContext = memory_exports.Assign(context, { [identifier["name"]]: variant });
  const instantiatedAs = InstantiateType(variantContext, state, as);
  const canonicalAs = CanonicalAs(instantiatedAs);
  const instantiatedProperty = InstantiateType(variantContext, state, property);
  return IsLiteralNumber(canonicalAs) || IsLiteralString(canonicalAs) ? { [canonicalAs.const]: instantiatedProperty } : {};
}
function MappedProperties(context, state, identifier, variants, as, property) {
  return variants.reduce((result, left) => {
    return [...result, MappedVariant(context, state, identifier, left, as, property)];
  }, []);
}
function MappedObjects(properties) {
  return properties.reduce((result, left) => {
    return [...result, _Object_(left)];
  }, []);
}
function MappedOperation(context, state, identifier, type, as, property) {
  const variants = MappedVariants(type);
  const mappedProperties = MappedProperties(context, state, identifier, variants, as, property);
  const mappedObjects = MappedObjects(mappedProperties);
  const result = EvaluateIntersect(mappedObjects);
  return result;
}
var init_mapped_operation = __esm({
  "node_modules/typebox/build/type/engine/mapped/mapped_operation.mjs"() {
    init_memory2();
    init_literal();
    init_object();
    init_template_literal();
    init_instantiate27();
    init_evaluate2();
    init_evaluate2();
    init_mapped_variants();
  }
});

// node_modules/typebox/build/type/engine/mapped/instantiate.mjs
function MappedAction(context, state, identifier, type, as, property, options2) {
  const result = CanInstantiate([type]) ? memory_exports.Update(MappedOperation(context, state, identifier, type, as, property), {}, options2) : MappedDeferred(identifier, type, as, property, options2);
  return result;
}
function MappedInstantiate(context, state, identifier, type, as, property, options2) {
  const instantiatedType = InstantiateType(context, state, type);
  return MappedAction(context, state, identifier, instantiatedType, as, property, options2);
}
var init_instantiate15 = __esm({
  "node_modules/typebox/build/type/engine/mapped/instantiate.mjs"() {
    init_memory2();
    init_mapped();
    init_instantiate27();
    init_mapped_operation();
  }
});

// node_modules/typebox/build/type/engine/module/instantiate.mjs
function InstantiateCyclics(context, declarations, cyclicKeys) {
  const declarationContext = memory_exports.Assign(context, declarations);
  const declarationKeys = guard_exports.Keys(declarations).filter((key) => cyclicKeys.includes(key));
  return declarationKeys.reduce((result, key) => {
    return { ...result, [key]: InstantiateCyclic(declarationContext, key, declarations[key]) };
  }, {});
}
function InstantiateNonCyclics(context, declarations, cyclicKeys) {
  const declarationContext = memory_exports.Assign(context, declarations);
  const declarationKeys = guard_exports.Keys(declarations).filter((key) => !cyclicKeys.includes(key));
  return declarationKeys.reduce((result, key) => {
    return { ...result, [key]: InstantiateType(declarationContext, State([], []), declarations[key]) };
  }, {});
}
function InstantiateModule(context, declarations, options2) {
  const cyclicCandidates = CyclicCandidates(declarations);
  const instantiatedCyclics = InstantiateCyclics(context, declarations, cyclicCandidates);
  const instantiatedNonCyclics = InstantiateNonCyclics(context, declarations, cyclicCandidates);
  const instantiatedModule = { ...instantiatedCyclics, ...instantiatedNonCyclics };
  return memory_exports.Update(instantiatedModule, {}, options2);
}
function ModuleInstantiate(context, _state, declarations, options2) {
  const instantiatedModule = InstantiateModule(context, declarations, options2);
  return instantiatedModule;
}
var init_instantiate16 = __esm({
  "node_modules/typebox/build/type/engine/module/instantiate.mjs"() {
    init_guard2();
    init_memory2();
    init_instantiate27();
    init_candidates();
    init_instantiate4();
    init_instantiate27();
  }
});

// node_modules/typebox/build/type/action/non_nullable.mjs
function NonNullableDeferred(type, options2 = {}) {
  return Deferred("NonNullable", [type], options2);
}
function NonNullable(type, options2 = {}) {
  return NonNullableAction(type, options2);
}
var init_non_nullable = __esm({
  "node_modules/typebox/build/type/action/non_nullable.mjs"() {
    init_deferred();
    init_instantiate17();
  }
});

// node_modules/typebox/build/type/engine/non_nullable/instantiate.mjs
function NonNullableOperation(type) {
  const excluded = Union([Null(), Undefined()]);
  return ExcludeAction(type, excluded, {});
}
function NonNullableAction(type, options2) {
  const result = CanInstantiate([type]) ? memory_exports.Update(NonNullableOperation(type), {}, options2) : NonNullableDeferred(type, options2);
  return result;
}
function NonNullableInstantiate(context, state, type, options2) {
  const instantiatedType = InstantiateType(context, state, type);
  return NonNullableAction(instantiatedType, options2);
}
var init_instantiate17 = __esm({
  "node_modules/typebox/build/type/engine/non_nullable/instantiate.mjs"() {
    init_memory2();
    init_null();
    init_undefined();
    init_union();
    init_instantiate10();
    init_non_nullable();
    init_instantiate27();
  }
});

// node_modules/typebox/build/type/action/omit.mjs
function OmitDeferred(type, indexer, options2 = {}) {
  return Deferred("Omit", [type, indexer], options2);
}
function Omit(type, indexer_or_keys, options2 = {}) {
  const indexer = guard_exports.IsArray(indexer_or_keys) ? KeysToIndexer(indexer_or_keys) : indexer_or_keys;
  return OmitAction(type, indexer, options2);
}
var init_omit = __esm({
  "node_modules/typebox/build/type/action/omit.mjs"() {
    init_guard2();
    init_deferred();
    init_keys_to_indexer();
    init_instantiate18();
  }
});

// node_modules/typebox/build/type/engine/indexable/to_indexable.mjs
function ToIndexable(type) {
  const collapsed = CollapseToObject(type);
  const result = IsObject2(collapsed) ? collapsed.properties : Unreachable();
  return result;
}
var init_to_indexable = __esm({
  "node_modules/typebox/build/type/engine/indexable/to_indexable.mjs"() {
    init_unreachable2();
    init_object();
    init_object3();
  }
});

// node_modules/typebox/build/type/engine/omit/from_type.mjs
function FromKeys(properties, keys) {
  const result = guard_exports.Keys(properties).reduce((result2, key) => {
    return keys.includes(key) ? result2 : { ...result2, [key]: properties[key] };
  }, {});
  return result;
}
function FromType14(type, indexer) {
  const indexable = ToIndexable(type);
  const indexableKeys = ToIndexableKeys(indexer);
  const omitted = FromKeys(indexable, indexableKeys);
  const result = _Object_(omitted);
  return result;
}
var init_from_type6 = __esm({
  "node_modules/typebox/build/type/engine/omit/from_type.mjs"() {
    init_guard2();
    init_object();
    init_to_indexable_keys();
    init_to_indexable();
  }
});

// node_modules/typebox/build/type/engine/omit/instantiate.mjs
function OmitAction(type, indexer, options2) {
  const result = CanInstantiate([type, indexer]) ? memory_exports.Update(FromType14(type, indexer), {}, options2) : OmitDeferred(type, indexer, options2);
  return result;
}
function OmitInstantiate(context, state, type, indexer, options2) {
  const instantiatedType = InstantiateType(context, state, type);
  const instantiatedIndexer = InstantiateType(context, state, indexer);
  return OmitAction(instantiatedType, instantiatedIndexer, options2);
}
var init_instantiate18 = __esm({
  "node_modules/typebox/build/type/engine/omit/instantiate.mjs"() {
    init_memory2();
    init_omit();
    init_instantiate27();
    init_from_type6();
  }
});

// node_modules/typebox/build/type/action/parameters.mjs
function ParametersDeferred(type, options2 = {}) {
  return Deferred("Parameters", [type], options2);
}
function Parameters(type, options2 = {}) {
  return ParametersAction(type, options2);
}
var init_parameters2 = __esm({
  "node_modules/typebox/build/type/action/parameters.mjs"() {
    init_deferred();
    init_instantiate19();
  }
});

// node_modules/typebox/build/type/engine/parameters/instantiate.mjs
function ParametersOperation(type) {
  const parameters = IsFunction2(type) ? type["parameters"] : [];
  const instantiatedParameters = InstantiateElements({}, State([], []), parameters);
  const result = Tuple(instantiatedParameters);
  return result;
}
function ParametersAction(type, options2) {
  const result = CanInstantiate([type]) ? memory_exports.Update(ParametersOperation(type), {}, options2) : ParametersDeferred(type, options2);
  return result;
}
function ParametersInstantiate(context, state, type, options2) {
  const instantiatedType = InstantiateType(context, state, type);
  return ParametersAction(instantiatedType, options2);
}
var init_instantiate19 = __esm({
  "node_modules/typebox/build/type/engine/parameters/instantiate.mjs"() {
    init_memory2();
    init_function();
    init_tuple();
    init_parameters2();
    init_instantiate27();
    init_instantiate27();
    init_instantiate27();
  }
});

// node_modules/typebox/build/type/action/partial.mjs
function PartialDeferred(type, options2 = {}) {
  return Deferred("Partial", [type], options2);
}
function Partial(type, options2 = {}) {
  return PartialAction(type, options2);
}
var init_partial = __esm({
  "node_modules/typebox/build/type/action/partial.mjs"() {
    init_deferred();
    init_instantiate20();
  }
});

// node_modules/typebox/build/type/engine/partial/from_cyclic.mjs
function FromCyclic3(defs, ref) {
  const target2 = CyclicTarget(defs, ref);
  const partial = FromType15(target2);
  const result = Cyclic(memory_exports.Assign(defs, { [ref]: partial }), ref);
  return result;
}
var init_from_cyclic3 = __esm({
  "node_modules/typebox/build/type/engine/partial/from_cyclic.mjs"() {
    init_memory2();
    init_cyclic();
    init_from_type7();
    init_target();
  }
});

// node_modules/typebox/build/type/engine/partial/from_dependent.mjs
function FromDependent3(if_, then_, else_) {
  const evaluated = EvaluateDependent(if_, then_, else_);
  const result = FromType15(evaluated);
  return result;
}
var init_from_dependent3 = __esm({
  "node_modules/typebox/build/type/engine/partial/from_dependent.mjs"() {
    init_from_type7();
    init_evaluate2();
  }
});

// node_modules/typebox/build/type/engine/partial/from_intersect.mjs
function FromIntersect3(types) {
  const evaluated = EvaluateIntersect(types);
  const result = FromType15(evaluated);
  return result;
}
var init_from_intersect3 = __esm({
  "node_modules/typebox/build/type/engine/partial/from_intersect.mjs"() {
    init_from_type7();
    init_evaluate2();
  }
});

// node_modules/typebox/build/type/engine/partial/from_union.mjs
function FromUnion6(types) {
  const result = types.map((type) => FromType15(type));
  return Union(result);
}
var init_from_union4 = __esm({
  "node_modules/typebox/build/type/engine/partial/from_union.mjs"() {
    init_union();
    init_from_type7();
  }
});

// node_modules/typebox/build/type/engine/partial/from_object.mjs
function FromObject6(properties) {
  const mapped = guard_exports.Keys(properties).reduce((result2, left) => {
    return { ...result2, [left]: AddOptional(properties[left]) };
  }, {});
  const result = _Object_(mapped);
  return result;
}
var init_from_object4 = __esm({
  "node_modules/typebox/build/type/engine/partial/from_object.mjs"() {
    init_guard2();
    init_object();
    init_add_optional();
  }
});

// node_modules/typebox/build/type/engine/partial/from_type.mjs
function FromType15(type) {
  return IsCyclic(type) ? FromCyclic3(type.$defs, type.$ref) : IsDependent(type) ? FromDependent3(type.if, type.then, type.else) : IsIntersect(type) ? FromIntersect3(type.allOf) : IsUnion(type) ? FromUnion6(type.anyOf) : IsObject2(type) ? FromObject6(type.properties) : _Object_({});
}
var init_from_type7 = __esm({
  "node_modules/typebox/build/type/engine/partial/from_type.mjs"() {
    init_cyclic();
    init_dependent();
    init_intersect();
    init_object();
    init_union();
    init_from_cyclic3();
    init_from_dependent3();
    init_from_intersect3();
    init_from_union4();
    init_from_object4();
  }
});

// node_modules/typebox/build/type/engine/partial/instantiate.mjs
function PartialAction(type, options2) {
  const result = CanInstantiate([type]) ? memory_exports.Update(FromType15(type), {}, options2) : PartialDeferred(type, options2);
  return result;
}
function PartialInstantiate(context, state, type, options2) {
  const instantiatedType = InstantiateType(context, state, type);
  return PartialAction(instantiatedType, options2);
}
var init_instantiate20 = __esm({
  "node_modules/typebox/build/type/engine/partial/instantiate.mjs"() {
    init_memory2();
    init_partial();
    init_from_type7();
    init_instantiate27();
  }
});

// node_modules/typebox/build/type/action/pick.mjs
function PickDeferred(type, indexer, options2 = {}) {
  return Deferred("Pick", [type, indexer], options2);
}
function Pick(type, indexer_or_keys, options2 = {}) {
  const indexer = guard_exports.IsArray(indexer_or_keys) ? KeysToIndexer(indexer_or_keys) : indexer_or_keys;
  return PickAction(type, indexer, options2);
}
var init_pick = __esm({
  "node_modules/typebox/build/type/action/pick.mjs"() {
    init_guard2();
    init_deferred();
    init_keys_to_indexer();
    init_instantiate21();
  }
});

// node_modules/typebox/build/type/engine/pick/from_type.mjs
function FromKeys2(properties, keys) {
  const result = guard_exports.Keys(properties).reduce((result2, key) => {
    return keys.includes(key) ? memory_exports.Assign(result2, { [key]: properties[key] }) : result2;
  }, {});
  return result;
}
function FromType16(type, indexer) {
  const indexable = ToIndexable(type);
  const keys = ToIndexableKeys(indexer);
  const applied = FromKeys2(indexable, keys);
  const result = _Object_(applied);
  return result;
}
var init_from_type8 = __esm({
  "node_modules/typebox/build/type/engine/pick/from_type.mjs"() {
    init_memory2();
    init_guard2();
    init_object();
    init_to_indexable_keys();
    init_to_indexable();
  }
});

// node_modules/typebox/build/type/engine/pick/instantiate.mjs
function PickAction(type, indexer, options2) {
  const result = CanInstantiate([type, indexer]) ? memory_exports.Update(FromType16(type, indexer), {}, options2) : PickDeferred(type, indexer, options2);
  return result;
}
function PickInstantiate(context, state, type, indexer, options2) {
  const instantiatedType = InstantiateType(context, state, type);
  const instantiatedIndexer = InstantiateType(context, state, indexer);
  return PickAction(instantiatedType, instantiatedIndexer, options2);
}
var init_instantiate21 = __esm({
  "node_modules/typebox/build/type/engine/pick/instantiate.mjs"() {
    init_memory2();
    init_pick();
    init_instantiate27();
    init_from_type8();
  }
});

// node_modules/typebox/build/type/action/readonly_object.mjs
function ReadonlyObjectDeferred(type, options2 = {}) {
  return Deferred("ReadonlyObject", [type], options2);
}
function ReadonlyObject(type, options2 = {}) {
  return ReadonlyObjectAction(type, options2);
}
var ReadonlyType;
var init_readonly_object = __esm({
  "node_modules/typebox/build/type/action/readonly_object.mjs"() {
    init_deferred();
    init_instantiate22();
    ReadonlyType = ReadonlyObject;
  }
});

// node_modules/typebox/build/type/engine/readonly_object/from_array.mjs
function FromArray4(type) {
  const result = AddImmutable(_Array_(type));
  return result;
}
var init_from_array3 = __esm({
  "node_modules/typebox/build/type/engine/readonly_object/from_array.mjs"() {
    init_array();
    init_add_immutable();
  }
});

// node_modules/typebox/build/type/engine/readonly_object/from_cyclic.mjs
function FromCyclic4(defs, ref) {
  const target2 = CyclicTarget(defs, ref);
  const partial = FromType17(target2);
  const result = Cyclic(memory_exports.Assign(defs, { [ref]: partial }), ref);
  return result;
}
var init_from_cyclic4 = __esm({
  "node_modules/typebox/build/type/engine/readonly_object/from_cyclic.mjs"() {
    init_memory2();
    init_cyclic();
    init_from_type9();
    init_target();
  }
});

// node_modules/typebox/build/type/engine/readonly_object/from_dependent.mjs
function FromDependent4(if_, then_, else_) {
  const evaluated = EvaluateDependent(if_, then_, else_);
  const result = FromType17(evaluated);
  return result;
}
var init_from_dependent4 = __esm({
  "node_modules/typebox/build/type/engine/readonly_object/from_dependent.mjs"() {
    init_from_type9();
    init_evaluate2();
  }
});

// node_modules/typebox/build/type/engine/readonly_object/from_intersect.mjs
function FromIntersect4(types) {
  const evaluated = EvaluateIntersect(types);
  const result = FromType17(evaluated);
  return result;
}
var init_from_intersect4 = __esm({
  "node_modules/typebox/build/type/engine/readonly_object/from_intersect.mjs"() {
    init_from_type9();
    init_evaluate2();
  }
});

// node_modules/typebox/build/type/engine/readonly_object/from_object.mjs
function FromObject7(properties) {
  const mapped = guard_exports.Keys(properties).reduce((result2, left) => {
    return { ...result2, [left]: AddReadonly(properties[left]) };
  }, {});
  const result = _Object_(mapped);
  return result;
}
var init_from_object5 = __esm({
  "node_modules/typebox/build/type/engine/readonly_object/from_object.mjs"() {
    init_guard2();
    init_object();
    init_add_readonly();
  }
});

// node_modules/typebox/build/type/engine/readonly_object/from_tuple.mjs
function FromTuple4(types) {
  const result = AddImmutable(Tuple(types));
  return result;
}
var init_from_tuple4 = __esm({
  "node_modules/typebox/build/type/engine/readonly_object/from_tuple.mjs"() {
    init_tuple();
    init_add_immutable();
  }
});

// node_modules/typebox/build/type/engine/readonly_object/from_union.mjs
function FromUnion7(types) {
  const result = types.map((type) => FromType17(type));
  return Union(result);
}
var init_from_union5 = __esm({
  "node_modules/typebox/build/type/engine/readonly_object/from_union.mjs"() {
    init_union();
    init_from_type9();
  }
});

// node_modules/typebox/build/type/engine/readonly_object/from_type.mjs
function FromType17(type) {
  return IsArray2(type) ? FromArray4(type.items) : IsCyclic(type) ? FromCyclic4(type.$defs, type.$ref) : IsDependent(type) ? FromDependent4(type.if, type.then, type.else) : IsIntersect(type) ? FromIntersect4(type.allOf) : IsObject2(type) ? FromObject7(type.properties) : IsTuple(type) ? FromTuple4(type.items) : IsUnion(type) ? FromUnion7(type.anyOf) : type;
}
var init_from_type9 = __esm({
  "node_modules/typebox/build/type/engine/readonly_object/from_type.mjs"() {
    init_array();
    init_cyclic();
    init_dependent();
    init_intersect();
    init_object();
    init_tuple();
    init_union();
    init_from_array3();
    init_from_cyclic4();
    init_from_dependent4();
    init_from_intersect4();
    init_from_object5();
    init_from_tuple4();
    init_from_union5();
  }
});

// node_modules/typebox/build/type/engine/readonly_object/instantiate.mjs
function ReadonlyObjectAction(type, options2) {
  const result = CanInstantiate([type]) ? memory_exports.Update(FromType17(type), {}, options2) : ReadonlyObjectDeferred(type);
  return result;
}
function ReadonlyObjectInstantiate(context, state, type, options2) {
  const instantiatedType = InstantiateType(context, state, type);
  return ReadonlyObjectAction(instantiatedType, options2);
}
var init_instantiate22 = __esm({
  "node_modules/typebox/build/type/engine/readonly_object/instantiate.mjs"() {
    init_memory2();
    init_readonly_object();
    init_from_type9();
    init_instantiate27();
  }
});

// node_modules/typebox/build/type/engine/ref/instantiate.mjs
function RefInstantiate(context, state, type, ref) {
  return state.visited.includes(ref) ? type : ref in context ? InstantiateType(context, State(state["callstack"], [...state["visited"], ref]), context[ref]) : type;
}
var init_instantiate23 = __esm({
  "node_modules/typebox/build/type/engine/ref/instantiate.mjs"() {
    init_instantiate27();
    init_instantiate27();
  }
});

// node_modules/typebox/build/type/engine/required/from_cyclic.mjs
function FromCyclic5(defs, ref) {
  const target2 = CyclicTarget(defs, ref);
  const partial = FromType18(target2);
  const result = Cyclic(memory_exports.Assign(defs, { [ref]: partial }), ref);
  return result;
}
var init_from_cyclic5 = __esm({
  "node_modules/typebox/build/type/engine/required/from_cyclic.mjs"() {
    init_memory2();
    init_cyclic();
    init_from_type10();
    init_target();
  }
});

// node_modules/typebox/build/type/engine/required/from_dependent.mjs
function FromDependent5(if_, then_, else_) {
  const evaluated = EvaluateDependent(if_, then_, else_);
  const result = FromType18(evaluated);
  return result;
}
var init_from_dependent5 = __esm({
  "node_modules/typebox/build/type/engine/required/from_dependent.mjs"() {
    init_from_type10();
    init_evaluate2();
  }
});

// node_modules/typebox/build/type/engine/required/from_intersect.mjs
function FromIntersect5(types) {
  const evaluated = EvaluateIntersect(types);
  const result = FromType18(evaluated);
  return result;
}
var init_from_intersect5 = __esm({
  "node_modules/typebox/build/type/engine/required/from_intersect.mjs"() {
    init_from_type10();
    init_evaluate2();
  }
});

// node_modules/typebox/build/type/engine/required/from_union.mjs
function FromUnion8(types) {
  const result = types.map((type) => FromType18(type));
  return Union(result);
}
var init_from_union6 = __esm({
  "node_modules/typebox/build/type/engine/required/from_union.mjs"() {
    init_union();
    init_from_type10();
  }
});

// node_modules/typebox/build/type/engine/required/from_object.mjs
function FromObject8(properties) {
  const mapped = guard_exports.Keys(properties).reduce((result2, left) => {
    return { ...result2, [left]: RemoveOptional(properties[left]) };
  }, {});
  const result = _Object_(mapped);
  return result;
}
var init_from_object6 = __esm({
  "node_modules/typebox/build/type/engine/required/from_object.mjs"() {
    init_guard2();
    init_object();
    init_remove_optional();
  }
});

// node_modules/typebox/build/type/engine/required/from_type.mjs
function FromType18(type) {
  return IsCyclic(type) ? FromCyclic5(type.$defs, type.$ref) : IsDependent(type) ? FromDependent5(type.if, type.then, type.else) : IsIntersect(type) ? FromIntersect5(type.allOf) : IsUnion(type) ? FromUnion8(type.anyOf) : IsObject2(type) ? FromObject8(type.properties) : _Object_({});
}
var init_from_type10 = __esm({
  "node_modules/typebox/build/type/engine/required/from_type.mjs"() {
    init_cyclic();
    init_dependent();
    init_intersect();
    init_object();
    init_union();
    init_from_cyclic5();
    init_from_dependent5();
    init_from_intersect5();
    init_from_union6();
    init_from_object6();
  }
});

// node_modules/typebox/build/type/action/required.mjs
function RequiredDeferred(type, options2 = {}) {
  return Deferred("Required", [type], options2);
}
function Required(type, options2 = {}) {
  return RequiredAction(type, options2);
}
var init_required = __esm({
  "node_modules/typebox/build/type/action/required.mjs"() {
    init_deferred();
    init_instantiate24();
  }
});

// node_modules/typebox/build/type/engine/required/instantiate.mjs
function RequiredAction(type, options2) {
  const result = CanInstantiate([type]) ? memory_exports.Update(FromType18(type), {}, options2) : RequiredDeferred(type, options2);
  return result;
}
function RequiredInstantiate(context, state, type, options2) {
  const instaniatedType = InstantiateType(context, state, type);
  return RequiredAction(instaniatedType, options2);
}
var init_instantiate24 = __esm({
  "node_modules/typebox/build/type/engine/required/instantiate.mjs"() {
    init_memory2();
    init_from_type10();
    init_required();
    init_instantiate27();
  }
});

// node_modules/typebox/build/type/action/return_type.mjs
function ReturnTypeDeferred(type, options2 = {}) {
  return Deferred("ReturnType", [type], options2);
}
function ReturnType(type, options2 = {}) {
  return ReturnTypeAction(type, options2);
}
var init_return_type2 = __esm({
  "node_modules/typebox/build/type/action/return_type.mjs"() {
    init_deferred();
    init_instantiate25();
  }
});

// node_modules/typebox/build/type/engine/return_type/instantiate.mjs
function ReturnTypeOperation(type) {
  return IsFunction2(type) ? type["returnType"] : Never();
}
function ReturnTypeAction(type, options2) {
  const result = CanInstantiate([type]) ? memory_exports.Update(ReturnTypeOperation(type), {}, options2) : ReturnTypeDeferred(type, options2);
  return result;
}
function ReturnTypeInstantiate(context, state, type, options2 = {}) {
  const instantiatedType = InstantiateType(context, state, type);
  return ReturnTypeAction(instantiatedType, options2);
}
var init_instantiate25 = __esm({
  "node_modules/typebox/build/type/engine/return_type/instantiate.mjs"() {
    init_memory2();
    init_function();
    init_never();
    init_return_type2();
    init_instantiate27();
  }
});

// node_modules/typebox/build/type/action/with.mjs
function WithDeferred(type, options2) {
  return Deferred("With", [type, options2], {});
}
function With2(type, options2) {
  return WithAction(type, options2);
}
var init_with = __esm({
  "node_modules/typebox/build/type/action/with.mjs"() {
    init_deferred();
    init_instantiate26();
  }
});

// node_modules/typebox/build/type/engine/with/instantiate.mjs
function WithAction(type, options2) {
  const result = CanInstantiate([type]) ? memory_exports.Update(type, {}, options2) : WithDeferred(type, options2);
  return result;
}
function WithInstantiate(context, state, type, options2) {
  const instaniatedType = InstantiateType(context, state, type);
  return WithAction(instaniatedType, options2);
}
var init_instantiate26 = __esm({
  "node_modules/typebox/build/type/engine/with/instantiate.mjs"() {
    init_memory2();
    init_instantiate27();
    init_with();
  }
});

// node_modules/typebox/build/type/engine/rest/spread.mjs
function SpreadElement(type) {
  const result = IsRest(type) ? IsTuple(type.items) ? RestSpread(type.items.items) : IsInfer(type.items) ? [type] : IsRef(type.items) ? [type] : [Never()] : [type];
  return result;
}
function RestSpread(types) {
  const result = types.reduce((result2, left) => {
    return [...result2, ...SpreadElement(left)];
  }, []);
  return result;
}
var init_spread = __esm({
  "node_modules/typebox/build/type/engine/rest/spread.mjs"() {
    init_infer();
    init_never();
    init_rest();
    init_ref();
    init_tuple();
  }
});

// node_modules/typebox/build/type/engine/rest/index.mjs
var init_rest3 = __esm({
  "node_modules/typebox/build/type/engine/rest/index.mjs"() {
    init_spread();
  }
});

// node_modules/typebox/build/type/engine/instantiate.mjs
function State(callstack, visited) {
  return { callstack, visited };
}
function CanInstantiate(types) {
  return guard_exports.ShiftLeft(types, (left, right) => IsRef(left) ? false : CanInstantiate(right), () => true);
}
function InstantiateProperties(context, state, properties) {
  return guard_exports.Keys(properties).reduce((result, key) => {
    return { ...result, [key]: InstantiateType(context, state, properties[key]) };
  }, {});
}
function InstantiateElements(context, state, types) {
  const elements = InstantiateTypes(context, state, types);
  const result = RestSpread(elements);
  return result;
}
function InstantiateTypes(context, state, types) {
  return types.map((type) => InstantiateType(context, state, type));
}
function WithModifiers(type, instantiatedType) {
  const withOptional = IsOptional(type) ? AddOptionalAction(instantiatedType, {}) : instantiatedType;
  const withReadonly = IsReadonly(type) ? AddReadonlyAction(withOptional, {}) : withOptional;
  const withImmutable = IsImmutable(type) ? AddImmutableAction(withReadonly, {}) : withReadonly;
  return withImmutable;
}
function InstantiateDeferred(context, state, action, parameters, options2) {
  return (
    // Modifiers
    guard_exports.IsEqual(action, "AddImmutable") ? AddImmutableInstantiate(context, state, parameters[0], options2) : guard_exports.IsEqual(action, "RemoveImmutable") ? RemoveImmutableInstantiate(context, state, parameters[0], options2) : guard_exports.IsEqual(action, "AddReadonly") ? AddReadonlyInstantiate(context, state, parameters[0], options2) : guard_exports.IsEqual(action, "RemoveReadonly") ? RemoveReadonlyInstantiate(context, state, parameters[0], options2) : guard_exports.IsEqual(action, "AddOptional") ? AddOptionalInstantiate(context, state, parameters[0], options2) : guard_exports.IsEqual(action, "RemoveOptional") ? RemoveOptionalInstantiate(context, state, parameters[0], options2) : (
      // Actions
      guard_exports.IsEqual(action, "Capitalize") ? CapitalizeInstantiate(context, state, parameters[0], options2) : guard_exports.IsEqual(action, "Conditional") ? ConditionalInstantiate(context, state, parameters[0], parameters[1], parameters[2], parameters[3], options2) : guard_exports.IsEqual(action, "ConstructorParameters") ? ConstructorParametersInstantiate(context, state, parameters[0], options2) : guard_exports.IsEqual(action, "Evaluate") ? EvaluateInstantiate(context, state, parameters[0], options2) : guard_exports.IsEqual(action, "Exclude") ? ExcludeInstantiate(context, state, parameters[0], parameters[1], options2) : guard_exports.IsEqual(action, "Extract") ? ExtractInstantiate(context, state, parameters[0], parameters[1], options2) : guard_exports.IsEqual(action, "Index") ? IndexInstantiate(context, state, parameters[0], parameters[1], options2) : guard_exports.IsEqual(action, "InstanceType") ? InstanceTypeInstantiate(context, state, parameters[0], options2) : guard_exports.IsEqual(action, "Interface") ? InterfaceInstantiate(context, state, parameters[0], parameters[1], options2) : guard_exports.IsEqual(action, "KeyOf") ? KeyOfInstantiate(context, state, parameters[0], options2) : guard_exports.IsEqual(action, "Lowercase") ? LowercaseInstantiate(context, state, parameters[0], options2) : guard_exports.IsEqual(action, "Mapped") ? MappedInstantiate(context, state, parameters[0], parameters[1], parameters[2], parameters[3], options2) : guard_exports.IsEqual(action, "Module") ? ModuleInstantiate(context, state, parameters[0], options2) : guard_exports.IsEqual(action, "NonNullable") ? NonNullableInstantiate(context, state, parameters[0], options2) : guard_exports.IsEqual(action, "Pick") ? PickInstantiate(context, state, parameters[0], parameters[1], options2) : guard_exports.IsEqual(action, "Parameters") ? ParametersInstantiate(context, state, parameters[0], options2) : guard_exports.IsEqual(action, "Partial") ? PartialInstantiate(context, state, parameters[0], options2) : guard_exports.IsEqual(action, "Omit") ? OmitInstantiate(context, state, parameters[0], parameters[1], options2) : guard_exports.IsEqual(action, "ReadonlyObject") ? ReadonlyObjectInstantiate(context, state, parameters[0], options2) : guard_exports.IsEqual(action, "Record") ? RecordInstantiate(context, state, parameters[0], parameters[1], options2) : guard_exports.IsEqual(action, "Required") ? RequiredInstantiate(context, state, parameters[0], options2) : guard_exports.IsEqual(action, "ReturnType") ? ReturnTypeInstantiate(context, state, parameters[0], options2) : guard_exports.IsEqual(action, "TemplateLiteral") ? TemplateLiteralInstantiate(context, state, parameters[0], options2) : guard_exports.IsEqual(action, "Uncapitalize") ? UncapitalizeInstantiate(context, state, parameters[0], options2) : guard_exports.IsEqual(action, "Uppercase") ? UppercaseInstantiate(context, state, parameters[0], options2) : guard_exports.IsEqual(action, "With") ? WithInstantiate(context, state, parameters[0], parameters[1]) : Deferred(action, parameters, options2)
    )
  );
}
function InstantiateImmediate(context, state, type) {
  const instantiatedType = IsRef(type) ? RefInstantiate(context, state, type, type.$ref) : IsArray2(type) ? _Array_(InstantiateType(context, state, type.items), ArrayOptions(type)) : IsCall(type) ? CallInstantiate(context, state, type.target, type.arguments) : IsConstructor2(type) ? Constructor(InstantiateTypes(context, state, type.parameters), InstantiateType(context, state, type.instanceType), ConstructorOptions(type)) : IsFunction2(type) ? _Function_(InstantiateTypes(context, state, type.parameters), InstantiateType(context, state, type.returnType), FunctionOptions(type)) : IsDependent(type) ? Dependent(InstantiateType(context, state, type.if), InstantiateType(context, state, type.then), InstantiateType(context, state, type.else), DependentOptions(type)) : IsIntersect(type) ? Intersect(InstantiateTypes(context, state, type.allOf), IntersectOptions(type)) : IsObject2(type) ? _Object_(InstantiateProperties(context, state, type.properties), ObjectOptions(type)) : IsRecord(type) ? RecordFromPattern(RecordPattern(type), InstantiateType(context, state, RecordValue(type))) : IsRest(type) ? Rest(InstantiateType(context, state, type.items)) : IsTuple(type) ? Tuple(InstantiateElements(context, state, type.items), TupleOptions(type)) : IsUnion(type) ? Union(InstantiateTypes(context, state, type.anyOf), UnionOptions(type)) : type;
  const withModifiers = WithModifiers(type, instantiatedType);
  return withModifiers;
}
function InstantiateType(context, state, type) {
  const result = IsDeferred(type) ? InstantiateDeferred(context, state, type.action, type.parameters, type.options) : InstantiateImmediate(context, state, type);
  return result;
}
function Instantiate(context, type) {
  return InstantiateType(context, State([], []), type);
}
var init_instantiate27 = __esm({
  "node_modules/typebox/build/type/engine/instantiate.mjs"() {
    init_guard2();
    init_instantiate_add3();
    init_instantiate_add();
    init_instantiate_add2();
    init_array();
    init_constructor();
    init_deferred();
    init_function();
    init_call();
    init_dependent();
    init_intersect();
    init_object();
    init_record();
    init_tuple();
    init_union();
    init_ref();
    init_rest();
    init_instantiate_add3();
    init_instantiate_remove3();
    init_instantiate_add();
    init_instantiate_remove();
    init_instantiate_add2();
    init_instantiate_remove2();
    init_optional();
    init_immutable();
    init_readonly();
    init_instantiate6();
    init_instantiate7();
    init_conditional2();
    init_instantiate9();
    init_instantiate5();
    init_instantiate10();
    init_instantiate11();
    init_instantiate12();
    init_instantiate13();
    init_instantiate3();
    init_instantiate14();
    init_instantiate7();
    init_instantiate15();
    init_instantiate16();
    init_instantiate17();
    init_instantiate18();
    init_instantiate19();
    init_instantiate20();
    init_instantiate21();
    init_instantiate22();
    init_instantiate();
    init_instantiate23();
    init_instantiate24();
    init_instantiate25();
    init_instantiate2();
    init_instantiate7();
    init_instantiate7();
    init_instantiate26();
    init_rest3();
  }
});

// node_modules/typebox/build/type/engine/immutable/instantiate_add.mjs
function AddImmutableOperation(type) {
  return memory_exports.Update(type, { "~immutable": true }, {});
}
function AddImmutableAction(type, options2) {
  const result = memory_exports.Update(AddImmutableOperation(type), {}, options2);
  return result;
}
function AddImmutableInstantiate(context, state, type, options2) {
  const instantiatedType = InstantiateType(context, state, type);
  return AddImmutableAction(instantiatedType, options2);
}
var init_instantiate_add3 = __esm({
  "node_modules/typebox/build/type/engine/immutable/instantiate_add.mjs"() {
    init_memory2();
    init_instantiate27();
  }
});

// node_modules/typebox/build/type/action/_add_immutable.mjs
function AddImmutableDeferred(type, options2 = {}) {
  return Deferred("AddImmutable", [type], options2);
}
function AddImmutable(type, options2 = {}) {
  return AddImmutableAction(type, options2);
}
var init_add_immutable = __esm({
  "node_modules/typebox/build/type/action/_add_immutable.mjs"() {
    init_deferred();
    init_instantiate_add3();
  }
});

// node_modules/typebox/build/type/action/_remove_immutable.mjs
var init_remove_immutable = __esm({
  "node_modules/typebox/build/type/action/_remove_immutable.mjs"() {
    init_deferred();
    init_instantiate_remove3();
  }
});

// node_modules/typebox/build/type/action/evaluate.mjs
function EvaluateDeferred(type, options2 = {}) {
  return Deferred("Evaluate", [type], options2);
}
function Evaluate(type, options2 = {}) {
  return EvaluateAction(type, options2);
}
var init_evaluate4 = __esm({
  "node_modules/typebox/build/type/action/evaluate.mjs"() {
    init_deferred();
    init_instantiate5();
  }
});

// node_modules/typebox/build/type/action/module.mjs
function ModuleDeferred(declarations, options2 = {}) {
  return Deferred("Module", [declarations], options2);
}
function Module2(declarations, options2 = {}) {
  return ModuleInstantiate({}, State([], []), declarations, options2);
}
var init_module = __esm({
  "node_modules/typebox/build/type/action/module.mjs"() {
    init_deferred();
    init_instantiate27();
    init_instantiate16();
  }
});

// node_modules/typebox/build/type/action/index.mjs
var init_action = __esm({
  "node_modules/typebox/build/type/action/index.mjs"() {
    init_add_immutable();
    init_add_readonly();
    init_add_optional();
    init_remove_immutable();
    init_remove_readonly();
    init_remove_optional();
    init_capitalize();
    init_conditional();
    init_constructor_parameters();
    init_evaluate4();
    init_exclude();
    init_extract();
    init_indexed();
    init_instance_type();
    init_interface();
    init_keyof();
    init_lowercase();
    init_mapped();
    init_module();
    init_non_nullable();
    init_omit();
    init_parameters2();
    init_partial();
    init_pick();
    init_readonly_object();
    init_required();
    init_return_type2();
    init_uncapitalize();
    init_uppercase();
    init_with();
  }
});

// node_modules/typebox/build/type/engine/constructor_parameters/index.mjs
var init_constructor_parameters2 = __esm({
  "node_modules/typebox/build/type/engine/constructor_parameters/index.mjs"() {
    init_instantiate9();
  }
});

// node_modules/typebox/build/type/engine/enum/index.mjs
var init_enum3 = __esm({
  "node_modules/typebox/build/type/engine/enum/index.mjs"() {
    init_typescript_enum_to_enum_values();
  }
});

// node_modules/typebox/build/type/engine/exclude/index.mjs
var init_exclude2 = __esm({
  "node_modules/typebox/build/type/engine/exclude/index.mjs"() {
    init_instantiate10();
  }
});

// node_modules/typebox/build/type/engine/extract/index.mjs
var init_extract2 = __esm({
  "node_modules/typebox/build/type/engine/extract/index.mjs"() {
    init_instantiate11();
  }
});

// node_modules/typebox/build/type/engine/helpers/union.mjs
var init_union3 = __esm({
  "node_modules/typebox/build/type/engine/helpers/union.mjs"() {
  }
});

// node_modules/typebox/build/type/engine/helpers/index.mjs
var init_helpers = __esm({
  "node_modules/typebox/build/type/engine/helpers/index.mjs"() {
    init_keys_to_indexer();
    init_keys();
    init_union3();
  }
});

// node_modules/typebox/build/type/engine/indexed/index.mjs
var init_indexed2 = __esm({
  "node_modules/typebox/build/type/engine/indexed/index.mjs"() {
    init_instantiate12();
  }
});

// node_modules/typebox/build/type/engine/instance_type/index.mjs
var init_instance_type2 = __esm({
  "node_modules/typebox/build/type/engine/instance_type/index.mjs"() {
    init_instantiate13();
  }
});

// node_modules/typebox/build/type/engine/interface/index.mjs
var init_interface2 = __esm({
  "node_modules/typebox/build/type/engine/interface/index.mjs"() {
    init_instantiate3();
  }
});

// node_modules/typebox/build/type/engine/intrinsics/index.mjs
var init_intrinsics = __esm({
  "node_modules/typebox/build/type/engine/intrinsics/index.mjs"() {
    init_instantiate7();
  }
});

// node_modules/typebox/build/type/engine/keyof/index.mjs
var init_keyof2 = __esm({
  "node_modules/typebox/build/type/engine/keyof/index.mjs"() {
    init_instantiate14();
  }
});

// node_modules/typebox/build/type/engine/mapped/index.mjs
var init_mapped2 = __esm({
  "node_modules/typebox/build/type/engine/mapped/index.mjs"() {
    init_instantiate15();
  }
});

// node_modules/typebox/build/type/engine/module/index.mjs
var init_module2 = __esm({
  "node_modules/typebox/build/type/engine/module/index.mjs"() {
    init_instantiate16();
  }
});

// node_modules/typebox/build/type/engine/non_nullable/index.mjs
var init_non_nullable2 = __esm({
  "node_modules/typebox/build/type/engine/non_nullable/index.mjs"() {
    init_instantiate17();
  }
});

// node_modules/typebox/build/type/engine/omit/index.mjs
var init_omit2 = __esm({
  "node_modules/typebox/build/type/engine/omit/index.mjs"() {
    init_instantiate18();
  }
});

// node_modules/typebox/build/type/engine/parameters/index.mjs
var init_parameters3 = __esm({
  "node_modules/typebox/build/type/engine/parameters/index.mjs"() {
    init_instantiate19();
  }
});

// node_modules/typebox/build/type/engine/patterns/index.mjs
var init_patterns = __esm({
  "node_modules/typebox/build/type/engine/patterns/index.mjs"() {
    init_pattern();
    init_template();
  }
});

// node_modules/typebox/build/type/engine/partial/index.mjs
var init_partial2 = __esm({
  "node_modules/typebox/build/type/engine/partial/index.mjs"() {
    init_instantiate20();
  }
});

// node_modules/typebox/build/type/engine/pick/index.mjs
var init_pick2 = __esm({
  "node_modules/typebox/build/type/engine/pick/index.mjs"() {
    init_instantiate21();
  }
});

// node_modules/typebox/build/type/engine/priority/priority.mjs
var init_priority = __esm({
  "node_modules/typebox/build/type/engine/priority/priority.mjs"() {
    init_guard2();
    init_compare();
  }
});

// node_modules/typebox/build/type/engine/priority/index.mjs
var init_priority2 = __esm({
  "node_modules/typebox/build/type/engine/priority/index.mjs"() {
    init_priority();
  }
});

// node_modules/typebox/build/type/engine/readonly_object/index.mjs
var init_readonly_object2 = __esm({
  "node_modules/typebox/build/type/engine/readonly_object/index.mjs"() {
    init_instantiate22();
  }
});

// node_modules/typebox/build/type/engine/record/index.mjs
var init_record3 = __esm({
  "node_modules/typebox/build/type/engine/record/index.mjs"() {
    init_instantiate();
  }
});

// node_modules/typebox/build/type/engine/ref/index.mjs
var init_ref2 = __esm({
  "node_modules/typebox/build/type/engine/ref/index.mjs"() {
    init_instantiate23();
  }
});

// node_modules/typebox/build/type/engine/required/index.mjs
var init_required2 = __esm({
  "node_modules/typebox/build/type/engine/required/index.mjs"() {
    init_instantiate24();
  }
});

// node_modules/typebox/build/type/engine/return_type/index.mjs
var init_return_type3 = __esm({
  "node_modules/typebox/build/type/engine/return_type/index.mjs"() {
    init_instantiate25();
  }
});

// node_modules/typebox/build/type/engine/template_literal/static.mjs
var init_static2 = __esm({
  "node_modules/typebox/build/type/engine/template_literal/static.mjs"() {
  }
});

// node_modules/typebox/build/type/engine/template_literal/is_pattern.mjs
var init_is_pattern = __esm({
  "node_modules/typebox/build/type/engine/template_literal/is_pattern.mjs"() {
    init_guard2();
    init_pattern();
  }
});

// node_modules/typebox/build/type/engine/template_literal/index.mjs
var init_template_literal3 = __esm({
  "node_modules/typebox/build/type/engine/template_literal/index.mjs"() {
    init_create2();
    init_decode();
    init_encode();
    init_static2();
    init_is_finite();
    init_is_pattern();
  }
});

// node_modules/typebox/build/type/engine/with/index.mjs
var init_with2 = __esm({
  "node_modules/typebox/build/type/engine/with/index.mjs"() {
    init_instantiate26();
  }
});

// node_modules/typebox/build/type/engine/index.mjs
var init_engine = __esm({
  "node_modules/typebox/build/type/engine/index.mjs"() {
    init_instantiate27();
    init_conditional2();
    init_constructor_parameters2();
    init_cyclic2();
    init_enum3();
    init_evaluate3();
    init_exclude2();
    init_extract2();
    init_helpers();
    init_indexed2();
    init_instance_type2();
    init_interface2();
    init_intrinsics();
    init_keyof2();
    init_mapped2();
    init_module2();
    init_non_nullable2();
    init_object3();
    init_omit2();
    init_parameters3();
    init_patterns();
    init_partial2();
    init_pick2();
    init_priority2();
    init_readonly_object2();
    init_record3();
    init_ref2();
    init_required2();
    init_return_type3();
    init_template_literal3();
    init_with2();
  }
});

// node_modules/typebox/build/type/script/script.mjs
function Script2(...args) {
  const [context, input, options2] = arguments_exports.Match(args, {
    2: (script, options3) => guard_exports.IsString(script) ? [{}, script, options3] : [script, options3, {}],
    3: (context2, script, options3) => [context2, script, options3],
    1: (script) => [{}, script, {}]
  });
  const result = Script(input);
  const parsed = guard_exports.IsArray(result) && guard_exports.IsEqual(result.length, 2) ? InstantiateType(context, State([], []), result[0]) : Never();
  return memory_exports.Update(parsed, {}, options2);
}
var init_script = __esm({
  "node_modules/typebox/build/type/script/script.mjs"() {
    init_arguments2();
    init_memory2();
    init_guard2();
    init_types();
    init_instantiate27();
    init_instantiate27();
    init_parser();
  }
});

// node_modules/typebox/build/type/script/index.mjs
var init_script2 = __esm({
  "node_modules/typebox/build/type/script/index.mjs"() {
    init_script();
  }
});

// node_modules/typebox/build/typebox.mjs
var typebox_exports = {};
__export(typebox_exports, {
  Any: () => Any,
  Array: () => _Array_,
  BigInt: () => BigInt2,
  Boolean: () => Boolean2,
  Call: () => Call,
  Capitalize: () => Capitalize,
  Codec: () => Codec,
  Conditional: () => Conditional,
  Constructor: () => Constructor,
  ConstructorParameters: () => ConstructorParameters,
  Cyclic: () => Cyclic,
  Decode: () => Decode,
  DecodeBuilder: () => DecodeBuilder,
  Dependent: () => Dependent,
  Encode: () => Encode,
  EncodeBuilder: () => EncodeBuilder,
  Enum: () => Enum,
  Evaluate: () => Evaluate,
  Exclude: () => Exclude,
  Extends: () => Extends,
  ExtendsResult: () => result_exports,
  Extract: () => Extract,
  Function: () => _Function_,
  Generic: () => Generic,
  Identifier: () => Identifier,
  Immutable: () => Immutable,
  Index: () => Index,
  Infer: () => Infer,
  InstanceType: () => InstanceType,
  Instantiate: () => Instantiate,
  Integer: () => Integer,
  Interface: () => Interface,
  Intersect: () => Intersect,
  IsAny: () => IsAny,
  IsArray: () => IsArray2,
  IsBigInt: () => IsBigInt2,
  IsBoolean: () => IsBoolean3,
  IsCall: () => IsCall,
  IsCodec: () => IsCodec,
  IsConstructor: () => IsConstructor2,
  IsCyclic: () => IsCyclic,
  IsDependent: () => IsDependent,
  IsEnum: () => IsEnum,
  IsEnumValue: () => IsEnumValue,
  IsFunction: () => IsFunction2,
  IsGeneric: () => IsGeneric,
  IsIdentifier: () => IsIdentifier,
  IsImmutable: () => IsImmutable,
  IsInfer: () => IsInfer,
  IsInteger: () => IsInteger2,
  IsIntersect: () => IsIntersect,
  IsKind: () => IsKind,
  IsLiteral: () => IsLiteral,
  IsNever: () => IsNever,
  IsNull: () => IsNull2,
  IsNumber: () => IsNumber3,
  IsObject: () => IsObject2,
  IsOptional: () => IsOptional,
  IsParameter: () => IsParameter,
  IsReadonly: () => IsReadonly,
  IsRecord: () => IsRecord,
  IsRef: () => IsRef,
  IsRefine: () => IsRefine,
  IsRest: () => IsRest,
  IsSchema: () => IsSchema,
  IsString: () => IsString3,
  IsSymbol: () => IsSymbol2,
  IsTemplateLiteral: () => IsTemplateLiteral,
  IsThis: () => IsThis,
  IsTuple: () => IsTuple,
  IsUndefined: () => IsUndefined2,
  IsUnion: () => IsUnion,
  IsUnknown: () => IsUnknown,
  IsUnsafe: () => IsUnsafe,
  IsVoid: () => IsVoid,
  KeyOf: () => KeyOf2,
  Literal: () => Literal,
  Lowercase: () => Lowercase,
  Mapped: () => Mapped,
  Module: () => Module2,
  Never: () => Never,
  NonNullable: () => NonNullable,
  Null: () => Null,
  Number: () => Number2,
  Object: () => _Object_,
  Omit: () => Omit,
  Optional: () => Optional,
  Parameter: () => Parameter,
  Parameters: () => Parameters,
  Partial: () => Partial,
  Pick: () => Pick,
  Readonly: () => Readonly,
  ReadonlyObject: () => ReadonlyObject,
  ReadonlyType: () => ReadonlyType,
  Record: () => Record,
  RecordKey: () => RecordKey,
  RecordPattern: () => RecordPattern,
  RecordValue: () => RecordValue,
  Ref: () => Ref,
  Refine: () => Refine,
  Required: () => Required,
  Rest: () => Rest,
  ReturnType: () => ReturnType,
  Script: () => Script2,
  String: () => String2,
  Symbol: () => Symbol2,
  TemplateLiteral: () => TemplateLiteral2,
  This: () => This,
  Tuple: () => Tuple,
  Uncapitalize: () => Uncapitalize,
  Undefined: () => Undefined,
  Union: () => Union,
  Unknown: () => Unknown,
  Unsafe: () => Unsafe,
  Uppercase: () => Uppercase,
  Void: () => Void,
  With: () => With2
});
var init_typebox = __esm({
  "node_modules/typebox/build/typebox.mjs"() {
    init_instantiate27();
    init_extends3();
    init_script2();
    init_capitalize();
    init_conditional();
    init_constructor_parameters();
    init_evaluate4();
    init_exclude();
    init_extract();
    init_action();
    init_instance_type();
    init_interface();
    init_keyof();
    init_lowercase();
    init_mapped();
    init_module();
    init_non_nullable();
    init_omit();
    init_parameters2();
    init_partial();
    init_pick();
    init_readonly_object();
    init_required();
    init_return_type2();
    init_uncapitalize();
    init_uppercase();
    init_with();
    init_codec();
    init_immutable();
    init_optional();
    init_readonly();
    init_refine();
    init_any();
    init_array();
    init_bigint();
    init_boolean();
    init_call();
    init_constructor();
    init_cyclic();
    init_enum();
    init_function();
    init_generic();
    init_identifier();
    init_dependent();
    init_infer();
    init_integer();
    init_intersect();
    init_literal();
    init_never();
    init_null();
    init_number();
    init_object();
    init_parameter();
    init_record();
    init_ref();
    init_rest();
    init_schema();
    init_string2();
    init_symbol();
    init_template_literal();
    init_this();
    init_tuple();
    init_undefined();
    init_union();
    init_unknown();
    init_unsafe();
    init_void();
  }
});

// node_modules/typebox/build/index.mjs
var init_build = __esm({
  "node_modules/typebox/build/index.mjs"() {
    init_action();
    init_engine();
    init_extends3();
    init_script2();
    init_types();
    init_typebox();
    init_typebox();
  }
});

// node_modules/@mariozechner/pi-tui/dist/fuzzy.js
var init_fuzzy = __esm({
  "node_modules/@mariozechner/pi-tui/dist/fuzzy.js"() {
  }
});

// node_modules/@mariozechner/pi-tui/dist/autocomplete.js
var init_autocomplete = __esm({
  "node_modules/@mariozechner/pi-tui/dist/autocomplete.js"() {
    init_fuzzy();
  }
});

// node_modules/get-east-asian-width/lookup-data.js
var ambiguousMinimalCodePoint, ambiguousMaximumCodePoint, ambiguousRanges, fullwidthMinimalCodePoint, fullwidthMaximumCodePoint, fullwidthRanges, wideMinimalCodePoint, wideMaximumCodePoint, wideRanges;
var init_lookup_data = __esm({
  "node_modules/get-east-asian-width/lookup-data.js"() {
    ambiguousMinimalCodePoint = 161;
    ambiguousMaximumCodePoint = 1114109;
    ambiguousRanges = [161, 161, 164, 164, 167, 168, 170, 170, 173, 174, 176, 180, 182, 186, 188, 191, 198, 198, 208, 208, 215, 216, 222, 225, 230, 230, 232, 234, 236, 237, 240, 240, 242, 243, 247, 250, 252, 252, 254, 254, 257, 257, 273, 273, 275, 275, 283, 283, 294, 295, 299, 299, 305, 307, 312, 312, 319, 322, 324, 324, 328, 331, 333, 333, 338, 339, 358, 359, 363, 363, 462, 462, 464, 464, 466, 466, 468, 468, 470, 470, 472, 472, 474, 474, 476, 476, 593, 593, 609, 609, 708, 708, 711, 711, 713, 715, 717, 717, 720, 720, 728, 731, 733, 733, 735, 735, 768, 879, 913, 929, 931, 937, 945, 961, 963, 969, 1025, 1025, 1040, 1103, 1105, 1105, 8208, 8208, 8211, 8214, 8216, 8217, 8220, 8221, 8224, 8226, 8228, 8231, 8240, 8240, 8242, 8243, 8245, 8245, 8251, 8251, 8254, 8254, 8308, 8308, 8319, 8319, 8321, 8324, 8364, 8364, 8451, 8451, 8453, 8453, 8457, 8457, 8467, 8467, 8470, 8470, 8481, 8482, 8486, 8486, 8491, 8491, 8531, 8532, 8539, 8542, 8544, 8555, 8560, 8569, 8585, 8585, 8592, 8601, 8632, 8633, 8658, 8658, 8660, 8660, 8679, 8679, 8704, 8704, 8706, 8707, 8711, 8712, 8715, 8715, 8719, 8719, 8721, 8721, 8725, 8725, 8730, 8730, 8733, 8736, 8739, 8739, 8741, 8741, 8743, 8748, 8750, 8750, 8756, 8759, 8764, 8765, 8776, 8776, 8780, 8780, 8786, 8786, 8800, 8801, 8804, 8807, 8810, 8811, 8814, 8815, 8834, 8835, 8838, 8839, 8853, 8853, 8857, 8857, 8869, 8869, 8895, 8895, 8978, 8978, 9312, 9449, 9451, 9547, 9552, 9587, 9600, 9615, 9618, 9621, 9632, 9633, 9635, 9641, 9650, 9651, 9654, 9655, 9660, 9661, 9664, 9665, 9670, 9672, 9675, 9675, 9678, 9681, 9698, 9701, 9711, 9711, 9733, 9734, 9737, 9737, 9742, 9743, 9756, 9756, 9758, 9758, 9792, 9792, 9794, 9794, 9824, 9825, 9827, 9829, 9831, 9834, 9836, 9837, 9839, 9839, 9886, 9887, 9919, 9919, 9926, 9933, 9935, 9939, 9941, 9953, 9955, 9955, 9960, 9961, 9963, 9969, 9972, 9972, 9974, 9977, 9979, 9980, 9982, 9983, 10045, 10045, 10102, 10111, 11094, 11097, 12872, 12879, 57344, 63743, 65024, 65039, 65533, 65533, 127232, 127242, 127248, 127277, 127280, 127337, 127344, 127373, 127375, 127376, 127387, 127404, 917760, 917999, 983040, 1048573, 1048576, 1114109];
    fullwidthMinimalCodePoint = 12288;
    fullwidthMaximumCodePoint = 65510;
    fullwidthRanges = [12288, 12288, 65281, 65376, 65504, 65510];
    wideMinimalCodePoint = 4352;
    wideMaximumCodePoint = 262141;
    wideRanges = [4352, 4447, 8986, 8987, 9001, 9002, 9193, 9196, 9200, 9200, 9203, 9203, 9725, 9726, 9748, 9749, 9776, 9783, 9800, 9811, 9855, 9855, 9866, 9871, 9875, 9875, 9889, 9889, 9898, 9899, 9917, 9918, 9924, 9925, 9934, 9934, 9940, 9940, 9962, 9962, 9970, 9971, 9973, 9973, 9978, 9978, 9981, 9981, 9989, 9989, 9994, 9995, 10024, 10024, 10060, 10060, 10062, 10062, 10067, 10069, 10071, 10071, 10133, 10135, 10160, 10160, 10175, 10175, 11035, 11036, 11088, 11088, 11093, 11093, 11904, 11929, 11931, 12019, 12032, 12245, 12272, 12287, 12289, 12350, 12353, 12438, 12441, 12543, 12549, 12591, 12593, 12686, 12688, 12773, 12783, 12830, 12832, 12871, 12880, 42124, 42128, 42182, 43360, 43388, 44032, 55203, 63744, 64255, 65040, 65049, 65072, 65106, 65108, 65126, 65128, 65131, 94176, 94180, 94192, 94198, 94208, 101589, 101631, 101662, 101760, 101874, 110576, 110579, 110581, 110587, 110589, 110590, 110592, 110882, 110898, 110898, 110928, 110930, 110933, 110933, 110948, 110951, 110960, 111355, 119552, 119638, 119648, 119670, 126980, 126980, 127183, 127183, 127374, 127374, 127377, 127386, 127488, 127490, 127504, 127547, 127552, 127560, 127568, 127569, 127584, 127589, 127744, 127776, 127789, 127797, 127799, 127868, 127870, 127891, 127904, 127946, 127951, 127955, 127968, 127984, 127988, 127988, 127992, 128062, 128064, 128064, 128066, 128252, 128255, 128317, 128331, 128334, 128336, 128359, 128378, 128378, 128405, 128406, 128420, 128420, 128507, 128591, 128640, 128709, 128716, 128716, 128720, 128722, 128725, 128728, 128732, 128735, 128747, 128748, 128756, 128764, 128992, 129003, 129008, 129008, 129292, 129338, 129340, 129349, 129351, 129535, 129648, 129660, 129664, 129674, 129678, 129734, 129736, 129736, 129741, 129756, 129759, 129770, 129775, 129784, 131072, 196605, 196608, 262141];
  }
});

// node_modules/get-east-asian-width/utilities.js
var isInRange;
var init_utilities = __esm({
  "node_modules/get-east-asian-width/utilities.js"() {
    isInRange = (ranges, codePoint) => {
      let low = 0;
      let high = Math.floor(ranges.length / 2) - 1;
      while (low <= high) {
        const mid = Math.floor((low + high) / 2);
        const i = mid * 2;
        if (codePoint < ranges[i]) {
          high = mid - 1;
        } else if (codePoint > ranges[i + 1]) {
          low = mid + 1;
        } else {
          return true;
        }
      }
      return false;
    };
  }
});

// node_modules/get-east-asian-width/lookup.js
function findWideFastPathRange(ranges) {
  let fastPathStart = ranges[0];
  let fastPathEnd = ranges[1];
  for (let index = 0; index < ranges.length; index += 2) {
    const start = ranges[index];
    const end = ranges[index + 1];
    if (commonCjkCodePoint >= start && commonCjkCodePoint <= end) {
      return [start, end];
    }
    if (end - start > fastPathEnd - fastPathStart) {
      fastPathStart = start;
      fastPathEnd = end;
    }
  }
  return [fastPathStart, fastPathEnd];
}
var commonCjkCodePoint, wideFastPathStart, wideFastPathEnd, isAmbiguous, isFullWidth, isWide;
var init_lookup = __esm({
  "node_modules/get-east-asian-width/lookup.js"() {
    init_lookup_data();
    init_utilities();
    commonCjkCodePoint = 19968;
    [wideFastPathStart, wideFastPathEnd] = /* @__PURE__ */ findWideFastPathRange(wideRanges);
    isAmbiguous = (codePoint) => {
      if (codePoint < ambiguousMinimalCodePoint || codePoint > ambiguousMaximumCodePoint) {
        return false;
      }
      return isInRange(ambiguousRanges, codePoint);
    };
    isFullWidth = (codePoint) => {
      if (codePoint < fullwidthMinimalCodePoint || codePoint > fullwidthMaximumCodePoint) {
        return false;
      }
      return isInRange(fullwidthRanges, codePoint);
    };
    isWide = (codePoint) => {
      if (codePoint >= wideFastPathStart && codePoint <= wideFastPathEnd) {
        return true;
      }
      if (codePoint < wideMinimalCodePoint || codePoint > wideMaximumCodePoint) {
        return false;
      }
      return isInRange(wideRanges, codePoint);
    };
  }
});

// node_modules/get-east-asian-width/index.js
function validate(codePoint) {
  if (!Number.isSafeInteger(codePoint)) {
    throw new TypeError(`Expected a code point, got \`${typeof codePoint}\`.`);
  }
}
function eastAsianWidth(codePoint, { ambiguousAsWide = false } = {}) {
  validate(codePoint);
  if (isFullWidth(codePoint) || isWide(codePoint) || ambiguousAsWide && isAmbiguous(codePoint)) {
    return 2;
  }
  return 1;
}
var init_get_east_asian_width = __esm({
  "node_modules/get-east-asian-width/index.js"() {
    init_lookup();
  }
});

// node_modules/@mariozechner/pi-tui/dist/utils.js
function getSegmenter() {
  return segmenter;
}
function couldBeEmoji(segment) {
  const cp = segment.codePointAt(0);
  return cp >= 126976 && cp <= 130047 || // Emoji and Pictograph
  cp >= 8960 && cp <= 9215 || // Misc technical
  cp >= 9728 && cp <= 10175 || // Misc symbols, dingbats
  cp >= 11088 && cp <= 11093 || // Specific stars/circles
  segment.includes("\uFE0F") || // Contains VS16 (emoji presentation selector)
  segment.length > 2;
}
function isPrintableAscii(str) {
  for (let i = 0; i < str.length; i++) {
    const code = str.charCodeAt(i);
    if (code < 32 || code > 126) {
      return false;
    }
  }
  return true;
}
function truncateFragmentToWidth(text, maxWidth) {
  if (maxWidth <= 0 || text.length === 0) {
    return { text: "", width: 0 };
  }
  if (isPrintableAscii(text)) {
    const clipped = text.slice(0, maxWidth);
    return { text: clipped, width: clipped.length };
  }
  const hasAnsi = text.includes("\x1B");
  const hasTabs = text.includes("	");
  if (!hasAnsi && !hasTabs) {
    let result2 = "";
    let width2 = 0;
    for (const { segment } of segmenter.segment(text)) {
      const w = graphemeWidth(segment);
      if (width2 + w > maxWidth) {
        break;
      }
      result2 += segment;
      width2 += w;
    }
    return { text: result2, width: width2 };
  }
  let result = "";
  let width = 0;
  let i = 0;
  let pendingAnsi = "";
  while (i < text.length) {
    const ansi = extractAnsiCode(text, i);
    if (ansi) {
      pendingAnsi += ansi.code;
      i += ansi.length;
      continue;
    }
    if (text[i] === "	") {
      if (width + 3 > maxWidth) {
        break;
      }
      if (pendingAnsi) {
        result += pendingAnsi;
        pendingAnsi = "";
      }
      result += "	";
      width += 3;
      i++;
      continue;
    }
    let end = i;
    while (end < text.length && text[end] !== "	") {
      const nextAnsi = extractAnsiCode(text, end);
      if (nextAnsi) {
        break;
      }
      end++;
    }
    for (const { segment } of segmenter.segment(text.slice(i, end))) {
      const w = graphemeWidth(segment);
      if (width + w > maxWidth) {
        return { text: result, width };
      }
      if (pendingAnsi) {
        result += pendingAnsi;
        pendingAnsi = "";
      }
      result += segment;
      width += w;
    }
    i = end;
  }
  return { text: result, width };
}
function finalizeTruncatedResult(prefix, prefixWidth, ellipsis, ellipsisWidth, maxWidth, pad) {
  const reset = "\x1B[0m";
  const visibleWidth2 = prefixWidth + ellipsisWidth;
  let result;
  if (ellipsis.length > 0) {
    result = `${prefix}${reset}${ellipsis}${reset}`;
  } else {
    result = `${prefix}${reset}`;
  }
  return pad ? result + " ".repeat(Math.max(0, maxWidth - visibleWidth2)) : result;
}
function graphemeWidth(segment) {
  if (zeroWidthRegex.test(segment)) {
    return 0;
  }
  if (couldBeEmoji(segment) && rgiEmojiRegex.test(segment)) {
    return 2;
  }
  const base = segment.replace(leadingNonPrintingRegex, "");
  const cp = base.codePointAt(0);
  if (cp === void 0) {
    return 0;
  }
  if (cp >= 127462 && cp <= 127487) {
    return 2;
  }
  let width = eastAsianWidth(cp);
  if (segment.length > 1) {
    for (const char of segment.slice(1)) {
      const c = char.codePointAt(0);
      if (c >= 65280 && c <= 65519) {
        width += eastAsianWidth(c);
      } else if (c === 3635 || c === 3763) {
        width += 1;
      }
    }
  }
  return width;
}
function visibleWidth(str) {
  if (str.length === 0) {
    return 0;
  }
  if (isPrintableAscii(str)) {
    return str.length;
  }
  const cached = widthCache.get(str);
  if (cached !== void 0) {
    return cached;
  }
  let clean = str;
  if (str.includes("	")) {
    clean = clean.replace(/\t/g, "   ");
  }
  if (clean.includes("\x1B")) {
    let stripped = "";
    let i = 0;
    while (i < clean.length) {
      const ansi = extractAnsiCode(clean, i);
      if (ansi) {
        i += ansi.length;
        continue;
      }
      stripped += clean[i];
      i++;
    }
    clean = stripped;
  }
  let width = 0;
  for (const { segment } of segmenter.segment(clean)) {
    width += graphemeWidth(segment);
  }
  if (widthCache.size >= WIDTH_CACHE_SIZE) {
    const firstKey = widthCache.keys().next().value;
    if (firstKey !== void 0) {
      widthCache.delete(firstKey);
    }
  }
  widthCache.set(str, width);
  return width;
}
function extractAnsiCode(str, pos) {
  if (pos >= str.length || str[pos] !== "\x1B")
    return null;
  const next = str[pos + 1];
  if (next === "[") {
    let j = pos + 2;
    while (j < str.length && !/[mGKHJ]/.test(str[j]))
      j++;
    if (j < str.length)
      return { code: str.substring(pos, j + 1), length: j + 1 - pos };
    return null;
  }
  if (next === "]") {
    let j = pos + 2;
    while (j < str.length) {
      if (str[j] === "\x07")
        return { code: str.substring(pos, j + 1), length: j + 1 - pos };
      if (str[j] === "\x1B" && str[j + 1] === "\\")
        return { code: str.substring(pos, j + 2), length: j + 2 - pos };
      j++;
    }
    return null;
  }
  if (next === "_") {
    let j = pos + 2;
    while (j < str.length) {
      if (str[j] === "\x07")
        return { code: str.substring(pos, j + 1), length: j + 1 - pos };
      if (str[j] === "\x1B" && str[j + 1] === "\\")
        return { code: str.substring(pos, j + 2), length: j + 2 - pos };
      j++;
    }
    return null;
  }
  return null;
}
function parseOsc8Hyperlink(ansiCode) {
  if (!ansiCode.startsWith("\x1B]8;")) {
    return void 0;
  }
  const terminator = ansiCode.endsWith("\x07") ? "\x07" : "\x1B\\";
  const body = ansiCode.slice(4, terminator === "\x07" ? -1 : -2);
  const separatorIndex = body.indexOf(";");
  if (separatorIndex === -1) {
    return void 0;
  }
  const params = body.slice(0, separatorIndex);
  const url = body.slice(separatorIndex + 1);
  if (!url) {
    return null;
  }
  return { params, url, terminator };
}
function formatOsc8Hyperlink(hyperlink2) {
  return `\x1B]8;${hyperlink2.params};${hyperlink2.url}${hyperlink2.terminator}`;
}
function formatOsc8Close(terminator) {
  return `\x1B]8;;${terminator}`;
}
function updateTrackerFromText(text, tracker) {
  let i = 0;
  while (i < text.length) {
    const ansiResult = extractAnsiCode(text, i);
    if (ansiResult) {
      tracker.process(ansiResult.code);
      i += ansiResult.length;
    } else {
      i++;
    }
  }
}
function splitIntoTokensWithAnsi(text) {
  const tokens = [];
  let current = "";
  let pendingAnsi = "";
  let inWhitespace = false;
  let i = 0;
  while (i < text.length) {
    const ansiResult = extractAnsiCode(text, i);
    if (ansiResult) {
      pendingAnsi += ansiResult.code;
      i += ansiResult.length;
      continue;
    }
    const char = text[i];
    const charIsSpace = char === " ";
    if (charIsSpace !== inWhitespace && current) {
      tokens.push(current);
      current = "";
    }
    if (pendingAnsi) {
      current += pendingAnsi;
      pendingAnsi = "";
    }
    inWhitespace = charIsSpace;
    current += char;
    i++;
  }
  if (pendingAnsi) {
    current += pendingAnsi;
  }
  if (current) {
    tokens.push(current);
  }
  return tokens;
}
function wrapTextWithAnsi(text, width) {
  if (!text) {
    return [""];
  }
  const inputLines = text.split("\n");
  const result = [];
  const tracker = new AnsiCodeTracker();
  for (const inputLine of inputLines) {
    const prefix = result.length > 0 ? tracker.getActiveCodes() : "";
    result.push(...wrapSingleLine(prefix + inputLine, width));
    updateTrackerFromText(inputLine, tracker);
  }
  return result.length > 0 ? result : [""];
}
function wrapSingleLine(line, width) {
  if (!line) {
    return [""];
  }
  const visibleLength = visibleWidth(line);
  if (visibleLength <= width) {
    return [line];
  }
  const wrapped = [];
  const tracker = new AnsiCodeTracker();
  const tokens = splitIntoTokensWithAnsi(line);
  let currentLine = "";
  let currentVisibleLength = 0;
  for (const token of tokens) {
    const tokenVisibleLength = visibleWidth(token);
    const isWhitespace = token.trim() === "";
    if (tokenVisibleLength > width && !isWhitespace) {
      if (currentLine) {
        const lineEndReset = tracker.getLineEndReset();
        if (lineEndReset) {
          currentLine += lineEndReset;
        }
        wrapped.push(currentLine);
        currentLine = "";
        currentVisibleLength = 0;
      }
      const broken = breakLongWord(token, width, tracker);
      wrapped.push(...broken.slice(0, -1));
      currentLine = broken[broken.length - 1];
      currentVisibleLength = visibleWidth(currentLine);
      continue;
    }
    const totalNeeded = currentVisibleLength + tokenVisibleLength;
    if (totalNeeded > width && currentVisibleLength > 0) {
      let lineToWrap = currentLine.trimEnd();
      const lineEndReset = tracker.getLineEndReset();
      if (lineEndReset) {
        lineToWrap += lineEndReset;
      }
      wrapped.push(lineToWrap);
      if (isWhitespace) {
        currentLine = tracker.getActiveCodes();
        currentVisibleLength = 0;
      } else {
        currentLine = tracker.getActiveCodes() + token;
        currentVisibleLength = tokenVisibleLength;
      }
    } else {
      currentLine += token;
      currentVisibleLength += tokenVisibleLength;
    }
    updateTrackerFromText(token, tracker);
  }
  if (currentLine) {
    wrapped.push(currentLine);
  }
  return wrapped.length > 0 ? wrapped.map((line2) => line2.trimEnd()) : [""];
}
function breakLongWord(word, width, tracker) {
  const lines = [];
  let currentLine = tracker.getActiveCodes();
  let currentWidth = 0;
  let i = 0;
  const segments = [];
  while (i < word.length) {
    const ansiResult = extractAnsiCode(word, i);
    if (ansiResult) {
      segments.push({ type: "ansi", value: ansiResult.code });
      i += ansiResult.length;
    } else {
      let end = i;
      while (end < word.length) {
        const nextAnsi = extractAnsiCode(word, end);
        if (nextAnsi)
          break;
        end++;
      }
      const textPortion = word.slice(i, end);
      for (const seg of segmenter.segment(textPortion)) {
        segments.push({ type: "grapheme", value: seg.segment });
      }
      i = end;
    }
  }
  for (const seg of segments) {
    if (seg.type === "ansi") {
      currentLine += seg.value;
      tracker.process(seg.value);
      continue;
    }
    const grapheme = seg.value;
    if (!grapheme)
      continue;
    const graphemeWidth2 = visibleWidth(grapheme);
    if (currentWidth + graphemeWidth2 > width) {
      const lineEndReset = tracker.getLineEndReset();
      if (lineEndReset) {
        currentLine += lineEndReset;
      }
      lines.push(currentLine);
      currentLine = tracker.getActiveCodes();
      currentWidth = 0;
    }
    currentLine += grapheme;
    currentWidth += graphemeWidth2;
  }
  if (currentLine) {
    lines.push(currentLine);
  }
  return lines.length > 0 ? lines : [""];
}
function applyBackgroundToLine(line, width, bgFn) {
  const visibleLen = visibleWidth(line);
  const paddingNeeded = Math.max(0, width - visibleLen);
  const padding = " ".repeat(paddingNeeded);
  const withPadding = line + padding;
  return bgFn(withPadding);
}
function truncateToWidth(text, maxWidth, ellipsis = "...", pad = false) {
  if (maxWidth <= 0) {
    return "";
  }
  if (text.length === 0) {
    return pad ? " ".repeat(maxWidth) : "";
  }
  const ellipsisWidth = visibleWidth(ellipsis);
  if (ellipsisWidth >= maxWidth) {
    const textWidth = visibleWidth(text);
    if (textWidth <= maxWidth) {
      return pad ? text + " ".repeat(maxWidth - textWidth) : text;
    }
    const clippedEllipsis = truncateFragmentToWidth(ellipsis, maxWidth);
    if (clippedEllipsis.width === 0) {
      return pad ? " ".repeat(maxWidth) : "";
    }
    return finalizeTruncatedResult("", 0, clippedEllipsis.text, clippedEllipsis.width, maxWidth, pad);
  }
  if (isPrintableAscii(text)) {
    if (text.length <= maxWidth) {
      return pad ? text + " ".repeat(maxWidth - text.length) : text;
    }
    const targetWidth2 = maxWidth - ellipsisWidth;
    return finalizeTruncatedResult(text.slice(0, targetWidth2), targetWidth2, ellipsis, ellipsisWidth, maxWidth, pad);
  }
  const targetWidth = maxWidth - ellipsisWidth;
  let result = "";
  let pendingAnsi = "";
  let visibleSoFar = 0;
  let keptWidth = 0;
  let keepContiguousPrefix = true;
  let overflowed = false;
  let exhaustedInput = false;
  const hasAnsi = text.includes("\x1B");
  const hasTabs = text.includes("	");
  if (!hasAnsi && !hasTabs) {
    for (const { segment } of segmenter.segment(text)) {
      const width = graphemeWidth(segment);
      if (keepContiguousPrefix && keptWidth + width <= targetWidth) {
        result += segment;
        keptWidth += width;
      } else {
        keepContiguousPrefix = false;
      }
      visibleSoFar += width;
      if (visibleSoFar > maxWidth) {
        overflowed = true;
        break;
      }
    }
    exhaustedInput = !overflowed;
  } else {
    let i = 0;
    while (i < text.length) {
      const ansi = extractAnsiCode(text, i);
      if (ansi) {
        pendingAnsi += ansi.code;
        i += ansi.length;
        continue;
      }
      if (text[i] === "	") {
        if (keepContiguousPrefix && keptWidth + 3 <= targetWidth) {
          if (pendingAnsi) {
            result += pendingAnsi;
            pendingAnsi = "";
          }
          result += "	";
          keptWidth += 3;
        } else {
          keepContiguousPrefix = false;
          pendingAnsi = "";
        }
        visibleSoFar += 3;
        if (visibleSoFar > maxWidth) {
          overflowed = true;
          break;
        }
        i++;
        continue;
      }
      let end = i;
      while (end < text.length && text[end] !== "	") {
        const nextAnsi = extractAnsiCode(text, end);
        if (nextAnsi) {
          break;
        }
        end++;
      }
      for (const { segment } of segmenter.segment(text.slice(i, end))) {
        const width = graphemeWidth(segment);
        if (keepContiguousPrefix && keptWidth + width <= targetWidth) {
          if (pendingAnsi) {
            result += pendingAnsi;
            pendingAnsi = "";
          }
          result += segment;
          keptWidth += width;
        } else {
          keepContiguousPrefix = false;
          pendingAnsi = "";
        }
        visibleSoFar += width;
        if (visibleSoFar > maxWidth) {
          overflowed = true;
          break;
        }
      }
      if (overflowed) {
        break;
      }
      i = end;
    }
    exhaustedInput = i >= text.length;
  }
  if (!overflowed && exhaustedInput) {
    return pad ? text + " ".repeat(Math.max(0, maxWidth - visibleSoFar)) : text;
  }
  return finalizeTruncatedResult(result, keptWidth, ellipsis, ellipsisWidth, maxWidth, pad);
}
var segmenter, zeroWidthRegex, leadingNonPrintingRegex, rgiEmojiRegex, WIDTH_CACHE_SIZE, widthCache, AnsiCodeTracker, pooledStyleTracker;
var init_utils = __esm({
  "node_modules/@mariozechner/pi-tui/dist/utils.js"() {
    init_get_east_asian_width();
    segmenter = new Intl.Segmenter(void 0, { granularity: "grapheme" });
    zeroWidthRegex = new RegExp("^(?:\\p{Default_Ignorable_Code_Point}|\\p{Control}|\\p{Mark}|\\p{Surrogate})+$", "v");
    leadingNonPrintingRegex = new RegExp("^[\\p{Default_Ignorable_Code_Point}\\p{Control}\\p{Format}\\p{Mark}\\p{Surrogate}]+", "v");
    rgiEmojiRegex = new RegExp("^\\p{RGI_Emoji}$", "v");
    WIDTH_CACHE_SIZE = 512;
    widthCache = /* @__PURE__ */ new Map();
    AnsiCodeTracker = class {
      // Track individual attributes separately so we can reset them specifically
      bold = false;
      dim = false;
      italic = false;
      underline = false;
      blink = false;
      inverse = false;
      hidden = false;
      strikethrough = false;
      fgColor = null;
      // Stores the full code like "31" or "38;5;240"
      bgColor = null;
      // Stores the full code like "41" or "48;5;240"
      activeHyperlink = null;
      process(ansiCode) {
        const hyperlink2 = parseOsc8Hyperlink(ansiCode);
        if (hyperlink2 !== void 0) {
          this.activeHyperlink = hyperlink2;
          return;
        }
        if (!ansiCode.endsWith("m")) {
          return;
        }
        const match = ansiCode.match(/\x1b\[([\d;]*)m/);
        if (!match)
          return;
        const params = match[1];
        if (params === "" || params === "0") {
          this.reset();
          return;
        }
        const parts = params.split(";");
        let i = 0;
        while (i < parts.length) {
          const code = Number.parseInt(parts[i], 10);
          if (code === 38 || code === 48) {
            if (parts[i + 1] === "5" && parts[i + 2] !== void 0) {
              const colorCode = `${parts[i]};${parts[i + 1]};${parts[i + 2]}`;
              if (code === 38) {
                this.fgColor = colorCode;
              } else {
                this.bgColor = colorCode;
              }
              i += 3;
              continue;
            } else if (parts[i + 1] === "2" && parts[i + 4] !== void 0) {
              const colorCode = `${parts[i]};${parts[i + 1]};${parts[i + 2]};${parts[i + 3]};${parts[i + 4]}`;
              if (code === 38) {
                this.fgColor = colorCode;
              } else {
                this.bgColor = colorCode;
              }
              i += 5;
              continue;
            }
          }
          switch (code) {
            case 0:
              this.reset();
              break;
            case 1:
              this.bold = true;
              break;
            case 2:
              this.dim = true;
              break;
            case 3:
              this.italic = true;
              break;
            case 4:
              this.underline = true;
              break;
            case 5:
              this.blink = true;
              break;
            case 7:
              this.inverse = true;
              break;
            case 8:
              this.hidden = true;
              break;
            case 9:
              this.strikethrough = true;
              break;
            case 21:
              this.bold = false;
              break;
            // Some terminals
            case 22:
              this.bold = false;
              this.dim = false;
              break;
            case 23:
              this.italic = false;
              break;
            case 24:
              this.underline = false;
              break;
            case 25:
              this.blink = false;
              break;
            case 27:
              this.inverse = false;
              break;
            case 28:
              this.hidden = false;
              break;
            case 29:
              this.strikethrough = false;
              break;
            case 39:
              this.fgColor = null;
              break;
            // Default fg
            case 49:
              this.bgColor = null;
              break;
            // Default bg
            default:
              if (code >= 30 && code <= 37 || code >= 90 && code <= 97) {
                this.fgColor = String(code);
              } else if (code >= 40 && code <= 47 || code >= 100 && code <= 107) {
                this.bgColor = String(code);
              }
              break;
          }
          i++;
        }
      }
      reset() {
        this.bold = false;
        this.dim = false;
        this.italic = false;
        this.underline = false;
        this.blink = false;
        this.inverse = false;
        this.hidden = false;
        this.strikethrough = false;
        this.fgColor = null;
        this.bgColor = null;
      }
      /** Clear all state for reuse. */
      clear() {
        this.reset();
        this.activeHyperlink = null;
      }
      getActiveCodes() {
        const codes = [];
        if (this.bold)
          codes.push("1");
        if (this.dim)
          codes.push("2");
        if (this.italic)
          codes.push("3");
        if (this.underline)
          codes.push("4");
        if (this.blink)
          codes.push("5");
        if (this.inverse)
          codes.push("7");
        if (this.hidden)
          codes.push("8");
        if (this.strikethrough)
          codes.push("9");
        if (this.fgColor)
          codes.push(this.fgColor);
        if (this.bgColor)
          codes.push(this.bgColor);
        let result = codes.length > 0 ? `\x1B[${codes.join(";")}m` : "";
        if (this.activeHyperlink) {
          result += formatOsc8Hyperlink(this.activeHyperlink);
        }
        return result;
      }
      hasActiveCodes() {
        return this.bold || this.dim || this.italic || this.underline || this.blink || this.inverse || this.hidden || this.strikethrough || this.fgColor !== null || this.bgColor !== null || this.activeHyperlink !== null;
      }
      /**
       * Get reset codes for attributes that need to be turned off at line end.
       * Underline must be closed to prevent bleeding into padding.
       * Active OSC 8 hyperlinks must be closed and re-opened on the next line.
       * Returns empty string if no attributes need closing.
       */
      getLineEndReset() {
        let result = "";
        if (this.underline) {
          result += "\x1B[24m";
        }
        if (this.activeHyperlink) {
          result += formatOsc8Close(this.activeHyperlink.terminator);
        }
        return result;
      }
    };
    pooledStyleTracker = new AnsiCodeTracker();
  }
});

// node_modules/@mariozechner/pi-tui/dist/components/box.js
var init_box = __esm({
  "node_modules/@mariozechner/pi-tui/dist/components/box.js"() {
    init_utils();
  }
});

// node_modules/@mariozechner/pi-tui/dist/keys.js
var MODIFIERS, LOCK_MASK, ARROW_CODEPOINTS, FUNCTIONAL_CODEPOINTS, KITTY_FUNCTIONAL_KEY_EQUIVALENTS, KITTY_PRINTABLE_ALLOWED_MODIFIERS;
var init_keys2 = __esm({
  "node_modules/@mariozechner/pi-tui/dist/keys.js"() {
    MODIFIERS = {
      shift: 1,
      alt: 2,
      ctrl: 4,
      super: 8
    };
    LOCK_MASK = 64 + 128;
    ARROW_CODEPOINTS = {
      up: -1,
      down: -2,
      right: -3,
      left: -4
    };
    FUNCTIONAL_CODEPOINTS = {
      delete: -10,
      insert: -11,
      pageUp: -12,
      pageDown: -13,
      home: -14,
      end: -15
    };
    KITTY_FUNCTIONAL_KEY_EQUIVALENTS = /* @__PURE__ */ new Map([
      [57399, 48],
      // KP_0 -> 0
      [57400, 49],
      // KP_1 -> 1
      [57401, 50],
      // KP_2 -> 2
      [57402, 51],
      // KP_3 -> 3
      [57403, 52],
      // KP_4 -> 4
      [57404, 53],
      // KP_5 -> 5
      [57405, 54],
      // KP_6 -> 6
      [57406, 55],
      // KP_7 -> 7
      [57407, 56],
      // KP_8 -> 8
      [57408, 57],
      // KP_9 -> 9
      [57409, 46],
      // KP_DECIMAL -> .
      [57410, 47],
      // KP_DIVIDE -> /
      [57411, 42],
      // KP_MULTIPLY -> *
      [57412, 45],
      // KP_SUBTRACT -> -
      [57413, 43],
      // KP_ADD -> +
      [57415, 61],
      // KP_EQUAL -> =
      [57416, 44],
      // KP_SEPARATOR -> ,
      [57417, ARROW_CODEPOINTS.left],
      [57418, ARROW_CODEPOINTS.right],
      [57419, ARROW_CODEPOINTS.up],
      [57420, ARROW_CODEPOINTS.down],
      [57421, FUNCTIONAL_CODEPOINTS.pageUp],
      [57422, FUNCTIONAL_CODEPOINTS.pageDown],
      [57423, FUNCTIONAL_CODEPOINTS.home],
      [57424, FUNCTIONAL_CODEPOINTS.end],
      [57425, FUNCTIONAL_CODEPOINTS.insert],
      [57426, FUNCTIONAL_CODEPOINTS.delete]
    ]);
    KITTY_PRINTABLE_ALLOWED_MODIFIERS = MODIFIERS.shift | LOCK_MASK;
  }
});

// node_modules/@mariozechner/pi-tui/dist/keybindings.js
var init_keybindings = __esm({
  "node_modules/@mariozechner/pi-tui/dist/keybindings.js"() {
    init_keys2();
  }
});

// node_modules/@mariozechner/pi-tui/dist/components/text.js
var Text;
var init_text = __esm({
  "node_modules/@mariozechner/pi-tui/dist/components/text.js"() {
    init_utils();
    Text = class {
      text;
      paddingX;
      // Left/right padding
      paddingY;
      // Top/bottom padding
      customBgFn;
      // Cache for rendered output
      cachedText;
      cachedWidth;
      cachedLines;
      constructor(text = "", paddingX = 1, paddingY = 1, customBgFn) {
        this.text = text;
        this.paddingX = paddingX;
        this.paddingY = paddingY;
        this.customBgFn = customBgFn;
      }
      setText(text) {
        this.text = text;
        this.cachedText = void 0;
        this.cachedWidth = void 0;
        this.cachedLines = void 0;
      }
      setCustomBgFn(customBgFn) {
        this.customBgFn = customBgFn;
        this.cachedText = void 0;
        this.cachedWidth = void 0;
        this.cachedLines = void 0;
      }
      invalidate() {
        this.cachedText = void 0;
        this.cachedWidth = void 0;
        this.cachedLines = void 0;
      }
      render(width) {
        if (this.cachedLines && this.cachedText === this.text && this.cachedWidth === width) {
          return this.cachedLines;
        }
        if (!this.text || this.text.trim() === "") {
          const result2 = [];
          this.cachedText = this.text;
          this.cachedWidth = width;
          this.cachedLines = result2;
          return result2;
        }
        const normalizedText = this.text.replace(/\t/g, "   ");
        const contentWidth = Math.max(1, width - this.paddingX * 2);
        const wrappedLines = wrapTextWithAnsi(normalizedText, contentWidth);
        const leftMargin = " ".repeat(this.paddingX);
        const rightMargin = " ".repeat(this.paddingX);
        const contentLines = [];
        for (const line of wrappedLines) {
          const lineWithMargins = leftMargin + line + rightMargin;
          if (this.customBgFn) {
            contentLines.push(applyBackgroundToLine(lineWithMargins, width, this.customBgFn));
          } else {
            const visibleLen = visibleWidth(lineWithMargins);
            const paddingNeeded = Math.max(0, width - visibleLen);
            contentLines.push(lineWithMargins + " ".repeat(paddingNeeded));
          }
        }
        const emptyLine = " ".repeat(width);
        const emptyLines = [];
        for (let i = 0; i < this.paddingY; i++) {
          const line = this.customBgFn ? applyBackgroundToLine(emptyLine, width, this.customBgFn) : emptyLine;
          emptyLines.push(line);
        }
        const result = [...emptyLines, ...contentLines, ...emptyLines];
        this.cachedText = this.text;
        this.cachedWidth = width;
        this.cachedLines = result;
        return result.length > 0 ? result : [""];
      }
    };
  }
});

// node_modules/@mariozechner/pi-tui/dist/components/loader.js
var init_loader = __esm({
  "node_modules/@mariozechner/pi-tui/dist/components/loader.js"() {
    init_text();
  }
});

// node_modules/@mariozechner/pi-tui/dist/components/cancellable-loader.js
var init_cancellable_loader = __esm({
  "node_modules/@mariozechner/pi-tui/dist/components/cancellable-loader.js"() {
    init_keybindings();
    init_loader();
  }
});

// node_modules/@mariozechner/pi-tui/dist/kill-ring.js
var init_kill_ring = __esm({
  "node_modules/@mariozechner/pi-tui/dist/kill-ring.js"() {
  }
});

// node_modules/@mariozechner/pi-tui/dist/terminal-image.js
var init_terminal_image = __esm({
  "node_modules/@mariozechner/pi-tui/dist/terminal-image.js"() {
  }
});

// node_modules/@mariozechner/pi-tui/dist/tui.js
var init_tui = __esm({
  "node_modules/@mariozechner/pi-tui/dist/tui.js"() {
    init_keys2();
    init_terminal_image();
    init_utils();
  }
});

// node_modules/@mariozechner/pi-tui/dist/undo-stack.js
var init_undo_stack = __esm({
  "node_modules/@mariozechner/pi-tui/dist/undo-stack.js"() {
  }
});

// node_modules/@mariozechner/pi-tui/dist/components/select-list.js
var init_select_list = __esm({
  "node_modules/@mariozechner/pi-tui/dist/components/select-list.js"() {
    init_keybindings();
    init_utils();
  }
});

// node_modules/@mariozechner/pi-tui/dist/components/editor.js
var baseSegmenter;
var init_editor = __esm({
  "node_modules/@mariozechner/pi-tui/dist/components/editor.js"() {
    init_keybindings();
    init_keys2();
    init_kill_ring();
    init_tui();
    init_undo_stack();
    init_utils();
    init_select_list();
    baseSegmenter = getSegmenter();
  }
});

// node_modules/@mariozechner/pi-tui/dist/components/image.js
var init_image = __esm({
  "node_modules/@mariozechner/pi-tui/dist/components/image.js"() {
    init_terminal_image();
  }
});

// node_modules/@mariozechner/pi-tui/dist/components/input.js
var segmenter2;
var init_input = __esm({
  "node_modules/@mariozechner/pi-tui/dist/components/input.js"() {
    init_keybindings();
    init_keys2();
    init_kill_ring();
    init_tui();
    init_undo_stack();
    init_utils();
    segmenter2 = getSegmenter();
  }
});

// node_modules/marked/lib/marked.esm.js
function _getDefaults() {
  return {
    async: false,
    breaks: false,
    extensions: null,
    gfm: true,
    hooks: null,
    pedantic: false,
    renderer: null,
    silent: false,
    tokenizer: null,
    walkTokens: null
  };
}
function changeDefaults(newDefaults) {
  _defaults = newDefaults;
}
function edit(regex, opt = "") {
  let source = typeof regex === "string" ? regex : regex.source;
  const obj = {
    replace: (name, val) => {
      let valSource = typeof val === "string" ? val : val.source;
      valSource = valSource.replace(other.caret, "$1");
      source = source.replace(name, valSource);
      return obj;
    },
    getRegex: () => {
      return new RegExp(source, opt);
    }
  };
  return obj;
}
function escape2(html2, encode) {
  if (encode) {
    if (other.escapeTest.test(html2)) {
      return html2.replace(other.escapeReplace, getEscapeReplacement);
    }
  } else {
    if (other.escapeTestNoEncode.test(html2)) {
      return html2.replace(other.escapeReplaceNoEncode, getEscapeReplacement);
    }
  }
  return html2;
}
function cleanUrl(href) {
  try {
    href = encodeURI(href).replace(other.percentDecode, "%");
  } catch {
    return null;
  }
  return href;
}
function splitCells(tableRow, count) {
  const row = tableRow.replace(other.findPipe, (match, offset, str) => {
    let escaped = false;
    let curr = offset;
    while (--curr >= 0 && str[curr] === "\\") escaped = !escaped;
    if (escaped) {
      return "|";
    } else {
      return " |";
    }
  }), cells = row.split(other.splitPipe);
  let i = 0;
  if (!cells[0].trim()) {
    cells.shift();
  }
  if (cells.length > 0 && !cells.at(-1)?.trim()) {
    cells.pop();
  }
  if (count) {
    if (cells.length > count) {
      cells.splice(count);
    } else {
      while (cells.length < count) cells.push("");
    }
  }
  for (; i < cells.length; i++) {
    cells[i] = cells[i].trim().replace(other.slashPipe, "|");
  }
  return cells;
}
function rtrim(str, c, invert) {
  const l = str.length;
  if (l === 0) {
    return "";
  }
  let suffLen = 0;
  while (suffLen < l) {
    const currChar = str.charAt(l - suffLen - 1);
    if (currChar === c && !invert) {
      suffLen++;
    } else if (currChar !== c && invert) {
      suffLen++;
    } else {
      break;
    }
  }
  return str.slice(0, l - suffLen);
}
function findClosingBracket(str, b) {
  if (str.indexOf(b[1]) === -1) {
    return -1;
  }
  let level = 0;
  for (let i = 0; i < str.length; i++) {
    if (str[i] === "\\") {
      i++;
    } else if (str[i] === b[0]) {
      level++;
    } else if (str[i] === b[1]) {
      level--;
      if (level < 0) {
        return i;
      }
    }
  }
  if (level > 0) {
    return -2;
  }
  return -1;
}
function outputLink(cap, link2, raw, lexer2, rules) {
  const href = link2.href;
  const title = link2.title || null;
  const text = cap[1].replace(rules.other.outputLinkReplace, "$1");
  lexer2.state.inLink = true;
  const token = {
    type: cap[0].charAt(0) === "!" ? "image" : "link",
    raw,
    href,
    title,
    text,
    tokens: lexer2.inlineTokens(text)
  };
  lexer2.state.inLink = false;
  return token;
}
function indentCodeCompensation(raw, text, rules) {
  const matchIndentToCode = raw.match(rules.other.indentCodeCompensation);
  if (matchIndentToCode === null) {
    return text;
  }
  const indentToCode = matchIndentToCode[1];
  return text.split("\n").map((node) => {
    const matchIndentInNode = node.match(rules.other.beginningSpace);
    if (matchIndentInNode === null) {
      return node;
    }
    const [indentInNode] = matchIndentInNode;
    if (indentInNode.length >= indentToCode.length) {
      return node.slice(indentToCode.length);
    }
    return node;
  }).join("\n");
}
function marked(src, opt) {
  return markedInstance.parse(src, opt);
}
var _defaults, noopTest, other, newline, blockCode, fences, hr, heading, bullet, lheadingCore, lheading, lheadingGfm, _paragraph, blockText, _blockLabel, def, list, _tag, _comment, html, paragraph, blockquote, blockNormal, gfmTable, blockGfm, blockPedantic, escape, inlineCode, br, inlineText, _punctuation, _punctuationOrSpace, _notPunctuationOrSpace, punctuation, _punctuationGfmStrongEm, _punctuationOrSpaceGfmStrongEm, _notPunctuationOrSpaceGfmStrongEm, blockSkip, emStrongLDelimCore, emStrongLDelim, emStrongLDelimGfm, emStrongRDelimAstCore, emStrongRDelimAst, emStrongRDelimAstGfm, emStrongRDelimUnd, anyPunctuation, autolink, _inlineComment, tag, _inlineLabel, link, reflink, nolink, reflinkSearch, inlineNormal, inlinePedantic, inlineGfm, inlineBreaks, block, inline, escapeReplacements, getEscapeReplacement, _Tokenizer, _Lexer, _Renderer, _TextRenderer, _Parser, _Hooks, Marked, markedInstance, options, setOptions, use, walkTokens, parseInline, parser, lexer;
var init_marked_esm = __esm({
  "node_modules/marked/lib/marked.esm.js"() {
    _defaults = _getDefaults();
    noopTest = { exec: () => null };
    other = {
      codeRemoveIndent: /^(?: {1,4}| {0,3}\t)/gm,
      outputLinkReplace: /\\([\[\]])/g,
      indentCodeCompensation: /^(\s+)(?:```)/,
      beginningSpace: /^\s+/,
      endingHash: /#$/,
      startingSpaceChar: /^ /,
      endingSpaceChar: / $/,
      nonSpaceChar: /[^ ]/,
      newLineCharGlobal: /\n/g,
      tabCharGlobal: /\t/g,
      multipleSpaceGlobal: /\s+/g,
      blankLine: /^[ \t]*$/,
      doubleBlankLine: /\n[ \t]*\n[ \t]*$/,
      blockquoteStart: /^ {0,3}>/,
      blockquoteSetextReplace: /\n {0,3}((?:=+|-+) *)(?=\n|$)/g,
      blockquoteSetextReplace2: /^ {0,3}>[ \t]?/gm,
      listReplaceTabs: /^\t+/,
      listReplaceNesting: /^ {1,4}(?=( {4})*[^ ])/g,
      listIsTask: /^\[[ xX]\] /,
      listReplaceTask: /^\[[ xX]\] +/,
      anyLine: /\n.*\n/,
      hrefBrackets: /^<(.*)>$/,
      tableDelimiter: /[:|]/,
      tableAlignChars: /^\||\| *$/g,
      tableRowBlankLine: /\n[ \t]*$/,
      tableAlignRight: /^ *-+: *$/,
      tableAlignCenter: /^ *:-+: *$/,
      tableAlignLeft: /^ *:-+ *$/,
      startATag: /^<a /i,
      endATag: /^<\/a>/i,
      startPreScriptTag: /^<(pre|code|kbd|script)(\s|>)/i,
      endPreScriptTag: /^<\/(pre|code|kbd|script)(\s|>)/i,
      startAngleBracket: /^</,
      endAngleBracket: />$/,
      pedanticHrefTitle: /^([^'"]*[^\s])\s+(['"])(.*)\2/,
      unicodeAlphaNumeric: /[\p{L}\p{N}]/u,
      escapeTest: /[&<>"']/,
      escapeReplace: /[&<>"']/g,
      escapeTestNoEncode: /[<>"']|&(?!(#\d{1,7}|#[Xx][a-fA-F0-9]{1,6}|\w+);)/,
      escapeReplaceNoEncode: /[<>"']|&(?!(#\d{1,7}|#[Xx][a-fA-F0-9]{1,6}|\w+);)/g,
      unescapeTest: /&(#(?:\d+)|(?:#x[0-9A-Fa-f]+)|(?:\w+));?/ig,
      caret: /(^|[^\[])\^/g,
      percentDecode: /%25/g,
      findPipe: /\|/g,
      splitPipe: / \|/,
      slashPipe: /\\\|/g,
      carriageReturn: /\r\n|\r/g,
      spaceLine: /^ +$/gm,
      notSpaceStart: /^\S*/,
      endingNewline: /\n$/,
      listItemRegex: (bull) => new RegExp(`^( {0,3}${bull})((?:[	 ][^\\n]*)?(?:\\n|$))`),
      nextBulletRegex: (indent) => new RegExp(`^ {0,${Math.min(3, indent - 1)}}(?:[*+-]|\\d{1,9}[.)])((?:[ 	][^\\n]*)?(?:\\n|$))`),
      hrRegex: (indent) => new RegExp(`^ {0,${Math.min(3, indent - 1)}}((?:- *){3,}|(?:_ *){3,}|(?:\\* *){3,})(?:\\n+|$)`),
      fencesBeginRegex: (indent) => new RegExp(`^ {0,${Math.min(3, indent - 1)}}(?:\`\`\`|~~~)`),
      headingBeginRegex: (indent) => new RegExp(`^ {0,${Math.min(3, indent - 1)}}#`),
      htmlBeginRegex: (indent) => new RegExp(`^ {0,${Math.min(3, indent - 1)}}<(?:[a-z].*>|!--)`, "i")
    };
    newline = /^(?:[ \t]*(?:\n|$))+/;
    blockCode = /^((?: {4}| {0,3}\t)[^\n]+(?:\n(?:[ \t]*(?:\n|$))*)?)+/;
    fences = /^ {0,3}(`{3,}(?=[^`\n]*(?:\n|$))|~{3,})([^\n]*)(?:\n|$)(?:|([\s\S]*?)(?:\n|$))(?: {0,3}\1[~`]* *(?=\n|$)|$)/;
    hr = /^ {0,3}((?:-[\t ]*){3,}|(?:_[ \t]*){3,}|(?:\*[ \t]*){3,})(?:\n+|$)/;
    heading = /^ {0,3}(#{1,6})(?=\s|$)(.*)(?:\n+|$)/;
    bullet = /(?:[*+-]|\d{1,9}[.)])/;
    lheadingCore = /^(?!bull |blockCode|fences|blockquote|heading|html|table)((?:.|\n(?!\s*?\n|bull |blockCode|fences|blockquote|heading|html|table))+?)\n {0,3}(=+|-+) *(?:\n+|$)/;
    lheading = edit(lheadingCore).replace(/bull/g, bullet).replace(/blockCode/g, /(?: {4}| {0,3}\t)/).replace(/fences/g, / {0,3}(?:`{3,}|~{3,})/).replace(/blockquote/g, / {0,3}>/).replace(/heading/g, / {0,3}#{1,6}/).replace(/html/g, / {0,3}<[^\n>]+>\n/).replace(/\|table/g, "").getRegex();
    lheadingGfm = edit(lheadingCore).replace(/bull/g, bullet).replace(/blockCode/g, /(?: {4}| {0,3}\t)/).replace(/fences/g, / {0,3}(?:`{3,}|~{3,})/).replace(/blockquote/g, / {0,3}>/).replace(/heading/g, / {0,3}#{1,6}/).replace(/html/g, / {0,3}<[^\n>]+>\n/).replace(/table/g, / {0,3}\|?(?:[:\- ]*\|)+[\:\- ]*\n/).getRegex();
    _paragraph = /^([^\n]+(?:\n(?!hr|heading|lheading|blockquote|fences|list|html|table| +\n)[^\n]+)*)/;
    blockText = /^[^\n]+/;
    _blockLabel = /(?!\s*\])(?:\\.|[^\[\]\\])+/;
    def = edit(/^ {0,3}\[(label)\]: *(?:\n[ \t]*)?([^<\s][^\s]*|<.*?>)(?:(?: +(?:\n[ \t]*)?| *\n[ \t]*)(title))? *(?:\n+|$)/).replace("label", _blockLabel).replace("title", /(?:"(?:\\"?|[^"\\])*"|'[^'\n]*(?:\n[^'\n]+)*\n?'|\([^()]*\))/).getRegex();
    list = edit(/^( {0,3}bull)([ \t][^\n]+?)?(?:\n|$)/).replace(/bull/g, bullet).getRegex();
    _tag = "address|article|aside|base|basefont|blockquote|body|caption|center|col|colgroup|dd|details|dialog|dir|div|dl|dt|fieldset|figcaption|figure|footer|form|frame|frameset|h[1-6]|head|header|hr|html|iframe|legend|li|link|main|menu|menuitem|meta|nav|noframes|ol|optgroup|option|p|param|search|section|summary|table|tbody|td|tfoot|th|thead|title|tr|track|ul";
    _comment = /<!--(?:-?>|[\s\S]*?(?:-->|$))/;
    html = edit(
      "^ {0,3}(?:<(script|pre|style|textarea)[\\s>][\\s\\S]*?(?:</\\1>[^\\n]*\\n+|$)|comment[^\\n]*(\\n+|$)|<\\?[\\s\\S]*?(?:\\?>\\n*|$)|<![A-Z][\\s\\S]*?(?:>\\n*|$)|<!\\[CDATA\\[[\\s\\S]*?(?:\\]\\]>\\n*|$)|</?(tag)(?: +|\\n|/?>)[\\s\\S]*?(?:(?:\\n[ 	]*)+\\n|$)|<(?!script|pre|style|textarea)([a-z][\\w-]*)(?:attribute)*? */?>(?=[ \\t]*(?:\\n|$))[\\s\\S]*?(?:(?:\\n[ 	]*)+\\n|$)|</(?!script|pre|style|textarea)[a-z][\\w-]*\\s*>(?=[ \\t]*(?:\\n|$))[\\s\\S]*?(?:(?:\\n[ 	]*)+\\n|$))",
      "i"
    ).replace("comment", _comment).replace("tag", _tag).replace("attribute", / +[a-zA-Z:_][\w.:-]*(?: *= *"[^"\n]*"| *= *'[^'\n]*'| *= *[^\s"'=<>`]+)?/).getRegex();
    paragraph = edit(_paragraph).replace("hr", hr).replace("heading", " {0,3}#{1,6}(?:\\s|$)").replace("|lheading", "").replace("|table", "").replace("blockquote", " {0,3}>").replace("fences", " {0,3}(?:`{3,}(?=[^`\\n]*\\n)|~{3,})[^\\n]*\\n").replace("list", " {0,3}(?:[*+-]|1[.)]) ").replace("html", "</?(?:tag)(?: +|\\n|/?>)|<(?:script|pre|style|textarea|!--)").replace("tag", _tag).getRegex();
    blockquote = edit(/^( {0,3}> ?(paragraph|[^\n]*)(?:\n|$))+/).replace("paragraph", paragraph).getRegex();
    blockNormal = {
      blockquote,
      code: blockCode,
      def,
      fences,
      heading,
      hr,
      html,
      lheading,
      list,
      newline,
      paragraph,
      table: noopTest,
      text: blockText
    };
    gfmTable = edit(
      "^ *([^\\n ].*)\\n {0,3}((?:\\| *)?:?-+:? *(?:\\| *:?-+:? *)*(?:\\| *)?)(?:\\n((?:(?! *\\n|hr|heading|blockquote|code|fences|list|html).*(?:\\n|$))*)\\n*|$)"
    ).replace("hr", hr).replace("heading", " {0,3}#{1,6}(?:\\s|$)").replace("blockquote", " {0,3}>").replace("code", "(?: {4}| {0,3}	)[^\\n]").replace("fences", " {0,3}(?:`{3,}(?=[^`\\n]*\\n)|~{3,})[^\\n]*\\n").replace("list", " {0,3}(?:[*+-]|1[.)]) ").replace("html", "</?(?:tag)(?: +|\\n|/?>)|<(?:script|pre|style|textarea|!--)").replace("tag", _tag).getRegex();
    blockGfm = {
      ...blockNormal,
      lheading: lheadingGfm,
      table: gfmTable,
      paragraph: edit(_paragraph).replace("hr", hr).replace("heading", " {0,3}#{1,6}(?:\\s|$)").replace("|lheading", "").replace("table", gfmTable).replace("blockquote", " {0,3}>").replace("fences", " {0,3}(?:`{3,}(?=[^`\\n]*\\n)|~{3,})[^\\n]*\\n").replace("list", " {0,3}(?:[*+-]|1[.)]) ").replace("html", "</?(?:tag)(?: +|\\n|/?>)|<(?:script|pre|style|textarea|!--)").replace("tag", _tag).getRegex()
    };
    blockPedantic = {
      ...blockNormal,
      html: edit(
        `^ *(?:comment *(?:\\n|\\s*$)|<(tag)[\\s\\S]+?</\\1> *(?:\\n{2,}|\\s*$)|<tag(?:"[^"]*"|'[^']*'|\\s[^'"/>\\s]*)*?/?> *(?:\\n{2,}|\\s*$))`
      ).replace("comment", _comment).replace(/tag/g, "(?!(?:a|em|strong|small|s|cite|q|dfn|abbr|data|time|code|var|samp|kbd|sub|sup|i|b|u|mark|ruby|rt|rp|bdi|bdo|span|br|wbr|ins|del|img)\\b)\\w+(?!:|[^\\w\\s@]*@)\\b").getRegex(),
      def: /^ *\[([^\]]+)\]: *<?([^\s>]+)>?(?: +(["(][^\n]+[")]))? *(?:\n+|$)/,
      heading: /^(#{1,6})(.*)(?:\n+|$)/,
      fences: noopTest,
      // fences not supported
      lheading: /^(.+?)\n {0,3}(=+|-+) *(?:\n+|$)/,
      paragraph: edit(_paragraph).replace("hr", hr).replace("heading", " *#{1,6} *[^\n]").replace("lheading", lheading).replace("|table", "").replace("blockquote", " {0,3}>").replace("|fences", "").replace("|list", "").replace("|html", "").replace("|tag", "").getRegex()
    };
    escape = /^\\([!"#$%&'()*+,\-./:;<=>?@\[\]\\^_`{|}~])/;
    inlineCode = /^(`+)([^`]|[^`][\s\S]*?[^`])\1(?!`)/;
    br = /^( {2,}|\\)\n(?!\s*$)/;
    inlineText = /^(`+|[^`])(?:(?= {2,}\n)|[\s\S]*?(?:(?=[\\<!\[`*_]|\b_|$)|[^ ](?= {2,}\n)))/;
    _punctuation = /[\p{P}\p{S}]/u;
    _punctuationOrSpace = /[\s\p{P}\p{S}]/u;
    _notPunctuationOrSpace = /[^\s\p{P}\p{S}]/u;
    punctuation = edit(/^((?![*_])punctSpace)/, "u").replace(/punctSpace/g, _punctuationOrSpace).getRegex();
    _punctuationGfmStrongEm = /(?!~)[\p{P}\p{S}]/u;
    _punctuationOrSpaceGfmStrongEm = /(?!~)[\s\p{P}\p{S}]/u;
    _notPunctuationOrSpaceGfmStrongEm = /(?:[^\s\p{P}\p{S}]|~)/u;
    blockSkip = /\[[^[\]]*?\]\((?:\\.|[^\\\(\)]|\((?:\\.|[^\\\(\)])*\))*\)|`[^`]*?`|<[^<>]*?>/g;
    emStrongLDelimCore = /^(?:\*+(?:((?!\*)punct)|[^\s*]))|^_+(?:((?!_)punct)|([^\s_]))/;
    emStrongLDelim = edit(emStrongLDelimCore, "u").replace(/punct/g, _punctuation).getRegex();
    emStrongLDelimGfm = edit(emStrongLDelimCore, "u").replace(/punct/g, _punctuationGfmStrongEm).getRegex();
    emStrongRDelimAstCore = "^[^_*]*?__[^_*]*?\\*[^_*]*?(?=__)|[^*]+(?=[^*])|(?!\\*)punct(\\*+)(?=[\\s]|$)|notPunctSpace(\\*+)(?!\\*)(?=punctSpace|$)|(?!\\*)punctSpace(\\*+)(?=notPunctSpace)|[\\s](\\*+)(?!\\*)(?=punct)|(?!\\*)punct(\\*+)(?!\\*)(?=punct)|notPunctSpace(\\*+)(?=notPunctSpace)";
    emStrongRDelimAst = edit(emStrongRDelimAstCore, "gu").replace(/notPunctSpace/g, _notPunctuationOrSpace).replace(/punctSpace/g, _punctuationOrSpace).replace(/punct/g, _punctuation).getRegex();
    emStrongRDelimAstGfm = edit(emStrongRDelimAstCore, "gu").replace(/notPunctSpace/g, _notPunctuationOrSpaceGfmStrongEm).replace(/punctSpace/g, _punctuationOrSpaceGfmStrongEm).replace(/punct/g, _punctuationGfmStrongEm).getRegex();
    emStrongRDelimUnd = edit(
      "^[^_*]*?\\*\\*[^_*]*?_[^_*]*?(?=\\*\\*)|[^_]+(?=[^_])|(?!_)punct(_+)(?=[\\s]|$)|notPunctSpace(_+)(?!_)(?=punctSpace|$)|(?!_)punctSpace(_+)(?=notPunctSpace)|[\\s](_+)(?!_)(?=punct)|(?!_)punct(_+)(?!_)(?=punct)",
      "gu"
    ).replace(/notPunctSpace/g, _notPunctuationOrSpace).replace(/punctSpace/g, _punctuationOrSpace).replace(/punct/g, _punctuation).getRegex();
    anyPunctuation = edit(/\\(punct)/, "gu").replace(/punct/g, _punctuation).getRegex();
    autolink = edit(/^<(scheme:[^\s\x00-\x1f<>]*|email)>/).replace("scheme", /[a-zA-Z][a-zA-Z0-9+.-]{1,31}/).replace("email", /[a-zA-Z0-9.!#$%&'*+/=?^_`{|}~-]+(@)[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?(?:\.[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?)+(?![-_])/).getRegex();
    _inlineComment = edit(_comment).replace("(?:-->|$)", "-->").getRegex();
    tag = edit(
      "^comment|^</[a-zA-Z][\\w:-]*\\s*>|^<[a-zA-Z][\\w-]*(?:attribute)*?\\s*/?>|^<\\?[\\s\\S]*?\\?>|^<![a-zA-Z]+\\s[\\s\\S]*?>|^<!\\[CDATA\\[[\\s\\S]*?\\]\\]>"
    ).replace("comment", _inlineComment).replace("attribute", /\s+[a-zA-Z:_][\w.:-]*(?:\s*=\s*"[^"]*"|\s*=\s*'[^']*'|\s*=\s*[^\s"'=<>`]+)?/).getRegex();
    _inlineLabel = /(?:\[(?:\\.|[^\[\]\\])*\]|\\.|`[^`]*`|[^\[\]\\`])*?/;
    link = edit(/^!?\[(label)\]\(\s*(href)(?:(?:[ \t]*(?:\n[ \t]*)?)(title))?\s*\)/).replace("label", _inlineLabel).replace("href", /<(?:\\.|[^\n<>\\])+>|[^ \t\n\x00-\x1f]*/).replace("title", /"(?:\\"?|[^"\\])*"|'(?:\\'?|[^'\\])*'|\((?:\\\)?|[^)\\])*\)/).getRegex();
    reflink = edit(/^!?\[(label)\]\[(ref)\]/).replace("label", _inlineLabel).replace("ref", _blockLabel).getRegex();
    nolink = edit(/^!?\[(ref)\](?:\[\])?/).replace("ref", _blockLabel).getRegex();
    reflinkSearch = edit("reflink|nolink(?!\\()", "g").replace("reflink", reflink).replace("nolink", nolink).getRegex();
    inlineNormal = {
      _backpedal: noopTest,
      // only used for GFM url
      anyPunctuation,
      autolink,
      blockSkip,
      br,
      code: inlineCode,
      del: noopTest,
      emStrongLDelim,
      emStrongRDelimAst,
      emStrongRDelimUnd,
      escape,
      link,
      nolink,
      punctuation,
      reflink,
      reflinkSearch,
      tag,
      text: inlineText,
      url: noopTest
    };
    inlinePedantic = {
      ...inlineNormal,
      link: edit(/^!?\[(label)\]\((.*?)\)/).replace("label", _inlineLabel).getRegex(),
      reflink: edit(/^!?\[(label)\]\s*\[([^\]]*)\]/).replace("label", _inlineLabel).getRegex()
    };
    inlineGfm = {
      ...inlineNormal,
      emStrongRDelimAst: emStrongRDelimAstGfm,
      emStrongLDelim: emStrongLDelimGfm,
      url: edit(/^((?:ftp|https?):\/\/|www\.)(?:[a-zA-Z0-9\-]+\.?)+[^\s<]*|^email/, "i").replace("email", /[A-Za-z0-9._+-]+(@)[a-zA-Z0-9-_]+(?:\.[a-zA-Z0-9-_]*[a-zA-Z0-9])+(?![-_])/).getRegex(),
      _backpedal: /(?:[^?!.,:;*_'"~()&]+|\([^)]*\)|&(?![a-zA-Z0-9]+;$)|[?!.,:;*_'"~)]+(?!$))+/,
      del: /^(~~?)(?=[^\s~])((?:\\.|[^\\])*?(?:\\.|[^\s~\\]))\1(?=[^~]|$)/,
      text: /^([`~]+|[^`~])(?:(?= {2,}\n)|(?=[a-zA-Z0-9.!#$%&'*+\/=?_`{\|}~-]+@)|[\s\S]*?(?:(?=[\\<!\[`*~_]|\b_|https?:\/\/|ftp:\/\/|www\.|$)|[^ ](?= {2,}\n)|[^a-zA-Z0-9.!#$%&'*+\/=?_`{\|}~-](?=[a-zA-Z0-9.!#$%&'*+\/=?_`{\|}~-]+@)))/
    };
    inlineBreaks = {
      ...inlineGfm,
      br: edit(br).replace("{2,}", "*").getRegex(),
      text: edit(inlineGfm.text).replace("\\b_", "\\b_| {2,}\\n").replace(/\{2,\}/g, "*").getRegex()
    };
    block = {
      normal: blockNormal,
      gfm: blockGfm,
      pedantic: blockPedantic
    };
    inline = {
      normal: inlineNormal,
      gfm: inlineGfm,
      breaks: inlineBreaks,
      pedantic: inlinePedantic
    };
    escapeReplacements = {
      "&": "&amp;",
      "<": "&lt;",
      ">": "&gt;",
      '"': "&quot;",
      "'": "&#39;"
    };
    getEscapeReplacement = (ch) => escapeReplacements[ch];
    _Tokenizer = class {
      options;
      rules;
      // set by the lexer
      lexer;
      // set by the lexer
      constructor(options2) {
        this.options = options2 || _defaults;
      }
      space(src) {
        const cap = this.rules.block.newline.exec(src);
        if (cap && cap[0].length > 0) {
          return {
            type: "space",
            raw: cap[0]
          };
        }
      }
      code(src) {
        const cap = this.rules.block.code.exec(src);
        if (cap) {
          const text = cap[0].replace(this.rules.other.codeRemoveIndent, "");
          return {
            type: "code",
            raw: cap[0],
            codeBlockStyle: "indented",
            text: !this.options.pedantic ? rtrim(text, "\n") : text
          };
        }
      }
      fences(src) {
        const cap = this.rules.block.fences.exec(src);
        if (cap) {
          const raw = cap[0];
          const text = indentCodeCompensation(raw, cap[3] || "", this.rules);
          return {
            type: "code",
            raw,
            lang: cap[2] ? cap[2].trim().replace(this.rules.inline.anyPunctuation, "$1") : cap[2],
            text
          };
        }
      }
      heading(src) {
        const cap = this.rules.block.heading.exec(src);
        if (cap) {
          let text = cap[2].trim();
          if (this.rules.other.endingHash.test(text)) {
            const trimmed = rtrim(text, "#");
            if (this.options.pedantic) {
              text = trimmed.trim();
            } else if (!trimmed || this.rules.other.endingSpaceChar.test(trimmed)) {
              text = trimmed.trim();
            }
          }
          return {
            type: "heading",
            raw: cap[0],
            depth: cap[1].length,
            text,
            tokens: this.lexer.inline(text)
          };
        }
      }
      hr(src) {
        const cap = this.rules.block.hr.exec(src);
        if (cap) {
          return {
            type: "hr",
            raw: rtrim(cap[0], "\n")
          };
        }
      }
      blockquote(src) {
        const cap = this.rules.block.blockquote.exec(src);
        if (cap) {
          let lines = rtrim(cap[0], "\n").split("\n");
          let raw = "";
          let text = "";
          const tokens = [];
          while (lines.length > 0) {
            let inBlockquote = false;
            const currentLines = [];
            let i;
            for (i = 0; i < lines.length; i++) {
              if (this.rules.other.blockquoteStart.test(lines[i])) {
                currentLines.push(lines[i]);
                inBlockquote = true;
              } else if (!inBlockquote) {
                currentLines.push(lines[i]);
              } else {
                break;
              }
            }
            lines = lines.slice(i);
            const currentRaw = currentLines.join("\n");
            const currentText = currentRaw.replace(this.rules.other.blockquoteSetextReplace, "\n    $1").replace(this.rules.other.blockquoteSetextReplace2, "");
            raw = raw ? `${raw}
${currentRaw}` : currentRaw;
            text = text ? `${text}
${currentText}` : currentText;
            const top = this.lexer.state.top;
            this.lexer.state.top = true;
            this.lexer.blockTokens(currentText, tokens, true);
            this.lexer.state.top = top;
            if (lines.length === 0) {
              break;
            }
            const lastToken = tokens.at(-1);
            if (lastToken?.type === "code") {
              break;
            } else if (lastToken?.type === "blockquote") {
              const oldToken = lastToken;
              const newText = oldToken.raw + "\n" + lines.join("\n");
              const newToken = this.blockquote(newText);
              tokens[tokens.length - 1] = newToken;
              raw = raw.substring(0, raw.length - oldToken.raw.length) + newToken.raw;
              text = text.substring(0, text.length - oldToken.text.length) + newToken.text;
              break;
            } else if (lastToken?.type === "list") {
              const oldToken = lastToken;
              const newText = oldToken.raw + "\n" + lines.join("\n");
              const newToken = this.list(newText);
              tokens[tokens.length - 1] = newToken;
              raw = raw.substring(0, raw.length - lastToken.raw.length) + newToken.raw;
              text = text.substring(0, text.length - oldToken.raw.length) + newToken.raw;
              lines = newText.substring(tokens.at(-1).raw.length).split("\n");
              continue;
            }
          }
          return {
            type: "blockquote",
            raw,
            tokens,
            text
          };
        }
      }
      list(src) {
        let cap = this.rules.block.list.exec(src);
        if (cap) {
          let bull = cap[1].trim();
          const isordered = bull.length > 1;
          const list2 = {
            type: "list",
            raw: "",
            ordered: isordered,
            start: isordered ? +bull.slice(0, -1) : "",
            loose: false,
            items: []
          };
          bull = isordered ? `\\d{1,9}\\${bull.slice(-1)}` : `\\${bull}`;
          if (this.options.pedantic) {
            bull = isordered ? bull : "[*+-]";
          }
          const itemRegex = this.rules.other.listItemRegex(bull);
          let endsWithBlankLine = false;
          while (src) {
            let endEarly = false;
            let raw = "";
            let itemContents = "";
            if (!(cap = itemRegex.exec(src))) {
              break;
            }
            if (this.rules.block.hr.test(src)) {
              break;
            }
            raw = cap[0];
            src = src.substring(raw.length);
            let line = cap[2].split("\n", 1)[0].replace(this.rules.other.listReplaceTabs, (t) => " ".repeat(3 * t.length));
            let nextLine = src.split("\n", 1)[0];
            let blankLine = !line.trim();
            let indent = 0;
            if (this.options.pedantic) {
              indent = 2;
              itemContents = line.trimStart();
            } else if (blankLine) {
              indent = cap[1].length + 1;
            } else {
              indent = cap[2].search(this.rules.other.nonSpaceChar);
              indent = indent > 4 ? 1 : indent;
              itemContents = line.slice(indent);
              indent += cap[1].length;
            }
            if (blankLine && this.rules.other.blankLine.test(nextLine)) {
              raw += nextLine + "\n";
              src = src.substring(nextLine.length + 1);
              endEarly = true;
            }
            if (!endEarly) {
              const nextBulletRegex = this.rules.other.nextBulletRegex(indent);
              const hrRegex = this.rules.other.hrRegex(indent);
              const fencesBeginRegex = this.rules.other.fencesBeginRegex(indent);
              const headingBeginRegex = this.rules.other.headingBeginRegex(indent);
              const htmlBeginRegex = this.rules.other.htmlBeginRegex(indent);
              while (src) {
                const rawLine = src.split("\n", 1)[0];
                let nextLineWithoutTabs;
                nextLine = rawLine;
                if (this.options.pedantic) {
                  nextLine = nextLine.replace(this.rules.other.listReplaceNesting, "  ");
                  nextLineWithoutTabs = nextLine;
                } else {
                  nextLineWithoutTabs = nextLine.replace(this.rules.other.tabCharGlobal, "    ");
                }
                if (fencesBeginRegex.test(nextLine)) {
                  break;
                }
                if (headingBeginRegex.test(nextLine)) {
                  break;
                }
                if (htmlBeginRegex.test(nextLine)) {
                  break;
                }
                if (nextBulletRegex.test(nextLine)) {
                  break;
                }
                if (hrRegex.test(nextLine)) {
                  break;
                }
                if (nextLineWithoutTabs.search(this.rules.other.nonSpaceChar) >= indent || !nextLine.trim()) {
                  itemContents += "\n" + nextLineWithoutTabs.slice(indent);
                } else {
                  if (blankLine) {
                    break;
                  }
                  if (line.replace(this.rules.other.tabCharGlobal, "    ").search(this.rules.other.nonSpaceChar) >= 4) {
                    break;
                  }
                  if (fencesBeginRegex.test(line)) {
                    break;
                  }
                  if (headingBeginRegex.test(line)) {
                    break;
                  }
                  if (hrRegex.test(line)) {
                    break;
                  }
                  itemContents += "\n" + nextLine;
                }
                if (!blankLine && !nextLine.trim()) {
                  blankLine = true;
                }
                raw += rawLine + "\n";
                src = src.substring(rawLine.length + 1);
                line = nextLineWithoutTabs.slice(indent);
              }
            }
            if (!list2.loose) {
              if (endsWithBlankLine) {
                list2.loose = true;
              } else if (this.rules.other.doubleBlankLine.test(raw)) {
                endsWithBlankLine = true;
              }
            }
            let istask = null;
            let ischecked;
            if (this.options.gfm) {
              istask = this.rules.other.listIsTask.exec(itemContents);
              if (istask) {
                ischecked = istask[0] !== "[ ] ";
                itemContents = itemContents.replace(this.rules.other.listReplaceTask, "");
              }
            }
            list2.items.push({
              type: "list_item",
              raw,
              task: !!istask,
              checked: ischecked,
              loose: false,
              text: itemContents,
              tokens: []
            });
            list2.raw += raw;
          }
          const lastItem = list2.items.at(-1);
          if (lastItem) {
            lastItem.raw = lastItem.raw.trimEnd();
            lastItem.text = lastItem.text.trimEnd();
          } else {
            return;
          }
          list2.raw = list2.raw.trimEnd();
          for (let i = 0; i < list2.items.length; i++) {
            this.lexer.state.top = false;
            list2.items[i].tokens = this.lexer.blockTokens(list2.items[i].text, []);
            if (!list2.loose) {
              const spacers = list2.items[i].tokens.filter((t) => t.type === "space");
              const hasMultipleLineBreaks = spacers.length > 0 && spacers.some((t) => this.rules.other.anyLine.test(t.raw));
              list2.loose = hasMultipleLineBreaks;
            }
          }
          if (list2.loose) {
            for (let i = 0; i < list2.items.length; i++) {
              list2.items[i].loose = true;
            }
          }
          return list2;
        }
      }
      html(src) {
        const cap = this.rules.block.html.exec(src);
        if (cap) {
          const token = {
            type: "html",
            block: true,
            raw: cap[0],
            pre: cap[1] === "pre" || cap[1] === "script" || cap[1] === "style",
            text: cap[0]
          };
          return token;
        }
      }
      def(src) {
        const cap = this.rules.block.def.exec(src);
        if (cap) {
          const tag2 = cap[1].toLowerCase().replace(this.rules.other.multipleSpaceGlobal, " ");
          const href = cap[2] ? cap[2].replace(this.rules.other.hrefBrackets, "$1").replace(this.rules.inline.anyPunctuation, "$1") : "";
          const title = cap[3] ? cap[3].substring(1, cap[3].length - 1).replace(this.rules.inline.anyPunctuation, "$1") : cap[3];
          return {
            type: "def",
            tag: tag2,
            raw: cap[0],
            href,
            title
          };
        }
      }
      table(src) {
        const cap = this.rules.block.table.exec(src);
        if (!cap) {
          return;
        }
        if (!this.rules.other.tableDelimiter.test(cap[2])) {
          return;
        }
        const headers = splitCells(cap[1]);
        const aligns = cap[2].replace(this.rules.other.tableAlignChars, "").split("|");
        const rows = cap[3]?.trim() ? cap[3].replace(this.rules.other.tableRowBlankLine, "").split("\n") : [];
        const item = {
          type: "table",
          raw: cap[0],
          header: [],
          align: [],
          rows: []
        };
        if (headers.length !== aligns.length) {
          return;
        }
        for (const align of aligns) {
          if (this.rules.other.tableAlignRight.test(align)) {
            item.align.push("right");
          } else if (this.rules.other.tableAlignCenter.test(align)) {
            item.align.push("center");
          } else if (this.rules.other.tableAlignLeft.test(align)) {
            item.align.push("left");
          } else {
            item.align.push(null);
          }
        }
        for (let i = 0; i < headers.length; i++) {
          item.header.push({
            text: headers[i],
            tokens: this.lexer.inline(headers[i]),
            header: true,
            align: item.align[i]
          });
        }
        for (const row of rows) {
          item.rows.push(splitCells(row, item.header.length).map((cell, i) => {
            return {
              text: cell,
              tokens: this.lexer.inline(cell),
              header: false,
              align: item.align[i]
            };
          }));
        }
        return item;
      }
      lheading(src) {
        const cap = this.rules.block.lheading.exec(src);
        if (cap) {
          return {
            type: "heading",
            raw: cap[0],
            depth: cap[2].charAt(0) === "=" ? 1 : 2,
            text: cap[1],
            tokens: this.lexer.inline(cap[1])
          };
        }
      }
      paragraph(src) {
        const cap = this.rules.block.paragraph.exec(src);
        if (cap) {
          const text = cap[1].charAt(cap[1].length - 1) === "\n" ? cap[1].slice(0, -1) : cap[1];
          return {
            type: "paragraph",
            raw: cap[0],
            text,
            tokens: this.lexer.inline(text)
          };
        }
      }
      text(src) {
        const cap = this.rules.block.text.exec(src);
        if (cap) {
          return {
            type: "text",
            raw: cap[0],
            text: cap[0],
            tokens: this.lexer.inline(cap[0])
          };
        }
      }
      escape(src) {
        const cap = this.rules.inline.escape.exec(src);
        if (cap) {
          return {
            type: "escape",
            raw: cap[0],
            text: cap[1]
          };
        }
      }
      tag(src) {
        const cap = this.rules.inline.tag.exec(src);
        if (cap) {
          if (!this.lexer.state.inLink && this.rules.other.startATag.test(cap[0])) {
            this.lexer.state.inLink = true;
          } else if (this.lexer.state.inLink && this.rules.other.endATag.test(cap[0])) {
            this.lexer.state.inLink = false;
          }
          if (!this.lexer.state.inRawBlock && this.rules.other.startPreScriptTag.test(cap[0])) {
            this.lexer.state.inRawBlock = true;
          } else if (this.lexer.state.inRawBlock && this.rules.other.endPreScriptTag.test(cap[0])) {
            this.lexer.state.inRawBlock = false;
          }
          return {
            type: "html",
            raw: cap[0],
            inLink: this.lexer.state.inLink,
            inRawBlock: this.lexer.state.inRawBlock,
            block: false,
            text: cap[0]
          };
        }
      }
      link(src) {
        const cap = this.rules.inline.link.exec(src);
        if (cap) {
          const trimmedUrl = cap[2].trim();
          if (!this.options.pedantic && this.rules.other.startAngleBracket.test(trimmedUrl)) {
            if (!this.rules.other.endAngleBracket.test(trimmedUrl)) {
              return;
            }
            const rtrimSlash = rtrim(trimmedUrl.slice(0, -1), "\\");
            if ((trimmedUrl.length - rtrimSlash.length) % 2 === 0) {
              return;
            }
          } else {
            const lastParenIndex = findClosingBracket(cap[2], "()");
            if (lastParenIndex === -2) {
              return;
            }
            if (lastParenIndex > -1) {
              const start = cap[0].indexOf("!") === 0 ? 5 : 4;
              const linkLen = start + cap[1].length + lastParenIndex;
              cap[2] = cap[2].substring(0, lastParenIndex);
              cap[0] = cap[0].substring(0, linkLen).trim();
              cap[3] = "";
            }
          }
          let href = cap[2];
          let title = "";
          if (this.options.pedantic) {
            const link2 = this.rules.other.pedanticHrefTitle.exec(href);
            if (link2) {
              href = link2[1];
              title = link2[3];
            }
          } else {
            title = cap[3] ? cap[3].slice(1, -1) : "";
          }
          href = href.trim();
          if (this.rules.other.startAngleBracket.test(href)) {
            if (this.options.pedantic && !this.rules.other.endAngleBracket.test(trimmedUrl)) {
              href = href.slice(1);
            } else {
              href = href.slice(1, -1);
            }
          }
          return outputLink(cap, {
            href: href ? href.replace(this.rules.inline.anyPunctuation, "$1") : href,
            title: title ? title.replace(this.rules.inline.anyPunctuation, "$1") : title
          }, cap[0], this.lexer, this.rules);
        }
      }
      reflink(src, links) {
        let cap;
        if ((cap = this.rules.inline.reflink.exec(src)) || (cap = this.rules.inline.nolink.exec(src))) {
          const linkString = (cap[2] || cap[1]).replace(this.rules.other.multipleSpaceGlobal, " ");
          const link2 = links[linkString.toLowerCase()];
          if (!link2) {
            const text = cap[0].charAt(0);
            return {
              type: "text",
              raw: text,
              text
            };
          }
          return outputLink(cap, link2, cap[0], this.lexer, this.rules);
        }
      }
      emStrong(src, maskedSrc, prevChar = "") {
        let match = this.rules.inline.emStrongLDelim.exec(src);
        if (!match) return;
        if (match[3] && prevChar.match(this.rules.other.unicodeAlphaNumeric)) return;
        const nextChar = match[1] || match[2] || "";
        if (!nextChar || !prevChar || this.rules.inline.punctuation.exec(prevChar)) {
          const lLength = [...match[0]].length - 1;
          let rDelim, rLength, delimTotal = lLength, midDelimTotal = 0;
          const endReg = match[0][0] === "*" ? this.rules.inline.emStrongRDelimAst : this.rules.inline.emStrongRDelimUnd;
          endReg.lastIndex = 0;
          maskedSrc = maskedSrc.slice(-1 * src.length + lLength);
          while ((match = endReg.exec(maskedSrc)) != null) {
            rDelim = match[1] || match[2] || match[3] || match[4] || match[5] || match[6];
            if (!rDelim) continue;
            rLength = [...rDelim].length;
            if (match[3] || match[4]) {
              delimTotal += rLength;
              continue;
            } else if (match[5] || match[6]) {
              if (lLength % 3 && !((lLength + rLength) % 3)) {
                midDelimTotal += rLength;
                continue;
              }
            }
            delimTotal -= rLength;
            if (delimTotal > 0) continue;
            rLength = Math.min(rLength, rLength + delimTotal + midDelimTotal);
            const lastCharLength = [...match[0]][0].length;
            const raw = src.slice(0, lLength + match.index + lastCharLength + rLength);
            if (Math.min(lLength, rLength) % 2) {
              const text2 = raw.slice(1, -1);
              return {
                type: "em",
                raw,
                text: text2,
                tokens: this.lexer.inlineTokens(text2)
              };
            }
            const text = raw.slice(2, -2);
            return {
              type: "strong",
              raw,
              text,
              tokens: this.lexer.inlineTokens(text)
            };
          }
        }
      }
      codespan(src) {
        const cap = this.rules.inline.code.exec(src);
        if (cap) {
          let text = cap[2].replace(this.rules.other.newLineCharGlobal, " ");
          const hasNonSpaceChars = this.rules.other.nonSpaceChar.test(text);
          const hasSpaceCharsOnBothEnds = this.rules.other.startingSpaceChar.test(text) && this.rules.other.endingSpaceChar.test(text);
          if (hasNonSpaceChars && hasSpaceCharsOnBothEnds) {
            text = text.substring(1, text.length - 1);
          }
          return {
            type: "codespan",
            raw: cap[0],
            text
          };
        }
      }
      br(src) {
        const cap = this.rules.inline.br.exec(src);
        if (cap) {
          return {
            type: "br",
            raw: cap[0]
          };
        }
      }
      del(src) {
        const cap = this.rules.inline.del.exec(src);
        if (cap) {
          return {
            type: "del",
            raw: cap[0],
            text: cap[2],
            tokens: this.lexer.inlineTokens(cap[2])
          };
        }
      }
      autolink(src) {
        const cap = this.rules.inline.autolink.exec(src);
        if (cap) {
          let text, href;
          if (cap[2] === "@") {
            text = cap[1];
            href = "mailto:" + text;
          } else {
            text = cap[1];
            href = text;
          }
          return {
            type: "link",
            raw: cap[0],
            text,
            href,
            tokens: [
              {
                type: "text",
                raw: text,
                text
              }
            ]
          };
        }
      }
      url(src) {
        let cap;
        if (cap = this.rules.inline.url.exec(src)) {
          let text, href;
          if (cap[2] === "@") {
            text = cap[0];
            href = "mailto:" + text;
          } else {
            let prevCapZero;
            do {
              prevCapZero = cap[0];
              cap[0] = this.rules.inline._backpedal.exec(cap[0])?.[0] ?? "";
            } while (prevCapZero !== cap[0]);
            text = cap[0];
            if (cap[1] === "www.") {
              href = "http://" + cap[0];
            } else {
              href = cap[0];
            }
          }
          return {
            type: "link",
            raw: cap[0],
            text,
            href,
            tokens: [
              {
                type: "text",
                raw: text,
                text
              }
            ]
          };
        }
      }
      inlineText(src) {
        const cap = this.rules.inline.text.exec(src);
        if (cap) {
          const escaped = this.lexer.state.inRawBlock;
          return {
            type: "text",
            raw: cap[0],
            text: cap[0],
            escaped
          };
        }
      }
    };
    _Lexer = class __Lexer {
      tokens;
      options;
      state;
      tokenizer;
      inlineQueue;
      constructor(options2) {
        this.tokens = [];
        this.tokens.links = /* @__PURE__ */ Object.create(null);
        this.options = options2 || _defaults;
        this.options.tokenizer = this.options.tokenizer || new _Tokenizer();
        this.tokenizer = this.options.tokenizer;
        this.tokenizer.options = this.options;
        this.tokenizer.lexer = this;
        this.inlineQueue = [];
        this.state = {
          inLink: false,
          inRawBlock: false,
          top: true
        };
        const rules = {
          other,
          block: block.normal,
          inline: inline.normal
        };
        if (this.options.pedantic) {
          rules.block = block.pedantic;
          rules.inline = inline.pedantic;
        } else if (this.options.gfm) {
          rules.block = block.gfm;
          if (this.options.breaks) {
            rules.inline = inline.breaks;
          } else {
            rules.inline = inline.gfm;
          }
        }
        this.tokenizer.rules = rules;
      }
      /**
       * Expose Rules
       */
      static get rules() {
        return {
          block,
          inline
        };
      }
      /**
       * Static Lex Method
       */
      static lex(src, options2) {
        const lexer2 = new __Lexer(options2);
        return lexer2.lex(src);
      }
      /**
       * Static Lex Inline Method
       */
      static lexInline(src, options2) {
        const lexer2 = new __Lexer(options2);
        return lexer2.inlineTokens(src);
      }
      /**
       * Preprocessing
       */
      lex(src) {
        src = src.replace(other.carriageReturn, "\n");
        this.blockTokens(src, this.tokens);
        for (let i = 0; i < this.inlineQueue.length; i++) {
          const next = this.inlineQueue[i];
          this.inlineTokens(next.src, next.tokens);
        }
        this.inlineQueue = [];
        return this.tokens;
      }
      blockTokens(src, tokens = [], lastParagraphClipped = false) {
        if (this.options.pedantic) {
          src = src.replace(other.tabCharGlobal, "    ").replace(other.spaceLine, "");
        }
        while (src) {
          let token;
          if (this.options.extensions?.block?.some((extTokenizer) => {
            if (token = extTokenizer.call({ lexer: this }, src, tokens)) {
              src = src.substring(token.raw.length);
              tokens.push(token);
              return true;
            }
            return false;
          })) {
            continue;
          }
          if (token = this.tokenizer.space(src)) {
            src = src.substring(token.raw.length);
            const lastToken = tokens.at(-1);
            if (token.raw.length === 1 && lastToken !== void 0) {
              lastToken.raw += "\n";
            } else {
              tokens.push(token);
            }
            continue;
          }
          if (token = this.tokenizer.code(src)) {
            src = src.substring(token.raw.length);
            const lastToken = tokens.at(-1);
            if (lastToken?.type === "paragraph" || lastToken?.type === "text") {
              lastToken.raw += "\n" + token.raw;
              lastToken.text += "\n" + token.text;
              this.inlineQueue.at(-1).src = lastToken.text;
            } else {
              tokens.push(token);
            }
            continue;
          }
          if (token = this.tokenizer.fences(src)) {
            src = src.substring(token.raw.length);
            tokens.push(token);
            continue;
          }
          if (token = this.tokenizer.heading(src)) {
            src = src.substring(token.raw.length);
            tokens.push(token);
            continue;
          }
          if (token = this.tokenizer.hr(src)) {
            src = src.substring(token.raw.length);
            tokens.push(token);
            continue;
          }
          if (token = this.tokenizer.blockquote(src)) {
            src = src.substring(token.raw.length);
            tokens.push(token);
            continue;
          }
          if (token = this.tokenizer.list(src)) {
            src = src.substring(token.raw.length);
            tokens.push(token);
            continue;
          }
          if (token = this.tokenizer.html(src)) {
            src = src.substring(token.raw.length);
            tokens.push(token);
            continue;
          }
          if (token = this.tokenizer.def(src)) {
            src = src.substring(token.raw.length);
            const lastToken = tokens.at(-1);
            if (lastToken?.type === "paragraph" || lastToken?.type === "text") {
              lastToken.raw += "\n" + token.raw;
              lastToken.text += "\n" + token.raw;
              this.inlineQueue.at(-1).src = lastToken.text;
            } else if (!this.tokens.links[token.tag]) {
              this.tokens.links[token.tag] = {
                href: token.href,
                title: token.title
              };
            }
            continue;
          }
          if (token = this.tokenizer.table(src)) {
            src = src.substring(token.raw.length);
            tokens.push(token);
            continue;
          }
          if (token = this.tokenizer.lheading(src)) {
            src = src.substring(token.raw.length);
            tokens.push(token);
            continue;
          }
          let cutSrc = src;
          if (this.options.extensions?.startBlock) {
            let startIndex = Infinity;
            const tempSrc = src.slice(1);
            let tempStart;
            this.options.extensions.startBlock.forEach((getStartIndex) => {
              tempStart = getStartIndex.call({ lexer: this }, tempSrc);
              if (typeof tempStart === "number" && tempStart >= 0) {
                startIndex = Math.min(startIndex, tempStart);
              }
            });
            if (startIndex < Infinity && startIndex >= 0) {
              cutSrc = src.substring(0, startIndex + 1);
            }
          }
          if (this.state.top && (token = this.tokenizer.paragraph(cutSrc))) {
            const lastToken = tokens.at(-1);
            if (lastParagraphClipped && lastToken?.type === "paragraph") {
              lastToken.raw += "\n" + token.raw;
              lastToken.text += "\n" + token.text;
              this.inlineQueue.pop();
              this.inlineQueue.at(-1).src = lastToken.text;
            } else {
              tokens.push(token);
            }
            lastParagraphClipped = cutSrc.length !== src.length;
            src = src.substring(token.raw.length);
            continue;
          }
          if (token = this.tokenizer.text(src)) {
            src = src.substring(token.raw.length);
            const lastToken = tokens.at(-1);
            if (lastToken?.type === "text") {
              lastToken.raw += "\n" + token.raw;
              lastToken.text += "\n" + token.text;
              this.inlineQueue.pop();
              this.inlineQueue.at(-1).src = lastToken.text;
            } else {
              tokens.push(token);
            }
            continue;
          }
          if (src) {
            const errMsg = "Infinite loop on byte: " + src.charCodeAt(0);
            if (this.options.silent) {
              console.error(errMsg);
              break;
            } else {
              throw new Error(errMsg);
            }
          }
        }
        this.state.top = true;
        return tokens;
      }
      inline(src, tokens = []) {
        this.inlineQueue.push({ src, tokens });
        return tokens;
      }
      /**
       * Lexing/Compiling
       */
      inlineTokens(src, tokens = []) {
        let maskedSrc = src;
        let match = null;
        if (this.tokens.links) {
          const links = Object.keys(this.tokens.links);
          if (links.length > 0) {
            while ((match = this.tokenizer.rules.inline.reflinkSearch.exec(maskedSrc)) != null) {
              if (links.includes(match[0].slice(match[0].lastIndexOf("[") + 1, -1))) {
                maskedSrc = maskedSrc.slice(0, match.index) + "[" + "a".repeat(match[0].length - 2) + "]" + maskedSrc.slice(this.tokenizer.rules.inline.reflinkSearch.lastIndex);
              }
            }
          }
        }
        while ((match = this.tokenizer.rules.inline.anyPunctuation.exec(maskedSrc)) != null) {
          maskedSrc = maskedSrc.slice(0, match.index) + "++" + maskedSrc.slice(this.tokenizer.rules.inline.anyPunctuation.lastIndex);
        }
        while ((match = this.tokenizer.rules.inline.blockSkip.exec(maskedSrc)) != null) {
          maskedSrc = maskedSrc.slice(0, match.index) + "[" + "a".repeat(match[0].length - 2) + "]" + maskedSrc.slice(this.tokenizer.rules.inline.blockSkip.lastIndex);
        }
        let keepPrevChar = false;
        let prevChar = "";
        while (src) {
          if (!keepPrevChar) {
            prevChar = "";
          }
          keepPrevChar = false;
          let token;
          if (this.options.extensions?.inline?.some((extTokenizer) => {
            if (token = extTokenizer.call({ lexer: this }, src, tokens)) {
              src = src.substring(token.raw.length);
              tokens.push(token);
              return true;
            }
            return false;
          })) {
            continue;
          }
          if (token = this.tokenizer.escape(src)) {
            src = src.substring(token.raw.length);
            tokens.push(token);
            continue;
          }
          if (token = this.tokenizer.tag(src)) {
            src = src.substring(token.raw.length);
            tokens.push(token);
            continue;
          }
          if (token = this.tokenizer.link(src)) {
            src = src.substring(token.raw.length);
            tokens.push(token);
            continue;
          }
          if (token = this.tokenizer.reflink(src, this.tokens.links)) {
            src = src.substring(token.raw.length);
            const lastToken = tokens.at(-1);
            if (token.type === "text" && lastToken?.type === "text") {
              lastToken.raw += token.raw;
              lastToken.text += token.text;
            } else {
              tokens.push(token);
            }
            continue;
          }
          if (token = this.tokenizer.emStrong(src, maskedSrc, prevChar)) {
            src = src.substring(token.raw.length);
            tokens.push(token);
            continue;
          }
          if (token = this.tokenizer.codespan(src)) {
            src = src.substring(token.raw.length);
            tokens.push(token);
            continue;
          }
          if (token = this.tokenizer.br(src)) {
            src = src.substring(token.raw.length);
            tokens.push(token);
            continue;
          }
          if (token = this.tokenizer.del(src)) {
            src = src.substring(token.raw.length);
            tokens.push(token);
            continue;
          }
          if (token = this.tokenizer.autolink(src)) {
            src = src.substring(token.raw.length);
            tokens.push(token);
            continue;
          }
          if (!this.state.inLink && (token = this.tokenizer.url(src))) {
            src = src.substring(token.raw.length);
            tokens.push(token);
            continue;
          }
          let cutSrc = src;
          if (this.options.extensions?.startInline) {
            let startIndex = Infinity;
            const tempSrc = src.slice(1);
            let tempStart;
            this.options.extensions.startInline.forEach((getStartIndex) => {
              tempStart = getStartIndex.call({ lexer: this }, tempSrc);
              if (typeof tempStart === "number" && tempStart >= 0) {
                startIndex = Math.min(startIndex, tempStart);
              }
            });
            if (startIndex < Infinity && startIndex >= 0) {
              cutSrc = src.substring(0, startIndex + 1);
            }
          }
          if (token = this.tokenizer.inlineText(cutSrc)) {
            src = src.substring(token.raw.length);
            if (token.raw.slice(-1) !== "_") {
              prevChar = token.raw.slice(-1);
            }
            keepPrevChar = true;
            const lastToken = tokens.at(-1);
            if (lastToken?.type === "text") {
              lastToken.raw += token.raw;
              lastToken.text += token.text;
            } else {
              tokens.push(token);
            }
            continue;
          }
          if (src) {
            const errMsg = "Infinite loop on byte: " + src.charCodeAt(0);
            if (this.options.silent) {
              console.error(errMsg);
              break;
            } else {
              throw new Error(errMsg);
            }
          }
        }
        return tokens;
      }
    };
    _Renderer = class {
      options;
      parser;
      // set by the parser
      constructor(options2) {
        this.options = options2 || _defaults;
      }
      space(token) {
        return "";
      }
      code({ text, lang, escaped }) {
        const langString = (lang || "").match(other.notSpaceStart)?.[0];
        const code = text.replace(other.endingNewline, "") + "\n";
        if (!langString) {
          return "<pre><code>" + (escaped ? code : escape2(code, true)) + "</code></pre>\n";
        }
        return '<pre><code class="language-' + escape2(langString) + '">' + (escaped ? code : escape2(code, true)) + "</code></pre>\n";
      }
      blockquote({ tokens }) {
        const body = this.parser.parse(tokens);
        return `<blockquote>
${body}</blockquote>
`;
      }
      html({ text }) {
        return text;
      }
      heading({ tokens, depth }) {
        return `<h${depth}>${this.parser.parseInline(tokens)}</h${depth}>
`;
      }
      hr(token) {
        return "<hr>\n";
      }
      list(token) {
        const ordered = token.ordered;
        const start = token.start;
        let body = "";
        for (let j = 0; j < token.items.length; j++) {
          const item = token.items[j];
          body += this.listitem(item);
        }
        const type = ordered ? "ol" : "ul";
        const startAttr = ordered && start !== 1 ? ' start="' + start + '"' : "";
        return "<" + type + startAttr + ">\n" + body + "</" + type + ">\n";
      }
      listitem(item) {
        let itemBody = "";
        if (item.task) {
          const checkbox = this.checkbox({ checked: !!item.checked });
          if (item.loose) {
            if (item.tokens[0]?.type === "paragraph") {
              item.tokens[0].text = checkbox + " " + item.tokens[0].text;
              if (item.tokens[0].tokens && item.tokens[0].tokens.length > 0 && item.tokens[0].tokens[0].type === "text") {
                item.tokens[0].tokens[0].text = checkbox + " " + escape2(item.tokens[0].tokens[0].text);
                item.tokens[0].tokens[0].escaped = true;
              }
            } else {
              item.tokens.unshift({
                type: "text",
                raw: checkbox + " ",
                text: checkbox + " ",
                escaped: true
              });
            }
          } else {
            itemBody += checkbox + " ";
          }
        }
        itemBody += this.parser.parse(item.tokens, !!item.loose);
        return `<li>${itemBody}</li>
`;
      }
      checkbox({ checked }) {
        return "<input " + (checked ? 'checked="" ' : "") + 'disabled="" type="checkbox">';
      }
      paragraph({ tokens }) {
        return `<p>${this.parser.parseInline(tokens)}</p>
`;
      }
      table(token) {
        let header = "";
        let cell = "";
        for (let j = 0; j < token.header.length; j++) {
          cell += this.tablecell(token.header[j]);
        }
        header += this.tablerow({ text: cell });
        let body = "";
        for (let j = 0; j < token.rows.length; j++) {
          const row = token.rows[j];
          cell = "";
          for (let k = 0; k < row.length; k++) {
            cell += this.tablecell(row[k]);
          }
          body += this.tablerow({ text: cell });
        }
        if (body) body = `<tbody>${body}</tbody>`;
        return "<table>\n<thead>\n" + header + "</thead>\n" + body + "</table>\n";
      }
      tablerow({ text }) {
        return `<tr>
${text}</tr>
`;
      }
      tablecell(token) {
        const content = this.parser.parseInline(token.tokens);
        const type = token.header ? "th" : "td";
        const tag2 = token.align ? `<${type} align="${token.align}">` : `<${type}>`;
        return tag2 + content + `</${type}>
`;
      }
      /**
       * span level renderer
       */
      strong({ tokens }) {
        return `<strong>${this.parser.parseInline(tokens)}</strong>`;
      }
      em({ tokens }) {
        return `<em>${this.parser.parseInline(tokens)}</em>`;
      }
      codespan({ text }) {
        return `<code>${escape2(text, true)}</code>`;
      }
      br(token) {
        return "<br>";
      }
      del({ tokens }) {
        return `<del>${this.parser.parseInline(tokens)}</del>`;
      }
      link({ href, title, tokens }) {
        const text = this.parser.parseInline(tokens);
        const cleanHref = cleanUrl(href);
        if (cleanHref === null) {
          return text;
        }
        href = cleanHref;
        let out = '<a href="' + href + '"';
        if (title) {
          out += ' title="' + escape2(title) + '"';
        }
        out += ">" + text + "</a>";
        return out;
      }
      image({ href, title, text, tokens }) {
        if (tokens) {
          text = this.parser.parseInline(tokens, this.parser.textRenderer);
        }
        const cleanHref = cleanUrl(href);
        if (cleanHref === null) {
          return escape2(text);
        }
        href = cleanHref;
        let out = `<img src="${href}" alt="${text}"`;
        if (title) {
          out += ` title="${escape2(title)}"`;
        }
        out += ">";
        return out;
      }
      text(token) {
        return "tokens" in token && token.tokens ? this.parser.parseInline(token.tokens) : "escaped" in token && token.escaped ? token.text : escape2(token.text);
      }
    };
    _TextRenderer = class {
      // no need for block level renderers
      strong({ text }) {
        return text;
      }
      em({ text }) {
        return text;
      }
      codespan({ text }) {
        return text;
      }
      del({ text }) {
        return text;
      }
      html({ text }) {
        return text;
      }
      text({ text }) {
        return text;
      }
      link({ text }) {
        return "" + text;
      }
      image({ text }) {
        return "" + text;
      }
      br() {
        return "";
      }
    };
    _Parser = class __Parser {
      options;
      renderer;
      textRenderer;
      constructor(options2) {
        this.options = options2 || _defaults;
        this.options.renderer = this.options.renderer || new _Renderer();
        this.renderer = this.options.renderer;
        this.renderer.options = this.options;
        this.renderer.parser = this;
        this.textRenderer = new _TextRenderer();
      }
      /**
       * Static Parse Method
       */
      static parse(tokens, options2) {
        const parser2 = new __Parser(options2);
        return parser2.parse(tokens);
      }
      /**
       * Static Parse Inline Method
       */
      static parseInline(tokens, options2) {
        const parser2 = new __Parser(options2);
        return parser2.parseInline(tokens);
      }
      /**
       * Parse Loop
       */
      parse(tokens, top = true) {
        let out = "";
        for (let i = 0; i < tokens.length; i++) {
          const anyToken = tokens[i];
          if (this.options.extensions?.renderers?.[anyToken.type]) {
            const genericToken = anyToken;
            const ret = this.options.extensions.renderers[genericToken.type].call({ parser: this }, genericToken);
            if (ret !== false || !["space", "hr", "heading", "code", "table", "blockquote", "list", "html", "paragraph", "text"].includes(genericToken.type)) {
              out += ret || "";
              continue;
            }
          }
          const token = anyToken;
          switch (token.type) {
            case "space": {
              out += this.renderer.space(token);
              continue;
            }
            case "hr": {
              out += this.renderer.hr(token);
              continue;
            }
            case "heading": {
              out += this.renderer.heading(token);
              continue;
            }
            case "code": {
              out += this.renderer.code(token);
              continue;
            }
            case "table": {
              out += this.renderer.table(token);
              continue;
            }
            case "blockquote": {
              out += this.renderer.blockquote(token);
              continue;
            }
            case "list": {
              out += this.renderer.list(token);
              continue;
            }
            case "html": {
              out += this.renderer.html(token);
              continue;
            }
            case "paragraph": {
              out += this.renderer.paragraph(token);
              continue;
            }
            case "text": {
              let textToken = token;
              let body = this.renderer.text(textToken);
              while (i + 1 < tokens.length && tokens[i + 1].type === "text") {
                textToken = tokens[++i];
                body += "\n" + this.renderer.text(textToken);
              }
              if (top) {
                out += this.renderer.paragraph({
                  type: "paragraph",
                  raw: body,
                  text: body,
                  tokens: [{ type: "text", raw: body, text: body, escaped: true }]
                });
              } else {
                out += body;
              }
              continue;
            }
            default: {
              const errMsg = 'Token with "' + token.type + '" type was not found.';
              if (this.options.silent) {
                console.error(errMsg);
                return "";
              } else {
                throw new Error(errMsg);
              }
            }
          }
        }
        return out;
      }
      /**
       * Parse Inline Tokens
       */
      parseInline(tokens, renderer = this.renderer) {
        let out = "";
        for (let i = 0; i < tokens.length; i++) {
          const anyToken = tokens[i];
          if (this.options.extensions?.renderers?.[anyToken.type]) {
            const ret = this.options.extensions.renderers[anyToken.type].call({ parser: this }, anyToken);
            if (ret !== false || !["escape", "html", "link", "image", "strong", "em", "codespan", "br", "del", "text"].includes(anyToken.type)) {
              out += ret || "";
              continue;
            }
          }
          const token = anyToken;
          switch (token.type) {
            case "escape": {
              out += renderer.text(token);
              break;
            }
            case "html": {
              out += renderer.html(token);
              break;
            }
            case "link": {
              out += renderer.link(token);
              break;
            }
            case "image": {
              out += renderer.image(token);
              break;
            }
            case "strong": {
              out += renderer.strong(token);
              break;
            }
            case "em": {
              out += renderer.em(token);
              break;
            }
            case "codespan": {
              out += renderer.codespan(token);
              break;
            }
            case "br": {
              out += renderer.br(token);
              break;
            }
            case "del": {
              out += renderer.del(token);
              break;
            }
            case "text": {
              out += renderer.text(token);
              break;
            }
            default: {
              const errMsg = 'Token with "' + token.type + '" type was not found.';
              if (this.options.silent) {
                console.error(errMsg);
                return "";
              } else {
                throw new Error(errMsg);
              }
            }
          }
        }
        return out;
      }
    };
    _Hooks = class {
      options;
      block;
      constructor(options2) {
        this.options = options2 || _defaults;
      }
      static passThroughHooks = /* @__PURE__ */ new Set([
        "preprocess",
        "postprocess",
        "processAllTokens"
      ]);
      /**
       * Process markdown before marked
       */
      preprocess(markdown) {
        return markdown;
      }
      /**
       * Process HTML after marked is finished
       */
      postprocess(html2) {
        return html2;
      }
      /**
       * Process all tokens before walk tokens
       */
      processAllTokens(tokens) {
        return tokens;
      }
      /**
       * Provide function to tokenize markdown
       */
      provideLexer() {
        return this.block ? _Lexer.lex : _Lexer.lexInline;
      }
      /**
       * Provide function to parse tokens
       */
      provideParser() {
        return this.block ? _Parser.parse : _Parser.parseInline;
      }
    };
    Marked = class {
      defaults = _getDefaults();
      options = this.setOptions;
      parse = this.parseMarkdown(true);
      parseInline = this.parseMarkdown(false);
      Parser = _Parser;
      Renderer = _Renderer;
      TextRenderer = _TextRenderer;
      Lexer = _Lexer;
      Tokenizer = _Tokenizer;
      Hooks = _Hooks;
      constructor(...args) {
        this.use(...args);
      }
      /**
       * Run callback for every token
       */
      walkTokens(tokens, callback) {
        let values = [];
        for (const token of tokens) {
          values = values.concat(callback.call(this, token));
          switch (token.type) {
            case "table": {
              const tableToken = token;
              for (const cell of tableToken.header) {
                values = values.concat(this.walkTokens(cell.tokens, callback));
              }
              for (const row of tableToken.rows) {
                for (const cell of row) {
                  values = values.concat(this.walkTokens(cell.tokens, callback));
                }
              }
              break;
            }
            case "list": {
              const listToken = token;
              values = values.concat(this.walkTokens(listToken.items, callback));
              break;
            }
            default: {
              const genericToken = token;
              if (this.defaults.extensions?.childTokens?.[genericToken.type]) {
                this.defaults.extensions.childTokens[genericToken.type].forEach((childTokens) => {
                  const tokens2 = genericToken[childTokens].flat(Infinity);
                  values = values.concat(this.walkTokens(tokens2, callback));
                });
              } else if (genericToken.tokens) {
                values = values.concat(this.walkTokens(genericToken.tokens, callback));
              }
            }
          }
        }
        return values;
      }
      use(...args) {
        const extensions = this.defaults.extensions || { renderers: {}, childTokens: {} };
        args.forEach((pack) => {
          const opts = { ...pack };
          opts.async = this.defaults.async || opts.async || false;
          if (pack.extensions) {
            pack.extensions.forEach((ext) => {
              if (!ext.name) {
                throw new Error("extension name required");
              }
              if ("renderer" in ext) {
                const prevRenderer = extensions.renderers[ext.name];
                if (prevRenderer) {
                  extensions.renderers[ext.name] = function(...args2) {
                    let ret = ext.renderer.apply(this, args2);
                    if (ret === false) {
                      ret = prevRenderer.apply(this, args2);
                    }
                    return ret;
                  };
                } else {
                  extensions.renderers[ext.name] = ext.renderer;
                }
              }
              if ("tokenizer" in ext) {
                if (!ext.level || ext.level !== "block" && ext.level !== "inline") {
                  throw new Error("extension level must be 'block' or 'inline'");
                }
                const extLevel = extensions[ext.level];
                if (extLevel) {
                  extLevel.unshift(ext.tokenizer);
                } else {
                  extensions[ext.level] = [ext.tokenizer];
                }
                if (ext.start) {
                  if (ext.level === "block") {
                    if (extensions.startBlock) {
                      extensions.startBlock.push(ext.start);
                    } else {
                      extensions.startBlock = [ext.start];
                    }
                  } else if (ext.level === "inline") {
                    if (extensions.startInline) {
                      extensions.startInline.push(ext.start);
                    } else {
                      extensions.startInline = [ext.start];
                    }
                  }
                }
              }
              if ("childTokens" in ext && ext.childTokens) {
                extensions.childTokens[ext.name] = ext.childTokens;
              }
            });
            opts.extensions = extensions;
          }
          if (pack.renderer) {
            const renderer = this.defaults.renderer || new _Renderer(this.defaults);
            for (const prop in pack.renderer) {
              if (!(prop in renderer)) {
                throw new Error(`renderer '${prop}' does not exist`);
              }
              if (["options", "parser"].includes(prop)) {
                continue;
              }
              const rendererProp = prop;
              const rendererFunc = pack.renderer[rendererProp];
              const prevRenderer = renderer[rendererProp];
              renderer[rendererProp] = (...args2) => {
                let ret = rendererFunc.apply(renderer, args2);
                if (ret === false) {
                  ret = prevRenderer.apply(renderer, args2);
                }
                return ret || "";
              };
            }
            opts.renderer = renderer;
          }
          if (pack.tokenizer) {
            const tokenizer = this.defaults.tokenizer || new _Tokenizer(this.defaults);
            for (const prop in pack.tokenizer) {
              if (!(prop in tokenizer)) {
                throw new Error(`tokenizer '${prop}' does not exist`);
              }
              if (["options", "rules", "lexer"].includes(prop)) {
                continue;
              }
              const tokenizerProp = prop;
              const tokenizerFunc = pack.tokenizer[tokenizerProp];
              const prevTokenizer = tokenizer[tokenizerProp];
              tokenizer[tokenizerProp] = (...args2) => {
                let ret = tokenizerFunc.apply(tokenizer, args2);
                if (ret === false) {
                  ret = prevTokenizer.apply(tokenizer, args2);
                }
                return ret;
              };
            }
            opts.tokenizer = tokenizer;
          }
          if (pack.hooks) {
            const hooks = this.defaults.hooks || new _Hooks();
            for (const prop in pack.hooks) {
              if (!(prop in hooks)) {
                throw new Error(`hook '${prop}' does not exist`);
              }
              if (["options", "block"].includes(prop)) {
                continue;
              }
              const hooksProp = prop;
              const hooksFunc = pack.hooks[hooksProp];
              const prevHook = hooks[hooksProp];
              if (_Hooks.passThroughHooks.has(prop)) {
                hooks[hooksProp] = (arg) => {
                  if (this.defaults.async) {
                    return Promise.resolve(hooksFunc.call(hooks, arg)).then((ret2) => {
                      return prevHook.call(hooks, ret2);
                    });
                  }
                  const ret = hooksFunc.call(hooks, arg);
                  return prevHook.call(hooks, ret);
                };
              } else {
                hooks[hooksProp] = (...args2) => {
                  let ret = hooksFunc.apply(hooks, args2);
                  if (ret === false) {
                    ret = prevHook.apply(hooks, args2);
                  }
                  return ret;
                };
              }
            }
            opts.hooks = hooks;
          }
          if (pack.walkTokens) {
            const walkTokens2 = this.defaults.walkTokens;
            const packWalktokens = pack.walkTokens;
            opts.walkTokens = function(token) {
              let values = [];
              values.push(packWalktokens.call(this, token));
              if (walkTokens2) {
                values = values.concat(walkTokens2.call(this, token));
              }
              return values;
            };
          }
          this.defaults = { ...this.defaults, ...opts };
        });
        return this;
      }
      setOptions(opt) {
        this.defaults = { ...this.defaults, ...opt };
        return this;
      }
      lexer(src, options2) {
        return _Lexer.lex(src, options2 ?? this.defaults);
      }
      parser(tokens, options2) {
        return _Parser.parse(tokens, options2 ?? this.defaults);
      }
      parseMarkdown(blockType) {
        const parse2 = (src, options2) => {
          const origOpt = { ...options2 };
          const opt = { ...this.defaults, ...origOpt };
          const throwError = this.onError(!!opt.silent, !!opt.async);
          if (this.defaults.async === true && origOpt.async === false) {
            return throwError(new Error("marked(): The async option was set to true by an extension. Remove async: false from the parse options object to return a Promise."));
          }
          if (typeof src === "undefined" || src === null) {
            return throwError(new Error("marked(): input parameter is undefined or null"));
          }
          if (typeof src !== "string") {
            return throwError(new Error("marked(): input parameter is of type " + Object.prototype.toString.call(src) + ", string expected"));
          }
          if (opt.hooks) {
            opt.hooks.options = opt;
            opt.hooks.block = blockType;
          }
          const lexer2 = opt.hooks ? opt.hooks.provideLexer() : blockType ? _Lexer.lex : _Lexer.lexInline;
          const parser2 = opt.hooks ? opt.hooks.provideParser() : blockType ? _Parser.parse : _Parser.parseInline;
          if (opt.async) {
            return Promise.resolve(opt.hooks ? opt.hooks.preprocess(src) : src).then((src2) => lexer2(src2, opt)).then((tokens) => opt.hooks ? opt.hooks.processAllTokens(tokens) : tokens).then((tokens) => opt.walkTokens ? Promise.all(this.walkTokens(tokens, opt.walkTokens)).then(() => tokens) : tokens).then((tokens) => parser2(tokens, opt)).then((html2) => opt.hooks ? opt.hooks.postprocess(html2) : html2).catch(throwError);
          }
          try {
            if (opt.hooks) {
              src = opt.hooks.preprocess(src);
            }
            let tokens = lexer2(src, opt);
            if (opt.hooks) {
              tokens = opt.hooks.processAllTokens(tokens);
            }
            if (opt.walkTokens) {
              this.walkTokens(tokens, opt.walkTokens);
            }
            let html2 = parser2(tokens, opt);
            if (opt.hooks) {
              html2 = opt.hooks.postprocess(html2);
            }
            return html2;
          } catch (e) {
            return throwError(e);
          }
        };
        return parse2;
      }
      onError(silent, async) {
        return (e) => {
          e.message += "\nPlease report this to https://github.com/markedjs/marked.";
          if (silent) {
            const msg = "<p>An error occurred:</p><pre>" + escape2(e.message + "", true) + "</pre>";
            if (async) {
              return Promise.resolve(msg);
            }
            return msg;
          }
          if (async) {
            return Promise.reject(e);
          }
          throw e;
        };
      }
    };
    markedInstance = new Marked();
    marked.options = marked.setOptions = function(options2) {
      markedInstance.setOptions(options2);
      marked.defaults = markedInstance.defaults;
      changeDefaults(marked.defaults);
      return marked;
    };
    marked.getDefaults = _getDefaults;
    marked.defaults = _defaults;
    marked.use = function(...args) {
      markedInstance.use(...args);
      marked.defaults = markedInstance.defaults;
      changeDefaults(marked.defaults);
      return marked;
    };
    marked.walkTokens = function(tokens, callback) {
      return markedInstance.walkTokens(tokens, callback);
    };
    marked.parseInline = markedInstance.parseInline;
    marked.Parser = _Parser;
    marked.parser = _Parser.parse;
    marked.Renderer = _Renderer;
    marked.TextRenderer = _TextRenderer;
    marked.Lexer = _Lexer;
    marked.lexer = _Lexer.lex;
    marked.Tokenizer = _Tokenizer;
    marked.Hooks = _Hooks;
    marked.parse = marked;
    options = marked.options;
    setOptions = marked.setOptions;
    use = marked.use;
    walkTokens = marked.walkTokens;
    parseInline = marked.parseInline;
    parser = _Parser.parse;
    lexer = _Lexer.lex;
  }
});

// node_modules/@mariozechner/pi-tui/dist/components/markdown.js
var STRICT_STRIKETHROUGH_REGEX, StrictStrikethroughTokenizer, markdownParser;
var init_markdown = __esm({
  "node_modules/@mariozechner/pi-tui/dist/components/markdown.js"() {
    init_marked_esm();
    init_terminal_image();
    init_utils();
    STRICT_STRIKETHROUGH_REGEX = /^(~~)(?=[^\s~])((?:\\.|[^\\])*?(?:\\.|[^\s~\\]))\1(?=[^~]|$)/;
    StrictStrikethroughTokenizer = class extends _Tokenizer {
      del(src) {
        const match = STRICT_STRIKETHROUGH_REGEX.exec(src);
        if (!match) {
          return void 0;
        }
        const text = match[2];
        return {
          type: "del",
          raw: match[0],
          text,
          tokens: this.lexer.inlineTokens(text)
        };
      }
    };
    markdownParser = new Marked();
    markdownParser.setOptions({
      tokenizer: new StrictStrikethroughTokenizer()
    });
  }
});

// node_modules/@mariozechner/pi-tui/dist/components/settings-list.js
var init_settings_list = __esm({
  "node_modules/@mariozechner/pi-tui/dist/components/settings-list.js"() {
    init_fuzzy();
    init_keybindings();
    init_utils();
    init_input();
  }
});

// node_modules/@mariozechner/pi-tui/dist/components/spacer.js
var init_spacer = __esm({
  "node_modules/@mariozechner/pi-tui/dist/components/spacer.js"() {
  }
});

// node_modules/@mariozechner/pi-tui/dist/components/truncated-text.js
var init_truncated_text = __esm({
  "node_modules/@mariozechner/pi-tui/dist/components/truncated-text.js"() {
    init_utils();
  }
});

// node_modules/@mariozechner/pi-tui/dist/stdin-buffer.js
var init_stdin_buffer = __esm({
  "node_modules/@mariozechner/pi-tui/dist/stdin-buffer.js"() {
  }
});

// node_modules/@mariozechner/pi-tui/dist/terminal.js
import { createRequire } from "node:module";
var cjsRequire;
var init_terminal = __esm({
  "node_modules/@mariozechner/pi-tui/dist/terminal.js"() {
    init_keys2();
    init_stdin_buffer();
    cjsRequire = createRequire(import.meta.url);
  }
});

// node_modules/@mariozechner/pi-tui/dist/index.js
var init_dist = __esm({
  "node_modules/@mariozechner/pi-tui/dist/index.js"() {
    init_autocomplete();
    init_box();
    init_cancellable_loader();
    init_editor();
    init_image();
    init_input();
    init_loader();
    init_markdown();
    init_select_list();
    init_settings_list();
    init_spacer();
    init_text();
    init_truncated_text();
    init_fuzzy();
    init_keybindings();
    init_keys2();
    init_stdin_buffer();
    init_terminal();
    init_terminal_image();
    init_tui();
    init_utils();
  }
});

// pi-compat.ts
import { readFileSync as readFileSync3 } from "node:fs";
function defineTool(tool) {
  return tool;
}
function StringEnum(values, options2) {
  return typebox_exports.Unsafe({
    type: "string",
    enum: values,
    ...options2?.description ? { description: options2.description } : {},
    ...options2?.default ? { default: options2.default } : {}
  });
}
function sessionInfoChangesReachExtensions(hostEntry = process.argv[1] ?? "") {
  const normalized = hostEntry.replaceAll("\\", "/");
  return normalized.includes("/@earendil-works/pi-coding-agent/");
}
function versionAtLeast(version2, minimum) {
  const match = /^(\d+)\.(\d+)\.(\d+)/.exec(version2);
  if (!match) return false;
  const actual = [Number(match[1]), Number(match[2]), Number(match[3])];
  for (let index = 0; index < minimum.length; index += 1) {
    if (actual[index] !== minimum[index]) return actual[index] > minimum[index];
  }
  return true;
}
function readEarendilHostVersion(hostEntry) {
  const normalized = hostEntry.replaceAll("\\", "/");
  const marker = "/@earendil-works/pi-coding-agent/";
  const markerIndex = normalized.lastIndexOf(marker);
  if (markerIndex < 0) return void 0;
  const packageRoot = normalized.slice(0, markerIndex + marker.length - 1);
  try {
    const manifest = JSON.parse(readFileSync3(`${packageRoot}/package.json`, "utf8"));
    return manifest.name === "@earendil-works/pi-coding-agent" && typeof manifest.version === "string" ? manifest.version : void 0;
  } catch {
    return void 0;
  }
}
function sessionCompactFailuresReachExtensions(hostEntry = process.argv[1] ?? "", hostVersion = readEarendilHostVersion(hostEntry)) {
  const normalized = hostEntry.replaceAll("\\", "/");
  return normalized.includes("/@earendil-works/pi-coding-agent/") && typeof hostVersion === "string" && versionAtLeast(hostVersion, [0, 85, 0]);
}
var init_pi_compat = __esm({
  "pi-compat.ts"() {
    init_build();
  }
});

// session-profile.ts
function normalizeInlineText(value) {
  return value.replace(/\s+/g, " ").trim();
}
function containsUnsafeControlCharacters(value) {
  return UNSAFE_CONTROL_CHARACTERS.test(value);
}
function isValidSessionName(value) {
  return typeof value === "string" && !containsUnsafeControlCharacters(value) && !value.startsWith(RESERVED_SESSION_NAME_PREFIX);
}
function normalizeSessionDescription(value) {
  return normalizeInlineText(value);
}
function sessionDescriptionWordCount(value) {
  const normalized = normalizeSessionDescription(value);
  return normalized ? normalized.split(" ").length : 0;
}
function isValidSessionDescription(value) {
  if (typeof value !== "string") return false;
  const normalized = normalizeSessionDescription(value);
  const words = sessionDescriptionWordCount(normalized);
  return normalized === value && !containsUnsafeControlCharacters(normalized) && normalized.length <= SESSION_DESCRIPTION_MAX_LENGTH && words >= SESSION_DESCRIPTION_MIN_WORDS && words <= SESSION_DESCRIPTION_MAX_WORDS;
}
function normalizeSelfProfileUpdate(value) {
  const profile = {};
  if (value.name !== void 0) {
    const name = normalizeInlineText(value.name);
    if (!name) return { ok: false, error: "profile.name must not be empty." };
    if (containsUnsafeControlCharacters(name)) {
      return { ok: false, error: "profile.name contains unsupported control characters." };
    }
    if (name.length > SESSION_PROFILE_NAME_MAX_LENGTH) {
      return { ok: false, error: `profile.name must be at most ${SESSION_PROFILE_NAME_MAX_LENGTH} characters.` };
    }
    profile.name = name;
  }
  if (value.description === null) {
    profile.description = null;
  } else if (value.description !== void 0) {
    const description = normalizeSessionDescription(value.description);
    const words = sessionDescriptionWordCount(description);
    if (containsUnsafeControlCharacters(description)) {
      return { ok: false, error: "profile.description contains unsupported control characters." };
    }
    if (description.length > SESSION_DESCRIPTION_MAX_LENGTH) {
      return { ok: false, error: `profile.description must be at most ${SESSION_DESCRIPTION_MAX_LENGTH} characters.` };
    }
    if (words < SESSION_DESCRIPTION_MIN_WORDS || words > SESSION_DESCRIPTION_MAX_WORDS) {
      return {
        ok: false,
        error: `profile.description must contain ${SESSION_DESCRIPTION_MIN_WORDS}-${SESSION_DESCRIPTION_MAX_WORDS} words (received ${words}).`
      };
    }
    profile.description = description;
  }
  return { ok: true, profile };
}
var SESSION_DESCRIPTION_MIN_WORDS, SESSION_DESCRIPTION_MAX_WORDS, SESSION_DESCRIPTION_MAX_LENGTH, SESSION_PROFILE_NAME_MAX_LENGTH, UNSAFE_CONTROL_CHARACTERS, RESERVED_SESSION_NAME_PREFIX;
var init_session_profile = __esm({
  "session-profile.ts"() {
    SESSION_DESCRIPTION_MIN_WORDS = 5;
    SESSION_DESCRIPTION_MAX_WORDS = 9;
    SESSION_DESCRIPTION_MAX_LENGTH = 96;
    SESSION_PROFILE_NAME_MAX_LENGTH = 64;
    UNSAFE_CONTROL_CHARACTERS = /[\p{Cc}\p{Cf}]/u;
    RESERVED_SESSION_NAME_PREFIX = "oqs1.";
  }
});

// broker/federation-types.ts
var FEDERATION_IDENTITY_FEATURE, FEDERATION_SINGLE_HOP_FEATURE, FEDERATION_ROSTER_FEATURE, FEDERATION_SEND_FEATURE, FEDERATION_EXACT_SEND_FEATURE, FEDERATION_CONVERSATION_FEATURE, FEDERATION_SCOPED_LINKS_FEATURE, FEDERATION_PING_FEATURE, FEDERATION_APP_FEATURE, FEDERATION_CORRELATION_ID_MAX_LENGTH, FEDERATION_ORIGIN_ID_MAX_LENGTH, FEDERATION_SCOPE_ALIAS_MAX_LENGTH, FEDERATION_SESSION_ID_MAX_LENGTH, FEDERATION_REQUIRED_FEATURES, FEDERATION_SUPPORTED_FEATURES;
var init_federation_types = __esm({
  "broker/federation-types.ts"() {
    FEDERATION_IDENTITY_FEATURE = "peer-identity-v1";
    FEDERATION_SINGLE_HOP_FEATURE = "peer-single-hop-v1";
    FEDERATION_ROSTER_FEATURE = "peer-roster-v1";
    FEDERATION_SEND_FEATURE = "peer-send-v1";
    FEDERATION_EXACT_SEND_FEATURE = "peer-send-exact-v1";
    FEDERATION_CONVERSATION_FEATURE = "peer-conversation-text-v1";
    FEDERATION_SCOPED_LINKS_FEATURE = "peer-links-by-scope-v1";
    FEDERATION_PING_FEATURE = "peer-ping-v1";
    FEDERATION_APP_FEATURE = "peer-roster-app-v1";
    FEDERATION_CORRELATION_ID_MAX_LENGTH = 128;
    FEDERATION_ORIGIN_ID_MAX_LENGTH = 96;
    FEDERATION_SCOPE_ALIAS_MAX_LENGTH = 64;
    FEDERATION_SESSION_ID_MAX_LENGTH = 512;
    FEDERATION_REQUIRED_FEATURES = [
      FEDERATION_IDENTITY_FEATURE,
      FEDERATION_SINGLE_HOP_FEATURE
    ];
    FEDERATION_SUPPORTED_FEATURES = [
      ...FEDERATION_REQUIRED_FEATURES,
      FEDERATION_ROSTER_FEATURE,
      FEDERATION_SEND_FEATURE,
      FEDERATION_EXACT_SEND_FEATURE,
      FEDERATION_CONVERSATION_FEATURE,
      FEDERATION_SCOPED_LINKS_FEATURE,
      FEDERATION_PING_FEATURE,
      FEDERATION_APP_FEATURE
    ];
  }
});

// broker/federation-protocol.ts
function isRecord(value) {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}
function hasOnlyKeys(value, required, optional = []) {
  const keys = Object.keys(value);
  const allowed = /* @__PURE__ */ new Set([...required, ...optional]);
  return required.every((key) => Object.prototype.hasOwnProperty.call(value, key)) && keys.every((key) => allowed.has(key));
}
function isSafeText(value, maxLength, requireTrimmed = true) {
  return typeof value === "string" && value.length > 0 && value.length <= maxLength && (!requireTrimmed || value.trim() === value) && value.trim().length > 0 && !CONTROL_OR_FORMAT_CHARACTERS.test(value);
}
function isCanonicalFederationOriginId(value) {
  return typeof value === "string" && value.length <= FEDERATION_ORIGIN_ID_MAX_LENGTH && ORIGIN_ID.test(value);
}
function isCanonicalFederationScopeAlias(value) {
  return typeof value === "string" && value.length <= FEDERATION_SCOPE_ALIAS_MAX_LENGTH && SCOPE_ALIAS.test(value);
}
function isFederationCorrelationId(value) {
  return typeof value === "string" && value.length <= FEDERATION_CORRELATION_ID_MAX_LENGTH && CORRELATION_ID.test(value);
}
function isStableSessionId(value) {
  return isSafeText(value, FEDERATION_SESSION_ID_MAX_LENGTH, false);
}
function isOriginQualifiedSessionIdentity(value) {
  if (!isRecord(value) || !hasOnlyKeys(value, ["originId", "remoteScopeAlias", "remoteStableSessionId"])) return false;
  return isCanonicalFederationOriginId(value.originId) && isCanonicalFederationScopeAlias(value.remoteScopeAlias) && isStableSessionId(value.remoteStableSessionId);
}
function encodeOriginQualifiedSessionIdentity(identity) {
  if (!isOriginQualifiedSessionIdentity(identity)) throw new Error("Invalid origin-qualified session identity");
  const tuple = [identity.originId, identity.remoteScopeAlias, identity.remoteStableSessionId];
  return `${QUALIFIED_ID_PREFIX}${Buffer.from(JSON.stringify(tuple), "utf8").toString("base64url")}`;
}
function decodeOriginQualifiedSessionIdentity(value) {
  if (!value.startsWith(QUALIFIED_ID_PREFIX) || value.length > MAX_QUALIFIED_ID_LENGTH) return void 0;
  const encoded = value.slice(QUALIFIED_ID_PREFIX.length);
  if (!encoded || !/^[A-Za-z0-9_-]+$/.test(encoded)) return void 0;
  try {
    const decoded = JSON.parse(Buffer.from(encoded, "base64url").toString("utf8"));
    if (!Array.isArray(decoded) || decoded.length !== 3) return void 0;
    const identity = {
      originId: decoded[0],
      remoteScopeAlias: decoded[1],
      remoteStableSessionId: decoded[2]
    };
    if (!isOriginQualifiedSessionIdentity(identity)) return void 0;
    return encodeOriginQualifiedSessionIdentity(identity) === value ? identity : void 0;
  } catch {
    return void 0;
  }
}
var CONTROL_OR_FORMAT_CHARACTERS, ORIGIN_ID, SCOPE_ALIAS, CORRELATION_ID, QUALIFIED_ID_PREFIX, MAX_QUALIFIED_ID_LENGTH;
var init_federation_protocol = __esm({
  "broker/federation-protocol.ts"() {
    init_federation_types();
    CONTROL_OR_FORMAT_CHARACTERS = /[\p{Cc}\p{Cf}]/u;
    ORIGIN_ID = /^[a-z][a-z0-9]*(?::[a-z0-9](?:[a-z0-9-]*[a-z0-9])?)+$/;
    SCOPE_ALIAS = /^[a-z0-9](?:[a-z0-9-]*[a-z0-9])?$/;
    CORRELATION_ID = /^[A-Za-z0-9][A-Za-z0-9_-]{7,127}$/;
    QUALIFIED_ID_PREFIX = "oqs1.";
    MAX_QUALIFIED_ID_LENGTH = 8192;
  }
});

// broker/protocol.ts
function isBrokerHealthOkMessage(message, requestId) {
  if (!isRecord2(message)) return false;
  return message.type === "health_ok" && message.requestId === requestId && message.protocol === PARLEY_PROTOCOL_NAME && message.version === PARLEY_PROTOCOL_VERSION;
}
function isRecord2(value) {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}
function isMessageReceiptStatus(value) {
  return value === "receiver_received" || value === "queued" || value === "injected" || value === "acknowledged" || value === "expired" || value === "cancelled" || value === "superseded" || value === "cancellation_requested";
}
function isMessageReceipt(value) {
  if (!isRecord2(value)) {
    return false;
  }
  if (typeof value.messageId !== "string" || !isMessageReceiptStatus(value.status) || typeof value.timestamp !== "number") {
    return false;
  }
  return value.detail === void 0 || typeof value.detail === "string";
}
function isMessageControl(value) {
  if (!isRecord2(value)) {
    return false;
  }
  if (typeof value.messageId !== "string" || typeof value.timestamp !== "number") {
    return false;
  }
  if (value.action !== "cancel" && value.action !== "supersede") {
    return false;
  }
  if (value.supersededBy !== void 0 && typeof value.supersededBy !== "string") {
    return false;
  }
  return value.detail === void 0 || typeof value.detail === "string";
}
function isAttachment(value) {
  if (!isRecord2(value)) {
    return false;
  }
  if (value.type !== "file" && value.type !== "snippet" && value.type !== "context") {
    return false;
  }
  if (typeof value.name !== "string" || typeof value.content !== "string") {
    return false;
  }
  return value.language === void 0 || typeof value.language === "string";
}
function isMessageProvenance(value) {
  if (!isRecord2(value)) {
    return false;
  }
  if (value.type === "session_person") return Object.keys(value).length === 1;
  return value.type === "extension_outbox" && typeof value.extensionId === "string" && typeof value.extensionName === "string" && typeof value.requestId === "string";
}
function isPeerCompactionNotice(value) {
  if (!isRecord2(value)) return false;
  if (typeof value.peerSessionId !== "string" || value.peerName !== void 0 && typeof value.peerName !== "string" || value.requestedPeerSessionId !== void 0 && typeof value.requestedPeerSessionId !== "string" || !Number.isSafeInteger(value.generation) || value.generation < 1 || !Number.isSafeInteger(value.previousGeneration) || value.previousGeneration < 0 || value.previousGeneration >= value.generation || !Number.isSafeInteger(value.compactedAt) || value.compactedAt < 0) {
    return false;
  }
  return value.contextPct === void 0 || typeof value.contextPct === "number";
}
function isMessage(value) {
  if (!isRecord2(value)) {
    return false;
  }
  if (typeof value.id !== "string" || typeof value.timestamp !== "number") {
    return false;
  }
  for (const key of ["senderSequence", "brokerReceivedAt", "brokerDeliveredAt", "receiverReceivedAt", "injectedAt"]) {
    if (value[key] !== void 0 && typeof value[key] !== "number") {
      return false;
    }
  }
  if (value.supersedes !== void 0 && typeof value.supersedes !== "string") {
    return false;
  }
  if (value.retryOf !== void 0 && typeof value.retryOf !== "string") {
    return false;
  }
  if (value.replyTo !== void 0 && typeof value.replyTo !== "string") {
    return false;
  }
  if (value.expectsReply !== void 0 && typeof value.expectsReply !== "boolean") {
    return false;
  }
  if (value.completesAsk !== void 0 && typeof value.completesAsk !== "boolean") return false;
  if (value.completesAsk === true && !value.replyTo) return false;
  if (value.senderWaitMode !== void 0 && value.senderWaitMode !== "blocking" && value.senderWaitMode !== "nonblocking") return false;
  if (value.replyDeadline !== void 0 && (!Number.isSafeInteger(value.replyDeadline) || value.replyDeadline < 0)) return false;
  if (value.provenance !== void 0 && !isMessageProvenance(value.provenance)) {
    return false;
  }
  if (value.peerCompaction !== void 0 && !isPeerCompactionNotice(value.peerCompaction)) {
    return false;
  }
  if (value.contactToken !== void 0 && typeof value.contactToken !== "string") {
    return false;
  }
  if (value.contactBaseline !== void 0 && typeof value.contactBaseline !== "boolean") {
    return false;
  }
  if (value.contactBaseline === true && typeof value.contactToken !== "string") {
    return false;
  }
  if (!isRecord2(value.content) || typeof value.content.text !== "string") {
    return false;
  }
  return value.content.attachments === void 0 || Array.isArray(value.content.attachments) && value.content.attachments.every(isAttachment);
}
function isAppLabel(value) {
  return typeof value === "string" && value.trim().length > 0 && value.length <= 64 && !/[\p{Cc}\p{Cf}]/u.test(value);
}
function isSessionInfo(value) {
  if (!isRecord2(value)) {
    return false;
  }
  if (typeof value.id !== "string" || typeof value.cwd !== "string" || typeof value.model !== "string" || typeof value.pid !== "number" || typeof value.startedAt !== "number" || typeof value.lastActivity !== "number") {
    return false;
  }
  if (value.endpointEpoch !== void 0 && typeof value.endpointEpoch !== "string") {
    return false;
  }
  if (value.name !== void 0 && !isValidSessionName(value.name)) {
    return false;
  }
  if (value.description !== void 0 && !isValidSessionDescription(value.description)) {
    return false;
  }
  if (value.runtimeFallbackAlias !== void 0 && typeof value.runtimeFallbackAlias !== "boolean") {
    return false;
  }
  if (value.status !== void 0 && typeof value.status !== "string") {
    return false;
  }
  if (value.peerUid !== void 0 && typeof value.peerUid !== "number") {
    return false;
  }
  for (const key of ["contextPct", "contextTokens", "contextWindow"]) {
    if (value[key] !== void 0 && typeof value[key] !== "number") {
      return false;
    }
  }
  if (value.tmuxPane !== void 0 && typeof value.tmuxPane !== "string") {
    return false;
  }
  if (value.app !== void 0 && !isAppLabel(value.app)) {
    return false;
  }
  if (value.isSubagent !== void 0 && typeof value.isSubagent !== "boolean") {
    return false;
  }
  if (value.supervisorSessionId !== void 0 && typeof value.supervisorSessionId !== "string") {
    return false;
  }
  if (value.supervisorName !== void 0 && typeof value.supervisorName !== "string") {
    return false;
  }
  if (value.advertised !== void 0 && typeof value.advertised !== "boolean") {
    return false;
  }
  if (value.extensions !== void 0) {
    if (!Array.isArray(value.extensions) || value.extensions.length > 32) return false;
    for (const extension of value.extensions) {
      if (!isRecord2(extension) || Object.keys(extension).some((key) => key !== "namespace" && key !== "ownerEligible")) return false;
      if (typeof extension.namespace !== "string" || extension.namespace.length === 0 || extension.namespace.length > 64 || !/^[a-z0-9][a-z0-9._/-]*$/.test(extension.namespace) || typeof extension.ownerEligible !== "boolean") return false;
    }
  }
  if (value.federation !== void 0) {
    if (!isRecord2(value.federation)) return false;
    const keys = Object.keys(value.federation);
    if (!keys.every((key) => ["originId", "originLabel", "remoteScopeAlias", "remoteStableSessionId", "conversation", "originEpoch"].includes(key))) return false;
    if (value.federation.conversation !== void 0 && typeof value.federation.conversation !== "boolean") return false;
    if (value.federation.originEpoch !== void 0 && !isFederationCorrelationId(value.federation.originEpoch)) return false;
    if (!isCanonicalFederationOriginId(value.federation.originId) || !isCanonicalFederationScopeAlias(value.federation.remoteScopeAlias) || typeof value.federation.remoteStableSessionId !== "string" || value.federation.remoteStableSessionId.length < 1 || value.federation.remoteStableSessionId.length > 512 || /[\p{Cc}\p{Cf}]/u.test(value.federation.remoteStableSessionId) || value.federation.originLabel !== void 0 && (typeof value.federation.originLabel !== "string" || value.federation.originLabel.length < 1 || value.federation.originLabel.length > 80 || /[\p{Cc}\p{Cf}]/u.test(value.federation.originLabel))) return false;
    if (value.trustedLocal !== false || value.peerUid !== void 0 || value.isSubagent !== void 0 || value.supervisorSessionId !== void 0 || value.supervisorName !== void 0 || value.advertised !== void 0 || value.extensions !== void 0) return false;
    try {
      if (value.id !== encodeOriginQualifiedSessionIdentity({
        originId: value.federation.originId,
        remoteScopeAlias: value.federation.remoteScopeAlias,
        remoteStableSessionId: value.federation.remoteStableSessionId
      })) return false;
    } catch {
      return false;
    }
  }
  return value.trustedLocal === void 0 || typeof value.trustedLocal === "boolean";
}
var init_protocol = __esm({
  "broker/protocol.ts"() {
    init_session_profile();
    init_federation_protocol();
    init_paths();
  }
});

// config.ts
import { readFileSync as readFileSync4 } from "fs";
import { join as join4 } from "path";
function getAskTimeoutMs() {
  const raw = process.env.PI_PARLEY_ASK_TIMEOUT_MS;
  if (raw === void 0 || raw.trim() === "") {
    return DEFAULT_ASK_TIMEOUT_MS;
  }
  const value = Number(raw);
  if (!Number.isSafeInteger(value) || value <= 0) {
    throw new Error("PI_PARLEY_ASK_TIMEOUT_MS must be a positive integer number of milliseconds");
  }
  return value;
}
function getParleyScopeId(env = process.env) {
  const scopeId = env[PARLEY_SCOPE_ID_ENV]?.trim();
  return scopeId ? scopeId : void 0;
}
function getConfigPath(parleyDir = getParleyDirPath()) {
  return join4(parleyDir, "config.json");
}
function readConfigRaw() {
  try {
    return readFileSync4(getConfigPath(), "utf-8");
  } catch (error) {
    if (error.code === "ENOENT") return void 0;
    throw error;
  }
}
function readConfig(fallback) {
  let parsed;
  try {
    const raw = readConfigRaw();
    if (raw === void 0) {
      return { ...defaults, brokerArgs: [...defaults.brokerArgs] };
    }
    parsed = JSON.parse(raw);
    if (typeof parsed !== "object" || parsed === null || Array.isArray(parsed)) {
      throw new Error("Config must be a JSON object");
    }
    const parsedConfig = parsed;
    const config = { ...defaults, brokerArgs: [...defaults.brokerArgs] };
    if (Object.hasOwn(parsedConfig, "brokerCommand")) {
      if (typeof parsedConfig.brokerCommand !== "string") {
        throw new Error(`"brokerCommand" must be a string`);
      }
      const brokerCommand = parsedConfig.brokerCommand.trim();
      if (!brokerCommand) {
        throw new Error(`"brokerCommand" must not be empty`);
      }
      config.brokerCommand = brokerCommand;
    }
    if (Object.hasOwn(parsedConfig, "brokerArgs")) {
      if (!Array.isArray(parsedConfig.brokerArgs)) {
        throw new Error(`"brokerArgs" must be an array`);
      }
      const brokerArgs = [];
      for (const arg of parsedConfig.brokerArgs) {
        if (typeof arg !== "string") {
          throw new Error(`"brokerArgs" items must be strings`);
        }
        brokerArgs.push(arg);
      }
      config.brokerArgs = brokerArgs;
    }
    if (Object.hasOwn(parsedConfig, "confirmSend")) {
      if (typeof parsedConfig.confirmSend !== "boolean") {
        throw new Error(`"confirmSend" must be a boolean`);
      }
      config.confirmSend = parsedConfig.confirmSend;
    }
    for (const key of ["linkByDefault", "herdrNames"]) {
      if (!Object.hasOwn(parsedConfig, key)) continue;
      if (typeof parsedConfig[key] !== "boolean") throw new Error(`"${key}" must be a boolean`);
      config[key] = parsedConfig[key];
    }
    if (Object.hasOwn(parsedConfig, "enabled")) {
      if (typeof parsedConfig.enabled !== "boolean") {
        throw new Error(`"enabled" must be a boolean`);
      }
      config.enabled = parsedConfig.enabled;
    }
    if (Object.hasOwn(parsedConfig, "inboundTrigger")) {
      if (parsedConfig.inboundTrigger !== "always" && parsedConfig.inboundTrigger !== "replies" && parsedConfig.inboundTrigger !== "never") {
        throw new Error(`"inboundTrigger" must be "always", "replies", or "never"`);
      }
      config.inboundTrigger = parsedConfig.inboundTrigger;
    }
    if (Object.hasOwn(parsedConfig, "replyHint")) {
      if (typeof parsedConfig.replyHint !== "boolean") {
        throw new Error(`"replyHint" must be a boolean`);
      }
      config.replyHint = parsedConfig.replyHint;
    }
    if (Object.hasOwn(parsedConfig, "status")) {
      if (typeof parsedConfig.status !== "string") {
        throw new Error(`"status" must be a string`);
      }
      config.status = parsedConfig.status;
    }
    if (Object.hasOwn(parsedConfig, "stableId")) {
      if (typeof parsedConfig.stableId !== "string") {
        throw new Error(`"stableId" must be a string`);
      }
      const stableId = parsedConfig.stableId.trim();
      if (!stableId) {
        throw new Error(`"stableId" must not be empty`);
      }
      config.stableId = stableId;
    }
    if (Object.hasOwn(parsedConfig, "projectLauncher")) {
      if (typeof parsedConfig.projectLauncher !== "string") {
        throw new Error(`"projectLauncher" must be a string`);
      }
      const projectLauncher = parsedConfig.projectLauncher.trim();
      if (!projectLauncher) {
        throw new Error(`"projectLauncher" must not be empty`);
      }
      if (projectLauncher.length > 1024) {
        throw new Error(`"projectLauncher" must be at most 1024 characters`);
      }
      config.projectLauncher = projectLauncher;
    }
    return config;
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    const failure = new Error(`Failed to load parley config: ${message}`, { cause: error });
    if (fallback) return fallback(failure, parsed);
    throw failure;
  }
}
function loadRuntimeConfig(reportError) {
  return readConfig((error, parsed) => {
    reportError(error);
    const explicitlyDisabled = typeof parsed === "object" && parsed !== null && !Array.isArray(parsed) && parsed.enabled === false;
    return {
      ...defaults,
      brokerArgs: [...defaults.brokerArgs],
      enabled: !explicitlyDisabled,
      inboundTrigger: "never",
      confirmSend: true,
      linkByDefault: false
    };
  });
}
var DEFAULT_ASK_TIMEOUT_MS, PARLEY_SCOPE_ID_ENV, defaults;
var init_config2 = __esm({
  "config.ts"() {
    init_paths();
    DEFAULT_ASK_TIMEOUT_MS = 10 * 60 * 1e3;
    PARLEY_SCOPE_ID_ENV = "PI_PARLEY_SCOPE_ID";
    defaults = {
      brokerCommand: "npx",
      brokerArgs: ["--no-install", "tsx"],
      confirmSend: false,
      inboundTrigger: "always",
      enabled: true,
      replyHint: true,
      linkByDefault: true,
      herdrNames: true
    };
  }
});

// types.ts
var EXTENSION_BUS_FEATURE, EXACT_SEND_FEATURE, EXACT_IDENTITY_SEND_FEATURE, COMPACTION_AWARENESS_FEATURE, SESSION_PROFILE_FEATURE, FEDERATED_CONVERSATION_FEATURE, CONVERSATION_CONTRACT_FEATURE, PERSON_PROVENANCE_FEATURE;
var init_types2 = __esm({
  "types.ts"() {
    EXTENSION_BUS_FEATURE = "extension-bus-v1";
    EXACT_SEND_FEATURE = "exact-send-v1";
    EXACT_IDENTITY_SEND_FEATURE = "exact-identity-send-v1";
    COMPACTION_AWARENESS_FEATURE = "compaction-awareness-v1";
    SESSION_PROFILE_FEATURE = "session-profile-v1";
    FEDERATED_CONVERSATION_FEATURE = "federated-conversation-text-v1";
    CONVERSATION_CONTRACT_FEATURE = "conversation-contract-v1";
    PERSON_PROVENANCE_FEATURE = "person-provenance-v1";
  }
});

// broker/client.ts
import { EventEmitter } from "events";
import net2 from "net";
import { randomUUID } from "crypto";
function toError(error) {
  return error instanceof Error ? error : new Error(String(error));
}
function getLivenessIntervalMs() {
  const raw = Number.parseInt(process.env.PI_PARLEY_LIVENESS_INTERVAL_MS ?? "", 10);
  return Number.isFinite(raw) && raw > 0 ? raw : 3e4;
}
function getLivenessTimeoutMs() {
  const raw = Number.parseInt(process.env.PI_PARLEY_LIVENESS_TIMEOUT_MS ?? "", 10);
  return Number.isFinite(raw) && raw > 0 ? Math.min(raw, getLivenessIntervalMs()) : 5e3;
}
function connectToBrokerTarget(target2) {
  return typeof target2 === "string" ? net2.connect(target2) : net2.connect({ host: target2.host, port: target2.port });
}
var ConversationPrepareError, ParleyClient;
var init_client = __esm({
  "broker/client.ts"() {
    init_framing();
    init_paths();
    init_protocol();
    init_config2();
    init_types2();
    ConversationPrepareError = class extends Error {
      constructor(code, message, outcomeKnown = true) {
        super(message);
        this.code = code;
        this.outcomeKnown = outcomeKnown;
        this.name = "ConversationPrepareError";
      }
      code;
      outcomeKnown;
    };
    ParleyClient = class extends EventEmitter {
      socket = null;
      _sessionId = null;
      _selfSession = null;
      _features = /* @__PURE__ */ new Set();
      pendingPreparations = /* @__PURE__ */ new Map();
      pendingSends = /* @__PURE__ */ new Map();
      pendingCancellations = /* @__PURE__ */ new Map();
      pendingLists = /* @__PURE__ */ new Map();
      pendingAdvertise = /* @__PURE__ */ new Map();
      pendingCompactionReports = /* @__PURE__ */ new Map();
      nextSenderSequence = 1;
      disconnecting = false;
      disconnectError = null;
      livenessTimer = null;
      livenessInFlight = false;
      failPending(error) {
        for (const pending of this.pendingPreparations.values()) pending.reject(error);
        this.pendingPreparations.clear();
        for (const pending of this.pendingSends.values()) {
          pending.reject(error);
        }
        this.pendingSends.clear();
        for (const pending of this.pendingCancellations.values()) pending.reject(error);
        this.pendingCancellations.clear();
        for (const pending of this.pendingAdvertise.values()) {
          pending.reject(error);
        }
        this.pendingAdvertise.clear();
        for (const pending of this.pendingLists.values()) {
          pending.reject(error);
        }
        this.pendingLists.clear();
        for (const pending of this.pendingCompactionReports.values()) {
          pending.reject(error);
        }
        this.pendingCompactionReports.clear();
      }
      get sessionId() {
        return this._sessionId;
      }
      supportsFeature(feature) {
        return this._features.has(feature);
      }
      getSelfSession() {
        return this._selfSession ? { ...this._selfSession } : void 0;
      }
      invalidateSelfSessionProjection() {
        this._selfSession = null;
      }
      isConnected() {
        const socket = this.socket;
        return Boolean(socket && this._sessionId && !this.disconnecting && !socket.destroyed && !socket.writableEnded && socket.writable);
      }
      /**
       * Start the liveness heartbeat. Must be called once the connection is
       * registered. The heartbeat periodically round-trips a lightweight list
       * request and tears down the socket if the broker does not respond within
       * the liveness timeout, so a half-open connection is detected within a
       * bounded window instead of silently lingering forever.
       */
      startLivenessHeartbeat() {
        this.stopLivenessHeartbeat();
        this.livenessTimer = setInterval(() => {
          this.runLivenessProbe();
        }, getLivenessIntervalMs());
        this.livenessTimer.unref?.();
      }
      stopLivenessHeartbeat() {
        if (this.livenessTimer) {
          clearInterval(this.livenessTimer);
          this.livenessTimer = null;
        }
        this.livenessInFlight = false;
      }
      async runLivenessProbe() {
        if (this.livenessInFlight || !this.isConnected()) {
          return;
        }
        this.livenessInFlight = true;
        try {
          await this.listSessions({ timeoutMs: getLivenessTimeoutMs() });
        } catch (error) {
          const socket = this.socket;
          if (socket && !socket.destroyed) {
            this.disconnectError = toError(error);
            socket.destroy();
          }
        } finally {
          this.livenessInFlight = false;
        }
      }
      requireActiveSocket() {
        if (this.disconnecting) {
          throw new Error("Client disconnecting");
        }
        const socket = this.socket;
        if (!socket || !this._sessionId) {
          throw new Error("Not connected");
        }
        if (socket.destroyed || socket.writableEnded || !socket.writable) {
          throw new Error("Client disconnected");
        }
        return socket;
      }
      /** Establishes the current conversation and exact-target contract. Registration
       * rejects brokers that do not advertise both; other capabilities are optional. */
      connect(session, sessionId) {
        if (this.socket) {
          return Promise.reject(new Error("Already connected"));
        }
        return new Promise((resolve4, reject) => {
          let socket;
          let target2;
          try {
            target2 = getBrokerConnectTarget();
            socket = connectToBrokerTarget(target2);
          } catch (error) {
            reject(toError(error));
            return;
          }
          this.socket = socket;
          this.disconnectError = null;
          let settled = false;
          const timeout = setTimeout(() => {
            if (!this._sessionId) {
              cleanupConnectionAttempt();
              cleanupSocketListeners();
              if (this.socket === socket) {
                this.socket = null;
              }
              socket.destroy();
              reject(new Error("Connection timeout"));
            }
          }, 1e4);
          let connectionEstablished = false;
          const onRegistered = () => {
            settled = true;
            connectionEstablished = true;
            cleanupConnectionAttempt();
            this.startLivenessHeartbeat();
            resolve4();
          };
          const onError = (err) => {
            settled = true;
            cleanupConnectionAttempt();
            cleanupSocketListeners();
            if (this.socket === socket) {
              this.socket = null;
            }
            socket.destroy();
            reject(err);
          };
          const onClose = () => {
            const wasConnecting = !settled && !this._sessionId;
            const wasDisconnecting = this.disconnecting;
            const disconnectError = this.disconnectError ?? new Error("Client disconnected");
            this.disconnecting = false;
            this.stopLivenessHeartbeat();
            cleanupConnectionAttempt();
            cleanupSocketListeners();
            this.failPending(disconnectError);
            if (this.socket === socket) {
              this.socket = null;
            }
            this._sessionId = null;
            this._selfSession = null;
            this._features.clear();
            this.disconnectError = null;
            if (connectionEstablished && !wasDisconnecting) {
              this.emit("disconnected", disconnectError);
            }
            if (wasConnecting) {
              reject(new Error("Connection closed before registration"));
            }
          };
          const onSocketError = (err) => {
            if (connectionEstablished) {
              this.disconnectError = err;
              if (!socket.destroyed) {
                socket.destroy();
              }
              if (this.listenerCount("error") > 0) this.emit("error", err);
            }
          };
          const onReaderError = (error) => {
            const protocolError = new Error(`Parley protocol error: ${error.message}`, { cause: error });
            if (!connectionEstablished) {
              onError(protocolError);
              return;
            }
            this.disconnectError = protocolError;
            socket.destroy();
            if (this.listenerCount("error") > 0) this.emit("error", protocolError);
          };
          const reader = createMessageReader((msg) => {
            this.handleBrokerMessage(msg);
          }, onReaderError);
          const cleanupConnectionAttempt = () => {
            this.off("_registered", onRegistered);
            socket.off("error", onError);
            clearTimeout(timeout);
          };
          const cleanupSocketListeners = () => {
            socket.off("data", reader);
            socket.off("error", onSocketError);
            socket.off("close", onClose);
          };
          socket.on("data", reader);
          socket.on("error", onError);
          socket.on("close", onClose);
          socket.on("error", onSocketError);
          this.once("_registered", onRegistered);
          try {
            const scopeId = getParleyScopeId();
            writeMessage(socket, {
              type: "register",
              session,
              ...sessionId ? { sessionId } : {},
              ...scopeId ? { scopeId } : {},
              clientFeatures: [COMPACTION_AWARENESS_FEATURE, CONVERSATION_CONTRACT_FEATURE, EXACT_SEND_FEATURE, EXACT_IDENTITY_SEND_FEATURE, FEDERATED_CONVERSATION_FEATURE, PERSON_PROVENANCE_FEATURE],
              ...typeof target2 === "string" ? {} : { stateId: target2.stateId }
            });
          } catch (error) {
            cleanupConnectionAttempt();
            cleanupSocketListeners();
            if (this.socket === socket) {
              this.socket = null;
            }
            socket.destroy();
            reject(toError(error));
          }
        });
      }
      takePendingDelivery(message) {
        if (message.requestId !== void 0) {
          if (typeof message.requestId !== "string" || message.requestId.length === 0) {
            throw new Error("Invalid delivery requestId");
          }
          if (message.type === "delivered" && (message.cancellation === void 0 || message.delivery !== "socket_delivered") || message.type === "delivery_failed" && message.cancellation !== void 0 || message.peerCompaction !== void 0 || message.contactToken !== void 0) {
            throw new Error("Invalid cancellation acknowledgement");
          }
          const pending2 = this.pendingCancellations.get(message.requestId);
          if (!pending2) return void 0;
          if (pending2.messageId !== message.messageId) {
            throw new Error("Cancellation acknowledgement messageId does not match requestId");
          }
          this.pendingCancellations.delete(message.requestId);
          return pending2;
        }
        const pending = this.pendingSends.get(message.messageId);
        if (pending) this.pendingSends.delete(message.messageId);
        return pending;
      }
      deliveryMetadata(message) {
        const { recipient, cancellation } = message;
        if (recipient !== void 0 && !isSessionInfo(recipient)) throw new Error("Invalid delivery recipient");
        if (cancellation !== void 0 && cancellation !== "removed_from_mailbox" && cancellation !== "withdrawal_requested" && cancellation !== "not_delivered") {
          throw new Error("Invalid cancellation state");
        }
        return {
          ...recipient !== void 0 ? { recipient } : {},
          ...cancellation !== void 0 ? { cancellation } : {}
        };
      }
      handleBrokerMessage(msg) {
        if (typeof msg !== "object" || msg === null || !("type" in msg) || typeof msg.type !== "string") {
          throw new Error("Invalid broker message");
        }
        const brokerMessage = msg;
        if (this._sessionId === null && brokerMessage.type !== "registered" && brokerMessage.type !== "error") {
          throw new Error(`Received ${brokerMessage.type} before registered`);
        }
        switch (brokerMessage.type) {
          case "registered": {
            if (typeof brokerMessage.sessionId !== "string") {
              throw new Error("Invalid registered message");
            }
            if (this._sessionId !== null) {
              throw new Error("Received duplicate registered message");
            }
            if (!Array.isArray(brokerMessage.features) || !brokerMessage.features.every((feature) => typeof feature === "string")) {
              throw new Error("Invalid registered features");
            }
            const features = new Set(brokerMessage.features);
            for (const required of [CONVERSATION_CONTRACT_FEATURE, EXACT_SEND_FEATURE]) {
              if (!features.has(required)) throw new Error(`Missing required broker feature: ${required}`);
            }
            if (brokerMessage.session !== void 0 && (!isSessionInfo(brokerMessage.session) || brokerMessage.session.id !== brokerMessage.sessionId)) {
              throw new Error("Invalid registered session");
            }
            this._sessionId = brokerMessage.sessionId;
            this._selfSession = brokerMessage.session ?? null;
            this._features = features;
            const registered = {
              type: "registered",
              sessionId: brokerMessage.sessionId,
              features: [...this._features],
              ...this._selfSession ? { session: { ...this._selfSession } } : {}
            };
            this.emit("broker_message", registered);
            this.emit("_registered", registered);
            break;
          }
          case "direct_contact_recorded": {
            if (typeof brokerMessage.token !== "string") {
              throw new Error("Invalid direct_contact_recorded message");
            }
            this.emit("broker_message", { type: "direct_contact_recorded", token: brokerMessage.token });
            break;
          }
          case "direct_contact_unknown": {
            if (typeof brokerMessage.token !== "string") {
              throw new Error("Invalid direct_contact_unknown message");
            }
            this.emit("broker_message", { type: "direct_contact_unknown", token: brokerMessage.token });
            break;
          }
          case "compaction_recorded": {
            const { eventId, generation, compactedAt } = brokerMessage;
            if (typeof eventId !== "string" || !Number.isSafeInteger(generation) || generation < 1 || !Number.isSafeInteger(compactedAt) || compactedAt < 0) {
              throw new Error("Invalid compaction_recorded message");
            }
            const pending = this.pendingCompactionReports.get(eventId);
            if (!pending) return;
            this.pendingCompactionReports.delete(eventId);
            pending.resolve({
              eventId,
              generation,
              compactedAt
            });
            break;
          }
          case "compaction_record_failed": {
            const { eventId, error } = brokerMessage;
            if (typeof eventId !== "string" || typeof error !== "string") {
              throw new Error("Invalid compaction_record_failed message");
            }
            const pending = this.pendingCompactionReports.get(eventId);
            if (!pending) return;
            this.pendingCompactionReports.delete(eventId);
            pending.reject(new Error(error));
            break;
          }
          case "sessions": {
            const { requestId, sessions } = brokerMessage;
            if (typeof requestId !== "string" || !Array.isArray(sessions) || !sessions.every(isSessionInfo)) {
              throw new Error("Invalid sessions message");
            }
            const pending = this.pendingLists.get(requestId);
            if (!pending) {
              return;
            }
            this.pendingLists.delete(requestId);
            this._selfSession = sessions.find((session) => session.id === this._sessionId) ?? this._selfSession;
            this.emit("roster", sessions);
            pending.resolve(sessions);
            break;
          }
          case "advertise_result": {
            const { requestId, ok, name, error, code } = brokerMessage;
            if (typeof requestId !== "string" || typeof ok !== "boolean") {
              throw new Error("Invalid advertise_result message");
            }
            const pending = this.pendingAdvertise.get(requestId);
            if (!pending) {
              return;
            }
            this.pendingAdvertise.delete(requestId);
            if (ok && typeof name === "string" && this._selfSession) {
              this._selfSession = { ...this._selfSession, name, advertised: true };
            }
            pending.resolve({
              ok,
              ...typeof name === "string" ? { name } : {},
              ...typeof error === "string" ? { error } : {},
              ...typeof code === "string" ? { code } : {}
            });
            break;
          }
          case "message": {
            const { from, message } = brokerMessage;
            if (!isSessionInfo(from) || !isMessage(message)) {
              throw new Error("Invalid message event");
            }
            this.emit("message", from, message);
            break;
          }
          case "delivered": {
            const { messageId, delivery, retryable, outcomeKnown, peerCompaction, contactToken } = brokerMessage;
            if (typeof messageId !== "string" || delivery !== "socket_delivered" && delivery !== "queued" || retryable !== false || outcomeKnown !== true || peerCompaction !== void 0 && !isPeerCompactionNotice(peerCompaction) || contactToken !== void 0 && typeof contactToken !== "string") {
              throw new Error("Invalid delivered message");
            }
            if (brokerMessage.code !== void 0 && typeof brokerMessage.code !== "string") throw new Error("Invalid delivery code");
            const metadata = this.deliveryMetadata(brokerMessage);
            const pending = this.takePendingDelivery(brokerMessage);
            if (!pending) {
              return;
            }
            if (typeof contactToken === "string" && peerCompaction === void 0) {
              this.acknowledgeDirectContact(contactToken);
            }
            pending.resolve({
              ...metadata,
              id: messageId,
              delivered: true,
              delivery,
              retryable: false,
              outcomeKnown: true,
              ...typeof brokerMessage.code === "string" ? { code: brokerMessage.code } : {},
              ...peerCompaction !== void 0 ? { peerCompaction } : {},
              ...typeof contactToken === "string" ? { contactToken } : {}
            });
            break;
          }
          case "delivery_failed": {
            const { messageId, reason, delivery, retryable, outcomeKnown } = brokerMessage;
            if (typeof messageId !== "string" || typeof reason !== "string" || delivery !== "failed" && delivery !== "unknown" || typeof retryable !== "boolean" || outcomeKnown !== (delivery !== "unknown") || delivery === "unknown" && retryable !== false) {
              throw new Error("Invalid delivery_failed message");
            }
            if (brokerMessage.code !== void 0 && typeof brokerMessage.code !== "string") throw new Error("Invalid delivery code");
            const metadata = this.deliveryMetadata(brokerMessage);
            const pending = this.takePendingDelivery(brokerMessage);
            if (!pending) {
              return;
            }
            pending.resolve({
              ...metadata,
              id: messageId,
              delivered: false,
              reason,
              delivery,
              retryable,
              outcomeKnown,
              ...typeof brokerMessage.code === "string" ? { code: brokerMessage.code } : {}
            });
            break;
          }
          case "conversation_prepared": {
            const prepared = brokerMessage.prepared;
            if (typeof brokerMessage.requestId !== "string" || !prepared || typeof prepared.messageId !== "string" || !isSessionInfo(prepared.author) || !isSessionInfo(prepared.recipient)) throw new Error("Invalid conversation preparation");
            const pending = this.pendingPreparations.get(brokerMessage.requestId);
            this.pendingPreparations.delete(brokerMessage.requestId);
            pending?.resolve(prepared);
            break;
          }
          case "conversation_prepare_failed": {
            if (typeof brokerMessage.requestId !== "string" || typeof brokerMessage.code !== "string" || typeof brokerMessage.error !== "string" || brokerMessage.outcomeKnown !== void 0 && typeof brokerMessage.outcomeKnown !== "boolean") {
              throw new Error("Invalid conversation preparation failure");
            }
            const pending = this.pendingPreparations.get(brokerMessage.requestId);
            this.pendingPreparations.delete(brokerMessage.requestId);
            pending?.reject(new ConversationPrepareError(brokerMessage.code, brokerMessage.error, brokerMessage.outcomeKnown ?? true));
            break;
          }
          case "message_receipt": {
            if (!isSessionInfo(brokerMessage.from) || !isMessageReceipt(brokerMessage.receipt)) {
              throw new Error("Invalid message_receipt event");
            }
            this.emit("broker_message", brokerMessage);
            this.emit("message_receipt", brokerMessage.from, brokerMessage.receipt);
            break;
          }
          case "message_control": {
            if (!isSessionInfo(brokerMessage.from) || !isMessageControl(brokerMessage.control)) {
              throw new Error("Invalid message_control event");
            }
            this.emit("broker_message", brokerMessage);
            this.emit("message_control", brokerMessage.from, brokerMessage.control);
            break;
          }
          case "session_joined": {
            if (!isSessionInfo(brokerMessage.session)) {
              throw new Error("Invalid session_joined message");
            }
            if (brokerMessage.session.id === this._sessionId) this._selfSession = brokerMessage.session;
            const message = { type: "session_joined", session: brokerMessage.session };
            this.emit("broker_message", message);
            this.emit("session_joined", brokerMessage.session);
            break;
          }
          case "session_left": {
            if (typeof brokerMessage.sessionId !== "string") {
              throw new Error("Invalid session_left message");
            }
            const message = { type: "session_left", sessionId: brokerMessage.sessionId };
            this.emit("broker_message", message);
            this.emit("session_left", brokerMessage.sessionId);
            break;
          }
          case "presence_update": {
            if (!isSessionInfo(brokerMessage.session)) {
              throw new Error("Invalid presence_update message");
            }
            if (brokerMessage.session.id === this._sessionId) this._selfSession = brokerMessage.session;
            const message = { type: "presence_update", session: brokerMessage.session };
            this.emit("broker_message", message);
            this.emit("presence_update", brokerMessage.session);
            break;
          }
          case "error": {
            if (typeof brokerMessage.error !== "string") {
              throw new Error("Invalid error message");
            }
            if (this._sessionId === null) {
              throw new Error(brokerMessage.error);
            }
            this.emit("error", new Error(brokerMessage.error));
            break;
          }
          case "extension_owner": {
            const hasOwnerId = typeof brokerMessage.ownerId === "string";
            const hasOwnerEpoch = typeof brokerMessage.ownerEpoch === "string";
            if (typeof brokerMessage.namespace !== "string" || hasOwnerId !== hasOwnerEpoch || brokerMessage.ownerId !== void 0 && !hasOwnerId || brokerMessage.ownerEpoch !== void 0 && !hasOwnerEpoch) {
              throw new Error("Invalid extension_owner message");
            }
            this.emit("broker_message", brokerMessage);
            this.emit("extension_owner", brokerMessage);
            break;
          }
          case "extension_message": {
            const hasOwnerId = typeof brokerMessage.ownerId === "string";
            const hasOwnerEpoch = typeof brokerMessage.ownerEpoch === "string";
            if (typeof brokerMessage.namespace !== "string" || typeof brokerMessage.fromSessionId !== "string" || hasOwnerId !== hasOwnerEpoch || brokerMessage.ownerId !== void 0 && !hasOwnerId || brokerMessage.ownerEpoch !== void 0 && !hasOwnerEpoch) {
              throw new Error("Invalid extension_message");
            }
            this.emit("broker_message", brokerMessage);
            this.emit("extension_message", brokerMessage);
            break;
          }
          case "extension_state": {
            if (typeof brokerMessage.namespace !== "string" || !Number.isSafeInteger(brokerMessage.revision) || Number(brokerMessage.revision) < 0) {
              throw new Error("Invalid extension_state");
            }
            this.emit("broker_message", brokerMessage);
            this.emit("extension_state", brokerMessage);
            break;
          }
          case "extension_state_result": {
            if (typeof brokerMessage.namespace !== "string" || typeof brokerMessage.committed !== "boolean" || !Number.isSafeInteger(brokerMessage.revision) || Number(brokerMessage.revision) < 0 || brokerMessage.reason !== void 0 && typeof brokerMessage.reason !== "string") {
              throw new Error("Invalid extension_state_result");
            }
            this.emit("broker_message", brokerMessage);
            this.emit("extension_state_result", brokerMessage);
            break;
          }
          default:
            throw new Error(`Unknown broker message type: ${brokerMessage.type}`);
        }
      }
      async disconnect() {
        const socket = this.socket;
        if (!socket) {
          return;
        }
        this.disconnecting = true;
        this.disconnectError = null;
        this.stopLivenessHeartbeat();
        this.failPending(new Error("Client disconnected"));
        await new Promise((resolve4) => {
          let settled = false;
          const finish = () => {
            if (settled) {
              return;
            }
            settled = true;
            clearTimeout(timeout);
            socket.off("close", onClose);
            socket.off("error", onError);
            resolve4();
          };
          const onClose = () => finish();
          const onError = () => {
            socket.destroy();
          };
          const timeout = setTimeout(() => {
            socket.destroy();
          }, 2e3);
          socket.once("close", onClose);
          socket.once("error", onError);
          try {
            writeMessage(socket, { type: "unregister" });
            socket.end();
          } catch {
            socket.destroy();
          }
        });
      }
      updateExtensionCapabilities(extensions) {
        if (!this.supportsFeature(EXTENSION_BUS_FEATURE)) return;
        const socket = this.requireActiveSocket();
        writeMessage(socket, { type: "extension_capabilities_update", extensions: extensions ?? [] });
      }
      // ACL fork: lets a subagent self-promote to full main-level visibility
      // under a chosen name. Broker-authoritative: the broker validates
      // eligibility (must currently be a restricted subagent) and name
      // uniqueness (including against live session IDs) before applying it, so
      // a client can never simply assert `advertised: true`.
      advertise(name, options2 = {}) {
        let socket;
        try {
          socket = this.requireActiveSocket();
        } catch (error) {
          return Promise.reject(toError(error));
        }
        return new Promise((resolve4, reject) => {
          const requestId = randomUUID();
          const wrappedResolve = (result) => {
            clearTimeout(timeout);
            resolve4(result);
          };
          const wrappedReject = (error) => {
            clearTimeout(timeout);
            reject(error);
          };
          const timeout = setTimeout(() => {
            if (this.pendingAdvertise.has(requestId)) {
              this.pendingAdvertise.delete(requestId);
              wrappedReject(new Error("Advertise timeout"));
            }
          }, options2.timeoutMs ?? 5e3);
          this.pendingAdvertise.set(requestId, { resolve: wrappedResolve, reject: wrappedReject });
          try {
            writeMessage(socket, { type: "advertise", requestId, name });
          } catch (error) {
            clearTimeout(timeout);
            this.pendingAdvertise.delete(requestId);
            reject(toError(error));
          }
        });
      }
      listSessions(options2 = {}) {
        let socket;
        try {
          socket = this.requireActiveSocket();
        } catch (error) {
          return Promise.reject(toError(error));
        }
        return new Promise((resolve4, reject) => {
          const requestId = randomUUID();
          const wrappedResolve = (sessions) => {
            clearTimeout(timeout);
            resolve4(sessions);
          };
          const wrappedReject = (error) => {
            clearTimeout(timeout);
            reject(error);
          };
          const timeout = setTimeout(() => {
            if (this.pendingLists.has(requestId)) {
              this.pendingLists.delete(requestId);
              wrappedReject(new Error("List sessions timeout"));
            }
          }, options2.timeoutMs ?? 5e3);
          this.pendingLists.set(requestId, { resolve: wrappedResolve, reject: wrappedReject });
          try {
            writeMessage(socket, { type: "list", requestId });
          } catch (error) {
            clearTimeout(timeout);
            this.pendingLists.delete(requestId);
            reject(toError(error));
          }
        });
      }
      /** Allocate/recheck the canonical retained identity BEFORE installing reply waiters.
       * No message is dispatched. Send the returned recipient snapshot and messageId;
       * the broker binds both author and recipient incarnations again at delivery.
       * A supplied qualified ID stays unchanged (including on failed rechecks).
       * A retained scalar ID is checked before conversion; earlier uncertainty is
       * reported through ConversationPrepareError.outcomeKnown, never rearmed. */
      async prepareConversation(recipient, options2 = {}) {
        const recheck = options2.messageId !== void 0;
        const unavailable = (error) => error instanceof ConversationPrepareError ? error : new ConversationPrepareError("E_PREPARE_TRANSPORT", `Conversation preflight unavailable; no new delivery was attempted: ${toError(error).message}`, !recheck);
        let socket;
        try {
          socket = this.requireActiveSocket();
        } catch (error) {
          throw unavailable(error);
        }
        const requestId = randomUUID();
        return new Promise((resolve4, reject) => {
          const timeout = setTimeout(() => {
            this.pendingPreparations.delete(requestId);
            reject(new ConversationPrepareError("E_PREPARE_TIMEOUT", "Conversation preflight timed out; no new delivery was attempted", !recheck));
          }, options2.timeoutMs ?? 5e3);
          this.pendingPreparations.set(requestId, {
            resolve: (result) => {
              clearTimeout(timeout);
              resolve4(result);
            },
            reject: (error) => {
              clearTimeout(timeout);
              reject(unavailable(error));
            }
          });
          try {
            writeMessage(socket, {
              type: "prepare_conversation",
              requestId,
              to: typeof recipient === "string" ? recipient : recipient.id,
              ...typeof recipient !== "string" && recipient.endpointEpoch ? { targetEpoch: recipient.endpointEpoch } : {},
              ...options2.messageId !== void 0 ? { messageId: options2.messageId } : {}
            });
          } catch (error) {
            this.pendingPreparations.delete(requestId);
            clearTimeout(timeout);
            reject(unavailable(error));
          }
        });
      }
      async send(to, options2) {
        return this.sendInternal(to, options2);
      }
      /**
       * Send to a session from a caller-owned roster snapshot without listing the
       * roster again. Multi-recipient callers can therefore expand visibility once,
       * while every recipient still gets an ordinary message ID, delivery record,
       * receipt route, rate-limit charge, and exact-endpoint rebound check. An
       * endpoint epoch supplied by the caller is never silently re-resolved.
       * Snapshots without an epoch, or whose remote peer cannot enforce endpoint
       * pins, fail closed. Ordinary send() discovery can use legacy federation.
       */
      async sendToSession(session, options2) {
        const retainedIdentity = options2.messageId !== void 0;
        if (!session.endpointEpoch) {
          const outcomeKnown = options2.messageId === void 0;
          return {
            id: options2.messageId ?? randomUUID(),
            delivered: false,
            delivery: outcomeKnown ? "failed" : "unknown",
            outcomeKnown,
            retryable: false,
            code: "E_INVALID_TARGET",
            reason: "Session snapshot has no endpoint epoch; exact delivery cannot be guaranteed"
          };
        }
        if (session.federation?.conversation && !options2.messageId?.startsWith("oqm1.")) {
          try {
            const prepared = await this.prepareConversation(session, { messageId: options2.messageId });
            options2 = { ...options2, messageId: prepared.messageId };
          } catch (error) {
            const outcomeKnown = error instanceof ConversationPrepareError ? error.outcomeKnown : options2.messageId === void 0;
            return {
              id: options2.messageId ?? randomUUID(),
              delivered: false,
              delivery: outcomeKnown ? "failed" : "unknown",
              outcomeKnown,
              retryable: false,
              code: error instanceof ConversationPrepareError ? error.code : "E_CONVERSATION_PREPARE",
              reason: toError(error).message
            };
          }
        }
        return this.sendInternal(session.id, options2, { id: session.id, epoch: session.endpointEpoch }, retainedIdentity);
      }
      async sendInternal(to, options2, rosterTarget, retainedIdentity = options2.messageId !== void 0) {
        let messageId = options2.messageId ?? randomUUID();
        const failedBeforeSend = (error, code = "E_NOT_CONNECTED") => {
          const preparation = error instanceof ConversationPrepareError;
          const outcomeKnown = preparation ? error.outcomeKnown : !retainedIdentity;
          return {
            id: messageId,
            delivered: false,
            delivery: outcomeKnown ? "failed" : "unknown",
            outcomeKnown,
            retryable: !preparation && outcomeKnown,
            code,
            reason: toError(error).message
          };
        };
        let socket;
        try {
          socket = this.requireActiveSocket();
        } catch (error) {
          return failedBeforeSend(error);
        }
        const message = {
          id: messageId,
          timestamp: Date.now(),
          senderSequence: this.nextSenderSequence++,
          supersedes: options2.supersedes,
          retryOf: options2.retryOf,
          replyTo: options2.replyTo,
          completesAsk: options2.completesAsk,
          expectsReply: options2.expectsReply,
          senderWaitMode: options2.senderWaitMode,
          provenance: options2.provenance,
          content: { text: options2.text, attachments: options2.attachments }
        };
        const cancelledResult = () => failedBeforeSend("Send cancelled before the next delivery attempt", "E_CANCELLED");
        const unknownResult = (error, code = "E_DELIVERY_UNKNOWN") => ({
          id: messageId,
          delivered: false,
          delivery: "unknown",
          outcomeKnown: false,
          retryable: false,
          code,
          reason: `${toError(error).message}; delivery may have occurred. The message has not been withdrawn.`
        });
        const sendOnce = (targetId, targetEpoch, offlineIdentity = false) => {
          if (options2.signal?.aborted) return Promise.resolve(cancelledResult());
          if (this.pendingSends.has(messageId)) return Promise.resolve(unknownResult("This message ID already has an in-flight send", "E_MESSAGE_IN_FLIGHT"));
          return new Promise((resolve4) => {
            const wrappedResolve = (result) => {
              clearTimeout(timeout);
              resolve4(result);
            };
            const wrappedReject = (error) => wrappedResolve(unknownResult(error));
            const timeout = setTimeout(() => {
              this.pendingSends.delete(messageId);
              wrappedResolve(unknownResult("Send acknowledgement timed out", "E_SEND_TIMEOUT"));
            }, options2.timeoutMs ?? 1e4);
            this.pendingSends.set(messageId, { resolve: wrappedResolve, reject: wrappedReject });
            try {
              writeMessage(socket, {
                type: "send",
                to,
                message,
                ...offlineIdentity ? { targetId: to, targetMode: "identity" } : targetId && targetEpoch ? { targetId, targetEpoch, targetMode: rosterTarget === void 0 && !messageId.startsWith("oqm1.") ? "resolved" : "snapshot" } : {},
                ...options2.contactKind ? { contactKind: options2.contactKind } : {}
              });
            } catch (error) {
              this.pendingSends.delete(messageId);
              wrappedReject(toError(error));
            }
          });
        };
        if (options2.replyTo && !options2.exactIdentity && rosterTarget === void 0 && !options2.replyTo.startsWith("oqm1.") && !to.startsWith("oqs1.")) return sendOnce();
        const resolveTarget = async () => {
          const sessions = await this.listSessions();
          const byId = sessions.find((session) => session.id === to);
          const byName = byId || options2.exactIdentity ? [] : sessions.filter((session) => session.name?.toLowerCase() === to.toLowerCase());
          const byPrefix = byId || options2.exactIdentity || byName.length > 0 ? [] : sessions.filter((session) => session.id.startsWith(to));
          const matches = byId ? [byId] : byName.length > 0 ? byName : byPrefix;
          const target2 = matches.length === 1 ? matches[0] : null;
          return target2?.endpointEpoch ? { id: target2.id, epoch: target2.endpointEpoch, session: target2 } : null;
        };
        try {
          const target2 = rosterTarget === void 0 ? await resolveTarget() : rosterTarget;
          if (options2.signal?.aborted) return cancelledResult();
          if (!target2) {
            if (options2.exactIdentity && !to.startsWith("oqs1.")) {
              if (!this.supportsFeature(EXACT_IDENTITY_SEND_FEATURE)) {
                return failedBeforeSend("This broker cannot address an exact offline session; no send frame was written by this attempt", "E_EXACT_IDENTITY_UNSUPPORTED");
              }
              return sendOnce(void 0, void 0, true);
            }
            return sendOnce();
          }
          if (target2.session?.federation?.conversation && !messageId.startsWith("oqm1.")) {
            try {
              const prepared = await this.prepareConversation(target2.session, { messageId: options2.messageId });
              messageId = prepared.messageId;
              message.id = messageId;
              return sendOnce(prepared.recipient.id, prepared.recipient.endpointEpoch);
            } catch (error) {
              return failedBeforeSend(error, error instanceof ConversationPrepareError ? error.code : "E_CONVERSATION_PREPARE");
            }
          }
          const result = await sendOnce(target2.id, target2.epoch);
          if (rosterTarget !== void 0 || messageId.startsWith("oqm1.") || result.code !== "E_TARGET_REBOUND" || !result.outcomeKnown || options2.signal?.aborted) return result;
          const reboundTarget = await resolveTarget();
          if (options2.signal?.aborted) return cancelledResult();
          return reboundTarget ? sendOnce(reboundTarget.id, reboundTarget.epoch) : result;
        } catch (error) {
          return failedBeforeSend(error, "E_TARGET_RESOLUTION");
        }
      }
      /** Withdraw a message. A live endpoint can only receive a withdrawal notice;
       * already-executed work cannot be retracted. An unknown result retains the ID. */
      cancelMessage(messageId, options2 = {}) {
        let socket;
        try {
          socket = this.requireActiveSocket();
        } catch (error) {
          return Promise.resolve({
            id: messageId,
            delivered: false,
            delivery: "failed",
            outcomeKnown: true,
            retryable: true,
            code: "E_NOT_CONNECTED",
            reason: toError(error).message
          });
        }
        const requestId = randomUUID();
        return new Promise((resolve4) => {
          const wrappedResolve = (result) => {
            clearTimeout(timeout);
            resolve4(result);
          };
          const wrappedReject = (error) => wrappedResolve({
            id: messageId,
            delivered: false,
            delivery: "unknown",
            outcomeKnown: false,
            retryable: false,
            code: "E_CANCELLATION_UNKNOWN",
            reason: `${error.message}; withdrawal may or may not have reached the recipient.`
          });
          const timeout = setTimeout(() => {
            this.pendingCancellations.delete(requestId);
            wrappedReject(new Error("Cancel acknowledgement timed out"));
          }, options2.timeoutMs ?? 1e4);
          this.pendingCancellations.set(requestId, { messageId, resolve: wrappedResolve, reject: wrappedReject });
          try {
            writeMessage(socket, { type: "cancel_message", messageId, requestId });
          } catch (error) {
            this.pendingCancellations.delete(requestId);
            wrappedReject(toError(error));
          }
        });
      }
      sendMessageReceipt(receipt) {
        if (this.disconnecting) {
          return;
        }
        const socket = this.socket;
        if (!socket || !this._sessionId || socket.destroyed || socket.writableEnded || !socket.writable) {
          return;
        }
        writeMessage(socket, { type: "message_receipt", receipt });
      }
      cancelAsk(messageId) {
        if (this.disconnecting) {
          return;
        }
        const socket = this.socket;
        if (!socket || !this._sessionId || socket.destroyed || socket.writableEnded || !socket.writable) {
          return;
        }
        try {
          writeMessage(socket, { type: "cancel_ask", messageId });
        } catch {
        }
      }
      acknowledgeSendContact(result) {
        if (result.contactToken) this.acknowledgeDirectContact(result.contactToken);
      }
      acknowledgeMessageContact(message) {
        if (message.contactToken) this.acknowledgeDirectContact(message.contactToken);
      }
      acknowledgeContactToken(token) {
        if (token) this.acknowledgeDirectContact(token);
      }
      acknowledgeDirectContact(token) {
        if (!this.supportsFeature(COMPACTION_AWARENESS_FEATURE) || this.disconnecting) return;
        const socket = this.socket;
        if (!socket || !this._sessionId || socket.destroyed || socket.writableEnded || !socket.writable) return;
        try {
          writeMessage(socket, { type: "direct_contact_seen", token });
        } catch {
        }
      }
      async reportCompactionCompleted(eventId = randomUUID()) {
        if (!eventId) throw new Error("Compaction event ID is required");
        if (!this.supportsFeature(COMPACTION_AWARENESS_FEATURE) || this.disconnecting) {
          throw new Error("Compaction awareness is unavailable");
        }
        const socket = this.requireActiveSocket();
        if (this.pendingCompactionReports.has(eventId)) {
          throw new Error(`Compaction event ${eventId} is already pending`);
        }
        return new Promise((resolve4, reject) => {
          const timeout = setTimeout(() => {
            const pending = this.pendingCompactionReports.get(eventId);
            if (!pending) return;
            this.pendingCompactionReports.delete(eventId);
            pending.reject(new Error("Compaction persistence acknowledgement timed out"));
          }, 1e4);
          timeout.unref?.();
          this.pendingCompactionReports.set(eventId, {
            resolve: (result) => {
              clearTimeout(timeout);
              resolve4(result);
            },
            reject: (error) => {
              clearTimeout(timeout);
              reject(error);
            }
          });
          try {
            writeMessage(socket, { type: "compaction_completed", eventId });
          } catch (error) {
            clearTimeout(timeout);
            this.pendingCompactionReports.delete(eventId);
            reject(toError(error));
          }
        });
      }
      updatePresence(updates) {
        if (this.disconnecting) {
          return;
        }
        const socket = this.socket;
        if (!socket || !this._sessionId || socket.destroyed || socket.writableEnded || !socket.writable) {
          return;
        }
        if (this._selfSession && updates.description !== void 0) {
          if (updates.description === null) {
            const { description: _description, ...session } = this._selfSession;
            this._selfSession = session;
          } else {
            this._selfSession = { ...this._selfSession, description: updates.description };
          }
        }
        writeMessage(socket, { type: "presence", ...updates });
      }
      sendExtensionMessage(message) {
        if (!this.supportsFeature(EXTENSION_BUS_FEATURE)) {
          throw new Error(`Connected broker does not support ${EXTENSION_BUS_FEATURE}`);
        }
        const socket = this.requireActiveSocket();
        writeMessage(socket, message);
      }
      onBrokerMessage(handler) {
        this.on("broker_message", handler);
        return () => this.off("broker_message", handler);
      }
      onMessageReceipt(handler) {
        this.on("message_receipt", handler);
        return () => this.off("message_receipt", handler);
      }
      onMessageControl(handler) {
        this.on("message_control", handler);
        return () => this.off("message_control", handler);
      }
    };
  }
});

// broker/process-lock.ts
import { randomUUID as randomUUID2 } from "node:crypto";
import {
  closeSync,
  constants,
  fstatSync,
  mkdirSync as mkdirSync3,
  openSync,
  readSync,
  writeFileSync as writeFileSync2
} from "node:fs";
import { createRequire as createRequire2 } from "node:module";
import path from "node:path";
function nativeLocks() {
  loadedNativeLocks ??= createRequire2(import.meta.url)("fs-native-extensions");
  return loadedNativeLocks;
}
function tryAcquireProcessLock(directory) {
  mkdirSync3(directory, { recursive: true, mode: 448 });
  const fd = openSync(path.join(directory, ".process.lock"), constants.O_RDWR | constants.O_CREAT, 384);
  let retained = false;
  try {
    if (!fstatSync(fd).isFile()) throw new Error("Process lock must be a regular file");
    if (!nativeLocks().tryLock(fd)) {
      return Object.freeze({ status: "occupied", owner: readDiagnosticOwner(directory) });
    }
    const owner = Object.freeze({
      pid: process.pid,
      claimId: randomUUID2(),
      acquiredAt: (/* @__PURE__ */ new Date()).toISOString()
    });
    writeFileSync2(path.join(directory, ".process.owner.json"), `${JSON.stringify(owner)}
`, { mode: 384 });
    let descriptor = fd;
    const lease = Object.freeze({
      owner,
      release() {
        if (descriptor === null) return;
        const ownedDescriptor = descriptor;
        descriptor = null;
        try {
          nativeLocks().unlock(ownedDescriptor);
        } finally {
          closeSync(ownedDescriptor);
        }
      }
    });
    retained = true;
    return Object.freeze({ status: "acquired", lease });
  } finally {
    if (!retained) closeSync(fd);
  }
}
function readDiagnosticOwner(directory) {
  let fd = null;
  try {
    fd = openSync(path.join(directory, ".process.owner.json"), "r");
    const bytes = Buffer.alloc(4096);
    const count = readSync(fd, bytes, 0, bytes.length, 0);
    if (count === 0 || count === bytes.length) return null;
    const value = JSON.parse(bytes.subarray(0, count).toString("utf8"));
    if (typeof value !== "object" || value === null) return null;
    const owner = value;
    if (typeof owner.pid !== "number" || !Number.isSafeInteger(owner.pid) || owner.pid <= 0 || typeof owner.claimId !== "string" || !/^[0-9a-f-]{36}$/.test(owner.claimId) || typeof owner.acquiredAt !== "string" || !Number.isFinite(Date.parse(owner.acquiredAt))) return null;
    return Object.freeze({ pid: owner.pid, claimId: owner.claimId, acquiredAt: owner.acquiredAt });
  } catch {
    return null;
  } finally {
    if (fd !== null) {
      try {
        closeSync(fd);
      } catch {
      }
    }
  }
}
var loadedNativeLocks;
var init_process_lock = __esm({
  "broker/process-lock.ts"() {
  }
});

// broker/runtime-claim.ts
var BROKER_RUNTIME_OCCUPIED_EXIT_CODE;
var init_runtime_claim = __esm({
  "broker/runtime-claim.ts"() {
    init_paths();
    init_framing();
    init_protocol();
    init_process_lock();
    BROKER_RUNTIME_OCCUPIED_EXIT_CODE = 75;
  }
});

// broker/spawn.ts
import { spawn } from "child_process";
import { existsSync as existsSync3, writeFileSync as writeFileSync3 } from "fs";
import { join as join5, dirname as dirname2 } from "path";
import { fileURLToPath as fileURLToPath2 } from "url";
import { createRequire as createRequire3 } from "module";
import net3 from "net";
import { randomUUID as randomUUID3 } from "crypto";
function sleep(ms) {
  return new Promise((resolve4) => setTimeout(resolve4, ms));
}
function getTsxCliPath(extensionDir = EXTENSION_DIR) {
  try {
    const requireFromExtension = createRequire3(join5(extensionDir, "package.json"));
    const tsxMain = requireFromExtension.resolve("tsx");
    return join5(dirname2(tsxMain), "cli.mjs");
  } catch {
    const siblingTsxCli = join5(extensionDir, "..", "tsx", "dist", "cli.mjs");
    if (existsSync3(siblingTsxCli)) {
      return siblingTsxCli;
    }
    return join5(extensionDir, "node_modules", "tsx", "dist", "cli.mjs");
  }
}
function quoteWindowsArg(value) {
  return `"${value.replace(/"/g, '""')}"`;
}
function getWindowsHiddenLauncherPath(parleyDir = PARLEY_DIR) {
  return join5(parleyDir, "broker-launch.vbs");
}
function usesDefaultBrokerCommand(brokerCommand, brokerArgs) {
  return brokerCommand === "npx" && brokerArgs.length === 2 && brokerArgs[0] === "--no-install" && brokerArgs[1] === "tsx";
}
function getNodeCommand(nodePath) {
  const executableName = nodePath.split(/[\\/]/).pop();
  return executableName && /^node(?:js)?(?:\.exe)?$/i.test(executableName) ? nodePath : "node";
}
function getWindowsBrokerCommandLine(brokerPath, extensionDir = EXTENSION_DIR, nodePath = process.execPath, brokerCommand = "npx", brokerArgs = ["--no-install", "tsx"]) {
  if (usesDefaultBrokerCommand(brokerCommand, brokerArgs)) {
    return [quoteWindowsArg(getNodeCommand(nodePath)), quoteWindowsArg(getTsxCliPath(extensionDir)), quoteWindowsArg(brokerPath)].join(" ");
  }
  return [quoteWindowsArg(brokerCommand), ...brokerArgs.map(quoteWindowsArg), quoteWindowsArg(brokerPath)].join(" ");
}
function getWindowsHiddenLauncherScript(commandLine) {
  return [
    'Set WshShell = CreateObject("WScript.Shell")',
    `WshShell.Run "${commandLine.replace(/"/g, '""')}", 0, False`,
    "Set WshShell = Nothing",
    ""
  ].join("\r\n");
}
function writeWindowsHiddenLauncher(commandLine, launcherPath = getWindowsHiddenLauncherPath()) {
  ensureParleyRuntimeDir(dirname2(launcherPath));
  writeFileSync3(launcherPath, `\uFEFF${getWindowsHiddenLauncherScript(commandLine)}`, {
    encoding: "utf16le",
    mode: PARLEY_RUNTIME_FILE_MODE
  });
  restrictParleyRuntimeFile(launcherPath);
  return launcherPath;
}
function getBrokerLaunchSpec(brokerPath, brokerCommand, brokerArgs, extensionDir = EXTENSION_DIR, platform = process.platform, parleyDir = PARLEY_DIR, nodePath = process.execPath) {
  if (platform === "win32") {
    const launcherPath = getWindowsHiddenLauncherPath(parleyDir);
    return {
      kind: "windows-launcher",
      command: "wscript.exe",
      args: ["//E:VBScript", launcherPath],
      launcherPath,
      launcherCommandLine: getWindowsBrokerCommandLine(brokerPath, extensionDir, nodePath, brokerCommand, brokerArgs),
      captureStartupStderr: false
    };
  }
  if (usesDefaultBrokerCommand(brokerCommand, brokerArgs)) {
    return {
      kind: "direct",
      command: getNodeCommand(nodePath),
      args: [getTsxCliPath(extensionDir), brokerPath],
      captureStartupStderr: true
    };
  }
  return {
    kind: "direct",
    command: brokerCommand,
    args: [...brokerArgs, brokerPath],
    captureStartupStderr: false
  };
}
function getBrokerSpawnOptions(extensionDir = EXTENSION_DIR, env = process.env, captureStderr = true) {
  return {
    detached: true,
    stdio: captureStderr ? ["ignore", "ignore", "pipe"] : "ignore",
    cwd: extensionDir,
    env: { ...env, PI_CODING_AGENT_DIR: getAgentDirPath(env), NODE_NO_WARNINGS: "1" },
    windowsHide: true
  };
}
function toError2(error) {
  return error instanceof Error ? error : new Error(String(error));
}
async function spawnBrokerIfNeeded(brokerCommand, brokerArgs) {
  ensureParleyRuntimeDir(PARLEY_DIR);
  if (await isBrokerRunning()) return;
  const startup = process.versions.bun ? { status: "acquired", lease: { release() {
  } } } : tryAcquireProcessLock(BROKER_STARTUP_LOCK_DIR);
  if (startup.status === "occupied") {
    await waitForBroker();
    return;
  }
  try {
    if (await isBrokerRunning()) return;
    const bundledBroker = bundledScript("broker");
    const brokerPath = bundledBroker ?? join5(dirname2(fileURLToPath2(import.meta.url)), "broker.ts");
    const launch = bundledBroker ? process.platform === "win32" ? {
      kind: "windows-launcher",
      command: "wscript.exe",
      args: ["//E:VBScript", getWindowsHiddenLauncherPath(PARLEY_DIR)],
      launcherPath: getWindowsHiddenLauncherPath(PARLEY_DIR),
      launcherCommandLine: [nodeCommand(), bundledBroker].map(quoteWindowsArg).join(" "),
      captureStartupStderr: false
    } : { kind: "direct", command: nodeCommand(), args: [bundledBroker], captureStartupStderr: true } : getBrokerLaunchSpec(brokerPath, brokerCommand, brokerArgs);
    if (launch.kind === "windows-launcher") {
      writeWindowsHiddenLauncher(launch.launcherCommandLine, launch.launcherPath);
    }
    const child = spawn(launch.command, launch.args, getBrokerSpawnOptions(EXTENSION_DIR, process.env, launch.captureStartupStderr));
    let brokerStderr = "";
    const rememberBrokerStderr = (chunk) => {
      brokerStderr = `${brokerStderr}${chunk.toString()}`.slice(-BROKER_STARTUP_STDERR_LIMIT);
    };
    const brokerStartupError = (message, cause) => {
      const stderr = brokerStderr.trim();
      const errorMessage = stderr ? `${message}
Broker stderr:
${stderr}` : message;
      return cause === void 0 ? new Error(errorMessage) : new Error(errorMessage, { cause });
    };
    child.stderr?.on("data", rememberBrokerStderr);
    child.stderr?.unref?.();
    child.unref();
    await new Promise((resolve4, reject) => {
      const cleanup = () => {
        child.stderr?.off("data", rememberBrokerStderr);
        child.stderr?.resume();
        child.off("error", onError);
        child.off("close", onExit);
      };
      const onError = (error) => {
        cleanup();
        reject(brokerStartupError(`Failed to spawn parley broker: ${error.message}`, error));
      };
      const onExit = (code, signal) => {
        if (signal === null && (launch.kind === "windows-launcher" && code === 0 || code === BROKER_RUNTIME_OCCUPIED_EXIT_CODE)) {
          return;
        }
        cleanup();
        if (signal) {
          reject(brokerStartupError(`Parley broker exited before startup with signal ${signal}`));
          return;
        }
        reject(brokerStartupError(`Parley broker exited before startup with code ${code ?? "unknown"}`));
      };
      child.once("error", onError);
      child.once("close", onExit);
      waitForBroker().then(() => {
        cleanup();
        resolve4();
      }, (error) => {
        cleanup();
        const startupError = toError2(error);
        reject(brokerStartupError(startupError.message, startupError));
      });
    });
  } finally {
    startup.lease.release();
  }
}
function isBrokerRunning() {
  return checkSocketConnectable();
}
function connectToBrokerTarget2(target2) {
  return typeof target2 === "string" ? net3.connect(target2) : net3.connect({ host: target2.host, port: target2.port });
}
function checkSocketConnectable() {
  return new Promise((resolve4) => {
    let target2;
    try {
      target2 = getBrokerConnectTarget();
    } catch {
      resolve4(false);
      return;
    }
    const socket = connectToBrokerTarget2(target2);
    const requestId = randomUUID3();
    const expectedStateId = typeof target2 === "string" ? void 0 : target2.stateId;
    let settled = false;
    const finish = (isConnected) => {
      if (settled) {
        return;
      }
      settled = true;
      clearTimeout(timeout);
      socket.off("connect", onConnect);
      socket.off("error", onError);
      socket.off("data", reader);
      socket.destroy();
      resolve4(isConnected);
    };
    const onConnect = () => {
      try {
        writeMessage(socket, {
          type: "health",
          requestId,
          ...expectedStateId ? { stateId: expectedStateId } : {}
        });
      } catch {
        finish(false);
      }
    };
    const onError = () => finish(false);
    const reader = createMessageReader((message) => {
      finish(isBrokerHealthOkMessage(message, requestId));
    }, () => finish(false));
    socket.on("connect", onConnect);
    socket.on("error", onError);
    socket.on("data", reader);
    const timeout = setTimeout(() => finish(false), 1e3);
  });
}
async function waitForBroker(timeoutMs = 5e3) {
  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    if (await checkSocketConnectable()) {
      return;
    }
    await sleep(100);
  }
  throw new Error("Broker failed to start within timeout");
}
var PARLEY_DIR, EXTENSION_DIR, BROKER_STARTUP_LOCK_DIR, BROKER_STARTUP_STDERR_LIMIT;
var init_spawn = __esm({
  "broker/spawn.ts"() {
    init_framing();
    init_paths();
    init_process_lock();
    init_protocol();
    init_runtime_claim();
    init_bundle();
    PARLEY_DIR = getParleyDirPath();
    EXTENSION_DIR = join5(dirname2(fileURLToPath2(import.meta.url)), "..");
    BROKER_STARTUP_LOCK_DIR = join5(PARLEY_DIR, "broker.startup");
    BROKER_STARTUP_STDERR_LIMIT = 4e3;
  }
});

// ui/session-list.ts
function middleTruncate(text, maxWidth) {
  if (visibleWidth(text) <= maxWidth) {
    return text;
  }
  if (maxWidth <= 3) {
    return truncateToWidth(text, maxWidth, "");
  }
  const chars = [...text];
  const targetSideWidth = Math.max(1, Math.floor((maxWidth - 1) / 2));
  let left = "";
  for (const char of chars) {
    if (visibleWidth(left + char) > targetSideWidth) break;
    left += char;
  }
  let right = "";
  for (const char of chars.slice().reverse()) {
    if (visibleWidth(char + right) > targetSideWidth) break;
    right = char + right;
  }
  return truncateToWidth(`${left}\u2026${right}`, maxWidth, "");
}
function shortSessionId(session) {
  return (session.federation?.remoteStableSessionId ?? session.id).slice(0, 8);
}
function sessionTitle(session, options2) {
  const name = options2?.reference ?? `${session.name || "Unnamed session"} (${shortSessionId(session)})`;
  const remote = session.federation ? `remote:${options2?.origin ?? session.federation.originLabel ?? session.federation.originId}` : void 0;
  const tags = [options2?.self ? "self" : void 0, options2?.sameCwd ? "same cwd" : void 0, remote].filter((tag2) => Boolean(tag2));
  const suffix = tags.length ? ` [${tags.join(", ")}]` : "";
  const description = session.description ? ` \u2014 ${session.description}` : "";
  return `${name}${suffix}${description}`;
}
var SessionListOverlay;
var init_session_list = __esm({
  "ui/session-list.ts"() {
    init_dist();
    SessionListOverlay = class {
      constructor(theme, keybindings, currentSession, sessions, done, referenceFor, originFor) {
        this.referenceFor = referenceFor;
        this.originFor = originFor;
        this.theme = theme;
        this.keybindings = keybindings;
        this.currentSession = currentSession;
        this.sessions = sessions;
        this.done = done;
      }
      referenceFor;
      originFor;
      theme;
      keybindings;
      currentSession;
      done;
      sessions;
      selectedIndex = 0;
      maxVisible = 8;
      onSessionSelect(sessionId) {
        const session = this.sessions.find((s) => s.id === sessionId);
        if (!session) return;
        this.done(session);
      }
      invalidate() {
      }
      handleInput(data) {
        if (this.keybindings.matches(data, "tui.select.cancel")) {
          this.done(void 0);
          return;
        }
        if (this.sessions.length === 0) {
          return;
        }
        if (this.keybindings.matches(data, "tui.select.up")) {
          this.selectedIndex = this.selectedIndex === 0 ? this.sessions.length - 1 : this.selectedIndex - 1;
          return;
        }
        if (this.keybindings.matches(data, "tui.select.down")) {
          this.selectedIndex = this.selectedIndex === this.sessions.length - 1 ? 0 : this.selectedIndex + 1;
          return;
        }
        if (this.keybindings.matches(data, "tui.select.confirm")) {
          const session = this.sessions[this.selectedIndex];
          if (session) {
            this.onSessionSelect(session.id);
          }
        }
      }
      render(width) {
        const innerWidth = Math.max(1, Math.min(width, 88));
        if (innerWidth === 1) {
          return [this.theme.fg("accent", "\u2502")];
        }
        const contentWidth = Math.max(0, innerWidth - 2);
        const footer = `${this.keybindings.getKeys("tui.select.confirm").join("/")}: Message \u2022 ${this.keybindings.getKeys("tui.select.cancel").join("/")}: Close`;
        const border = (text) => this.theme.fg("accent", text);
        const row = (text = "") => {
          const clipped = truncateToWidth(text, contentWidth, "", true);
          return `${border("\u2502")}${clipped}${" ".repeat(Math.max(0, contentWidth - visibleWidth(clipped)))}${border("\u2502")}`;
        };
        const lines = [];
        lines.push(border(`\u256D${"\u2500".repeat(contentWidth)}\u256E`));
        lines.push(row(this.theme.bold(" Current Session")));
        lines.push(border(`\u251C${"\u2500".repeat(contentWidth)}\u2524`));
        lines.push(row());
        lines.push(row(`  ${this.theme.fg("dim", sessionTitle(this.currentSession, { self: true, reference: this.referenceFor?.(this.currentSession), origin: this.originFor?.(this.currentSession) }))}`));
        lines.push(row(`  ${this.theme.fg("dim", `${middleTruncate(this.currentSession.cwd, Math.max(8, contentWidth - 4))} \u2022 ${this.currentSession.model}`)}`));
        lines.push(row());
        lines.push(border(`\u251C${"\u2500".repeat(contentWidth)}\u2524`));
        lines.push(row(this.theme.bold(" Other Sessions")));
        lines.push(row());
        if (this.sessions.length === 0) {
          lines.push(row(this.theme.fg("dim", " No other parley-connected sessions")));
        } else {
          const startIndex = Math.max(
            0,
            Math.min(this.selectedIndex - Math.floor(this.maxVisible / 2), this.sessions.length - this.maxVisible)
          );
          const endIndex = Math.min(startIndex + this.maxVisible, this.sessions.length);
          for (let index = startIndex; index < endIndex; index += 1) {
            const session = this.sessions[index];
            const isSelected = index === this.selectedIndex;
            const sameCwd2 = session.cwd === this.currentSession.cwd;
            const prefix = isSelected ? this.theme.fg("accent", "\u2192 ") : "  ";
            const title = sessionTitle(session, { sameCwd: sameCwd2, reference: this.referenceFor?.(session), origin: this.originFor?.(session) });
            const pathText = `${middleTruncate(session.cwd, Math.max(8, contentWidth - 4))} \u2022 ${session.model}`;
            lines.push(row(`${prefix}${isSelected ? this.theme.fg("accent", title) : title}`));
            lines.push(row(`  ${this.theme.fg("dim", pathText)}`));
            if (index < endIndex - 1) {
              lines.push(row());
            }
          }
          if (startIndex > 0 || endIndex < this.sessions.length) {
            lines.push(row());
            lines.push(row(this.theme.fg("dim", ` ${this.selectedIndex + 1}/${this.sessions.length}`)));
          }
        }
        lines.push(row());
        lines.push(border(`\u251C${"\u2500".repeat(contentWidth)}\u2524`));
        lines.push(row(this.theme.fg("dim", ` ${footer}`)));
        lines.push(border(`\u2570${"\u2500".repeat(contentWidth)}\u256F`));
        return lines;
      }
    };
  }
});

// ui/compose.ts
var ComposeOverlay;
var init_compose = __esm({
  "ui/compose.ts"() {
    init_dist();
    init_types2();
    ComposeOverlay = class {
      tui;
      theme;
      keybindings;
      target;
      targetLabel;
      client;
      done;
      inputBuffer = "";
      sending = false;
      error = null;
      constructor(tui, theme, keybindings, target2, targetLabel, client, done) {
        this.tui = tui;
        this.theme = theme;
        this.keybindings = keybindings;
        this.target = target2;
        this.targetLabel = targetLabel;
        this.client = client;
        this.done = done;
      }
      invalidate() {
      }
      handleInput(data) {
        if (this.sending) return;
        if (this.keybindings.matches(data, "tui.select.cancel")) {
          this.done({ sent: false });
          return;
        }
        if (data.startsWith("\x1B")) {
          return;
        }
        if (this.keybindings.matches(data, "tui.select.confirm")) {
          if (this.inputBuffer.trim()) {
            this.sendMessage();
          }
          return;
        }
        if (this.keybindings.matches(data, "tui.editor.deleteCharBackward")) {
          this.inputBuffer = [...this.inputBuffer].slice(0, -1).join("");
          this.tui.requestRender();
          return;
        }
        const printable = [...data].filter((c) => c >= " ").join("");
        if (printable) {
          this.inputBuffer += printable;
          this.tui.requestRender();
        }
      }
      async sendMessage() {
        this.sending = true;
        this.error = null;
        this.tui.requestRender();
        try {
          const result = await this.client.send(this.target.id, {
            text: this.inputBuffer.trim(),
            // The person typed this, not the agent. Older brokers would refuse the field.
            ...this.client.supportsFeature(PERSON_PROVENANCE_FEATURE) ? { provenance: { type: "session_person" } } : {}
          });
          if (!result.delivered) {
            this.error = result.reason ?? "Message not delivered. Session may not exist or has disconnected.";
            this.sending = false;
            this.tui.requestRender();
            return;
          }
          this.done({
            sent: true,
            messageId: result.id,
            text: this.inputBuffer.trim(),
            delivery: result.delivery,
            ...result.peerCompaction ? { peerCompaction: result.peerCompaction } : {},
            ...result.contactToken ? { contactToken: result.contactToken } : {}
          });
        } catch (error) {
          this.error = error instanceof Error ? error.message : String(error);
          this.sending = false;
          this.tui.requestRender();
        }
      }
      render(width) {
        const innerWidth = Math.max(1, Math.min(width, 72));
        if (innerWidth === 1) {
          return [this.theme.fg("accent", "\u2502")];
        }
        const contentWidth = Math.max(0, innerWidth - 2);
        const footer = `${this.keybindings.getKeys("tui.select.confirm").join("/")}: Send \u2022 ${this.keybindings.getKeys("tui.select.cancel").join("/")}: Close`;
        const border = (text) => this.theme.fg("accent", text);
        const row = (text = "") => {
          const clipped = truncateToWidth(text, contentWidth, "", true);
          return `${border("\u2502")}${clipped}${" ".repeat(Math.max(0, contentWidth - visibleWidth(clipped)))}${border("\u2502")}`;
        };
        const lines = [];
        lines.push(border(`\u256D${"\u2500".repeat(contentWidth)}\u256E`));
        lines.push(row(this.theme.bold(` Send to: ${this.targetLabel}`)));
        lines.push(row(this.theme.fg("dim", ` ${this.target.cwd} \u2022 ${this.target.model}`)));
        lines.push(border(`\u251C${"\u2500".repeat(contentWidth)}\u2524`));
        lines.push(row());
        if (this.sending) {
          lines.push(row(this.theme.fg("dim", " Sending...")));
        } else if (this.error) {
          lines.push(row(this.theme.fg("error", ` Error: ${this.error}`)));
          lines.push(row());
          lines.push(row(` > ${this.inputBuffer}\u2588`));
        } else {
          lines.push(row(` > ${this.inputBuffer}\u2588`));
        }
        lines.push(row());
        lines.push(border(`\u251C${"\u2500".repeat(contentWidth)}\u2524`));
        lines.push(row(this.theme.fg("dim", ` ${footer}`)));
        lines.push(border(`\u2570${"\u2500".repeat(contentWidth)}\u256F`));
        return lines;
      }
    };
  }
});

// compaction-awareness.ts
function formatPeerCompactionNotice(peerDisplay, notice, expectedPeerSessionId, sessionRef) {
  const compactionCount = notice.generation - notice.previousGeneration;
  const countText = compactionCount === 1 ? "" : ` (${compactionCount} times)`;
  const shortPeerId = notice.peerSessionId.slice(0, 8);
  const requested = peerDisplay.trim();
  const peerName = notice.peerName?.trim();
  const expectedPeerId = notice.requestedPeerSessionId ?? expectedPeerSessionId;
  const expectedMatchesName = notice.requestedPeerSessionId === void 0 && peerName !== void 0 && expectedPeerId?.toLocaleLowerCase() === peerName.toLocaleLowerCase();
  const rebound = expectedPeerId !== void 0 && expectedPeerId !== notice.peerSessionId && !expectedMatchesName;
  const requestedMatchesActual = requested === notice.peerSessionId || requested.includes(shortPeerId) || peerName !== void 0 && requested.toLocaleLowerCase() === peerName.toLocaleLowerCase();
  const actual = sessionRef?.(notice.peerSessionId, peerName);
  const identifiedPeer = sessionRef ? rebound ? `${actual} (message was requested for ${sessionRef(expectedPeerId)})` : requestedMatchesActual || actual === requested ? requested : `${actual} (requested as ${requested})` : rebound ? `${peerName || "peer"} [session ${notice.peerSessionId}] (message was requested for ${requested} [session ${expectedPeerId}])` : requestedMatchesActual ? requested : peerName ? `${peerName} (requested as ${requested})` : `${requested} [peer session ${notice.peerSessionId}]`;
  return `Note: ${identifiedPeer} has compacted since you last talked${countText}. It probably still knows what's going on, but if this depends on something specific from before, its memory of that may be vaguer.`;
}
var init_compaction_awareness = __esm({
  "compaction-awareness.ts"() {
  }
});

// ui/inline-message.ts
function messageExpanded(messageId, expandedByDefault) {
  return clickedOpen.get(messageId) ?? expandedByDefault;
}
var clickedOpen, InlineMessageComponent;
var init_inline_message = __esm({
  "ui/inline-message.ts"() {
    init_dist();
    init_compaction_awareness();
    clickedOpen = /* @__PURE__ */ new Map();
    InlineMessageComponent = class {
      constructor(from, message, theme, replyCommand, bodyText, collapsed = false, references) {
        this.references = references;
        this.from = from;
        this.message = message;
        this.theme = theme;
        this.replyCommand = replyCommand;
        this.bodyText = bodyText;
        this.collapsed = collapsed;
      }
      references;
      from;
      message;
      theme;
      replyCommand;
      bodyText;
      collapsed;
      // Caches assume message/bodyText never mutate after construction; theme
      // styling stays outside the caches so live theme changes apply per render.
      collapsedPreview;
      wrappedBody;
      invalidate() {
      }
      /** In fullscreen mode, a click opens or shuts this message. */
      handleMouse(event) {
        if (event.type !== "click" || event.button !== "left") return void 0;
        this.collapsed = !this.collapsed;
        clickedOpen.set(this.message.id, !this.collapsed);
        return { handled: true, render: true };
      }
      render(width) {
        const lines = [];
        const borderChar = "\u2500";
        const senderName = this.references?.session(this.from) ?? (this.from.name || this.from.id.slice(0, 8));
        const replyToLabel = this.message.replyTo ? this.references?.message(this.message.replyTo) ?? this.message.replyTo.slice(0, 8) : void 0;
        const messageLabel = this.references ? ` \xB7 ${this.references.message(this.message.id)}` : "";
        if (width < 3) {
          return [truncateToWidth(`From ${senderName}`, width)];
        }
        const bodyWidth = Math.max(1, width - 2);
        const header = ` From: ${senderName} (${this.from.cwd})${messageLabel} `;
        const headerText = truncateToWidth(this.collapsed ? `${header} Ctrl+O expands ` : header, bodyWidth, "");
        const headerPadding = Math.max(0, bodyWidth - visibleWidth(headerText));
        lines.push(
          this.theme.fg("muted", "\u256D") + this.theme.fg("toolTitle", headerText) + this.theme.fg("muted", `${borderChar.repeat(headerPadding)}\u256E`)
        );
        const frameLine = (content) => {
          const text = truncateToWidth(content, bodyWidth, "");
          const padding = Math.max(0, bodyWidth - visibleWidth(text));
          return this.theme.fg("muted", "\u2502") + text + this.theme.fg("muted", `${" ".repeat(padding)}\u2502`);
        };
        if (this.collapsed) {
          this.collapsedPreview ??= (this.bodyText || this.message.content.text).replace(/\s+/g, " ").trim();
          lines.push(frameLine(this.theme.fg("text", this.collapsedPreview)));
          const meta = [];
          if (this.replyCommand) meta.push(`To reply: ${this.replyCommand}`);
          if (this.message.content.attachments?.length) {
            const count = this.message.content.attachments.length;
            meta.push(`${count} attachment${count === 1 ? "" : "s"}`);
          }
          if (this.message.provenance?.type === "extension_outbox") meta.push(`Via ${this.message.provenance.extensionName}`);
          if (this.message.provenance?.type === "session_person") meta.push("Written by the person there");
          if (this.message.replyTo && !this.message.expectsReply) meta.push(`Reply to ${replyToLabel}`);
          if (this.message.peerCompaction) meta.push("Sender compacted since prior direct contact");
          meta.push("Click or Ctrl+O to expand");
          lines.push(frameLine(this.theme.fg("dim", ` ${meta.join(" \xB7 ")}`)));
          lines.push(this.theme.fg("muted", `\u2570${borderChar.repeat(bodyWidth)}\u256F`));
          return lines;
        }
        if (this.message.peerCompaction) {
          const sessionName = this.references?.sessionName?.bind(this.references);
          const rawNotice = formatPeerCompactionNotice(senderName, this.message.peerCompaction, this.from.id, sessionName);
          const notice = sessionName ? rawNotice : this.references?.present?.(rawNotice) ?? rawNotice;
          for (const line of wrapTextWithAnsi(this.theme.fg("warning", ` ${notice}`), bodyWidth)) {
            lines.push(frameLine(line));
          }
          lines.push(frameLine(""));
        }
        if (this.wrappedBody?.width !== bodyWidth) {
          this.wrappedBody = {
            width: bodyWidth,
            lines: wrapTextWithAnsi(this.bodyText || this.message.content.text, bodyWidth)
          };
        }
        for (const line of this.wrappedBody.lines) {
          lines.push(frameLine(this.theme.fg("text", line)));
        }
        if (this.replyCommand) {
          lines.push(frameLine(""));
          const replyLines = wrapTextWithAnsi(this.theme.fg("dim", ` To reply: ${this.replyCommand}`), bodyWidth);
          for (const line of replyLines) {
            lines.push(frameLine(line));
          }
        }
        if (this.message.content.attachments?.length) {
          lines.push(frameLine(""));
          for (const att of this.message.content.attachments) {
            lines.push(frameLine(this.theme.fg("dim", ` Attachment: ${att.name}`)));
          }
        }
        if (this.message.replyTo && !this.message.expectsReply) {
          lines.push(frameLine(""));
          lines.push(frameLine(this.theme.fg("dim", ` Reply to ${replyToLabel}`)));
        }
        if (this.message.provenance?.type === "extension_outbox") {
          lines.push(frameLine(""));
          lines.push(frameLine(this.theme.fg("dim", ` Via extension: ${this.message.provenance.extensionName}`)));
        }
        if (this.message.provenance?.type === "session_person") {
          lines.push(frameLine(""));
          lines.push(frameLine(this.theme.fg("dim", " Written by the person at the sending session")));
        }
        lines.push(this.theme.fg("muted", `\u2570${borderChar.repeat(bodyWidth)}\u256F`));
        return lines;
      }
    };
  }
});

// ui/click-to-expand.ts
var ClickToExpand;
var init_click_to_expand = __esm({
  "ui/click-to-expand.ts"() {
    ClickToExpand = class {
      constructor(child, onClick) {
        this.child = child;
        this.onClick = onClick;
      }
      child;
      onClick;
      render(width) {
        return this.child.render(width);
      }
      invalidate() {
        this.child.invalidate?.();
      }
      handleMouse(event) {
        if (event.type !== "click" || event.button !== "left") return void 0;
        this.onClick();
        return { handled: true, render: true };
      }
    };
  }
});

// extension-api.ts
var PARLEY_EXTENSION_REGISTER_EVENT, PARLEY_EXTENSION_REGISTRY_READY_EVENT, PARLEY_OUTBOX_REQUEST_EVENT, PARLEY_OUTBOX_RESULT_EVENT;
var init_extension_api = __esm({
  "extension-api.ts"() {
    PARLEY_EXTENSION_REGISTER_EVENT = "parley:extension-register";
    PARLEY_EXTENSION_REGISTRY_READY_EVENT = "parley:extension-registry-ready";
    PARLEY_OUTBOX_REQUEST_EVENT = "parley:outbox-request";
    PARLEY_OUTBOX_RESULT_EVENT = "parley:outbox-result";
  }
});

// reply-tracker.ts
function senderMatchPriority(context, to) {
  if (context.from.id === to) return 0;
  if (context.from.name?.toLowerCase() === to.toLowerCase()) return 1;
  if (context.from.id.startsWith(to)) return 2;
  return 3;
}
function resolvePendingSender(pending, to, name = (value) => `"${value}"`) {
  const exactIdMatches = pending.filter((context) => senderMatchPriority(context, to) === 0);
  if (exactIdMatches.length === 1) {
    return exactIdMatches[0];
  }
  if (exactIdMatches.length > 1) {
    throw new Error(`Multiple pending asks from ${name(to)} \u2014 specify \`replyTo\` with the message reference`);
  }
  const exactNameMatches = pending.filter((context) => senderMatchPriority(context, to) === 1);
  if (exactNameMatches.length === 1) {
    return exactNameMatches[0];
  }
  if (exactNameMatches.length > 1) {
    throw new Error(`Multiple pending asks match sender name "${to}" \u2014 specify \`replyTo\` with the message reference`);
  }
  const idPrefixMatches = pending.filter((context) => senderMatchPriority(context, to) === 2);
  if (idPrefixMatches.length === 1) {
    return idPrefixMatches[0];
  }
  if (idPrefixMatches.length > 1) {
    throw new Error(`Multiple pending asks match ID prefix "${to}" \u2014 use a longer session ID prefix or specify \`replyTo\``);
  }
  throw new Error(`No pending ask from ${name(to)}`);
}
var RAW_REFERENCES, ReplyTracker;
var init_reply_tracker = __esm({
  "reply-tracker.ts"() {
    init_config2();
    RAW_REFERENCES = {
      session: (from) => from.name || from.id,
      message: (id) => id
    };
    ReplyTracker = class {
      constructor(askTimeoutMs = getAskTimeoutMs(), references = RAW_REFERENCES) {
        this.askTimeoutMs = askTimeoutMs;
        this.references = references;
      }
      askTimeoutMs;
      references;
      messages = /* @__PURE__ */ new Map();
      pendingAsks = /* @__PURE__ */ new Map();
      activeContexts = [];
      /** Requests already mentioned in automatic context while elapsed or unreachable. */
      quietAnnounced = /* @__PURE__ */ new Set();
      recordIncomingMessage(from, message, receivedAt = Date.now()) {
        const context = { from, message, receivedAt };
        this.messages.set(message.id, context);
        for (const id of this.messages.keys()) {
          if (this.messages.size <= 200) break;
          if (!this.pendingAsks.has(id) && id !== message.id) this.messages.delete(id);
        }
        if (message.expectsReply) {
          this.pendingAsks.set(message.id, context);
        }
        return context;
      }
      /** Newly surfaced messages replace the active conversation; tool-only iterations retain it.
       * Keep simultaneous candidates distinct from an absence of context. */
      activateContexts(contexts) {
        if (contexts.length > 0) this.activeContexts = [...contexts];
      }
      /** A sender as the model knows it: its reference when this tracker has seen it, else the text given. */
      senderName(to) {
        for (const context of this.messages.values()) if (context.from.id === to) return this.references.session(context.from);
        return `"${to}"`;
      }
      clearActiveContexts() {
        this.activeContexts = [];
      }
      reset() {
        this.messages.clear();
        this.pendingAsks.clear();
        this.quietAnnounced.clear();
        this.clearActiveContexts();
      }
      /**
       * `exactSender` means `to` is a pinned identity: only that exact sender matches, never a
       * session whose name or ID merely overlaps it.
       */
      resolveReplyTarget(options2, now = Date.now()) {
        const priorityOf = (context, to) => {
          const priority = senderMatchPriority(context, to);
          return options2.exactSender && priority !== 0 ? 3 : priority;
        };
        if (options2.replyTo) {
          const target2 = this.messages.get(options2.replyTo);
          if (!target2) {
            throw new Error(`No retained message ${this.references.message(options2.replyTo)}`);
          }
          if (options2.to && priorityOf(target2, options2.to) === 3) {
            throw new Error(`Pending ask ${this.references.message(options2.replyTo)} is not from ${this.senderName(options2.to)}`);
          }
          return target2;
        }
        const pending = Array.from(this.pendingAsks.values());
        if (options2.to) {
          const candidates = [...this.activeContexts, ...pending];
          const priority = candidates.reduce((best, context) => Math.min(best, priorityOf(context, options2.to)), 3);
          const matches = candidates.filter((context) => priority < 3 && priorityOf(context, options2.to) === priority);
          const activeMatches = this.activeContexts.filter((context) => matches.includes(context));
          if (activeMatches.length > 0 && new Set(matches.map((context) => context.from.id)).size > 1) {
            throw new Error(`Multiple senders match ${this.senderName(options2.to)} \u2014 specify \`replyTo\` with the message reference.`);
          }
          if (activeMatches.length > 1) {
            throw new Error(`Multiple active messages match ${this.senderName(options2.to)} \u2014 specify \`replyTo\`.`);
          }
          if (activeMatches.length === 1) return activeMatches[0];
          if (pending.some((context) => priorityOf(context, options2.to) < 3)) {
            try {
              return resolvePendingSender(options2.exactSender ? pending.filter((context) => context.from.id === options2.to) : pending, options2.to, (to) => this.senderName(to));
            } catch (error) {
              throw new Error(`${error.message}
${this.formatConversationContext({ now, complete: true })}`);
            }
          }
          throw new Error(`No pending ask from ${this.senderName(options2.to)}. Exact replyTo can identify a retained ordinary message.`);
        }
        if (this.activeContexts.length > 1) {
          throw new Error("Multiple active conversations \u2014 specify `to` or `replyTo`.");
        }
        if (this.activeContexts.length === 1) {
          return this.activeContexts[0];
        }
        if (pending.length === 1) {
          return pending[0];
        }
        if (pending.length === 0) {
          throw new Error("No active parley context to reply to");
        }
        throw new Error(`Multiple pending asks \u2014 specify \`to\` or \`replyTo\`.
${this.formatConversationContext({ now, complete: true })}`);
      }
      findUniquePendingAskFrom(to, now = Date.now()) {
        const candidates = Array.from(this.pendingAsks.values()).filter((context) => {
          if (now - context.receivedAt > this.askTimeoutMs) {
            return false;
          }
          return context.from.id === to || context.from.name?.toLowerCase() === to.toLowerCase();
        });
        return candidates.length === 1 ? candidates[0] : null;
      }
      getActiveReplyTarget(now = Date.now()) {
        const active = this.activeContexts.length === 1 ? this.activeContexts[0] : void 0;
        return active?.message.expectsReply ? active : null;
      }
      findActiveReplyTargetMismatch(to, now = Date.now()) {
        const activeReplyTarget = this.getActiveReplyTarget(now);
        if (!activeReplyTarget) {
          return null;
        }
        return activeReplyTarget.from.id === to ? null : activeReplyTarget;
      }
      markReplied(replyTo) {
        this.dismissPendingAsk(replyTo);
      }
      dismissPendingAsk(replyTo) {
        this.pendingAsks.delete(replyTo);
        this.activeContexts = this.activeContexts.filter((context) => context.message.id !== replyTo);
      }
      listPending(now = Date.now()) {
        return Array.from(this.pendingAsks.values()).sort((a, b) => a.receivedAt - b.receivedAt);
      }
      /** Retained text keeps its later withdrawal/replacement context. */
      setDisposition(messageId, disposition) {
        const context = this.messages.get(messageId);
        if (context) this.messages.set(messageId, { ...context, disposition });
        this.dismissPendingAsk(messageId);
      }
      /** Full retained snapshot for recovery or explicit conversation threading. */
      getMessage(messageId) {
        return this.messages.get(messageId);
      }
      /** A response timeout is a waiting-window boundary, not completed or withdrawn work. */
      replyWindowElapsed(context, now = Date.now()) {
        return now > (context.message.replyDeadline ?? context.receivedAt + this.askTimeoutMs);
      }
      /**
       * Bounded adjacent context for action results; never selects or answers a request.
       * Automatic context mentions an elapsed or unreachable request once, then only counts
       * it: waiting is not settlement, and `complete` (pending, errors) always lists everything.
       */
      formatConversationContext(options2 = {}) {
        const now = options2.now ?? Date.now();
        const limit = Math.max(1, Math.floor(options2.limit ?? 3));
        const previewLength = Math.max(20, Math.min(300, options2.previewLength ?? 120));
        const pending = this.listPending(now);
        const activeIds = new Set(this.activeContexts.map((context) => context.message.id));
        const contexts = [...this.activeContexts, ...pending.filter((item) => !activeIds.has(item.message.id))];
        if (contexts.length === 0) return "";
        const quote2 = this.references.verbatim ?? ((text) => text);
        const lines = [];
        let quieted = 0;
        let omitted = 0;
        for (const context of contexts) {
          const id = context.message.id;
          const awaiting = this.pendingAsks.has(id);
          const elapsed = awaiting && this.replyWindowElapsed(context, now);
          const unreachable = this.references.isReachable?.(context.from) === false;
          const quietKey = (elapsed || unreachable) && !activeIds.has(id) ? `${id}\0${elapsed ? "elapsed" : ""}\0${unreachable ? "unreachable" : ""}` : void 0;
          if (!options2.complete && quietKey && this.quietAnnounced.has(quietKey)) {
            quieted += 1;
            continue;
          }
          if (lines.length >= limit) {
            omitted += 1;
            continue;
          }
          if (!options2.complete && quietKey) this.quietAnnounced.add(quietKey);
          const preview = context.message.content.text.replace(/\s+/g, " ").trim();
          const status = awaiting ? elapsed ? "unanswered; reply window elapsed, not withdrawn" : "awaiting your reply" : "active conversation";
          const attachments = context.message.content.attachments?.length;
          lines.push(`- ${this.references.session(context.from)}, message ${this.references.message(id)} \u2014 ${status}${unreachable ? " (sender currently unreachable)" : ""}: ${quote2(JSON.stringify(preview.length > previewLength ? `${preview.slice(0, previewLength - 1)}\u2026` : preview))}${attachments ? ` [${attachments} attachment snapshot(s)]` : ""}${context.message.peerCompaction ? " [sender compacted since prior direct contact (notice at message arrival)]" : ""}`);
        }
        if (omitted > 0) lines.push(`- ${omitted} more conversation(s); pending has the complete request list.`);
        if (quieted > 0) lines.push(`- ${quieted} earlier unanswered request(s) already mentioned (reply window elapsed or sender unreachable); pending lists them.`);
        return `Conversation context (${pending.length} unanswered request${pending.length === 1 ? "" : "s"}):
${lines.join("\n")}`;
      }
    };
  }
});

// broker/federation-conversation.ts
function validEndpoint(value) {
  return isCanonicalFederationOriginId(value.originId) && isFederationCorrelationId(value.originEpoch) && isCanonicalFederationScopeAlias(value.scopeAlias) && typeof value.stableSessionId === "string" && value.stableSessionId.length > 0 && value.stableSessionId.length <= 512 && value.stableSessionId.trim().length > 0 && !/[\p{Cc}\p{Cf}]/u.test(value.stableSessionId) && isFederationCorrelationId(value.endpointEpoch);
}
function encodeConversationMessageId(identity) {
  if (!validEndpoint(identity) || !isFederationCorrelationId(identity.nonce)) throw new Error("Invalid conversation message identity");
  return PREFIX + Buffer.from(JSON.stringify([
    identity.originId,
    identity.originEpoch,
    identity.scopeAlias,
    identity.stableSessionId,
    identity.endpointEpoch,
    identity.nonce
  ])).toString("base64url");
}
function decodeConversationMessageId(value) {
  if (typeof value !== "string" || !value.startsWith(PREFIX) || value.length > 8192) return;
  const encoded = value.slice(PREFIX.length);
  if (!/^[A-Za-z0-9_-]+$/.test(encoded)) return;
  try {
    const tuple = JSON.parse(Buffer.from(encoded, "base64url").toString("utf8"));
    if (!Array.isArray(tuple) || tuple.length !== 6) return;
    const identity = {
      originId: tuple[0],
      originEpoch: tuple[1],
      scopeAlias: tuple[2],
      stableSessionId: tuple[3],
      endpointEpoch: tuple[4],
      nonce: tuple[5]
    };
    if (encodeConversationMessageId(identity) === value) return identity;
  } catch {
  }
}
var PREFIX;
var init_federation_conversation = __esm({
  "broker/federation-conversation.ts"() {
    init_federation_protocol();
    PREFIX = "oqm1.";
  }
});

// references.ts
import { randomBytes } from "node:crypto";
function referenceKey(value) {
  return value.trim().toLowerCase();
}
function sessionBase(session) {
  const name = session.runtimeFallbackAlias ? void 0 : session.name?.trim();
  return name || UNNAMED_BASE;
}
function isCanonicalIdentity(value) {
  return CANONICAL_SHAPE.test(value) && isCanonicalToken(value);
}
function isCanonicalToken(token) {
  if (token.startsWith("oqs1.")) return decodeOriginQualifiedSessionIdentity(token) !== void 0;
  if (token.startsWith("oqm1.")) return decodeConversationMessageId(token) !== void 0;
  return true;
}
var REFERENCE_ENTRY_TYPE, MESSAGE_ID_KEYS, SESSION_ID_KEYS, MARKER_NONCE, VERBATIM_START, VERBATIM_END, VERBATIM_SEGMENT, UNNAMED_BASE, LABEL_SEPARATOR, CANONICAL_SHAPE, CANONICAL_TOKEN, CONTAINS_CANONICAL, ReferenceBook;
var init_references = __esm({
  "references.ts"() {
    init_federation_protocol();
    init_federation_conversation();
    REFERENCE_ENTRY_TYPE = "parley_reference";
    MESSAGE_ID_KEYS = /* @__PURE__ */ new Set(["messageId", "replyMessageId", "replyTo", "supersedes", "retryOf", "supersededBy", "replacementId", "questionId", "requestMessageId"]);
    SESSION_ID_KEYS = /* @__PURE__ */ new Set(["targetId"]);
    MARKER_NONCE = randomBytes(12).toString("hex");
    VERBATIM_START = `\uE000${MARKER_NONCE}\uE001`;
    VERBATIM_END = `\uE001${MARKER_NONCE}\uE000`;
    VERBATIM_SEGMENT = new RegExp(`${VERBATIM_START}([\\s\\S]*?)${VERBATIM_END}`, "g");
    UNNAMED_BASE = "unnamed";
    LABEL_SEPARATOR = " \xB7 ";
    CANONICAL_SHAPE = /^(?:oq[sm]1\.[A-Za-z0-9_-]+|[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12})$/;
    CANONICAL_TOKEN = /(?<![\w.-])(?:oqs1\.[A-Za-z0-9_-]+|oqm1\.[A-Za-z0-9_-]+|[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12})(?![\w-])/g;
    CONTAINS_CANONICAL = /oq[sm]1\.|[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}/;
    ReferenceBook = class {
      constructor(persist = () => void 0) {
        this.persist = persist;
      }
      persist;
      sessionOwners = /* @__PURE__ */ new Map();
      sessionPrimary = /* @__PURE__ */ new Map();
      /** Authoritative current views: complete rosters, joins, presence, and fresh arrivals. */
      liveViews = /* @__PURE__ */ new Map();
      liveSessions;
      messageOwners = /* @__PURE__ */ new Map();
      messagePrimary = /* @__PURE__ */ new Map();
      messageLabels = /* @__PURE__ */ new Map();
      pendingLabels = /* @__PURE__ */ new Map();
      originRefs = /* @__PURE__ */ new Map();
      highWater = 0;
      originHighWater = 0;
      /** Replay previously persisted allocations without persisting them again. */
      restore(records) {
        for (const record of records) {
          if (!record || typeof record !== "object") continue;
          const value = record;
          if (typeof value.ref !== "string" || typeof value.id !== "string" || !value.ref.trim()) continue;
          if (value.kind === "session" && typeof value.base === "string") {
            this.sessionOwners.set(referenceKey(value.ref), value.id);
            this.sessionPrimary.set(value.id, { ref: value.ref, base: value.base });
          } else if (value.kind === "message") {
            this.messageOwners.set(referenceKey(value.ref), value.id);
            if (value.label === true) this.messageLabels.set(value.id, [...this.messageLabels.get(value.id) ?? [], value.ref]);
            else if (!this.messagePrimary.has(value.id)) this.messagePrimary.set(value.id, value.ref);
            const number = typeof value.number === "number" ? value.number : Number(value.ref.match(/^#(\d+)$/)?.[1]);
            if (Number.isSafeInteger(number)) this.highWater = Math.max(this.highWater, number);
          } else if (value.kind === "origin") {
            this.originRefs.set(value.id, value.ref);
            const number = Number(value.ref.match(/^remote-(\d+)$/)?.[1]);
            if (Number.isSafeInteger(number)) this.originHighWater = Math.max(this.originHighWater, number);
          }
        }
      }
      // ---------------------------------------------------------------- sessions
      /**
       * Learn current names from authoritative observations. A complete roster also defines
       * reachability; anything else only adds to it. Retained snapshots never come through here.
       */
      observeLive(sessions, options2 = {}) {
        const ids = /* @__PURE__ */ new Set();
        for (const session of sessions) {
          if (!session?.id) continue;
          ids.add(session.id);
          this.liveViews.set(session.id, { ...this.liveViews.get(session.id), ...session });
          if (!options2.complete) this.liveSessions?.add(session.id);
        }
        if (options2.complete) this.liveSessions = ids;
      }
      observeSessionLeft(id) {
        this.liveSessions?.delete(id);
      }
      /** Transport loss: reachability is unknown until the next complete roster. */
      forgetReachability() {
        this.liveSessions = void 0;
      }
      /** Undefined until a complete roster has been seen: absence then means unreachable, never ended. */
      isReachable(id) {
        return this.liveSessions ? this.liveSessions.has(id) : void 0;
      }
      isKnownSession(id) {
        return this.liveViews.has(id) || this.sessionPrimary.has(id);
      }
      /**
       * The reference a model should use for this session, allocating and pinning it on first use.
       * Only authoritative observations move the primary reference; a retained snapshot names an
       * identity only when nothing better is known, so old names never flip a session backwards.
       */
      sessionRef(session) {
        const view = typeof session === "string" ? { id: session } : session;
        const live = this.liveViews.get(view.id);
        const current = this.sessionPrimary.get(view.id);
        if (!live && current) return current.ref;
        const base = this.qualifiedBase(live ?? view);
        if (current && current.base === base) return current.ref;
        const ref = this.firstAvailableRef(base, view.id);
        this.sessionOwners.set(referenceKey(ref), view.id);
        this.sessionPrimary.set(view.id, { ref, base });
        this.persist({ kind: "session", ref, id: view.id, base });
        return ref;
      }
      /** Pinned references only; unpinned text is left to ordinary live name resolution. */
      resolveSession(text) {
        const id = this.sessionOwners.get(referenceKey(text));
        return id ? { id, ref: this.sessionPrimary.get(id)?.ref ?? text.trim() } : void 0;
      }
      /**
       * When a pinned session is unreachable and a live session now uses its name,
       * name that session so a model can choose it deliberately. A hint, not a verdict.
       */
      successorHint(id, addressedBase) {
        if (!this.liveSessions || this.liveSessions.has(id)) return void 0;
        const base = addressedBase ?? this.sessionPrimary.get(id)?.base;
        if (!base || base === UNNAMED_BASE) return void 0;
        for (const liveId of this.liveSessions) {
          const live = this.liveViews.get(liveId);
          if (liveId !== id && live && referenceKey(this.qualifiedBase(live)) === referenceKey(base)) return this.sessionRef(live);
        }
        return void 0;
      }
      /** A session on this computer is just its name; one on another computer carries that computer. */
      qualifiedBase(session) {
        const name = sessionBase(session);
        const originId = session.federation?.originId;
        return originId ? `${this.originRef({ originId, originLabel: session.federation?.originLabel })}:${name}` : name;
      }
      /** A readable host description: its own label, or a pinned remote-N for unlabelled origins. */
      originRef(federation) {
        const label = federation.originLabel?.trim();
        if (label) return label;
        const existing = this.originRefs.get(federation.originId);
        if (existing) return existing;
        const ref = `remote-${++this.originHighWater}`;
        this.originRefs.set(federation.originId, ref);
        this.persist({ kind: "origin", ref, id: federation.originId });
        return ref;
      }
      firstAvailableRef(base, id) {
        const owner = this.sessionOwners.get(referenceKey(base));
        if (owner === void 0 || owner === id) return base;
        for (let suffix = 2; ; suffix++) {
          const candidate = `${base}~${suffix}`;
          const candidateOwner = this.sessionOwners.get(referenceKey(candidate));
          if (candidateOwner === void 0 || candidateOwner === id) return candidate;
        }
      }
      // ---------------------------------------------------------------- messages
      /** `#N`, allocated once per canonical message and never reused. */
      messageNumber(id) {
        const existing = this.messagePrimary.get(id);
        if (existing) return existing;
        const number = ++this.highWater;
        const ref = `#${number}`;
        this.messageOwners.set(referenceKey(ref), id);
        this.messagePrimary.set(id, ref);
        this.persist({ kind: "message", ref, id, number });
        return ref;
      }
      /** How a message is shown: its stable number, followed by any local labels. */
      messageRef(id) {
        const number = this.messageNumber(id);
        const labels = this.messageLabels.get(id);
        return labels?.length ? [number, ...labels].join(LABEL_SEPARATOR) : number;
      }
      isKnownMessage(id) {
        return this.messagePrimary.has(id);
      }
      /** Whether text is a reference or label this book has issued, of either kind. */
      isIssuedReference(text) {
        return this.isIssuedSessionReference(text) || this.isIssuedMessageReference(text);
      }
      isIssuedSessionReference(text) {
        return this.sessionOwners.has(referenceKey(text));
      }
      isIssuedMessageReference(text) {
        const key = referenceKey(text);
        return this.messageOwners.has(key) || this.pendingLabels.has(key);
      }
      /** Undefined means the text is not written as a message reference at all. */
      resolveMessage(text) {
        const candidates = [text.trim(), ...text.split(LABEL_SEPARATOR).map((part) => part.trim())].filter(Boolean);
        for (const candidate of candidates) {
          const normalized2 = /^\d+$/.test(candidate) ? `#${candidate}` : candidate;
          const id = this.messageOwners.get(referenceKey(normalized2));
          if (id && !this.pendingLabels.has(referenceKey(normalized2))) return { kind: "message", id, ref: this.messageRef(id) };
        }
        const first = candidates[0] ?? "";
        const normalized = /^\d+$/.test(first) ? `#${first}` : first;
        return /^#\d+$/.test(normalized) || this.pendingLabels.has(referenceKey(normalized)) ? { kind: "unknown", ref: normalized } : void 0;
      }
      /** Why a label cannot name a message, or undefined when it can. */
      labelProblem(label, id) {
        const trimmed = label.trim();
        if (!trimmed || trimmed !== label || /\s/.test(trimmed)) return "Labels are one word without spaces, like review or notes.";
        if (/[\p{Cc}\p{Cf}]/u.test(trimmed)) return "Labels cannot contain control or formatting characters.";
        if (trimmed.length > 64) return "Labels are at most 64 characters.";
        if (/^#?\d+$/.test(trimmed) || CANONICAL_SHAPE.test(trimmed) || trimmed.startsWith("oq")) return `"${trimmed}" looks like a message number or identity; choose a word.`;
        const owner = this.messageOwners.get(referenceKey(trimmed));
        if (this.pendingLabels.has(referenceKey(trimmed)) || owner !== void 0 && owner !== id) return `"${trimmed}" already names another message.`;
        return void 0;
      }
      /** Give a known message a session-local label; failures change nothing. */
      labelMessage(id, label) {
        const problem = this.labelProblem(label, id);
        if (problem) return { ok: false, reason: problem };
        this.messageNumber(id);
        if (this.messageOwners.get(referenceKey(label)) !== id) {
          this.messageOwners.set(referenceKey(label), id);
          this.messageLabels.set(id, [...this.messageLabels.get(id) ?? [], label]);
          this.persist({ kind: "message", ref: label, id, label: true });
        }
        return { ok: true, ref: this.messageRef(id) };
      }
      /**
       * Reserve a label for a message not created yet. Reservation is synchronous, so concurrent
       * calls cannot both claim it; bind or release once the send outcome is known.
       */
      reserveLabel(label) {
        const problem = this.labelProblem(label);
        if (problem) return { ok: false, reason: problem };
        const key = referenceKey(label);
        this.pendingLabels.set(key, label);
        return {
          ok: true,
          bind: (id) => {
            this.pendingLabels.delete(key);
            this.labelMessage(id, label);
          },
          release: () => {
            this.pendingLabels.delete(key);
          }
        };
      }
      // ------------------------------------------------------------ presentation
      /** Mark peer-authored text so presentation leaves it exactly as written. */
      static verbatim(text) {
        return `${VERBATIM_START}${text}${VERBATIM_END}`;
      }
      /**
       * Register message and session identities a structured value declares
       * (tool result details, message envelopes) so free text mentioning them can
       * be presented. Keys, not string shapes, decide what is a message. Snapshots
       * learned here never become authoritative names.
       */
      learnFrom(value, declared = { messages: /* @__PURE__ */ new Set(), sessions: /* @__PURE__ */ new Set() }, depth = 0) {
        if (!value || typeof value !== "object" || depth > 4) return declared;
        if (Array.isArray(value)) {
          for (const item of value) this.learnFrom(item, declared, depth + 1);
          return declared;
        }
        const record = value;
        for (const [key, field] of Object.entries(record)) {
          if (MESSAGE_ID_KEYS.has(key) && typeof field === "string" && field) {
            this.messageNumber(field);
            declared.messages.add(field);
          } else if (SESSION_ID_KEYS.has(key) && typeof field === "string" && field) {
            declared.sessions.add(field);
          } else if (key === "peerCompaction" && field && typeof field === "object") {
            const notice = field;
            if (typeof notice.peerSessionId === "string") {
              this.sessionRef({ id: notice.peerSessionId, ...typeof notice.peerName === "string" ? { name: notice.peerName } : {} });
              declared.sessions.add(notice.peerSessionId);
            }
            if (typeof notice.requestedPeerSessionId === "string") declared.sessions.add(notice.requestedPeerSessionId);
          } else if ((key === "from" || key === "recipient" || key === "session") && field && typeof field === "object" && typeof field.id === "string") {
            this.sessionRef(field);
            declared.sessions.add(field.id);
          } else if (key === "message" && field && typeof field === "object" && typeof field.id === "string") {
            this.messageNumber(field.id);
            declared.messages.add(field.id);
            this.learnFrom(field, declared, depth + 1);
          } else if (field && typeof field === "object") this.learnFrom(field, declared, depth + 1);
        }
        return declared;
      }
      /**
       * Replace canonical identities in model-facing text with references. Valid origin-
       * qualified handles are always replaced; bare UUIDs only when this book knows what they
       * identify; identities a structured value declared, whatever their shape, exactly.
       * Verbatim (peer-authored) segments and references already generated are never altered.
       * `name (name)` collapses. Presenting presented text changes nothing.
       */
      present(text, declared) {
        if (!text) return text;
        const produced = /* @__PURE__ */ new Set();
        const exact = [
          ...[...declared?.messages ?? []].map((id) => ({ id, ref: () => this.messageRef(id) })),
          ...[...declared?.sessions ?? []].map((id) => ({ id, ref: () => this.sessionRef(id) }))
        ].filter(({ id }) => id.length >= 4 && !(CANONICAL_SHAPE.test(id) && isCanonicalToken(id)) && !this.isIssuedReference(id)).sort((left, right) => right.id.length - left.id.length);
        const fragile = [...this.sessionPrimary.values()].map(({ ref }) => ref).filter((ref) => CONTAINS_CANONICAL.test(ref) || exact.some(({ id }) => ref !== id && ref.includes(id))).sort((left, right) => right.length - left.length);
        const delimited = (value) => new RegExp(`(?<![\\w.:-])${value.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")}(?![\\w:-])`, "g");
        const replaceIdentities = (segment) => {
          let source = segment;
          for (const { id, ref } of exact) {
            const pattern = delimited(id);
            if (!pattern.test(source)) continue;
            const reference = ref();
            produced.add(reference);
            source = source.replace(pattern, () => reference);
          }
          return source.replace(CANONICAL_TOKEN, (token) => {
            let ref;
            if (declared?.messages.has(token)) ref = this.messageRef(token);
            else if (declared?.sessions.has(token)) ref = this.sessionRef(token);
            else if (token.startsWith("oqs1.")) ref = isCanonicalToken(token) || this.isKnownSession(token) ? this.sessionRef(token) : void 0;
            else if (token.startsWith("oqm1.")) ref = isCanonicalToken(token) || this.messagePrimary.has(token) ? this.messageRef(token) : void 0;
            else if (this.isKnownSession(token)) ref = this.sessionRef(token);
            else if (this.messagePrimary.has(token)) ref = this.messageRef(token);
            if (!ref) return token;
            produced.add(ref);
            return ref;
          });
        };
        const collapse = (segment) => {
          let out = segment;
          for (const ref of produced) out = out.split(`${ref} (${ref})`).join(ref);
          return out;
        };
        const presentSegment = (segment) => collapse(presentGeneratedSegment(segment));
        const presentGeneratedSegment = (segment) => {
          if (fragile.length === 0) return replaceIdentities(segment);
          const protectedRefs = new RegExp(fragile.map((ref) => delimited(ref).source).join("|"), "g");
          let out = "";
          let cursor2 = 0;
          for (const match of segment.matchAll(protectedRefs)) {
            out += replaceIdentities(segment.slice(cursor2, match.index)) + match[0];
            cursor2 = match.index + match[0].length;
          }
          return out + replaceIdentities(segment.slice(cursor2));
        };
        let result = "";
        let cursor = 0;
        for (const match of text.matchAll(VERBATIM_SEGMENT)) {
          result += presentSegment(text.slice(cursor, match.index)) + match[1];
          cursor = match.index + match[0].length;
        }
        result += presentSegment(text.slice(cursor));
        return result;
      }
    };
  }
});

// conversation-history.ts
function matchesAskCounterpart(ask, from) {
  return ask.to === from.id && (ask.endpointEpoch === void 0 || ask.endpointEpoch === from.endpointEpoch) && (ask.originEpoch === void 0 || ask.originEpoch === from.federation?.originEpoch);
}
function messageControlKey(control) {
  return JSON.stringify([control.messageId, control.action, control.supersededBy ?? null]);
}
function restoreConversationHistory(entries) {
  const state = {
    incoming: /* @__PURE__ */ new Map(),
    settledIncoming: /* @__PURE__ */ new Set(),
    persistedIncoming: /* @__PURE__ */ new Set(),
    controls: /* @__PURE__ */ new Map(),
    persistedControls: /* @__PURE__ */ new Set(),
    outgoing: /* @__PURE__ */ new Map()
  };
  for (const raw of entries) {
    if (!raw || typeof raw !== "object") continue;
    const entry = raw;
    const data = entry.data;
    if (entry.type === "message" && entry.message?.role === "toolResult") {
      const replyId = entry.message.details?.replyMessageId;
      if (typeof replyId === "string") state.persistedIncoming.add(replyId);
      continue;
    }
    if (entry.type === "custom_message") {
      if (entry.customType === "parley_message") {
        const details = entry.details;
        if (details?.from?.id && details.message?.id && details.message.content) {
          state.incoming.set(details.message.id, { from: details.from, message: details.message, receivedAt: details.message.receiverReceivedAt ?? details.message.timestamp });
          state.persistedIncoming.add(details.message.id);
        }
      } else if (entry.customType === "parley_message_control") {
        const details = entry.details;
        if (details?.control?.messageId) {
          const key = messageControlKey(details.control);
          state.persistedControls.add(key);
          state.settledIncoming.add(details.control.messageId);
          if (details.from?.id) state.controls.set(key, { from: details.from, control: details.control });
        }
      }
      continue;
    }
    if (entry.type !== "custom" || !data) continue;
    switch (entry.customType) {
      case "parley_inbound_received": {
        const context = data;
        if (context.from?.id && context.message?.id && context.message.content) {
          state.incoming.set(context.message.id, context);
        }
        break;
      }
      case "parley_inbound_settled":
        if (typeof data.messageId === "string") state.settledIncoming.add(data.messageId);
        break;
      case "parley_inbound_control": {
        const value = data;
        if (value.from?.id && value.control?.messageId) {
          state.controls.set(messageControlKey(value.control), value);
          state.settledIncoming.add(value.control.messageId);
        }
        break;
      }
      case "parley_sent": {
        const message = data.message;
        if (typeof message?.replyTo === "string" && message.completesAsk !== false && !message.expectsReply) state.settledIncoming.add(message.replyTo);
        break;
      }
      case "parley_ask_pending":
        if (typeof data.messageId === "string" && typeof data.to === "string" && typeof data.sentAt === "number") {
          const message = data.message;
          state.outgoing.set(data.messageId, {
            to: data.to,
            targetDisplay: typeof data.targetDisplay === "string" ? data.targetDisplay : data.to,
            ...typeof data.endpointEpoch === "string" ? { endpointEpoch: data.endpointEpoch } : {},
            ...typeof data.originEpoch === "string" ? { originEpoch: data.originEpoch } : {},
            sentAt: data.sentAt,
            preview: message?.text?.replace(/\s+/g, " ").slice(0, 120) ?? "",
            ...message ? { message } : {}
          });
        }
        break;
      case "parley_ask_settled":
        if (typeof data.messageId === "string") state.outgoing.delete(data.messageId);
        break;
    }
  }
  for (const id of state.persistedIncoming) {
    const context = state.incoming.get(id);
    if (!context) continue;
    const message = context.message;
    if (!message.replyTo || message.completesAsk === false || message.expectsReply) continue;
    const ask = state.outgoing.get(message.replyTo);
    if (ask && matchesAskCounterpart(ask, context.from)) state.outgoing.delete(message.replyTo);
  }
  return state;
}
var init_conversation_history = __esm({
  "conversation-history.ts"() {
  }
});

// cwd.ts
import { resolve as resolve2 } from "node:path";
import { realpathSync } from "node:fs";
function normalizeCwd(cwd) {
  const cached = normalizeCache.get(cwd);
  if (cached !== void 0) {
    return cached;
  }
  const resolved = resolve2(cwd);
  let normalized;
  try {
    normalized = realpathSync(resolved);
  } catch {
    normalized = resolved;
  }
  normalizeCache.set(cwd, normalized);
  return normalized;
}
function sameCwd(a, b) {
  return normalizeCwd(a) === normalizeCwd(b);
}
var normalizeCache;
var init_cwd = __esm({
  "cwd.ts"() {
    normalizeCache = /* @__PURE__ */ new Map();
  }
});

// message-results.ts
function formatDeliveryResult(result, context) {
  const messageName = context.messageRef ?? ((id) => id);
  const recipient = result.recipient;
  const actualTarget = recipient ? context.sessionRef ? context.sessionRef(recipient) : `${recipient.name || recipient.id} (${recipient.id})` : context.target;
  const identity = `as ${context.sender} to ${actualTarget}`;
  const lines = [];
  if (!result.outcomeKnown || result.delivery === "unknown") {
    lines.push(
      `${context.kind} delivery outcome unknown ${identity}.`,
      "The message may have arrived; sending it again could repeat it."
    );
  } else if (!result.delivered) {
    lines.push(`${context.kind} not delivered ${identity}.`);
  } else if (result.delivery === "queued") {
    lines.push(
      `${context.kind} queued ${identity}.`,
      "Recipient offline; queued for up to 24 hours while this broker remains running."
    );
  } else {
    lines.push(`${context.kind} sent ${identity}.`);
  }
  lines.push(`Message: ${messageName(result.id)}${result.delivered && result.delivery === "socket_delivered" ? " \xB7 endpoint accepted" : ""}`);
  if (result.reason) lines.push(`Reason: ${result.reason}`);
  if (result.code) lines.push(`Outcome code: ${result.code}`);
  if (result.code === "E_REPLY_TARGET") {
    lines.push("Parley no longer knows that thread, usually because a broker restarted or a session reconnected after the original message. Send a new message instead of a reply; nothing was delivered.");
  }
  if (result.peerCompaction) {
    lines.push(formatPeerCompactionNotice(context.target, result.peerCompaction, recipient?.id, context.sessionName));
  }
  return lines.join("\n");
}
function formatCancellationResult(result, messageRef = (id) => id) {
  const message = messageRef(result.id);
  if (!result.outcomeKnown || result.delivery === "unknown") {
    return `Cancellation outcome unknown for ${message}. The request may still be actionable.${result.reason ? ` ${result.reason}` : ""}`;
  }
  if (!result.delivered) {
    return `Cancellation was not accepted for ${message}.${result.reason ? ` ${result.reason}` : ""}`;
  }
  if (result.cancellation === "removed_from_mailbox") {
    return `Cancelled ${message}: removed from the offline mailbox before delivery.`;
  }
  if (result.cancellation === "not_delivered") {
    return `Cancelled ${message}: the original message was not delivered.`;
  }
  return `Withdrawal requested for ${message}. Work may already have happened; this does not undo it.`;
}
var init_message_results = __esm({
  "message-results.ts"() {
    init_compaction_awareness();
  }
});

// link/paths.ts
import { createHash as createHash2 } from "node:crypto";
import { join as join6, win32 as win322 } from "node:path";
function getLinkPaths(env = process.env, platform = process.platform) {
  const agentDir = getAgentDirPath(env);
  const parleyDir = getParleyDirPath(agentDir);
  let control = join6(parleyDir, "link.sock");
  if (platform === "win32") {
    const digest = createHash2("sha256").update("pi-parley-link\0").update(win322.normalize(agentDir).toLowerCase()).digest("hex");
    control = `\\\\.\\pipe\\pi-parley-link-${digest}`;
  }
  return Object.freeze({
    parleyDir,
    config: join6(parleyDir, "links.json"),
    control,
    lock: join6(parleyDir, "link.keeper"),
    sshControlDir: join6(parleyDir, "ssh"),
    log: join6(parleyDir, "link.log")
  });
}
var init_paths2 = __esm({
  "link/paths.ts"() {
    init_paths();
  }
});

// link/client.ts
import { spawn as spawn2 } from "node:child_process";
import { randomUUID as randomUUID4 } from "node:crypto";
import { readFileSync as readFileSync5 } from "node:fs";
import net4 from "node:net";
import { dirname as dirname3, join as join7 } from "node:path";
import { fileURLToPath as fileURLToPath3 } from "node:url";
function linksConfigured(env = process.env, byDefault = false) {
  let parsed;
  try {
    parsed = JSON.parse(readFileSync5(getLinkPaths(env).config, "utf8"));
  } catch {
    parsed = void 0;
  }
  if (Array.isArray(parsed?.hosts) && parsed.hosts.length > 0) return true;
  return typeof parsed?.discover === "boolean" ? parsed.discover : byDefault;
}
function startKeeper(env, discoverByDefault) {
  const paths = getLinkPaths(env);
  ensureParleyRuntimeDir(paths.parleyDir);
  const childEnv = {
    ...env,
    PI_CODING_AGENT_DIR: getAgentDirPath(env),
    NODE_NO_WARNINGS: "1",
    PI_PARLEY_LINK_DISCOVER_DEFAULT: discoverByDefault ? "1" : "0"
  };
  const bundledKeeper = bundledScript("keeper");
  if (process.platform === "win32") {
    const launcher = writeWindowsHiddenLauncher(
      bundledKeeper ? `"${nodeCommand()}" "${bundledKeeper}"` : getWindowsBrokerCommandLine(KEEPER_PATH, EXTENSION_DIR2),
      join7(paths.parleyDir, "link-launch.vbs")
    );
    spawn2("wscript.exe", ["//E:VBScript", launcher], { detached: true, stdio: "ignore", windowsHide: true, env: childEnv }).unref();
    return;
  }
  spawn2(nodeCommand(), bundledKeeper ? [bundledKeeper] : [getTsxCliPath(EXTENSION_DIR2), KEEPER_PATH], {
    detached: true,
    stdio: "ignore",
    cwd: EXTENSION_DIR2,
    env: childEnv
  }).unref();
}
function connectOnce(path2) {
  return new Promise((resolve4, reject) => {
    const socket = net4.connect(path2);
    socket.once("connect", () => {
      socket.off("error", reject);
      resolve4(socket);
    });
    socket.once("error", reject);
  });
}
async function connectLinkKeeper(options2 = {}) {
  const env = { ...options2.env ?? process.env };
  const paths = getLinkPaths(env);
  const deadline = Date.now() + (options2.timeoutMs ?? 1e4);
  let socket;
  let started = false;
  while (!socket) {
    try {
      socket = await connectOnce(paths.control);
    } catch (error) {
      if (Date.now() > deadline) throw new Error("Parley's link keeper didn't start", { cause: error });
      if (!started) {
        startKeeper(env, options2.discoverByDefault === true);
        started = true;
      }
      await new Promise((r) => setTimeout(r, 150));
    }
  }
  const pending = /* @__PURE__ */ new Map();
  const listeners = /* @__PURE__ */ new Set();
  let resolveClosed;
  const closed = new Promise((r) => {
    resolveClosed = r;
  });
  const connected = socket;
  connected.on("data", createMessageReader((value) => {
    const message = value;
    if (message?.type === "event") {
      for (const listener of listeners) listener(message);
      return;
    }
    if (message?.type === "result") {
      pending.get(message.requestId)?.(message);
      pending.delete(message.requestId);
    }
  }, () => connected.destroy()));
  connected.on("error", () => {
  });
  connected.on("close", () => {
    for (const settle of pending.values()) settle({ type: "result", requestId: "", ok: false, error: "the link keeper went away" });
    pending.clear();
    resolveClosed();
  });
  connected.unref();
  return {
    request(command2, requestOptions = {}) {
      const requestId = randomUUID4();
      return new Promise((resolve4) => {
        const timer = setTimeout(() => {
          pending.delete(requestId);
          resolve4({ type: "result", requestId, ok: false, error: "the link keeper didn't answer in time" });
        }, requestOptions.timeoutMs ?? 6e4);
        pending.set(requestId, (response) => {
          clearTimeout(timer);
          resolve4(response);
        });
        try {
          writeMessage(connected, { ...command2, requestId });
        } catch {
        }
      });
    },
    onEvent(listener) {
      listeners.add(listener);
      return () => listeners.delete(listener);
    },
    closed,
    close() {
      connected.destroy();
    }
  };
}
function describeLinks(status) {
  const lines = [`This computer is ${status.label}. Finding computers it has ssh connections to: ${status.discover ? "on" : "off"}.`];
  if (status.hosts.length === 0) lines.push("No other computers yet.");
  for (const host of status.hosts) {
    const how = host.via === "found" ? "found" : "added";
    lines.push(`\u2022 ${host.label} (${host.destination}, ${how}${host.remembered ? "" : ", not remembered"}): ${host.state}${host.detail ? ` \u2014 ${host.detail}` : ""}`);
  }
  for (const relay of status.relays) {
    lines.push(`\u2022 ${relay.between[0]} \u2194 ${relay.between[1]} through this computer: ${relay.state}${relay.detail ? ` \u2014 ${relay.detail}` : ""}`);
  }
  return lines.join("\n");
}
var KEEPER_PATH, EXTENSION_DIR2;
var init_client2 = __esm({
  "link/client.ts"() {
    init_framing();
    init_paths();
    init_spawn();
    init_paths2();
    init_bundle();
    KEEPER_PATH = join7(dirname3(fileURLToPath3(import.meta.url)), "keeper.ts");
    EXTENSION_DIR2 = join7(dirname3(fileURLToPath3(import.meta.url)), "..");
  }
});

// link/session.ts
function hostSentence(host) {
  switch (host.state) {
    case "linked":
      return `${host.label} is linked; its sessions appear here as ${host.label}:\u2026`;
    case "checking":
      return `${host.label} is still being checked.`;
    case "same-computer":
      return `${host.label} is ${host.detail ?? "a computer already listed"}.`;
    case "no-parley":
      return `${host.label} is reachable, but Parley has never run there.`;
    default:
      return `${host.label}: ${host.state}${host.detail ? ` \u2014 ${host.detail}` : ""}. Parley keeps trying.`;
  }
}
var CONFIG_CHECK_MS, SessionLinks, LINK_USAGE;
var init_session = __esm({
  "link/session.ts"() {
    init_client2();
    CONFIG_CHECK_MS = 15e3;
    SessionLinks = class {
      /** notify reaches the person only (UI), never the model. byDefault says whether
       * this session links computers when nobody has said otherwise. */
      constructor(notify, byDefault = () => false) {
        this.notify = notify;
        this.byDefault = byDefault;
      }
      notify;
      byDefault;
      connection;
      connecting;
      timer;
      active = false;
      /** This session's environment as it started: which agent directory, which defaults. */
      env = { ...process.env };
      /** Stay connected to the keeper whenever this computer links with others. */
      start(env = process.env) {
        this.active = true;
        this.env = { ...env };
        const byDefault = this.byDefault();
        const tick = () => {
          if (!this.active) return;
          if (!this.connection && !this.connecting && linksConfigured(this.env, byDefault)) void this.connect().catch(() => {
          });
        };
        tick();
        this.timer = setInterval(tick, CONFIG_CHECK_MS);
        this.timer.unref();
      }
      stop() {
        this.active = false;
        clearInterval(this.timer);
        this.connection?.close();
        this.connection = void 0;
      }
      connect() {
        if (this.connection) return Promise.resolve(this.connection);
        this.connecting ??= connectLinkKeeper({ env: this.env, discoverByDefault: this.byDefault() }).then((connection) => {
          if (!this.active) {
            connection.close();
            throw new Error("session ended");
          }
          this.connection = connection;
          connection.onEvent((event) => {
            if (event.event === "linked") this.notify(`Parley linked ${event.label}`);
          });
          void connection.closed.then(() => {
            if (this.connection === connection) this.connection = void 0;
          });
          return connection;
        }).finally(() => {
          this.connecting = void 0;
        });
        return this.connecting;
      }
      async run(command2) {
        let connection;
        try {
          connection = await this.connect();
        } catch (error) {
          return { ok: false, text: `Couldn't reach Parley's link keeper: ${error instanceof Error ? error.message : String(error)}` };
        }
        const response = await connection.request(command2, { timeoutMs: 9e4 });
        if (!response.ok) {
          return { ok: false, text: response.status ? `${response.error}

${describeLinks(response.status)}` : response.error, status: response.status };
        }
        const lead = response.host ? hostSentence(response.host) : void 0;
        return {
          ok: true,
          text: lead ? `${lead}

${describeLinks(response.status)}` : describeLinks(response.status),
          status: response.status,
          ...response.host ? { host: response.host } : {}
        };
      }
      /** The parley tool's link/unlink actions. */
      tool(params) {
        const host = params.host?.trim() || void 0;
        if (params.action === "unlink") {
          if (!host) return Promise.resolve({ ok: false, text: "unlink needs host." });
          return this.run({ type: "remove", destination: host });
        }
        return (async () => {
          let last;
          if (typeof params.discover === "boolean") {
            last = await this.run({ type: "discover", on: params.discover });
            if (!last.ok) return last;
          }
          if (host && !params.recheck) return this.run({ type: "add", destination: host });
          if (params.recheck) return this.run({ type: "recheck", ...host ? { destination: host } : {} });
          return last ?? this.run({ type: "status" });
        })();
      }
      /** `/parley link [on|off|add <host>|recheck [host]|remove <host>]`. */
      command(args) {
        const [verb = "", ...rest2] = args.trim().split(/\s+/).filter(Boolean);
        const target2 = rest2.join(" ") || void 0;
        switch (verb.toLowerCase()) {
          case "":
          case "status":
            return this.run({ type: "status" });
          case "on":
            return this.run({ type: "discover", on: true });
          case "off":
            return this.run({ type: "discover", on: false });
          case "add":
            return target2 ? this.run({ type: "add", destination: target2 }) : Promise.resolve({ ok: false, text: LINK_USAGE });
          case "recheck":
            return this.run({ type: "recheck", ...target2 ? { destination: target2 } : {} });
          case "remove":
            return target2 ? this.run({ type: "remove", destination: target2 }) : Promise.resolve({ ok: false, text: LINK_USAGE });
          default:
            return Promise.resolve({ ok: false, text: LINK_USAGE });
        }
      }
    };
    LINK_USAGE = "Usage: /parley link [on | off | add <host> | recheck [host] | remove <host>]. <host> is what you'd type after ssh.";
  }
});

// link/completions.ts
import { readdirSync as readdirSync2, readFileSync as readFileSync6 } from "node:fs";
import { homedir as homedir2 } from "node:os";
import { basename, dirname as dirname4, isAbsolute as isAbsolute2, join as join8 } from "node:path";
function linkSuggestions(text, env = process.env) {
  const match = /^(\S*)(\s+(.*))?$/.exec(text);
  const verb = match?.[1] ?? "";
  if (match?.[2] === void 0) {
    return VERBS.filter((item) => item.value.startsWith(verb.toLowerCase())).map((item) => ({ ...item, value: ["add", "recheck", "remove"].includes(item.value) ? `${item.value} ` : item.value }));
  }
  const typed = match[3] ?? "";
  const hosts = verb === "add" ? [.../* @__PURE__ */ new Set([...sshConfigHosts(env), ...rememberedHosts(env)])].filter((host) => !/(^|\.)(github|gitlab|bitbucket)\./i.test(host)) : verb === "recheck" || verb === "remove" ? rememberedHosts(env) : [];
  return hosts.filter((host) => host.startsWith(typed)).slice(0, 50).map((host) => ({ value: `${verb} ${host}`, label: host }));
}
function rememberedHosts(env) {
  try {
    const parsed = JSON.parse(readFileSync6(getLinkPaths(env).config, "utf8"));
    return Array.isArray(parsed?.hosts) ? parsed.hosts.map((h) => h?.destination).filter((d) => typeof d === "string") : [];
  } catch {
    return [];
  }
}
function sshConfigHosts(env = process.env, home = env.HOME || homedir2()) {
  const hosts = [];
  const seen = /* @__PURE__ */ new Set();
  const visit = (file, depth) => {
    if (depth > 3 || seen.has(file)) return;
    seen.add(file);
    let text;
    try {
      text = readFileSync6(file, "utf8");
    } catch {
      return;
    }
    for (const raw of text.split("\n")) {
      const line = raw.replace(/#.*/, "").trim();
      const [keyword, ...values] = line.split(/\s+/);
      if (!keyword) continue;
      if (/^host$/i.test(keyword)) {
        for (const value of values) if (!/[*?!]/.test(value) && !hosts.includes(value)) hosts.push(value);
      } else if (/^include$/i.test(keyword)) {
        for (const value of values) {
          const path2 = isAbsolute2(value) ? value : value.startsWith("~/") ? join8(home, value.slice(2)) : join8(home, ".ssh", value);
          if (!/[*?]/.test(path2)) {
            visit(path2, depth + 1);
            continue;
          }
          const dir = dirname4(path2);
          if (/[*?]/.test(dir)) continue;
          const pattern = new RegExp(`^${basename(path2).replace(/[.+^${}()|[\]\\]/g, "\\$&").replace(/\*/g, ".*").replace(/\?/g, ".")}$`);
          let names = [];
          try {
            names = readdirSync2(dir).filter((name) => pattern.test(name)).sort();
          } catch {
          }
          for (const name of names) visit(join8(dir, name), depth + 1);
        }
      }
    }
  };
  visit(join8(home, ".ssh", "config"), 0);
  return hosts;
}
var VERBS;
var init_completions = __esm({
  "link/completions.ts"() {
    init_paths2();
    VERBS = [
      { value: "on", label: "on", description: "Link computers this one has ssh connections to" },
      { value: "off", label: "off", description: "Stop finding computers; remembered ones stay linked" },
      { value: "add", label: "add", description: "Link a computer, as you'd type it after ssh" },
      { value: "recheck", label: "recheck", description: "Check one computer, or all, again now" },
      { value: "remove", label: "remove", description: "Stop linking a computer and forget it" },
      { value: "status", label: "status", description: "What's linked, and why anything isn't" }
    ];
  }
});

// herdr/socket.ts
import net5 from "node:net";
import { randomUUID as randomUUID5 } from "node:crypto";
function herdrRequest(socketPath, method, params, timeoutMs = 3e3) {
  return new Promise((resolve4, reject) => {
    const id = `parley:${randomUUID5()}`;
    const socket = net5.connect(socketPath);
    let buffer = "";
    const done = (error, value) => {
      clearTimeout(timer);
      socket.destroy();
      if (error) reject(error);
      else resolve4(value);
    };
    const timer = setTimeout(() => done(new Error(`herdr ${method} timed out`)), timeoutMs);
    timer.unref?.();
    socket.on("connect", () => socket.write(`${JSON.stringify({ id, method, params })}
`));
    socket.on("data", (chunk) => {
      buffer += chunk.toString("utf8");
      let newline2;
      while ((newline2 = buffer.indexOf("\n")) >= 0) {
        const line = buffer.slice(0, newline2);
        buffer = buffer.slice(newline2 + 1);
        let message;
        try {
          message = JSON.parse(line);
        } catch {
          continue;
        }
        if (message.id !== id) continue;
        if (message.error) done(new Error(message.error.message ?? `herdr ${method} failed`));
        else done(void 0, message.result);
      }
    });
    socket.on("error", (error) => done(error));
    socket.on("close", () => done(new Error("herdr closed the connection")));
  });
}
function herdrSubscribe(socketPath, types, handlers) {
  const socket = net5.connect(socketPath);
  let buffer = "";
  let ready = false;
  socket.on("connect", () => socket.write(`${JSON.stringify({
    id: `parley:subscribe:${randomUUID5()}`,
    method: "events.subscribe",
    params: { subscriptions: types.map((type) => ({ type })) }
  })}
`));
  socket.on("data", (chunk) => {
    buffer += chunk.toString("utf8");
    let newline2;
    while ((newline2 = buffer.indexOf("\n")) >= 0) {
      const line = buffer.slice(0, newline2);
      buffer = buffer.slice(newline2 + 1);
      let message;
      try {
        message = JSON.parse(line);
      } catch {
        continue;
      }
      if (!ready && message.result?.type === "subscription_started") {
        ready = true;
        handlers.onReady();
        continue;
      }
      if (typeof message.event === "string" && message.data && typeof message.data === "object") {
        handlers.onEvent({ event: message.event, data: message.data });
      }
    }
  });
  socket.on("error", () => {
  });
  socket.on("close", () => handlers.onClose());
  socket.unref();
  return { close: () => socket.destroy() };
}
var init_socket = __esm({
  "herdr/socket.ts"() {
  }
});

// herdr/names.ts
function cleanName(name) {
  return name.replace(/^(?:[π\u03C0]\s*[-–—:]\s*|pi\s+[-–—:]\s+)/i, "").trim();
}
function registerHerdrNames(pi, options2 = {}) {
  const env = options2.env ?? process.env;
  const socketPath = env.HERDR_SOCKET_PATH?.trim();
  const paneId = env.HERDR_PANE_ID?.trim();
  if (env.HERDR_ENV !== "1" || !socketPath || !paneId || env.FLIGHTDECK_TAB_ID?.trim()) return;
  let claimed = false;
  pi.events.emit(CLAIM_EVENT, { claim: () => claimed = true });
  if (claimed) return;
  const releaseClaim = pi.events.on(CLAIM_EVENT, (payload) => payload?.claim?.());
  const isChild = env.PI_SUBAGENT_CHILD === "1" || Boolean(env.PI_SUBAGENT_CHILD_AGENT?.trim());
  let pane;
  let tab;
  let workspace;
  let lastApplied;
  let stopped = false;
  let subscription;
  const timers = /* @__PURE__ */ new Set();
  const later = (fn, ms, repeat = false) => {
    const timer = repeat ? setInterval(fn, ms) : setTimeout(() => {
      timers.delete(timer);
      fn();
    }, ms);
    timer.unref?.();
    timers.add(timer);
  };
  const place = () => cleanName(pane?.label || "") || cleanName(tab?.label || "") || paneId;
  const prefix = () => `${workspace?.label || "herdr"}:${place()}`;
  const placeFrom = (sessionName) => {
    const name = cleanName(sessionName);
    const qualifier = `${workspace?.label || "herdr"}:`;
    return name.startsWith(qualifier) ? cleanName(name.slice(qualifier.length)) : name;
  };
  function apply() {
    if (!workspace) return;
    let wanted = prefix();
    if (isChild) {
      const current = cleanName(pi.getSessionName()?.trim() || "");
      wanted = current.startsWith(`${prefix()}:`) ? current : `${prefix()}:${current || "agent"}`;
    }
    lastApplied = wanted;
    if (pi.getSessionName() !== wanted) pi.setSessionName(wanted);
  }
  async function refresh(applyNames = true) {
    try {
      const paneResult = await herdrRequest(socketPath, "pane.get", { pane_id: paneId });
      const [tabResult, workspaceResult] = await Promise.all([
        herdrRequest(socketPath, "tab.get", { tab_id: paneResult.pane.tab_id }),
        herdrRequest(socketPath, "workspace.get", { workspace_id: paneResult.pane.workspace_id })
      ]);
      if (stopped) return;
      pane = paneResult.pane;
      tab = tabResult.tab;
      workspace = workspaceResult.workspace;
      if (applyNames) apply();
    } catch {
    }
  }
  async function renamedInPi(name) {
    const raw = name?.trim();
    if (!raw || raw === lastApplied || !workspace) return;
    if (isChild) {
      apply();
      return;
    }
    const next = placeFrom(raw);
    if (!next || next === place()) {
      apply();
      return;
    }
    try {
      if (pane?.label || (tab?.pane_count ?? 1) > 1) {
        const result = await herdrRequest(socketPath, "pane.rename", { pane_id: paneId, label: next });
        pane = result.pane;
      } else if (tab) {
        const result = await herdrRequest(socketPath, "tab.rename", { tab_id: tab.tab_id, label: next });
        tab = result.tab ?? { ...tab, label: next };
      }
    } catch {
    }
    apply();
  }
  function subscribe() {
    if (stopped) return;
    subscription = herdrSubscribe(socketPath, ["workspace.renamed", "tab.renamed", "pane.moved", "pane.updated"], {
      onReady: () => void refresh(),
      onEvent: ({ event, data }) => {
        if (event === "workspace_renamed" && data.workspace_id === workspace?.workspace_id && typeof data.label === "string") {
          workspace = { ...workspace, label: data.label };
          apply();
        } else if (event === "tab_renamed" && data.tab_id === tab?.tab_id && typeof data.label === "string") {
          tab = { ...tab, label: data.label };
          apply();
        } else if (data.pane_id === paneId) {
          void refresh();
        }
      },
      onClose: () => {
        if (!stopped) later(subscribe, RESUBSCRIBE_MS);
      }
    });
  }
  pi.on("session_start", async () => {
    const existing = pi.getSessionName()?.trim();
    const resumed = !isChild && Boolean(existing);
    await refresh(!resumed);
    if (resumed) await renamedInPi(existing);
    subscribe();
    later(() => void refresh(), PANE_LABEL_POLL_MS, true);
    if (options2.sessionRenamesArrive === false) {
      later(() => {
        const current = pi.getSessionName();
        if (current !== lastApplied) void renamedInPi(current);
      }, 1e3, true);
    }
  });
  const onSessionInfoChanged = pi.on.bind(pi);
  onSessionInfoChanged("session_info_changed", (event) => void renamedInPi(event?.name));
  pi.on("session_shutdown", () => {
    stopped = true;
    subscription?.close();
    for (const timer of timers) {
      clearTimeout(timer);
      clearInterval(timer);
    }
    timers.clear();
    releaseClaim?.();
  });
}
var CLAIM_EVENT, PANE_LABEL_POLL_MS, RESUBSCRIBE_MS;
var init_names = __esm({
  "herdr/names.ts"() {
    init_socket();
    CLAIM_EVENT = "pi-parley:herdr-names:v1";
    PANE_LABEL_POLL_MS = 15e3;
    RESUBSCRIBE_MS = 3e3;
  }
});

// project-agent.ts
import { spawn as spawn3 } from "child_process";
import { realpathSync as realpathSync2, statSync } from "fs";
import { resolve as resolve3 } from "path";
function projectLaunchRequestText(request) {
  return JSON.stringify(request);
}
function findProjectLaunchProvider(sessions, currentSessionId) {
  return sessions.filter((candidate) => candidate.id !== currentSessionId && candidate.federation === void 0 && candidate.extensions?.some((extension) => extension.namespace === PROJECT_LAUNCH_NAMESPACE)).sort((left, right) => (left.startedAt ?? 0) - (right.startedAt ?? 0) || left.id.localeCompare(right.id))[0];
}
function resolveProjectLauncherCommand(env = process.env, configured) {
  const fromEnv = env.PI_PARLEY_PROJECT_LAUNCHER?.trim();
  if (fromEnv) return fromEnv;
  const fromConfig = configured?.trim();
  return fromConfig ? fromConfig : void 0;
}
function shellQuotePath(value) {
  if (process.platform === "win32") return `"${value.replaceAll('"', '\\"')}"`;
  return `'${value.replaceAll("'", "'\\''")}'`;
}
async function launchProjectCommand(command2, root2, options2 = {}) {
  if (options2.signal?.aborted) throw new LaunchCommandError("Project launcher command was cancelled before startup.", "not-started");
  if (!command2.trim()) throw new LaunchCommandError("Project launcher command must not be empty.", "not-started");
  if (command2.length > LAUNCH_COMMAND_MAX_LENGTH) {
    throw new LaunchCommandError(`Project launcher command must be at most ${LAUNCH_COMMAND_MAX_LENGTH} characters.`, "not-started");
  }
  const commandLine = command2.replaceAll("{root}", shellQuotePath(root2));
  const spawnImpl = options2.spawnImpl ?? spawn3;
  await new Promise((resolveLaunch, rejectLaunch) => {
    let child;
    let settled = false;
    const windowMs = options2.failureWindowMs ?? LAUNCH_COMMAND_FAILURE_WINDOW_MS;
    try {
      child = spawnImpl(commandLine, {
        shell: true,
        windowsHide: true,
        cwd: root2,
        env: {
          ...process.env,
          PI_PARLEY_PROJECT_ROOT: root2
        },
        detached: true
      });
    } catch (cause) {
      rejectLaunch(new LaunchCommandError(`Failed to start the project launcher: ${errorText(cause)}`, "not-started"));
      return;
    }
    const finish = (error) => {
      if (settled) return;
      settled = true;
      clearTimeout(window);
      options2.signal?.removeEventListener("abort", onAbort);
      error ? rejectLaunch(error) : resolveLaunch();
    };
    const onAbort = () => {
      try {
        child.kill();
      } catch {
      }
      finish(new LaunchCommandError("Project launcher command was aborted after startup was attempted.", "unknown"));
    };
    const window = setTimeout(() => finish(), windowMs);
    window.unref?.();
    let spawned = false;
    child.on("spawn", () => {
      spawned = true;
    });
    child.on("error", (cause) => finish(new LaunchCommandError(
      `Project launcher command failed: ${cause.message}`,
      spawned || child.pid !== void 0 ? "unknown" : "not-started"
    )));
    child.on("close", (exitCode, signal) => {
      if (exitCode !== 0) {
        finish(new LaunchCommandError(
          signal ? `Project launcher command ended on signal ${signal}.` : `Project launcher command exited with code ${exitCode}.`,
          "unknown"
        ));
        return;
      }
      finish();
    });
    if (options2.signal?.aborted) {
      onAbort();
      return;
    }
    options2.signal?.addEventListener("abort", onAbort, { once: true });
  });
}
function resolveTargetInCwd(input) {
  const inCwd = input.sessions.filter((session) => session.federation === void 0 && sameCwd(session.cwd, input.targetCwd));
  const target2 = input.to?.trim();
  if (!target2) {
    const candidates = inCwd.filter((session) => session.id !== input.currentSessionId);
    if (candidates.length === 1) {
      return { kind: "found", session: candidates[0], targetCwd: input.targetCwd };
    }
    if (candidates.length === 0) {
      return { kind: "missing", targetCwd: input.targetCwd, reason: `No other local parley sessions are visible in ${input.targetCwd}.` };
    }
    throw new Error(`Multiple parley sessions are connected in ${input.targetCwd}: ${formatSessionRefs(candidates, input.sessionRef)}. Specify 'to'.`);
  }
  const byId = inCwd.find((session) => session.id === target2);
  if (byId) return { kind: "found", session: byId, targetCwd: input.targetCwd };
  const lowerName = target2.toLowerCase();
  const byName = inCwd.filter((session) => session.name?.toLowerCase() === lowerName);
  if (byName.length === 1) return { kind: "found", session: byName[0], targetCwd: input.targetCwd };
  if (byName.length > 1) {
    throw new Error(`Multiple parley sessions named "${target2}" are connected in ${input.targetCwd}: ${formatSessionRefs(byName, input.sessionRef)}. Address one by the reference shown.`);
  }
  const byIdPrefix = inCwd.filter((session) => session.id.startsWith(target2));
  if (byIdPrefix.length === 1) return { kind: "found", session: byIdPrefix[0], targetCwd: input.targetCwd };
  if (byIdPrefix.length > 1) {
    throw new Error(`Multiple parley sessions in ${input.targetCwd} match ID prefix "${target2}". Use a longer session ID prefix.`);
  }
  return { kind: "missing", targetCwd: input.targetCwd, reason: `No local parley session matching "${target2}" is visible in ${input.targetCwd}.` };
}
async function openProjectPane(input) {
  if (input.signal?.aborted) {
    throw new ProjectLaunchError("Project launch was cancelled before it was requested.", { stage: "launch" });
  }
  let projectRoot;
  try {
    projectRoot = resolveProjectRoot(input.cwd);
  } catch (cause) {
    throw new ProjectLaunchError(errorText(cause), { stage: "launch", cause });
  }
  const command2 = process.env.PI_PARLEY_PI_BIN?.trim() || process.env.PI_BIN?.trim() || "pi";
  const provider = findProjectLaunchProvider(input.sessions, input.currentSessionId);
  if (provider) {
    const launch = {
      projectRoot,
      provider: { kind: "session", sessionId: provider.id, name: provider.name ?? provider.id },
      outcome: "unknown"
    };
    const request = {
      type: PROJECT_LAUNCH_REQUEST_TYPE,
      root: projectRoot,
      command: command2,
      focus: input.focus !== false
    };
    let sent;
    try {
      sent = await input.sendRequest(provider, request);
    } catch (cause) {
      throw new ProjectLaunchError(
        `The launch request to ${provider.name ?? provider.id} has an unknown outcome: ${errorText(cause)} The provider may already have acted; another launch could create a duplicate.`,
        { stage: "launch", launch, cause }
      );
    }
    const receipt = {
      ...launch,
      outcome: sent.delivered ? "request-accepted" : sent.outcomeKnown === true ? "not-started" : "unknown",
      ...sent.id ? { requestMessageId: sent.id } : {}
    };
    if (!sent.delivered) {
      throw new ProjectLaunchError(
        `The launch request to ${provider.name ?? provider.id} was not confirmed accepted: ${sent.reason ?? "no delivery confirmation."}` + (receipt.outcome === "unknown" ? " The provider may already have acted; another launch could create a duplicate." : " No launch request was accepted."),
        { stage: "launch", launch: receipt }
      );
    }
    return receipt;
  }
  if (input.launcherCommand) {
    const launch = {
      projectRoot,
      provider: { kind: "command", command: input.launcherCommand },
      outcome: "unknown"
    };
    try {
      await launchProjectCommand(input.launcherCommand, projectRoot, { signal: input.signal, spawnImpl: input.spawnImpl });
    } catch (cause) {
      const outcome = cause instanceof LaunchCommandError ? cause.outcome : "unknown";
      throw new ProjectLaunchError(
        errorText(cause) + (outcome === "unknown" ? " The command may already have created resources; another launch could create a duplicate." : " No launcher command was started."),
        { stage: "launch", launch: { ...launch, outcome }, cause }
      );
    }
    return { ...launch, outcome: "command-started" };
  }
  throw new ProjectLaunchError(
    `No project launcher is available. No launch was attempted. Local project launch is supported through a visible session advertising the "${PROJECT_LAUNCH_NAMESPACE}" capability or a configured PI_PARLEY_PROJECT_LAUNCHER (config "projectLauncher"). Neither is available in this session; parley does not supply a terminal manager.`,
    { stage: "launch" }
  );
}
async function waitForProjectSession(client, input) {
  const deadline = Date.now() + (input.timeoutMs ?? DEFAULT_PROJECT_AGENT_TIMEOUT_MS);
  const pollMs = input.pollMs ?? DEFAULT_PROJECT_AGENT_POLL_MS;
  try {
    while (Date.now() < deadline) {
      if (input.signal?.aborted) throw new Error("Cancelled while waiting for project registration.");
      const sessions = await client.listSessions({ timeoutMs: Math.min(5e3, deadline - Date.now()) });
      if (input.signal?.aborted) throw new Error("Cancelled while waiting for project registration.");
      if (Date.now() >= deadline) break;
      const newInProject = sessions.filter(
        (session) => session.federation === void 0 && session.id !== input.currentSessionId && !input.beforeSessionIds.has(session.id) && sameCwd(session.cwd, input.projectRoot)
      );
      if (newInProject.length === 1) return newInProject[0];
      if (newInProject.length > 1) {
        throw new Error(`Multiple new local parley sessions are visible in ${input.projectRoot}: ${formatSessionRefs(newInProject, input.sessionRef)}. Their relationship to the launch is unknown.`);
      }
      await sleep2(Math.min(pollMs, Math.max(0, deadline - Date.now())), input.signal);
    }
    throw new Error(`Timed out waiting for a local Pi parley session to register in ${input.projectRoot}. The project launcher may still be starting, or parley may not be loaded there.`);
  } catch (cause) {
    throw new ProjectLaunchError(
      errorText(cause) + (input.launch ? " The launch attempt has not been undone; another launch could create a duplicate." : ""),
      { stage: "registration", launch: input.launch, cause }
    );
  }
}
function resolveProjectRoot(cwd) {
  const resolved = resolve3(cwd);
  const stat = statSync(resolved);
  if (!stat.isDirectory()) {
    throw new Error(`Project target '${resolved}' is not a directory.`);
  }
  return realpathSync2(resolved);
}
function errorText(error) {
  return error instanceof Error ? error.message : String(error);
}
function formatSessionRefs(sessions, sessionRef) {
  return sessions.map((session) => sessionRef ? sessionRef(session) : `${session.name || "Unnamed session"} (${session.id})`).join(", ");
}
function sleep2(ms, signal) {
  return new Promise((resolveSleep, reject) => {
    if (signal?.aborted) {
      reject(new Error("Cancelled"));
      return;
    }
    let timer;
    const cleanup = () => signal?.removeEventListener("abort", onAbort);
    const onAbort = () => {
      clearTimeout(timer);
      cleanup();
      reject(new Error("Cancelled"));
    };
    timer = setTimeout(() => {
      cleanup();
      resolveSleep();
    }, ms);
    timer.unref?.();
    signal?.addEventListener("abort", onAbort, { once: true });
  });
}
var DEFAULT_PROJECT_AGENT_TIMEOUT_MS, DEFAULT_PROJECT_AGENT_POLL_MS, LAUNCH_COMMAND_FAILURE_WINDOW_MS, LAUNCH_COMMAND_MAX_LENGTH, PROJECT_LAUNCH_NAMESPACE, PROJECT_LAUNCH_REQUEST_TYPE, ProjectLaunchError, LaunchCommandError;
var init_project_agent = __esm({
  "project-agent.ts"() {
    init_cwd();
    DEFAULT_PROJECT_AGENT_TIMEOUT_MS = 2e4;
    DEFAULT_PROJECT_AGENT_POLL_MS = 250;
    LAUNCH_COMMAND_FAILURE_WINDOW_MS = 1e3;
    LAUNCH_COMMAND_MAX_LENGTH = 1024;
    PROJECT_LAUNCH_NAMESPACE = "pi-parley/project-launch-v1";
    PROJECT_LAUNCH_REQUEST_TYPE = "pi-parley/project-launch-request";
    ProjectLaunchError = class extends Error {
      stage;
      launch;
      session;
      constructor(message, context) {
        super(message, { cause: context.cause });
        this.name = "ProjectLaunchError";
        this.stage = context.stage;
        this.launch = context.launch;
        this.session = context.session;
      }
    };
    LaunchCommandError = class extends Error {
      constructor(message, outcome) {
        super(message);
        this.outcome = outcome;
      }
      outcome;
    };
  }
});

// index.ts
var index_exports = {};
__export(index_exports, {
  default: () => index_default,
  registerParleyExtension: () => registerParleyExtension,
  sessionIdPrefixes: () => sessionIdPrefixes,
  sessionPlace: () => sessionPlace
});
import { randomUUID as randomUUID6 } from "crypto";
import { resolve as resolvePath } from "node:path";
import { AsyncLocalStorage } from "node:async_hooks";
function sessionPlace(env = process.env) {
  if (env.FLIGHTDECK_TAB_ID?.trim() || env.FLIGHTDECK_PANE_ID?.trim()) return "FlightDeck";
  if (env.HERDR_ENV === "1") return "herdr";
  if (env.TMUX_PANE?.trim()) return "tmux";
  return void 0;
}
function appLabel(value) {
  const label = value?.trim();
  return label && label.length <= 64 ? label : void 0;
}
function detectApp(env = process.env) {
  const agent = process.versions.bun ? "Oh My Pi" : "Pi";
  const place = sessionPlace(env);
  return place ? `${agent} in ${place}` : agent;
}
function failureIdentities(error) {
  if (!(error instanceof ProjectLaunchError)) return {};
  return {
    ...error.launch ? { projectLaunch: error.launch } : {},
    ...error.session ? { session: error.session } : {}
  };
}
function getErrorMessage(error, names) {
  if (error instanceof ProjectLaunchError) {
    const launch = error.launch;
    const request = launch?.requestMessageId ? ` (request ${names ? names.message(launch.requestMessageId) : launch.requestMessageId})` : "";
    const receipt = launch ? `
Launch in ${launch.projectRoot}: ${launch.outcome}${request}.` : "";
    const observed = error.session ? `
Observed session: ${names ? names.session(error.session) : `${error.session.name || error.session.id} (${error.session.id})`}.` : "";
    return `${error.message}
Stopped at ${error.stage}.${receipt}${observed}`;
  }
  return error instanceof Error ? error.message : String(error);
}
function deliveryDetails(result) {
  return {
    messageId: result.id,
    delivered: result.delivered,
    delivery: result.delivery,
    retryable: result.retryable,
    outcomeKnown: result.outcomeKnown,
    ...result.code ? { code: result.code } : {},
    ...result.reason ? { reason: result.reason } : {},
    ...result.peerCompaction ? { peerCompaction: result.peerCompaction } : {},
    ...result.recipient ? { recipient: result.recipient } : {},
    ...result.cancellation ? { cancellation: result.cancellation } : {}
  };
}
async function mapWithConcurrency(values, concurrency, mapper) {
  const results = new Array(values.length);
  let nextIndex = 0;
  const workers = Array.from({ length: Math.min(concurrency, values.length) }, async () => {
    while (nextIndex < values.length) {
      const index = nextIndex++;
      results[index] = await mapper(values[index], index);
    }
  });
  await Promise.all(workers);
  return results;
}
function batchSendToolResult(options2) {
  const messageName = options2.message ?? ((id) => id);
  const compactionNotice = options2.compaction ?? formatPeerCompactionNotice;
  const acceptedCount = options2.outcomes.filter((outcome) => outcome.delivered).length;
  const unknownCount = options2.outcomes.filter((outcome) => !outcome.outcomeKnown || outcome.delivery === "unknown").length;
  const failedCount = options2.outcomes.length - acceptedCount - unknownCount;
  const noun = options2.broadcast ? `visible session${options2.outcomes.length === 1 ? "" : "s"}` : `target${options2.outcomes.length === 1 ? "" : "s"}`;
  const heading2 = options2.broadcast ? `Broadcast accepted for ${acceptedCount} of ${options2.outcomes.length} ${noun}.` : `Message accepted for ${acceptedCount} of ${options2.outcomes.length} ${noun}.`;
  const lines = options2.outcomes.map((outcome) => {
    if (outcome.delivered) {
      const state = outcome.delivery === "queued" ? "queued for offline delivery (up to 24h while this broker remains running)" : "sent";
      const deliveryLine = `- \u2713 ${outcome.to}: ${state}${outcome.messageId ? ` (${messageName(outcome.messageId)})` : ""}`;
      return outcome.peerCompaction ? `${deliveryLine}
  ${compactionNotice(outcome.to, outcome.peerCompaction, outcome.targetId)}` : deliveryLine;
    }
    const unknown = !outcome.outcomeKnown || outcome.delivery === "unknown";
    return `- ${unknown ? "?" : "\u2717"} ${outcome.to}: ${unknown ? "outcome unknown; repeating may duplicate delivery \u2014 " : ""}${outcome.reason ?? "delivery failed"}${outcome.messageId ? ` (message ${messageName(outcome.messageId)})` : " (no message created)"}`;
  });
  if (options2.duplicateCount > 0) {
    lines.push(`- Skipped ${options2.duplicateCount} duplicate target${options2.duplicateCount === 1 ? "" : "s"}.`);
  }
  if (options2.broadcast) {
    lines.push("", `Host-local broadcast; ${options2.excludedRemoteCount ?? 0} visible remote peer(s) were not included.`);
  }
  return {
    content: [{ type: "text", text: `${heading2} Sent as ${options2.sender}.
${lines.join("\n")}` }],
    details: {
      ...acceptedCount === 0 ? { error: true } : {},
      batch: true,
      broadcast: options2.broadcast,
      batchId: options2.batchId,
      requestedTargetCount: options2.requestedTargetCount,
      recipientCount: options2.outcomes.length,
      acceptedCount,
      failedCount,
      unknownCount,
      duplicateCount: options2.duplicateCount,
      allAccepted: acceptedCount === options2.outcomes.length,
      outcomes: options2.outcomes
    }
  };
}
function toError3(error) {
  return error instanceof Error ? error : new Error(String(error));
}
function formatAttachments(attachments) {
  let text = "";
  for (const att of attachments) {
    const label = `Attachment snapshot: ${att.name} (${att.type})`;
    if (att.language) {
      const runs = att.content.match(/~+/g) ?? [];
      const fence = "~".repeat(Math.max(3, ...runs.map((run3) => run3.length + 1)));
      text += `

---
${label}
${fence}${att.language}
${att.content}
${fence}`;
    } else {
      text += `

---
${label}
${att.content}`;
    }
  }
  return text;
}
function readChildOrchestratorMetadata() {
  const orchestratorTarget = process.env[SUBAGENT_ORCHESTRATOR_TARGET_ENV]?.trim();
  const orchestratorSessionId = process.env[SUBAGENT_ORCHESTRATOR_SESSION_ID_ENV]?.trim() || process.env[PARLEY_SESSION_ID_ENV]?.trim();
  const runId = process.env[SUBAGENT_RUN_ID_ENV]?.trim();
  const agent = process.env[SUBAGENT_CHILD_AGENT_ENV]?.trim();
  const index = process.env[SUBAGENT_CHILD_INDEX_ENV]?.trim();
  if (!orchestratorTarget || !runId || !agent || !index) {
    return null;
  }
  const sessionName = process.env[SUBAGENT_PARLEY_SESSION_NAME_ENV]?.trim();
  return {
    orchestratorTarget,
    ...orchestratorSessionId ? { orchestratorSessionId } : {},
    runId,
    agent,
    index,
    ...sessionName ? { sessionName } : {}
  };
}
function formatChildOrchestratorMessage(kind, metadata, message) {
  const heading2 = kind === "ask" ? "Subagent needs a supervisor decision." : kind === "interview" ? "Subagent requests a structured supervisor interview." : "Subagent progress update.";
  return [
    heading2,
    `Run: ${metadata.runId}`,
    `Agent: ${metadata.agent}`,
    `Child index: ${metadata.index}`,
    metadata.sessionName ? `Child parley target: ${metadata.sessionName}` : void 0,
    "",
    message
  ].filter((line) => line !== void 0).join("\n");
}
function validateSupervisorInterviewRequest(input) {
  if (!input || typeof input !== "object" || Array.isArray(input)) {
    return { ok: false, error: "interview must be an object with a questions array" };
  }
  const raw = input;
  if (raw.title !== void 0 && typeof raw.title !== "string") {
    return { ok: false, error: "interview.title must be a string when provided" };
  }
  if (raw.description !== void 0 && typeof raw.description !== "string") {
    return { ok: false, error: "interview.description must be a string when provided" };
  }
  if (!Array.isArray(raw.questions) || raw.questions.length === 0) {
    return { ok: false, error: "interview.questions must be a non-empty array" };
  }
  const validTypes = /* @__PURE__ */ new Set(["single", "multi", "text", "image", "info"]);
  const ids = /* @__PURE__ */ new Set();
  const questions = [];
  for (let index = 0; index < raw.questions.length; index++) {
    const questionInput = raw.questions[index];
    if (!questionInput || typeof questionInput !== "object" || Array.isArray(questionInput)) {
      return { ok: false, error: `interview.questions[${index}] must be an object` };
    }
    const question = questionInput;
    if (typeof question.id !== "string" || question.id.trim() === "") {
      return { ok: false, error: `interview.questions[${index}].id must be a non-empty string` };
    }
    const id = question.id.trim();
    if (ids.has(id)) {
      return { ok: false, error: `interview question id must be unique: ${id}` };
    }
    ids.add(id);
    if (typeof question.type !== "string" || !validTypes.has(question.type)) {
      return { ok: false, error: `interview.questions[${index}].type must be one of: single, multi, text, image, info` };
    }
    if (typeof question.question !== "string" || question.question.trim() === "") {
      return { ok: false, error: `interview.questions[${index}].question must be a non-empty string` };
    }
    if (question.context !== void 0 && typeof question.context !== "string") {
      return { ok: false, error: `interview.questions[${index}].context must be a string when provided` };
    }
    let options2;
    if (question.options !== void 0) {
      if (!Array.isArray(question.options)) {
        return { ok: false, error: `interview.questions[${index}].options must be an array when provided` };
      }
      options2 = [];
      for (let optionIndex = 0; optionIndex < question.options.length; optionIndex++) {
        const option = question.options[optionIndex];
        if (typeof option === "string") {
          const label = option.trim();
          if (!label) {
            return { ok: false, error: `interview.questions[${index}].options[${optionIndex}] must not be empty` };
          }
          options2.push(label);
        } else if (!option || typeof option !== "object" || Array.isArray(option) || typeof option.label !== "string" || option.label.trim() === "") {
          return { ok: false, error: `interview.questions[${index}].options[${optionIndex}] must be a non-empty string or an object with a non-empty label` };
        } else {
          options2.push({ ...option, label: option.label.trim() });
        }
      }
    }
    if ((question.type === "single" || question.type === "multi") && (!options2 || options2.length === 0)) {
      return { ok: false, error: `interview.questions[${index}].options must be a non-empty array for ${question.type} questions` };
    }
    if (question.type !== "single" && question.type !== "multi" && options2) {
      return { ok: false, error: `interview.questions[${index}].options is only valid for single and multi questions` };
    }
    questions.push({
      ...question,
      id,
      type: question.type,
      question: question.question.trim(),
      ...options2 ? { options: options2 } : {}
    });
  }
  return {
    ok: true,
    interview: {
      ...raw,
      ...typeof raw.title === "string" ? { title: raw.title.trim() } : {},
      ...typeof raw.description === "string" ? { description: raw.description.trim() } : {},
      questions
    }
  };
}
function interviewOptionLabel(option) {
  return typeof option === "string" ? option : option.label;
}
function interviewExampleValue(question) {
  if (question.type === "multi") {
    return ["<selected option label>"];
  }
  if (question.type === "single") {
    return "<selected option label>";
  }
  if (question.type === "image") {
    return "image/file reference or description";
  }
  return "answer text";
}
function formatSupervisorInterviewRequest(interview, message) {
  const lines = [];
  const title = interview.title?.trim();
  if (title) lines.push(`Interview: ${title}`);
  const description = interview.description?.trim();
  if (description) lines.push(description);
  const note = message?.trim();
  if (note) lines.push(`Child note: ${note}`);
  if (lines.length > 0) lines.push("");
  lines.push("Questions:");
  interview.questions.forEach((question, index) => {
    lines.push(`${index + 1}. [${question.id}] (${question.type}) ${question.question}`);
    if (typeof question.context === "string" && question.context.trim()) {
      lines.push(`   Context: ${question.context.trim()}`);
    }
    if (question.options?.length) {
      lines.push("   Options:");
      for (const option of question.options) {
        lines.push(`   - ${interviewOptionLabel(option)}`);
      }
    }
  });
  const responseExample = {
    responses: interview.questions.filter((question) => question.type !== "info").map((question) => ({
      id: question.id,
      value: interviewExampleValue(question)
    }))
  };
  lines.push(
    "",
    "Answer format:",
    "Answers are parsed by question id: one option label for single, a list of labels for multi, and text for text/image. Info entries are context only.",
    "",
    "```json",
    JSON.stringify(responseExample, null, 2),
    "```"
  );
  return lines.join("\n");
}
function validateSupervisorInterviewReply(value, interview) {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    throw new Error("reply JSON must be an object with a responses array");
  }
  const responsesInput = value.responses;
  if (!Array.isArray(responsesInput)) {
    throw new Error("reply JSON must include a responses array");
  }
  const questionById = new Map(interview.questions.filter((question) => question.type !== "info").map((question) => [question.id, question]));
  const seenIds = /* @__PURE__ */ new Set();
  const responses = [];
  for (let index = 0; index < responsesInput.length; index++) {
    const response = responsesInput[index];
    if (!response || typeof response !== "object" || Array.isArray(response)) {
      throw new Error(`responses[${index}] must be an object`);
    }
    const raw = response;
    if (typeof raw.id !== "string" || raw.id.trim() === "") {
      throw new Error(`responses[${index}].id must be a non-empty string`);
    }
    const id = raw.id.trim();
    const question = questionById.get(id);
    if (!question) {
      throw new Error(`responses[${index}].id must match a non-info interview question id`);
    }
    if (seenIds.has(id)) {
      throw new Error(`responses[${index}].id is duplicated: ${id}`);
    }
    seenIds.add(id);
    if (!Object.hasOwn(raw, "value")) {
      throw new Error(`responses[${index}].value is required`);
    }
    const value2 = raw.value;
    if (question.type === "single") {
      if (typeof value2 !== "string") throw new Error(`responses[${index}].value must be a string for single questions`);
      const optionLabels = new Set(question.options?.map(interviewOptionLabel));
      if (!optionLabels.has(value2.trim())) throw new Error(`responses[${index}].value must match one of the question options`);
      responses.push({ id, value: value2.trim() });
      continue;
    }
    if (question.type === "multi") {
      if (!Array.isArray(value2) || value2.some((item) => typeof item !== "string")) {
        throw new Error(`responses[${index}].value must be an array of strings for multi questions`);
      }
      const optionLabels = new Set(question.options?.map(interviewOptionLabel));
      const selected = value2.map((item) => item.trim());
      const invalid = selected.find((item) => !optionLabels.has(item));
      if (invalid) throw new Error(`responses[${index}].value contains an option that is not in the question options: ${invalid}`);
      responses.push({ id, value: selected });
      continue;
    }
    if (typeof value2 !== "string") {
      throw new Error(`responses[${index}].value must be a string for ${question.type} questions`);
    }
    responses.push({ id, value: value2 });
  }
  return { responses };
}
function parseStructuredSupervisorReply(text, interview) {
  const fencedMatch = text.match(/```(?:json)?\s*([\s\S]*?)```/i);
  const candidate = (fencedMatch?.[1] ?? text).trim();
  if (!candidate.startsWith("{") && !candidate.startsWith("[")) {
    return void 0;
  }
  try {
    return { value: validateSupervisorInterviewReply(JSON.parse(candidate), interview) };
  } catch (error) {
    return { error: getErrorMessage(error) };
  }
}
function sessionIdPrefixes(sessions) {
  const prefixes = /* @__PURE__ */ new Map();
  for (const session of sessions) {
    let longestSharedPrefix = 0;
    for (const other2 of sessions) {
      if (other2.id === session.id) {
        continue;
      }
      let length2 = 0;
      while (length2 < session.id.length && session.id[length2] === other2.id[length2]) {
        length2 += 1;
      }
      longestSharedPrefix = Math.max(longestSharedPrefix, length2);
    }
    const minimumLength = Math.max(8, longestSharedPrefix + 1);
    let groupBoundary = -1;
    for (const separator of ["-", ":"]) {
      const boundary = session.id.indexOf(separator, minimumLength);
      if (boundary !== -1 && (groupBoundary === -1 || boundary < groupBoundary)) {
        groupBoundary = boundary;
      }
    }
    const length = groupBoundary === -1 ? session.id.length : groupBoundary;
    prefixes.set(session.id, session.id.slice(0, length));
  }
  return prefixes;
}
function parseSubagentParleyPayload(payload) {
  if (typeof payload !== "object" || payload === null) {
    return null;
  }
  const record = payload;
  if (typeof record.to !== "string" || typeof record.message !== "string") {
    return null;
  }
  const requestId = typeof record.requestId === "string" ? record.requestId : void 0;
  return { to: record.to, message: record.message, ...requestId ? { requestId } : {} };
}
function parseOutboxRequestPayload(payload) {
  if (typeof payload !== "object" || payload === null || Array.isArray(payload)) {
    return { ok: false, detail: "request must be an object" };
  }
  const record = payload;
  const requestId = typeof record.requestId === "string" && record.requestId.trim() ? record.requestId : void 0;
  const extensionId = typeof record.extensionId === "string" && record.extensionId.trim() ? record.extensionId.trim() : void 0;
  const extensionName = typeof record.extensionName === "string" && record.extensionName.trim() ? record.extensionName.trim() : void 0;
  if (record.version !== 1) {
    return { ok: false, requestId, extensionId, extensionName, detail: "version must be 1" };
  }
  if (!requestId) {
    return { ok: false, extensionId, extensionName, detail: "requestId is required" };
  }
  if (!extensionId) {
    return { ok: false, requestId, extensionName, detail: "extensionId is required" };
  }
  if (!extensionName) {
    return { ok: false, requestId, extensionId, detail: "extensionName is required" };
  }
  if (typeof record.to !== "string" || !record.to.trim()) {
    return { ok: false, requestId, extensionId, extensionName, detail: "to is required" };
  }
  if (typeof record.message !== "string" || !record.message.trim()) {
    return { ok: false, requestId, extensionId, extensionName, detail: "message is required" };
  }
  return {
    ok: true,
    request: {
      version: 1,
      requestId,
      extensionId,
      extensionName,
      to: record.to.trim(),
      message: record.message
    }
  };
}
function resolveParleyPresenceName(sessionName, sessionId) {
  const trimmedName = sessionName?.trim();
  if (trimmedName) {
    return trimmedName;
  }
  const normalizedSessionId = sessionId.startsWith("session-") ? sessionId.slice("session-".length) : sessionId;
  return `${DEFAULT_UNNAMED_SESSION_ALIAS_PREFIX}-${normalizedSessionId.slice(0, 18)}`;
}
function resolveEmbeddedPresenceName(configuration, candidate, context) {
  const resolver = configuration.resolvePresenceName;
  if (!resolver) return candidate;
  configuration.presenceNameResolutionStarted = true;
  const resolved = resolver(candidate, context);
  if (typeof resolved !== "string" || !resolved.trim()) {
    throw new Error(`resolvePresenceName must return a non-empty string for ${context.kind} identity`);
  }
  return resolved.trim();
}
function buildPresenceIdentity(pi, sessionId, configuration) {
  const sessionName = pi.getSessionName()?.trim() || void 0;
  configuration.presenceNameResolutionStarted = true;
  const resolvedName = resolveEmbeddedPresenceName(configuration, sessionName, SESSION_PRESENCE_NAME_CONTEXT);
  return {
    name: resolveParleyPresenceName(resolvedName, sessionId),
    runtimeFallbackAlias: resolvedName === void 0
  };
}
function resolveConfiguredParleySessionId(piSessionId, config) {
  return process.env[STABLE_PARLEY_SESSION_ID_ENV]?.trim() || config.stableId || piSessionId;
}
function currentTmuxPane() {
  const pane = process.env.TMUX_PANE?.trim();
  return pane ? pane : void 0;
}
function formatParleyContactSnippet(sessionId) {
  return `Pi parley target: ${sessionId}`;
}
function formatSessionListRow(session, currentCwd, isSelf, reference, origin) {
  const remote = session.federation ? `remote:${origin ?? session.federation.originLabel ?? "unlabelled host"}; ${session.federation.conversation ? "text conversations (ask/reply)" : "text sends when supported, no asks/replies"}; no attachments` : void 0;
  const tags = [isSelf ? "self" : session.cwd === currentCwd ? "same cwd" : void 0, remote, session.status].filter((tag2) => Boolean(tag2));
  const suffix = tags.length ? ` [${tags.join(", ")}]` : "";
  const pane = session.tmuxPane ? ` \xB7 tmux ${session.tmuxPane}` : "";
  const description = session.description ? ` \u2014 ${session.description}` : "";
  const about = [session.model && session.model !== "unknown" ? session.model : void 0, session.app].filter(Boolean).join(" \xB7 ") || session.model;
  return `\u2022 ${reference}${description} \u2014 ${session.cwd} (${about}${pane})${suffix}`;
}
function previewText(value, maxLength = 72) {
  if (typeof value !== "string") {
    return void 0;
  }
  const normalized = value.replace(/\s+/g, " ").trim();
  if (!normalized) {
    return void 0;
  }
  return normalized.length > maxLength ? `${normalized.slice(0, maxLength - 1)}\u2026` : normalized;
}
function firstTextContent(result) {
  return result.content?.find((item) => item.type === "text" && typeof item.text === "string")?.text?.replace(/\*\*/g, "") ?? "";
}
function formatMessageTimestamp(timestamp) {
  return typeof timestamp === "number" && Number.isFinite(timestamp) ? new Date(timestamp).toISOString() : void 0;
}
function formatInboundDeliveryMetadata(message, messageRef) {
  const parts = [`Message: ${messageRef(message.id)}`];
  if (message.replyTo) parts.push(`Reply to: ${messageRef(message.replyTo)}`);
  if (message.supersedes) parts.push(`Supersedes: ${messageRef(message.supersedes)}`);
  if (message.retryOf) parts.push(`Retry of: ${messageRef(message.retryOf)}`);
  if (message.provenance?.type === "extension_outbox") {
    parts.push(`Via extension ${message.provenance.extensionName} (${message.provenance.extensionId}); request ${message.provenance.requestId}`);
  } else if (message.provenance?.type === "session_person") {
    parts.push("Written by the person at the sending session, not its agent (as reported by that session)");
  }
  if (Date.now() - message.timestamp > 6e4) parts.push(`Originally sent: ${formatMessageTimestamp(message.timestamp)}`);
  return parts.join("\n");
}
function normalizeParleyExtensionOptions(options2) {
  if (options2 === void 0) return Object.freeze({});
  if (!options2 || typeof options2 !== "object") {
    throw new TypeError("Parley extension options must be an object");
  }
  if (options2.resolvePresenceName !== void 0 && typeof options2.resolvePresenceName !== "function") {
    throw new TypeError("resolvePresenceName must be a function");
  }
  if (options2.app !== void 0 && (typeof options2.app !== "string" || !options2.app.trim() || options2.app.length > 64)) {
    throw new TypeError("app must be a short label (1-64 characters)");
  }
  if (options2.resolveApp !== void 0 && typeof options2.resolveApp !== "function") {
    throw new TypeError("resolveApp must be a function");
  }
  return Object.freeze({
    ...options2.resolvePresenceName ? { resolvePresenceName: options2.resolvePresenceName } : {},
    ...options2.app ? { app: options2.app.trim() } : {},
    ...options2.resolveApp ? { resolveApp: options2.resolveApp } : {}
  });
}
function configureParleyExtension(configuration, options2) {
  if (options2.app) configuration.app = options2.app;
  if (options2.resolveApp) configuration.resolveApp = options2.resolveApp;
  const resolver = options2.resolvePresenceName;
  if (!resolver || resolver === configuration.resolvePresenceName) return;
  if (configuration.resolvePresenceName) {
    throw new Error("Parley presence-name resolution is already configured by another extension wrapper");
  }
  if (configuration.presenceNameResolutionStarted) {
    throw new Error("Parley presence-name resolution cannot be configured after identity publication has started");
  }
  configuration.resolvePresenceName = resolver;
}
function registerParleyExtension(pi, options2) {
  const normalizedOptions = normalizeParleyExtensionOptions(options2);
  let alreadyClaimed = false;
  let configureClaimedExtension;
  const probe = {
    version: 1,
    claim: (configure) => {
      alreadyClaimed = true;
      configureClaimedExtension = configure;
    }
  };
  pi.events.emit(PARLEY_EXTENSION_RUNTIME_CLAIM_EVENT, probe);
  if (alreadyClaimed) {
    if (normalizedOptions.resolvePresenceName && !configureClaimedExtension) {
      throw new Error("The active Parley extension does not support presence-name configuration");
    }
    configureClaimedExtension?.(normalizedOptions);
    return;
  }
  const configuration = { presenceNameResolutionStarted: false };
  configureParleyExtension(configuration, normalizedOptions);
  const releaseRuntimeClaim = pi.events.on(PARLEY_EXTENSION_RUNTIME_CLAIM_EVENT, (payload) => {
    if (!payload || typeof payload !== "object") return;
    const candidate = payload;
    if (candidate.version !== 1 || typeof candidate.claim !== "function") return;
    candidate.claim((claimedOptions) => {
      configureParleyExtension(configuration, normalizeParleyExtensionOptions(claimedOptions));
    });
  });
  const registrationRollback = [releaseRuntimeClaim];
  try {
    installParleyExtension(pi, configuration, releaseRuntimeClaim, (cleanup) => registrationRollback.push(cleanup));
  } catch (error) {
    for (const cleanup of registrationRollback.reverse()) {
      try {
        cleanup();
      } catch {
      }
    }
    throw error;
  }
}
function installParleyExtension(pi, configuration, releaseRuntimeClaim, retainRegistrationCleanup) {
  let client = null;
  const config = loadRuntimeConfig((error) => console.error(error.message));
  const askTimeoutMs = getAskTimeoutMs();
  const compactionPresenceSupported = false;
  void sessionCompactFailuresReachExtensions;
  const localExtensions = /* @__PURE__ */ new Map();
  let runtimeContext = null;
  if (config.enabled && config.herdrNames) registerHerdrNames(pi, { sessionRenamesArrive: sessionInfoChangesReachExtensions() });
  let linkSessionInteractive = false;
  const links = new SessionLinks(
    (message) => {
      if (runtimeContext) notifyIfLive(runtimeContext, message, "info");
    },
    () => (process.env.PI_PARLEY_LINK_BY_DEFAULT?.trim() ? process.env.PI_PARLEY_LINK_BY_DEFAULT.trim() !== "0" : config.linkByDefault) && linkSessionInteractive && !process.env.NODE_TEST_CONTEXT && !process.env.FLIGHTDECK_TAB_ID?.trim() && !process.env.FLIGHTDECK_PANE_ID?.trim() && !process.env[SUBAGENT_CHILD_AGENT_ENV]?.trim() && process.env.PI_SUBAGENT_CHILD !== "1"
  );
  let currentSessionId = null;
  let currentParleySessionId = null;
  let currentModel = "unknown";
  let sessionStartedAt = null;
  let reconnectTimer = null;
  const previousParleySessionId = process.env[PARLEY_SESSION_ID_ENV];
  let reconnectPromise = null;
  let reconnectPromiseGeneration = null;
  let startupConnectTimer = null;
  let sessionNameCompatibilityTimer = null;
  let observedSessionName;
  let currentSessionDescription;
  let profileManagedName;
  let lastPresenceRequestedName;
  let profileNameMutationTarget;
  let profileOwnershipRevocationPending = false;
  let reconnectAttempt = 0;
  let shuttingDown = false;
  let disposed = true;
  let runtimeStarted = false;
  let runtimeGeneration = 0;
  let agentRunning = false;
  let compactionRunning = false;
  let compactionStatusGeneration = 0;
  let compactionStatusTimer = null;
  const pendingCompactionReports = /* @__PURE__ */ new Map();
  const pendingReceiverBaselineTokens = /* @__PURE__ */ new Set();
  let compactionReportFlush = null;
  let compactionReportRetryTimer = null;
  let receiverBaselineRetryTimer = null;
  const activeTools = /* @__PURE__ */ new Map();
  function createReferenceBook() {
    const generation = runtimeGeneration;
    return new ReferenceBook((record) => {
      if (generation === runtimeGeneration) recordConversationEntry(REFERENCE_ENTRY_TYPE, record);
    });
  }
  let sessionBook = createReferenceBook();
  const callBook = new AsyncLocalStorage();
  const shortNameNotes = new AsyncLocalStorage();
  const references = {
    get current() {
      return callBook.getStore() ?? sessionBook;
    }
  };
  const replyTracker = new ReplyTracker(void 0, {
    session: (from) => references.current.sessionRef(from),
    message: (id) => references.current.messageRef(id),
    isReachable: (from) => references.current.isReachable(from.id),
    verbatim
  });
  let conversationPersistenceWarning;
  const messageRef = (id) => references.current.messageRef(id);
  const sessionName = (id, name) => references.current.sessionRef(name ? { id, name } : id);
  const identityNames = {
    session: (session) => references.current.sessionRef(session),
    message: (id) => references.current.messageRef(id)
  };
  const failureText = (error) => getErrorMessage(error, identityNames);
  const describeDelivery = (result, context) => formatDeliveryResult(result, { ...context, sessionRef: (recipient) => references.current.sessionRef(recipient), messageRef, sessionName });
  const describeCancellation = (result) => formatCancellationResult(result, messageRef);
  const describeCompaction = (display, notice, expectedPeerSessionId) => formatPeerCompactionNotice(display, notice, expectedPeerSessionId, sessionName);
  function presentForModel(text, declared) {
    return references.current.present(text, declared === void 0 ? void 0 : references.current.learnFrom(declared));
  }
  function projectForModel(content, details, declared) {
    if (typeof details === "object" && details !== null && details[PRESENTED_MARK] === true) return content;
    const peerText = legacyPeerText(details);
    const project = (text) => references.current.present(protectLegacyPeerText(text, peerText), declared);
    if (typeof content === "string") return project(content);
    return content.map((part) => part.type === "text" && typeof part.text === "string" ? { ...part, text: project(part.text) } : part);
  }
  function legacyPeerText(details) {
    const found = /* @__PURE__ */ new Set();
    const addMessage = (message) => {
      if (!message || typeof message !== "object") return;
      const content = message.content;
      if (typeof content?.text === "string") found.add(content.text);
      if (Array.isArray(content?.attachments)) {
        for (const attachment of content.attachments) {
          if (typeof attachment.content === "string") found.add(attachment.content);
        }
      }
    };
    const visit = (value, depth) => {
      if (!value || typeof value !== "object" || depth > 4) return;
      if (Array.isArray(value)) {
        value.forEach((item) => visit(item, depth + 1));
        return;
      }
      for (const [key, field] of Object.entries(value)) {
        if (key === "bodyText" && typeof field === "string") found.add(field);
        else if ((key === "message" || key === "reply" || key === "original") && field && typeof field === "object") addMessage(field);
        else if (["messageId", "replyMessageId", "replyTo", "questionId"].includes(key) && typeof field === "string") {
          addMessage(replyTracker.getMessage(field)?.message);
        }
        if (field && typeof field === "object") visit(field, depth + 1);
      }
    };
    visit(details, 0);
    return [...found].filter((text) => text.length > 0).sort((left, right) => right.length - left.length);
  }
  function protectLegacyPeerText(text, peerText) {
    let parts = [{ text, verbatim: false }];
    const protect = (find) => {
      parts = parts.flatMap((part) => {
        if (part.verbatim) return [part];
        const out = [];
        let cursor = 0;
        for (const [start, end] of find(part.text)) {
          if (start < cursor) continue;
          if (start > cursor) out.push({ text: part.text.slice(cursor, start), verbatim: false });
          out.push({ text: part.text.slice(start, end), verbatim: true });
          cursor = end;
        }
        if (cursor < part.text.length) out.push({ text: part.text.slice(cursor), verbatim: false });
        return out;
      });
    };
    for (const peer of peerText) {
      protect((segment) => {
        const ranges = [];
        for (let index = segment.indexOf(peer); index !== -1; index = segment.indexOf(peer, index + peer.length)) ranges.push([index, index + peer.length]);
        return ranges;
      });
    }
    protect((segment) => [...segment.matchAll(/^(?:Original message|Reply to your message): .*$/gm)].map((match) => [match.index, match.index + match[0].length]));
    return parts.map((part) => part.verbatim ? verbatim(part.text) : part.text).join("");
  }
  function declareToolArguments(args, declared) {
    const book = references.current;
    const session = (value) => {
      if (typeof value !== "string" || !value.trim()) return;
      if (book.isIssuedSessionReference(value)) return;
      if (book.isKnownSession(value) || isCanonicalIdentity(value)) declared.sessions.add(value);
    };
    const message = (value) => {
      if (typeof value !== "string" || !value.trim() || book.isIssuedMessageReference(value) || /^#\d+$/.test(value.trim())) return;
      declared.messages.add(value);
    };
    session(args.to);
    if (Array.isArray(args.targets)) args.targets.forEach(session);
    for (const key of ["replyTo", "messageId", "supersedes", "retryOf"]) message(args[key]);
  }
  function presentToolArgumentsForModel(args, declared) {
    const book = references.current;
    const asSession = (value) => {
      if (typeof value !== "string" || book.isIssuedSessionReference(value)) return value;
      return declared.sessions.has(value) ? book.sessionRef(value) : value;
    };
    const asMessage = (value) => {
      if (typeof value !== "string" || book.isIssuedMessageReference(value)) return value;
      return declared.messages.has(value) ? book.messageRef(value) : value;
    };
    const next = { ...args };
    if ("to" in next) next.to = asSession(next.to);
    if (Array.isArray(next.targets)) next.targets = next.targets.map(asSession);
    for (const key of ["replyTo", "messageId", "supersedes", "retryOf"]) if (key in next) next[key] = asMessage(next[key]);
    return next;
  }
  function presentingTool(tool) {
    const execute = tool.execute;
    return { ...tool, execute: async (...args) => {
      const book = references.current;
      return callBook.run(book, async () => presentToolResult(await execute(...args), { book }));
    } };
  }
  function presentToolResult(result, options2 = {}) {
    const book = options2.book ?? references.current;
    const identities = book.learnFrom(result.details);
    result.details = { ...typeof result.details === "object" && result.details !== null ? result.details : {}, [PRESENTED_MARK]: true };
    for (const id of options2.sessions ?? []) identities.sessions.add(id);
    for (const part of result.content) {
      if (part.type === "text" && typeof part.text === "string") part.text = book.present(part.text, identities);
    }
    return result;
  }
  function recordConversationEntry(type, data) {
    try {
      pi.appendEntry(type, data);
    } catch (error) {
      conversationPersistenceWarning = `Parley history was not fully persisted (${type}: ${previewText(failureText(error), 160) ?? "history write failed"}). Messages and conversation updates remain available in this running session, but recovery after restart may be incomplete.`;
    }
  }
  const seenInboundMessages = /* @__PURE__ */ new Map();
  const latestOutboundReceipts = /* @__PURE__ */ new Map();
  const outboxRequestIds = /* @__PURE__ */ new Set();
  const pendingOutboxRequests = /* @__PURE__ */ new Map();
  function dismissIncomingAsk(messageId) {
    replyTracker.dismissPendingAsk(messageId);
    freshModelContexts.delete(messageId);
    settledInboundMessages.add(messageId);
    recordConversationEntry("parley_inbound_settled", { messageId, timestamp: Date.now() });
  }
  function hasSeenInboundMessage(from, message, now = Date.now()) {
    for (const [key2, seenAt] of seenInboundMessages) {
      if (now - seenAt > INBOUND_MESSAGE_DEDUPE_RETENTION_MS) {
        seenInboundMessages.delete(key2);
      }
    }
    const key = `${from.id}\0${message.id}`;
    if (seenInboundMessages.has(key)) {
      return true;
    }
    seenInboundMessages.set(key, now);
    while (seenInboundMessages.size > INBOUND_MESSAGE_DEDUPE_MAX) {
      const oldestKey = seenInboundMessages.keys().next().value;
      if (typeof oldestKey !== "string") break;
      seenInboundMessages.delete(oldestKey);
    }
    return false;
  }
  function emitMessageReceipt(messageId, status, detail) {
    try {
      client?.sendMessageReceipt({
        messageId,
        status,
        timestamp: Date.now(),
        ...detail ? { detail } : {}
      });
    } catch {
    }
  }
  function handleMessageControl(from, control, restored = false) {
    if (!restored) recordConversationEntry("parley_inbound_control", { from, control });
    const disposition = {
      state: control.action === "cancel" ? "withdrawn" : "superseded",
      ...control.supersededBy ? { replacementId: control.supersededBy } : {}
    };
    inboundMessageDispositions.set(control.messageId, disposition);
    replyTracker.setDisposition(control.messageId, disposition);
    deferredInboundMessages.delete(control.messageId);
    pendingHostEnvelopes.delete(`message:${control.messageId}`);
    freshModelContexts.delete(control.messageId);
    const original = replyTracker.getMessage(control.messageId);
    const topic = original ? `
Original message: ${verbatim(JSON.stringify(previewText(original.message.content.text, 180)))}` : "";
    const sender = references.current.sessionRef(from);
    const content = presentForModel(control.action === "cancel" ? `**Parley withdrawal from ${sender}**

Message ${references.current.messageRef(control.messageId)} was withdrawn by its sender.${topic}
The sender no longer requests this work. Earlier delivery or work may already have happened; withdrawal does not undo it.` : `**Parley update from ${sender}**

Message ${references.current.messageRef(control.messageId)} was superseded${control.supersededBy ? ` by ${references.current.messageRef(control.supersededBy)}` : ""}.${topic}
The earlier message is no longer the current request; prior work is not undone.`);
    const key = messageControlKey(control);
    deferredInboundControls.set(key, { from, control, content, receivedAt: Date.now() });
    flushInboundControl(key);
  }
  function flushInboundControl(key) {
    const entry = deferredInboundControls.get(key);
    const ctx = getLiveContext();
    if (!entry || !ctx) return;
    const envelope = { customType: "parley_message_control", content: entry.content, display: true, details: { from: entry.from, control: entry.control, receivedAt: entry.receivedAt, [PRESENTED_MARK]: true }, timestamp: entry.receivedAt };
    pendingHostEnvelopes.set(`control:${key}`, envelope);
    try {
      pi.sendMessage(envelope, ctx.isIdle() ? { triggerTurn: true } : { deliverAs: "steer" });
    } catch {
    }
    confirmPersistedInbound();
    scheduleInboundRetry();
  }
  function latestDeliveryState(messageId, fallback) {
    if (!messageId) {
      return fallback;
    }
    const receipt = latestOutboundReceipts.get(messageId);
    return receipt ? receipt.status : fallback;
  }
  let replyWaiter = null;
  const outstandingAsks = /* @__PURE__ */ new Map();
  const deferredInboundMessages = /* @__PURE__ */ new Map();
  const deferredInboundControls = /* @__PURE__ */ new Map();
  let inboundRetryTimer = null;
  const pendingHostEnvelopes = /* @__PURE__ */ new Map();
  const contextFallbackEnvelopes = /* @__PURE__ */ new Map();
  const presentedInboundEnvelopes = /* @__PURE__ */ new Set();
  const settledInboundMessages = /* @__PURE__ */ new Set();
  const inboundMessageDispositions = /* @__PURE__ */ new Map();
  const freshModelContexts = /* @__PURE__ */ new Map();
  function scheduleInboundRetry() {
    if (inboundRetryTimer || !deferredInboundMessages.size && !deferredInboundControls.size) return;
    const generation = runtimeGeneration;
    inboundRetryTimer = setTimeout(() => {
      inboundRetryTimer = null;
      const ctx = getLiveContext(runtimeContext, generation);
      if (!ctx) return;
      confirmPersistedInbound();
      if (ctx.isIdle()) {
        for (const entry of deferredInboundMessages.values()) sendIncomingMessage(entry, "trigger", generation);
        for (const key of deferredInboundControls.keys()) flushInboundControl(key);
      }
      scheduleInboundRetry();
    }, 1e3);
    inboundRetryTimer.unref?.();
  }
  function confirmPersistedInbound() {
    const ctx = getLiveContext();
    if (!ctx || pendingHostEnvelopes.size === 0 && contextFallbackEnvelopes.size === 0) return;
    for (const entry of ctx.sessionManager.getEntries()) {
      if (entry.type !== "custom_message") continue;
      const key = inboundEnvelopeKey(entry);
      if (key) {
        contextFallbackEnvelopes.delete(key);
        confirmInboundEnvelope(key);
      }
    }
  }
  function inboundEnvelopeKey(value) {
    const details = value.details;
    if (value.customType === "parley_message" && details?.message?.id) return `message:${details.message.id}`;
    if (value.customType === "parley_message_control" && details?.control?.messageId) return `control:${messageControlKey(details.control)}`;
    return void 0;
  }
  function confirmInboundEnvelope(key) {
    if (!pendingHostEnvelopes.delete(key)) return;
    if (key.startsWith("message:")) {
      const id = key.slice("message:".length);
      const entry = deferredInboundMessages.get(id);
      if (!entry) return;
      recordConversationEntry("parley_inbound_visible", { messageId: id, timestamp: Date.now() });
      deferredInboundMessages.delete(id);
      if (!isHistoricalInboundEnvelope(key)) {
        freshModelContexts.set(id, { from: entry.from, message: entry.message, receivedAt: entry.message.receiverReceivedAt ?? Date.now() });
      }
      emitMessageReceipt(id, "injected", "observed in host conversation/model context; not proof of processing");
      settleOutgoingReply(entry.from, entry.message);
      acknowledgeInboundMessageContact(client, entry.message);
    } else {
      const controlId = key.slice("control:".length);
      const entry = deferredInboundControls.get(controlId);
      if (!entry) return;
      recordConversationEntry("parley_control_visible", { key: controlId, timestamp: Date.now() });
      deferredInboundControls.delete(controlId);
      emitMessageReceipt(entry.control.messageId, entry.control.action === "cancel" ? "cancellation_requested" : "superseded", "withdrawal/update observed in host conversation/model context; earlier work may have happened");
    }
  }
  function isHistoricalInboundEnvelope(key) {
    if (presentedInboundEnvelopes.has(key)) return true;
    if (!key.startsWith("message:")) return false;
    const id = key.slice("message:".length);
    return settledInboundMessages.has(id) || inboundMessageDispositions.has(id);
  }
  function historicalInboundEnvelope(envelope) {
    if (envelope.customType === "parley_message_control") {
      const details = envelope.details;
      const disposition2 = inboundMessageDispositions.get(details.control.messageId);
      const status2 = disposition2 ? `${disposition2.state}${disposition2.replacementId ? ` by message ${references.current.messageRef(disposition2.replacementId)}` : ""}` : details.control.action === "cancel" ? "withdrawn" : "superseded";
      const raw = typeof envelope.content === "string" ? envelope.content : envelope.content.map((part) => part.text ?? "").join("\n");
      const content = envelope.details?.[PRESENTED_MARK] === true ? verbatim(raw) : protectLegacyPeerText(raw, legacyPeerText(envelope.details));
      return presentForModel(`**Parley history \u2014 withdrawal/update**
Received: ${formatMessageTimestamp(envelope.timestamp)}
Status: ${status2}
Previously received context, not a new delivery.

${content}`, details);
    }
    const entry = envelope.details;
    const { from, message } = entry;
    const retained = replyTracker.getMessage(message.id);
    const disposition = inboundMessageDispositions.get(message.id);
    const answered = settledInboundMessages.has(message.id);
    const status = disposition ? `${disposition.state}${disposition.replacementId ? ` by message ${references.current.messageRef(disposition.replacementId)}` : ""}` : answered ? "answered" : message.expectsReply ? `unanswered request${replyTracker.replyWindowElapsed(retained ?? { from, message, receivedAt: envelope.timestamp }) ? "; reply window elapsed, not withdrawn" : ""}` : message.replyTo ? message.completesAsk === false ? "received threaded progress" : "received reply" : "received notification; no reply requested";
    const origin = from.federation ? `
Remote origin: ${references.current.originRef(from.federation)}; scope ${from.federation.remoteScopeAlias}` : "";
    const compaction = message.peerCompaction ? `

${describeCompaction(references.current.sessionRef(from), message.peerCompaction, from.id)}` : "";
    const body = entry.bodyText ?? message.content.text + (message.content.attachments?.length ? formatAttachments(message.content.attachments) : "");
    return presentForModel(`**Parley history \u2014 from ${references.current.sessionRef(from)}**${from.description ? ` \u2014 ${from.description}` : ""}
Received: ${formatMessageTimestamp(envelope.timestamp)}
Status: ${status}
Shown again from earlier in this conversation, not a new delivery; the status above is current.${verbatim(entry.replyTopic ?? "")}

${verbatim(body)}

${formatInboundDeliveryMetadata(message, messageRef)}
Session: ${from.cwd}${origin}${compaction}`, { from, message });
  }
  function settleOutgoingReply(from, message) {
    if (!message.replyTo || message.completesAsk === false || message.expectsReply) return;
    const ask = outstandingAsks.get(message.replyTo);
    if (!ask || !matchesAskCounterpart(ask, from)) return;
    recordConversationEntry("parley_ask_settled", { messageId: message.replyTo, reason: "reply accepted by host", timestamp: Date.now() });
    outstandingAsks.delete(message.replyTo);
  }
  function currentSendIdentity(client2) {
    const self = client2.getSelfSession();
    const name = self?.runtimeFallbackAlias ? void 0 : self?.name?.trim();
    return name || pi.getSessionName()?.trim() || "unnamed";
  }
  function waitForReply(from, replyTo, signal, cancelOnAbort, getDeliveryState = () => "unknown", endpointEpoch, originEpoch) {
    if (replyWaiter) {
      return Promise.reject(new Error("Already waiting for a reply"));
    }
    if (signal?.aborted) {
      return Promise.reject(new Error("Cancelled"));
    }
    return new Promise((resolve4, reject) => {
      const timeout = setTimeout(() => {
        const timeoutDescription = askTimeoutMs % 6e4 === 0 ? `${askTimeoutMs / 6e4} minutes` : `${askTimeoutMs}ms`;
        rejectReplyWaiter(new Error(`No reply from ${sessionName(from)} for message ${messageRef(replyTo)} within ${timeoutDescription}. Last known delivery state: ${getDeliveryState()}. This waiter timeout is not cancellation; the delivered message may still be queued or actionable in the recipient session.`));
      }, askTimeoutMs);
      const cleanup = () => {
        clearTimeout(timeout);
        signal?.removeEventListener("abort", onAbort);
        if (replyWaiter?.replyTo === replyTo) {
          replyWaiter = null;
        }
      };
      const onAbort = () => {
        cancelOnAbort?.();
        cleanup();
        reject(new Error("Cancelled"));
      };
      signal?.addEventListener("abort", onAbort, { once: true });
      replyWaiter = {
        from,
        ...endpointEpoch ? { endpointEpoch } : {},
        ...originEpoch ? { originEpoch } : {},
        replyTo,
        resolve: (message) => {
          cleanup();
          resolve4(message);
        },
        reject: (error) => {
          cleanup();
          reject(error);
        }
      };
    });
  }
  function rejectOwnedReplyWaiter(messageId, error) {
    if (messageId && replyWaiter?.replyTo === messageId) rejectReplyWaiter(error);
  }
  function rejectReplyWaiter(error) {
    replyWaiter?.reject(error);
  }
  function clearReconnectTimer() {
    if (!reconnectTimer) {
      return;
    }
    clearTimeout(reconnectTimer);
    reconnectTimer = null;
  }
  function clearStartupConnectTimer() {
    if (!startupConnectTimer) {
      return;
    }
    clearTimeout(startupConnectTimer);
    startupConnectTimer = null;
  }
  function clearSessionNameCompatibilityTimer() {
    if (!sessionNameCompatibilityTimer) return;
    clearInterval(sessionNameCompatibilityTimer);
    sessionNameCompatibilityTimer = null;
  }
  function clearCompactionStatusTimer() {
    if (!compactionStatusTimer) return;
    clearTimeout(compactionStatusTimer);
    compactionStatusTimer = null;
  }
  function resetCompactionStatus() {
    compactionStatusGeneration += 1;
    compactionRunning = false;
    clearCompactionStatusTimer();
  }
  function startSessionNameCompatibilityTimer() {
    clearSessionNameCompatibilityTimer();
    observedSessionName = pi.getSessionName()?.trim() || void 0;
    if (sessionInfoChangesReachExtensions()) return;
    sessionNameCompatibilityTimer = setInterval(() => {
      if (!currentSessionId || !getLiveContext()) return;
      const currentName = pi.getSessionName()?.trim() || void 0;
      if (currentName === observedSessionName) return;
      observedSessionName = currentName;
      try {
        syncPresenceIdentity(currentSessionId);
      } catch (error) {
        console.error(`Parley presence name update failed: ${failureText(error)}`);
      }
    }, 1e3);
    sessionNameCompatibilityTimer.unref?.();
  }
  function getLiveContext(ctx = runtimeContext, generation = runtimeGeneration) {
    if (disposed || shuttingDown || generation !== runtimeGeneration || !ctx) {
      return null;
    }
    try {
      if (currentSessionId && ctx.sessionManager.getSessionId() !== currentSessionId) {
        return null;
      }
      void ctx.hasUI;
      return ctx;
    } catch {
      return null;
    }
  }
  function notifyIfLive(ctx, message, level, generation = runtimeGeneration) {
    const liveContext = getLiveContext(ctx, generation);
    if (!liveContext?.hasUI) {
      return;
    }
    try {
      liveContext.ui.notify(message, level);
    } catch {
    }
  }
  function notifyAliasCommand(ctx, message, level, generation = runtimeGeneration) {
    const liveContext = getLiveContext(ctx, generation);
    if (!liveContext) return;
    if (!liveContext.hasUI) {
      console.error(message);
      return;
    }
    notifyIfLive(liveContext, message, level, generation);
  }
  function getReconnectDelayMs() {
    const backoffMs = [1e3, 2e3, 5e3, 1e4, 3e4];
    return backoffMs[Math.min(reconnectAttempt, backoffMs.length - 1)];
  }
  function currentStatus() {
    const activeToolName = activeTools.values().next().value;
    const lifecycleStatus = compactionRunning ? "compacting" : activeToolName ? `tool:${activeToolName}` : agentRunning ? "thinking" : "idle";
    return config.status ? `${lifecycleStatus} \xB7 ${config.status}` : lifecycleStatus;
  }
  function emitLocalExtensionEvent(namespace, event) {
    try {
      localExtensions.get(namespace)?.registration.onEvent(event);
    } catch {
    }
  }
  function createExtensionChannel(namespace) {
    return {
      namespace,
      snapshot() {
        const extension = localExtensions.get(namespace);
        return {
          connected: Boolean(client?.isConnected()),
          supported: Boolean(client?.supportsFeature(EXTENSION_BUS_FEATURE)),
          ...extension?.owner ? { owner: extension.owner } : {},
          ...extension?.state ? { state: extension.state } : {}
        };
      },
      publish(payload, options2 = {}) {
        const activeClient = client;
        if (!activeClient?.isConnected()) throw new Error("Parley is not connected");
        const extension = localExtensions.get(namespace);
        const ownerOnly = options2.ownerOnly ?? false;
        const ownerEpoch = ownerOnly ? extension?.owner?.epoch : void 0;
        if (ownerOnly && !ownerEpoch) throw new Error(`No owner is available for ${namespace}`);
        activeClient.sendExtensionMessage({
          type: "extension_publish",
          namespace,
          audience: options2.audience ?? "owner",
          ...ownerOnly ? { ownerOnly: true, ownerEpoch } : {},
          payload
        });
      },
      commitState(payload, expectedRevision) {
        const activeClient = client;
        if (!activeClient?.isConnected()) throw new Error("Parley is not connected");
        const extension = localExtensions.get(namespace);
        const ownerEpoch = extension?.owner?.epoch;
        if (!ownerEpoch || extension.owner?.sessionId !== activeClient.sessionId) {
          throw new Error(`Current session is not the owner of ${namespace}`);
        }
        activeClient.sendExtensionMessage({
          type: "extension_state_commit",
          namespace,
          ownerEpoch,
          expectedRevision: expectedRevision ?? extension.state?.revision ?? 0,
          payload
        });
      },
      async listSessions() {
        const activeClient = client;
        if (!activeClient?.isConnected()) throw new Error("Parley is not connected");
        return activeClient.listSessions();
      }
    };
  }
  function currentExtensionCapabilities() {
    return [...localExtensions.values()].map(({ registration }) => ({
      namespace: registration.namespace,
      ownerEligible: registration.ownerEligible
    }));
  }
  function registerLocalExtension(registration) {
    if (!/^[a-z0-9][a-z0-9._/-]{0,63}$/.test(registration.namespace)) {
      throw new Error(`Invalid parley extension namespace: ${registration.namespace}`);
    }
    if (localExtensions.has(registration.namespace)) {
      throw new Error(`Parley extension namespace already registered: ${registration.namespace}`);
    }
    const channel = createExtensionChannel(registration.namespace);
    localExtensions.set(registration.namespace, { registration, channel });
    const activeClient = client;
    const connected = Boolean(activeClient?.isConnected());
    const supported = Boolean(activeClient?.supportsFeature(EXTENSION_BUS_FEATURE));
    if (activeClient && connected && supported) {
      activeClient.updateExtensionCapabilities(currentExtensionCapabilities());
    }
    registration.onReady(channel);
    if (connected) {
      emitLocalExtensionEvent(registration.namespace, { type: "connection", connected: true, supported });
    }
  }
  function buildRegistration() {
    const liveContext = getLiveContext();
    if (!liveContext || !currentSessionId || sessionStartedAt === null) {
      throw new Error("Parley runtime not initialized");
    }
    const identity = buildPresenceIdentity(pi, currentParleySessionId ?? currentSessionId, configuration);
    const tmuxPane = currentTmuxPane();
    return {
      ...identity,
      ...currentSessionDescription ? { description: currentSessionDescription } : {},
      cwd: liveContext.cwd,
      model: currentModel,
      pid: process.pid,
      startedAt: sessionStartedAt,
      lastActivity: Date.now(),
      status: currentStatus(),
      ...tmuxPane ? { tmuxPane } : {},
      app: appLabel(configuration.resolveApp?.()) ?? configuration.app ?? detectApp(),
      ...localExtensions.size > 0 ? {
        extensions: currentExtensionCapabilities()
      } : {},
      ...subagentAclFields()
    };
  }
  function subagentAclFields() {
    const metadata = readChildOrchestratorMetadata();
    if (!metadata) {
      return {};
    }
    return {
      isSubagent: true,
      supervisorName: metadata.orchestratorTarget,
      ...metadata.orchestratorSessionId ? { supervisorSessionId: metadata.orchestratorSessionId } : {}
    };
  }
  function persistProfileOwnershipRevocation() {
    if (!profileOwnershipRevocationPending) return;
    try {
      pi.appendEntry("parley_profile_updated", {
        managedName: null,
        description: currentSessionDescription ?? null,
        timestamp: Date.now()
      });
      profileOwnershipRevocationPending = false;
    } catch {
    }
  }
  function syncPresenceIdentity(sessionId) {
    if (!getLiveContext()) return;
    const canonicalSessionName = pi.getSessionName()?.trim() || void 0;
    const identity = buildPresenceIdentity(pi, currentParleySessionId ?? sessionId, configuration);
    if (profileManagedName && canonicalSessionName && canonicalSessionName !== profileManagedName && canonicalSessionName !== profileNameMutationTarget) {
      profileManagedName = void 0;
      profileOwnershipRevocationPending = true;
    }
    persistProfileOwnershipRevocation();
    if (!client) return;
    if (lastPresenceRequestedName !== void 0 && lastPresenceRequestedName !== identity.name) {
      client.invalidateSelfSessionProjection();
    }
    lastPresenceRequestedName = identity.name;
    client.updatePresence({
      ...identity,
      ...client.supportsFeature(SESSION_PROFILE_FEATURE) ? { description: currentSessionDescription ?? null } : {},
      status: currentStatus()
    });
  }
  function publishParleySessionId(sessionId) {
    process.env[PARLEY_SESSION_ID_ENV] = sessionId;
  }
  function restoreParleySessionId() {
    if (previousParleySessionId === void 0) {
      delete process.env[PARLEY_SESSION_ID_ENV];
      return;
    }
    process.env[PARLEY_SESSION_ID_ENV] = previousParleySessionId;
  }
  function syncPresenceStatus() {
    if (!client || !currentSessionId || !getLiveContext()) {
      return;
    }
    client.updatePresence({ status: currentStatus() });
  }
  function finishCompactionStatus(ctx, expectedGeneration) {
    if (!getLiveContext(ctx)) {
      return;
    }
    if (expectedGeneration !== void 0 && expectedGeneration !== compactionStatusGeneration) {
      return;
    }
    resetCompactionStatus();
    syncPresenceStatus();
  }
  function clearCompactionReportRetryTimer() {
    if (!compactionReportRetryTimer) return;
    clearTimeout(compactionReportRetryTimer);
    compactionReportRetryTimer = null;
  }
  function scheduleCompactionReportRetry(expectedGeneration) {
    if (compactionReportRetryTimer || pendingCompactionReports.size === 0) return;
    compactionReportRetryTimer = setTimeout(() => {
      compactionReportRetryTimer = null;
      if (expectedGeneration === runtimeGeneration && getLiveContext()) {
        flushPendingCompactionReports();
      }
    }, 1e3);
    compactionReportRetryTimer.unref?.();
  }
  function restorePendingCompactionReports(ctx) {
    pendingCompactionReports.clear();
    pendingReceiverBaselineTokens.clear();
    currentSessionDescription = void 0;
    profileManagedName = void 0;
    lastPresenceRequestedName = void 0;
    profileNameMutationTarget = void 0;
    profileOwnershipRevocationPending = false;
    const sessionManager = ctx.sessionManager;
    const entries = sessionManager.getEntries?.() ?? [];
    const pendingProfiles = /* @__PURE__ */ new Map();
    let lastAppliedProfileIndex = -1;
    const parseProfileState = (data, index) => {
      if (data.description !== void 0 && data.description !== null && !isValidSessionDescription(data.description)) return void 0;
      if (data.managedName !== void 0 && data.managedName !== null && !isValidSessionName(data.managedName)) return void 0;
      if (data.requiredName !== void 0 && !isValidSessionName(data.requiredName)) return void 0;
      return {
        ...data.description === null || isValidSessionDescription(data.description) ? { description: data.description } : {},
        ...data.managedName === null || typeof data.managedName === "string" ? { managedName: data.managedName } : {},
        ...typeof data.requiredName === "string" ? { requiredName: data.requiredName } : {},
        index
      };
    };
    const applyProfileState = (state) => {
      if (state.description === null) currentSessionDescription = void 0;
      else if (state.description !== void 0) currentSessionDescription = state.description;
      if (state.managedName === null) profileManagedName = void 0;
      else if (state.managedName) profileManagedName = state.managedName;
      lastAppliedProfileIndex = state.index;
    };
    for (const [entryIndex, entry] of entries.entries()) {
      if (entry.type === "custom_message" && entry.customType === "parley_message" && typeof entry.details === "object" && entry.details !== null) {
        const details = entry.details;
        const delivered = details.message;
        if (delivered?.contactBaseline === true && typeof delivered.contactToken === "string") {
          pendingReceiverBaselineTokens.add(delivered.contactToken);
        }
        continue;
      }
      if (entry.type !== "custom" || typeof entry.data !== "object" || entry.data === null) continue;
      const data = entry.data;
      if (entry.customType === "parley_profile_pending" && typeof data.updateId === "string") {
        const state = parseProfileState(data, entryIndex);
        if (state) pendingProfiles.set(data.updateId, state);
      } else if (entry.customType === "parley_profile_abandoned" && typeof data.updateId === "string") {
        pendingProfiles.delete(data.updateId);
      } else if (entry.customType === "parley_profile_updated") {
        const state = parseProfileState(data, entryIndex);
        if (state) {
          applyProfileState(state);
          if (typeof data.updateId === "string") pendingProfiles.delete(data.updateId);
        }
      }
      if (entry.customType === "parley_receiver_baseline_pending" && typeof data.token === "string") {
        pendingReceiverBaselineTokens.add(data.token);
      } else if ((entry.customType === "parley_receiver_baseline_recorded" || entry.customType === "parley_receiver_baseline_abandoned") && typeof data.token === "string") {
        pendingReceiverBaselineTokens.delete(data.token);
      }
      if (typeof data.eventId !== "string" || data.eventId.length === 0) continue;
      if (entry.customType === "parley_compaction_pending") {
        pendingCompactionReports.set(
          data.eventId,
          typeof data.compactedAt === "number" ? data.compactedAt : 0
        );
      } else if (entry.customType === "parley_compaction_recorded") {
        pendingCompactionReports.delete(data.eventId);
      }
    }
    const currentName = pi.getSessionName()?.trim();
    const recoverablePending = [...pendingProfiles.values()].filter((state) => state.index > lastAppliedProfileIndex && state.requiredName === currentName).sort((left, right) => right.index - left.index)[0];
    if (recoverablePending) applyProfileState(recoverablePending);
    if (profileManagedName !== currentName) profileManagedName = void 0;
  }
  function flushPendingCompactionReports() {
    if (compactionReportFlush || pendingCompactionReports.size === 0) return;
    const activeClient = client;
    const expectedGeneration = runtimeGeneration;
    if (!activeClient?.isConnected() || !activeClient.supportsFeature(COMPACTION_AWARENESS_FEATURE)) return;
    clearCompactionReportRetryTimer();
    compactionReportFlush = (async () => {
      while (expectedGeneration === runtimeGeneration && activeClient === client && activeClient.isConnected() && pendingCompactionReports.size > 0) {
        const next = pendingCompactionReports.entries().next().value;
        if (!next) return;
        const [eventId, compactedAt] = next;
        try {
          const recorded = await activeClient.reportCompactionCompleted(eventId);
          if (expectedGeneration !== runtimeGeneration || activeClient !== client) return;
          pi.appendEntry("parley_compaction_recorded", {
            eventId,
            compactedAt,
            generation: recorded.generation,
            recordedAt: recorded.compactedAt
          });
          pendingCompactionReports.delete(eventId);
        } catch {
          scheduleCompactionReportRetry(expectedGeneration);
          return;
        }
      }
    })().finally(() => {
      if (expectedGeneration !== runtimeGeneration) return;
      compactionReportFlush = null;
      if (pendingCompactionReports.size > 0) {
        scheduleCompactionReportRetry(expectedGeneration);
      }
    });
  }
  function beginCompactionStatus(ctx, signal) {
    if (!compactionPresenceSupported || !getLiveContext(ctx)) {
      return;
    }
    resetCompactionStatus();
    compactionRunning = true;
    const generation = compactionStatusGeneration;
    compactionStatusTimer = setTimeout(() => {
      if (generation !== compactionStatusGeneration || !compactionRunning) {
        return;
      }
      finishCompactionStatus(ctx, generation);
    }, COMPACTION_STATUS_FAILSAFE_MS);
    compactionStatusTimer.unref?.();
    if (signal?.aborted) {
      finishCompactionStatus(ctx, generation);
      return;
    }
    signal?.addEventListener("abort", () => finishCompactionStatus(ctx, generation), { once: true });
    syncPresenceStatus();
  }
  function currentSessionTargetMatches(to, resolvedTo, activeClient) {
    const targets = /* @__PURE__ */ new Set();
    const addTarget = (target2) => {
      const trimmed = target2?.trim();
      if (trimmed) targets.add(trimmed.toLowerCase());
    };
    addTarget(currentSessionId);
    addTarget(currentParleySessionId);
    addTarget(activeClient?.sessionId);
    addTarget(pi.getSessionName());
    addTarget(lastPresenceRequestedName);
    return Boolean(resolvedTo && activeClient?.sessionId && resolvedTo === activeClient.sessionId) || targets.has(to.trim().toLowerCase());
  }
  function buildOutboxResult(request, status, options2 = {}) {
    return {
      version: 1,
      requestId: request.requestId,
      status,
      ...options2.code ? { code: options2.code } : {},
      ...request.extensionId ? { extensionId: request.extensionId } : {},
      ...request.extensionName ? { extensionName: request.extensionName } : {},
      ...options2.messageId ? { messageId: options2.messageId } : {},
      ...options2.detail ? { detail: options2.detail } : {}
    };
  }
  function emitOutboxResult(result, request) {
    recordConversationEntry("parley_outbox_result", {
      ...result,
      ...request.to ? { to: request.to } : {},
      ...request.message ? { message: { text: request.message } } : {},
      timestamp: Date.now()
    });
    pi.events.emit(PARLEY_OUTBOX_RESULT_EVENT, result);
  }
  function settleOutboxRequest(requestId, status, options2 = {}) {
    const pending = pendingOutboxRequests.get(requestId);
    if (!pending) {
      return false;
    }
    pendingOutboxRequests.delete(requestId);
    emitOutboxResult(buildOutboxResult(pending.request, status, options2), pending.request);
    return true;
  }
  function failPendingOutboxRequests(generation, code, detail) {
    for (const [requestId, pending] of [...pendingOutboxRequests]) {
      if (pending.generation === generation) {
        settleOutboxRequest(requestId, "failed", { code, detail });
      }
    }
  }
  function resolveOutboxTarget(sessions, currentId, to) {
    const byId = sessions.find((session) => session.id === to);
    const lowerName = to.toLowerCase();
    const byName = byId ? [] : sessions.filter((session) => session.name?.toLowerCase() === lowerName);
    const byPrefix = byId || byName.length > 0 ? [] : sessions.filter((session) => session.id.startsWith(to));
    const matches = byId ? [byId] : byName.length > 0 ? byName : byPrefix;
    if (matches.length === 0) {
      return { ok: false, code: "target_not_found", detail: `Session "${to}" is not currently connected.` };
    }
    if (matches.length > 1) {
      return { ok: false, code: "target_ambiguous", detail: `Multiple sessions match "${to}".` };
    }
    const target2 = matches[0];
    if (target2.id === currentId) {
      return { ok: false, code: "self_target", detail: "Cannot message the current session." };
    }
    return { ok: true, target: { id: target2.id, label: target2.name || target2.id } };
  }
  function handleOutboxRequest(payload) {
    const parsed = parseOutboxRequestPayload(payload);
    if (parsed.ok === false) {
      if (parsed.requestId) {
        const trace2 = {
          requestId: parsed.requestId,
          ...parsed.extensionId ? { extensionId: parsed.extensionId } : {},
          ...parsed.extensionName ? { extensionName: parsed.extensionName } : {}
        };
        emitOutboxResult(buildOutboxResult(trace2, "rejected", { code: "invalid_request", detail: parsed.detail }), trace2);
      }
      return;
    }
    const request = parsed.request;
    const trace = {
      requestId: request.requestId,
      extensionId: request.extensionId,
      extensionName: request.extensionName,
      to: request.to,
      message: request.message
    };
    if (outboxRequestIds.has(request.requestId)) {
      emitOutboxResult(buildOutboxResult(trace, "rejected", { code: "duplicate_request", detail: "requestId has already been used in this session runtime" }), trace);
      return;
    }
    outboxRequestIds.add(request.requestId);
    const outboxGeneration = runtimeGeneration;
    pendingOutboxRequests.set(request.requestId, { generation: outboxGeneration, request: trace });
    void (async () => {
      const liveContext = getLiveContext(runtimeContext, outboxGeneration);
      if (!liveContext) {
        settleOutboxRequest(request.requestId, "failed", { code: "session_unavailable", detail: "Parley session is not active" });
        return;
      }
      if (config.confirmSend && !liveContext.hasUI) {
        settleOutboxRequest(request.requestId, "blocked", { code: "confirmation_unavailable", detail: "confirmSend is enabled but no UI is available" });
        return;
      }
      let activeClient;
      try {
        activeClient = await ensureConnected("background");
      } catch (error) {
        settleOutboxRequest(request.requestId, "failed", { code: "session_unavailable", detail: failureText(error) });
        return;
      }
      if (!getLiveContext(liveContext, outboxGeneration)) {
        settleOutboxRequest(request.requestId, "failed", { code: "session_ended", detail: "Session ended before target resolution" });
        return;
      }
      let target2;
      try {
        const currentClientSessionId = activeClient.sessionId;
        const sessions = await activeClient.listSessions();
        if (!currentClientSessionId) {
          settleOutboxRequest(request.requestId, "failed", { code: "session_unavailable", detail: "Current session is not registered with parley" });
          return;
        }
        const resolved = resolveOutboxTarget(sessions, currentClientSessionId, request.to);
        if (resolved.ok === false) {
          settleOutboxRequest(request.requestId, "blocked", { code: resolved.code, detail: resolved.detail });
          return;
        }
        target2 = resolved.target;
      } catch (error) {
        settleOutboxRequest(request.requestId, "failed", { code: "session_unavailable", detail: failureText(error) });
        return;
      }
      if (!getLiveContext(liveContext, outboxGeneration)) {
        settleOutboxRequest(request.requestId, "failed", { code: "session_ended", detail: "Session ended before confirmation" });
        return;
      }
      if (config.confirmSend) {
        let confirmed = false;
        try {
          confirmed = await liveContext.ui.confirm(
            "Send extension message",
            `Allow ${request.extensionName} (${request.extensionId}) to send to "${target2.label}":

${request.message}`
          );
        } catch (error) {
          settleOutboxRequest(request.requestId, "blocked", { code: "confirmation_unavailable", detail: failureText(error) });
          return;
        }
        if (!getLiveContext(liveContext, outboxGeneration)) {
          settleOutboxRequest(request.requestId, "failed", { code: "session_ended", detail: "Session ended during confirmation" });
          return;
        }
        if (!confirmed) {
          settleOutboxRequest(request.requestId, "rejected", { code: "user_cancelled", detail: "User cancelled the outbox request" });
          return;
        }
      }
      try {
        if (!getLiveContext(liveContext, outboxGeneration) || client !== activeClient || !activeClient.isConnected()) {
          settleOutboxRequest(request.requestId, "failed", { code: "session_ended", detail: "Session ended before delivery" });
          return;
        }
        const result = await activeClient.send(target2.id, {
          text: request.message,
          provenance: {
            type: "extension_outbox",
            extensionId: request.extensionId,
            extensionName: request.extensionName,
            requestId: request.requestId
          }
        });
        if (!getLiveContext(liveContext, outboxGeneration)) {
          settleOutboxRequest(request.requestId, "failed", { code: "session_ended", detail: "Session ended during delivery" });
          return;
        }
        if (!result.delivered) {
          settleOutboxRequest(request.requestId, "failed", { code: "delivery_failed", messageId: result.id, detail: result.reason ?? "Delivery failed" });
          return;
        }
        surfaceBackgroundPeerCompaction(activeClient, target2.label, result, outboxGeneration, target2.id);
        recordConversationEntry("parley_sent", {
          to: target2.label,
          targetId: result.recipient?.id ?? target2.id,
          message: { text: request.message },
          messageId: result.id,
          timestamp: Date.now(),
          extension: { id: request.extensionId, name: request.extensionName, requestId: request.requestId }
        });
        settleOutboxRequest(request.requestId, "sent", { messageId: result.id });
      } catch (error) {
        const live = getLiveContext(liveContext, outboxGeneration);
        settleOutboxRequest(request.requestId, "failed", {
          code: live ? "session_unavailable" : "session_ended",
          detail: failureText(error)
        });
      }
    })();
  }
  function shouldTriggerInboundMessage(entry, forceTrigger = false) {
    if (forceTrigger) {
      return true;
    }
    if (config.inboundTrigger === "always") {
      return true;
    }
    if (config.inboundTrigger === "replies") {
      return Boolean(entry.message.replyTo);
    }
    return false;
  }
  function handleExpiredOutboundReceipt(from, receipt) {
    if (!runtimeStarted || !getLiveContext()) {
      return;
    }
    const targetDisplay = references.current.sessionRef(from);
    const original = findOutgoingTopic(receipt.messageId);
    const topic = original ? `
Original message: ${verbatim(JSON.stringify(previewText(original.message?.text ?? original.preview, 240)))}` : "";
    pi.appendEntry("parley_delivery_failed", {
      to: targetDisplay,
      messageId: receipt.messageId,
      detail: receipt.detail ?? "Mailbox delivery expired",
      timestamp: receipt.timestamp
    });
    pi.sendMessage(
      {
        customType: "parley_delivery_notice",
        content: presentForModel(`**Parley delivery failed:** queued message to ${targetDisplay} (${from.cwd}) expired before delivery.
Message: ${references.current.messageRef(receipt.messageId)}${topic}
Reason: ${receipt.detail ?? "mailbox entry expired"}.`),
        display: true,
        details: { to: targetDisplay, messageId: receipt.messageId, expired: true, ...receipt.detail ? { detail: receipt.detail } : {}, [PRESENTED_MARK]: true }
      },
      { triggerTurn: true }
    );
  }
  function surfaceBackgroundPeerCompaction(sourceClient, peerDisplay, result, generation = runtimeGeneration, expectedPeerSessionId) {
    if (!result.peerCompaction || runtimeStarted && !getLiveContext(runtimeContext, generation)) return;
    const notice = describeCompaction(peerDisplay, result.peerCompaction, expectedPeerSessionId);
    pi.sendMessage(
      {
        customType: "parley_compaction_awareness",
        content: presentForModel(`**Parley compaction awareness**

${notice}`, { messageId: result.id }),
        display: true,
        details: { messageId: result.id, peerCompaction: result.peerCompaction, [PRESENTED_MARK]: true }
      },
      { triggerTurn: false }
    );
    sourceClient.acknowledgeSendContact(result);
  }
  function clearReceiverBaselineRetryTimer() {
    if (!receiverBaselineRetryTimer) return;
    clearTimeout(receiverBaselineRetryTimer);
    receiverBaselineRetryTimer = null;
  }
  function scheduleReceiverBaselineRetry(expectedGeneration = runtimeGeneration) {
    if (receiverBaselineRetryTimer || pendingReceiverBaselineTokens.size === 0) return;
    receiverBaselineRetryTimer = setTimeout(() => {
      receiverBaselineRetryTimer = null;
      const activeClient = client;
      if (expectedGeneration !== runtimeGeneration || !activeClient?.isConnected()) return;
      for (const token of pendingReceiverBaselineTokens) activeClient.acknowledgeContactToken(token);
      scheduleReceiverBaselineRetry(expectedGeneration);
    }, 1e3);
    receiverBaselineRetryTimer.unref?.();
  }
  function acknowledgeInboundMessageContact(sourceClient, message) {
    if (!message.contactToken || !sourceClient) return;
    if (message.contactBaseline) {
      recordConversationEntry("parley_receiver_baseline_pending", {
        token: message.contactToken,
        messageId: message.id,
        timestamp: Date.now()
      });
      pendingReceiverBaselineTokens.add(message.contactToken);
      scheduleReceiverBaselineRetry();
    }
    sourceClient.acknowledgeMessageContact(message);
  }
  function findOutgoingTopic(messageId) {
    const entries = getLiveContext()?.sessionManager.getEntries() ?? [];
    for (let index = entries.length - 1; index >= 0; index--) {
      const entry = entries[index];
      if (entry?.type !== "custom" || entry.customType !== "parley_ask_pending" && entry.customType !== "parley_sent") continue;
      const data = entry.data;
      const recipient = data?.targetId ?? data?.to;
      if (data?.messageId === messageId && recipient && data.message) return { to: recipient, preview: data.message.text, message: data.message };
    }
    return void 0;
  }
  function formatInboundEnvelope(entry, injectedMessage) {
    references.current.learnFrom({ from: entry.from, message: injectedMessage });
    const senderDisplay = references.current.sessionRef(entry.from);
    const focus = entry.from.description ? ` \u2014 ${entry.from.description}` : "";
    const origin = entry.from.federation ? `
Remote origin: ${references.current.originRef(entry.from.federation)}; scope ${entry.from.federation.remoteScopeAlias}` : "";
    const requestedAsk = injectedMessage.replyTo ? outstandingAsks.get(injectedMessage.replyTo) : void 0;
    const isRequestedAnswer = requestedAsk !== void 0 && matchesAskCounterpart(requestedAsk, entry.from) && injectedMessage.completesAsk !== false && !injectedMessage.expectsReply;
    const outgoing = requestedAsk ?? (injectedMessage.replyTo ? findOutgoingTopic(injectedMessage.replyTo) : void 0);
    const topic = outgoing && outgoing.to === entry.from.id ? `
Reply to your message: ${JSON.stringify(previewText(outgoing.message?.text ?? outgoing.preview, 240))}` : "";
    const elapsed = injectedMessage.replyDeadline !== void 0 && injectedMessage.replyDeadline < Date.now();
    const waiting = injectedMessage.senderWaitMode === "blocking" ? " \xB7 blocking ask" : injectedMessage.senderWaitMode === "nonblocking" ? " \xB7 async ask" : "";
    const request = injectedMessage.expectsReply ? `
Reply requested${waiting}.${elapsed ? " The original wait window has elapsed." : ""}` : "";
    const compactionNotice = injectedMessage.peerCompaction ? `

${describeCompaction(senderDisplay, injectedMessage.peerCompaction, entry.from.id)}` : "";
    const body = entry.bodyText ?? injectedMessage.content.text + (injectedMessage.content.attachments?.length ? formatAttachments(injectedMessage.content.attachments) : "");
    return {
      content: presentForModel(`**From ${senderDisplay}**${focus}${request}${verbatim(topic)}

${verbatim(body)}

${formatInboundDeliveryMetadata(injectedMessage, messageRef)}
Session: ${entry.from.cwd}${origin}${compactionNotice}`),
      topic,
      isRequestedAnswer
    };
  }
  function sendIncomingMessage(entry, delivery, generation = runtimeGeneration, forceTrigger = false) {
    if (runtimeStarted && !getLiveContext(runtimeContext, generation)) return;
    const injectedMessage = { ...entry.message, injectedAt: Date.now() };
    const { content, topic, isRequestedAnswer } = formatInboundEnvelope(entry, injectedMessage);
    const envelope = {
      customType: "parley_message",
      content,
      display: true,
      details: { ...entry, message: injectedMessage, replyTopic: topic, [PRESENTED_MARK]: true },
      timestamp: entry.message.receiverReceivedAt ?? entry.message.timestamp
    };
    deferredInboundMessages.set(entry.message.id, entry);
    pendingHostEnvelopes.set(`message:${entry.message.id}`, envelope);
    try {
      pi.sendMessage(
        envelope,
        delivery === "trigger" && shouldTriggerInboundMessage(entry, forceTrigger || isRequestedAnswer) ? { triggerTurn: true } : { deliverAs: "steer" }
      );
    } catch {
      emitMessageReceipt(entry.message.id, "queued", "retained locally; host injection will be retried");
    }
    confirmPersistedInbound();
    scheduleInboundRetry();
  }
  function surfaceInboundCompactionOnly(from, message, generation) {
    if (!message.peerCompaction || !getLiveContext(runtimeContext, generation)) return;
    const senderDisplay = references.current.sessionRef(from);
    pi.sendMessage(
      {
        customType: "parley_compaction_awareness",
        content: presentForModel(`**Parley compaction awareness**

${describeCompaction(senderDisplay, message.peerCompaction, from.id)}`, { from, messageId: message.id }),
        display: true,
        details: { messageId: message.id, peerCompaction: message.peerCompaction, [PRESENTED_MARK]: true }
      },
      { triggerTurn: false }
    );
    acknowledgeInboundMessageContact(client, message);
  }
  function sendIncomingBrokerMessage(entry, delivery, generation = runtimeGeneration) {
    sendIncomingMessage(entry, delivery, generation);
  }
  function handleIncomingMessage(ctx, from, message) {
    const messageGeneration = runtimeGeneration;
    const liveContext = getLiveContext(ctx, messageGeneration);
    if (!liveContext) {
      return;
    }
    const receiverReceivedAt = Date.now();
    if (hasSeenInboundMessage(from, message, receiverReceivedAt)) {
      surfaceInboundCompactionOnly(from, message, messageGeneration);
      emitMessageReceipt(message.id, "acknowledged", "duplicate message id suppressed");
      return;
    }
    const receivedMessage = { ...message, receiverReceivedAt };
    emitMessageReceipt(receivedMessage.id, "receiver_received");
    const waiter = replyWaiter;
    const fromMatches = waiter && ((from.name || from.id).toLowerCase() === waiter.from.toLowerCase() || from.id === waiter.from);
    const matchedWaiter = fromMatches && (waiter.endpointEpoch === void 0 || waiter.endpointEpoch === from.endpointEpoch) && (waiter.originEpoch === void 0 || waiter.originEpoch === from.federation?.originEpoch) && receivedMessage.replyTo === waiter.replyTo && receivedMessage.completesAsk !== false && !receivedMessage.expectsReply;
    replyTracker.recordIncomingMessage(from, receivedMessage, receiverReceivedAt);
    recordConversationEntry("parley_inbound_received", {
      from,
      message: receivedMessage,
      receivedAt: receiverReceivedAt,
      ...matchedWaiter ? { delivery: "tool_result" } : {}
    });
    if (matchedWaiter) {
      emitMessageReceipt(receivedMessage.id, "acknowledged", "matched reply waiter");
      waiter.resolve(receivedMessage);
      return;
    }
    const attachmentText = receivedMessage.content.attachments?.length ? formatAttachments(receivedMessage.content.attachments) : "";
    const bodyText = `${receivedMessage.content.text}${attachmentText}`;
    const replyCommand = config.replyHint && receivedMessage.expectsReply ? `parley({ action: "reply", message: "..." })` : void 0;
    emitMessageReceipt(receivedMessage.id, "acknowledged", "accepted by receiver");
    const entry = { from, message: receivedMessage, replyCommand, bodyText };
    void (async () => {
      const activeContext = getLiveContext(liveContext, messageGeneration);
      if (!activeContext) {
        return;
      }
      if (!activeContext.isIdle()) {
        sendIncomingBrokerMessage(entry, "steer");
        return;
      }
      if (getLiveContext(liveContext, messageGeneration)) {
        sendIncomingBrokerMessage(entry, "trigger", messageGeneration);
      }
    })();
  }
  function attachClientHandlers(nextClient) {
    nextClient.onBrokerMessage((message) => {
      if (client !== nextClient) return;
      switch (message.type) {
        case "registered": {
          const supported = message.features?.includes(EXTENSION_BUS_FEATURE) ?? false;
          if (supported && localExtensions.size > 0) {
            nextClient.updateExtensionCapabilities(currentExtensionCapabilities());
          }
          for (const namespace of localExtensions.keys()) {
            emitLocalExtensionEvent(namespace, { type: "connection", connected: true, supported });
          }
          flushPendingCompactionReports();
          for (const token of pendingReceiverBaselineTokens) {
            nextClient.acknowledgeContactToken(token);
          }
          scheduleReceiverBaselineRetry();
          break;
        }
        case "direct_contact_recorded":
          if (pendingReceiverBaselineTokens.delete(message.token)) {
            if (pendingReceiverBaselineTokens.size === 0) clearReceiverBaselineRetryTimer();
            recordConversationEntry("parley_receiver_baseline_recorded", {
              token: message.token,
              timestamp: Date.now()
            });
          }
          break;
        case "direct_contact_unknown":
          if (pendingReceiverBaselineTokens.delete(message.token)) {
            if (pendingReceiverBaselineTokens.size === 0) clearReceiverBaselineRetryTimer();
            recordConversationEntry("parley_receiver_baseline_abandoned", {
              token: message.token,
              timestamp: Date.now(),
              reason: "Broker no longer retains the bounded staged baseline token"
            });
          }
          break;
        case "extension_owner": {
          const extension = localExtensions.get(message.namespace);
          if (!extension) break;
          extension.owner = message.ownerId && message.ownerEpoch ? { sessionId: message.ownerId, epoch: message.ownerEpoch } : void 0;
          emitLocalExtensionEvent(message.namespace, { type: "owner", ...extension.owner ? { owner: extension.owner } : {} });
          break;
        }
        case "extension_message": {
          const extension = localExtensions.get(message.namespace);
          if (!extension) break;
          emitLocalExtensionEvent(message.namespace, {
            type: "message",
            fromSessionId: message.fromSessionId,
            ...message.ownerId && message.ownerEpoch ? { owner: { sessionId: message.ownerId, epoch: message.ownerEpoch } } : {},
            payload: message.payload
          });
          break;
        }
        case "extension_state": {
          const extension = localExtensions.get(message.namespace);
          if (!extension) break;
          extension.state = { revision: message.revision, payload: message.payload };
          emitLocalExtensionEvent(message.namespace, { type: "state", state: extension.state });
          break;
        }
        case "extension_state_result":
          emitLocalExtensionEvent(message.namespace, {
            type: "state_result",
            committed: message.committed,
            revision: message.revision,
            ...message.reason ? { reason: message.reason } : {}
          });
          break;
        case "message_receipt":
          latestOutboundReceipts.set(message.receipt.messageId, {
            status: message.receipt.status,
            timestamp: message.receipt.timestamp,
            ...message.receipt.detail ? { detail: message.receipt.detail } : {}
          });
          if (message.receipt.status === "expired") {
            handleExpiredOutboundReceipt(message.from, message.receipt);
          }
          break;
        case "message_control":
          handleMessageControl(message.from, message.control);
          break;
        case "session_joined":
          references.current.observeLive([message.session]);
          for (const namespace of localExtensions.keys()) {
            emitLocalExtensionEvent(namespace, { type: "session_joined", session: message.session });
          }
          break;
        case "session_left":
          references.current.observeSessionLeft(message.sessionId);
          for (const namespace of localExtensions.keys()) {
            emitLocalExtensionEvent(namespace, { type: "session_left", sessionId: message.sessionId });
          }
          break;
        case "presence_update":
          references.current.observeLive([message.session]);
          for (const namespace of localExtensions.keys()) {
            emitLocalExtensionEvent(namespace, { type: "presence_update", session: message.session });
          }
          break;
      }
    });
    nextClient.on("roster", (sessions) => {
      if (client === nextClient) references.current.observeLive(sessions, { complete: true });
    });
    nextClient.on("message", (from, message) => {
      const liveContext = getLiveContext();
      if (client !== nextClient || !liveContext) {
        return;
      }
      handleIncomingMessage(liveContext, from, message);
    });
    nextClient.on("disconnected", (error) => {
      if (client !== nextClient) {
        return;
      }
      references.current.forgetReachability();
      rejectReplyWaiter(new Error(`Disconnected while waiting for reply: ${error.message}`, { cause: error }));
      for (const [namespace, extension] of localExtensions) {
        extension.owner = void 0;
        emitLocalExtensionEvent(namespace, { type: "connection", connected: false, supported: false });
        emitLocalExtensionEvent(namespace, { type: "owner" });
      }
      lastPresenceRequestedName = void 0;
      client = null;
      if (!shuttingDown && !disposed) {
        clearReconnectTimer();
        scheduleReconnect();
      }
    });
    nextClient.on("error", () => {
    });
  }
  function scheduleReconnect() {
    if (disposed || shuttingDown || reconnectTimer || reconnectPromise || !getLiveContext()) {
      return;
    }
    const scheduledGeneration = runtimeGeneration;
    reconnectTimer = setTimeout(() => {
      reconnectTimer = null;
      if (scheduledGeneration !== runtimeGeneration || !getLiveContext()) {
        return;
      }
      reconnectAttempt += 1;
      void ensureConnected("background").catch(() => {
      });
    }, getReconnectDelayMs());
  }
  async function ensureConnected(reason) {
    if (!config.enabled) {
      throw new Error("Parley disabled");
    }
    if (disposed || shuttingDown) {
      throw new Error("Parley shutting down");
    }
    if (client && client.isConnected()) {
      return client;
    }
    const contextAtStart = getLiveContext();
    const generationAtStart = runtimeGeneration;
    if (!contextAtStart || !currentSessionId || sessionStartedAt === null) {
      throw new Error("Parley runtime not initialized");
    }
    clearReconnectTimer();
    if (reconnectPromise && reconnectPromiseGeneration === generationAtStart) {
      return reconnectPromise;
    }
    let nextReconnectPromise;
    let retryAfterFailure = false;
    nextReconnectPromise = (async () => {
      const nextClient = new ParleyClient();
      client = nextClient;
      attachClientHandlers(nextClient);
      try {
        await spawnBrokerIfNeeded(config.brokerCommand, config.brokerArgs);
        await nextClient.connect(buildRegistration(), currentParleySessionId ?? currentSessionId);
        if (!getLiveContext(contextAtStart, generationAtStart)) {
          await nextClient.disconnect();
          throw new Error("Parley runtime no longer active");
        }
        client = nextClient;
        reconnectAttempt = 0;
        syncPresenceIdentity(currentSessionId);
        return nextClient;
      } catch (error) {
        if (client === nextClient) {
          client = null;
        }
        await nextClient.disconnect().catch(() => void 0);
        retryAfterFailure = getLiveContext(contextAtStart, generationAtStart) !== null;
        throw toError3(error);
      } finally {
        if (reconnectPromise === nextReconnectPromise) {
          reconnectPromise = null;
          reconnectPromiseGeneration = null;
        }
        if (retryAfterFailure) {
          scheduleReconnect();
        }
      }
    })();
    reconnectPromise = nextReconnectPromise;
    reconnectPromiseGeneration = generationAtStart;
    return nextReconnectPromise;
  }
  function originOf(session) {
    return session.federation ? references.current.originRef(session.federation) : void 0;
  }
  function rosterReferences(sessions) {
    const selfId = client?.sessionId;
    const ordered = [...sessions].sort((left, right) => Number(right.id === selfId) - Number(left.id === selfId) || Number(Boolean(left.federation)) - Number(Boolean(right.federation)) || left.startedAt - right.startedAt);
    return new Map(ordered.map((session) => [session.id, references.current.sessionRef(session)]));
  }
  function resolveSessionFromRoster(sessions, nameOrId, exact = false) {
    const byId = sessions.find((s) => s.id === nameOrId);
    if (byId) {
      return byId;
    }
    if (exact) {
      return null;
    }
    const lowerName = nameOrId.toLowerCase();
    const byName = sessions.filter((s) => s.name?.toLowerCase() === lowerName);
    if (byName.length > 1) {
      const candidates = [...byName].sort((left, right) => Number(Boolean(left.federation)) - Number(Boolean(right.federation))).map((session) => `${references.current.sessionRef(session)} (${session.federation ? `remote:${references.current.originRef(session.federation)}` : session.cwd})`).join(", ");
      throw new Error(`Multiple sessions named "${nameOrId}" are connected: ${candidates}. Address one by its reference.`);
    }
    if (byName.length === 1) {
      return byName[0];
    }
    const byLastPart = sessions.filter((s) => s.name?.toLowerCase().split(":").at(-1)?.trim() === lowerName.trim());
    if (byLastPart.length === 1) {
      const session = byLastPart[0];
      const ref = references.current.sessionRef(session);
      shortNameNotes.getStore()?.push(`"${nameOrId}" isn't a full session name, so this went to ${ref}, the only session it matches. Use ${ref} to be exact.`);
      return session;
    }
    const byIdPrefix = sessions.filter((s) => s.id.startsWith(nameOrId));
    if (byIdPrefix.length === 1) {
      return byIdPrefix[0];
    }
    if (byIdPrefix.length > 1) {
      throw new Error(`Multiple sessions match ID prefix "${nameOrId}". Use a longer session ID prefix.`);
    }
    return null;
  }
  async function resolveSessionTarget(activeClient, nameOrId, exact = false) {
    return resolveSessionFromRoster(await activeClient.listSessions(), nameOrId, exact)?.id ?? null;
  }
  async function resolveSupervisorTarget(activeClient, metadata) {
    const sessions = await activeClient.listSessions();
    if (metadata.orchestratorSessionId) {
      const bySessionId = resolveSessionFromRoster(sessions, metadata.orchestratorSessionId);
      if (bySessionId) return bySessionId;
    }
    return resolveSessionFromRoster(sessions, metadata.orchestratorTarget);
  }
  async function resolveCwdDeliveryTarget(activeClient, options2) {
    const sessions = await activeClient.listSessions();
    const currentSessionId2 = activeClient.sessionId;
    if (!currentSessionId2) {
      throw new Error("Current session is not registered with parley.");
    }
    const currentSession = sessions.find((session2) => session2.id === currentSessionId2);
    if (!currentSession) {
      throw new Error("Current session is missing from parley session list.");
    }
    const targetCwd = options2.cwd && options2.cwd !== "." ? resolvePath(currentSession.cwd, options2.cwd) : currentSession.cwd;
    const exactSession = options2.exactTo && options2.to ? sessions.find((session2) => session2.id === options2.to && !session2.federation && sameCwd(session2.cwd, targetCwd)) : void 0;
    const existing = options2.exactTo && options2.to ? exactSession ? { kind: "found", session: exactSession, targetCwd } : { kind: "missing", targetCwd, reason: `${sessionName(options2.to)} is not connected in ${targetCwd}.` } : resolveTargetInCwd({
      sessions,
      currentSessionId: currentSessionId2,
      targetCwd,
      ...options2.to ? { to: options2.to } : {},
      sessionRef: (session2) => references.current.sessionRef(session2)
    });
    if (existing.kind === "found" && existing.session) {
      return { id: existing.session.id, label: options2.to || existing.session.name || existing.session.id, session: existing.session };
    }
    if (!options2.openProjectPaneIfMissing) {
      throw new Error(`${existing.reason ?? `No parley session is connected in ${targetCwd}.`} No session was launched and no message was sent.`);
    }
    const beforeSessionIds = new Set(sessions.map((session2) => session2.id));
    const projectPane = await openProjectPane({
      cwd: targetCwd,
      focus: options2.focus,
      sessions,
      currentSessionId: currentSessionId2,
      launcherCommand: resolveProjectLauncherCommand(process.env, config.projectLauncher),
      sendRequest: (provider, request) => activeClient.send(provider.id, {
        text: projectLaunchRequestText(request),
        signal: options2.signal
      }).then((result) => ({ delivered: result.delivered, id: result.id, outcomeKnown: result.outcomeKnown, ...result.reason ? { reason: result.reason } : {} })),
      signal: options2.signal
    });
    const session = await waitForProjectSession(activeClient, {
      sessionRef: (candidate) => references.current.sessionRef(candidate),
      projectRoot: projectPane.projectRoot,
      currentSessionId: currentSessionId2,
      beforeSessionIds,
      launch: projectPane,
      signal: options2.signal
    });
    return { id: session.id, label: session.name || session.id, projectPane, session };
  }
  function projectObservation(target2) {
    if (!target2.projectPane) return "";
    const launch = target2.projectPane;
    return `
Project launch: ${launch.outcome} in ${launch.projectRoot}${launch.requestMessageId ? ` (request ${messageRef(launch.requestMessageId)})` : ""}.
Observed local session: ${target2.session ? references.current.sessionRef(target2.session) : sessionName(target2.id)}. Registration does not prove which launch created it.`;
  }
  async function sendBatchMessages(activeClient, targets, options2) {
    const record = options2.record ?? recordConversationEntry;
    return mapWithConcurrency(targets, SEND_FANOUT_CONCURRENCY, async (target2) => {
      if (options2.signal?.aborted) {
        return {
          to: target2.label,
          ...target2.session ? { targetId: target2.session.id } : {},
          delivered: false,
          delivery: "failed",
          retryable: true,
          outcomeKnown: true,
          code: "E_CANCELLED",
          reason: "Send cancelled before this recipient was attempted"
        };
      }
      if (target2.resolutionError) {
        return {
          to: target2.label,
          delivered: false,
          delivery: "failed",
          retryable: false,
          outcomeKnown: true,
          code: target2.resolutionCode ?? "E_TARGET_RESOLUTION",
          reason: target2.resolutionError
        };
      }
      try {
        const sendOptions = {
          text: options2.message,
          attachments: options2.attachments,
          signal: options2.signal,
          contactKind: options2.broadcast ? "broadcast" : "direct"
        };
        let result = target2.session ? await activeClient.sendToSession(target2.session, sendOptions) : await activeClient.send(target2.requested, { ...sendOptions, exactIdentity: options2.isExactIdentity?.(target2.requested) === true });
        if (!options2.broadcast && options2.allowRosterMailboxFallback !== false && target2.session && result.code === "E_TARGET_NOT_FOUND") {
          if (options2.signal?.aborted) {
            return {
              to: target2.label,
              targetId: target2.session.id,
              messageId: result.id,
              delivered: false,
              delivery: "failed",
              retryable: true,
              outcomeKnown: true,
              code: "E_CANCELLED",
              reason: "Send cancelled before offline delivery was attempted"
            };
          }
          result = await activeClient.send(target2.session.id, { ...sendOptions, exactIdentity: true });
        }
        const outcome = {
          to: result.recipient ? references.current.sessionRef(result.recipient) : target2.label,
          requested: target2.requested,
          ...result.recipient ? { targetId: result.recipient.id } : target2.session ? { targetId: target2.session.id } : {},
          messageId: result.id,
          delivered: result.delivered,
          delivery: result.delivery,
          retryable: result.retryable,
          outcomeKnown: result.outcomeKnown,
          ...result.code ? { code: result.code } : {},
          ...result.reason ? { reason: result.reason } : {},
          ...result.peerCompaction ? { peerCompaction: result.peerCompaction } : {}
        };
        if (result.delivered) {
          record("parley_sent", {
            to: target2.label,
            targetId: result.recipient?.id ?? target2.session?.id,
            message: { text: options2.message, attachments: options2.attachments },
            messageId: result.id,
            batchId: options2.batchId,
            broadcast: options2.broadcast,
            ...result.peerCompaction ? { peerCompaction: result.peerCompaction } : {},
            timestamp: Date.now()
          });
        }
        activeClient.acknowledgeSendContact(result);
        return outcome;
      } catch (error) {
        return {
          to: target2.label,
          ...target2.session ? { targetId: target2.session.id } : {},
          delivered: false,
          delivery: "unknown",
          retryable: true,
          outcomeKnown: false,
          reason: failureText(error)
        };
      }
    });
  }
  function deliverLocalSubagentRelayMessage(sender, status, messageText) {
    const liveContext = getLiveContext();
    const now = Date.now();
    sendIncomingMessage({
      from: {
        id: sender,
        name: sender,
        cwd: liveContext?.cwd ?? "",
        model: sender,
        pid: process.pid,
        startedAt: now,
        lastActivity: now,
        status
      },
      message: {
        id: randomUUID6(),
        timestamp: now,
        content: { text: messageText }
      },
      bodyText: messageText
    }, "trigger", runtimeGeneration, true);
  }
  function recordSubagentDeliveryError(entryType, to, message, error) {
    pi.appendEntry(entryType, {
      to,
      message,
      error: failureText(error),
      timestamp: Date.now()
    });
  }
  function restoreConversations(ctx) {
    if (inboundRetryTimer) clearTimeout(inboundRetryTimer);
    inboundRetryTimer = null;
    deferredInboundMessages.clear();
    deferredInboundControls.clear();
    pendingHostEnvelopes.clear();
    contextFallbackEnvelopes.clear();
    presentedInboundEnvelopes.clear();
    settledInboundMessages.clear();
    inboundMessageDispositions.clear();
    freshModelContexts.clear();
    outstandingAsks.clear();
    seenInboundMessages.clear();
    latestOutboundReceipts.clear();
    conversationPersistenceWarning = void 0;
    const restoredEntries = ctx.sessionManager.getEntries();
    sessionBook = createReferenceBook();
    references.current.restore(restoredEntries.flatMap((entry) => {
      const custom = entry;
      return custom.type === "custom" && custom.customType === REFERENCE_ENTRY_TYPE ? [custom.data] : [];
    }));
    const history = restoreConversationHistory(restoredEntries);
    for (const id of history.persistedIncoming) presentedInboundEnvelopes.add(`message:${id}`);
    for (const key of history.persistedControls) presentedInboundEnvelopes.add(`control:${key}`);
    for (const id of history.settledIncoming) settledInboundMessages.add(id);
    for (const [id, ask] of history.outgoing) outstandingAsks.set(id, ask);
    for (const context of history.incoming.values()) {
      replyTracker.recordIncomingMessage(context.from, context.message, context.receivedAt);
      hasSeenInboundMessage(context.from, context.message);
      if (history.settledIncoming.has(context.message.id)) {
        replyTracker.dismissPendingAsk(context.message.id);
      } else if (!history.persistedIncoming.has(context.message.id)) {
        deferredInboundMessages.set(context.message.id, {
          from: context.from,
          message: context.message,
          bodyText: context.message.content.text + (context.message.content.attachments?.length ? formatAttachments(context.message.content.attachments) : "")
        });
      }
    }
    for (const [key, value] of history.controls) {
      const disposition = {
        state: value.control.action === "cancel" ? "withdrawn" : "superseded",
        ...value.control.supersededBy ? { replacementId: value.control.supersededBy } : {}
      };
      inboundMessageDispositions.set(value.control.messageId, disposition);
      replyTracker.setDisposition(value.control.messageId, disposition);
      if (!history.persistedControls.has(key)) handleMessageControl(value.from, value.control, true);
    }
    for (const entry of deferredInboundMessages.values()) sendIncomingMessage(entry, ctx.isIdle() ? "trigger" : "steer");
    if (deferredInboundMessages.size || deferredInboundControls.size) scheduleInboundRetry();
  }
  function startSessionRuntime(ctx) {
    const nextSessionId = ctx.sessionManager.getSessionId();
    const nextParleySessionId = resolveConfiguredParleySessionId(nextSessionId, config);
    try {
      buildPresenceIdentity(pi, nextParleySessionId, configuration);
    } catch (error) {
      console.warn(`Parley presence name resolution deferred on session start: ${failureText(error)}`);
    }
    const previousClient = client;
    failPendingOutboxRequests(runtimeGeneration, "session_ended", "Session replaced");
    shuttingDown = false;
    disposed = false;
    runtimeStarted = true;
    runtimeGeneration += 1;
    compactionReportFlush = null;
    clearCompactionReportRetryTimer();
    clearReceiverBaselineRetryTimer();
    restorePendingCompactionReports(ctx);
    outboxRequestIds.clear();
    reconnectAttempt = 0;
    clearReconnectTimer();
    clearStartupConnectTimer();
    rejectReplyWaiter(new Error("Session replaced"));
    replyTracker.reset();
    if (previousClient) {
      client = null;
      void previousClient.disconnect().catch(() => void 0);
    }
    runtimeContext = ctx;
    currentSessionId = nextSessionId;
    currentParleySessionId = nextParleySessionId;
    publishParleySessionId(currentParleySessionId);
    currentModel = ctx.model?.id ?? "unknown";
    sessionStartedAt = Date.now();
    restoreConversations(ctx);
    agentRunning = false;
    activeTools.clear();
    resetCompactionStatus();
    startSessionNameCompatibilityTimer();
    const startupGeneration = runtimeGeneration;
    startupConnectTimer = setTimeout(() => {
      startupConnectTimer = null;
      if (!getLiveContext(ctx, startupGeneration)) {
        return;
      }
      try {
        buildPresenceIdentity(pi, nextParleySessionId, configuration);
      } catch (error) {
        console.warn(`Parley broker connection deferred: presence name resolution failed (${failureText(error)})`);
        return;
      }
      void ensureConnected("startup").catch(() => {
        if (!getLiveContext(ctx, startupGeneration)) {
          return;
        }
        client = null;
        scheduleReconnect();
      });
    }, 0);
  }
  function emitResultDelivery(requestId, delivered, error) {
    if (!requestId) return;
    pi.events.emit(SUBAGENT_RESULT_PARLEY_DELIVERY_EVENT, {
      requestId,
      delivered,
      ...error ? { error: failureText(error) } : {}
    });
  }
  function relaySubagentParleyPayload(payload, options2) {
    const parsed = parseSubagentParleyPayload(payload);
    if (!parsed) return;
    const relayGeneration = runtimeGeneration;
    void (async () => {
      const relayStillLive = () => !runtimeStarted || Boolean(getLiveContext(runtimeContext, relayGeneration));
      if (!relayStillLive()) {
        return;
      }
      if (currentSessionTargetMatches(parsed.to)) {
        deliverLocalSubagentRelayMessage(options2.sender, options2.status, parsed.message);
        if (options2.acknowledge) emitResultDelivery(parsed.requestId, true);
        return;
      }
      let activeClient;
      let target2;
      try {
        activeClient = await ensureConnected("background");
        target2 = await resolveSessionTarget(activeClient, parsed.to) ?? parsed.to;
      } catch (error) {
        if (!relayStillLive()) return;
        recordSubagentDeliveryError(options2.errorEntryType, parsed.to, parsed.message, error);
        if (options2.acknowledge) emitResultDelivery(parsed.requestId, false, error);
        return;
      }
      if (!relayStillLive()) {
        return;
      }
      if (currentSessionTargetMatches(parsed.to, target2, activeClient)) {
        deliverLocalSubagentRelayMessage(options2.sender, options2.status, parsed.message);
        if (options2.acknowledge) emitResultDelivery(parsed.requestId, true);
        return;
      }
      try {
        const result = await activeClient.send(target2, { text: parsed.message });
        if (!relayStillLive()) return;
        if (!result.delivered) {
          const error = new Error(result.reason ?? "Session may not exist or has disconnected.");
          recordSubagentDeliveryError(options2.errorEntryType, parsed.to, parsed.message, error);
          if (options2.acknowledge) emitResultDelivery(parsed.requestId, false, error);
          return;
        }
        surfaceBackgroundPeerCompaction(activeClient, parsed.to, result, relayGeneration, target2);
        if (options2.acknowledge) emitResultDelivery(parsed.requestId, true);
      } catch (error) {
        if (!relayStillLive()) return;
        recordSubagentDeliveryError(options2.errorEntryType, parsed.to, parsed.message, error);
        if (options2.acknowledge) emitResultDelivery(parsed.requestId, false, error);
      }
    })();
  }
  const unsubscribeExtensionRegister = pi.events.on(PARLEY_EXTENSION_REGISTER_EVENT, (payload) => {
    if (!payload || typeof payload !== "object") return;
    const registration = payload;
    if (typeof registration.namespace !== "string" || typeof registration.ownerEligible !== "boolean" || typeof registration.onEvent !== "function" || typeof registration.onReady !== "function") {
      return;
    }
    registerLocalExtension(registration);
  });
  retainRegistrationCleanup(unsubscribeExtensionRegister);
  const unsubscribeSubagentControlParley = pi.events.on(SUBAGENT_CONTROL_PARLEY_EVENT, (payload) => {
    relaySubagentParleyPayload(payload, {
      sender: "subagent-control",
      status: "needs_attention",
      errorEntryType: "parley_control_error"
    });
  });
  retainRegistrationCleanup(unsubscribeSubagentControlParley);
  const unsubscribeSubagentResultParley = pi.events.on(SUBAGENT_RESULT_PARLEY_EVENT, (payload) => {
    relaySubagentParleyPayload(payload, {
      sender: "subagent-result",
      status: "result",
      errorEntryType: "parley_result_error",
      acknowledge: true
    });
  });
  retainRegistrationCleanup(unsubscribeSubagentResultParley);
  const unsubscribeOutboxRequest = pi.events.on(PARLEY_OUTBOX_REQUEST_EVENT, handleOutboxRequest);
  retainRegistrationCleanup(unsubscribeOutboxRequest);
  pi.on("session_start", (_event, ctx) => {
    if (!config.enabled) {
      return;
    }
    startSessionRuntime(ctx);
    try {
      linkSessionInteractive = ctx.hasUI === true;
    } catch {
      linkSessionInteractive = false;
    }
    links.start();
  });
  pi.on("session_shutdown", async () => {
    unsubscribeExtensionRegister();
    unsubscribeSubagentControlParley();
    unsubscribeSubagentResultParley();
    unsubscribeOutboxRequest();
    shuttingDown = true;
    disposed = true;
    links.stop();
    failPendingOutboxRequests(runtimeGeneration, "session_ended", "Session shutting down");
    runtimeGeneration += 1;
    compactionReportFlush = null;
    clearCompactionReportRetryTimer();
    clearReceiverBaselineRetryTimer();
    clearStartupConnectTimer();
    clearReconnectTimer();
    clearSessionNameCompatibilityTimer();
    restoreParleySessionId();
    rejectReplyWaiter(new Error("Session shutting down"));
    replyTracker.reset();
    outstandingAsks.clear();
    deferredInboundMessages.clear();
    deferredInboundControls.clear();
    pendingHostEnvelopes.clear();
    contextFallbackEnvelopes.clear();
    presentedInboundEnvelopes.clear();
    settledInboundMessages.clear();
    inboundMessageDispositions.clear();
    freshModelContexts.clear();
    if (inboundRetryTimer) clearTimeout(inboundRetryTimer);
    inboundRetryTimer = null;
    agentRunning = false;
    activeTools.clear();
    resetCompactionStatus();
    if (client) {
      await client.disconnect();
      client = null;
    }
    runtimeContext = null;
    currentSessionId = null;
    currentParleySessionId = null;
    currentSessionDescription = void 0;
    profileManagedName = void 0;
    lastPresenceRequestedName = void 0;
    profileNameMutationTarget = void 0;
    profileOwnershipRevocationPending = false;
    sessionStartedAt = null;
  });
  const onSessionInfoChanged = pi.on.bind(pi);
  onSessionInfoChanged("session_info_changed", (event, ctx) => {
    if (!currentSessionId || !getLiveContext(ctx)) {
      return;
    }
    observedSessionName = event.name?.trim() || void 0;
    syncPresenceIdentity(currentSessionId);
  });
  pi.on("session_before_compact", (event, ctx) => {
    beginCompactionStatus(ctx, event.signal);
  });
  pi.on("session_compact", (_event, ctx) => {
    finishCompactionStatus(ctx);
    if (getLiveContext(ctx)) {
      const eventId = randomUUID6();
      const compactedAt = Date.now();
      pi.appendEntry("parley_compaction_pending", { eventId, compactedAt });
      pendingCompactionReports.set(eventId, compactedAt);
      flushPendingCompactionReports();
    }
  });
  const onSessionCompactFailed = pi.on.bind(pi);
  onSessionCompactFailed("session_compact_failed", (_event, ctx) => {
    finishCompactionStatus(ctx);
  });
  pi.on("context", (event) => {
    if (!getLiveContext()) return;
    confirmPersistedInbound();
    const seen = /* @__PURE__ */ new Set();
    const messages = [];
    const projectionIdentities = { messages: /* @__PURE__ */ new Set(), sessions: /* @__PURE__ */ new Set() };
    for (const message of event.messages) {
      if (message.role === "assistant" && Array.isArray(message.content)) {
        for (const part of message.content) {
          if (part.type === "toolCall" && part.name === PARLEY_TOOL_NAME) declareToolArguments(part.arguments, projectionIdentities);
        }
      } else if (message.role === "toolResult" && (message.toolName === PARLEY_TOOL_NAME || message.toolName === "contact_supervisor") || message.role === "custom" && message.customType?.startsWith("parley_")) {
        references.current.learnFrom(message.details, projectionIdentities);
      }
    }
    const canonicalIdentities = {
      messages: new Set([...projectionIdentities.messages].filter(isCanonicalIdentity)),
      sessions: new Set([...projectionIdentities.sessions].filter(isCanonicalIdentity))
    };
    for (const message of event.messages) {
      if (message.role === "assistant" && Array.isArray(message.content)) {
        messages.push({ ...message, content: message.content.map((part) => part.type === "toolCall" && part.name === PARLEY_TOOL_NAME ? { ...part, arguments: presentToolArgumentsForModel(part.arguments, projectionIdentities) } : part) });
        continue;
      }
      if (message.role === "compactionSummary" || message.role === "branchSummary") {
        messages.push({ ...message, summary: references.current.present(message.summary, canonicalIdentities) });
        continue;
      }
      if (message.role === "toolResult" && (message.toolName === PARLEY_TOOL_NAME || message.toolName === "contact_supervisor")) {
        messages.push({ ...message, content: projectForModel(message.content, message.details, canonicalIdentities) });
        continue;
      }
      if (message.role !== "custom") {
        messages.push(message);
        continue;
      }
      if (message.customType === "parley_persistence_notice" && conversationPersistenceWarning) continue;
      const key = inboundEnvelopeKey(message);
      if (!key) {
        messages.push(message.customType?.startsWith("parley_") ? { ...message, content: projectForModel(message.content, message.details, canonicalIdentities) } : message);
        continue;
      }
      if (seen.has(key)) continue;
      seen.add(key);
      const details = message.details;
      const timestamp = details.message?.receiverReceivedAt ?? details.receivedAt ?? details.message?.timestamp ?? details.control?.timestamp ?? message.timestamp;
      if (!isHistoricalInboundEnvelope(key)) {
        const presented = message.details?.[PRESENTED_MARK] === true;
        const stored = typeof message.content === "string" ? message.content : message.content.map((part) => part.type === "text" ? part.text : "").join("\n");
        const content = !presented && message.customType === "parley_message" && details.message && references.current.present(stored, canonicalIdentities) !== stored ? formatInboundEnvelope(message.details, details.message).content : projectForModel(message.content, message.details, canonicalIdentities);
        messages.push({ ...message, content, timestamp });
        continue;
      }
      messages.push({ ...message, content: historicalInboundEnvelope({ ...message, timestamp }), timestamp });
    }
    for (const [key, envelope] of pendingHostEnvelopes) {
      if (!seen.has(key)) contextFallbackEnvelopes.set(key, envelope);
      confirmInboundEnvelope(key);
    }
    const history = [];
    for (const [key, envelope] of contextFallbackEnvelopes) {
      if (seen.has(key)) continue;
      if (isHistoricalInboundEnvelope(key)) {
        history.push({ role: "custom", ...envelope, content: historicalInboundEnvelope(envelope) });
      } else {
        messages.push({ role: "custom", ...envelope });
      }
      seen.add(key);
    }
    for (const key of seen) presentedInboundEnvelopes.add(key);
    messages.unshift(...history);
    if (conversationPersistenceWarning) messages.push({
      role: "custom",
      customType: "parley_persistence_notice",
      content: conversationPersistenceWarning,
      display: true,
      timestamp: Date.now()
    });
    replyTracker.activateContexts([...freshModelContexts.values()]);
    freshModelContexts.clear();
    return { messages };
  });
  pi.on("agent_start", () => {
    if (!getLiveContext()) {
      return;
    }
    agentRunning = true;
    activeTools.clear();
    syncPresenceStatus();
  });
  pi.on("tool_execution_start", (event) => {
    if (!getLiveContext()) {
      return;
    }
    activeTools.set(event.toolCallId, event.toolName);
    syncPresenceStatus();
  });
  pi.on("tool_execution_end", (event) => {
    if (!getLiveContext()) {
      return;
    }
    activeTools.delete(event.toolCallId);
    syncPresenceStatus();
  });
  pi.on("agent_end", () => {
    if (!getLiveContext()) {
      return;
    }
    replyTracker.clearActiveContexts();
    agentRunning = false;
    activeTools.clear();
    syncPresenceStatus();
  });
  pi.on("turn_start", (_event, ctx) => {
    const sessionId = ctx.sessionManager.getSessionId();
    if (!currentSessionId || sessionId !== currentSessionId) {
      if (!config.enabled) {
        return;
      }
      startSessionRuntime(ctx);
      return;
    }
    if (!getLiveContext(ctx)) {
      return;
    }
    try {
      syncPresenceIdentity(sessionId);
    } catch (error) {
      console.warn(`Parley presence sync on turn start failed: ${failureText(error)}`);
    }
  });
  pi.on("model_select", (event, ctx) => {
    if (!getLiveContext(ctx)) {
      return;
    }
    currentModel = event.model.id;
    if (client) {
      try {
        client.updatePresence({
          ...buildPresenceIdentity(pi, currentParleySessionId ?? ctx.sessionManager.getSessionId(), configuration),
          model: event.model.id,
          status: currentStatus()
        });
      } catch (error) {
        console.warn(`Parley presence update on model select failed: ${failureText(error)}`);
      }
    }
  });
  pi.registerMessageRenderer("parley_message", (message, options2, theme) => {
    const details = message.details;
    if (!details) return void 0;
    return new InlineMessageComponent(details.from, details.message, theme, details.replyCommand, details.bodyText, !messageExpanded(details.message.id, options2.expanded), {
      session: (from) => references.current.sessionRef(from),
      message: (id) => references.current.messageRef(id),
      present: (text) => references.current.present(text),
      sessionName
    });
  });
  pi.on("tool_result", (event) => {
    if (event.toolName !== PARLEY_TOOL_NAME && event.toolName !== "contact_supervisor") {
      return;
    }
    if (!event.details || typeof event.details !== "object") {
      return;
    }
    const details = event.details;
    const receivedAnswer = typeof details.replyMessageId === "string" && details.replyMessageId.length > 0;
    if (details.error === true || details.delivered === false && !receivedAnswer) {
      return { isError: true };
    }
  });
  const childOrchestratorMetadata = readChildOrchestratorMetadata();
  const nativeSupervisorChannelAvailable = Boolean(process.env[SUBAGENT_SUPERVISOR_CHANNEL_DIR_ENV]?.trim());
  if (childOrchestratorMetadata && !nativeSupervisorChannelAvailable) {
    pi.registerTool(presentingTool(defineTool({
      name: "contact_supervisor",
      label: "Contact Supervisor",
      description: "Conversation with the supervisor who delegated this task. need_decision waits for a reply; interview_request waits for structured answers; progress_update returns a delivery receipt without waiting. Task completion has its own return channel.",
      promptSnippet: "Conversation with the delegating supervisor.",
      parameters: typebox_exports.Object({
        reason: StringEnum(["need_decision", "progress_update", "interview_request"], {
          description: "Contact reason: 'need_decision' waits for a reply; 'interview_request' sends structured questions and waits for a reply; 'progress_update' sends a non-blocking update"
        }),
        message: typebox_exports.Optional(typebox_exports.String({
          description: "Decision request, optional interview note, or meaningful progress update for the supervisor"
        })),
        interview: typebox_exports.Optional(typebox_exports.Object({
          title: typebox_exports.Optional(typebox_exports.String()),
          description: typebox_exports.Optional(typebox_exports.String()),
          questions: typebox_exports.Array(typebox_exports.Object({
            id: typebox_exports.String(),
            type: StringEnum(["single", "multi", "text", "image", "info"], {
              description: "Question type: single, multi, text, image, or info"
            }),
            question: typebox_exports.String(),
            options: typebox_exports.Optional(typebox_exports.Array(typebox_exports.Any())),
            context: typebox_exports.Optional(typebox_exports.String())
          }))
        }, { description: "Structured interview request for reason='interview_request'" }))
      }),
      async execute(_toolCallId, params, signal, _onUpdate, ctx) {
        const actionGeneration = runtimeGeneration;
        const historyWarnings = [];
        let lateHistorySkipped = false;
        const recordActionEntry = (type, data) => {
          if (runtimeGeneration !== actionGeneration) {
            lateHistorySkipped = true;
            return;
          }
          try {
            pi.appendEntry(type, data);
          } catch (error) {
            historyWarnings.push(failureText(error));
          }
        };
        const historyNote = () => `${historyWarnings.length ? `
Local history was not fully persisted; recovery may be incomplete. ${historyWarnings.join("; ")}` : ""}${lateHistorySkipped || runtimeGeneration !== actionGeneration ? "\nThis result arrived after the Pi session changed. What it reports happened, but it was not recorded in the current session's history." : ""}`;
        const reason = params.reason;
        if (reason !== "need_decision" && reason !== "progress_update" && reason !== "interview_request") {
          return {
            content: [{ type: "text", text: "Invalid reason. Use 'need_decision', 'interview_request', or 'progress_update'." }],
            details: { error: true }
          };
        }
        if ((reason === "need_decision" || reason === "progress_update") && typeof params.message !== "string") {
          return {
            content: [{ type: "text", text: `Missing 'message' parameter for reason '${reason}'.` }],
            details: { error: true }
          };
        }
        const interviewValidation = reason === "interview_request" ? validateSupervisorInterviewRequest(params.interview) : void 0;
        if (interviewValidation?.ok === false) {
          return {
            content: [{ type: "text", text: `Invalid interview request: ${interviewValidation.error}` }],
            details: { error: true }
          };
        }
        const supervisorInterview = interviewValidation?.ok === true ? interviewValidation.interview : void 0;
        let connectedClient;
        try {
          connectedClient = await ensureConnected("tool");
        } catch (error) {
          return {
            content: [{ type: "text", text: `Parley not connected: ${failureText(error)}` }],
            details: { error: true }
          };
        }
        syncPresenceIdentity(ctx.sessionManager.getSessionId());
        if (signal?.aborted) {
          return {
            content: [{ type: "text", text: "Cancelled" }],
            details: { error: true }
          };
        }
        const metadata = childOrchestratorMetadata;
        let resolvedSupervisor;
        try {
          resolvedSupervisor = await resolveSupervisorTarget(connectedClient, metadata);
        } catch (error) {
          return {
            content: [{ type: "text", text: `Failed to resolve supervisor target: ${failureText(error)}` }],
            details: { error: true }
          };
        }
        if (!resolvedSupervisor && reason !== "progress_update") {
          return {
            content: [{ type: "text", text: `Supervisor "${metadata.orchestratorTarget}" is not connected. No question was sent.` }],
            details: { error: true }
          };
        }
        const sendTo = resolvedSupervisor?.id ?? metadata.orchestratorTarget;
        const senderIdentity = currentSendIdentity(connectedClient);
        if (signal?.aborted) {
          return {
            content: [{ type: "text", text: "Cancelled" }],
            details: { error: true }
          };
        }
        if (sendTo === connectedClient.sessionId) {
          return {
            content: [{ type: "text", text: "Cannot message the current session" }],
            details: { error: true }
          };
        }
        if (reason === "progress_update") {
          const message = params.message;
          try {
            const result = await connectedClient.send(sendTo, {
              text: formatChildOrchestratorMessage("update", metadata, message),
              completesAsk: false
            });
            if (!result.delivered) {
              const errorText2 = result.reason ?? "Session may not exist or has disconnected.";
              return {
                content: [{ type: "text", text: describeDelivery(result, { kind: "Progress update", sender: senderIdentity, target: metadata.orchestratorTarget }) }],
                details: deliveryDetails(result)
              };
            }
            recordActionEntry("parley_sent", {
              to: metadata.orchestratorTarget,
              targetId: result.recipient?.id ?? sendTo,
              as: senderIdentity,
              message: { text: message, reason, completesAsk: false },
              messageId: result.id,
              timestamp: Date.now(),
              subagent: { runId: metadata.runId, agent: metadata.agent, index: metadata.index }
            });
            const awareness = result.peerCompaction ? `

${describeCompaction(metadata.orchestratorTarget, result.peerCompaction, sendTo)}` : "";
            connectedClient.acknowledgeSendContact(result);
            const conversationContext = runtimeGeneration === actionGeneration ? replyTracker.formatConversationContext() : "";
            return {
              // Conversation context is stateful (mention once, then count): render it exactly once.
              content: [{ type: "text", text: describeDelivery(result, { kind: "Progress update", sender: senderIdentity, target: metadata.orchestratorTarget }) + historyNote() + (conversationContext ? `

${conversationContext}` : "") }],
              details: deliveryDetails(result)
            };
          } catch (error) {
            return {
              content: [{ type: "text", text: `Failed to send progress update: ${failureText(error)}` }],
              details: { error: true }
            };
          }
        }
        if (replyWaiter) {
          return {
            content: [{ type: "text", text: "Already waiting for a reply" }],
            details: { error: true }
          };
        }
        let replyPromise = null;
        let deliveryState = "created";
        let questionId = null;
        let requestSendResult;
        const settleQuestion = (id, disposition) => {
          outstandingAsks.delete(id);
          recordActionEntry("parley_ask_settled", { messageId: id, reason: disposition, timestamp: Date.now() });
        };
        try {
          const prepared = resolvedSupervisor?.federation ? await connectedClient.prepareConversation(resolvedSupervisor) : void 0;
          if (prepared && (!getLiveContext(ctx, actionGeneration) || client !== connectedClient)) {
            throw new Error("Session ended during conversation preparation; no question was sent");
          }
          if (signal?.aborted) throw new Error("Cancelled before question dispatch");
          if (replyWaiter) throw new Error("Already waiting for a reply");
          questionId = prepared?.messageId ?? randomUUID6();
          const binding = {
            ...prepared?.recipient.endpointEpoch ? { endpointEpoch: prepared.recipient.endpointEpoch } : {},
            ...prepared?.recipient.federation?.originEpoch ? { originEpoch: prepared.recipient.federation.originEpoch } : {}
          };
          const requestText = reason === "interview_request" ? formatChildOrchestratorMessage("interview", metadata, formatSupervisorInterviewRequest(supervisorInterview, typeof params.message === "string" ? params.message : void 0)) : formatChildOrchestratorMessage("ask", metadata, params.message);
          const sentAt = Date.now();
          outstandingAsks.set(questionId, {
            to: sendTo,
            ...binding,
            targetDisplay: metadata.orchestratorTarget,
            preview: previewText(requestText, 120) ?? requestText,
            sentAt,
            message: { text: requestText }
          });
          recordActionEntry("parley_ask_pending", {
            messageId: questionId,
            to: sendTo,
            ...binding,
            targetDisplay: metadata.orchestratorTarget,
            message: { text: requestText },
            sentAt
          });
          replyPromise = waitForReply(sendTo, questionId, signal, () => connectedClient.cancelAsk(questionId), () => latestDeliveryState(questionId, deliveryState), binding.endpointEpoch, binding.originEpoch);
          replyPromise.catch(() => void 0);
          if (signal?.aborted) {
            rejectOwnedReplyWaiter(questionId, new Error("Cancelled"));
            try {
              await replyPromise;
            } catch {
            }
            settleQuestion(questionId, "not-delivered");
            return {
              content: [{ type: "text", text: "Cancelled" }],
              details: { error: true }
            };
          }
          const sendQuestion = (options2) => prepared ? connectedClient.sendToSession(prepared.recipient, options2) : connectedClient.send(sendTo, options2);
          const sendResult = await sendQuestion({
            messageId: questionId,
            text: requestText,
            expectsReply: true,
            completesAsk: false,
            senderWaitMode: "blocking",
            signal
          });
          requestSendResult = sendResult;
          deliveryState = sendResult.delivery;
          if (!sendResult.delivered) {
            if (sendResult.outcomeKnown) settleQuestion(questionId, "not-delivered");
            const errorText2 = sendResult.reason ?? "Session may not exist or has disconnected.";
            rejectOwnedReplyWaiter(questionId, new Error(`Message to "${metadata.orchestratorTarget}" was not delivered: ${errorText2}`));
            let answered = false;
            try {
              await replyPromise;
              answered = true;
            } catch {
            }
            if (sendResult.outcomeKnown || !answered) return {
              content: [{ type: "text", text: describeDelivery(sendResult, { kind: "Ask", sender: senderIdentity, target: metadata.orchestratorTarget }) }],
              details: { error: true, ...deliveryDetails(sendResult) }
            };
          }
          recordActionEntry("parley_sent", {
            to: metadata.orchestratorTarget,
            targetId: sendResult.recipient?.id ?? sendTo,
            as: senderIdentity,
            message: {
              text: reason === "interview_request" ? requestText : params.message,
              reason,
              ...reason === "interview_request" ? { interview: supervisorInterview } : {}
            },
            messageId: sendResult.id,
            timestamp: Date.now(),
            subagent: { runId: metadata.runId, agent: metadata.agent, index: metadata.index }
          });
          const requestCompaction = sendResult.peerCompaction;
          const replyMessage = await replyPromise;
          settleQuestion(questionId, "answer returned");
          const replyText = replyMessage.content.text;
          const replyAttachments = replyMessage.content.attachments?.length ? formatAttachments(replyMessage.content.attachments) : "";
          const structuredReply = reason === "interview_request" ? parseStructuredSupervisorReply(replyText, supervisorInterview) : void 0;
          recordActionEntry("parley_received", {
            from: metadata.orchestratorTarget,
            message: { text: replyText, attachments: replyMessage.content.attachments },
            messageId: replyMessage.id,
            timestamp: replyMessage.timestamp,
            subagent: { runId: metadata.runId, agent: metadata.agent, index: metadata.index }
          });
          const awareness = [
            requestCompaction ? describeCompaction(metadata.orchestratorTarget, requestCompaction, sendTo) : void 0,
            replyMessage.peerCompaction ? describeCompaction(metadata.orchestratorTarget, replyMessage.peerCompaction, sendTo) : void 0
          ].filter((notice) => Boolean(notice));
          connectedClient.acknowledgeSendContact(sendResult);
          acknowledgeInboundMessageContact(connectedClient, replyMessage);
          return {
            content: [{
              type: "text",
              text: `${!sendResult.outcomeKnown ? "Ask acceptance remains unknown; no replay was attempted. A correlated answer was received independently.\n\n" : ""}${awareness.length ? `${awareness.join("\n\n")}

` : ""}**Reply from supervisor:**
Question: ${messageRef(questionId)}
Reply: ${messageRef(replyMessage.id)}

${verbatim(`${replyText}${replyAttachments}`)}${historyNote()}${structuredReply?.error ? `

The structured answer could not be validated: ${structuredReply.error}` : ""}`
            }],
            details: {
              ...deliveryDetails(sendResult),
              replyMessageId: replyMessage.id,
              ...structuredReply ? structuredReply.value !== void 0 ? { structuredReply: structuredReply.value } : { error: true, structuredReplyParseError: structuredReply.error } : {},
              ...requestCompaction ? { requestPeerCompaction: requestCompaction } : {},
              ...replyMessage.peerCompaction ? { replyPeerCompaction: replyMessage.peerCompaction } : {}
            }
          };
        } catch (error) {
          rejectOwnedReplyWaiter(questionId, toError3(error));
          if (replyPromise) {
            try {
              await replyPromise;
            } catch {
            }
          }
          const requestAwareness = requestSendResult?.peerCompaction ? `

${describeCompaction(metadata.orchestratorTarget, requestSendResult.peerCompaction, sendTo)}` : "";
          if (requestSendResult) connectedClient.acknowledgeSendContact(requestSendResult);
          return {
            content: [{ type: "text", text: `Failed: ${failureText(error)}${requestAwareness}` }],
            details: {
              error: true,
              ...questionId ? { messageId: questionId, deliveryState: latestDeliveryState(questionId, deliveryState) } : {},
              ...requestSendResult?.peerCompaction ? { peerCompaction: requestSendResult.peerCompaction } : {}
            }
          };
        }
      },
      renderCall(args, theme) {
        const reason = typeof args.reason === "string" ? args.reason : "contact";
        const messagePreview = previewText(args.message, 96);
        const interview = args.interview && typeof args.interview === "object" ? args.interview : void 0;
        let text = theme.fg("toolTitle", theme.bold("contact_supervisor "));
        text += theme.fg(reason === "need_decision" ? "warning" : reason === "progress_update" ? "muted" : "accent", reason);
        if (typeof interview?.title === "string" && interview.title.trim()) {
          text += " " + theme.fg("accent", interview.title.trim());
        }
        if (messagePreview) {
          text += "\n  " + theme.fg("dim", messagePreview);
        }
        return new Text(text, 0, 0);
      },
      renderResult(result, { isPartial }, theme, context) {
        if (isPartial) {
          return new Text(theme.fg("warning", "Waiting for supervisor..."), 0, 0);
        }
        const details = result.details;
        const textContent = firstTextContent(result);
        const receivedAnswer = Boolean(details?.replyMessageId);
        const failed = Boolean(context.isError || details?.error === true || details?.delivered === false && !receivedAnswer);
        const parseWarning = typeof details?.structuredReplyParseError === "string";
        const uncertain = details?.outcomeKnown === false && !parseWarning;
        let text = uncertain ? theme.fg("warning", "? ") : failed ? theme.fg("error", "\u2717 ") : parseWarning ? theme.fg("warning", "\u26A0 ") : theme.fg("success", "\u2713 ");
        text += theme.fg(uncertain ? "warning" : failed ? "error" : "text", textContent);
        if (parseWarning) {
          text += "\n" + theme.fg("warning", `Structured reply parse issue: ${details.structuredReplyParseError}`);
        }
        return new Text(text, 0, 0);
      }
    })));
  }
  async function nameSuggestions(requested) {
    const live = client;
    if (!live?.isConnected()) return void 0;
    let sessions;
    try {
      sessions = await live.listSessions({ timeoutMs: 2e3 });
    } catch {
      return void 0;
    }
    const lines = [];
    for (const raw of requested) {
      const want = raw.trim().toLowerCase();
      if (sessions.some((session) => session.name?.toLowerCase() === want || references.current.sessionRef(session).toLowerCase() === want)) continue;
      const matches = sessions.filter((session) => {
        const last = session.name?.toLowerCase().split(":").at(-1)?.trim();
        const persona = session.description?.split(/\s+[—–-]\s+/)[0]?.trim().toLowerCase();
        return last === want || persona === want;
      });
      if (matches.length) {
        const refs = matches.map((session) => references.current.sessionRef(session)).join(", ");
        lines.push(`No session is named "${raw}". ${matches.length === 1 ? `Did you mean ${refs}?` : `Sessions it could mean: ${refs}.`}`);
      }
    }
    return lines.length ? lines.join("\n") : void 0;
  }
  function withoutPlaceholders(params) {
    const next = {};
    for (const [key, value] of Object.entries(params)) {
      if (typeof value === "string" && !value.trim()) continue;
      if (Array.isArray(value) && value.length === 0) continue;
      next[key] = value;
    }
    if (Array.isArray(next.targets)) {
      const targets = next.targets;
      const blank = (target2) => typeof target2 === "string" && !target2.trim();
      const to = typeof next.to === "string" ? next.to.trim().toLowerCase() : void 0;
      if (targets.every(blank) || to && targets.every((target2) => typeof target2 === "string" && target2.trim().toLowerCase() === to)) delete next.targets;
      else if (next.action !== "send") delete next.targets;
      else if (to && next.cwd === void 0) {
        next.targets = [next.to, ...targets.filter((target2) => !(typeof target2 === "string" && target2.trim().toLowerCase() === to))];
        delete next.to;
      }
    }
    return next;
  }
  function normalizeToolProfilePlaceholders(profile) {
    if (!profile) return void 0;
    const name = typeof profile.name === "string" && profile.name.trim() ? profile.name : void 0;
    const description = typeof profile.description === "string" && profile.description.trim() ? profile.description : void 0;
    if (name === void 0 && description === void 0) return void 0;
    return {
      ...name !== void 0 ? { name } : {},
      ...description !== void 0 ? { description } : {}
    };
  }
  function applySelfProfile(profile, ctx) {
    if (!profile) return void 0;
    persistProfileOwnershipRevocation();
    const normalized = normalizeSelfProfileUpdate(profile);
    if (!normalized.ok) return normalized.error;
    if (normalized.profile.name === void 0 && normalized.profile.description === void 0) {
      return "profile requires a name, a description, or both.";
    }
    const currentName = pi.getSessionName()?.trim();
    const requestedName = normalized.profile.name;
    const nameChanges = requestedName !== void 0 && currentName !== requestedName;
    if (nameChanges && requestedName !== void 0) {
      try {
        resolveEmbeddedPresenceName(configuration, requestedName, SESSION_PRESENCE_NAME_CONTEXT);
      } catch (error) {
        return `Unable to resolve profile name: ${failureText(error)}`;
      }
    }
    if (requestedName !== void 0) {
      if (currentName && nameChanges && currentName !== profileManagedName) {
        return `profile.name cannot replace the explicit session name "${currentName}". Omit it and update only the description.`;
      }
    }
    const nextDescription = normalized.profile.description === void 0 ? currentSessionDescription : normalized.profile.description ?? void 0;
    const descriptionChanges = nextDescription !== currentSessionDescription;
    if (!nameChanges && !descriptionChanges) {
      syncPresenceIdentity(ctx.sessionManager.getSessionId());
      return void 0;
    }
    const nextManagedName = nameChanges ? requestedName : profileOwnershipRevocationPending ? null : profileManagedName;
    const profileState = {
      ...nextManagedName === null ? { managedName: null } : nextManagedName ? { managedName: nextManagedName } : {},
      description: nextDescription ?? null,
      timestamp: Date.now()
    };
    if (nameChanges && requestedName) {
      const updateId = randomUUID6();
      try {
        pi.appendEntry("parley_profile_pending", {
          ...profileState,
          updateId,
          requiredName: requestedName
        });
      } catch (error) {
        return `Unable to persist self profile; no changes were applied: ${failureText(error)}`;
      }
      profileNameMutationTarget = requestedName;
      try {
        pi.setSessionName(requestedName);
      } catch (error) {
        profileNameMutationTarget = void 0;
        try {
          pi.appendEntry("parley_profile_abandoned", { updateId, timestamp: Date.now() });
        } catch {
        }
        return `Unable to update profile name: ${failureText(error)}`;
      }
      profileNameMutationTarget = void 0;
      observedSessionName = requestedName;
      profileManagedName = requestedName;
      currentSessionDescription = nextDescription;
      try {
        pi.appendEntry("parley_profile_updated", { ...profileState, updateId });
        profileOwnershipRevocationPending = false;
      } catch {
      }
    } else {
      try {
        pi.appendEntry("parley_profile_updated", profileState);
        profileOwnershipRevocationPending = false;
      } catch (error) {
        return `Unable to persist self profile; no changes were applied: ${failureText(error)}`;
      }
      currentSessionDescription = nextDescription;
    }
    syncPresenceIdentity(ctx.sessionManager.getSessionId());
    return void 0;
  }
  function currentSelfProfile() {
    const requestedName = pi.getSessionName()?.trim() || observedSessionName;
    const fallbackId = currentParleySessionId ?? currentSessionId;
    void fallbackId;
    const name = requestedName || "unnamed";
    const self = client?.getSelfSession();
    const effectiveName = self?.runtimeFallbackAlias ? void 0 : self?.name;
    return {
      name,
      ...currentSessionDescription ? { description: currentSessionDescription } : {},
      ...effectiveName ? { parleyName: effectiveName } : {},
      descriptionPublished: currentSessionDescription === void 0 || client?.supportsFeature(SESSION_PROFILE_FEATURE) === true
    };
  }
  function attachSelfProfile(result, profileUpdated = false) {
    const profile = currentSelfProfile();
    const existingDetails = typeof result.details === "object" && result.details !== null && !Array.isArray(result.details) ? result.details : {};
    const identityAlreadyShown = !profileUpdated && existingDetails.delivered === true && existingDetails.senderIdentity === profile.name && profile.parleyName === profile.name && profile.descriptionPublished;
    const parleyProjection = profile.parleyName && profile.parleyName !== profile.name ? ` (parley: ${profile.parleyName})` : profile.parleyName ? "" : " (parley name awaiting broker confirmation)";
    const publication = profile.description && !profile.descriptionPublished ? " [description local only: broker does not support profiles]" : "";
    if (!identityAlreadyShown) result.content.push({
      type: "text",
      text: `Self profile: ${profile.name}${parleyProjection}${profile.description ? ` \u2014 ${profile.description}` : ""}${publication}`
    });
    result.details = { ...existingDetails, selfProfile: profile };
    return result;
  }
  function counterpartRef(id, display) {
    const pinned = display ? references.current.resolveSession(display) : void 0;
    if (references.current.isKnownSession(id) || !display || display === id || pinned && pinned.id !== id) {
      return references.current.sessionRef(id);
    }
    return references.current.sessionRef({ id, name: display });
  }
  const quietOutstandingAsks = /* @__PURE__ */ new Set();
  function formatOutstandingAsks(limit = 3, options2 = {}) {
    const entries = [...outstandingAsks.entries()].sort((a, b) => a[1].sentAt - b[1].sentAt);
    if (!entries.length) return "";
    const now = Date.now();
    const lines = [];
    let quieted = 0;
    let omitted = 0;
    for (const [id, ask] of entries) {
      const ageMs = Math.max(0, now - ask.sentAt);
      const elapsed = ageMs > askTimeoutMs;
      if (!options2.complete && elapsed && quietOutstandingAsks.has(id)) {
        quieted += 1;
        continue;
      }
      if (lines.length >= limit) {
        omitted += 1;
        continue;
      }
      if (!options2.complete && elapsed) quietOutstandingAsks.add(id);
      const age = ageMs < 6e4 ? `${Math.round(ageMs / 1e3)}s` : `${Math.round(ageMs / 6e4)}m`;
      lines.push(`- ${references.current.messageRef(id)} to ${counterpartRef(ask.to, ask.targetDisplay)} \xB7 ${age}${elapsed ? "; local wait window elapsed" : ""} \xB7 ${verbatim(ask.preview)}`);
    }
    if (omitted > 0) lines.push(`\u2026 ${omitted} more outstanding questions.`);
    if (quieted > 0) lines.push(`\u2026 ${quieted} earlier question(s) whose wait window elapsed; status lists them.`);
    return `Outstanding asks (${entries.length}, local tracking):
${lines.join("\n")}`;
  }
  function resolveModelReferences(params) {
    const next = { ...params };
    const pinnedIds = /* @__PURE__ */ new Set();
    const displayFor = /* @__PURE__ */ new Map();
    const pinSession = (text) => {
      const resolved = references.current.resolveSession(text);
      if (!resolved) return text;
      pinnedIds.add(resolved.id);
      displayFor.set(resolved.id, text.trim());
      return resolved.id;
    };
    let pinnedTarget;
    if (typeof next.to === "string" && next.to.trim()) {
      const id = pinSession(next.to);
      if (pinnedIds.has(id)) pinnedTarget = { id, ref: next.to.trim() };
      next.to = id;
    }
    if (Array.isArray(next.targets)) {
      next.targets = next.targets.map((target2) => typeof target2 === "string" && target2.trim() ? pinSession(target2) : target2);
    }
    for (const key of ["replyTo", "messageId", "supersedes", "retryOf"]) {
      const value = next[key];
      if (typeof value !== "string" || !value.trim()) continue;
      const resolved = references.current.resolveMessage(value);
      if (resolved?.kind === "unknown") {
        return { error: `${key} ${resolved.ref} is not a message this session knows. Message references such as #12 appear in receipts, incoming messages, pending, and status.` };
      }
      if (resolved) next[key] = resolved.id;
    }
    return { params: next, pinnedIds, displayFor, ...pinnedTarget ? { pinnedTarget } : {} };
  }
  function successorHintFor(params, resolved, details) {
    if (!["send", "ask", "reply"].includes(params.action ?? "")) return void 0;
    const outcome = details;
    const addressedAs = (id) => resolved.displayFor.get(id) ?? references.current.sessionRef(id);
    const rebound = (addressedId, actualId, projectLaunch) => {
      const addressed2 = addressedAs(addressedId);
      const actual = references.current.sessionRef(actualId);
      if (projectLaunch) return `${addressed2} was not connected in that directory; this went to ${actual}, the session observed after the project launch.`;
      const known = references.current.isKnownSession(actualId) ? actual : "another session";
      return `${addressed2} itself was not reached: the broker's offline-mail rule delivered this to ${known}, a different session using that name in the same directory. Address ${actual} directly if that is who you mean.`;
    };
    if (Array.isArray(outcome?.outcomes)) {
      const lines = outcome.outcomes.filter((item) => item.delivered && item.requested && resolved.pinnedIds.has(item.requested) && item.targetId && item.targetId !== item.requested).map((item) => `- ${rebound(item.requested, item.targetId, false)}`);
      return lines.length ? lines.join("\n") : void 0;
    }
    const counterpart = resolved.pinnedTarget?.id ?? (params.replyTo ? replyTracker.getMessage(params.replyTo)?.from.id : void 0);
    if (!counterpart) return void 0;
    const addressed = addressedAs(counterpart);
    const recipientId = outcome?.recipient?.id;
    if (outcome?.delivered && recipientId && recipientId !== counterpart) return rebound(counterpart, recipientId, outcome.openedProjectPane === true);
    const uncertain = outcome?.outcomeKnown === false || outcome?.delivery === "unknown";
    if (uncertain) {
      return references.current.isReachable(counterpart) === false ? `${addressed} was not connected, and the recipient of this message is unknown; the broker may have delivered it elsewhere or not at all. Sending again could repeat it.` : void 0;
    }
    const successor = references.current.successorHint(counterpart, addressed.replace(/~\d+$/, ""));
    if (!successor) return void 0;
    const queued = outcome?.delivery === "queued" && outcome.messageId ? ` ${references.current.messageRef(outcome.messageId)} is queued for ${addressed}; if you meant ${successor}, cancel ${references.current.messageRef(outcome.messageId)} before sending to ${successor}.` : "";
    return `${addressed} is not currently reachable. A different session, ${successor}, now uses that name; address ${successor} explicitly if you mean it. Parley did not redirect this.${queued}`;
  }
  pi.registerTool(defineTool({
    name: PARLEY_TOOL_NAME,
    label: "Parley",
    description: `Conversation with other Pi sessions visible in the current scope.

\u2022 list / list-cwd: Connected peers, their identity, focus, location, and activity.
\u2022 send: A notification to one peer or an explicit group; it does not answer a pending question.
\u2022 ask: A question whose answer returns here. Waits by default; blocking: false delivers the answer later in the conversation.
\u2022 reply: Responds to a message, answering its question when applicable.
\u2022 pending / read: Unanswered questions, or a retained message's full content.
\u2022 status: Connection state and outstanding questions.
\u2022 cancel: Removes offline mail or communicates withdrawal; work already done is unchanged.
\u2022 broadcast: Independent messages to visible local peers, not remote peers.
\u2022 rename: Changes this session's name.
\u2022 label: Gives a message a local name, like review or notes, usable wherever a message reference is.
\u2022 link: Links this computer with others over SSH, so their sessions appear here. host adds one, as you'd give it to ssh, and links it now; recheck checks again (host, or everything); discover: true links computers this one has ssh connections to automatically. With none of these, shows the links.
\u2022 unlink: Unlinks one computer and forgets it.

Sessions and messages are named the way people would name them: a session by its name (name~2 when a different session later took a name you've already seen), a message by its number here, like #12. Use references as shown; parley keeps each one attached to what it first named. Receipts include sender and recipient, the message reference, delivery state, and nearby conversation context. Endpoint acceptance is not an acknowledgement from the colleague.`,
    promptSnippet: "Communicate with other Pi sessions.",
    promptGuidelines: [
      "Parley messages can wake colleagues; broadcasts reach every visible local peer."
    ],
    parameters: typebox_exports.Object({
      action: StringEnum(["list", "list-cwd", "send", "broadcast", "ask", "reply", "pending", "status", "cancel", "rename", "read", "label", "link", "unlink"], {
        description: "Parley operation."
      }),
      profile: typebox_exports.Optional(typebox_exports.Object({
        name: typebox_exports.Optional(typebox_exports.String({
          description: "Name for an unnamed or profile-managed session; does not replace a user-set name."
        })),
        description: typebox_exports.Optional(typebox_exports.Union([
          typebox_exports.String(),
          typebox_exports.Null()
        ], {
          description: "Current focus (5-9 words). A new description replaces the old one. Display metadata, not routing identity."
        }))
      }, {
        description: "Optional self-profile update."
      })),
      to: typebox_exports.Optional(typebox_exports.String({
        description: "One session, by the reference shown in list, receipts, or messages. For send/ask with cwd, omit to target the sole live session in that cwd or the newly opened project-pane session. For 'reply', narrows which sender you are answering."
      })),
      targets: typebox_exports.Optional(typebox_exports.Array(typebox_exports.String(), {
        maxItems: MAX_EXPLICIT_SEND_TARGETS,
        description: "For 'send', several session references. Each receives an independent message and outcome; with 'to' as well, everyone named is included. Not combined with cwd targeting, replyTo, supersedes, or retryOf. Ignored by other actions."
      })),
      message: typebox_exports.Optional(typebox_exports.String({
        description: "Message to send (for 'send', 'broadcast', 'ask', or 'reply' action)"
      })),
      attachments: typebox_exports.Optional(typebox_exports.Array(typebox_exports.Object({
        type: StringEnum(["file", "snippet", "context"]),
        name: typebox_exports.String(),
        content: typebox_exports.String(),
        language: typebox_exports.Optional(typebox_exports.String())
      }), { description: "Inline text snapshots; no files are created in the receiving workspace." })),
      replyTo: typebox_exports.Optional(typebox_exports.String({
        description: "Message reference (like #12) to reply to, for threading or answering an ask."
      })),
      messageId: typebox_exports.Optional(typebox_exports.String({
        description: "Message reference (like #12) for read, cancel, or label."
      })),
      label: typebox_exports.Optional(typebox_exports.String({
        description: "A one-word local name for a message, like review or notes. With 'label', names the existing message in messageId; with send or ask, names the new message. Usable anywhere a message reference is."
      })),
      supersedes: typebox_exports.Optional(typebox_exports.String({
        description: "Reference of your earlier send/ask that this one replaces. Only works for the same sender and receiver."
      })),
      retryOf: typebox_exports.Optional(typebox_exports.String({
        description: "Reference of an earlier send/ask this is a user-authored retry of. A retry is always a new message with its own reference."
      })),
      cwd: typebox_exports.Optional(typebox_exports.String({
        description: "Working directory filter for 'list-cwd'. For send/ask, scopes target lookup to that directory; omit 'to' to target the sole live peer there. Absolute, or relative to the current session's cwd; '.' means the current cwd."
      })),
      blocking: typebox_exports.Optional(typebox_exports.Boolean({
        description: "For 'ask', true waits for the answer in this tool call (default); false returns immediately and delivers the answer as an incoming message."
      })),
      openProjectPaneIfMissing: typebox_exports.Optional(typebox_exports.Boolean({
        description: "For send/ask with cwd, launch Pi in that project through a registered generic project launcher when no matching live session exists."
      })),
      focus: typebox_exports.Optional(typebox_exports.Boolean({
        description: "For openProjectPaneIfMissing, focus the new terminal when the launcher supports it. Defaults to true."
      })),
      name: typebox_exports.Optional(typebox_exports.String({
        description: "Canonical self-name for 'rename'."
      })),
      host: typebox_exports.Optional(typebox_exports.String({
        description: "For link/unlink: another computer, as you'd give it to ssh (an alias, user@host, optionally -p/-l/-i/-J/-F/-o)."
      })),
      recheck: typebox_exports.Optional(typebox_exports.Boolean({
        description: "For link: check again now."
      })),
      discover: typebox_exports.Optional(typebox_exports.Boolean({
        description: "For link: turn automatic linking of computers this one has ssh connections to on or off."
      }))
    }),
    async execute(_toolCallId, modelParams, _signal, _onUpdate, ctx) {
      const actionGeneration = runtimeGeneration;
      const book = references.current;
      const callShortNameNotes = [];
      return shortNameNotes.run(callShortNameNotes, () => callBook.run(book, async () => {
        const sentTo = modelParams.to;
        const sentTargets = modelParams.targets;
        modelParams = withoutPlaceholders(modelParams);
        const ignoredTargets = modelParams.action !== "send" && Array.isArray(sentTargets) && modelParams.targets === void 0 && sentTargets.some((target2) => typeof target2 === "string" && target2.trim() && target2.trim().toLowerCase() !== (typeof sentTo === "string" ? sentTo.trim().toLowerCase() : "")) ? sentTargets : void 0;
        const resolvedReferences = resolveModelReferences(modelParams);
        if ("error" in resolvedReferences) {
          return attachSelfProfile(presentToolResult({ content: [{ type: "text", text: resolvedReferences.error }], details: { error: true } }, { book }));
        }
        const params = resolvedReferences.params;
        const pinned = (value) => typeof value === "string" && resolvedReferences.pinnedIds.has(value);
        const knownTarget = (value) => value !== void 0 && (pinned(value) || isCanonicalIdentity(value) || references.current.isKnownSession(value)) ? value : void 0;
        const display = (value) => value === void 0 ? void 0 : resolvedReferences.displayFor.get(value) ?? value;
        const routedSessions = new Set(resolvedReferences.pinnedIds);
        const requestedLabel = typeof params.label === "string" && params.label.trim() ? params.label : void 0;
        let labelReservation;
        const labelSkippedForBatch = Boolean(requestedLabel && params.action === "send" && params.targets?.some((target2) => target2.trim()));
        if (requestedLabel && !labelSkippedForBatch && (params.action === "send" || params.action === "ask")) {
          labelReservation = book.reserveLabel(requestedLabel);
          if (!labelReservation.ok) {
            return attachSelfProfile(presentToolResult({ content: [{ type: "text", text: `Label not available: ${labelReservation.reason} Nothing was sent.` }], details: { error: true } }, { book }));
          }
        }
        let labelSettled = false;
        const settleLabel = (outcome) => {
          if (!labelReservation?.ok || labelSettled) return;
          labelSettled = true;
          const id = typeof outcome?.messageId === "string" ? outcome.messageId : typeof outcome?.id === "string" ? outcome.id : void 0;
          const knownNotCreated = id === void 0 || outcome.delivered === false && outcome.outcomeKnown === true || outcome.deliveryState === "failed";
          if (knownNotCreated) labelReservation.release();
          else labelReservation.bind(id);
        };
        let sendIdentity;
        const historyWarnings = [];
        let lateHistorySkipped = false;
        const recordActionEntry = (type, data) => {
          if (runtimeGeneration !== actionGeneration) {
            lateHistorySkipped = true;
            return;
          }
          try {
            pi.appendEntry(type, data);
          } catch (error) {
            historyWarnings.push(`${type}: ${failureText(error)}`);
          }
        };
        const ownsSession = () => runtimeGeneration === actionGeneration;
        const settleSupersededQuestion = () => {
          if (!params.supersedes || !ownsSession()) return;
          outstandingAsks.delete(params.supersedes);
          rejectOwnedReplyWaiter(params.supersedes, new Error(`Request ${messageRef(params.supersedes)} was superseded.`));
          recordActionEntry("parley_ask_settled", { messageId: params.supersedes, reason: "superseded", timestamp: Date.now() });
        };
        const toolResult = await (async () => {
          const profile = normalizeToolProfilePlaceholders(params.profile);
          const profileError = applySelfProfile(profile, ctx);
          if (profileError) {
            return {
              content: [{ type: "text", text: `Action ${params.action} was not performed. ${profileError}` }],
              details: { error: true }
            };
          }
          if (params.action === "link" || params.action === "unlink") {
            const outcome = await links.tool(params);
            return {
              content: [{ type: "text", text: outcome.text }],
              details: { ...outcome.ok ? {} : { error: true }, ...outcome.status ? { links: outcome.status } : {}, ...outcome.host ? { host: outcome.host } : {} }
            };
          }
          let connectedClient;
          try {
            connectedClient = await ensureConnected("tool");
          } catch (error) {
            return {
              content: [{ type: "text", text: `Parley not connected: ${failureText(error)}` }],
              details: { error: true }
            };
          }
          let requestedPresenceName;
          try {
            syncPresenceIdentity(ctx.sessionManager.getSessionId());
            requestedPresenceName = buildPresenceIdentity(
              pi,
              currentParleySessionId ?? ctx.sessionManager.getSessionId(),
              configuration
            ).name;
          } catch (error) {
            return {
              content: [{ type: "text", text: `Action ${params.action} was not performed. Presence name resolution failed: ${failureText(error)}` }],
              details: { error: true }
            };
          }
          const selfProjection = connectedClient.getSelfSession();
          const projectionMayChange = selfProjection === void 0 || selfProjection.name !== requestedPresenceName;
          if (profile?.name !== void 0 || projectionMayChange) {
            try {
              await connectedClient.listSessions({ timeoutMs: 1e3 });
            } catch {
            }
          }
          const captureSendIdentity = () => {
            sendIdentity = currentSendIdentity(connectedClient);
            _onUpdate?.({ content: [{ type: "text", text: `Communicating as ${sendIdentity}\u2026` }], details: { senderIdentity: sendIdentity } });
          };
          if (["send", "ask", "reply", "broadcast"].includes(params.action)) captureSendIdentity();
          const {
            action,
            to,
            message,
            attachments,
            replyTo,
            messageId,
            supersedes,
            retryOf,
            cwd,
            openProjectPaneIfMissing,
            focus,
            blocking,
            name
          } = params;
          const allBlankTargets = params.targets?.every((target2) => !target2.trim()) ?? false;
          const duplicatedSingularTarget = params.targets !== void 0 && to !== void 0 && params.targets.length === 1 && params.targets[0]?.trim() === to.trim();
          const targets = params.targets === void 0 || allBlankTargets || duplicatedSingularTarget ? void 0 : params.targets;
          if (action === "advertise") {
            return {
              content: [{ type: "text", text: "Action advertise is retired. Subagents are scoped to their supervisor tree." }],
              details: { error: true }
            };
          }
          if (messageId && action !== "cancel" && action !== "read" && action !== "label") {
            return {
              content: [{ type: "text", text: "messageId identifies a retained message for read or cancel; sends and asks create a new message." }],
              details: { error: true }
            };
          }
          switch (action) {
            case "list": {
              try {
                const mySessionId = connectedClient.sessionId;
                const sessions = await connectedClient.listSessions();
                const currentSession = sessions.find((s) => s.id === mySessionId);
                const otherSessions = sessions.filter((s) => s.id !== mySessionId);
                if (!currentSession) {
                  return {
                    content: [{ type: "text", text: "Current session is missing from parley session list." }],
                    details: { error: true }
                  };
                }
                const refs = rosterReferences(sessions);
                const currentSection = `**Current session:**
${formatSessionListRow(currentSession, currentSession.cwd, true, refs.get(currentSession.id), originOf(currentSession))}`;
                const otherSection = otherSessions.length === 0 ? "**Other sessions:**\nNo other sessions connected." : `**Other sessions:**
${otherSessions.map((session) => formatSessionListRow(session, currentSession.cwd, false, refs.get(session.id), originOf(session))).join("\n")}`;
                return {
                  content: [{ type: "text", text: `${currentSection}

${otherSection}` }],
                  details: {}
                };
              } catch (error) {
                return {
                  content: [{ type: "text", text: `Failed to list sessions: ${failureText(error)}` }],
                  details: { error: true }
                };
              }
            }
            case "list-cwd": {
              try {
                const mySessionId = connectedClient.sessionId;
                const sessions = await connectedClient.listSessions();
                const currentSession = sessions.find((s) => s.id === mySessionId);
                if (!currentSession) {
                  return {
                    content: [{ type: "text", text: "Current session is missing from parley session list." }],
                    details: { error: true }
                  };
                }
                const filterCwd = cwd && cwd !== "." ? resolvePath(currentSession.cwd, cwd) : currentSession.cwd;
                const otherSessions = sessions.filter(
                  (s) => s.id !== mySessionId && sameCwd(s.cwd, filterCwd)
                );
                let emptyNote = "No other sessions in this directory.";
                if (otherSessions.length === 0 && !sameCwd(filterCwd, currentSession.cwd)) {
                  const here = sessions.filter(
                    (s) => s.id !== mySessionId && sameCwd(s.cwd, currentSession.cwd)
                  ).length;
                  if (here > 0) {
                    emptyNote += ` Your session's cwd is ${currentSession.cwd} (${here} peer${here === 1 ? "" : "s"} there) \u2014 call list-cwd without a cwd argument to list them.`;
                  }
                }
                const refs = rosterReferences(sessions);
                const currentSection = `**Current session:**
${formatSessionListRow(currentSession, currentSession.cwd, true, refs.get(currentSession.id), originOf(currentSession))}`;
                const otherSection = otherSessions.length === 0 ? `**Other sessions (cwd: ${filterCwd}):**
${emptyNote}` : `**Other sessions (cwd: ${filterCwd}):**
${otherSessions.map((session) => formatSessionListRow(session, currentSession.cwd, false, refs.get(session.id), originOf(session))).join("\n")}`;
                return {
                  content: [{ type: "text", text: `${currentSection}

${otherSection}` }],
                  details: {}
                };
              } catch (error) {
                return {
                  content: [{ type: "text", text: `Failed to list sessions: ${failureText(error)}` }],
                  details: { error: true }
                };
              }
            }
            case "cancel": {
              if (!messageId) {
                return {
                  content: [{ type: "text", text: "Missing 'messageId' parameter" }],
                  details: { error: true }
                };
              }
              try {
                const result = await connectedClient.cancelMessage(messageId);
                if (result.delivered && result.outcomeKnown && ownsSession()) {
                  outstandingAsks.delete(messageId);
                  rejectOwnedReplyWaiter(messageId, new Error(`Request ${messageRef(messageId)} was withdrawn.`));
                  recordActionEntry("parley_ask_settled", { messageId, reason: "withdrawn", timestamp: Date.now() });
                }
                return {
                  content: [{ type: "text", text: describeCancellation(result) }],
                  details: deliveryDetails(result)
                };
              } catch (error) {
                return {
                  content: [{ type: "text", text: `Failed to cancel message: ${failureText(error)}` }],
                  details: { error: true, messageId }
                };
              }
            }
            case "rename": {
              const requestedName = name?.trim();
              if (!requestedName) {
                return {
                  content: [{ type: "text", text: "rename requires a non-empty 'name'." }],
                  details: { error: true }
                };
              }
              if (!isValidSessionName(requestedName)) {
                return {
                  content: [{ type: "text", text: `"${requestedName}" cannot be used as a session name.` }],
                  details: { error: true }
                };
              }
              try {
                resolveEmbeddedPresenceName(configuration, requestedName, SESSION_PRESENCE_NAME_CONTEXT);
                pi.setSessionName(requestedName);
                syncPresenceIdentity(ctx.sessionManager.getSessionId());
              } catch (error) {
                return {
                  content: [{ type: "text", text: `Unable to set the session name: ${failureText(error)}` }],
                  details: { error: true }
                };
              }
              let publication = "";
              try {
                await connectedClient.listSessions({ timeoutMs: 1e3 });
              } catch {
                publication = " Broker publication has not been confirmed.";
              }
              return {
                content: [{ type: "text", text: `Session name set to "${requestedName}".${publication}` }],
                details: {}
              };
            }
            case "broadcast": {
              if (!message) {
                return {
                  content: [{ type: "text", text: "Missing 'message' parameter" }],
                  details: { error: true }
                };
              }
              if (to || targets || cwd || openProjectPaneIfMissing) {
                return {
                  content: [{ type: "text", text: "broadcast does not accept 'to', 'targets', 'cwd', or openProjectPaneIfMissing; it uses the current visible live-session roster." }],
                  details: { error: true }
                };
              }
              if (replyTo || supersedes || retryOf) {
                return {
                  content: [{ type: "text", text: "broadcast cannot use replyTo, supersedes, or retryOf because each recipient receives an independent message." }],
                  details: { error: true }
                };
              }
              try {
                const sessions = await connectedClient.listSessions();
                const currentSessionId2 = connectedClient.sessionId;
                const refs = rosterReferences(sessions);
                const recipients = sessions.filter((session) => session.id !== currentSessionId2 && !session.federation).sort((left, right) => left.id.localeCompare(right.id)).map((session) => ({
                  requested: session.id,
                  label: refs.get(session.id),
                  session
                }));
                if (recipients.length === 0) {
                  return {
                    content: [{ type: "text", text: "No other visible local sessions are connected; nothing was broadcast." }],
                    details: { error: true, broadcast: true, recipientCount: 0 }
                  };
                }
                const attachmentText = attachments?.length ? formatAttachments(attachments) : "";
                if (config.confirmSend && ctx.hasUI) {
                  const confirmed = await ctx.ui.confirm(
                    "Broadcast message",
                    `Broadcast to ${recipients.length} visible live session${recipients.length === 1 ? "" : "s"}:

${message}${attachmentText}`
                  );
                  if (!confirmed) {
                    return {
                      content: [{ type: "text", text: "Broadcast cancelled by user" }],
                      details: {}
                    };
                  }
                }
                const batchId = randomUUID6();
                const outcomes = await sendBatchMessages(connectedClient, recipients, {
                  message,
                  attachments,
                  batchId,
                  broadcast: true,
                  signal: _signal,
                  record: recordActionEntry
                });
                return batchSendToolResult({
                  message: messageRef,
                  compaction: describeCompaction,
                  batchId,
                  outcomes,
                  requestedTargetCount: recipients.length,
                  duplicateCount: 0,
                  broadcast: true,
                  sender: sendIdentity,
                  excludedRemoteCount: sessions.filter((session) => Boolean(session.federation)).length
                });
              } catch (error) {
                return {
                  content: [{ type: "text", text: `Failed to broadcast: ${failureText(error)}` }],
                  details: { error: true }
                };
              }
            }
            case "send": {
              if (to && targets) {
                return {
                  content: [{ type: "text", text: `Nothing was sent. This call had to: ${JSON.stringify(sentTo)} and targets: ${JSON.stringify(sentTargets)}. For one colleague, use to alone; for several, targets alone (leave the other out or empty).` }],
                  details: { error: true }
                };
              }
              if (targets) {
                if (!message) {
                  return {
                    content: [{ type: "text", text: "Missing 'message' parameter" }],
                    details: { error: true }
                  };
                }
                if (cwd || openProjectPaneIfMissing) {
                  return {
                    content: [{ type: "text", text: "Multi-target send does not accept cwd or openProjectPaneIfMissing; identify each live session explicitly." }],
                    details: { error: true }
                  };
                }
                if (replyTo || supersedes || retryOf) {
                  return {
                    content: [{ type: "text", text: "Multi-target send cannot use replyTo, supersedes, or retryOf because each recipient receives an independent message." }],
                    details: { error: true }
                  };
                }
                if (targets.length === 0 || targets.length > MAX_EXPLICIT_SEND_TARGETS || targets.some((target2) => !target2.trim())) {
                  return {
                    content: [{ type: "text", text: `targets must contain 1-${MAX_EXPLICIT_SEND_TARGETS} non-empty session names or IDs.` }],
                    details: { error: true }
                  };
                }
                try {
                  const requestedTargets = targets.map((target2) => target2.trim());
                  const sessions = await connectedClient.listSessions();
                  const recipients = [];
                  const seenSessionIds = /* @__PURE__ */ new Set();
                  const seenUnresolved = /* @__PURE__ */ new Set();
                  let duplicateCount = 0;
                  for (const requested of requestedTargets) {
                    try {
                      const session = resolveSessionFromRoster(sessions, requested, pinned(requested));
                      if (session) {
                        if (seenSessionIds.has(session.id)) {
                          duplicateCount += 1;
                          continue;
                        }
                        seenSessionIds.add(session.id);
                        recipients.push({
                          requested,
                          label: display(requested),
                          session,
                          ...session.id === connectedClient.sessionId ? { resolutionError: "Cannot message the current session", resolutionCode: "E_SELF_TARGET" } : {}
                        });
                        continue;
                      }
                      if (seenUnresolved.has(requested)) {
                        duplicateCount += 1;
                        continue;
                      }
                      seenUnresolved.add(requested);
                      recipients.push({ requested, label: display(requested) });
                    } catch (error) {
                      recipients.push({
                        requested,
                        label: display(requested),
                        resolutionError: failureText(error),
                        resolutionCode: "E_TARGET_RESOLUTION"
                      });
                    }
                  }
                  let confirmedSnapshot = false;
                  if (config.confirmSend && ctx.hasUI) {
                    const attachmentText = attachments?.length ? formatAttachments(attachments) : "";
                    const recipientLines = recipients.map((recipient) => {
                      if (recipient.session) {
                        const name2 = recipient.session.name ? `${recipient.session.name} ` : "";
                        return `- ${recipient.label} \u2192 ${name2}(${recipient.session.id})`;
                      }
                      if (recipient.resolutionError) {
                        return `- ${recipient.requested} \u2192 will fail: ${recipient.resolutionError}`;
                      }
                      return `- ${recipient.requested} \u2192 not currently live; ordinary offline delivery may apply`;
                    });
                    if (duplicateCount > 0) {
                      recipientLines.push(`- ${duplicateCount} duplicate target${duplicateCount === 1 ? "" : "s"} omitted`);
                    }
                    const confirmed = await ctx.ui.confirm(
                      "Send message",
                      `Send independently to this resolved recipient snapshot:
${recipientLines.join("\n")}

${message}${attachmentText}`
                    );
                    if (!confirmed) {
                      return {
                        content: [{ type: "text", text: "Message cancelled by user" }],
                        details: {}
                      };
                    }
                    confirmedSnapshot = true;
                  }
                  const batchId = randomUUID6();
                  const outcomes = await sendBatchMessages(connectedClient, recipients, {
                    message,
                    attachments,
                    batchId,
                    broadcast: false,
                    signal: _signal,
                    record: recordActionEntry,
                    isExactIdentity: (requested) => pinned(requested),
                    allowRosterMailboxFallback: !confirmedSnapshot
                  });
                  return batchSendToolResult({
                    message: messageRef,
                    compaction: describeCompaction,
                    batchId,
                    outcomes,
                    requestedTargetCount: requestedTargets.length,
                    duplicateCount,
                    broadcast: false,
                    sender: sendIdentity
                  });
                } catch (error) {
                  return {
                    content: [{ type: "text", text: `Failed to send: ${failureText(error)}` }],
                    details: { error: true }
                  };
                }
              }
              if (!to && !cwd || !message) {
                return {
                  content: [{ type: "text", text: "Missing 'to' or 'cwd', or missing 'message' parameter" }],
                  details: { error: true }
                };
              }
              try {
                if (openProjectPaneIfMissing && !cwd) {
                  return {
                    content: [{ type: "text", text: "openProjectPaneIfMissing requires a target cwd." }],
                    details: { error: true }
                  };
                }
                const confirmSend = !replyTo && config.confirmSend && ctx.hasUI;
                const attachmentText = attachments?.length ? formatAttachments(attachments) : "";
                if (confirmSend && cwd && openProjectPaneIfMissing) {
                  const confirmed = await ctx.ui.confirm(
                    "Send message",
                    `Send to "${to ?? cwd}":

${message}${attachmentText}`
                  );
                  if (!confirmed) {
                    return {
                      content: [{ type: "text", text: "Message cancelled by user" }],
                      details: {}
                    };
                  }
                }
                const resolvedTargetId = cwd ? void 0 : await resolveSessionTarget(connectedClient, to, pinned(to));
                const target2 = cwd ? await resolveCwdDeliveryTarget(connectedClient, { to, exactTo: pinned(to), cwd, openProjectPaneIfMissing, focus, signal: _signal }) : { id: resolvedTargetId ?? to, label: display(to) };
                const sendTo = target2.id;
                if (cwd || resolvedTargetId || knownTarget(sendTo)) routedSessions.add(sendTo);
                const targetDisplay = target2.projectPane ? target2.label : display(to) ?? target2.label;
                if (sendTo === connectedClient.sessionId) {
                  return {
                    content: [{ type: "text", text: "Cannot message the current session" }],
                    details: { error: true }
                  };
                }
                if (confirmSend && !(cwd && openProjectPaneIfMissing)) {
                  const confirmed = await ctx.ui.confirm(
                    "Send message",
                    `Send to "${targetDisplay}":

${message}${attachmentText}`
                  );
                  if (!confirmed) {
                    return {
                      content: [{ type: "text", text: "Message cancelled by user" }],
                      details: {}
                    };
                  }
                }
                const threadRecipient = replyTo ? resolveSessionFromRoster(await connectedClient.listSessions(), sendTo, pinned(sendTo)) : void 0;
                const prepared = threadRecipient?.federation ? await connectedClient.prepareConversation(threadRecipient) : void 0;
                if (prepared && (!getLiveContext(ctx, actionGeneration) || client !== connectedClient)) {
                  throw new Error("Session ended during thread preparation; no message was sent");
                }
                const sendOptions = {
                  ...prepared ? { messageId: prepared.messageId } : {},
                  text: message,
                  attachments,
                  replyTo,
                  completesAsk: false,
                  supersedes,
                  retryOf,
                  signal: _signal,
                  contactKind: "direct"
                };
                captureSendIdentity();
                const result = prepared ? await connectedClient.sendToSession(prepared.recipient, sendOptions) : await connectedClient.send(sendTo, { ...sendOptions, exactIdentity: pinned(sendTo) });
                settleLabel(result);
                if (!result.delivered) {
                  return {
                    content: [{ type: "text", text: describeDelivery(result, { kind: "Message", sender: sendIdentity, target: targetDisplay }) + projectObservation(target2) }],
                    details: { ...deliveryDetails(result), ...target2.projectPane ? { projectLaunch: target2.projectPane } : {} }
                  };
                }
                settleSupersededQuestion();
                recordActionEntry("parley_sent", {
                  to: targetDisplay,
                  targetId: result.recipient?.id ?? knownTarget(sendTo),
                  as: sendIdentity,
                  toolCallId: _toolCallId,
                  message: { text: message, attachments, replyTo, completesAsk: false, supersedes, retryOf },
                  messageId: result.id,
                  ...result.peerCompaction ? { peerCompaction: result.peerCompaction } : {},
                  timestamp: Date.now()
                });
                const awarenessText = describeDelivery(result, { kind: "Message", sender: sendIdentity, target: targetDisplay }) + projectObservation(target2);
                connectedClient.acknowledgeSendContact(result);
                return {
                  content: [{
                    type: "text",
                    text: awarenessText
                  }],
                  details: {
                    ...deliveryDetails(result),
                    ...replyTo ? { replyTo } : {},
                    ...target2.projectPane ? { openedProjectPane: true, projectLaunch: target2.projectPane, projectRoot: target2.projectPane.projectRoot, projectLauncher: target2.projectPane.provider.kind === "session" ? target2.projectPane.provider.name : "configured-command" } : {}
                  }
                };
              } catch (error) {
                return {
                  content: [{ type: "text", text: `Failed to send: ${failureText(error)}` }],
                  details: { error: true, ...failureIdentities(error) }
                };
              }
            }
            case "ask": {
              if (targets) {
                return {
                  content: [{ type: "text", text: "ask accepts one recipient through 'to' or cwd targeting; use separate asks when each recipient needs to reply." }],
                  details: { error: true }
                };
              }
              if (!to && !cwd || !message) {
                return {
                  content: [{ type: "text", text: "Missing 'to' or 'cwd', or missing 'message' parameter" }],
                  details: { error: true }
                };
              }
              if (replyWaiter && blocking !== false) {
                return {
                  content: [{ type: "text", text: "Already waiting for a reply" }],
                  details: { error: true }
                };
              }
              if (_signal?.aborted) {
                return {
                  content: [{ type: "text", text: "Cancelled" }],
                  details: { error: true }
                };
              }
              let replyPromise = null;
              let deliveryState = "created";
              let questionId = null;
              let questionCompaction;
              let questionSendResult;
              let questionTargetDisplay = display(to) ?? cwd ?? "peer";
              let questionTargetId;
              const rememberQuestion = (id, target2, label, endpointEpoch, originEpoch) => {
                if (!ownsSession()) {
                  lateHistorySkipped = true;
                  return;
                }
                const sentAt = Date.now();
                const binding = { ...endpointEpoch ? { endpointEpoch } : {}, ...originEpoch ? { originEpoch } : {} };
                outstandingAsks.set(id, { to: target2, ...binding, targetDisplay: label, preview: previewText(message, 120) ?? message, sentAt, message: { text: message, attachments } });
                recordActionEntry("parley_ask_pending", { messageId: id, to: target2, ...binding, targetDisplay: label, message: { text: message, attachments }, sentAt });
              };
              const settleQuestion = (id, reason) => {
                if (!ownsSession()) return;
                outstandingAsks.delete(id);
                recordActionEntry("parley_ask_settled", { messageId: id, reason, timestamp: Date.now() });
              };
              try {
                if (openProjectPaneIfMissing && !cwd) {
                  return {
                    content: [{ type: "text", text: "openProjectPaneIfMissing requires a target cwd." }],
                    details: { error: true }
                  };
                }
                let target2;
                if (cwd) {
                  target2 = await resolveCwdDeliveryTarget(connectedClient, { to, exactTo: pinned(to), cwd, openProjectPaneIfMissing, focus, signal: _signal });
                } else {
                  const resolved = resolveSessionFromRoster(await connectedClient.listSessions(), to, pinned(to));
                  if (!resolved) {
                    return {
                      content: [{ type: "text", text: `Session "${display(to)}" is not currently connected. Questions require a connected peer; no question was sent.` }],
                      details: { error: true }
                    };
                  }
                  target2 = { id: resolved.id, label: display(to), session: resolved };
                }
                const sendTo = target2.id;
                routedSessions.add(sendTo);
                const targetDisplay = target2.projectPane ? target2.label : display(to) ?? target2.label;
                questionTargetDisplay = targetDisplay;
                questionTargetId = sendTo;
                if (_signal?.aborted) {
                  return {
                    content: [{ type: "text", text: "Cancelled" }],
                    details: { error: true }
                  };
                }
                if (sendTo === connectedClient.sessionId) {
                  return {
                    content: [{ type: "text", text: "Cannot message the current session" }],
                    details: { error: true }
                  };
                }
                if (replyWaiter && blocking !== false) {
                  return {
                    content: [{ type: "text", text: "Already waiting for a reply" }],
                    details: { error: true }
                  };
                }
                const prepared = target2.session?.federation ? await connectedClient.prepareConversation(target2.session) : void 0;
                if (prepared && (!getLiveContext(ctx, actionGeneration) || client !== connectedClient)) {
                  throw new Error("Session ended during conversation preparation; no question was sent");
                }
                if (_signal?.aborted) throw new Error("Cancelled before question dispatch");
                if (replyWaiter && blocking !== false) throw new Error("Already waiting for a reply");
                const preparedId = prepared?.messageId ?? randomUUID6();
                const responderEpoch = prepared?.recipient.endpointEpoch;
                const responderOriginEpoch = prepared?.recipient.federation?.originEpoch;
                const sendQuestion = (options2) => prepared ? connectedClient.sendToSession(prepared.recipient, options2) : connectedClient.send(sendTo, { ...options2, exactIdentity: pinned(sendTo) });
                if (blocking === false) {
                  const askId = preparedId;
                  rememberQuestion(askId, sendTo, targetDisplay, responderEpoch, responderOriginEpoch);
                  captureSendIdentity();
                  const sendResult2 = await sendQuestion({
                    messageId: askId,
                    text: message,
                    attachments,
                    replyTo,
                    expectsReply: true,
                    completesAsk: false,
                    senderWaitMode: "nonblocking",
                    supersedes,
                    retryOf,
                    signal: _signal,
                    contactKind: "direct"
                  });
                  settleLabel({ ...sendResult2, messageId: askId });
                  if (!sendResult2.delivered) {
                    if (sendResult2.outcomeKnown) {
                      settleQuestion(askId, "not-delivered");
                    }
                    return {
                      content: [{ type: "text", text: describeDelivery(sendResult2, { kind: "Ask", sender: sendIdentity, target: targetDisplay }) + projectObservation(target2) }],
                      details: { error: true, ...deliveryDetails(sendResult2) }
                    };
                  }
                  settleSupersededQuestion();
                  recordActionEntry("parley_sent", {
                    to: targetDisplay,
                    targetId: sendResult2.recipient?.id ?? knownTarget(sendTo),
                    as: sendIdentity,
                    toolCallId: _toolCallId,
                    message: { text: message, attachments, replyTo, completesAsk: false, supersedes, retryOf },
                    messageId: sendResult2.id,
                    ...sendResult2.peerCompaction ? { peerCompaction: sendResult2.peerCompaction } : {},
                    timestamp: Date.now()
                  });
                  connectedClient.acknowledgeSendContact(sendResult2);
                  return {
                    content: [{
                      type: "text",
                      text: `${describeDelivery(sendResult2, { kind: "Ask", sender: sendIdentity, target: targetDisplay }) + projectObservation(target2)}
Non-blocking: the answer arrives in this conversation.`
                    }],
                    details: {
                      nonBlocking: true,
                      messageId: askId,
                      ...deliveryDetails(sendResult2),
                      ...target2.projectPane ? { openedProjectPane: true, projectLaunch: target2.projectPane, projectRoot: target2.projectPane.projectRoot, projectLauncher: target2.projectPane.provider.kind === "session" ? target2.projectPane.provider.name : "configured-command" } : {}
                    }
                  };
                }
                questionId = preparedId;
                rememberQuestion(questionId, sendTo, targetDisplay, responderEpoch, responderOriginEpoch);
                replyPromise = waitForReply(sendTo, questionId, _signal, () => connectedClient.cancelAsk(questionId), () => latestDeliveryState(questionId, deliveryState), responderEpoch, responderOriginEpoch);
                replyPromise.catch(() => void 0);
                captureSendIdentity();
                const sendResult = await sendQuestion({
                  messageId: questionId,
                  text: message,
                  attachments,
                  replyTo,
                  expectsReply: true,
                  completesAsk: false,
                  senderWaitMode: "blocking",
                  supersedes,
                  retryOf,
                  signal: _signal,
                  contactKind: "direct"
                });
                settleLabel({ ...sendResult, messageId: questionId });
                questionSendResult = sendResult;
                deliveryState = sendResult.delivery;
                questionCompaction = sendResult.peerCompaction;
                if (!sendResult.delivered) {
                  if (sendResult.outcomeKnown) settleQuestion(questionId, "not-delivered");
                  const errorText2 = sendResult.reason ?? "Session may not exist or has disconnected.";
                  rejectOwnedReplyWaiter(questionId, new Error(`Message to "${targetDisplay}" was not delivered: ${errorText2}`));
                  let answered = false;
                  try {
                    await replyPromise;
                    answered = true;
                  } catch {
                  }
                  if (sendResult.outcomeKnown || !answered) return {
                    content: [{ type: "text", text: describeDelivery(sendResult, { kind: "Ask", sender: sendIdentity, target: targetDisplay }) + projectObservation(target2) }],
                    details: { error: true, ...deliveryDetails(sendResult) }
                  };
                }
                settleSupersededQuestion();
                recordActionEntry("parley_sent", {
                  to: targetDisplay,
                  targetId: sendResult.recipient?.id ?? knownTarget(sendTo),
                  as: sendIdentity,
                  toolCallId: _toolCallId,
                  message: { text: message, attachments, replyTo, completesAsk: false, supersedes, retryOf },
                  messageId: sendResult.id,
                  ...sendResult.peerCompaction ? { peerCompaction: sendResult.peerCompaction } : {},
                  timestamp: Date.now()
                });
                const replyMessage = await replyPromise;
                settleQuestion(questionId, "answer returned");
                const replyText = replyMessage.content.text;
                const replyAttachments = replyMessage.content.attachments?.length ? formatAttachments(replyMessage.content.attachments) : "";
                recordActionEntry("parley_received", {
                  from: targetDisplay,
                  message: { text: replyText, attachments: replyMessage.content.attachments },
                  messageId: replyMessage.id,
                  ...replyMessage.peerCompaction ? { peerCompaction: replyMessage.peerCompaction } : {},
                  timestamp: replyMessage.timestamp
                });
                const latestCompaction = replyMessage.peerCompaction && (!questionCompaction || replyMessage.peerCompaction.generation > questionCompaction.generation) ? replyMessage.peerCompaction : questionCompaction;
                const awarenessText = latestCompaction ? `${describeCompaction(targetDisplay, latestCompaction, sendTo)}

` : "";
                connectedClient.acknowledgeSendContact(sendResult);
                acknowledgeInboundMessageContact(connectedClient, replyMessage);
                return {
                  content: [{ type: "text", text: `${!sendResult.outcomeKnown ? "Ask acceptance remains unknown; no replay was attempted. A correlated answer was received independently.\n\n" : ""}${awarenessText}**Reply from ${targetDisplay}** (asked as ${sendIdentity}):
Question: ${messageRef(questionId)}
Reply: ${messageRef(replyMessage.id)}

${verbatim(`${replyText}${replyAttachments}`)}` }],
                  details: {
                    ...deliveryDetails(sendResult),
                    replyMessageId: replyMessage.id,
                    ...latestCompaction ? { peerCompaction: latestCompaction } : {},
                    ...target2.projectPane ? { openedProjectPane: true, projectLaunch: target2.projectPane, projectRoot: target2.projectPane.projectRoot, projectLauncher: target2.projectPane.provider.kind === "session" ? target2.projectPane.provider.name : "configured-command" } : {}
                  }
                };
              } catch (error) {
                rejectOwnedReplyWaiter(questionId, toError3(error));
                if (replyPromise) {
                  try {
                    await replyPromise;
                  } catch {
                  }
                }
                const failureMessage = `Failed: ${failureText(error)}`;
                if (questionSendResult) connectedClient.acknowledgeSendContact(questionSendResult);
                return {
                  content: [{
                    type: "text",
                    text: questionCompaction ? `${failureMessage}

${describeCompaction(questionTargetDisplay, questionCompaction, questionTargetId)}` : failureMessage
                  }],
                  details: {
                    error: true,
                    ...failureIdentities(error),
                    ...questionId ? { messageId: questionId, deliveryState: latestDeliveryState(questionId, deliveryState) } : {},
                    ...questionCompaction ? { peerCompaction: questionCompaction } : {}
                  }
                };
              }
            }
            case "reply": {
              if (targets) {
                return {
                  content: [{ type: "text", text: "reply accepts one pending conversation; 'targets' is only available for send." }],
                  details: { error: true }
                };
              }
              if (!message) {
                return {
                  content: [{ type: "text", text: "Missing 'message' parameter" }],
                  details: { error: true }
                };
              }
              try {
                const target2 = replyTracker.resolveReplyTarget({ to, replyTo, exactSender: pinned(to) });
                if (target2.from.id === connectedClient.sessionId) {
                  return {
                    content: [{ type: "text", text: "Cannot message the current session" }],
                    details: { error: true }
                  };
                }
                const prepared = target2.from.federation ? await connectedClient.prepareConversation(target2.from) : void 0;
                if (prepared && (!getLiveContext(ctx, actionGeneration) || client !== connectedClient)) {
                  throw new Error("Session ended during reply preparation; no answer was sent");
                }
                const replyOptions = {
                  ...prepared ? { messageId: prepared.messageId } : {},
                  text: message,
                  attachments,
                  replyTo: target2.message.id,
                  completesAsk: true,
                  signal: _signal,
                  contactKind: "direct"
                };
                captureSendIdentity();
                const result = prepared ? await connectedClient.sendToSession(prepared.recipient, replyOptions) : await connectedClient.send(target2.from.id, { ...replyOptions, exactIdentity: pinned(to) });
                if (!result.delivered) {
                  return {
                    content: [{ type: "text", text: describeDelivery(result, { kind: "Reply", sender: sendIdentity, target: references.current.sessionRef(target2.from) }) }],
                    details: deliveryDetails(result)
                  };
                }
                if (ownsSession()) dismissIncomingAsk(target2.message.id);
                recordActionEntry("parley_sent", {
                  to: references.current.sessionRef(target2.from),
                  targetId: result.recipient?.id ?? target2.from.id,
                  as: sendIdentity,
                  toolCallId: _toolCallId,
                  message: { text: message, attachments, replyTo: target2.message.id, completesAsk: true },
                  messageId: result.id,
                  ...result.peerCompaction ? { peerCompaction: result.peerCompaction } : {},
                  timestamp: Date.now()
                });
                const targetDisplay = references.current.sessionRef(target2.from);
                const awarenessText = describeDelivery(result, { kind: "Reply", sender: sendIdentity, target: targetDisplay });
                connectedClient.acknowledgeSendContact(result);
                return {
                  content: [{
                    type: "text",
                    text: awarenessText
                  }],
                  details: { ...deliveryDetails(result), replyTo: target2.message.id }
                };
              } catch (error) {
                return {
                  content: [{ type: "text", text: `Failed to reply: ${failureText(error)}` }],
                  details: { error: true }
                };
              }
            }
            case "label": {
              if (!messageId || !requestedLabel) {
                return { content: [{ type: "text", text: "label needs messageId (the message to name) and label (its new name)." }], details: { error: true } };
              }
              if (!book.isKnownMessage(messageId)) {
                return { content: [{ type: "text", text: `${messageId} is not a message this session knows.` }], details: { error: true } };
              }
              const labelled = book.labelMessage(messageId, requestedLabel);
              if (!labelled.ok) {
                return { content: [{ type: "text", text: `Label not applied: ${labelled.reason}` }], details: { error: true } };
              }
              return { content: [{ type: "text", text: `Message ${labelled.ref}; ${requestedLabel} now names it here.` }], details: { messageId } };
            }
            case "read": {
              const retained = messageId ? replyTracker.getMessage(messageId) : void 0;
              if (!retained) {
                return { content: [{ type: "text", text: messageId ? `Message ${references.current.isKnownMessage(messageId) ? messageRef(messageId) : messageId} is not retained in this session.` : "Missing messageId." }], details: { error: true } };
              }
              const { from, message: original, disposition } = retained;
              const state = disposition ? `
Status: ${disposition.state}${disposition.replacementId ? ` by message ${references.current.messageRef(disposition.replacementId)}` : ""}` : "";
              const attachmentsText = original.content.attachments?.length ? formatAttachments(original.content.attachments) : "";
              const origin = from.federation ? `
Remote origin: ${references.current.originRef(from.federation)}` : "";
              return {
                content: [{ type: "text", text: `From ${references.current.sessionRef(from)}${from.description ? ` \u2014 ${from.description}` : ""} (${from.cwd})${origin}
${formatInboundDeliveryMetadata(original, messageRef)}${state}

${verbatim(`${original.content.text}${attachmentsText}`)}` }],
                details: { messageId: original.id }
              };
            }
            case "pending": {
              const conversation = replyTracker.formatConversationContext({ limit: Infinity, previewLength: 180, complete: true });
              return {
                content: [{ type: "text", text: conversation || "No unresolved inbound asks." }],
                details: {}
              };
            }
            case "status": {
              try {
                const mySessionId = connectedClient.sessionId;
                const sessions = await connectedClient.listSessions();
                const outstandingText = `
${formatOutstandingAsks(20, { complete: true }) || "Outstanding asks: none"}`;
                return {
                  content: [{
                    type: "text",
                    text: `**Parley Status:**
Connected: Yes
Session: ${references.current.sessionRef(sessions.find((session) => session.id === mySessionId) ?? mySessionId ?? "unknown")}
Visible connected sessions: ${sessions.length} (including this session; scope and permissions apply)${outstandingText}`
                  }],
                  details: {
                    outstandingAsks: [...outstandingAsks.entries()].map(([id, ask]) => ({
                      messageId: id,
                      to: ask.targetDisplay,
                      sentAt: ask.sentAt
                    }))
                  }
                };
              } catch (error) {
                return {
                  content: [{ type: "text", text: `Failed to get status: ${failureText(error)}` }],
                  details: { error: true }
                };
              }
            }
            default:
              return {
                content: [{ type: "text", text: `Unknown action: ${action}` }],
                details: { error: true }
              };
          }
        })();
        if (sendIdentity && ["send", "ask", "reply", "broadcast"].includes(params.action)) {
          toolResult.details = { ...toolResult.details ?? {}, senderIdentity: sendIdentity };
        }
        const stale = runtimeGeneration !== actionGeneration;
        settleLabel(toolResult.details);
        if (!stale && ["send", "ask", "reply", "cancel", "broadcast"].includes(params.action)) {
          const conversation = replyTracker.formatConversationContext();
          if (conversation) toolResult.content.push({ type: "text", text: conversation });
          const outstanding = formatOutstandingAsks();
          if (outstanding) toolResult.content.push({ type: "text", text: outstanding });
        }
        if (stale || lateHistorySkipped) {
          toolResult.content.push({ type: "text", text: "This result arrived after the Pi session changed. What it reports happened, but it was not recorded in the current session's history." });
        }
        if (conversationPersistenceWarning) toolResult.content.push({ type: "text", text: conversationPersistenceWarning });
        if (historyWarnings.length) {
          toolResult.content.push({ type: "text", text: `Local history was not fully persisted; recovery may be incomplete. ${historyWarnings.join("; ")}` });
        }
        const hint = stale ? void 0 : successorHintFor(params, resolvedReferences, toolResult.details);
        if (hint) toolResult.content.push({ type: "text", text: hint });
        for (const note of callShortNameNotes) toolResult.content.push({ type: "text", text: note });
        if (labelSkippedForBatch) toolResult.content.push({ type: "text", text: `Label "${requestedLabel}" was not applied: a multi-target send makes one message per recipient. Label one afterwards with the label action.` });
        const resultText = toolResult.content.map((part) => part.type === "text" ? part.text ?? "" : "").join("\n");
        if (/Session not found/.test(resultText)) {
          const requested = [sentTo, ...Array.isArray(sentTargets) ? sentTargets : []].filter((value) => typeof value === "string" && Boolean(value.trim()));
          const hint2 = await nameSuggestions(requested);
          if (hint2) toolResult.content.push({ type: "text", text: hint2 });
        }
        if (ignoredTargets) toolResult.content.push({ type: "text", text: `Ignored targets ${JSON.stringify(ignoredTargets)}: ${params.action} goes to one colleague. To reach several, send to them with targets.` });
        return attachSelfProfile(presentToolResult(toolResult, { book, sessions: routedSessions }), normalizeToolProfilePlaceholders(params.profile) !== void 0);
      }));
    },
    renderCall(args, theme, context) {
      const action = typeof args.action === "string" ? args.action : PARLEY_TOOL_NAME;
      const target2 = typeof args.to === "string" && args.to.trim() ? args.to.trim() : void 0;
      const targets = Array.isArray(args.targets) ? args.targets.filter((value) => typeof value === "string" && Boolean(value.trim())) : [];
      const messagePreview = previewText(args.message, 96);
      const attachmentCount = Array.isArray(args.attachments) ? args.attachments.length : 0;
      let text = theme.fg("toolTitle", theme.bold("parley "));
      text += theme.fg(action === "ask" || action === "broadcast" ? "warning" : action === "reply" ? "success" : "accent", action);
      const senderIdentity = context?.state?.senderIdentity;
      if (senderIdentity && ["send", "ask", "reply", "broadcast"].includes(action)) {
        text += " " + theme.fg("muted", `as ${senderIdentity}`);
      }
      if (target2) {
        text += " " + theme.fg("muted", "\u2192") + " " + theme.fg("accent", references.current.present(target2));
      } else if (targets.length > 0) {
        const presentedTargets = targets.map((value) => references.current.present(value));
        const targetSummary = presentedTargets.length <= 3 ? presentedTargets.join(", ") : `${presentedTargets.slice(0, 3).join(", ")} +${presentedTargets.length - 3}`;
        text += " " + theme.fg("muted", "\u2192") + " " + theme.fg("accent", targetSummary);
      } else if (action === "broadcast") {
        text += " " + theme.fg("muted", "\u2192 visible local sessions");
      }
      if (attachmentCount > 0) {
        text += " " + theme.fg("dim", `(${attachmentCount} attachment${attachmentCount === 1 ? "" : "s"})`);
      }
      const open = rowExpanded(context);
      const fullMessage = typeof args.message === "string" ? args.message.trim() : "";
      if (open && fullMessage) {
        text += "\n" + fullMessage.split("\n").map((line) => "  " + theme.fg("text", line)).join("\n");
      } else if (messagePreview) {
        text += "\n  " + theme.fg("dim", messagePreview);
      }
      return clickToExpand(new Text(text, 0, 0), context);
    },
    renderResult(result, { isPartial }, theme, context) {
      const details = result.details;
      if (details?.senderIdentity) {
        const state = context.state ?? {};
        if (state.senderIdentity !== details.senderIdentity) {
          state.senderIdentity = details.senderIdentity;
          context.state = state;
          context.invalidate?.();
        }
      }
      if (isPartial) {
        return new Text(theme.fg("warning", details?.senderIdentity ? `Parley working as ${details.senderIdentity}\u2026` : "Parley working..."), 0, 0);
      }
      const failed = Boolean(context.isError || details?.error === true || details?.delivered === false && !details?.replyMessageId);
      const uncertain = details?.outcomeKnown === false || (details?.unknownCount ?? 0) > 0;
      let text = uncertain ? theme.fg("warning", "? ") : failed ? theme.fg("error", "\u2717 ") : theme.fg("success", "\u2713 ");
      text += theme.fg(uncertain ? "warning" : failed ? "error" : "text", references.current.present(result.content.filter((item) => item.type === "text").map((item) => item.text).join("\n")));
      const open = rowExpanded(context);
      if (details?.messageId && !open) {
        const messageRef2 = references.current.messageRef(details.messageId);
        if (!text.includes(messageRef2)) text += theme.fg("dim", ` (${messageRef2})`);
      }
      if (details?.reason && open) {
        text += "\n" + theme.fg("dim", `Reason: ${details.reason}`);
      }
      return clickToExpand(new Text(text, 0, 0), context);
    }
  }));
  function insertIntoEditor(ctx, text) {
    if (!ctx.hasUI) return false;
    const ui = ctx.ui;
    if (typeof ui.setEditorText !== "function") return false;
    const existing = typeof ui.getEditorText === "function" ? ui.getEditorText() : "";
    ui.setEditorText(existing.trim() ? `${existing.trimEnd()}

${text}` : text);
    return true;
  }
  async function insertParleyId(ctx, args = "") {
    const canonical = args.trim().split(/\s+/).includes("--id");
    const commandGeneration = runtimeGeneration;
    const liveContext = getLiveContext(ctx, commandGeneration);
    if (!liveContext) return;
    let contactClient;
    try {
      contactClient = await ensureConnected("tool");
    } catch (error) {
      notifyIfLive(ctx, `Parley unavailable: ${failureText(error)}`, "error", commandGeneration);
      return;
    }
    const sessionId = contactClient.sessionId;
    if (!sessionId || !getLiveContext(liveContext, commandGeneration)) return;
    const self = contactClient.getSelfSession();
    const contact = self?.name?.trim() || currentSendIdentity(contactClient);
    const snippet = canonical ? formatParleyContactSnippet(sessionId) : `Pi parley contact: ${contact}${self?.cwd ? ` (in ${self.cwd})` : ""}`;
    const summary = canonical ? `parley contact target: ${sessionId}` : `parley contact: ${contact}`;
    if (insertIntoEditor(liveContext, snippet)) {
      notifyIfLive(liveContext, `Inserted ${summary}`, "info", commandGeneration);
      return;
    }
    notifyIfLive(liveContext, summary.charAt(0).toUpperCase() + summary.slice(1), "info", commandGeneration);
  }
  async function setParleyAlias(args, ctx) {
    const commandGeneration = runtimeGeneration;
    const liveContext = getLiveContext(ctx, commandGeneration);
    if (!liveContext) return;
    let alias = args.trim();
    const opensAliasInput = !alias || alias.toLowerCase() === "menu";
    if (opensAliasInput) {
      if (!liveContext.hasUI) {
        const currentAlias2 = pi.getSessionName()?.trim();
        notifyAliasCommand(
          liveContext,
          alias ? "The alias menu requires an interactive UI; use /alias <name>." : currentAlias2 ? `Session alias: ${currentAlias2}` : "No session alias set. Use /alias <name>.",
          alias ? "warning" : "info",
          commandGeneration
        );
        return;
      }
      const currentAlias = pi.getSessionName()?.trim();
      let entered;
      try {
        entered = await liveContext.ui.input(
          "Set session alias",
          currentAlias ? `Current alias: ${currentAlias}` : "Enter an alias"
        );
      } catch (error) {
        notifyAliasCommand(liveContext, `Unable to set session alias: ${failureText(error)}`, "error", commandGeneration);
        return;
      }
      if (entered === void 0) return;
      alias = entered.trim();
      if (!alias) {
        notifyAliasCommand(liveContext, "Session alias cannot be empty.", "warning", commandGeneration);
        return;
      }
    }
    if (!getLiveContext(liveContext, commandGeneration)) return;
    try {
      resolveEmbeddedPresenceName(configuration, alias, SESSION_PRESENCE_NAME_CONTEXT);
      pi.setSessionName(alias);
      syncPresenceIdentity(liveContext.sessionManager.getSessionId());
    } catch (error) {
      notifyAliasCommand(liveContext, `Unable to set session alias: ${failureText(error)}`, "error", commandGeneration);
      return;
    }
    notifyAliasCommand(liveContext, `Session alias set: ${alias}`, "info", commandGeneration);
  }
  async function openParleyOverlay(ctx) {
    const overlayGeneration = runtimeGeneration;
    const liveContext = getLiveContext(ctx, overlayGeneration);
    const mode = liveContext?.mode;
    if (!liveContext?.hasUI || mode !== void 0 && mode !== "tui") return;
    let overlayClient;
    try {
      overlayClient = await ensureConnected("overlay");
    } catch (error) {
      notifyIfLive(ctx, `Parley unavailable: ${failureText(error)}`, "error", overlayGeneration);
      return;
    }
    if (!getLiveContext(ctx, overlayGeneration)) return;
    syncPresenceIdentity(ctx.sessionManager.getSessionId());
    let currentSession;
    let sessions;
    let rosterRefs;
    try {
      const mySessionId = overlayClient.sessionId;
      const allSessions = await overlayClient.listSessions();
      if (!getLiveContext(ctx, overlayGeneration)) return;
      const foundCurrentSession = allSessions.find((s) => s.id === mySessionId);
      if (!foundCurrentSession) {
        notifyIfLive(ctx, "Current session is missing from parley session list", "error", overlayGeneration);
        return;
      }
      currentSession = foundCurrentSession;
      rosterRefs = rosterReferences(allSessions);
      sessions = allSessions.filter((s) => s.id !== mySessionId);
    } catch (error) {
      notifyIfLive(ctx, `Failed to list sessions: ${failureText(error)}`, "error", overlayGeneration);
      return;
    }
    const selectedSession = await ctx.ui.custom(
      (_tui, theme, keybindings, done) => new SessionListOverlay(theme, keybindings, currentSession, sessions, done, (session) => rosterRefs.get(session.id) ?? references.current.sessionRef(session), originOf),
      { overlay: true, overlayOptions: { width: 88 } }
    ).catch(() => void 0);
    if (!selectedSession || !getLiveContext(ctx, overlayGeneration)) return;
    try {
      overlayClient = await ensureConnected("overlay");
    } catch (error) {
      notifyIfLive(ctx, `Parley unavailable: ${failureText(error)}`, "error", overlayGeneration);
      return;
    }
    if (!getLiveContext(ctx, overlayGeneration)) return;
    const targetLabel = rosterRefs.get(selectedSession.id) ?? references.current.sessionRef(selectedSession);
    const result = await ctx.ui.custom(
      (tui, theme, keybindings, done) => new ComposeOverlay(tui, theme, keybindings, selectedSession, targetLabel, overlayClient, done),
      { overlay: true, overlayOptions: { width: 72 } }
    ).catch(() => void 0);
    if (result?.sent && result.messageId && result.text && getLiveContext(ctx, overlayGeneration)) {
      recordConversationEntry("parley_sent", {
        to: selectedSession.name || selectedSession.id,
        targetId: selectedSession.id,
        as: currentSendIdentity(overlayClient),
        message: { text: result.text },
        messageId: result.messageId,
        ...result.peerCompaction ? { peerCompaction: result.peerCompaction } : {},
        timestamp: Date.now()
      });
      const deliveryNotice = result.delivery === "queued" ? `Message queued for offline session ${targetLabel} \u2014 delivered only if it reconnects within 24h` : `Message sent to ${targetLabel} (as ${currentSendIdentity(overlayClient)})`;
      notifyIfLive(
        ctx,
        result.peerCompaction ? presentForModel(`${deliveryNotice}

${describeCompaction(targetLabel, result.peerCompaction, selectedSession.id)}`) : deliveryNotice,
        "info",
        overlayGeneration
      );
      overlayClient.acknowledgeSendContact(result);
    }
  }
  const parleyCommand = {
    description: "Browse sessions and send a message; type a space for links to other computers, your contact, and your name",
    getArgumentCompletions: (prefix) => parleyCommandSuggestions(prefix),
    handler: async (args, ctx) => {
      const text = (args ?? "").trim();
      const [first = "", ...rest2] = text.split(/\s+/);
      switch (first.toLowerCase()) {
        case "link": {
          const outcome = await links.command(rest2.join(" "));
          notifyAliasCommand(ctx, outcome.text, outcome.ok ? "info" : "warning");
          return;
        }
        case "id":
          return insertParleyId(ctx, rest2.join(" "));
        case "alias":
          return setParleyAlias(rest2.join(" "), ctx);
        default:
          return openParleyOverlay(ctx);
      }
    }
  };
  pi.registerCommand("parley", parleyCommand);
  pi.registerCommand("intercom", { ...parleyCommand, description: `Parley: ${parleyCommand.description}` });
  pi.registerCommand("parley-id", {
    description: "Insert this session's parley contact into the editor (--id for the stable canonical target)",
    handler: async (args, ctx) => insertParleyId(ctx, args)
  });
  pi.registerCommand("alias", {
    description: "Set or inspect this session's alias (usage: /alias <name> or /alias menu)",
    handler: async (args, ctx) => setParleyAlias(args, ctx)
  });
  pi.registerShortcut("alt+m", {
    description: "Open session parley",
    handler: async (ctx) => openParleyOverlay(ctx)
  });
  pi.on("session_shutdown", () => releaseRuntimeClaim());
  pi.events.emit(PARLEY_EXTENSION_REGISTRY_READY_EVENT, { version: 1 });
}
function parleyCommandSuggestions(prefix) {
  const match = /^(\S*)(\s+([\s\S]*))?$/.exec(prefix);
  const verb = (match?.[1] ?? "").toLowerCase();
  if (match?.[2] === void 0) {
    const items = PARLEY_COMMAND_VERBS.filter((item) => item.label.startsWith(verb));
    return items.length > 0 ? items.map((item) => ({ ...item })) : null;
  }
  if (verb === "link") {
    const items = linkSuggestions(match[3] ?? "").map((item) => ({ ...item, value: `link ${item.value}` }));
    return items.length > 0 ? items : null;
  }
  if (verb === "alias" && "menu".startsWith((match[3] ?? "").trim())) {
    return [{ value: "alias menu", label: "menu", description: "Choose a name from a menu" }];
  }
  return null;
}
function rowExpanded(context) {
  const clicked = context?.state?.clickedOpen;
  return clicked ?? Boolean(context?.expanded);
}
function clickToExpand(child, context) {
  if (!context) return child;
  return new ClickToExpand(child, () => {
    const state = context.state ?? {};
    state.clickedOpen = !rowExpanded(context);
    context.state = state;
    context.invalidate?.();
  });
}
var verbatim, PRESENTED_MARK, PARLEY_TOOL_NAME, SUBAGENT_CONTROL_PARLEY_EVENT, SUBAGENT_RESULT_PARLEY_EVENT, SUBAGENT_RESULT_PARLEY_DELIVERY_EVENT, INBOUND_MESSAGE_DEDUPE_MAX, INBOUND_MESSAGE_DEDUPE_RETENTION_MS, MAX_EXPLICIT_SEND_TARGETS, SEND_FANOUT_CONCURRENCY, COMPACTION_STATUS_FAILSAFE_MS, DEFAULT_UNNAMED_SESSION_ALIAS_PREFIX, SUBAGENT_ORCHESTRATOR_TARGET_ENV, SUBAGENT_ORCHESTRATOR_SESSION_ID_ENV, PARLEY_SESSION_ID_ENV, STABLE_PARLEY_SESSION_ID_ENV, SUBAGENT_RUN_ID_ENV, SUBAGENT_CHILD_AGENT_ENV, SUBAGENT_CHILD_INDEX_ENV, SUBAGENT_PARLEY_SESSION_NAME_ENV, SUBAGENT_SUPERVISOR_CHANNEL_DIR_ENV, SESSION_PRESENCE_NAME_CONTEXT, PARLEY_EXTENSION_RUNTIME_CLAIM_EVENT, PARLEY_COMMAND_VERBS, index_default;
var init_index = __esm({
  "index.ts"() {
    init_build();
    init_dist();
    init_pi_compat();
    init_client();
    init_spawn();
    init_session_list();
    init_compose();
    init_inline_message();
    init_click_to_expand();
    init_config2();
    init_types2();
    init_extension_api();
    init_reply_tracker();
    init_references();
    init_conversation_history();
    init_cwd();
    init_compaction_awareness();
    init_message_results();
    init_session();
    init_completions();
    init_names();
    init_project_agent();
    init_session_profile();
    verbatim = (text) => ReferenceBook.verbatim(text);
    PRESENTED_MARK = "referencesPresented";
    PARLEY_TOOL_NAME = "parley";
    SUBAGENT_CONTROL_PARLEY_EVENT = "subagent:control-parley";
    SUBAGENT_RESULT_PARLEY_EVENT = "subagent:result-parley";
    SUBAGENT_RESULT_PARLEY_DELIVERY_EVENT = "subagent:result-parley-delivery";
    INBOUND_MESSAGE_DEDUPE_MAX = 1e3;
    INBOUND_MESSAGE_DEDUPE_RETENTION_MS = 60 * 60 * 1e3;
    MAX_EXPLICIT_SEND_TARGETS = 32;
    SEND_FANOUT_CONCURRENCY = 8;
    COMPACTION_STATUS_FAILSAFE_MS = 15 * 60 * 1e3;
    DEFAULT_UNNAMED_SESSION_ALIAS_PREFIX = "session";
    SUBAGENT_ORCHESTRATOR_TARGET_ENV = "PI_SUBAGENT_ORCHESTRATOR_TARGET";
    SUBAGENT_ORCHESTRATOR_SESSION_ID_ENV = "PI_SUBAGENT_ORCHESTRATOR_SESSION_ID";
    PARLEY_SESSION_ID_ENV = "PI_PARLEY_SESSION_ID";
    STABLE_PARLEY_SESSION_ID_ENV = "PI_PARLEY_STABLE_ID";
    SUBAGENT_RUN_ID_ENV = "PI_SUBAGENT_RUN_ID";
    SUBAGENT_CHILD_AGENT_ENV = "PI_SUBAGENT_CHILD_AGENT";
    SUBAGENT_CHILD_INDEX_ENV = "PI_SUBAGENT_CHILD_INDEX";
    SUBAGENT_PARLEY_SESSION_NAME_ENV = "PI_SUBAGENT_PARLEY_SESSION_NAME";
    SUBAGENT_SUPERVISOR_CHANNEL_DIR_ENV = "PI_SUBAGENT_SUPERVISOR_CHANNEL_DIR";
    SESSION_PRESENCE_NAME_CONTEXT = Object.freeze({ kind: "session" });
    PARLEY_EXTENSION_RUNTIME_CLAIM_EVENT = "pi-parley:extension-runtime-claim:v1";
    PARLEY_COMMAND_VERBS = [
      { value: "sessions", label: "sessions", description: "Browse sessions and send a message (same as /parley alone)" },
      { value: "link ", label: "link", description: "Link this computer with others over SSH" },
      { value: "id", label: "id", description: "Insert this session's contact into the editor (--id for the canonical target)" },
      { value: "alias ", label: "alias", description: "Set or show this session's name (alias menu to choose)" }
    ];
    index_default = registerParleyExtension;
  }
});

// hosts/hosted-session.ts
import { EventEmitter as EventEmitter2 } from "node:events";
import { appendFileSync, existsSync as existsSync4, mkdirSync as mkdirSync4, readFileSync as readFileSync7 } from "node:fs";
import { dirname as dirname5 } from "node:path";
import { randomUUID as randomUUID7 } from "node:crypto";
async function createHostedSession(options2) {
  let sessionId;
  let stateFile;
  let history = [];
  const record = (entry) => {
    history.push(entry);
    if (stateFile) appendFileSync(stateFile, `${JSON.stringify(entry)}
`, { mode: 384 });
  };
  let sessionName;
  let app;
  let busy = false;
  const bus = new EventEmitter2();
  const handlers = /* @__PURE__ */ new Map();
  const tools = [];
  const pi = {
    getSessionName: () => sessionName,
    setSessionName: (name) => {
      if (name === sessionName) return;
      sessionName = name;
      record({ kind: "name", name, at: (/* @__PURE__ */ new Date()).toISOString() });
      void emit("session_info_changed", { type: "session_info_changed", name });
    },
    events: {
      on: (channel, handler) => {
        bus.on(channel, handler);
        return () => bus.off(channel, handler);
      },
      emit: (channel, payload) => bus.emit(channel, payload)
    },
    on: (event, handler) => {
      handlers.set(event, [...handlers.get(event) ?? [], handler]);
      return () => handlers.set(event, (handlers.get(event) ?? []).filter((h) => h !== handler));
    },
    registerTool: (tool2) => {
      tools.push(tool2);
    },
    getActiveTools: () => tools.map((tool2) => tool2.name),
    setActiveTools: () => void 0,
    // A host has its own commands, shortcuts and rendering.
    registerCommand: () => void 0,
    registerShortcut: () => void 0,
    registerMessageRenderer: () => void 0,
    sendMessage: (message, delivery) => {
      record({ kind: "message", message, at: (/* @__PURE__ */ new Date()).toISOString() });
      const text = typeof message.content === "string" ? message.content : Array.isArray(message.content) ? message.content.map((part) => part?.text ?? "").join("\n") : "";
      if (!text.trim()) return;
      options2.deliver({
        text,
        wake: delivery?.triggerTurn === true,
        ...delivery?.deliverAs === "steer" || delivery?.deliverAs === "followUp" ? { during: delivery.deliverAs } : {}
      });
    },
    appendEntry: (type, data) => record({ kind: "entry", type, data, at: (/* @__PURE__ */ new Date()).toISOString() })
  };
  const ctx = {
    cwd: options2.cwd,
    model: { id: options2.model ?? "unknown" },
    hasUI: false,
    isIdle: () => !busy,
    abort: () => void 0,
    ui: void 0,
    sessionManager: {
      getSessionId: () => sessionId ?? "",
      getEntries: () => history.flatMap((entry, index) => {
        if (entry.kind === "entry") return [{ type: "custom", customType: entry.type, data: entry.data, id: `entry-${index}`, parentId: index > 0 ? `entry-${index - 1}` : null, timestamp: entry.at }];
        if (entry.kind === "message") return [{ type: "custom_message", ...entry.message, id: `message-${index}`, parentId: null, timestamp: entry.at }];
        return [];
      })
    }
  };
  async function emit(event, payload = {}) {
    for (const handler of handlers.get(event) ?? []) {
      try {
        await handler(payload, ctx);
      } catch (error) {
        console.error(`[parley] ${event} handler failed: ${error instanceof Error ? error.message : String(error)}`);
      }
    }
  }
  const { registerParleyExtension: registerParleyExtension2 } = await Promise.resolve().then(() => (init_index(), index_exports));
  registerParleyExtension2(pi, { resolveApp: () => app });
  const tool = tools.find((candidate) => candidate.name === "parley");
  if (!tool) throw new Error("Parley registered no parley tool");
  const instructions = [tool.promptSnippet, ...tool.promptGuidelines ?? []].filter(Boolean).join("\n");
  return {
    async begin(id) {
      if (sessionId) return;
      sessionId = id;
      stateFile = options2.stateFile(id);
      mkdirSync4(dirname5(stateFile), { recursive: true, mode: 448 });
      history = existsSync4(stateFile) ? readFileSync7(stateFile, "utf8").split("\n").filter(Boolean).flatMap((line) => {
        try {
          return [JSON.parse(line)];
        } catch {
          return [];
        }
      }) : [];
      sessionName = [...history].reverse().find((entry) => entry.kind === "name")?.name ?? options2.defaultName();
      app = options2.app?.();
      await emit("session_start", { type: "session_start", reason: history.length > 0 ? "resume" : "startup" });
    },
    started: () => sessionId !== void 0,
    tool: {
      name: tool.name,
      description: tool.description,
      // TypeBox schemas are JSON Schema; drop its symbol-keyed metadata.
      inputSchema: JSON.parse(JSON.stringify(tool.parameters))
    },
    instructions,
    async call(args, signal) {
      if (!sessionId) return { text: "Parley is still starting in this session; try again in a moment.", isError: true };
      const toolCallId = `host-${randomUUID7()}`;
      await emit("tool_execution_start", { toolCallId, toolName: tool.name, args });
      let result;
      try {
        result = await tool.execute(toolCallId, args, signal, void 0, ctx);
      } catch (error) {
        await emit("tool_execution_end", { toolCallId, toolName: tool.name, isError: true });
        return { text: `Parley failed: ${error instanceof Error ? error.message : String(error)}`, isError: true };
      }
      await emit("tool_result", { toolCallId, toolName: tool.name, input: args, content: result.content, details: result.details, isError: result.isError ?? false });
      await emit("tool_execution_end", { toolCallId, toolName: tool.name, isError: result.isError ?? false });
      const text = result.content.map((part) => part.text ?? "").filter(Boolean).join("\n");
      const failed = result.isError === true || result.details?.error === true;
      return { text, isError: failed };
    },
    setBusy(next) {
      if (next === busy) return;
      busy = next;
      void emit(next ? "agent_start" : "agent_end", { type: next ? "agent_start" : "agent_end", messages: [] });
    },
    name: () => sessionName,
    async stop() {
      await emit("session_shutdown", { type: "session_shutdown" });
    }
  };
}
var init_hosted_session = __esm({
  "hosts/hosted-session.ts"() {
  }
});

// hosts/connect.ts
var connect_exports = {};
__export(connect_exports, {
  PASS_THROUGH_ENV: () => PASS_THROUGH_ENV,
  connectHost: () => connectHost
});
import { execFile } from "node:child_process";
import { copyFileSync, existsSync as existsSync5, mkdirSync as mkdirSync5, readFileSync as readFileSync8, renameSync, writeFileSync as writeFileSync4 } from "node:fs";
import { homedir as homedir3 } from "node:os";
import { dirname as dirname6, join as join9 } from "node:path";
import { promisify } from "node:util";
function tomlString(value) {
  return JSON.stringify(value);
}
function writeCodexBlock(file, bin, connect) {
  const text = existsSync5(file) ? readFileSync8(file, "utf8") : "";
  const begin = text.indexOf(CODEX_BEGIN);
  const end = text.indexOf(CODEX_END);
  let rest2 = begin >= 0 && end > begin ? `${text.slice(0, begin).trimEnd()}
${text.slice(end + CODEX_END.length).replace(/^\n/, "")}` : text;
  if (/^\[mcp_servers\.parley[\].]/m.test(rest2)) {
    throw new Error(`${file} already has an [mcp_servers.parley] Parley didn't write; remove it (codex mcp remove parley) and connect again`);
  }
  if (connect) {
    rest2 = `${rest2.trimEnd()}

${[
      CODEX_BEGIN,
      "[mcp_servers.parley]",
      `command = "node"`,
      `args = [${[bin, "mcp", "codex"].map(tomlString).join(", ")}]`,
      `env_vars = [${PASS_THROUGH_ENV.map(tomlString).join(", ")}]`,
      `default_tools_approval_mode = "approve"`,
      "startup_timeout_sec = 30",
      CODEX_END
    ].join("\n")}
`;
  }
  mkdirSync5(dirname6(file), { recursive: true });
  if (existsSync5(file)) copyFileSync(file, `${file}.before-parley`);
  const tmp = `${file}.${process.pid}.tmp`;
  writeFileSync4(tmp, rest2.startsWith("\n") ? rest2.slice(1) : rest2, { mode: 384 });
  renameSync(tmp, file);
}
function hooksFileFor(host, env) {
  if (host === "claude") return join9(env.CLAUDE_CONFIG_DIR?.trim() || join9(homedir3(), ".claude"), "settings.json");
  return join9(env.CODEX_HOME?.trim() || join9(homedir3(), ".codex"), "hooks.json");
}
function quote(value) {
  return /^[A-Za-z0-9_./:-]+$/.test(value) ? value : `'${value.replace(/'/g, `'\\''`)}'`;
}
function withoutParley(hooks) {
  const kept = {};
  for (const [event, entries] of Object.entries(hooks)) {
    const remaining = entries.map((entry) => ({ ...entry, hooks: entry.hooks.filter((hook) => hook[MARK] === void 0) })).filter((entry) => entry.hooks.length > 0);
    if (remaining.length > 0) kept[event] = remaining;
  }
  return kept;
}
async function connectHost(host, connect, options2) {
  const env = options2.env ?? process.env;
  const bin = join9(options2.root, "bin", "parley.mjs");
  const lines = [];
  if (host === "claude" && !options2.skipMcp) {
    await run("claude", ["mcp", "remove", "--scope", "user", "parley"], { timeout: 3e4 }).catch(() => void 0);
    if (connect) await run("claude", ["mcp", "add", "--scope", "user", "parley", "--", "node", bin, "mcp", "claude"], { timeout: 3e4 });
  }
  if (host === "codex") {
    const configFile = options2.codexConfigFile ?? join9(env.CODEX_HOME?.trim() || join9(homedir3(), ".codex"), "config.toml");
    writeCodexBlock(configFile, bin, connect);
  }
  lines.push(`${connect ? "Registered" : "Removed"} Parley's MCP server ${connect ? "with" : "from"} ${host === "claude" ? "Claude Code" : "Codex"}.`);
  const file = options2.hooksFile ?? hooksFileFor(host, env);
  const existing = existsSync5(file) ? JSON.parse(readFileSync8(file, "utf8")) : {};
  const hooks = withoutParley(existing.hooks ?? {});
  if (connect) {
    for (const event of EVENTS) {
      hooks[event] = [...hooks[event] ?? [], {
        hooks: [{ type: "command", command: `node ${quote(bin)} hook ${event}`, timeout: 10, [MARK]: "v1" }]
      }];
    }
  }
  const next = { ...existing, hooks };
  if (Object.keys(hooks).length === 0) delete next.hooks;
  if (host === "claude") {
    const permissions = { ...existing.permissions ?? {} };
    const allow = (permissions.allow ?? []).filter((rule) => rule !== CLAUDE_TOOL);
    permissions.allow = connect ? [...allow, CLAUDE_TOOL] : allow;
    next.permissions = permissions;
  }
  mkdirSync5(dirname6(file), { recursive: true });
  if (existsSync5(file)) copyFileSync(file, `${file}.before-parley`);
  const tmp = `${file}.${process.pid}.tmp`;
  writeFileSync4(tmp, `${JSON.stringify(next, null, 2)}
`);
  renameSync(tmp, file);
  lines.push(connect ? `Added Parley's hooks to ${file} (backup: ${file}.before-parley).` : `Removed Parley's hooks from ${file}.`);
  if (connect && host === "codex") lines.push("Codex asks you to trust new hooks once: run /hooks in Codex and trust Parley's.");
  if (connect) lines.push(`Start a new ${host === "claude" ? "Claude Code" : "Codex"} session to use it.`);
  return lines;
}
var run, MARK, EVENTS, PASS_THROUGH_ENV, CLAUDE_TOOL, CODEX_BEGIN, CODEX_END;
var init_connect = __esm({
  "hosts/connect.ts"() {
    run = promisify(execFile);
    MARK = "_parley";
    EVENTS = ["SessionStart", "UserPromptSubmit", "PostToolUse", "Stop"];
    PASS_THROUGH_ENV = [
      "PI_PARLEY_SCOPE_ID",
      "PI_CODING_AGENT_DIR",
      "PI_PARLEY_LINK_BY_DEFAULT",
      "PI_PARLEY_LINK_LABEL",
      "FLIGHTDECK_WORKSPACE_NAME",
      "FLIGHTDECK_TAB_NAME",
      "FLIGHTDECK_TAB_ID",
      "FLIGHTDECK_PANE_ID",
      "HERDR_ENV",
      "HERDR_SOCKET_PATH",
      "HERDR_PANE_ID",
      "HERDR_TAB_ID",
      "HERDR_WORKSPACE_ID"
    ];
    CLAUDE_TOOL = "mcp__parley__parley";
    CODEX_BEGIN = "# >>> parley: managed by `parley connect codex`; `parley disconnect codex` removes it";
    CODEX_END = "# <<< parley";
  }
});

// hosts/mcp.ts
import { createInterface } from "node:readline";
function serveMcp(tools, options2) {
  const write = (message) => process.stdout.write(`${JSON.stringify({ jsonrpc: "2.0", ...message })}
`);
  const running = /* @__PURE__ */ new Map();
  const handle = async (message) => {
    const { id, method, params = {} } = message;
    const reply = (result) => {
      if (id !== void 0) write({ id, result });
    };
    const fail = (code, text) => {
      if (id !== void 0) write({ id, error: { code, message: text } });
    };
    switch (method) {
      case "initialize": {
        const client = params.clientInfo ?? {};
        reply({
          protocolVersion: typeof params.protocolVersion === "string" ? params.protocolVersion : "2025-06-18",
          capabilities: { tools: {}, ...options2.capabilities ?? {} },
          serverInfo: { name: options2.name, version: options2.version },
          ...options2.instructions ? { instructions: options2.instructions } : {}
        });
        queueMicrotask(() => options2.onReady?.(client));
        return;
      }
      case "ping":
        reply({});
        return;
      case "tools/list":
        reply({ tools: (await tools()).map(({ name, description, inputSchema }) => ({ name, description, inputSchema })) });
        return;
      case "tools/call": {
        const tool = (await tools()).find((candidate) => candidate.name === params.name);
        if (!tool) {
          fail(-32602, `Unknown tool: ${String(params.name)}`);
          return;
        }
        const controller = new AbortController();
        if (id !== void 0) running.set(id, controller);
        try {
          const meta = params._meta && typeof params._meta === "object" ? params._meta : void 0;
          const outcome = await tool.call(params.arguments ?? {}, controller.signal, meta);
          reply({ content: [{ type: "text", text: outcome.text }], isError: outcome.isError });
        } catch (error) {
          reply({ content: [{ type: "text", text: error instanceof Error ? error.message : String(error) }], isError: true });
        } finally {
          if (id !== void 0) running.delete(id);
        }
        return;
      }
      case "notifications/cancelled": {
        const requestId = params.requestId;
        if (requestId !== void 0) running.get(requestId)?.abort();
        return;
      }
      default:
        if (method?.startsWith("notifications/")) return;
        fail(-32601, `Method not found: ${String(method)}`);
    }
  };
  const lines = createInterface({ input: process.stdin });
  lines.on("line", (line) => {
    if (!line.trim()) return;
    let message;
    try {
      message = JSON.parse(line);
    } catch {
      return;
    }
    void handle(message);
  });
  lines.on("close", () => options2.onClose?.());
  return { notify: (method, params) => write({ method, params }) };
}
var init_mcp = __esm({
  "hosts/mcp.ts"() {
  }
});

// hosts/serve.ts
var serve_exports = {};
__export(serve_exports, {
  serveHost: () => serveHost
});
import { basename as basename2, join as join10 } from "node:path";
function forgetInheritedIdentity(env) {
  for (const key of Object.keys(env)) {
    if (key === "PI_PARLEY_SESSION_ID" || key === "PI_PARLEY_STABLE_ID" || key.startsWith("PI_SUBAGENT_") || key === "PI_CLIENT_SESSION_ID") delete env[key];
  }
}
function defaultName(host, cwd, env) {
  const workspace = env.FLIGHTDECK_WORKSPACE_NAME?.trim();
  const tab = env.FLIGHTDECK_TAB_NAME?.trim();
  if (workspace && tab) return `${workspace}:${tab}`;
  return `${basename2(cwd) || "home"}:${host.id}`;
}
async function serveHost(host) {
  console.log = (...args) => console.error(...args);
  forgetInheritedIdentity(process.env);
  const hostPid = process.ppid;
  const cwd = host.cwd();
  let sessionId = host.initialSessionId() ?? takeEarlySessionId(hostPid, cwd);
  const pending = [];
  let wakeTarget = sessionId;
  const deliver = (delivery) => {
    if (host.wake && wakeTarget) {
      void host.wake(delivery.text, { sessionId: wakeTarget }).then((accepted) => {
        if (!accepted) pending.push(delivery);
      });
      return;
    }
    pending.push(delivery);
  };
  const takePending = () => pending.splice(0).map((delivery) => delivery.text).join("\n\n---\n\n");
  const agentDir = getAgentDirPath(process.env);
  const session = await createHostedSession({
    cwd,
    defaultName: () => defaultName(host, cwd, process.env),
    app: () => host.app(process.env),
    stateFile: (id) => join10(getParleyDirPath(agentDir), "hosts", host.id, `${id.replace(/[^A-Za-z0-9_.-]/g, "_")}.jsonl`),
    deliver
  });
  const join_ = (id) => {
    sessionId ??= id;
    control.claim(sessionId);
    void session.begin(sessionId);
  };
  let released = false;
  const onHook = async ({ event, payload, hostEnv }) => {
    const hookSession = typeof payload.session_id === "string" && payload.session_id ? payload.session_id : void 0;
    if (event === RELEASE_EVENT) {
      if (!hookSession || hookSession !== sessionId || released) return {};
      released = true;
      control.unclaim();
      const handed = pending.splice(0).map((delivery) => delivery.text);
      if (session.started()) await session.stop().catch(() => {
      });
      return { output: { released: true, pending: handed } };
    }
    if (released) return {};
    if (!session.started() && hostEnv && !sharedParent) {
      for (const [key, value] of Object.entries(hostEnv)) if (PASS_THROUGH_ENV.includes(key) && !process.env[key]) process.env[key] = value;
    }
    if (hookSession) {
      wakeTarget = hookSession;
      if (!session.started()) join_(hookSession);
    }
    if (event === "UserPromptSubmit") session.setBusy(true);
    if (event === "Stop") session.setBusy(false);
    if (!BOUNDARY_EVENTS.has(event) || pending.length === 0) return {};
    const output = host.hookOutput(event, takePending());
    if (output && event === "Stop") session.setBusy(true);
    return output ? { output } : {};
  };
  const sharedParent = host.sharedParent?.() ?? false;
  const control = serveControl(hostPid, cwd, onHook);
  if (sessionId) join_(sessionId);
  const callSession = async (meta) => {
    const exact = host.sessionOfCall?.(meta);
    if (!exact || released) return;
    wakeTarget = exact;
    if (session.started() && sessionId === exact) return;
    if (session.started()) return;
    const handed = await askSiblingToRelease(hostPid, exact);
    for (const text of handed?.output?.pending ?? []) pending.push({ text, wake: false });
    sessionId = exact;
    control.claim(exact);
    await session.begin(exact);
  };
  serveMcp(() => [{ ...session.tool, call: async (args, signal, meta) => {
    await callSession(meta);
    return session.call(args, signal);
  } }], {
    name: "parley",
    version: host.version,
    instructions: "Parley: conversations with other agent sessions (Pi, Oh My Pi, Claude Code, Codex), on this computer and linked ones. Use the parley tool to list, send, ask and reply.",
    ...host.capabilities ? { capabilities: host.capabilities } : {},
    onClose: () => void shutdown()
  });
  let stopping = false;
  async function shutdown() {
    if (stopping) return;
    stopping = true;
    control.close();
    if (session.started()) await session.stop().catch(() => {
    });
    process.exit(0);
  }
  process.on("SIGTERM", () => void shutdown());
  process.on("SIGINT", () => void shutdown());
}
var BOUNDARY_EVENTS;
var init_serve = __esm({
  "hosts/serve.ts"() {
    init_paths();
    init_control();
    init_hosted_session();
    init_connect();
    init_mcp();
    BOUNDARY_EVENTS = /* @__PURE__ */ new Set(["UserPromptSubmit", "PostToolUse", "Stop"]);
  }
});

// hosts/claude.ts
var claude_exports = {};
__export(claude_exports, {
  claudeHost: () => claudeHost
});
import { randomBytes as randomBytes2 } from "node:crypto";
import net6 from "node:net";
function claudeHost(env = process.env, version2 = "0") {
  return {
    id: "claude",
    version: version2,
    initialSessionId: () => env.CLAUDE_CODE_SESSION_ID?.trim() || void 0,
    cwd: () => env.CLAUDE_PROJECT_DIR?.trim() || process.cwd(),
    wake: (text) => sendToInbox(env, text),
    // Claude Desktop's Code tab says so in CLAUDE_CODE_ENTRYPOINT ("claude-desktop…").
    app: (current) => /desktop/i.test(current.CLAUDE_CODE_ENTRYPOINT ?? "") ? "Claude Desktop" : sessionPlace(current) ? `Claude Code in ${sessionPlace(current)}` : "Claude Code",
    hookOutput: (event, text) => event === "Stop" ? { decision: "block", reason: text } : { hookSpecificOutput: { hookEventName: event, additionalContext: text } }
  };
}
function sendToInbox(env, text) {
  const path2 = env.CLAUDE_CODE_MESSAGING_SOCKET?.trim();
  if (!path2) return Promise.resolve(false);
  return new Promise((resolve4) => {
    const socket = net6.connect(path2);
    let settled = false;
    const done = (accepted) => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      socket.destroy();
      resolve4(accepted);
    };
    const timer = setTimeout(() => done(true), 3e3);
    socket.on("connect", () => {
      const frames = [
        ...env.CLAUDE_CODE_MESSAGING_TOKEN ? [{ type: "auth", token: env.CLAUDE_CODE_MESSAGING_TOKEN }] : [],
        { type: "user", message: { role: "user", content: text }, priority: "next", msgV: 1, msg_id: `cc-msg-${randomBytes2(16).toString("hex")}` }
      ];
      socket.end(frames.map((frame) => `${JSON.stringify(frame)}
`).join(""), () => done(true));
    });
    socket.on("error", () => done(false));
  });
}
var init_claude = __esm({
  "hosts/claude.ts"() {
    init_index();
  }
});

// hosts/codex.ts
var codex_exports = {};
__export(codex_exports, {
  codexHost: () => codexHost
});
import { execFile as execFile2, execFileSync as execFileSync2 } from "node:child_process";
import { promisify as promisify2 } from "node:util";
function codexHost(env = process.env, version2 = "0") {
  let canQueue;
  const codex = env.PI_PARLEY_CODEX_BIN?.trim() || "codex";
  return {
    id: "codex",
    version: version2,
    initialSessionId: () => env.CODEX_THREAD_ID?.trim() || env.CODEX_SESSION_ID?.trim() || resumedThread(),
    cwd: () => process.cwd(),
    async wake(text, { sessionId }) {
      canQueue ??= run2(codex, ["queue", "--help"], { timeout: 1e4 }).then(() => true, () => false);
      if (!await canQueue) return false;
      try {
        await run2(codex, ["queue", "--thread", sessionId, "--message", text], { timeout: 15e3 });
        return true;
      } catch {
        return false;
      }
    },
    sharedParent: () => /\bapp-server\b/.test(parentCommand()),
    // Codex puts the thread in every tool call's _meta.
    sessionOfCall: (meta) => typeof meta?.threadId === "string" && meta.threadId ? meta.threadId : void 0,
    app: (current) => startedByChatGptApp() ? "Codex in the ChatGPT app" : sessionPlace(current) ? `Codex in ${sessionPlace(current)}` : "Codex",
    hookOutput: (event, text) => event === "Stop" ? { decision: "block", reason: text } : { hookSpecificOutput: { hookEventName: event, additionalContext: text } }
  };
}
function resumedThread() {
  try {
    const args = execFileSync2("ps", ["-o", "args=", "-p", String(process.ppid)], { encoding: "utf8", timeout: 2e3 });
    return /\bresume\s+([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})\b/.exec(args)?.[1];
  } catch {
    return void 0;
  }
}
function parentCommand() {
  try {
    return execFileSync2("ps", ["-o", "command=", "-p", String(process.ppid)], { encoding: "utf8", timeout: 2e3 });
  } catch {
    return "";
  }
}
function startedByChatGptApp() {
  return /ChatGPT\.app\//.test(parentCommand());
}
var run2;
var init_codex = __esm({
  "hosts/codex.ts"() {
    init_index();
    run2 = promisify2(execFile2);
  }
});

// hosts/cli.ts
init_bundle();
import { readFileSync as readFileSync9 } from "node:fs";
import { dirname as dirname7, join as join11 } from "node:path";
import { fileURLToPath as fileURLToPath4 } from "node:url";
var root = join11(dirname7(fileURLToPath4(import.meta.url)), "..");
var version = bundledBuild?.packageVersion ?? JSON.parse(readFileSync9(join11(root, "package.json"), "utf8")).version;
var [command, initialTarget, ...rest] = process.argv.slice(2);
var target = initialTarget;
async function main() {
  switch (command) {
    case "mcp": {
      const { serveHost: serveHost2 } = await Promise.resolve().then(() => (init_serve(), serve_exports));
      if (target === "auto") target = process.env.CLAUDECODE === "1" || process.env.CLAUDE_CODE_SESSION_ID ? "claude" : "codex";
      if (target === "claude") {
        const { claudeHost: claudeHost2 } = await Promise.resolve().then(() => (init_claude(), claude_exports));
        return serveHost2(claudeHost2(process.env, version));
      }
      if (target === "codex") {
        const { codexHost: codexHost2 } = await Promise.resolve().then(() => (init_codex(), codex_exports));
        return serveHost2(codexHost2(process.env, version));
      }
      break;
    }
    case "hook": {
      const { sendHook: sendHook2 } = await Promise.resolve().then(() => (init_control(), control_exports));
      const input = await new Promise((resolve4) => {
        let text = "";
        process.stdin.on("data", (d) => {
          text += d;
        });
        process.stdin.on("end", () => resolve4(text));
      });
      let payload = {};
      try {
        payload = JSON.parse(input || "{}");
      } catch {
      }
      const event = target ?? String(payload.hook_event_name ?? "");
      const { PASS_THROUGH_ENV: PASS_THROUGH_ENV2 } = await Promise.resolve().then(() => (init_connect(), connect_exports));
      const hostEnv = Object.fromEntries(PASS_THROUGH_ENV2.flatMap((key) => process.env[key] ? [[key, process.env[key]]] : []));
      const response = await sendHook2({ event, payload, hostEnv });
      if (response?.output) process.stdout.write(`${JSON.stringify(response.output)}
`);
      return;
    }
    case "connect":
    case "disconnect": {
      const { connectHost: connectHost2 } = await Promise.resolve().then(() => (init_connect(), connect_exports));
      if (target === "claude" || target === "codex") {
        const lines = await connectHost2(target, command === "connect", { root, args: rest });
        console.log(lines.join("\n"));
        return;
      }
      break;
    }
  }
  console.error("usage: parley connect|disconnect claude|codex \xB7 parley mcp claude|codex \xB7 parley hook <event>");
  process.exitCode = 2;
}
main().catch((error) => {
  console.error(`parley: ${error instanceof Error ? error.message : String(error)}`);
  process.exitCode = command === "hook" ? 0 : 1;
});
