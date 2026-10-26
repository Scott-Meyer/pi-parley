// In a herdr pane, Pi's session name and herdr's names stay in step, the way
// FlightDeck's tabs and Pi's session names do: the session is named
// "<workspace>:<pane or tab>", which Parley uses as its presence name.
//
// - herdr → Pi: renaming the workspace, tab or pane in herdr renames the session.
// - Pi → herdr: renaming the session (/name, Parley's rename, an alias) renames
//   this pane's label, or the tab when this pane is the tab's only one, so a
//   rename never relabels another pane's tab.
// - A resumed session keeps its name; herdr follows it.
// - Subagents inherit the pane's environment but don't rename anything; they
//   get "<workspace>:<place>:<own name>".
//
// Best effort throughout: herdr being unreachable never disturbs Pi.
import type { ExtensionAPI } from "@mariozechner/pi-coding-agent";
import { herdrRequest, herdrSubscribe, type HerdrPane, type HerdrTab, type HerdrWorkspace } from "./socket.ts";

const CLAIM_EVENT = "pi-parley:herdr-names:v1";
const PANE_LABEL_POLL_MS = 15_000; // herdr pushes no event for pane label changes
const RESUBSCRIBE_MS = 3_000;

export interface HerdrNamesOptions {
  env?: NodeJS.ProcessEnv;
  /** Whether session_info_changed reaches extensions on this host; otherwise names are polled. */
  sessionRenamesArrive?: boolean;
}

/** Leading "π - " style markers, as terminal titles carry. */
function cleanName(name: string): string {
  return name.replace(/^(?:[π\u03C0]\s*[-–—:]\s*|pi\s+[-–—:]\s+)/i, "").trim();
}

export function registerHerdrNames(pi: ExtensionAPI, options: HerdrNamesOptions = {}): void {
  const env = options.env ?? process.env;
  const socketPath = env.HERDR_SOCKET_PATH?.trim();
  const paneId = env.HERDR_PANE_ID?.trim();
  // FlightDeck names its own tabs' sessions, even inside herdr.
  if (env.HERDR_ENV !== "1" || !socketPath || !paneId || env.FLIGHTDECK_TAB_ID?.trim()) return;

  let claimed = false;
  pi.events.emit(CLAIM_EVENT, { claim: () => (claimed = true) });
  if (claimed) return;
  const releaseClaim = pi.events.on(CLAIM_EVENT, (payload) => (payload as { claim?: () => void } | undefined)?.claim?.());

  const isChild = env.PI_SUBAGENT_CHILD === "1" || Boolean(env.PI_SUBAGENT_CHILD_AGENT?.trim());
  let pane: HerdrPane | undefined;
  let tab: HerdrTab | undefined;
  let workspace: HerdrWorkspace | undefined;
  let lastApplied: string | undefined;
  let stopped = false;
  let subscription: { close(): void } | undefined;
  const timers = new Set<NodeJS.Timeout>();
  const later = (fn: () => void, ms: number, repeat = false) => {
    const timer = repeat ? setInterval(fn, ms) : setTimeout(() => { timers.delete(timer); fn(); }, ms);
    timer.unref?.();
    timers.add(timer);
  };

  const place = () => cleanName(pane?.label || "") || cleanName(tab?.label || "") || paneId;
  const prefix = () => `${workspace?.label || "herdr"}:${place()}`;

  /** The place a session name stands for: strips only our "<workspace>:" qualifier. */
  const placeFrom = (sessionName: string) => {
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
      const paneResult = await herdrRequest<{ pane: HerdrPane }>(socketPath!, "pane.get", { pane_id: paneId });
      const [tabResult, workspaceResult] = await Promise.all([
        herdrRequest<{ tab: HerdrTab }>(socketPath!, "tab.get", { tab_id: paneResult.pane.tab_id }),
        herdrRequest<{ workspace: HerdrWorkspace }>(socketPath!, "workspace.get", { workspace_id: paneResult.pane.workspace_id }),
      ]);
      if (stopped) return;
      pane = paneResult.pane;
      tab = tabResult.tab;
      workspace = workspaceResult.workspace;
      if (applyNames) apply();
    } catch { /* herdr not reachable now; the subscription retries */ }
  }

  /** The session was renamed in Pi: name this pane (or its tab) after it. */
  async function renamedInPi(name: string | undefined) {
    const raw = name?.trim();
    if (!raw || raw === lastApplied || !workspace) return;
    if (isChild) { apply(); return; }
    const next = placeFrom(raw);
    if (!next || next === place()) { apply(); return; }
    try {
      // A tab shared with other panes keeps its label; this pane gets its own.
      if (pane?.label || (tab?.pane_count ?? 1) > 1) {
        const result = await herdrRequest<{ pane: HerdrPane }>(socketPath!, "pane.rename", { pane_id: paneId, label: next });
        pane = result.pane;
      } else if (tab) {
        const result = await herdrRequest<{ tab: HerdrTab }>(socketPath!, "tab.rename", { tab_id: tab.tab_id, label: next });
        tab = result.tab ?? { ...tab, label: next };
      }
    } catch { /* keep Pi's name; herdr catches up on the next refresh */ }
    apply();
  }

  function subscribe() {
    if (stopped) return;
    subscription = herdrSubscribe(socketPath!, ["workspace.renamed", "tab.renamed", "pane.moved", "pane.updated"], {
      onReady: () => void refresh(),
      onEvent: ({ event, data }) => {
        if (event === "workspace_renamed" && data.workspace_id === workspace?.workspace_id && typeof data.label === "string") {
          workspace = { ...workspace!, label: data.label };
          apply();
        } else if (event === "tab_renamed" && data.tab_id === tab?.tab_id && typeof data.label === "string") {
          tab = { ...tab!, label: data.label };
          apply();
        } else if (data.pane_id === paneId) {
          void refresh();
        }
      },
      onClose: () => { if (!stopped) later(subscribe, RESUBSCRIBE_MS); },
    });
  }

  pi.on("session_start", async () => {
    const existing = pi.getSessionName()?.trim();
    // A resumed session already has a name someone chose; herdr takes it.
    const resumed = !isChild && Boolean(existing);
    await refresh(!resumed);
    if (resumed) await renamedInPi(existing);
    subscribe();
    later(() => void refresh(), PANE_LABEL_POLL_MS, true);
    if (options.sessionRenamesArrive === false) {
      later(() => { const current = pi.getSessionName(); if (current !== lastApplied) void renamedInPi(current); }, 1_000, true);
    }
  });
  // Upstream 0.73.1 emits this event without declaring it; bound because OMP's `on` needs its receiver.
  const onSessionInfoChanged = pi.on.bind(pi) as unknown as (event: "session_info_changed", handler: (event: { name?: string }) => void) => void;
  onSessionInfoChanged("session_info_changed", (event) => void renamedInPi(event?.name));
  pi.on("session_shutdown", () => {
    stopped = true;
    subscription?.close();
    for (const timer of timers) { clearTimeout(timer); clearInterval(timer); }
    timers.clear();
    releaseClaim?.();
  });
}
