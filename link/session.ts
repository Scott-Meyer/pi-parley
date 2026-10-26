// Cross-computer links as one Pi session sees them: the `link`/`unlink` tool
// actions, `/parley link …`, and staying connected to this computer's keeper
// while links are configured, so the keeper runs while sessions do and the
// person hears when a computer becomes linked.
import { connectLinkKeeper, describeLinks, linksConfigured, type LinkCommand, type LinkKeeperConnection } from "./client.ts";
import type { LinkHostStatus, LinkStatus } from "./protocol.ts";

export interface LinkToolParams {
  readonly action: "link" | "unlink";
  readonly host?: string;
  readonly recheck?: boolean;
  readonly discover?: boolean;
}

export interface LinkOutcome {
  readonly ok: boolean;
  readonly text: string;
  readonly status?: LinkStatus;
  readonly host?: LinkHostStatus;
}

const CONFIG_CHECK_MS = 15_000;

export class SessionLinks {
  private connection: LinkKeeperConnection | undefined;
  private connecting: Promise<LinkKeeperConnection> | undefined;
  private timer: NodeJS.Timeout | undefined;
  private active = false;

  /** notify reaches the person only (UI), never the model. byDefault says whether
   * this session links computers when nobody has said otherwise. */
  constructor(private readonly notify: (message: string) => void, private readonly byDefault: () => boolean = () => false) {}

  /** Stay connected to the keeper whenever this computer links with others. */
  start(): void {
    this.active = true;
    const tick = () => {
      if (!this.active) return;
      if (!this.connection && !this.connecting && linksConfigured(process.env, this.byDefault())) void this.connect().catch(() => {});
    };
    tick();
    this.timer = setInterval(tick, CONFIG_CHECK_MS);
    this.timer.unref();
  }

  stop(): void {
    this.active = false;
    clearInterval(this.timer);
    this.connection?.close();
    this.connection = undefined;
  }

  private connect(): Promise<LinkKeeperConnection> {
    if (this.connection) return Promise.resolve(this.connection);
    this.connecting ??= connectLinkKeeper({ discoverByDefault: this.byDefault() }).then((connection) => {
      if (!this.active) { connection.close(); throw new Error("session ended"); }
      this.connection = connection;
      connection.onEvent((event) => {
        if (event.event === "linked") this.notify(`Parley linked ${event.label}`);
      });
      void connection.closed.then(() => { if (this.connection === connection) this.connection = undefined; });
      return connection;
    }).finally(() => { this.connecting = undefined; });
    return this.connecting;
  }

  async run(command: LinkCommand): Promise<LinkOutcome> {
    let connection: LinkKeeperConnection;
    try {
      connection = await this.connect();
    } catch (error) {
      return { ok: false, text: `Couldn't reach Parley's link keeper: ${error instanceof Error ? error.message : String(error)}` };
    }
    const response = await connection.request(command, { timeoutMs: 90_000 });
    if (!response.ok) {
      return { ok: false, text: response.status ? `${response.error}\n\n${describeLinks(response.status)}` : response.error, status: response.status };
    }
    const lead = response.host ? hostSentence(response.host) : undefined;
    return {
      ok: true,
      text: lead ? `${lead}\n\n${describeLinks(response.status)}` : describeLinks(response.status),
      status: response.status,
      ...(response.host ? { host: response.host } : {}),
    };
  }

  /** The parley tool's link/unlink actions. */
  tool(params: LinkToolParams): Promise<LinkOutcome> {
    const host = params.host?.trim() || undefined;
    if (params.action === "unlink") {
      if (!host) return Promise.resolve({ ok: false, text: "unlink needs host." });
      return this.run({ type: "remove", destination: host });
    }
    return (async () => {
      let last: LinkOutcome | undefined;
      if (typeof params.discover === "boolean") {
        last = await this.run({ type: "discover", on: params.discover });
        if (!last.ok) return last;
      }
      if (host && !params.recheck) return this.run({ type: "add", destination: host });
      if (params.recheck) return this.run({ type: "recheck", ...(host ? { destination: host } : {}) });
      return last ?? this.run({ type: "status" });
    })();
  }

  /** `/parley link [on|off|add <host>|recheck [host]|remove <host>]`. */
  command(args: string): Promise<LinkOutcome> {
    const [verb = "", ...rest] = args.trim().split(/\s+/).filter(Boolean);
    const target = rest.join(" ") || undefined;
    switch (verb.toLowerCase()) {
      case "": case "status": return this.run({ type: "status" });
      case "on": return this.run({ type: "discover", on: true });
      case "off": return this.run({ type: "discover", on: false });
      case "add": return target ? this.run({ type: "add", destination: target }) : Promise.resolve({ ok: false, text: LINK_USAGE });
      case "recheck": return this.run({ type: "recheck", ...(target ? { destination: target } : {}) });
      case "remove": return target ? this.run({ type: "remove", destination: target }) : Promise.resolve({ ok: false, text: LINK_USAGE });
      default: return Promise.resolve({ ok: false, text: LINK_USAGE });
    }
  }
}

export const LINK_USAGE = "Usage: /parley link [on | off | add <host> | recheck [host] | remove <host>]. <host> is what you'd type after ssh.";

function hostSentence(host: LinkHostStatus): string {
  switch (host.state) {
    case "linked": return `${host.label} is linked; its sessions appear here as ${host.label}:…`;
    case "checking": return `${host.label} is still being checked.`;
    case "same-computer": return `${host.label} is ${host.detail ?? "a computer already listed"}.`;
    case "no-parley": return `${host.label} is reachable, but Parley has never run there.`;
    default: return `${host.label}: ${host.state}${host.detail ? ` — ${host.detail}` : ""}. Parley keeps trying.`;
  }
}
