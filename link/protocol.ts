// What a Pi session and this computer's link keeper say to each other.
//
// The keeper is the one owner of this computer's cross-computer links. Sessions
// connect to it to make requests and, by staying connected, keep it alive. A
// connection hears "linked" events: the first time a computer becomes linked
// after it was added or found.

/** Where a computer stands, from this computer's point of view. */
export type LinkHostState =
  | "checking" // connecting over ssh, or attempting the first link
  | "linked" // this computer's broker and that one's are linked
  | "waiting" // reachable, link not up yet; detail says why, retrying
  | "unreachable" // ssh couldn't connect; detail has ssh's words, retrying
  | "no-parley" // reachable, but Parley has never run there; not retried until a recheck
  | "same-computer"; // another spelling of this computer or of one listed already

export interface LinkHostStatus {
  /** How to name it in requests: the destination as given to ssh. */
  readonly destination: string;
  /** What its sessions are prefixed with here. */
  readonly label: string;
  /** Added by hand, or found among this computer's ssh connections. */
  readonly via: "added" | "found";
  /** Remembered hosts are kept linked across restarts until removed. */
  readonly remembered: boolean;
  readonly state: LinkHostState;
  readonly detail?: string;
  readonly since: string;
}

export interface LinkStatus {
  /** Whether computers this one has ssh connections to are found and linked automatically. */
  readonly discover: boolean;
  /** This computer's label, as other computers show it. */
  readonly label: string;
  readonly hosts: readonly LinkHostStatus[];
  /** Links between two other computers, carried through this one. */
  readonly relays: readonly { readonly between: readonly [string, string]; readonly state: "linked" | "waiting"; readonly detail?: string }[];
}

export type LinkRequest =
  | { readonly type: "status"; readonly requestId: string }
  /** Remember one computer and link it now; answers once it is linked or why not. */
  | { readonly type: "add"; readonly requestId: string; readonly destination: string }
  /** Check again now: one computer, or every computer and this one's ssh connections. */
  | { readonly type: "recheck"; readonly requestId: string; readonly destination?: string }
  /** Unlink and forget one computer; it isn't found automatically again until added. */
  | { readonly type: "remove"; readonly requestId: string; readonly destination: string }
  | { readonly type: "discover"; readonly requestId: string; readonly on: boolean };

export type LinkResponse =
  | { readonly type: "result"; readonly requestId: string; readonly ok: true; readonly status: LinkStatus; readonly host?: LinkHostStatus }
  | { readonly type: "result"; readonly requestId: string; readonly ok: false; readonly error: string; readonly status?: LinkStatus };

export type LinkEvent = { readonly type: "event"; readonly event: "linked"; readonly label: string; readonly via: "added" | "found" };
