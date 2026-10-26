// The part of pi-tui's fullscreen mouse contract Parley uses. Older hosts have
// no mouse input and never call handleMouse; newer ones pass these events.
export interface TuiMouseEvent {
  readonly type: "press" | "release" | "move" | "drag" | "click" | "wheel";
  readonly button: "left" | "middle" | "right" | "none";
}
export interface TuiMouseEventResult {
  handled?: boolean;
  render?: boolean;
}
