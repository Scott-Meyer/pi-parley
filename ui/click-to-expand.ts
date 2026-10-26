import type { Component } from "@mariozechner/pi-tui";
import type { TuiMouseEvent, TuiMouseEventResult } from "./mouse.ts";

/** Renders its child unchanged; a left click (fullscreen mode) calls onClick. */
export class ClickToExpand implements Component {
  constructor(private readonly child: Component, private readonly onClick: () => void) {}

  render(width: number): string[] {
    return this.child.render(width);
  }

  invalidate(): void {
    this.child.invalidate?.();
  }

  handleMouse(event: TuiMouseEvent): TuiMouseEventResult | undefined {
    if (event.type !== "click" || event.button !== "left") return undefined;
    this.onClick();
    return { handled: true, render: true };
  }
}
