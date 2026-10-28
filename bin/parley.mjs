#!/usr/bin/env node
// Runs hosts/cli.ts with tsx's loader in this process, so MCP stdio stays direct.
import { register } from "tsx/esm/api";
register();
await import("../hosts/cli.ts");
