// An OMP session on "laptop" links "box" and sees box's OMP session as box:omp-box.
import { runProbe } from "./probe.mjs";

const args = process.argv.slice(2);
const [laptopHome, boxHome] = args.slice(4);
await runProbe("OMP link over SSH", args, async ({ start, until }) => {
  const box = await start("box", boxHome);
  const registration = box.prompt("OMP-BOX register");
  await until(() => box.completed.has(registration) && box.results.some((text) => text.includes("Session: omp-box")),
    30000, "box's session to register");
  const laptop = await start("laptop", laptopHome);
  const link = laptop.prompt("OMP-LINK add");
  await until(() => laptop.completed.has(link) && laptop.results.some((text) => text.includes("box is linked")),
    60000, "laptop to link box");
  const list = laptop.prompt("OMP-LINK list");
  await until(() => laptop.completed.has(list) && laptop.results.some((text) => text.includes("• box:omp-box")),
    30000, "box's session to appear on laptop as box:omp-box");
});
