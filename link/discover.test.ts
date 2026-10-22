// Which ssh processes say "this computer works on that one", and how to get
// there again. Cases are shaped after a real developer machine's process list.
import test from "node:test";
import assert from "node:assert/strict";
import { destinationFromArgv } from "./discover.ts";

const argv = (line: string) => line.split(" ");

test("a session to a computer is found with only the options that say where and as whom", () => {
  assert.deepEqual(destinationFromArgv(argv("ssh build-vm")), { options: [], host: "build-vm" });
  assert.deepEqual(destinationFromArgv(argv("/usr/bin/ssh -tt -A -L 8080:localhost:80 -p2222 me@box tmux attach")),
    { options: ["-p", "2222"], host: "me@box" }, "forwards, terminals, agents and the remote command aren't repeated");
  assert.deepEqual(destinationFromArgv(argv("ssh -o ControlMaster=auto -o ControlPersist=10m -oUser=bob -J jump -o BatchMode=yes vm")),
    { options: ["-o", "User=bob", "-J", "jump"], host: "vm" });
  // Tools write throwaway configs and control sockets; their host is still reachable the person's own way.
  assert.deepEqual(destinationFromArgv(argv("ssh -F /var/folders/x/T/sw-tunnel-1/config -S /var/folders/x/T/sw-tunnel-1/control -N workspace-5")),
    { options: [], host: "workspace-5" });
  assert.deepEqual(destinationFromArgv(argv("ssh -F /Users/me/.config/sw/tunnel-ssh-config -T workspace-4")),
    { options: ["-F", "/Users/me/.config/sw/tunnel-ssh-config"], host: "workspace-4" }, "a lasting config file is kept");
  assert.deepEqual(destinationFromArgv(argv("C:\\Windows\\System32\\OpenSSH\\ssh.exe penguin")), { options: [], host: "penguin" });
});

test("ssh used as a pipe or a control channel isn't a computer someone works on", () => {
  for (const line of [
    "ssh git@github.com git-upload-pack 'org/repo.git'",
    "ssh deploy@host git-receive-pack 'repo.git'",
    "ssh host rsync --server -vlogDtpre.iLsfxC . /srv",
    "ssh -s host sftp",
    "ssh -W [192.168.1.125]:22 home-gateway",
    "ssh -O check build-vm",
    "ssh -G build-vm",
    "ssh: /Users/me/.ssh/cm-abc123 [mux]",
    "sshd: me@pts/0",
    "ssh",
  ]) assert.equal(destinationFromArgv(argv(line)), undefined, line);
});
