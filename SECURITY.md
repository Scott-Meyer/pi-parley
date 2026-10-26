# Security

Please report security problems privately, through GitHub's [private vulnerability reporting](https://github.com/Scott-Meyer/pi-parley/security/advisories/new), not in a public issue. Include what you found, how to reproduce it, and what it lets someone do. You'll get a reply, and a fix will be released before details are made public.

Parley's trust model, briefly: whatever can open a broker's local endpoint (its Unix socket, named pipe, or authenticated loopback port) can act as a session on that computer. Linking computers uses your own `ssh`, and a linked computer's sessions can message yours. The README's "Other computers" and "Design Decisions" sections describe the boundaries.
