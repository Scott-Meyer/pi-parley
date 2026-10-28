# Contributing to Parley

Thanks for helping. Issues are the place for bugs, questions and ideas. For anything bigger than a small fix, open an issue first so we can agree on the shape before you build it.

## Running it

```bash
npm ci
npm test               # the suite, four files at a time, in an environment of its own
npm run typecheck
npm run test:host-compat   # boots the packed extension under the supported Pi versions
```

To try a change in Pi, install the checkout (`pi install /path/to/pi-parley`) and `/reload`. Broker changes also need a broker restart; see "Updating Parley" in the README.

## What a change needs

- **Behaviour people can rely on, tested through the boundary they use.** Most tests drive the `parley` tool, the `/parley` command or real brokers, rather than internals. A good test reads as "given this, Parley does that", and keeps passing if the implementation is rewritten.
- **Words written for the reader.** Tool results, receipts and docs are read by people and by models; say what happened and what it means, in plain language.
- **No surprises across versions.** Brokers and clients from different releases meet in the field. Wire changes are negotiated features, and older peers keep working.
- **A CHANGELOG entry** under "Unreleased" for anything a user would notice.

## Releases

Maintainers release by tagging `vX.Y.Z` on `main`; GitHub Actions validates and publishes to npm.

`npm version <x.y.z> --no-git-tag-version` also rebuilds the federation facade (`dist/`) and the Claude Code and Codex plugin (`plugins/parley`, installed from this repository as-is), so they're committed with the release. CI fails if either doesn't match its source. After a change to anything the plugin bundles, run `npm run build:plugin` and commit the result.
