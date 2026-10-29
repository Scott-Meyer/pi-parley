/** The build identity of the Parley sources being bundled, as a running broker reports it. */
export function parleyBundleIdentity(): { readonly packageVersion: string; readonly sourceId: string };
/** esbuild's `define` that stamps that identity into a bundle of pi-parley/host. */
export function parleyBundleDefine(): { readonly PARLEY_BUNDLE: string };
