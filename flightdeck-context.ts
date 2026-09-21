/** Inherited terminal routing identifies FlightDeck participation, not broker
 * authority. Retained terminals remain participants while their GUI is offline. */
export function isFlightDeckTerminal(env: NodeJS.ProcessEnv = process.env): boolean {
  const tabId = env.FLIGHTDECK_TAB_ID?.trim() || env.FLIGHTDECK_PANE_ID?.trim();
  const socketPath = env.FLIGHTDECK_STATUS_SOCK?.trim();
  const port = env.FLIGHTDECK_STATUS_PORT?.trim() ?? "";
  const hasTcpEndpoint = /^\d+$/.test(port) && Number(port) > 0 && Number(port) <= 65535
    && Boolean(env.FLIGHTDECK_STATUS_TOKEN?.trim());
  return Boolean(tabId && (socketPath || hasTcpEndpoint));
}
