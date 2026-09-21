import type { Socket } from "net";
export declare const MAX_FRAME_BYTES: number;
/**
 * Write a length-prefixed message to a socket.
 * Format: 4-byte big-endian length + JSON payload
 */
export declare function writeMessage(socket: Socket, msg: unknown): void;
/**
 * Create a message reader that handles partial reads.
 * Calls onMessage for each complete message received.
 * Protocol or handler errors are reported to onError so the caller can close the socket.
 */
export declare function createMessageReader(onMessage: (msg: unknown) => void, onError: (error: Error) => void, maxFrameBytes?: number): (data: Buffer) => void;
