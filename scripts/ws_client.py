#!/usr/bin/env python3
"""
scripts/ws_client.py — WebSocket test client for RTMDS with auto-reconnect.

Implements the fat client pattern: subscriptions are tracked client-side
and automatically re-sent on reconnect, so any gateway can serve any client.

Usage:
    python scripts/ws_client.py --url ws://localhost:8080/ws --symbols AAPL TSLA
    python scripts/ws_client.py --url ws://localhost:8080/ws --reconnect

Requires: pip install websocket-client
"""
import argparse
import json
import signal
import sys
import time

try:
    import websocket
except ImportError:
    sys.exit("Error: run 'pip install websocket-client' first")


class ReconnectingClient:
    """Fat-client that tracks subscriptions and re-sends them on reconnect."""

    def __init__(self, url: str, symbols: list[str], max_reconnect: int = 0):
        self.url = url
        self.symbols = symbols
        self.max_reconnect = max_reconnect  # 0 = unlimited
        self.ws: websocket.WebSocket | None = None
        self._running = True

    def connect(self) -> bool:
        """Establish WebSocket connection and subscribe."""
        try:
            self.ws = websocket.WebSocket()
            self.ws.connect(self.url)
            self._send_subscribe()
            return True
        except Exception as e:
            print(f"Connection failed: {e}")
            return False

    def _send_subscribe(self):
        """Send subscribe message for tracked symbols."""
        if self.ws and self.symbols:
            msg = json.dumps({"action": "subscribe", "symbols": self.symbols})
            self.ws.send(msg)
            print(f"Subscribed to: {self.symbols}")

    def _send_unsubscribe(self):
        """Send unsubscribe message."""
        if self.ws and self.symbols:
            try:
                msg = json.dumps({"action": "unsubscribe", "symbols": self.symbols})
                self.ws.send(msg)
            except Exception:
                pass

    def run(self):
        """Main loop with auto-reconnect."""
        backoff = 0.5
        max_backoff = 30.0
        attempts = 0

        if not self.connect():
            return

        print(f"{'─' * 60}")

        while self._running:
            try:
                raw = self.ws.recv()
                msg = json.loads(raw)
                payload = msg.get("payload", {})
                if isinstance(payload, dict):
                    print(
                        f"{payload.get('symbol', '?'):6s}  "
                        f"price={payload.get('price', 0):>10.4f}  "
                        f"bid={payload.get('bid', 0):>10.4f}  "
                        f"ask={payload.get('ask', 0):>10.4f}  "
                        f"ts={payload.get('timestamp', '')}"
                    )
                # Reset backoff on successful message.
                backoff = 0.5
                attempts = 0
            except websocket.WebSocketConnectionClosedException:
                print("Connection closed by server.")
                if not self._running:
                    break
            except Exception as e:
                print(f"Read error: {e}")
                if not self._running:
                    break

            # Reconnect loop.
            while self._running:
                if self.max_reconnect > 0 and attempts >= self.max_reconnect:
                    print("Max reconnect attempts reached.")
                    return
                attempts += 1
                print(f"Reconnecting in {backoff:.1f}s (attempt {attempts})...")
                time.sleep(backoff)

                try:
                    self.ws = websocket.WebSocket()
                    self.ws.connect(self.url)
                    # Fat client: re-send subscriptions to new gateway.
                    self._send_subscribe()
                    print(f"{'─' * 60}")
                    backoff = 0.5
                    attempts = 0
                    break
                except Exception as e:
                    print(f"Reconnect failed: {e}")
                    backoff = min(backoff * 2, max_backoff)

        self._send_unsubscribe()
        try:
            self.ws.close()
        except Exception:
            pass

    def stop(self):
        self._running = False


def main() -> None:
    parser = argparse.ArgumentParser(description="RTMDS WebSocket test client")
    parser.add_argument("--url", default="ws://localhost:8080/ws", help="Server WebSocket URL")
    parser.add_argument("--symbols", nargs="+", default=["AAPL", "TSLA"], help="Symbols to subscribe")
    parser.add_argument("--reconnect", action="store_true", help="Enable auto-reconnect (fat client)")
    parser.add_argument("--max-reconnect", type=int, default=0, help="Max reconnect attempts (0=unlimited)")
    args = parser.parse_args()

    if args.reconnect:
        client = ReconnectingClient(args.url, args.symbols, args.max_reconnect)

        def _shutdown(sig, frame):
            print("\nDisconnecting...")
            client.stop()

        signal.signal(signal.SIGINT, _shutdown)
        client.run()
    else:
        # Legacy single-shot mode.
        ws = websocket.WebSocket()
        print(f"Connecting to {args.url} …")
        ws.connect(args.url)

        subscribe_msg = json.dumps({"action": "subscribe", "symbols": args.symbols})
        ws.send(subscribe_msg)
        print(f"Subscribed to: {args.symbols}\n{'─' * 60}")

        def _shutdown(sig, frame):
            print("\nDisconnecting …")
            ws.send(json.dumps({"action": "unsubscribe", "symbols": args.symbols}))
            ws.close()
            sys.exit(0)

        signal.signal(signal.SIGINT, _shutdown)

        while True:
            try:
                raw = ws.recv()
                msg = json.loads(raw)
                payload = msg.get("payload", {})
                if isinstance(payload, dict):
                    print(
                        f"{payload.get('symbol', '?'):6s}  "
                        f"price={payload.get('price', 0):>10.4f}  "
                        f"bid={payload.get('bid', 0):>10.4f}  "
                        f"ask={payload.get('ask', 0):>10.4f}  "
                        f"ts={payload.get('timestamp', '')}"
                    )
            except websocket.WebSocketConnectionClosedException:
                print("Connection closed by server.")
                break


if __name__ == "__main__":
    main()
