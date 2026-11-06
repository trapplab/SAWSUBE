from __future__ import annotations
import asyncio
import logging
import os
from datetime import datetime
from pathlib import Path
from typing import Any, Optional
from sqlalchemy import select
from ..config import settings
from ..database import SessionLocal
from ..models.tv import TV
from .ws_manager import ws_manager

log = logging.getLogger(__name__)

# samsungtvws (NickWaterton fork) optional import — guarded so app still boots
# even if user hasn't installed deps yet.
try:
    from samsungtvws.async_art import SamsungTVAsyncArt
    from samsungtvws.async_remote import SamsungTVWSAsyncRemote
    HAS_LIB = True
except Exception as e:  # pragma: no cover
    log.warning("samsungtvws not available: %s", e)
    SamsungTVAsyncArt = None  # type: ignore
    SamsungTVWSAsyncRemote = None  # type: ignore
    HAS_LIB = False

try:
    from samsungtvws import exceptions as tvexc
except Exception:  # pragma: no cover
    tvexc = None  # type: ignore

try:
    from websockets.exceptions import ConnectionClosed, InvalidHandshake
except Exception:  # pragma: no cover
    class ConnectionClosed(Exception):  # type: ignore
        pass

    class InvalidHandshake(Exception):  # type: ignore
        pass

try:
    from wakeonlan import send_magic_packet
    HAS_WOL = True
except Exception:
    send_magic_packet = None  # type: ignore
    HAS_WOL = False


def token_path_for(tv: TV) -> str:
    return tv.token_path or os.path.join(settings.TOKEN_DIR, f"tv_{tv.id}.token")


# ── Error classification ─────────────────────────────────────────────────────
# Kinds of failure when talking to a TV:
#   offline      TV is off / unreachable / the socket dropped   → drop connection
#   rejected     TV refused the websocket (not paired, handshake) → drop connection
#   no_response  connected, but the art service didn't answer    → keep connection
#                (samsungtvws counts timeouts and backs off by itself)
#   tv_error     TV answered with an error for this request      → keep connection
#   unexpected   anything else (bugs)                            → drop + traceback
OFFLINE, REJECTED, NO_RESPONSE, TV_ERROR, UNEXPECTED = (
    "offline", "rejected", "no_response", "tv_error", "unexpected")

# events the TV broadcasts to every connected client; when one arrives before our
# own ms.channel.ready, samsungtvws raises ConnectionFailure — a harmless race.
_BROADCAST_EVENTS = {"ms.channel.clientConnect", "ms.channel.clientDisconnect"}


def _lib_exc(name: str) -> tuple[type, ...]:
    cls = getattr(tvexc, name, None) if tvexc else None
    return (cls,) if isinstance(cls, type) else ()


def _handshake_event(e: BaseException) -> str | None:
    if _lib_exc("ConnectionFailure") and isinstance(e, _lib_exc("ConnectionFailure")) and e.args:
        arg = e.args[0]
        if isinstance(arg, dict):
            return arg.get("event")
    return None


def classify_error(e: BaseException) -> tuple[str, str]:
    """Return (kind, human readable message) for an exception from samsungtvws."""
    if isinstance(e, _lib_exc("ArtServiceUnavailableError")):
        return NO_RESPONSE, str(e)
    if isinstance(e, ConnectionRefusedError):
        return OFFLINE, "TV is off or unreachable (connection refused)"
    if isinstance(e, asyncio.TimeoutError):
        return OFFLINE, "TV is off or unreachable (connection timed out)"
    if isinstance(e, ConnectionClosed):
        return OFFLINE, "connection to the TV was closed"
    if isinstance(e, OSError):
        return OFFLINE, f"TV is unreachable ({e.strerror or e})"
    if isinstance(e, InvalidHandshake):
        return REJECTED, f"websocket handshake failed ({e})"
    if isinstance(e, _lib_exc("UnauthorizedError") + _lib_exc("ChannelTimeoutError")):
        return REJECTED, "TV rejected the connection — pair the TV again (press Allow on the TV)"
    if isinstance(e, _lib_exc("ConnectionFailure")):
        event = _handshake_event(e)
        if event:
            return REJECTED, f"TV handshake interrupted by '{event}' event"
        return NO_RESPONSE, str(e) or "TV art service did not become ready"
    if isinstance(e, _lib_exc("ResponseError")):
        return TV_ERROR, str(e) or "TV returned an error"
    if isinstance(e, _lib_exc("TimeoutError") + (AssertionError,)):
        # samsungtvws asserts on an empty reply, i.e. the request timed out
        return NO_RESPONSE, "TV did not answer the request (timed out)"
    return UNEXPECTED, f"{type(e).__name__}: {e}"


def _is_connection_problem(e: BaseException) -> bool:
    return classify_error(e)[0] in (OFFLINE, REJECTED, NO_RESPONSE)


class TVConnection:
    def __init__(self, tv: TV) -> None:
        self.tv_id = tv.id
        self.ip = tv.ip
        self.port = tv.port
        self.mac = tv.mac
        self.name = tv.name
        self.token_file = token_path_for(tv)
        self.lock = asyncio.Lock()
        self.art: Optional[Any] = None
        self.remote: Optional[Any] = None
        self.last_status: dict[str, Any] = {"online": False, "artmode": None, "current": None}
        self._closed = False
        self.status_error: Optional[str] = None  # last fetch_status failure, for log de-duplication

    async def _ensure_art(self) -> Any:
        if not HAS_LIB:
            raise RuntimeError("samsungtvws library not installed")
        if self.art is not None:
            return self.art
        async with self.lock:
            attempts = 3
            for attempt in range(1, attempts + 1):
                if self.art is not None:
                    break
                art = SamsungTVAsyncArt(
                    host=self.ip, port=self.port, token_file=self.token_file, name=self.name or "SAWSUBE"
                )
                try:
                    await art.start_listening()
                except Exception as e:
                    await _close_quietly(art)
                    event = _handshake_event(e)
                    if event in _BROADCAST_EVENTS and attempt < attempts:
                        log.debug("TV %s art handshake interrupted by %s, retrying (%d/%d)",
                                  self.tv_id, event, attempt, attempts)
                        await asyncio.sleep(0.5 * attempt)
                        continue
                    raise
                self.art = art
        return self.art

    async def _ensure_remote(self) -> Any:
        if not HAS_LIB:
            raise RuntimeError("samsungtvws library not installed")
        if self.remote is not None:
            return self.remote
        async with self.lock:
            if self.remote is None:
                self.remote = SamsungTVWSAsyncRemote(
                    host=self.ip, port=self.port, token_file=self.token_file, name=self.name or "SAWSUBE"
                )
                try:
                    await self.remote.start_listening()
                except Exception:
                    remote, self.remote = self.remote, None
                    await _close_quietly(remote)
                    raise
        return self.remote

    async def drop_art(self) -> None:
        """Forget the art connection (closing its websocket) so the next call reconnects."""
        art, self.art = self.art, None
        await _close_quietly(art)

    async def drop_remote(self) -> None:
        remote, self.remote = self.remote, None
        await _close_quietly(remote)

    async def close(self) -> None:
        self._closed = True
        await self.drop_art()
        await self.drop_remote()

    async def reset_connection(self) -> None:
        """Close art/remote and clear handles — lock-safe relative to the poll loop."""
        async with self.lock:
            await self.drop_art()
            await self.drop_remote()

    async def handle_error(self, op: str, e: BaseException, detail: str = "",
                           level: int = logging.INFO, remote: bool = False) -> str:
        """Log a failed TV call according to its kind, drop the connection when it is
        unusable, and return a short message suitable for API responses."""
        kind, msg = classify_error(e)
        where = f"{op} TV {self.tv_id}" + (f" {detail}" if detail else "")
        if kind == UNEXPECTED:
            # level DEBUG means "already reported" — don't repeat the traceback at WARNING
            log.log(logging.WARNING if level >= logging.INFO else logging.DEBUG,
                    "%s failed unexpectedly: %s", where, msg, exc_info=e)
        else:
            log.log(level, "%s failed (%s): %s", where, kind, msg)
        if kind in (OFFLINE, REJECTED, UNEXPECTED):
            await (self.drop_remote() if remote else self.drop_art())
        return msg


async def _close_quietly(c: Any) -> None:
    if c is None:
        return
    try:
        await c.close()
    except Exception as e:  # closing a dead socket can fail in many ways
        log.debug("ignoring error while closing TV connection: %s", e)


class TVManager:
    def __init__(self) -> None:
        self.connections: dict[int, TVConnection] = {}
        self._poll_tasks: dict[int, asyncio.Task] = {}
        self._lock = asyncio.Lock()

    async def get(self, tv: TV) -> TVConnection:
        async with self._lock:
            conn = self.connections.get(tv.id)
            if conn is None:
                conn = TVConnection(tv)
                self.connections[tv.id] = conn
                self._poll_tasks[tv.id] = asyncio.create_task(self._poll_loop(tv.id))
            else:
                # refresh basic fields in case TV row changed
                conn.ip = tv.ip
                conn.port = tv.port
                conn.mac = tv.mac
                conn.name = tv.name
            return conn

    async def remove(self, tv_id: int) -> None:
        async with self._lock:
            conn = self.connections.pop(tv_id, None)
            task = self._poll_tasks.pop(tv_id, None)
        if task:
            task.cancel()
        if conn:
            await conn.close()

    async def shutdown(self) -> None:
        for t in list(self._poll_tasks.values()):
            t.cancel()
        for c in list(self.connections.values()):
            await c.close()
        self.connections.clear()
        self._poll_tasks.clear()

    async def _poll_loop(self, tv_id: int) -> None:
        backoff = 5
        while True:
            try:
                await asyncio.sleep(settings.POLL_INTERVAL_SECS)
                conn = self.connections.get(tv_id)
                if conn is None or conn._closed:
                    return
                status = await self.fetch_status(tv_id)
                # Only broadcast when meaningful keys differ (ignore transient 'error' string)
                keys = ("online", "artmode", "current")
                changed = any(status.get(k) != conn.last_status.get(k) for k in keys)
                if changed:
                    conn.last_status = status
                    await ws_manager.broadcast({"type": "tv_status", "tv_id": tv_id, "payload": status})
                backoff = 5
            except asyncio.CancelledError:
                return
            except Exception as e:
                log.debug("poll err tv %s: %s", tv_id, e)
                await asyncio.sleep(min(backoff, 60))
                backoff = min(backoff * 2, 60)

    async def fetch_status(self, tv_id: int) -> dict[str, Any]:
        async with SessionLocal() as s:
            tv = await s.get(TV, tv_id)
            if not tv:
                return {"online": False, "artmode": None, "current": None}
            conn = await self.get(tv)
            try:
                art = await conn._ensure_art()
                artmode = await art.get_artmode()
                current = None
                try:
                    cur = await art.get_current()
                    current = cur.get("content_id") if isinstance(cur, dict) else None
                except Exception as e:
                    if classify_error(e)[0] == UNEXPECTED:
                        raise
                    log.debug("fetch_status TV %s: get_current failed: %s", tv_id, e)
                tv.last_seen = datetime.utcnow()
                await s.commit()
                if conn.status_error is not None:
                    log.info("TV %s is reachable again", tv_id)
                    conn.status_error = None
                return {"online": True, "artmode": (artmode == "on" or artmode is True),
                        "current": current}
            except Exception as e:
                _, msg = classify_error(e)
                # The poll loop calls this every few seconds: only log when the
                # reason changes, otherwise an offline TV floods the log.
                changed = conn.status_error != msg
                level = logging.INFO if changed else logging.DEBUG
                msg = await conn.handle_error("fetch_status", e, level=level)
                conn.status_error = msg
                return {"online": False, "artmode": None, "current": None, "error": msg}

    # ── High-level operations ────────────────────────────────────────────────
    async def pair(self, tv: TV) -> bool:
        """Connect to the TV to trigger the Allow/Deny prompt (or accept silently in
        developer mode) and record the token.  Returns True if the TV accepted the
        connection.  Completes within ~15 seconds — does NOT poll or block the HTTP
        connection for a long time."""
        conn = await self.get(tv)
        await conn.reset_connection()  # lock-safe reset before fresh handshake

        try:
            await asyncio.wait_for(conn._ensure_art(), timeout=15.0)
        except asyncio.TimeoutError:
            log.warning("pair: TV %s did not complete the handshake within 15s "
                        "(Allow prompt not answered?)", tv.id)
            await conn.drop_art()
            return False
        except Exception as e:
            await conn.handle_error("pair", e, level=logging.WARNING)
            return False

        # Connection succeeded — check whether the library received a real token.
        token_path = conn.token_file
        if os.path.exists(token_path) and os.path.getsize(token_path) > 0:
            log.info("pair: token saved for TV %s", tv.id)
            return True

        # Connected without a real token — normal when the TV is in developer mode
        # (it accepts silently and never issues a new token).  Write a placeholder
        # so the UI reports paired: true.
        try:
            os.makedirs(os.path.dirname(token_path), exist_ok=True)
            Path(token_path).write_text("developer-mode\n")
            log.info("pair: developer-mode connection for TV %s — placeholder token written", tv.id)
        except Exception as e:
            log.warning("pair: could not write placeholder token for TV %s: %s", tv.id, e)
        return True

    async def power_on(self, tv: TV) -> bool:
        if not tv.mac or not HAS_WOL:
            return False
        try:
            send_magic_packet(tv.mac)
            return True
        except Exception as e:
            log.warning("WoL to %s failed for TV %s: %s", tv.mac, tv.id, e)
            return False

    async def power_off(self, tv: TV) -> bool:
        conn = await self.get(tv)
        try:
            remote = await conn._ensure_remote()
            await remote.send_command(("KEY_POWER",))
            return True
        except Exception as e:
            await conn.handle_error("power_off", e, remote=True)
            return False

    async def set_artmode(self, tv: TV, on: bool) -> bool:
        conn = await self.get(tv)
        try:
            art = await conn._ensure_art()
            await art.set_artmode("on" if on else "off")
            return True
        except Exception as e:
            await conn.handle_error("set_artmode", e, detail=f"on={on}")
            return False

    async def upload_image(self, tv: TV, file_path: str, matte: str = "none",
                           file_type: str = "JPEG") -> str | None:
        # read the file first so a local I/O error isn't mistaken for "TV offline"
        try:
            data = await asyncio.to_thread(Path(file_path).read_bytes)
        except OSError as e:
            log.warning("upload_image: cannot read %s for TV %s: %s", file_path, tv.id, e)
            return None
        conn = await self.get(tv)
        try:
            art = await conn._ensure_art()
            log.debug("upload_image: uploading %s to TV %s", file_path, tv.id)
            content_id = await art.upload(data, file_type=file_type, matte=matte)
            log.info("upload_image: TV %s accepted %s → remote_id=%s", tv.id, file_path, content_id)
            return content_id
        except Exception as e:
            await conn.handle_error("upload_image", e, detail=f"path={file_path}", level=logging.WARNING)
            return None

    async def select_image(self, tv: TV, remote_id: str, show: bool = True) -> bool:
        conn = await self.get(tv)
        try:
            art = await conn._ensure_art()
            await art.select_image(remote_id, show=show)
        except Exception as e:
            await conn.handle_error("select_image", e, detail=f"remote_id={remote_id}", level=logging.WARNING)
            return False
        await ws_manager.broadcast({"type": "art_changed", "tv_id": tv.id, "remote_id": remote_id})
        return True

    async def delete_image(self, tv: TV, remote_id: str) -> bool:
        conn = await self.get(tv)
        try:
            art = await conn._ensure_art()
            await art.delete_list([remote_id])
            return True
        except Exception as e:
            await conn.handle_error("delete_image", e, detail=f"remote_id={remote_id}")
            return False

    async def list_images(self, tv: TV) -> list[dict]:
        conn = await self.get(tv)
        try:
            art = await conn._ensure_art()
            res = await art.available()
            return res if isinstance(res, list) else []
        except Exception as e:
            await conn.handle_error("list_images", e)
            return []

    async def get_thumbnail(self, tv: TV, content_id: str) -> bytes | None:
        conn = await self.get(tv)
        try:
            art = await conn._ensure_art()
            data = await art.get_thumbnail(content_id, as_bytes=True)
            return data if isinstance(data, (bytes, bytearray)) else None
        except Exception as e:
            await conn.handle_error("get_thumbnail", e, detail=f"content_id={content_id}",
                                    level=logging.DEBUG)
            return None

    async def set_matte(self, tv: TV, remote_id: str, matte: str) -> bool:
        conn = await self.get(tv)
        try:
            art = await conn._ensure_art()
            await art.change_matte(remote_id, matte)
            return True
        except Exception as e:
            # tv_error here usually means the matte isn't valid for this image's size
            await conn.handle_error("set_matte", e, detail=f"remote_id={remote_id} matte={matte}")
            return False

    async def get_settings(self, tv: TV) -> dict:
        conn = await self.get(tv)
        out: dict = {}
        try:
            art = await conn._ensure_art()
            for key, fn in [
                ("brightness", "get_brightness"),
                ("color_temp", "get_color_temperature"),
                ("slideshow", "get_slideshow_status"),
                ("motion_timer", "get_motion_timer"),
                ("motion_sensitivity", "get_motion_sensitivity"),
                ("brightness_sensor", "get_brightness_sensor_setting"),
            ]:
                try:
                    if hasattr(art, fn):
                        out[key] = await getattr(art, fn)()
                except Exception as e:
                    # connection-level failures abort the whole read (otherwise every
                    # remaining setting waits for its own timeout)
                    if _is_connection_problem(e):
                        raise
                    log.debug("get_settings TV %s: %s unsupported/failed: %s", tv.id, fn, e)
                    out[key] = None
        except Exception as e:
            await conn.handle_error("get_settings", e)
        return out

    async def apply_settings(self, tv: TV, payload: dict) -> dict:
        conn = await self.get(tv)
        result: dict = {}
        try:
            art = await conn._ensure_art()
            mapping = {
                "brightness": "set_brightness",
                "color_temp": "set_color_temperature",
                "motion_timer": "set_motion_timer",
                "motion_sensitivity": "set_motion_sensitivity",
                "brightness_sensor": "set_brightness_sensor_setting",
            }
            for key, fn in mapping.items():
                if payload.get(key) is not None and hasattr(art, fn):
                    try:
                        await getattr(art, fn)(payload[key])
                        result[key] = "ok"
                    except Exception as e:
                        if _is_connection_problem(e):
                            raise
                        result[key] = classify_error(e)[1]
            if payload.get("shuffle") is not None or payload.get("slideshow_interval") is not None:
                try:
                    await art.set_slideshow_status(
                        duration=payload.get("slideshow_interval") or 3,
                        type="shuffle" if payload.get("shuffle") else "serial",
                        category=2,
                    )
                    result["slideshow"] = "ok"
                except Exception as e:
                    if _is_connection_problem(e):
                        raise
                    result["slideshow"] = classify_error(e)[1]
        except Exception as e:
            result["_error"] = await conn.handle_error("apply_settings", e)
        return result

    async def list_mattes(self, tv: TV) -> list[str]:
        conn = await self.get(tv)
        try:
            art = await conn._ensure_art()
            # samsungtvws returns dicts like [{"matte_type": "modern"}, ...]
            # (and [{"color": "apricot"}, ...] with include_colour=True).
            # The TV expects "<type>_<color>" ids, e.g. "modern_apricot".
            try:
                types_raw, colors_raw = await art.get_matte_list(include_colour=True)
            except TypeError:
                types_raw, colors_raw = await art.get_matte_list(), []

            def _names(items, key: str) -> list[str]:
                out: list[str] = []
                for it in items or []:
                    v = it.get(key) if isinstance(it, dict) else it
                    if isinstance(v, str) and v and v not in out:
                        out.append(v)
                return out

            types = _names(types_raw, "matte_type")
            colors = _names(colors_raw, "color")
            if not colors:
                return types
            mattes: list[str] = []
            for t in types:
                if t == "none":
                    mattes.append(t)
                else:
                    mattes.extend(f"{t}_{c}" for c in colors)
            return mattes
        except Exception as e:
            await conn.handle_error("list_mattes", e)
            return []

    async def get_current(self, tv: TV) -> dict | None:
        conn = await self.get(tv)
        try:
            art = await conn._ensure_art()
            return await art.get_current()
        except Exception as e:
            await conn.handle_error("get_current", e, level=logging.DEBUG)
            return None

    async def device_info(self, tv: TV) -> dict:
        conn = await self.get(tv)
        try:
            art = await conn._ensure_art()
            return await art.get_device_info() if hasattr(art, "get_device_info") else {}
        except Exception as e:
            await conn.handle_error("device_info", e)
            return {}


tv_manager = TVManager()
