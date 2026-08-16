#!/usr/bin/env python3
"""Low-priority Scrabble bot that speaks the normal player protocol."""
import asyncio
import json
import logging
import os
import random
import sys
import time
import urllib.parse
import urllib.request
import zipfile
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

VERSION = 6
MAX_BOTS_PER_LANGUAGE = 100
DEFAULT_NAMES = {
    "ru": ("Александр", "Мария", "Дмитрий", "Анна", "Сергей", "Елена", "Алексей", "Ольга"),
    "en": ("Alex", "Emma", "Daniel", "Sophia", "James", "Olivia", "Michael", "Mia"),
}
FIELD_MULTIPLIERS = (
    (3, 1, 1, 2, 1, 1, 1, 3, 1, 1, 1, 2, 1, 1, 3),
    (1, 2, 1, 1, 1, 3, 1, 1, 1, 3, 1, 1, 1, 2, 1),
    (1, 1, 2, 1, 1, 1, 2, 1, 2, 1, 1, 1, 2, 1, 1),
    (2, 1, 1, 2, 1, 1, 1, 2, 1, 1, 1, 2, 1, 1, 2),
    (1, 1, 1, 1, 2, 1, 1, 1, 1, 1, 2, 1, 1, 1, 1),
    (1, 3, 1, 1, 1, 3, 1, 1, 1, 3, 1, 1, 1, 3, 1),
    (1, 1, 2, 1, 1, 1, 2, 1, 2, 1, 1, 1, 2, 1, 1),
    (3, 1, 1, 2, 1, 1, 1, 2, 1, 1, 1, 2, 1, 1, 3),
    (1, 1, 2, 1, 1, 1, 2, 1, 2, 1, 1, 1, 2, 1, 1),
    (1, 3, 1, 1, 1, 3, 1, 1, 1, 3, 1, 1, 1, 3, 1),
    (1, 1, 1, 1, 2, 1, 1, 1, 1, 1, 2, 1, 1, 1, 1),
    (2, 1, 1, 2, 1, 1, 1, 2, 1, 1, 1, 2, 1, 1, 2),
    (1, 1, 2, 1, 1, 1, 2, 1, 2, 1, 1, 1, 2, 1, 1),
    (1, 2, 1, 1, 1, 3, 1, 1, 1, 3, 1, 1, 1, 2, 1),
    (3, 1, 1, 2, 1, 1, 1, 3, 1, 1, 1, 2, 1, 1, 3),
)


def envelope(message_type, request_id, payload, match_id=None):
    message = {"v": VERSION, "type": message_type, "request_id": request_id, "payload": payload}
    if match_id:
        message["match_id"] = match_id
    return message


def load_language_pack(dist_dir, language):
    dist = Path(dist_dir)
    catalog = json.loads((dist / "catalog.json").read_text())
    entry = catalog.get("languages", {}).get(language)
    if not entry:
        raise ValueError(f"language pack is unavailable: {language}")
    with zipfile.ZipFile(dist / entry["url"]) as archive:
        return (
            entry["rules_version"],
            json.loads(archive.read("dictionary_trie.json")),
            json.loads(archive.read("alphabet.json")),
        )


def validate_queue_endpoint(lobby_url, queue_url):
    lobby, queue = urllib.parse.urlsplit(lobby_url), urllib.parse.urlsplit(queue_url)
    local_hosts = {"localhost", "127.0.0.1", "::1"}
    if not lobby.hostname or not queue.hostname or lobby.hostname.lower() != queue.hostname.lower():
        raise ValueError("ORCHESTRATOR_QUEUE_URL host must match ORCHESTRATOR_URL")
    local = queue.hostname.lower() in local_hosts
    if queue.scheme != "https" and not (local and queue.scheme == "http"):
        raise ValueError("ORCHESTRATOR_QUEUE_URL must use HTTPS outside localhost")
    expected_lobby_scheme = "ws" if queue.scheme == "http" else "wss"
    if lobby.scheme != expected_lobby_scheme:
        raise ValueError(f"ORCHESTRATOR_URL must use {expected_lobby_scheme.upper()} for the queue URL")


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, request, fp, code, message, headers, newurl):
        return None


def queue_target(entries, rules_by_language):
    """Return the oldest compatible fallback ticket, without retaining its identity."""
    eligible = (
        entry for entry in entries
        if entry.get("fallback_eligible") is True
        and entry.get("mode", "default") == "default"
        and isinstance(entry.get("language"), str)
        and entry.get("rules_version") == rules_by_language.get(entry.get("language"))
    )
    return max(eligible, key=lambda entry: entry.get("wait_seconds", 0), default=None)


def ticket_is_admitted(services, entry):
    ticket_id = entry.get("ticket_id")
    return bool(ticket_id) and any(service.admission and service.admission.get("ticket_id") == ticket_id for service in services)


async def poll_queue(lobby_url, queue_url):
    validate_queue_endpoint(lobby_url, queue_url)

    def fetch():
        with urllib.request.build_opener(_NoRedirect()).open(queue_url, timeout=5) as response:
            data = json.loads(response.read())
        entries = data.get("entries")
        if not isinstance(entries, list):
            raise ValueError("queue response has no entries list")
        return entries

    return await asyncio.to_thread(fetch)


async def next_queue_target(lobby_url, queue_url, rules_by_language):
    try:
        return queue_target(await poll_queue(lobby_url, queue_url), rules_by_language)
    except Exception as error:
        logging.warning("queue poll failed: %s", error)
        return None


def read_pool(path, default_count=2):
    try:
        value = json.loads(Path(path).read_text())
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        value = {}
    count = value.get("count", default_count)
    last_count = value.get("last_count", count if count else default_count)
    if type(count) is not int or not 0 <= count <= MAX_BOTS_PER_LANGUAGE:
        count = default_count
    if type(last_count) is not int or not 1 <= last_count <= MAX_BOTS_PER_LANGUAGE:
        last_count = max(1, default_count)
    return {"count": count, "last_count": last_count}


def write_pool(path, count, last_count):
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = target.with_suffix(".tmp")
    temporary.write_text(json.dumps({"count": count, "last_count": last_count}, separators=(",", ":")))
    temporary.replace(target)


def configure_pool(command, path, default_count=2):
    current = read_pool(path, default_count)
    if command == "status":
        return current
    if command == "on":
        count = current["last_count"]
    elif command == "off":
        count = 0
    else:
        try:
            count = int(command)
        except ValueError as error:
            raise ValueError("bot count must be on, off, status, or an integer") from error
        if not 0 <= count <= MAX_BOTS_PER_LANGUAGE:
            raise ValueError(f"bot count must be between 0 and {MAX_BOTS_PER_LANGUAGE}")
    last_count = count if count > 0 else current["last_count"]
    write_pool(path, count, last_count)
    return {"count": count, "last_count": last_count}


def bot_name(language, slot):
    configured = os.getenv(f"BOT_NAMES_{language.upper()}", os.getenv("BOT_NAMES", os.getenv("BOT_NAME", "")))
    names = tuple(name.strip() for name in configured.split(",") if name.strip()) or DEFAULT_NAMES.get(language, DEFAULT_NAMES["en"])
    return names[slot % len(names)]


def worker_state_path(base_path, languages, language, slot):
    base = Path(base_path)
    suffix = f"-{language}" if len(languages) > 1 else ""
    if slot > 0:
        suffix += f"-{slot + 1}"
    return base if not suffix else base.with_name(f"{base.stem}{suffix}{base.suffix}")


class Brain:
    """Greedy placement search adapted from the client's bot.lua/word_solver.lua."""

    def __init__(self, trie, alphabet, max_words=30000, max_candidates=800, top_pick_ratio=0.3):
        self.trie, self.alphabet = trie, alphabet
        self.max_words, self.max_candidates, self.top_pick_ratio = max_words, max_candidates, top_pick_ratio

    def has_word(self, word):
        node = self.trie
        for index, letter in enumerate(word.lower()):
            child = node.get(letter)
            if not isinstance(child, dict):
                return False
            if index == len(word) - 1:
                return bool(child.get("complete"))
            node = child.get("children", {})
        return False

    def collect_words(self, counts, min_length, max_length, deadline=None):
        words, used = [], Counter()

        def walk(nodes, prefix):
            for letter in sorted(nodes):
                if deadline is not None and time.monotonic() >= deadline:
                    return
                child = nodes[letter]
                normalized = letter.lower()
                if used[normalized] >= counts[normalized]:
                    continue
                used[normalized] += 1
                word = prefix + letter
                if child.get("complete") and len(word) >= min_length:
                    words.append(word)
                if len(words) < self.max_words and len(word) < max_length:
                    walk(child.get("children", {}), word)
                used[normalized] -= 1
                if len(words) >= self.max_words:
                    return

        walk(self.trie, "")
        return words

    @staticmethod
    def _board(state):
        return {(int(tile["x"]), int(tile["y"])): tile for tile in state.get("board", {}).values()}

    @staticmethod
    def _candidate_starts(chars, board, size, dx, dy):
        if not board:
            center = (size + 1) // 2
            return {(center - index * dx, center - index * dy) for index in range(len(chars))}
        starts = set()
        for (x, y), tile in board.items():
            for index, letter in enumerate(chars):
                if str(tile.get("tile", "")).lower() == letter:
                    starts.add((x - index * dx, y - index * dy))
                for nx, ny in ((x - 1, y), (x + 1, y), (x, y - 1), (x, y + 1)):
                    if (nx, ny) not in board:
                        starts.add((nx - index * dx, ny - index * dy))
        return {start for start in starts
                if start[0] >= 1 and start[1] >= 1
                and start[0] + (len(chars) - 1) * dx <= size
                and start[1] + (len(chars) - 1) * dy <= size}

    @staticmethod
    def _connected(board, placed):
        placed_set, walkable, seen, stack = set(placed), set(board) | set(placed), set(), [placed[0]]
        while stack:
            position = stack.pop()
            if position in seen:
                continue
            seen.add(position)
            x, y = position
            for neighbor in ((x - 1, y), (x + 1, y), (x, y - 1), (x, y + 1)):
                if neighbor in walkable and neighbor not in seen:
                    stack.append(neighbor)
        return bool(set(board) & seen) and placed_set <= seen if board else placed_set <= seen

    def _try_word(self, word, start, direction, board, hand, size):
        dx, dy = direction
        rack, candidate, placements = Counter(tile.upper() for tile in hand), dict(board), []
        for index, letter in enumerate(word.lower()):
            x, y = start[0] + index * dx, start[1] + index * dy
            cell = candidate.get((x, y))
            tile = letter.upper()
            if cell:
                if str(cell.get("tile", "")).upper() != tile:
                    return None
            elif rack[tile] > 0:
                rack[tile] -= 1
                cell = {"x": x, "y": y, "tile": tile, "points": int(self.alphabet.get(tile, {}).get("points", 0))}
                candidate[(x, y)] = cell
                placements.append((x, y))
            else:
                return None
        if not placements or not self._connected(board, placements):
            return None

        words, covered = {}, set()
        for x, y in placements:
            for word_dx, word_dy in ((1, 0), (0, 1)):
                begin_x, begin_y = x, y
                while (begin_x - word_dx, begin_y - word_dy) in candidate:
                    begin_x, begin_y = begin_x - word_dx, begin_y - word_dy
                cells, cursor = [], (begin_x, begin_y)
                while cursor in candidate:
                    cells.append(cursor)
                    cursor = cursor[0] + word_dx, cursor[1] + word_dy
                if len(cells) < 2:
                    continue
                text = "".join(str(candidate[position].get("tile", "")) for position in cells)
                if not self.has_word(text):
                    return None
                words[tuple(cells)] = cells
                covered.update(cells)
        if not set(placements) <= covered:
            return None

        placed_set, score = set(placements), 0
        for cells in words.values():
            for x, y in cells:
                points = int(candidate[(x, y)].get("points", 0))
                score += points * (FIELD_MULTIPLIERS[y - 1][x - 1] if (x, y) in placed_set else 1)
        if len(placements) == len(hand):
            score += 50
        return {
            "action": "place",
            "params": {"tiles": [{"tile": candidate[position]["tile"], "x": position[0], "y": position[1]} for position in placements]},
            "score": score,
        }

    def choose_action(self, public_state, max_seconds=None):
        deadline = time.monotonic() + max_seconds if max_seconds is not None else None
        state = public_state.get("state", public_state)
        hand, size = state.get("hand", []), int(state.get("board_size", 15))
        board = self._board(state)
        available = Counter(str(tile).lower() for tile in hand)
        available.update(str(tile.get("tile", "")).lower() for tile in board.values())
        words = self.collect_words(available, int(state.get("min_word_length", 2)), min(size, len(hand) + 8), deadline)
        candidates = []
        for word in words:
            if deadline is not None and time.monotonic() >= deadline:
                break
            chars = list(word.lower())
            for direction in ((1, 0), (0, 1)):
                for start in self._candidate_starts(chars, board, size, *direction):
                    if deadline is not None and time.monotonic() >= deadline:
                        break
                    candidate = self._try_word(word, start, direction, board, hand, size)
                    if candidate:
                        candidates.append(candidate)
                        if len(candidates) >= self.max_candidates:
                            break
                if len(candidates) >= self.max_candidates:
                    break
            if len(candidates) >= self.max_candidates:
                break
        if not candidates:
            return {"action": "pass", "params": {"reason": "no_move"}, "score": 0}
        candidates.sort(key=lambda item: item["score"], reverse=True)
        return random.choice(candidates[:max(1, int(len(candidates) * self.top_pick_ratio + 0.999))])


class BotService:
    def __init__(self, lobby_url, match_url, name, language, rules_version, brain, state_path,
                 min_turn_seconds=2.0, max_search_seconds=4.0):
        self.lobby_url, self.match_url = lobby_url, match_url
        self.language, self.rules_version, self.brain, self.retire = language, rules_version, brain, False
        self.admission, self.wake = None, asyncio.Event()
        self.state_path, self.state, self.sequence = Path(state_path), {}, 0
        self.min_turn_seconds, self.max_search_seconds = min_turn_seconds, max_search_seconds
        if self.state_path.exists():
            self.state = json.loads(self.state_path.read_text())
        self.name = self.state.get("player_name") or name
        self.state["player_name"] = self.name
        self.save()

    def request_id(self, prefix):
        self.sequence += 1
        return f"bot-{prefix}-{int(time.time())}-{self.sequence}"

    def save(self):
        self.state_path.parent.mkdir(parents=True, exist_ok=True)
        temporary = self.state_path.with_suffix(".tmp")
        temporary.write_text(json.dumps(self.state, separators=(",", ":")))
        temporary.replace(self.state_path)

    def clear_missing_match(self, code):
        if code != "match_not_found":
            return False
        self.state.pop("active", None)
        self.save()
        return True

    async def send(self, websocket, message):
        await websocket.send(json.dumps(message, separators=(",", ":"), ensure_ascii=False))

    def admit(self, entry):
        self.admission = entry
        self.wake.set()

    def find_match_payload(self):
        return {
            "mode": "default", "language": self.language, "rules_version": self.rules_version,
            "queue_tier": "fallback",
        }

    async def lobby_assignment(self):
        import websockets
        async with websockets.connect(self.lobby_url, max_size=32 * 1024) as websocket:
            payload = {"player_name": self.name}
            if self.state.get("profile_token"):
                payload["profile_token"] = self.state["profile_token"]
            await self.send(websocket, envelope("client_hello", self.request_id("hello"), payload))
            async for raw in websocket:
                message = json.loads(raw)
                kind, payload = message.get("type"), message.get("payload", {})
                if kind == "server_hello":
                    self.state["profile_token"] = payload["profile_token"]
                    self.save()
                    active = self.state.get("active")
                    if not active:
                        if self.retire or not self.admission:
                            raise asyncio.CancelledError
                        await self.send(websocket, envelope("find_match", self.request_id("find"), self.find_match_payload()))
                    elif active.get("resume_token"):
                        await self.send(websocket, envelope("resume_match", self.request_id("resume"), {
                            "player_id": active["player_id"], "resume_token": active["resume_token"], "server_id": active["server_id"],
                        }, active["match_id"]))
                    else:
                        await self.send(websocket, envelope("join_match", self.request_id("reroute"), {
                            "player_id": active["player_id"], "join_token": active["join_token"], "server_id": active["server_id"],
                        }, active["match_id"]))
                elif kind == "match_assigned":
                    active = self.state.setdefault("active", {})
                    active.update({key: value for key, value in {
                        "match_id": message.get("match_id"), "player_id": payload.get("player_id"),
                        "join_token": payload.get("join_token"), "server_id": payload.get("server_id"),
                        "server_url": payload.get("server_url"),
                    }.items() if value})
                    self.save()
                    self.admission = None
                    return active
                elif kind == "error":
                    if self.state.get("active") and payload.get("code") in ("match_server_unavailable", "match_not_found"):
                        self.state.pop("active", None)
                        self.save()
                        if self.retire or not self.admission:
                            raise asyncio.CancelledError
                        await self.send(websocket, envelope("find_match", self.request_id("find"), self.find_match_payload()))
                    else:
                        raise RuntimeError(f"lobby error: {payload.get('code')}")
        raise ConnectionError("lobby closed before assignment")

    @staticmethod
    def _state(message):
        payload = message.get("payload", {})
        return payload.get("state") if isinstance(payload.get("state"), dict) else None

    async def _send_move(self, websocket, active, public_state, action):
        turn = int(public_state.get("turn", public_state.get("state", {}).get("turn", 0)))
        await self.send(websocket, envelope("make_move", self.request_id("move"), {
            "turn": turn, "move_id": self.request_id("move-id"),
            "action": action["action"], "params": action["params"],
        }, active["match_id"]))

    async def _act(self, websocket, active, public_state, ready_turns, submitted_turns, force_pass=False):
        game = public_state.get("state", public_state)
        turn = int(public_state.get("turn", game.get("turn", 0)))
        player_id = str(public_state.get("current_player_id", game.get("current_player_id", "")))
        phase = public_state.get("turn_phase", "")
        if public_state.get("status") != "active" or player_id != active["player_id"]:
            return
        if phase == "preparing" and turn not in ready_turns:
            ready_turns.add(turn)
            await self.send(websocket, envelope("turn_ready", self.request_id("ready"), {"turn": turn}, active["match_id"]))
        elif phase == "active" and (turn not in submitted_turns or force_pass):
            submitted_turns.add(turn)
            started = time.monotonic()
            action = {"action": "pass", "params": {"reason": "rejected_move"}} if force_pass else await asyncio.to_thread(
                self.brain.choose_action, public_state, self.max_search_seconds,
            )
            if not force_pass:
                await asyncio.sleep(max(0.0, self.min_turn_seconds - (time.monotonic() - started)))
            await self._send_move(websocket, active, public_state, action)

    async def play_match(self, active):
        import websockets
        url = self.match_url or active["server_url"]
        ready_turns, submitted_turns, fallback_turns = set(), set(), set()
        public_state = None
        async with websockets.connect(url, max_size=32 * 1024) as websocket:
            await self.send(websocket, envelope("client_hello", self.request_id("match-hello"), {
                "profile_token": self.state["profile_token"], "player_name": self.name,
            }))
            async for raw in websocket:
                message = json.loads(raw)
                kind = message.get("type")
                if kind == "server_hello":
                    if active.get("resume_token"):
                        await self.send(websocket, envelope("resume_match", self.request_id("match-resume"), {
                            "player_id": active["player_id"], "resume_token": active["resume_token"], "server_id": active["server_id"],
                        }, active["match_id"]))
                    else:
                        await self.send(websocket, envelope("join_match", self.request_id("join"), {
                            "player_id": active["player_id"], "join_token": active["join_token"], "server_id": active["server_id"],
                        }, active["match_id"]))
                elif kind == "match_joined":
                    active["resume_token"] = message["payload"]["resume_token"]
                    active.pop("join_token", None)
                    self.save()
                    public_state = self._state(message)
                elif kind in ("match_state", "move_accepted", "match_finished"):
                    public_state = self._state(message) or public_state
                elif kind == "turn_started" and public_state:
                    public_state.update({"turn": message["payload"]["turn"], "current_player_id": message["payload"]["player_id"], "turn_phase": "active"})
                elif kind == "move_rejected" and public_state:
                    rejected_turn = int(message.get("payload", {}).get("turn", public_state.get("turn", 0)))
                    public_state = self._state(message) or public_state
                    if rejected_turn not in fallback_turns:
                        fallback_turns.add(rejected_turn)
                        await self._act(websocket, active, public_state, ready_turns, submitted_turns, True)
                        continue
                elif kind == "error":
                    code = message.get("payload", {}).get("code")
                    if self.clear_missing_match(code):
                        return
                    raise RuntimeError(f"match error: {code}")

                if public_state and public_state.get("status") == "completed":
                    self.state.pop("active", None)
                    self.save()
                    return
                if public_state:
                    await self._act(websocket, active, public_state, ready_turns, submitted_turns)
        raise ConnectionError("match connection closed")

    async def run(self):
        while not self.retire or self.state.get("active"):
            try:
                if not self.state.get("active"):
                    await self.wake.wait()
                    self.wake.clear()
                    if self.retire:
                        continue
                await self.play_match(await self.lobby_assignment())
            except asyncio.CancelledError:
                raise
            except Exception as error:
                logging.warning("bot connection failed: %s", error)
                await asyncio.sleep(2)


async def run_pool():
    lobby_url = os.getenv("ORCHESTRATOR_URL", "")
    queue_url = os.getenv("ORCHESTRATOR_QUEUE_URL", "")
    if not lobby_url or not queue_url:
        raise SystemExit("ORCHESTRATOR_URL and ORCHESTRATOR_QUEUE_URL are required")
    try:
        validate_queue_endpoint(lobby_url, queue_url)
    except ValueError as error:
        raise SystemExit(str(error)) from error
    languages = [value.strip() for value in os.getenv("BOT_LANGUAGES", os.getenv("BOT_LANGUAGE", "ru,en")).split(",") if value.strip()]
    if not languages:
        raise SystemExit("BOT_LANGUAGES must list at least one language")
    default_count = int(os.getenv("BOT_COUNT", "2"))
    if not 0 <= default_count <= MAX_BOTS_PER_LANGUAGE:
        raise SystemExit(f"BOT_COUNT must be between 0 and {MAX_BOTS_PER_LANGUAGE}")
    search_workers = int(os.getenv("BOT_SEARCH_WORKERS", "1"))
    min_turn_seconds = float(os.getenv("BOT_MIN_TURN_SECONDS", "2"))
    max_search_seconds = float(os.getenv("BOT_MAX_SEARCH_SECONDS", "4"))
    if search_workers < 1 or min_turn_seconds < 0 or max_search_seconds <= 0:
        raise SystemExit("BOT_SEARCH_WORKERS and BOT_MAX_SEARCH_SECONDS must be positive; BOT_MIN_TURN_SECONDS must not be negative")
    asyncio.get_running_loop().set_default_executor(ThreadPoolExecutor(max_workers=search_workers, thread_name_prefix="bot-search"))
    pool_path = os.getenv("BOT_POOL_PATH", "/data/pool.json")
    state_path = os.getenv("BOT_STATE_PATH", "/data/state.json")
    poll_seconds = float(os.getenv("BOT_QUEUE_POLL_SECONDS", "2"))
    if poll_seconds <= 0:
        raise SystemExit("BOT_QUEUE_POLL_SECONDS must be positive")
    brains, workers, previous_count, next_poll = {}, {}, None, 0.0
    while True:
        for key, (service, task) in list(workers.items()):
            if not task.done():
                continue
            try:
                task.result()
            except asyncio.CancelledError:
                pass
            except Exception as error:
                logging.warning("bot worker stopped: %s", error)
            workers.pop(key)

        count = read_pool(pool_path, default_count)["count"]
        if count != previous_count:
            logging.info("bot pool target changed: %d per language", count)
            previous_count = count
        desired = {(language, slot) for language in languages for slot in range(count)}
        for key, (service, task) in list(workers.items()):
            service.retire = key not in desired
            if service.retire and not service.state.get("active"):
                task.cancel()
        for language, slot in sorted(desired):
            key = language, slot
            if key in workers:
                workers[key][0].retire = False
                continue
            if language not in brains:
                rules_version, trie, alphabet = load_language_pack(os.getenv("LANGUAGE_PACK_DIR", "/app/localization/dist"), language)
                brains[language] = rules_version, Brain(
                    trie, alphabet, int(os.getenv("BOT_MAX_WORDS", "30000")), int(os.getenv("BOT_MAX_CANDIDATES", "800")),
                )
            rules_version, brain = brains[language]
            service = BotService(
                lobby_url, os.getenv("BOT_MATCH_SERVER_URL", ""), bot_name(language, slot), language, rules_version, brain,
                worker_state_path(state_path, languages, language, slot),
                min_turn_seconds, max_search_seconds,
            )
            workers[key] = service, asyncio.create_task(service.run())
        if not workers:
            brains.clear()
        elif time.monotonic() >= next_poll:
            next_poll = time.monotonic() + poll_seconds
            target = await next_queue_target(lobby_url, queue_url, {language: rules[0] for language, rules in brains.items()})
            if target and not ticket_is_admitted((service for service, _ in workers.values()), target):
                for service, _ in workers.values():
                    if (not service.state.get("active") and not service.admission and not service.retire
                            and service.language == target["language"]):
                        service.admit(target)
                        break
        await asyncio.sleep(1)


def pool_cli(command):
    default_count = int(os.getenv("BOT_COUNT", "2"))
    if not 0 <= default_count <= MAX_BOTS_PER_LANGUAGE:
        raise SystemExit(f"BOT_COUNT must be between 0 and {MAX_BOTS_PER_LANGUAGE}")
    try:
        result = configure_pool(command, os.getenv("BOT_POOL_PATH", "/data/pool.json"), default_count)
    except ValueError as error:
        raise SystemExit(str(error)) from error
    print(f"bots per language: {result['count']} (on restores {result['last_count']})")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    if len(sys.argv) == 3 and sys.argv[1] == "--pool":
        pool_cli(sys.argv[2])
    elif len(sys.argv) == 1:
        asyncio.run(run_pool())
    else:
        raise SystemExit("usage: bot_service.py [--pool on|off|status|COUNT]")
