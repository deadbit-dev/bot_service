import json
import sys
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import patch

from services.bot_service import (
    BotService, Brain, _NoRedirect, bot_name, configure_pool, next_queue_target,
    queue_target, read_pool, ticket_is_admitted, validate_queue_endpoint, worker_state_path,
)


class FakeConnection:
    """Stand-in for a `websockets` connection: an async context manager and
    async iterator over pre-scripted incoming messages."""

    def __init__(self, messages):
        self._messages = list(messages)
        self.sent = []

    async def send(self, message):
        self.sent.append(json.loads(message))

    def __aiter__(self):
        return self

    async def __anext__(self):
        if not self._messages:
            raise StopAsyncIteration
        return self._messages.pop(0)

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False


class FakeWebsocketsModule:
    def __init__(self, connection):
        self._connection = connection

    def connect(self, url, max_size=None):
        return self._connection


def trie(words):
    root = {}
    for word in words:
        node = root
        for index, letter in enumerate(word):
            entry = node.setdefault(letter, {"complete": False, "children": {}})
            entry["complete"] = entry["complete"] or index == len(word) - 1
            node = entry["children"]
    return root


class BrainTests(unittest.TestCase):
    def test_places_a_valid_word_touching_the_board(self):
        brain = Brain(trie(["at", "to"]), {
            "A": {"points": 1}, "T": {"points": 1}, "O": {"points": 1},
        }, max_words=20, max_candidates=20, top_pick_ratio=1)
        action = brain.choose_action({"state": {
            "board_size": 3,
            "min_word_length": 2,
            "hand": ["T", "O"],
            "board": {"2,2": {"x": 2, "y": 2, "tile": "A", "points": 1}},
        }})

        self.assertEqual(action["action"], "place")
        self.assertTrue(action["params"]["tiles"])

    def test_connects_new_tiles_through_existing_word(self):
        brain = Brain(trie(["tat"]), {"A": {"points": 1}, "T": {"points": 1}}, max_words=10, max_candidates=10)
        action = brain.choose_action({"state": {
            "board_size": 3,
            "min_word_length": 2,
            "hand": ["T", "T"],
            "board": {"2,2": {"x": 2, "y": 2, "tile": "A", "points": 1}},
        }})

        self.assertEqual(len(action["params"]["tiles"]), 2)

    def test_search_budget_still_returns_an_action(self):
        brain = Brain(trie(["at"]), {"A": {"points": 1}, "T": {"points": 1}})
        action = brain.choose_action({"state": {
            "board_size": 15, "min_word_length": 2, "hand": ["A", "T"], "board": {},
        }}, max_seconds=0)

        self.assertEqual(action["action"], "pass")


class PoolTests(unittest.TestCase):
    def test_pool_toggle_preserves_count_and_worker_identities(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "pool.json"
            self.assertEqual(configure_pool("off", path, 3), {"count": 0, "last_count": 3})
            self.assertEqual(configure_pool("on", path, 1)["count"], 3)
            self.assertEqual(configure_pool("5", path, 1)["count"], 5)
            self.assertEqual(read_pool(path, 1)["count"], 5)

        self.assertEqual(worker_state_path("/data/state.json", ["ru", "en"], "ru", 0), Path("/data/state-ru.json"))
        self.assertEqual(worker_state_path("/data/state.json", ["ru", "en"], "ru", 1), Path("/data/state-ru-2.json"))
        self.assertNotEqual(bot_name("ru", 0).lower(), "bot")

    def test_missing_match_returns_worker_to_pool(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "state.json"
            service = BotService("", "", "Bot", "ru", "1", None, path)
            service.state["active"] = {"match_id": "gone"}
            service.save()

            # Any error code abandons an active match -- wedging a pool slot
            # is always worse than abandoning it.
            self.assertTrue(service.abandon_active_match("profile_mismatch"))
            self.assertNotIn("active", json.loads(path.read_text()))

            # Nothing left to abandon: caller should treat this as unrelated
            # to a match and back off instead.
            self.assertFalse(service.abandon_active_match("match_not_found"))


class QueueTests(unittest.TestCase):
    def test_admits_oldest_compatible_fallback_ticket_only(self):
        entries = [
            {"language": "ru", "rules_version": "1", "mode": "default", "wait_seconds": 99, "fallback_eligible": False},
            {"language": "en", "rules_version": "old", "mode": "default", "wait_seconds": 99, "fallback_eligible": True},
            {"language": "en", "rules_version": "1", "mode": "default", "wait_seconds": 3, "fallback_eligible": True},
            {"language": "en", "rules_version": "1", "mode": "default", "wait_seconds": 8, "fallback_eligible": True},
        ]
        self.assertIs(queue_target(entries, {"en": "1"}), entries[-1])

    def test_rejects_untrusted_queue_urls_and_redirects(self):
        validate_queue_endpoint("ws://host.docker.internal:9000/", "http://host.docker.internal:9001/v1/matchmaking/queue")
        with self.assertRaises(ValueError):
            validate_queue_endpoint("wss://game.example.com/", "http://game.example.com/queue")
        with self.assertRaises(ValueError):
            validate_queue_endpoint("wss://game.example.com/", "https://other.example.com/queue")
        self.assertIsNone(_NoRedirect().redirect_request(None, None, 302, "Found", {}, "https://other.example.com/"))

    def test_does_not_admit_the_same_ticket_twice(self):
        ticket = {"ticket_id": "ticket-1"}
        first = type("Worker", (), {"admission": ticket})()
        second = type("Worker", (), {"admission": None})()
        self.assertTrue(ticket_is_admitted((first, second), ticket))
        self.assertFalse(ticket_is_admitted((second,), ticket))


class LobbyAssignmentTests(unittest.IsolatedAsyncioTestCase):
    async def test_new_match_id_does_not_inherit_stale_resume_token(self):
        with tempfile.TemporaryDirectory() as directory:
            service = BotService("wss://lobby", "", "Bot", "ru", "1", None, Path(directory) / "state.json")
            service.state["active"] = {
                "match_id": "M1", "player_id": "p1", "resume_token": "stale-token", "server_id": "s1",
            }
            service.save()
            connection = FakeConnection([json.dumps({
                "type": "match_assigned", "match_id": "M2",
                "payload": {"player_id": "p2", "join_token": "jt2", "server_id": "s2", "server_url": "u2"},
            })])

            with patch.dict(sys.modules, {"websockets": FakeWebsocketsModule(connection)}):
                active = await service.lobby_assignment()

            self.assertEqual(active["match_id"], "M2")
            self.assertNotIn("resume_token", active)
            self.assertNotIn("resume_token", service.state["active"])

    async def test_same_match_id_merges_and_keeps_resume_token(self):
        with tempfile.TemporaryDirectory() as directory:
            service = BotService("wss://lobby", "", "Bot", "ru", "1", None, Path(directory) / "state.json")
            service.state["active"] = {
                "match_id": "M1", "player_id": "p1", "resume_token": "keep-me", "server_id": "s1",
            }
            service.save()
            connection = FakeConnection([json.dumps({
                "type": "match_assigned", "match_id": "M1",
                "payload": {"player_id": "p1", "join_token": "jt-new", "server_id": "s2", "server_url": "u2"},
            })])

            with patch.dict(sys.modules, {"websockets": FakeWebsocketsModule(connection)}):
                active = await service.lobby_assignment()

            self.assertEqual(active["resume_token"], "keep-me")
            self.assertEqual(active["join_token"], "jt-new")

    async def test_arbitrary_error_code_abandons_active_match_without_wedging(self):
        with tempfile.TemporaryDirectory() as directory:
            service = BotService("wss://lobby", "", "Bot", "ru", "1", None, Path(directory) / "state.json")
            service.state["active"] = {"match_id": "M1", "player_id": "p1", "resume_token": "tok", "server_id": "s1"}
            service.save()
            service.admission = {"ticket_id": "t1"}  # so the bot re-queues instead of retiring
            connection = FakeConnection([json.dumps({
                "type": "error", "payload": {"code": "invalid_resume_token"},
            })])

            with patch.dict(sys.modules, {"websockets": FakeWebsocketsModule(connection)}):
                with self.assertRaises(ConnectionError):
                    await service.lobby_assignment()

            self.assertNotIn("active", service.state)
            self.assertEqual(connection.sent[-1]["type"], "find_match")

    async def test_error_without_active_match_still_raises_for_backoff(self):
        with tempfile.TemporaryDirectory() as directory:
            service = BotService("wss://lobby", "", "Bot", "ru", "1", None, Path(directory) / "state.json")
            connection = FakeConnection([json.dumps({
                "type": "error", "payload": {"code": "profile_service_unavailable"},
            })])

            with patch.dict(sys.modules, {"websockets": FakeWebsocketsModule(connection)}):
                with self.assertRaises(RuntimeError):
                    await service.lobby_assignment()


class StoppedMatchTests(unittest.IsolatedAsyncioTestCase):
    async def test_stopped_match_releases_the_bot(self):
        """A match the server stops (opponent never joined) must free the slot."""
        with tempfile.TemporaryDirectory() as directory:
            service = BotService("wss://lobby", "wss://match", "Bot", "ru", "1", None, Path(directory) / "state.json")
            service.state.update({"profile_token": "tok", "active": {"match_id": "M1", "player_id": "p2", "server_id": "s1"}})
            service.save()
            connection = FakeConnection([json.dumps({
                "type": "match_state", "match_id": "M1",
                "payload": {"state": {"status": "stopped", "turn": 0}},
            })])

            with patch.dict(sys.modules, {"websockets": FakeWebsocketsModule(connection)}):
                await service.play_match(dict(service.state["active"]))

            self.assertNotIn("active", service.state)


class TurnTimingTests(unittest.IsolatedAsyncioTestCase):
    async def test_fast_search_waits_for_minimum_turn_time(self):
        class WebSocket:
            async def send(self, message):
                self.message = json.loads(message)

        with tempfile.TemporaryDirectory() as directory:
            service = BotService("", "", "Bot", "en", "1", Brain(trie(["at"]), {
                "A": {"points": 1}, "T": {"points": 1},
            }), Path(directory) / "state.json", min_turn_seconds=0.02)
            websocket, started = WebSocket(), time.monotonic()
            await service._act(websocket, {"player_id": "player", "match_id": "match"}, {
                "status": "active", "current_player_id": "player", "turn_phase": "active", "turn": 1,
                "state": {"board_size": 15, "min_word_length": 2, "hand": ["A", "T"], "board": {}},
            }, set(), set())

        self.assertGreaterEqual(time.monotonic() - started, 0.015)
        self.assertEqual(websocket.message["type"], "make_move")

    async def test_poll_failure_never_admits(self):
        from unittest.mock import patch
        with patch("services.bot_service.poll_queue", side_effect=OSError("offline")):
            self.assertIsNone(await next_queue_target("wss://game.example.com/", "https://game.example.com/queue", {"en": "1"}))


if __name__ == "__main__":
    unittest.main()
