import json
import tempfile
import time
import unittest
from pathlib import Path

from services.bot_service import (
    BotService, Brain, _NoRedirect, bot_name, configure_pool, next_queue_target,
    queue_target, read_pool, ticket_is_admitted, validate_queue_endpoint, worker_state_path,
)


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

            self.assertFalse(service.clear_missing_match("profile_mismatch"))
            self.assertTrue(service.clear_missing_match("match_not_found"))
            self.assertNotIn("active", json.loads(path.read_text()))


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
