#!/usr/bin/env python3
"""
chessdb_uci.py — A UCI engine that exposes ChessDB (chessdb.cn) as if it were
a local search engine. Instead of running a tree search, every "info" line is
backed by ChessDB's distributed min-max tree.

Why this exists
---------------
For correspondence/ICCF preparation, ChessDB is often the most authoritative
single source: it represents a curated, distributed analysis tree maintained
across many contributors. But the workflow of "paste FEN into a browser, copy
moves out, paste into your GUI" loses the integration with PGN trees, opening
books, multi-engine comparison panes, and analysis logging that any UCI GUI
provides for free.

This engine bridges that gap. Point any UCI-compliant GUI (Arena, Cute Chess,
ChessBase, BanksiaGUI, Scid vs PC, lichess-bot, etc.) at this script and
ChessDB shows up as just another engine slot.

Behavior
--------
- On every `position`/`go`, the worker performs a breadth-first expansion of
  the variation tree from the current root, querying `queryall` for the top
  moves at each node and `querypv` to fetch CDB's full principal variation
  at the leaves of the expansion frontier.
- Then it polls. Every PollingRate seconds it re-queries every position it
  knows about and re-emits MultiPV info lines. CDB's distributed tree is
  *alive* — re-querying after a few seconds frequently produces materially
  different results as the crawler resolves new branches.
- Unknown positions are optionally queued to CDB's analysis backlog
  (`action=queue`), so positions that miss this run land in the database for
  the next one.

UCI options
-----------
PollingRate    (string, default "5.0")  — seconds between re-poll cycles
MultiPV        (spin,   default 5)      — number of root variations to track
ExpansionDepth (spin,   default 6)      — plies of BFS expansion from root
ExpansionWidth (spin,   default 2)      — branches followed at each interior node
PVDepth        (spin,   default 24)     — max plies in any emitted PV line
QueueUnknown   (check,  default true)   — POST `action=queue` for unknown FENs
ApiTimeout     (string, default "8.0")  — HTTP request timeout (seconds)
MinRequestGap  (string, default "0.05") — minimum delay between API calls
LogFile        (string, default "")     — if non-empty, append protocol log here

Dependencies
------------
- python-chess (`pip install python-chess`)
- Standard library only otherwise.

Usage
-----
Configure your GUI to launch:
    python /path/to/chessdb_uci.py

Author: Matt Tedesco
License: MIT
"""

from __future__ import annotations

import json
import os
import sys
import threading
import time
import traceback
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass, field
from typing import Optional

try:
    import chess
except ImportError:
    sys.stderr.write(
        "ERROR: python-chess is required.\n"
        "Install with: pip install python-chess\n"
    )
    sys.exit(1)


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

ENGINE_NAME    = "ChessDB-UCI"
ENGINE_VERSION = "1.1"
ENGINE_AUTHOR  = "Matt Tedesco"

CDB_URL        = "http://www.chessdb.cn/cdb.php"

# CDB returns "win" scores in the high tens of thousands. Anything past this
# we treat as a forced-mate-ish signal and convert to a UCI mate score.
MATE_THRESHOLD = 25000
# When CDB only gives us a "win" sentinel and no mate distance, fake one so
# the GUI shows mate-in-N rather than a literal +25000 cp readout.
MATE_FAKE      = 50

CDB_FAIL_PREFIXES = (
    "unknown", "invalid", "nobestmove", "checkmate", "stalemate", "error"
)

CDB_RANK_LABEL = {2: "Best", 1: "Good", 0: "Bad", -1: "?"}


# ---------------------------------------------------------------------------
# Logger
# ---------------------------------------------------------------------------

class Logger:
    """Optional file-based protocol logger. Off by default (empty path)."""

    def __init__(self, path: str = ""):
        self._path = path
        self._lock = threading.Lock()
        self._fp = None
        if path:
            try:
                self._fp = open(path, "a", buffering=1, encoding="utf-8")
                self._write_locked(f"\n=== {ENGINE_NAME} {ENGINE_VERSION} "
                                   f"started {time.strftime('%Y-%m-%d %H:%M:%S')} ===\n")
            except OSError:
                self._fp = None

    def _write_locked(self, msg: str) -> None:
        if self._fp:
            try:
                self._fp.write(msg)
            except OSError:
                pass

    def __call__(self, msg: str) -> None:
        if not self._fp:
            return
        line = f"{time.strftime('%H:%M:%S')} {msg}\n"
        with self._lock:
            self._write_locked(line)

    def close(self) -> None:
        with self._lock:
            if self._fp:
                try:
                    self._fp.close()
                except OSError:
                    pass
                self._fp = None


# ---------------------------------------------------------------------------
# ChessDB HTTP client
# ---------------------------------------------------------------------------

class CDBClient:
    """Thin HTTP client over chessdb.cn's `cdb.php` endpoint.

    ChessDB returns pipe-delimited records by default; passing `json=1` gives
    a JSON response. We try JSON first and fall back to the legacy text format
    so the engine keeps working if the JSON shape is ever tweaked.

    A single global throttle (`min_gap`) prevents the engine from hammering
    the API when the expansion frontier is wide. CDB is a shared resource —
    being a polite client matters.
    """

    def __init__(self, timeout: float = 8.0, min_gap: float = 0.05,
                 logger: Optional[Logger] = None):
        self.timeout  = timeout
        self.min_gap  = min_gap
        self._last    = 0.0
        self._lock    = threading.Lock()
        self._log     = logger or (lambda *_: None)

    def _throttle(self) -> None:
        with self._lock:
            now     = time.time()
            elapsed = now - self._last
            if elapsed < self.min_gap:
                time.sleep(self.min_gap - elapsed)
            self._last = time.time()

    def _get(self, params: dict) -> str:
        self._throttle()
        url = CDB_URL + "?" + urllib.parse.urlencode(params)
        req = urllib.request.Request(
            url, headers={"User-Agent": f"{ENGINE_NAME}/{ENGINE_VERSION}"}
        )
        try:
            with urllib.request.urlopen(req, timeout=self.timeout) as r:
                body = r.read().decode("utf-8", errors="replace").strip()
                self._log(f"GET {params.get('action')} -> {body[:120]}")
                return body
        except (urllib.error.URLError, urllib.error.HTTPError, OSError) as e:
            self._log(f"HTTP error: {e}")
            return f"error:{e}"

    # -- queryall ----------------------------------------------------------

    def queryall(self, fen: str) -> list[dict]:
        """Return [{move, score, rank, note, winrate}, ...] sorted as CDB
        returned them (which is by score desc). Empty list on miss/error."""
        text = self._get({"action": "queryall", "board": fen, "json": 1})
        moves = self._parse_queryall_json(text)
        if moves is not None:
            return moves
        return self._parse_queryall_text(text)

    @staticmethod
    def _parse_queryall_json(text: str) -> Optional[list[dict]]:
        if not text or text[0] not in "{[":
            return None
        try:
            data = json.loads(text)
        except json.JSONDecodeError:
            return None
        if isinstance(data, dict):
            status = data.get("status")
            if status and status != "ok":
                return []
            raw = data.get("moves", [])
        elif isinstance(data, list):
            raw = data
        else:
            return None
        out = []
        for m in raw:
            if not isinstance(m, dict):
                continue
            uci = m.get("uci") or m.get("move")
            if not uci:
                continue
            try:
                out.append({
                    "move":    uci,
                    "score":   int(m.get("score", 0)),
                    "rank":    int(m.get("rank", -1)),
                    "note":    str(m.get("note", "")).strip(),
                    "winrate": str(m.get("winrate", "")).strip(),
                })
            except (TypeError, ValueError):
                continue
        return out

    @staticmethod
    def _parse_queryall_text(text: str) -> list[dict]:
        if not text:
            return []
        low = text.lower()
        if any(low.startswith(p) for p in CDB_FAIL_PREFIXES):
            return []
        out = []
        for entry in text.split("|"):
            d = {}
            for part in entry.split(","):
                k, sep, v = part.partition(":")
                if sep:
                    d[k.strip()] = v.strip()
            uci = d.get("move")
            if not uci:
                continue
            try:
                out.append({
                    "move":    uci,
                    "score":   int(d.get("score", 0)),
                    "rank":    int(d.get("rank", -1)),
                    "note":    d.get("note", ""),
                    "winrate": d.get("winrate", ""),
                })
            except ValueError:
                continue
        return out

    # -- querypv -----------------------------------------------------------

    def querypv(self, fen: str) -> Optional[dict]:
        """Return {score, depth, pv: [uci, ...]} or None."""
        text = self._get({"action": "querypv", "board": fen, "json": 1})
        result = self._parse_querypv_json(text)
        if result is not None:
            return result
        return self._parse_querypv_text(text)

    @staticmethod
    def _parse_querypv_json(text: str) -> Optional[dict]:
        if not text or text[0] != "{":
            return None
        try:
            data = json.loads(text)
        except json.JSONDecodeError:
            return None
        status = data.get("status")
        if status and status != "ok":
            return None
        pv = data.get("pv")
        if isinstance(pv, str):
            pv_moves = [m for m in pv.split("|") if m]
        elif isinstance(pv, list):
            pv_moves = [str(m) for m in pv if m]
        else:
            pv_moves = []
        if not pv_moves:
            return None
        try:
            return {
                "score": int(data.get("score", 0)),
                "depth": int(data.get("depth", 0)),
                "pv":    pv_moves,
            }
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _parse_querypv_text(text: str) -> Optional[dict]:
        if not text:
            return None
        low = text.lower()
        if any(low.startswith(p) for p in CDB_FAIL_PREFIXES):
            return None
        d = {}
        for part in text.split(","):
            k, sep, v = part.partition(":")
            if sep:
                d[k.strip()] = v.strip()
        if "pv" not in d:
            return None
        pv_moves = [m for m in d["pv"].split("|") if m]
        if not pv_moves:
            return None
        try:
            return {
                "score": int(d.get("score", 0)),
                "depth": int(d.get("depth", 0)),
                "pv":    pv_moves,
            }
        except ValueError:
            return None

    # -- queue -------------------------------------------------------------

    def queue(self, fen: str) -> bool:
        """Queue a position for CDB's distributed analyzers. Returns True if
        the API echoed an apparent success."""
        text = self._get({"action": "queue", "board": fen})
        return text.lower().startswith("ok")


# ---------------------------------------------------------------------------
# Variation tree cache
# ---------------------------------------------------------------------------

@dataclass
class TreeNode:
    """Cached CDB data for one position, plus trend tracking across polls."""
    moves:       list[dict] = field(default_factory=list)
    pv:          Optional[dict] = None
    last_polled: float = 0.0
    poll_count:  int = 0
    score_hist:  list[int] = field(default_factory=list)

    def record_score(self, score: int, history_max: int = 8) -> None:
        self.score_hist.append(score)
        if len(self.score_hist) > history_max:
            self.score_hist.pop(0)

    def trend(self) -> int:
        """Centipawn delta between most-recent and oldest tracked score.
        Positive means the position is improving for the side to move."""
        if len(self.score_hist) < 2:
            return 0
        return self.score_hist[-1] - self.score_hist[0]


class VariationTree:
    """Thread-safe cache mapping FEN -> TreeNode."""

    def __init__(self):
        self._nodes: dict[str, TreeNode] = {}
        self._lock = threading.Lock()

    def get(self, fen: str) -> Optional[TreeNode]:
        with self._lock:
            return self._nodes.get(fen)

    def update(self, fen: str, moves=None, pv=None) -> TreeNode:
        with self._lock:
            node = self._nodes.setdefault(fen, TreeNode())
            if moves is not None:
                node.moves = moves
                if moves:
                    node.record_score(moves[0]["score"])
            if pv is not None:
                node.pv = pv
            node.last_polled = time.time()
            node.poll_count += 1
            return node

    def known_fens(self) -> list[str]:
        with self._lock:
            return list(self._nodes.keys())

    def reset(self) -> None:
        with self._lock:
            self._nodes.clear()


# ---------------------------------------------------------------------------
# UCI engine
# ---------------------------------------------------------------------------

@dataclass
class EngineConfig:
    polling_rate:         float = 5.0
    multipv:              int   = 5
    expansion_depth:      int   = 6
    expansion_width:      int   = 2
    pv_depth:             int   = 24
    queue_unknown:        bool  = True
    api_timeout:          float = 8.0
    min_request_gap:      float = 0.05
    log_file:             str   = ""
    tree_refresh_interval: float = 60.0  # seconds between full-tree refreshes


class ChessDBEngine:
    """The UCI engine. One instance per process; one search worker at a time."""

    def __init__(self):
        self.config        = EngineConfig()
        self.logger        = Logger(self.config.log_file)
        self.cdb           = self._build_client()
        self.tree          = VariationTree()
        self.board         = chess.Board()
        self._stop_event   = threading.Event()
        self._worker       = None  # type: Optional[threading.Thread]
        self._worker_lock  = threading.Lock()
        self._start_time   = 0.0

    # ---- I/O helpers -----------------------------------------------------

    def _send(self, line: str) -> None:
        sys.stdout.write(line + "\n")
        sys.stdout.flush()
        self.logger(f"OUT {line}")

    def _info_string(self, msg: str) -> None:
        self._send(f"info string {msg}")

    def _build_client(self) -> CDBClient:
        return CDBClient(
            timeout=self.config.api_timeout,
            min_gap=self.config.min_request_gap,
            logger=self.logger,
        )

    # ---- UCI command handlers --------------------------------------------

    def cmd_uci(self) -> None:
        self._send(f"id name {ENGINE_NAME} {ENGINE_VERSION}")
        self._send(f"id author {ENGINE_AUTHOR}")
        # Options. UCI option types: spin, check, string, combo, button.
        c = self.config
        self._send(f"option name PollingRate type string default {c.polling_rate}")
        self._send(f"option name MultiPV type spin default {c.multipv} min 1 max 20")
        self._send(f"option name ExpansionDepth type spin default {c.expansion_depth} min 0 max 20")
        self._send(f"option name ExpansionWidth type spin default {c.expansion_width} min 1 max 8")
        self._send(f"option name PVDepth type spin default {c.pv_depth} min 1 max 64")
        self._send(f"option name QueueUnknown type check default {'true' if c.queue_unknown else 'false'}")
        self._send(f"option name TreeRefreshInterval type string default {c.tree_refresh_interval}")
        self._send(f"option name ApiTimeout type string default {c.api_timeout}")
        self._send(f"option name MinRequestGap type string default {c.min_request_gap}")
        self._send(f"option name LogFile type string default {c.log_file or '<empty>'}")
        self._send("uciok")

    def cmd_isready(self) -> None:
        self._send("readyok")

    def cmd_ucinewgame(self) -> None:
        self.tree.reset()
        self.board.reset()

    def cmd_setoption(self, args: list[str]) -> None:
        # Format: setoption name <name> [value <value>]
        if "name" not in args:
            return
        try:
            name_idx = args.index("name") + 1
        except ValueError:
            return
        if "value" in args:
            value_idx = args.index("value")
            name  = " ".join(args[name_idx:value_idx])
            value = " ".join(args[value_idx + 1:])
        else:
            name  = " ".join(args[name_idx:])
            value = ""
        self._apply_option(name.strip(), value.strip())

    def _apply_option(self, name: str, value: str) -> None:
        c = self.config
        try:
            if   name == "PollingRate":         c.polling_rate         = max(0.1, float(value))
            elif name == "MultiPV":             c.multipv              = max(1, int(value))
            elif name == "ExpansionDepth":      c.expansion_depth      = max(0, int(value))
            elif name == "ExpansionWidth":      c.expansion_width      = max(1, int(value))
            elif name == "PVDepth":             c.pv_depth             = max(1, int(value))
            elif name == "QueueUnknown":        c.queue_unknown        = value.lower() in ("true", "1", "yes", "on")
            elif name == "TreeRefreshInterval": c.tree_refresh_interval= max(1.0, float(value))
            elif name == "ApiTimeout":          c.api_timeout          = max(1.0, float(value))
            elif name == "MinRequestGap":       c.min_request_gap      = max(0.0, float(value))
            elif name == "LogFile":
                # Empty string disables logging.
                c.log_file = "" if value in ("<empty>", "") else value
                self.logger.close()
                self.logger = Logger(c.log_file)
        except ValueError:
            self.logger(f"Bad option value: {name}={value!r}")
            return
        # Rebuild the HTTP client so timeout / gap / logger changes apply.
        self.cdb = self._build_client()

    def cmd_position(self, args: list[str]) -> None:
        # Forms:
        #   position startpos [moves m1 m2 ...]
        #   position fen <fen-fields...> [moves m1 m2 ...]
        if not args:
            return
        if args[0] == "startpos":
            self.board.reset()
            rest = args[1:]
        elif args[0] == "fen":
            # FEN is six space-separated fields; some GUIs send fewer.
            try:
                moves_idx = args.index("moves", 1)
            except ValueError:
                moves_idx = len(args)
            fen = " ".join(args[1:moves_idx])
            try:
                self.board.set_fen(fen)
            except ValueError:
                self._info_string(f"invalid FEN: {fen}")
                return
            rest = args[moves_idx:]
        else:
            return
        if rest and rest[0] == "moves":
            for mv in rest[1:]:
                try:
                    self.board.push_uci(mv)
                except (ValueError, AssertionError):
                    self._info_string(f"illegal move in position: {mv}")
                    return

    def cmd_go(self, args: list[str]) -> None:
        # We mostly ignore time-control args. CDB lookups are essentially
        # free (the work happens on CDB's side). The GUI may send `infinite`
        # for analysis mode; that's our normal mode anyway.
        self.cmd_stop()  # cancel any in-flight worker
        self._stop_event.clear()
        self._start_time = time.time()
        root_fen = self.board.fen()
        with self._worker_lock:
            self._worker = threading.Thread(
                target=self._run_search,
                args=(root_fen,),
                name="cdb-worker",
                daemon=True,
            )
            self._worker.start()

    def cmd_stop(self) -> None:
        self._stop_event.set()
        with self._worker_lock:
            t = self._worker
        if t and t.is_alive():
            t.join(timeout=2.0)

    def cmd_quit(self) -> None:
        self.cmd_stop()
        self.logger.close()

    # ---- Search worker ---------------------------------------------------

    def _run_search(self, root_fen: str) -> None:
        """Top-level worker.

        The naive design (re-poll every cached position before each emit)
        becomes invisible to the user when the tree is wide: a 64-position
        tree at ~300 ms/query takes 20+ seconds per cycle, far longer than
        any sensible PollingRate. The GUI sees one info burst at startup
        and nothing more until you change positions.

        So we split the work into two cadences:

        - Root tier (every PollingRate seconds): re-query just the root
          via queryall + querypv, then emit MultiPV info lines. Two HTTP
          calls per cycle, sub-second response in practice.

        - Tree tier (every TreeRefreshInterval seconds): re-query every
          interior position. This refreshes the data backing PV lookups
          and trend tracking, but doesn't gate the emit cadence.
        """
        try:
            self._expand_tree(root_fen)
            self._emit_root_info(root_fen, cycle=1)
            cycle = 1
            last_full_refresh = time.time()

            while not self._stop_event.wait(self.config.polling_rate):
                cycle += 1
                tick_start = time.time()

                # Tier 1: refresh root every cycle.
                self._poll_position(root_fen, with_pv=True)

                # Tier 2: refresh wider tree on its own (slower) interval.
                did_full = False
                if (tick_start - last_full_refresh) >= self.config.tree_refresh_interval:
                    self._repoll_rest(root_fen)
                    last_full_refresh = time.time()
                    did_full = True

                self._info_string(
                    f"poll cycle={cycle} root-refreshed"
                    + (" + tree-refreshed" if did_full else "")
                    + f" elapsed={int((time.time() - tick_start)*1000)}ms"
                )
                self._emit_root_info(root_fen, cycle=cycle)
        except Exception as e:
            self._info_string(f"worker exception: {e!r}")
            self.logger("WORKER TRACEBACK\n" + traceback.format_exc())
        finally:
            self._emit_bestmove(root_fen)

    def _poll_position(self, fen: str, with_pv: bool = False) -> None:
        """Refresh one position. Always queryall; querypv only on request."""
        moves = self.cdb.queryall(fen)
        pv = self.cdb.querypv(fen) if with_pv else None
        self.tree.update(fen, moves=moves, pv=pv)

    def _repoll_rest(self, root_fen: str) -> None:
        """Re-query every cached position except root (root is handled in
        the main tick). Used for the slower tree-refresh tier."""
        for fen in self.tree.known_fens():
            if fen == root_fen or self._stop_event.is_set():
                continue
            moves = self.cdb.queryall(fen)
            self.tree.update(fen, moves=moves)

    def _expand_tree(self, root_fen: str) -> None:
        """Breadth-first expansion of the variation tree from root_fen.

        At each visited node we run `queryall` to capture top moves; at the
        leaves of the expansion frontier we additionally call `querypv` so
        the emitted UCI PVs extend past the BFS horizon when CDB has more.
        """
        c       = self.config
        visited = set()
        # Frontier: list of (fen, depth, board_state). We track the board
        # explicitly so we don't rebuild from scratch with each push.
        frontier = [(root_fen, 0)]
        while frontier and not self._stop_event.is_set():
            fen, depth = frontier.pop(0)
            if fen in visited:
                continue
            visited.add(fen)

            moves = self.cdb.queryall(fen)
            if not moves:
                if c.queue_unknown:
                    self.cdb.queue(fen)
                self.tree.update(fen, moves=[], pv=None)
                continue

            # At leaf depth we want a deeper PV than what we'll BFS-expand.
            pv = None
            if depth >= c.expansion_depth:
                pv = self.cdb.querypv(fen)
            self.tree.update(fen, moves=moves, pv=pv)

            # Emit a partial root view as soon as the root itself is back.
            if fen == root_fen:
                self._emit_root_info(root_fen, cycle=0, partial=True)

            # Schedule expansion of the top-N moves.
            if depth < c.expansion_depth:
                board = chess.Board(fen)
                width = min(c.expansion_width, len(moves))
                for m in moves[:width]:
                    try:
                        move = chess.Move.from_uci(m["move"])
                        if move not in board.legal_moves:
                            continue
                        board.push(move)
                        new_fen = board.fen()
                        board.pop()
                        if new_fen not in visited:
                            frontier.append((new_fen, depth + 1))
                    except (ValueError, AssertionError):
                        continue

    # ---- Output ----------------------------------------------------------

    def _emit_root_info(self, root_fen: str, cycle: int,
                        partial: bool = False) -> None:
        """Emit MultiPV info lines for the root position."""
        node = self.tree.get(root_fen)
        if node is None or not node.moves:
            if partial:
                return
            self._info_string("ChessDB returned no data for the root position")
            return

        elapsed_ms = max(1, int((time.time() - self._start_time) * 1000))
        nodes      = self._estimated_nodes()
        nps        = max(1, int(nodes * 1000 / elapsed_ms))
        depth_root = node.pv["depth"] if node.pv else 0
        trend      = node.trend()

        # Trend / cycle status line for human visibility.
        trend_str = ""
        if trend != 0:
            trend_str = f" trend {trend:+d}cp"
        self._info_string(
            f"cycle {cycle} polled {node.poll_count}x "
            f"tree {len(self.tree.known_fens())} nodes{trend_str}"
        )

        for i, mv in enumerate(node.moves[:self.config.multipv]):
            multipv = i + 1
            score_str = self._format_score(mv["score"])
            pv_uci    = self._build_pv(root_fen, mv["move"])
            depth     = max(depth_root, len(pv_uci))
            rank_str  = CDB_RANK_LABEL.get(mv["rank"], "?")
            note      = mv.get("note", "")
            extra_bits = []
            if rank_str:
                extra_bits.append(f"rank={rank_str}")
            if mv.get("winrate"):
                extra_bits.append(f"wr={mv['winrate']}")
            if note:
                extra_bits.append(f"note={note}")
            if extra_bits:
                self._info_string(
                    f"multipv {multipv} {mv['move']}: " + " ".join(extra_bits)
                )
            self._send(
                f"info multipv {multipv} depth {depth} seldepth {len(pv_uci)} "
                f"score {score_str} nodes {nodes} nps {nps} time {elapsed_ms} "
                f"pv {' '.join(pv_uci)}"
            )

    def _build_pv(self, root_fen: str, first_uci: str) -> list[str]:
        """Construct the PV displayed for a given root candidate.

        Walk the cached tree as far as it goes, then splice CDB's `querypv`
        result onto the tail when we run out of cached data. Capped at
        config.pv_depth plies to avoid hostile-length info lines.
        """
        max_plies = self.config.pv_depth
        board     = chess.Board(root_fen)
        try:
            board.push_uci(first_uci)
        except (ValueError, AssertionError):
            return [first_uci]
        pv: list[str] = [first_uci]

        while len(pv) < max_plies:
            fen = board.fen()
            node = self.tree.get(fen)
            if node is None:
                # No cached data for this position; ask CDB for its PV inline.
                cdb_pv = self.cdb.querypv(fen)
                if cdb_pv:
                    self.tree.update(fen, pv=cdb_pv)
                    if not self._extend_pv_with_cdb(pv, board, cdb_pv["pv"], max_plies):
                        break
                break
            # Prefer CDB's full PV from this cached position when we have it.
            if node.pv and node.pv.get("pv"):
                if not self._extend_pv_with_cdb(pv, board, node.pv["pv"], max_plies):
                    break
                break
            # Otherwise step one move using the cached top move and recurse.
            if not node.moves:
                break
            top = node.moves[0]["move"]
            try:
                move = chess.Move.from_uci(top)
                if move not in board.legal_moves:
                    break
                board.push(move)
                pv.append(top)
            except (ValueError, AssertionError):
                break

        return pv

    @staticmethod
    def _extend_pv_with_cdb(pv: list[str], board: chess.Board,
                            cdb_pv: list[str], max_plies: int) -> bool:
        """Append CDB PV moves to `pv`, validating each one is legal.
        Returns False if a move turned out to be illegal (PV is truncated)."""
        for uci in cdb_pv:
            if len(pv) >= max_plies:
                return True
            try:
                move = chess.Move.from_uci(uci)
            except ValueError:
                return False
            if move not in board.legal_moves:
                return False
            board.push(move)
            pv.append(uci)
        return True

    @staticmethod
    def _format_score(cdb_score: int) -> str:
        """Convert CDB's centipawn score (side-to-move POV) to UCI's
        `cp <n>` or `mate <n>` form."""
        if cdb_score >= MATE_THRESHOLD:
            mate_in = max(1, MATE_FAKE - (cdb_score - MATE_THRESHOLD) // 2)
            return f"mate {mate_in}"
        if cdb_score <= -MATE_THRESHOLD:
            mate_in = -max(1, MATE_FAKE - (-cdb_score - MATE_THRESHOLD) // 2)
            return f"mate {mate_in}"
        return f"cp {cdb_score}"

    def _estimated_nodes(self) -> int:
        """A loose 'nodes' tally so GUIs that show nodes/nps have something
        non-zero to display. We count one 'node' per polled position-cycle."""
        total = 0
        for fen in self.tree.known_fens():
            n = self.tree.get(fen)
            if n is not None:
                total += max(1, n.poll_count)
        return total

    def _emit_bestmove(self, root_fen: str) -> None:
        node = self.tree.get(root_fen)
        if not node or not node.moves:
            self._send("bestmove 0000")
            return
        best = node.moves[0]["move"]
        # Look up ponder move from the resulting position.
        ponder = None
        try:
            board = chess.Board(root_fen)
            board.push_uci(best)
            child = self.tree.get(board.fen())
            if child and child.moves:
                ponder = child.moves[0]["move"]
        except (ValueError, AssertionError):
            ponder = None
        if ponder:
            self._send(f"bestmove {best} ponder {ponder}")
        else:
            self._send(f"bestmove {best}")


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------

def main() -> int:
    engine = ChessDBEngine()
    # On Windows GUIs sometimes send CRLF; strip handles that.
    for raw in sys.stdin:
        line = raw.strip()
        if not line:
            continue
        engine.logger(f"IN  {line}")
        parts = line.split()
        cmd, args = parts[0], parts[1:]
        try:
            if   cmd == "uci":         engine.cmd_uci()
            elif cmd == "isready":     engine.cmd_isready()
            elif cmd == "ucinewgame":  engine.cmd_ucinewgame()
            elif cmd == "setoption":   engine.cmd_setoption(args)
            elif cmd == "position":    engine.cmd_position(args)
            elif cmd == "go":          engine.cmd_go(args)
            elif cmd == "stop":        engine.cmd_stop()
            elif cmd == "ponderhit":   pass  # not meaningful for a CDB engine
            elif cmd == "quit":
                engine.cmd_quit()
                return 0
            elif cmd == "debug":       pass
            elif cmd == "register":    pass
            else:
                engine.logger(f"unknown command: {cmd}")
        except Exception as e:
            sys.stderr.write(f"command error: {e!r}\n")
            engine.logger("EXCEPTION\n" + traceback.format_exc())
    engine.cmd_quit()
    return 0


if __name__ == "__main__":
    sys.exit(main())
