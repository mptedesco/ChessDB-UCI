"""
chessdb_uci.py — A UCI "engine" that exposes ChessDB (chessdb.cn) as if it were
a local search engine. Instead of running a tree search, every "info" line is
backed by ChessDB's distributed min-max tree.

For more information on the ChessDB tool and project see:

https://www.chessdb.cn/queryc_en/

and

https://www.chessdb.cn/cloudbookc_api_en.html

Why this exists
---------------
For correspondence/ICCF preparation, ChessDB is often the most authoritative
single source. Point any UCI-compliant GUI (e.g., 
ChessBase, HIARCS, SCID) at this script and
ChessDB shows up as just another engine slot. It will optionally send requests to
ChessDB to add positions to the analysis queue if they are not already evaluated
in the ChessDB tree.
 
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

NOTE: It may look like the engine is not doing anything.  This is because 
ChessDB often needs some time to perform the analysis on tasks in the queue.
But if you let it sit for a bit you will see the variations do expand over time.
My recommended use case is to have this running in parallel with other engines and 
you can see when and if there are disparities.  This would be a clue that the position
is worth some time to investigate further.
 
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
 
Dependencies for the Python Code
------------
- python-chess (`pip install python-chess`)
- Standard library only otherwise.
 
Usage
-----
Configure your GUI to point to the exe file
