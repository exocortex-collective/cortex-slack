# Configuration

The four reviewers this repo adds beyond the family baseline, and why:

| Reviewer | Why it is here |
|----------|----------------|
| `clock-discipline-reviewer` | From cortex-utils. Every poll compares an API `updated_at` against a Postgres-stamped column; a second clock here loses notes. |
| `transactional-decision-reviewer` | From cortex-utils. `ingest_one()` deliberately does not commit, so the caller owns a transaction spanning a raw upsert and a queue enqueue. |
| `watermark-integrity-reviewer` | Repo-specific. The incremental watermark is the only component here whose failure mode is silent data loss. |
| `export-destructiveness-reviewer` | Repo-specific. The export worker deletes files inside the user's Obsidian vault. |

`defaults_version_checked` is pinned to the plugin version these additions were
authored against -- not copied from a sibling repo, whose audit says nothing
about this file.

```json
{
  "defaults_version_checked": "1.5.1",
  "disabled": [
    "code-simplifier",
    "comment-analyzer",
    "silent-failure-hunter"
  ],
  "overlap_acknowledged": {
    "test-coverage-reviewer": {
      "overlaps_with": "pr-test-analyzer",
      "reason": "Different lenses: test-coverage-reviewer enforces hard rules (touched code must be covered; mocking discipline; missing assertions; skip-without-reason). pr-test-analyzer scores behavioral gaps on a criticality 1-10 axis. Both contribute independent signal during review loops."
    },
    "watermark-integrity-reviewer": {
      "overlaps_with": "clock-discipline-reviewer",
      "reason": "clock-discipline asks which clock produced a value; watermark-integrity asks whether the stored value may move at all given what the run did. A client-clock watermark is both findings, and both are correct."
    }
  },
  "independent_validator": {
    "enabled": true,
    "skip_for": [],
    "uncertain_action": "post_with_annotation"
  }
}
```

# Agents

## test-coverage-reviewer

Review code changes to **ensure the code touched by the PR has test coverage**. The goal is to incrementally grow the test suite — every PR either adds coverage or preserves it.

**Core rule: Code touched by a PR must be covered by tests. Either the coverage already exists, or this PR adds it.**

The unit of obligation is *coverage of the touched code*, not *net-new test functions for every diff*. A pure rename of a well-tested function does not need a new test; adding a new branch to that function does.

**Rules to enforce:**

1. **Coverage required for all touched code.** For any modified or new function/method, verify that *some* test exercises it — existing or new. If the function is uncovered today, this PR adds coverage. Exceptions exist (see below) but require a tracked beads ticket created before merge.

2. **Refactors preserve coverage, not duplicate it.** Pure refactors (rename, extract-function, signature reshape with no behavior change) do not need net-new tests if existing tests still exercise the refactored code and stay green. Flag a refactor only when the touched code was uncovered before the refactor — that's the moment to add the test, not after.

3. **New behavior needs new assertions.** Adding a branch, exception path, or output shape to an already-tested function requires a new test case (or parametrize entry) that hits the new behavior. Reusing the existing test name without new assertions is not coverage.

4. **Test file naming and structure.** Tests live in `tests/test_<module>.py` matching the module being tested (e.g. `mypkg/store.py` → `tests/test_store.py`). Recognize as valid coverage:
   - Top-level `def test_*(...)` functions
   - `class Test...:` test classes with `def test_*` methods
   - `@pytest.mark.parametrize` parametrized tests (each parameter set counts as a distinct test case)
   - Async tests via `@pytest.mark.asyncio` or `pytest-asyncio` auto mode
   - `@pytest.mark.parametrize` indirect-via-fixture forms (still counts)
   - Doctest blocks if `--doctest-modules` runs in CI
   - Fixtures in `conftest.py` are infrastructure, not tests — verify they're consumed by an actual `test_*` function

5. **Bug fix documentation.** If the change fixes a bug:
   - Require a comment or commit message explaining what was broken and why the fix works.
   - Require a regression test that would have failed before the fix.

**Integration tests as coverage — acceptable boundary:**

Integration tests count as coverage when the unit boundary is **glue code with no branching logic** — e.g., a Click command that parses args and calls one function, or a Flask route that decodes JSON and dispatches to a service. Demanding a separate unit test for such glue produces low-value mock-heavy tests.

Integration tests do **not** count when the unit being touched has its own branching, validation, parsing, or business logic that can be exercised in isolation. In that case the agent must flag the missing unit test even if an integration test exists.

**Test quality patterns to flag (P2):**

Beyond coverage existence, the reviewer also flags tests whose behavior is broken or weak. These are P2 — the PR is salvageable with a same-cycle fix.

1. **`mock.patch` at point-of-definition instead of point-of-use:**

   ```python
   # In mypkg/module.py
   import os
   def check(path):
       return os.path.exists(path)

   # BAD — doesn't intercept the call inside mypkg.module
   @mock.patch("os.path.exists")
   def test_check(mock_exists):
       ...

   # GOOD — patches the symbol that mypkg.module actually consults
   @mock.patch("mypkg.module.os.path.exists")
   def test_check(mock_exists):
       ...
   ```

   `mypkg.module` did `import os` at module load — it has its OWN reference to `os.path.exists`. Patching `os.path.exists` rebinds the original module's attribute, but mypkg.module's local-bound reference still points at the original function. This is the single most common reason `mock.patch` "doesn't work."

2. **Mocking what you don't own:**

   ```python
   # BAD — couples the test to the library's internals
   @mock.patch("requests.get")
   def test_fetch_user(mock_get):
       mock_get.return_value = ...

   # GOOD — mock at YOUR boundary
   @mock.patch("mypkg.http_client.fetch")
   def test_fetch_user(mock_fetch):
       mock_fetch.return_value = ...
   ```

   Mock at the seam between your code and theirs. Mocking the third-party library directly forces the test to rewrite itself every time the library changes its internals — and ties you to library-specific quirks that have nothing to do with what you're verifying.

3. **Over-mocking:**

   A test that mocks five or more collaborators is verifying very little — it's mostly testing that the mocks return what the mocks return. Flag tests where the mock count exceeds the assertion count, or where most of the test body is `mock_X.return_value = ...` setup. The right fix is usually to test at a higher level (the integration boundary) or to refactor the code under test for fewer collaborators.

4. **`@pytest.mark.skip` without `reason=`:**

   ```python
   # BAD — dead test, no plan to unskip
   @pytest.mark.skip
   def test_thing():
       ...

   # GOOD — reason documents the deferral and (ideally) the unblock condition
   @pytest.mark.skip(reason="blocked on beads-1234; remove after fix lands")
   def test_thing():
       ...
   ```

   A skipped test with no reason is a dead test. Either fix it now, file a beads ticket and reference it in the reason, or delete the test entirely. The same rule applies to `@pytest.mark.skipif(condition)` — the condition should be self-documenting (`skipif(sys.platform == "win32", reason="POSIX-only feature")`).

5. **Tests that don't assert anything:**

   ```python
   # BAD — verifies only that the import works and the function doesn't raise
   def test_compute():
       compute(x=1, y=2)

   # GOOD — verifies behavior
   def test_compute():
       assert compute(x=1, y=2) == 3
   ```

   Flag the absence of `assert`, `pytest.raises`, `mock.assert_called_with`, `mock.assert_called_once`, `mock.assert_not_called`, or equivalent. Tests that don't assert look like coverage but verify nothing meaningful — the function could return any value and the test would still pass.

   Acceptable exception: tests whose body is entirely a `with pytest.raises(...):` block (the raises IS the assertion), or smoke tests that explicitly use `pytest.fail()` if a condition isn't met (rare).

**Review approach:**

1. Identify all functions/methods added or modified in the PR.
2. For each, grep `tests/` for tests that name or call it.
3. For modifications, verify the new behavior path is asserted, not just compiled.
4. If coverage is missing, post a comment naming the specific functions and suggesting test cases based on edge cases visible in the code.
5. Distinguish between "no coverage" (block), "coverage exists but doesn't hit the new branch" (block), and "pure refactor of already-covered code" (allow).
6. For each new or modified test file, scan for the five test-quality patterns: `mock.patch` targets, mocks of third-party libraries, mock count vs assertion count, `@pytest.mark.skip` without `reason=`, and missing assertions.

**What to flag:**

- New public functions/methods with no test.
- Modified functions where the modification's branch isn't asserted.
- New exception paths with no test that triggers the exception.
- Edge cases visible in the code (`None` inputs, empty collections, boundary numbers) without assertions.

**Do NOT allow:**

- "Verified manually via CLI" or "I ran it once" as a substitute when the unit has branching logic.
- Marking test coverage as "out of scope" without an accompanying beads ticket.
- Adding a new branch to a tested function without an assertion that hits the new branch.

**Acceptable exceptions (each requires a beads ticket created before merge):**

- Adding tests to legacy uncovered code is genuinely larger than this PR.
- The change is a config/data file edit with no executable logic.
- The change is a dependency bump with no source change in this repo.

---

## error-handling-reviewer

Review Python code for **error handling correctness**. The focus is on errors that vanish: exceptions caught and discarded, exceptions converted to defaults without logging, exception context stripped by missing `raise from`. Silent error handling makes failures impossible to investigate after the fact — the original traceback is the most valuable debugging signal you have, and discarding it deletes the investigation trail.

This reviewer enforces universal Python error-handling hygiene. It does **not** take a stance on whether a codebase should be defensively forgiving (web apps where `user.get("name", "Anonymous")` is idiomatic) or aggressively brittle (parsers / data pipelines where any unexpected structure should fail loudly). That's a project-level architectural choice and lives in the project's own AGENT-REVIEWERS.md.

**Patterns to FLAG:**

1. **Silent exception swallowing — the most damaging pattern (P1):**

   ```python
   # BAD — exception silently discarded
   try:
       parse_data(html)
   except Exception:
       pass

   # BAD — default return masks the failure
   try:
       return parse_data(html)
   except Exception:
       return {}
   ```

   If parsing fails, callers see `{}` and assume success. The original error never reaches the operator. The traceback is lost and the bug is invisible until downstream code chokes on missing fields.

2. **`return` statement inside a `finally` block (P1):**

   ```python
   # BAD — return in finally silences any exception raised in try
   try:
       return do_something()
   finally:
       return default  # swallows any exception
   ```

   Python's semantics: a `return` (or `raise`) in `finally` overrides any pending exception. This is almost always a bug.

3. **Generic `except Exception` without re-raise (P2):**

   ```python
   # BAD — catches everything, logs, continues silently
   try:
       do_complex_thing()
   except Exception as e:
       logger.warning(f"Error: {e}")
       # implicit None return
   ```

   If a caller branches on the return value, "warning logged + None returned" looks identical to "everything was fine, here's None." Either re-raise after logging, or document why a sentinel return is correct.

4. **Bare `except:` (catches `BaseException`) (P2):**

   ```python
   # BAD — also catches KeyboardInterrupt and SystemExit
   try:
       work()
   except:
       cleanup()
   ```

   Use `except Exception:` at minimum. `BaseException` includes `KeyboardInterrupt` and `SystemExit` — catching them turns Ctrl-C and `sys.exit()` into silent no-ops.

5. **Missing exception chaining (`raise ... from`) (P3):**

   ```python
   # BAD — original exception context lost
   try:
       value = int(text)
   except ValueError:
       raise ParseError("invalid number")

   # GOOD — preserves the cause for the traceback
   try:
       value = int(text)
   except ValueError as e:
       raise ParseError(f"invalid number: {text!r}") from e
   ```

   Use `raise ... from e` to preserve the original exception in the traceback. Use `raise ... from None` only when deliberately suppressing the cause is correct (rare — usually it's not).

6. **Custom exception classes for caller-handleable cases (P3):**

   Errors that callers will branch on programmatically (not-found, already-exists, validation-failure) should be declared as classes inheriting from a meaningful base:

   ```python
   # GOOD
   class LexiconError(Exception):
       """Base class for lexicon-layer errors."""

   class EntryNotFound(LexiconError):
       pass

   def get_entry(game_id: str) -> Entry:
       row = db.fetchone("SELECT * FROM entries WHERE game_id = ?", (game_id,))
       if row is None:
           raise EntryNotFound(game_id)
       return Entry.from_row(row)
   ```

   Flag ad-hoc `raise ValueError("not found")` in places where the caller clearly needs to detect the case but can't, because `ValueError` is too generic to match safely.

7. **`try/finally` for cleanup when a context manager would do (P3):**

   ```python
   # BAD — manual cleanup that early returns can skip
   f = open(path)
   try:
       data = f.read()
   finally:
       f.close()

   # GOOD
   with open(path) as f:
       data = f.read()
   ```

8. **`assert` used for runtime validation rather than invariants (P2):**

   ```python
   # BAD — asserts can be stripped with python -O
   def process(user_input):
       assert user_input is not None, "input required"

   # GOOD — actual validation
   def process(user_input):
       if user_input is None:
           raise ValueError("input required")
   ```

   `assert` is for internal invariants the developer knows must be true. Use real validation for user-facing checks.

**Acceptable patterns:**

1. **Assertions with context (fail-fast on internal invariants):**

   ```python
   assert len(children) == 2, f"Expected 2 children, got {len(children)}"
   ```

2. **Specific exception handling with `raise ... from`:**

   ```python
   try:
       value = int(text.strip())
   except ValueError as e:
       raise ParseError(f"Invalid number: {text!r}") from e
   ```

3. **Known exception handling for documented expected cases:**

   ```python
   try:
       score = int(stat.strip())
   except ValueError:
       # Known case: stat blocks use "-" for missing ability scores.
       # None is the correct value for this case.
       score = None
   ```

   The "Known case" comment makes the intentional choice explicit and reviewable.

4. **Bare `except` with `raise` for cleanup:**

   ```python
   try:
       work()
   except:
       cleanup()
       raise  # re-raises the original exception
   ```

5. **`contextlib.suppress` for genuinely-ignorable cases:**

   ```python
   from contextlib import suppress

   with suppress(FileNotFoundError):
       os.remove(temp_path)
   ```

   Better than `try/except FileNotFoundError: pass` because intent is explicit and the diff stays a one-liner.

**Review approach:**

1. Grep for `except` patterns: `except Exception:`, `except:`, `except (...)`. For each, confirm the handler either logs AND re-raises, returns the correct value for a documented case (with comment), or has another defensible justification.
2. Grep for `pass` immediately after `except`. Flag as P1.
3. Grep for `return` inside `finally`. Flag as P1.
4. Grep for `try` blocks that could be `with` statements (i.e., the body acquires and the `finally` releases a resource).
5. Grep for `raise X(...)` after `except`. Verify `from e` (or `from None` with justification) is present.
6. For each ad-hoc `raise ValueError(...)` or `raise RuntimeError(...)` in places where callers might want to detect the case, suggest a custom exception class.

---

## logging-reviewer

Review Python code for **logging discipline**. Logging is observability infrastructure: it determines what operators see in production when things go wrong, and it's also a security boundary (logs are often shipped off-host and indexed by tools that don't redact secrets). The reviewer covers two contexts with different rules:

- **Libraries and services** — long-running code that ships logs to operators.
- **CLI tools** — entry points that follow Unix's rule of silence: terse by default, verbose on request.

### Patterns to FLAG — universal (apply in any context)

1. **Sensitive data in log messages (P1 — security leak):**

   ```python
   # BAD — credentials in logs that ship off-host
   logger.info(f"user={user}, token={token}")
   logger.debug(f"request headers: {request.headers}")  # often includes Authorization
   ```

   Flag string interpolation of variables named `token`, `password`, `secret`, `api_key`, `credential`, `auth`, `cookie`, `session`, `private_key`, or `bearer` into any log call. Also flag dumping entire request/response objects without redaction — `request.headers` typically contains `Authorization`.

   Acceptable: explicit redaction (`token[:8] + "..."`), or logging a hash/fingerprint instead of the value.

2. **`logger.error(...)` inside an `except` block without `exc_info=True`:**

   ```python
   # BAD — traceback silently discarded
   try:
       work()
   except SomeError as e:
       logger.error(f"work failed: {e}")
       raise

   # GOOD — exception() is shorthand for error + exc_info=True
   try:
       work()
   except SomeError:
       logger.exception("work failed")
       raise
   ```

   `logger.exception()` captures the full traceback in the log record automatically. Use it inside `except` blocks unless you have a specific reason to omit the traceback.

3. **`logging.basicConfig()` called from library code:**

   ```python
   # BAD — library hijacks the host application's logging setup
   # mypkg/__init__.py
   import logging
   logging.basicConfig(level=logging.INFO)
   ```

   Libraries should call `logging.getLogger(__name__)` and let the application decide where logs go and how they're formatted. `basicConfig` belongs in entry points, never in library modules.

4. **Eager formatting in hot debug paths:**

   ```python
   # BAD in hot paths — formats unconditionally, even when DEBUG is filtered out
   logger.debug(f"processed {item.expensive_repr()}")

   # GOOD — lazy formatting; __str__ only called if DEBUG level is enabled
   logger.debug("processed %s", item)
   ```

   The performance gap matters in hot paths (request handlers, parsers, tight loops). The side-effect gap matters because `f"..."` calls `__str__`/`__repr__` unconditionally — which can be expensive, or can leak values through repr that you didn't intend.

### Patterns to FLAG — CLI entry points only

A file is treated as a **CLI entry point** when any of the following is true:

- Contains `if __name__ == "__main__":`
- Uses `click` decorators (`@click.command`, `@click.group`) or builds an `argparse.ArgumentParser`
- Located under `bin/` or `scripts/`
- Is the target of a `[project.scripts]` / `entry_points` declaration in `pyproject.toml` / `setup.cfg`

CLI tools follow Unix's rule of silence: *when a program has nothing surprising to say, it should say nothing.* Be sparse by default; verbose output is opt-in via flags.

5. **Default log level too verbose:**

   ```python
   # BAD — INFO at default makes the CLI noisy on every run
   logging.basicConfig(level=logging.INFO)

   # GOOD — silent unless something's wrong
   logging.basicConfig(level=logging.WARNING)
   ```

   CLIs should default to WARNING. INFO and DEBUG belong behind verbosity flags.

6. **No verbosity flag:**

   A CLI that uses `logging` (or emits diagnostic output) without exposing a verbosity flag gives the user no way to dial up debug output when needed. Recommend the canonical ladder:

   | Flag | Level | What surfaces |
   |------|-------|---------------|
   | (none) | WARNING | Only problems |
   | `-v` | INFO | Milestones, decisions |
   | `-vv` | DEBUG | Operational detail |
   | `-vvv` | DEBUG + extras | HTTP bodies, SQL queries, etc. |
   | `-q` / `--quiet` | ERROR | Suppress warnings — errors only |

   Recognized as correct (do NOT flag):

   ```python
   # Click
   @click.option("-v", "--verbose", count=True)
   @click.option("-q", "--quiet", is_flag=True)
   def main(verbose, quiet):
       if quiet:
           level = logging.ERROR
       else:
           level = max(logging.WARNING - verbose * 10, logging.DEBUG)
       logging.basicConfig(level=level, stream=sys.stderr)
   ```

   ```python
   # argparse
   parser.add_argument("-v", "--verbose", action="count", default=0)
   parser.add_argument("-q", "--quiet", action="store_true")
   args = parser.parse_args()
   if args.quiet:
       level = logging.ERROR
   else:
       level = max(logging.WARNING - args.verbose * 10, logging.DEBUG)
   logging.basicConfig(level=level, stream=sys.stderr)
   ```

7. **Verbosity flag that doesn't stack:**

   ```python
   # BAD — -vv has no more effect than -v
   parser.add_argument("-v", "--verbose", action="store_true")
   ```

   `action="count"` is the correct argparse form. For Click, `count=True`.

8. **`print()` for diagnostic output in CLI tools:**

   ```python
   # BAD — diagnostics on stdout, mixed with the data the user asked for
   print(f"Processing {filename}...")
   print(json.dumps(result))

   # GOOD — diagnostics on stderr via the logger; only the result on stdout
   logger.info("processing %s", filename)
   print(json.dumps(result))
   ```

   Stdout is for the data the user requested; stderr is for everything else (progress, warnings, errors). Mixing them breaks pipelines (`mycli | jq`).

### Patterns to FLAG — library / service only

9. **`print()` in library or service code:**

   In libraries and long-running services, `print()` is almost always wrong — it bypasses the logger, can't be filtered by level, doesn't include timestamps or correlation IDs, and goes to stdout (where it can corrupt structured output meant for downstream consumers). Flag `print()` in any module that isn't a CLI entry point.

   Acceptable: `print()` in a `__main__` block that is intentionally a quick-and-dirty entry point with no logging configured.

10. **Log-level discipline:**

    Everything emitted at INFO drowns real signal. Recommend the canonical levels:
    - **DEBUG** — fine-grained operational detail, traces, intermediate state.
    - **INFO** — milestones (worker started, request received, batch completed).
    - **WARNING** — recoverable issues (retry happened, fallback used, deprecated path hit).
    - **ERROR** — actionable failures (request failed, write rejected).
    - **CRITICAL** — process-level emergencies (out of disk, lost master connection).

    Flag patterns like `logger.info(f"checking {x}")` in a hot loop (should be DEBUG), or `logger.error("retrying...")` on a recoverable retry (should be WARNING).

### Do NOT flag

- `print()` in tests, ad-hoc scripts, and Jupyter notebooks.
- `print()` in a `__main__` block that is intentionally a quick-and-dirty entry point with no logger configured.
- Logging configuration in a single canonical entry point of an application (that's exactly where it belongs).
- Eager formatting in cold paths (a once-at-startup log line). The lazy-formatting rule only matters in hot loops.
- CLI tools that use `click.echo()` / `click.secho()` — these are Click's stderr-aware print equivalents and respect Click's output conventions.

### Review approach

1. For each `*.py` file in the diff, classify as CLI entry point, library, or service.
2. Grep for `logger.` / `logging.` / `print(` calls.
3. For each log call: check for sensitive variable names interpolated, `logger.error` inside `except` (should be `logger.exception`), eager `f"..."` formatting in hot paths.
4. For CLI files: check default level, verbosity flag presence + stacking, stdout/stderr discipline.
5. For library files: flag any `basicConfig` call (P2).
6. For `print()`: classify by file type; flag in libraries/services, allow in CLIs only for stdout data output.

---

## complexity-reviewer

Review **production code only** for function complexity. **Skip all files in `tests/`** — test files often have long fixtures, parametrize tables, and assertion blocks that don't need the same complexity constraints.

**Objective floor: `ruff check --select C901 --max-complexity 10 .` must pass on every PR.** McCabe cyclomatic complexity above 10 in a single function is a hard signal, independent of the subjective heuristics below. The agent should run `ruff check --select C901` (or accept CI's output) and flag every function that exceeds the threshold.

Apply these heuristics on top of the ruff floor:

1. **"And/Or" test**: Minimize the number of "and" or "or" needed to describe what a function does. If you need multiple conjunctions, the function is doing too much.
   - Good: "This function extracts traits from a stat block."
   - Bad: "This function extracts traits AND normalizes them AND adds links AND handles edge cases for variants."

2. **One-screen rule**: Functions should fit on one screen (~50–60 lines). Longer functions are harder to reason about.
   - **Internal functions don't count**: When a function contains internal helper `def`s, the lines of those helpers do NOT count against the parent's line limit. Only the "main body" lines count.
   - **Internal functions at the top**: Internal private functions should be declared at the top of their parent function, before the main logic begins.

3. **Extractable inner structures**: If a block of code has a clear purpose, consider extraction:
   - **First choice**: Extract to module level (prefixed with `_`) if reusable within the file.
   - **Second choice**: Extract to a sibling helper module within the same package.
   - **Last choice**: Keep as an internal function if truly tied to the parent's context.

4. **Nesting depth**: Flag functions with **indent depth ≥ 4 inside the function body** (four levels of indentation beyond the `def` line). Deep nesting makes control flow hard to follow.
   - Python idiom: use early returns to reduce nesting (`if not x: return`).
   - Example of depth 4 (flag): `def f():\n    for x in xs:\n        if x:\n            with open(...) as f:\n                for line in f: ...`

5. **Parameter count**: Flag functions with more than **5 positional parameters**. Long parameter lists are a complexity smell — refactor to a config object (dataclass, TypedDict), keyword-only args (`def foo(*, a, b, c)`), or split the function. `ruff` catches some of this via `PLR0913` (default threshold 5); flag here even if ruff isn't configured for it.

6. **Class size (public method count)**: Flag classes with **more than 20 public methods** (`ruff PLR0904` default). Count only public methods — methods without a leading underscore and excluding dunders (`__init__`, `__eq__`, `__hash__`, etc.). **Private helper methods (`_foo`) do NOT count** — extracting helpers as private methods is exactly the pattern the rest of this reviewer encourages, so penalizing them would be self-contradictory. Dunders are protocol implementations, not logic.

   The class-size rule is **advisory (P3)**: when triggered, ask whether the public methods cluster around a single responsibility. All variations on one concern (different serializations of the same object, different query methods on the same store) → fine. Multiple unrelated concerns (serialize + persist + render + authorize) → suggest splitting the class along the responsibility lines.

7. **Nested ternaries**: Chained `x if a else y if b else z` is hard to read once you go past one level. Recommend `match`/`case`, an `if/elif/else` chain, or a dict-dispatch lookup.

   ```python
   # BAD — three levels, eyes can't follow
   status = "critical" if cpu > 90 else "warning" if cpu > 70 else "ok" if cpu > 0 else "unknown"

   # GOOD
   if cpu > 90:
       status = "critical"
   elif cpu > 70:
       status = "warning"
   elif cpu > 0:
       status = "ok"
   else:
       status = "unknown"
   ```

   A single ternary (`x if a else y`) is fine; flag at the second `if/else` inside an expression.

8. **Redundant single-call wrappers**: A function that exists only to call one other function, with no added validation, normalization, error context, or naming benefit. The wrapper is indirection the call site has to chase without getting anything back.

   ```python
   # BAD — wraps str.upper() for no reason
   def to_upper(s):
       return s.upper()

   # call site:
   shouted = to_upper(name)  # why not name.upper()?
   ```

   Wrappers that add validation, normalization, error context, or are reused enough that the rename earns its keep are fine. The pattern to flag is single-call, single-caller, no-added-meaning.

9. **Generic identifiers in long functions**: Variables and functions named `data`, `temp`, `result`, `value`, `obj`, `thing` in a function with multiple of each. At the call site, the reader has to look up the definition to know what `result` actually is.

   ```python
   # BAD — three different "result" variables
   def process(rows):
       result = parse(rows)
       result = filter(result)
       result = serialize(result)
       return result

   # GOOD — each transformation has a meaningful name
   def process(rows):
       parsed = parse(rows)
       filtered = filter(parsed)
       serialized = serialize(filtered)
       return serialized
   ```

   Flag only when the function is long enough that the generic name actively misleads at the call site. Short helper functions (≤10 lines) can use generic names — the body is small enough that the name is recoverable from context.

**Do NOT flag:**

- Test files (`tests/`).
- Functions that are long but linear (no branching, just sequential transformations like a parser pipeline) **up to ~100 lines**. Beyond that, still recommend extraction — a 200-line linear function is hard to review in chunks and impossible to test in pieces, even without branching.
- `match` / `if-elif-else` chains where each branch is **a short value→action mapping** (one or two statements per branch, often just a return or a function call). These are inherently flat.
- Functions whose length comes from a single long parametrize table or a single long literal data structure (dict/list).

**DO flag (match/elif-specific):**

- `match` statements or `if-elif-else` chains with **multi-statement branch bodies that do branching logic** (each branch has its own `if`/`for`/`try`). The flatness exemption applies to dispatch tables, not to chains of mini-functions in disguise. If a branch body would itself trigger any heuristic above, extract it to a helper.

**Note:** It is acceptable to acknowledge complexity and defer refactoring by creating a beads ticket, rather than fixing it in the current PR. This applies to heuristic findings; ruff `C901` violations should be resolved in-PR unless there's a documented reason.

---

## concurrency-reviewer

Review Python code for **concurrency correctness**: threads, processes, asyncio, and shared state. Python's concurrency model is messy (GIL, threading-vs-multiprocessing trade-offs, async/await coloring), and the dominant failure mode is silent data corruption from shared mutable state.

**Design philosophy: "Processes over threads. Queues over locks. Async message passing over shared state."** Threads with locks are a code smell — they look correct in single-function examples and almost always grow into race conditions, deadlocks, and GIL-bound work pretending to scale. **Empirical position of this reviewer:** developers using threading primitives get the concurrent code wrong approximately 100% of the time. The bugs are subtle, survive code review, pass tests, and surface as rare data corruption and hangs under production load. **Any new `threading.Lock` / `threading.RLock` / shared mutable global is a code smell** and must clear a high bar — not "is there a simpler alternative" but "would I defend this design choice in a postmortem."

**Top-priority pattern to FLAG — any new `threading.Lock` / `threading.RLock`:**

```python
# BAD — protecting shared state with a lock
class Cache:
    def __init__(self):
        self._lock = threading.Lock()
        self._data: dict[str, Any] = {}

    def get(self, key):
        with self._lock:
            return self._data.get(key)
```

```python
# GOOD — a worker process owns the data; the queue is the only interface
class Cache:
    def __init__(self):
        self._ops: queue.Queue = queue.Queue()
        threading.Thread(target=self._run, daemon=False).start()
    def _run(self):
        data = {}
        while (op := self._ops.get()) is not None:
            op(data)

# OR — an asyncio task owns the state; no lock needed (single event loop = no race)
class Cache:
    def __init__(self):
        self._data: dict[str, Any] = {}  # safe: only the cache task touches it
    async def get(self, key):
        return self._data.get(key)
```

Flag every new `threading.Lock`, `threading.RLock`, `threading.Semaphore` (used as mutex), `threading.Condition`, or any shared mutable global written from multiple threads. The author must justify why a process boundary + queue, or an asyncio task that owns the state, doesn't work for this case. Acceptable answers fall into "Tolerated uses" below; everything else should be rewritten.

**Other patterns to FLAG:**

1. **CPU-bound work in `ThreadPoolExecutor` (GIL trap):**

   ```python
   # BAD — threads don't parallelize CPU work in CPython
   with ThreadPoolExecutor() as exec:
       results = list(exec.map(parse_file, files))  # GIL-serialized
   ```

   Use `multiprocessing.Pool` or `concurrent.futures.ProcessPoolExecutor` for CPU-bound work.

2. **Fire-and-forget `asyncio.create_task` without a reference:**

   ```python
   # BAD — task reference dropped; GC may cancel the task mid-flight
   asyncio.create_task(background_work())

   # GOOD — keep a strong reference (or use TaskGroup in 3.11+)
   self._tasks.add(asyncio.create_task(background_work()))

   # GOOD (3.11+)
   async with asyncio.TaskGroup() as tg:
       tg.create_task(background_work())
   ```

   Per CPython docs: tasks held only by the loop may be garbage-collected. Keep a strong reference, or use `TaskGroup`.

3. **`asyncio.gather` swallowing exception context:**

   ```python
   # BAD — first exception propagates, but the rest of the tasks aren't awaited;
   # their results / exceptions are silently dropped
   await asyncio.gather(*tasks)
   ```

   `asyncio.gather(*tasks, return_exceptions=True)` collects everything; the default fails fast. Either form is fine — but make the choice deliberate, and don't assume "gather waited for all of them" if the default raised.

4. **Blocking `subprocess` calls in `async def`:**

   ```python
   # BAD — blocks the event loop
   async def fetch():
       subprocess.run(["git", "fetch"])
   ```

   In async contexts use `asyncio.create_subprocess_exec` (or `loop.run_in_executor` for code that genuinely can't be async).

5. **Missing `asyncio.timeout` / `asyncio.wait_for` on async I/O:**

   ```python
   # BAD — hangs forever if the remote stalls
   result = await fetch(url)

   # GOOD (3.11+)
   async with asyncio.timeout(30):
       result = await fetch(url)

   # GOOD (older)
   result = await asyncio.wait_for(fetch(url), timeout=30)
   ```

   Every external I/O in an async function should have a timeout.

6. **Mutable shared global written from multiple threads:**

   ```python
   # BAD
   _CACHE: dict[str, Any] = {}

   def worker(key):
       _CACHE[key] = compute(key)  # concurrent writers race
   ```

   Move the cache into a worker process behind a queue, use `threading.local()` for per-thread state, or use `functools.lru_cache` (thread-safe).

7. **Daemon threads that write data:**

   ```python
   # BAD — daemon thread killed mid-write at interpreter exit
   t = threading.Thread(target=write_loop, daemon=True)
   t.start()
   ```

   Daemon threads can leave files half-written or sockets half-closed. If the worker writes anything, it should not be a daemon; set up explicit shutdown signaling via a sentinel value on the queue or a `threading.Event`.

8. **`multiprocessing.Pool` constructed at module level:**

   ```python
   # BAD — fork() / spawn() runs the import path, which spawns the pool, recursively
   pool = multiprocessing.Pool(8)  # at module scope
   ```

   Pool construction at module load forks the program recursively on `spawn`-mode platforms (macOS default, Windows). Guard with `if __name__ == "__main__":`.

9. **`queue.Queue.get()` without `timeout=`:**

   ```python
   # BAD — worker hangs forever if the producer dies
   item = q.get()

   # GOOD
   item = q.get(timeout=30)
   ```

10. **Non-pickleable objects passed to `multiprocessing`:**

    Generators, lambdas, local functions, open file handles, database connections, and most context managers don't pickle. Flag any of these passed to `Pool.map`, `Process(target=...)`, or `Queue.put`.

11. **Mixing `asyncio` and `threading` without `run_in_executor`:**

    Calling blocking code from `async def` blocks the entire event loop. Use `loop.run_in_executor(None, blocking_call)` to bridge — and even then, keep the bridge thin.

12. **`nest_asyncio` (anti-pattern):**

    ```python
    # BAD — almost always a hack to call asyncio.run from inside a running loop
    import nest_asyncio
    nest_asyncio.apply()
    ```

    `nest_asyncio` patches `asyncio` to allow nested `asyncio.run()` calls from inside an existing event loop. This is almost always a sign that sync-and-async code is being bridged the wrong way (a sync API trying to internally run an async coroutine). Refactor so the async-ness is honest: either make the caller async, or move the coroutine's actual work behind a sync-callable boundary that doesn't require a fresh event loop.

13. **`asyncio.run()` called from anywhere except the entry point:**

    ```python
    # BAD — library function spinning up its own event loop
    def get_user(id):
        return asyncio.run(_async_get_user(id))

    # GOOD — let the caller decide; expose the coroutine
    async def get_user(id):
        return await _async_get_user(id)
    ```

    `asyncio.run()` creates and tears down an event loop. It belongs at the top of `main()` (or the test harness), not deep in library code. Multiple `asyncio.run()` calls in a call tree is a smell — each one is a fresh loop, defeats any connection pooling tied to the loop, and breaks if the caller is already inside an event loop.

**Tolerated uses of `threading` / sync primitives** (the reviewer should still verify the constraint actually holds — and lean toward "rewrite it" when in doubt):

- `threading.local()` — thread-local storage, no sharing at all.
- `queue.Queue` / `multiprocessing.Queue` — the recommended communication primitives, not flagged.
- `threading.Event` for one-shot startup/shutdown signaling (single transition, set-once semantics). Subject to flag #7 (daemon-thread cleanup).
- `concurrent.futures.ThreadPoolExecutor` for I/O-bound work where async isn't feasible (e.g., a sync library with no async equivalent that's deep in the call tree).
- `asyncio.Lock` / `asyncio.Semaphore` inside a single event loop. Safer than `threading.Lock` (no preemption between awaits) but still flagged when a queue or task-owned state would do.
- `asyncio.Semaphore` for rate-limiting external API calls — this is the right tool.
- `multiprocessing.Manager` for genuinely shared state across processes — it's the supported API and Manager mediates serialization correctly.

Each tolerated use must have a comment explaining *why* it's not a queue or process boundary. "Was simpler" is not an acceptable answer.

**Do NOT flag:**

- `asyncio.Lock` / `asyncio.Semaphore` for rate-limiting external API calls (semaphore is the right tool there).
- `multiprocessing.Manager` for genuinely shared state across processes (it's the supported API).
- `concurrent.futures` for I/O-bound parallelism in code that can't be ported to async.

**Required tooling:**

- Tests that exercise concurrent code should use `pytest-asyncio` (or equivalent) and run under `pytest --full-trace` to surface async stack frames.
- For production async code, enable `PYTHONASYNCIODEBUG=1` in development to catch unawaited coroutines and slow callbacks.
- For production threading code, run a stress test with `--count=100` or `pytest-repeat` — single-shot tests miss most races.

**Review approach:**

1. Search the diff for `import threading`, `import multiprocessing`, `import asyncio`, `import concurrent.futures`, `import queue`.
2. For each `threading.Lock` / `RLock`: flag P1.
3. For each `asyncio.create_task`: confirm a reference is kept (or `TaskGroup` is used).
4. For each `asyncio.gather`: confirm the exception-handling mode is deliberate.
5. For each `subprocess.*` inside an `async def`: flag.
6. For each external I/O in async: confirm there's a timeout.
7. For each mutable module-level dict/set/list written from multiple threads: flag.

---

## resource-leak-reviewer

Review Python code for **resource leaks** in long-running processes (daemons, servers, workers, CLI tools that process large batches). Python's GC is forgiving but not magic — file handles, sockets, database connections, and processes can pile up faster than GC frees them, especially in async code where the same coroutine reopens resources in a loop.

**Patterns to FLAG:**

1. **`open()` without a context manager:**

   ```python
   # BAD — file handle relies on GC; on CPython may stay open indefinitely if a reference survives
   f = open(path)
   data = f.read()

   # GOOD
   with open(path) as f:
       data = f.read()
   ```

2. **`subprocess.Popen` without `.wait()` or context manager:**

   ```python
   # BAD — zombie process, fd leak
   p = subprocess.Popen(["git", "fetch"])
   # ... no .wait()
   ```

   Use `subprocess.run(...)` for one-shot commands, or `with subprocess.Popen(...) as p:` for streaming I/O.

3. **Database connections not closed:**

   ```python
   # BAD — connection leaked
   conn = sqlite3.connect(path)
   cursor = conn.execute(query)

   # GOOD
   with sqlite3.connect(path) as conn:
       cursor = conn.execute(query)
   ```

   Note: the `sqlite3` connection context manager commits/rolls back but does NOT close — close explicitly if the connection isn't intended to outlive the block.

4. **HTTP responses not closed:**

   ```python
   # BAD — connection held by the response object
   resp = requests.get(url)
   data = resp.json()
   ```

   Use `with requests.get(url, stream=True) as resp:` for streamed responses, or a session context manager. For `httpx`, always use `with httpx.Client() as client:`.

5. **Unbounded `io.read()` / `response.content` from untrusted sources:**

   ```python
   # BAD — caller controls how much memory you allocate
   data = resp.content  # may be gigabytes
   ```

   For untrusted sources, check `Content-Length` first, set a max size on the HTTP client, or stream with bounded chunks.

6. **`functools.lru_cache` without `maxsize=` on unbounded key spaces:**

   ```python
   # BAD — cache grows without bound when keys come from external input
   @functools.lru_cache(maxsize=None)
   def fetch(user_id):
       ...
   ```

   Set `maxsize=` to a sensible cap when keys derive from external input. Or use `cachetools.TTLCache`.

7. **Long-lived module-level dicts/sets as caches without eviction:**

   ```python
   # BAD — grows forever as long as the process is up
   _CACHE: dict[str, Any] = {}

   def serve(req):
       _CACHE[req.key] = compute(req)
   ```

   Use `functools.lru_cache`, `cachetools`, or an explicit eviction policy.

8. **`threading.Thread` / `multiprocessing.Process` not joined or daemonized correctly:**

   A non-daemon thread blocks process exit forever. A daemon thread leaves resources mid-cleanup. See `concurrency-reviewer` for the deeper rules; this reviewer flags the leak shape (process can't exit, or worker dies with files half-written).

9. **`aiohttp.ClientSession` constructed per request:**

   ```python
   # BAD — defeats connection pooling and leaks sockets
   async def fetch(url):
       async with aiohttp.ClientSession() as session:
           return await session.get(url)
   ```

   Construct a session once at module/app level and reuse it.

10. **`tempfile.NamedTemporaryFile(delete=False)` without explicit cleanup:**

    `delete=False` is sometimes necessary (the file is consumed by a subprocess after the Python writer closes it), but always pair it with `finally: os.unlink(name)` or use `tempfile.TemporaryDirectory()` for batches.

11. **Manual `try/finally` chains where `contextlib.ExitStack` would do:**

    ```python
    # BAD — fragile when N varies, breaks on intermediate failure
    f1 = open(a)
    try:
        f2 = open(b)
        try:
            f3 = open(c)
            try:
                ...
            finally:
                f3.close()
        finally:
            f2.close()
    finally:
        f1.close()

    # GOOD — ExitStack handles unwinding even when count varies at runtime
    from contextlib import ExitStack
    with ExitStack() as stack:
        files = [stack.enter_context(open(p)) for p in paths]
        ...
    ```

    Flag manual try/finally chains protecting two or more resources, especially when the resource count is dynamic. `ExitStack` is the right pattern.

12. **Long-lived caches keyed by object identity without `weakref`:**

    ```python
    # BAD — cache pins the keys in memory forever, prevents GC
    _cache: dict[SomeObject, Result] = {}

    # GOOD — entries vanish when the key is GC'd elsewhere
    import weakref
    _cache: weakref.WeakKeyDictionary[SomeObject, Result] = weakref.WeakKeyDictionary()
    ```

    When a cache is keyed by an object (rather than a primitive ID or string), plain `dict` holds a strong reference to the key, preventing the key from being garbage collected even after the rest of the program has dropped its references. `weakref.WeakKeyDictionary` (for object keys) and `weakref.WeakValueDictionary` (for object values) let cache entries evict naturally when the underlying objects are no longer reachable.

**Cross-reference:** Task leaks in async code (`asyncio.create_task` without keeping a reference) are covered in `concurrency-reviewer`. This reviewer focuses on non-task resources (fds, sockets, processes, memory).

**Do NOT flag:**

- `pathlib.Path.read_text()` / `read_bytes()` — closes the file internally.
- `with` blocks that close resources in normal flow.
- Bounded `functools.lru_cache` with an explicit `maxsize`.
- One-shot scripts that exit shortly after the work (CLIs, build scripts) — the leak shape doesn't apply when the process exits in seconds.

**Review approach:**

1. For each `open()`, `subprocess.Popen`, `sqlite3.connect`, `psycopg2.connect`, `requests.get`, `httpx.get`: verify a `with` block or explicit close on every return path.
2. For each `.read()` / `.content` / `.text` from external input: verify a size bound upstream.
3. For each module-level `dict` / `set` written from multiple call sites: ask about eviction.
4. For each `lru_cache`: verify `maxsize` is set when the key space is external.

---

## external-process-reviewer

Review Python code that shells out via `subprocess`. CLI failures (timeout, malformed output, missing binary) are routine, and Python's defaults are dangerous.

**Patterns to FLAG:**

1. **Missing `timeout=`:**

   ```python
   # BAD — hangs forever if the remote is down
   subprocess.run(["git", "fetch", "origin"])

   # GOOD
   subprocess.run(["git", "fetch", "origin"], timeout=30)
   ```

   Every `subprocess.run` / `subprocess.check_call` / `subprocess.check_output` in a long-running process must have a `timeout=`.

2. **`shell=True` with any non-literal input (P1 — command injection):**

   ```python
   # BAD — command injection
   subprocess.run(f"git log {user_input}", shell=True)

   # GOOD — args as a list, no shell
   subprocess.run(["git", "log", user_input])
   ```

   `shell=True` should almost never be used. Even with `shlex.quote`, list-form args are safer.

3. **`subprocess.Popen` without context manager:**

   ```python
   # BAD
   p = subprocess.Popen(["cmd"], stdout=subprocess.PIPE)
   out, _ = p.communicate()

   # GOOD
   with subprocess.Popen(["cmd"], stdout=subprocess.PIPE) as p:
       out, _ = p.communicate()
   ```

4. **No stderr capture on failure:**

   ```python
   # BAD — when the command fails, the error message is just "Command 'X' returned non-zero exit status 1"
   result = subprocess.run(["cmd"], check=True)

   # GOOD — capture stderr so the error is useful
   result = subprocess.run(
       ["cmd"], check=True, capture_output=True, text=True
   )
   # CalledProcessError now carries .stderr
   ```

5. **Missing `cwd=`:**

   ```python
   # BAD — runs in whatever cwd the process happens to have
   subprocess.run(["git", "status"])

   # GOOD
   subprocess.run(["git", "status"], cwd=repo_path)
   ```

6. **No differentiation between failure modes:**

   ```python
   # BAD — timeout, exit code, missing binary all look the same
   try:
       result = subprocess.run([...], check=True)
   except subprocess.SubprocessError:
       return None

   # GOOD
   try:
       result = subprocess.run([...], check=True, timeout=30)
   except subprocess.TimeoutExpired:
       ...
   except subprocess.CalledProcessError as e:
       ...
   except FileNotFoundError:
       # binary missing
       ...
   ```

7. **Unbounded output capture:**

   `subprocess.run(..., capture_output=True)` reads all of stdout/stderr into memory. For untrusted commands, use a streamed `Popen` with a bounded buffer or write output to a temp file.

8. **Binary presence assumed, never checked:**

   ```python
   # BAD — assumes "myhelper" is on PATH
   subprocess.run(["myhelper", "--flag"])

   # GOOD — verify at startup, fail fast
   if shutil.which("myhelper") is None:
       raise SystemExit("myhelper not installed")
   ```

9. **Output parsing without validation:**

   - Assuming JSON output has specific fields without checking.
   - `str.split()` on output without handling empty result.
   - Parsing version strings without handling pre-release suffixes or unexpected formats.

**Do NOT flag:**

- `subprocess.run` with hardcoded args in init/setup paths designed to fail fast.
- Tests that intentionally invoke commands without timeout (test runner enforces overall timeout).

**Review approach:**

1. For each `subprocess.*`: check timeout, `shell=False`, `cwd`, error-category handling, stderr capture.
2. For each binary invoked: verify presence checked at startup (or first-use, with a clear error message).
3. Flag any `shell=True` with non-literal arguments as P1 (command injection).

---

## dead-code-reviewer

Review PRs for **dead code introduction**. Python's runtime has no compile-time check for unused symbols. `vulture` and `ruff` (`F401` unused imports, `F841` unused variables) catch most cases; this reviewer fills the gaps — especially around public APIs, reflection-driven code, and partial refactors.

**What to flag:**

1. **Unused module-level functions, classes, constants** that nothing in the package references.
2. **Unused exported names** in a package's `__init__.py` that nothing imports (verify by grepping the whole workspace).
3. **Unused fixtures in `conftest.py`** that no test references.
4. **Commented-out code** — delete; git history preserves it.
5. **Partial refactors** — old function name still defined after every call site moved to a new name.
6. **Stale `__all__` entries** referring to names that no longer exist (these `AttributeError` at import time when consumed).
7. **Unused parameters with default values** (especially after a refactor that stopped passing them).
8. **`if False:` / `if True:` / `if 0:` dead branches** — these typically appear in code that's been partially commented out as a refactor scaffold. Delete the branch; git history preserves it.
9. **`def f(): pass` stubs with no implementation and no callers.** Either should be `@abstractmethod` on an ABC (and implemented in subclasses) or removed. A `pass`-only function that's never called is dead weight; a `pass`-only function that's called silently no-ops without telling anyone.

**Review approach:**

1. For each new/modified file, check: did the PR remove call sites without removing the called function?
2. For renamed/moved functions, check: is the old name still defined somewhere?
3. For removed features, check: are all supporting helpers and constants also removed?
4. Grep the workspace for each flagged symbol to confirm it's truly unreferenced. Include the grep result in the comment so the author can verify.

**Do NOT flag:**

- **Reflection-driven code**: SQLAlchemy mapped columns, Pydantic model fields, Click command callbacks (decorator side-effects), pytest fixtures (discovered by name), marshmallow schemas. These look unused but aren't.
- Code referenced only via `getattr` / `hasattr` / `importlib.import_module` (search for the bare string, not just the symbol).
- Public API functions/classes with no internal callers (they may be called by external packages — verify against the package's documented surface or `__all__`).
- `__init__.py` re-exports that look unused but are part of the package's documented public surface.
- Test helpers discovered automatically (`conftest.py` fixtures used implicitly via name).
- Build-tag-gated or platform-specific code (e.g., `if sys.platform == "win32":` branches).

**Tooling cross-reference:** `vulture` catches most of this; assume CI runs it. This reviewer focuses on what vulture misses — public APIs, reflection-driven code (vulture's whitelist mechanism handles some of this if configured), and partial refactors that vulture sees as "still has callers" because the call site is in the same PR.

---

## import-reviewer

Review Python imports for **PEP 8 compliance and placement discipline**. Most import-ordering issues are caught by `ruff check --select I`; this reviewer focuses on the structural rules ruff doesn't enforce.

**Patterns to FLAG:**

1. **Function-level imports:**

   ```python
   # BAD — import inside a function
   def parse(path):
       import json
       return json.loads(open(path).read())

   # GOOD — top of file
   import json

   def parse(path):
       with open(path) as f:
           return json.load(f)
   ```

2. **Import order violations** (caught by `ruff I001` — flag only if CI doesn't run ruff):

   PEP 8 ordering: stdlib → third-party → local, blank line between groups.

3. **`from X import *`** (wildcard imports):

   ```python
   # BAD — pollutes namespace, defeats static analysis
   from module import *
   ```

   Exceptions: `__init__.py` re-exports where `__all__` is also defined.

4. **Relative imports past package boundaries:**

   ```python
   # BAD — fragile, breaks under restructuring
   from ...other_package import thing
   ```

   Use absolute imports for cross-package references.

**Acceptable function-level imports** (each requires a comment explaining why):

- Optional dependencies wrapped in `try`/`except ImportError` — the dep may not be installed.
- Imports that genuinely break a circular dependency. The comment should name the cycle.
- Imports deferred to minimize CLI startup latency (heavy modules like `numpy`, `pandas`, `torch`). The comment should justify the latency budget.

**Review approach:**

1. Grep for `^\s+import` and `^\s+from` (indented import statements) — these are function-level.
2. For each, check if it falls into an acceptable exception. If yes, verify the documenting comment is present.
3. Cross-check that `ruff check --select I` runs in CI; if not, also flag import-order violations.

---

## importlib-resources-reviewer

Review Python code for **correct package data access**. Package data files (JSON sidecars, SQL schema, text fixtures, templates) must be loaded via `importlib.resources`, not `Path(__file__).parent`. The `__file__.parent` pattern silently breaks when a module is renamed or moved to a subpackage — the parent directory shifts and the relative path no longer resolves. The `importlib.resources` pattern is robust to package moves and works identically in dev (editable install), `pip install`, frozen builds, and Lambda zips.

**FLAG when a file in a packaged module contains:**

- `Path(__file__).parent / "data"` (or any path navigation from `__file__`)
- `os.path.dirname(__file__)` used to locate bundled data
- `__file__.parents[N]` for sibling resources

**Acceptable pattern:**

```python
from importlib import resources

data_file = resources.files("mypackage.data").joinpath("config.json")
config = json.loads(data_file.read_text())
```

For Python ≥3.9. On older versions, `importlib.resources.read_text("mypackage.data", "config.json")` is the equivalent.

**Do NOT flag:**

- `Path(__file__).parent` used for **write paths** — tests writing to a tmp dir relative to the test file, scripts emitting output next to themselves. Resources are read-only by definition.
- `__file__` references in `tests/` — tests have a stable layout and aren't packaged.
- `__file__` references in entry-point scripts (`bin/`, top-level CLI shims) that are not part of the package surface.

**Review approach:**

1. Grep PR diff for `Path(__file__).parent` and `__file__.parents`.
2. For each hit in package code, check whether the path resolves to a bundled data file. If so, recommend the `importlib.resources` form.
3. For each new package data file added under a package directory, verify it's also declared in `pyproject.toml` / `setup.cfg` `package_data` or `MANIFEST.in` (depending on the build backend) — otherwise it won't be shipped.

---

## dataclass-decorator-reviewer

Review Python code for **missing `@dataclass` decorators on classes that use `dataclasses.field()`**. The decorator is what makes `field()` resolve to a `Field` descriptor at class-construction time. Without it, `field(default_factory=list)` becomes a literal `Field` object as the attribute value — passing tests at import time but failing at first instance use with errors like `argument of type 'Field' is not iterable` or `'Field' object has no attribute 'append'`.

**Default severity: P1.** The bug surfaces at instance-construction or first-attribute-access time, not at import time, so type checkers and quick smoke imports won't catch it. The class is broken in production-shaped ways.

**FLAG when a PR adds or modifies a class that uses `field(default_factory=...)` or `field(default=...)` without a `@dataclass` (or `@dataclass(...)`) decorator on the line above:**

```python
from dataclasses import dataclass, field

class X:                                       # ← needs @dataclass!
    forms: list[str] = field(default_factory=list)
    citations: set[str] = field(default_factory=set)
```

```python
from dataclasses import dataclass, field

class Y:                                       # ← needs @dataclass!
    name: str = field(default="")
```

**Acceptable patterns (do NOT flag):**

- `@dataclass`-decorated classes with `field(...)` defaults — the decorator is what makes `field()` resolve correctly.
- Classes that reference `field` only as a typing annotation (`x: dataclasses.Field[int]`) without calling it.
- `attrs.field(...)` or `pydantic.Field(...)` — those have their own decorator chains (`@attrs.define`, model inheritance) and don't fit this rule's signature.
- Standalone `field()` calls used to build `dataclasses.make_dataclass()` arguments dynamically.

**Review approach:**

1. Grep new/modified files for `field(default` — every hit must be inside a `@dataclass`-decorated class (check the line above the enclosing `class` statement).
2. If a `field(...)` call is in a class without the decorator: flag it.

This rule fires almost exclusively on **extraction PRs** — when a `@dataclass` class is moved from one file into another via line-range copy, the `@dataclass` decorator line above `class X:` is often missed if the extraction range starts at `class`. Pay extra attention to refactors that copy classes between files.

---

## typing-consistency-reviewer

Review Python type annotations for **consistency within files that have opted in to typing**. This reviewer does NOT push untyped codebases toward typing — Python's flexibility is a feature, and gratuitous type ceremony adds complexity for little gain. But once a file opts in, the standards apply: half-typed code is worse than no types, because it gives a false signal to type checkers and human readers alike.

**Scope rule:** A file is in scope when ANY of the following is true:

- At least one function parameter, return value, variable, or class attribute is annotated.
- The file has `from __future__ import annotations`.
- The file imports from `typing` and the import is used (not dead).

Files with zero annotations are out of scope. Leave them alone — the choice not to type is a legitimate design decision.

**Patterns to FLAG (only when the file is in scope):**

1. **`# type: ignore` without an error code and reason comment:**

   ```python
   # BAD — silent escape hatch, no paper trail
   result = some_func(x)  # type: ignore

   # GOOD — narrow ignore with documented reason
   result = some_func(x)  # type: ignore[arg-type]  # x is dynamically validated upstream
   ```

   The `[error-code]` form makes the ignore narrow (it only suppresses that specific error, so unrelated type errors still surface). The reason comment makes the choice reviewable later.

2. **Half-typed function signatures:**

   ```python
   # BAD — some params annotated, some not
   def foo(x: int, y, z: str) -> bool:
       ...

   # BAD — params annotated, return type missing
   def foo(x: int, y: str):
       return x > 0

   # GOOD — fully annotated
   def foo(x: int, y: str) -> bool:
       return x > 0

   # GOOD — explicit None return
   def emit(event: Event) -> None:
       ...
   ```

   Either all parameters are annotated and the return type is present, or the function is unannotated. Mixed signatures are the worst of both worlds: type checkers complain unhelpfully, and human readers can't tell whether the missing types are deliberate or forgotten.

3. **`Any` as an escape hatch when the type is inferable:**

   ```python
   # BAD — function body clearly expects str
   def upper(x: Any) -> Any:
       return x.upper()

   # GOOD
   def upper(x: str) -> str:
       return x.upper()
   ```

   `Any` is acceptable for genuinely generic code (a logger formatter that accepts any object, a serializer's input, a `**kwargs` passthrough). It is **not** acceptable as a way to dodge type-checker complaints when the actual type is obvious from the function body or its callers.

4. **`typing.cast` used to silence an error rather than narrow a type:**

   ```python
   # BAD — cast lies about the type to silence mypy
   value = cast(int, request.json["count"])  # could be anything from JSON

   # GOOD — narrow a known-broader type, with validation upstream
   raw = request.json["count"]
   if not isinstance(raw, int):
       raise ValidationError("count must be int")
   value = raw  # mypy now knows raw is int, no cast needed

   # GOOD — cast when the runtime guarantee is real but the type checker can't see it
   value = cast(int, validated_int_from_jsonschema)
   ```

   `cast` is for telling the type checker something it can't infer but you can prove. Using it to bypass a real type error is a future debugging problem.

5. **Inconsistent `Optional[X]` vs `X | None` within the same file:**

   ```python
   # BAD — same file
   def find(name: str) -> Optional[Entry]: ...
   def find_all(name: str) -> list[Entry] | None: ...
   ```

   Pick one form per file. This reviewer does NOT enforce a project-wide choice — `Optional` works everywhere `typing` does; `X | None` requires Python ≥3.10 (PEP 604) or `from __future__ import annotations`. Local consistency is what matters; suggest the form already dominant in the file.

6. **Completely-unannotated functions in an otherwise-typed file:**

   ```python
   # File has typed functions throughout, then:
   def helper(x, y):  # BAD — drops the type discipline mid-file
       return x + y
   ```

   In a file that opts in to types, every function should be annotated. Either annotate this one, or move it to an untyped module if you genuinely don't want to type it.

**Do NOT flag:**

- Files with zero annotations — out of scope entirely. Untyped Python is a legitimate choice.
- `Any` when the function is genuinely generic over arbitrary types (loggers, serializers, deepcopy-style helpers, `**kwargs` passthroughs).
- Missing type annotations on public APIs as a universal rule.
- Cross-version concerns (`from __future__ import annotations` discipline, `list` vs `List`) — that's a project-level architectural choice and belongs in the project's own AGENT-REVIEWERS.md.
- Type-checker errors themselves — those should already be P1 from CI's mypy/pyright run, not from this reviewer.
- Project-wide enforcement of `Optional` vs `X | None` — only local-to-file consistency matters here.
- `# type: ignore` with the `[error-code]` form and a brief reason comment.

**Review approach:**

1. For each `*.py` file in the diff, determine whether it's in scope (any annotation present anywhere in the file).
2. If in scope, scan for the six patterns above.
3. For `# type: ignore` hits, verify both an error code in brackets AND a reason comment are present.
4. For half-typed signatures, propose the full annotation in the suggested fix.
5. For `Any` hits, look at the function body — if the actual type is obvious from how the parameter is used, suggest the concrete type.
6. For `Optional` vs `X | None` drift within a file, suggest the form already dominant.

This reviewer enforces "if you opt in, do it well" rather than "everyone must opt in." The line between typed and untyped is a project-level decision; the consistency of typing *within* a file is universal hygiene.

---

## clarity-reviewer

Review markdown documentation AND Python docstrings/comments for terseness, structure, and factual accuracy. Every token costs money and attention — cut the fat, fix the layout, and don't let docstrings lie about what the code does.

This reviewer applies to:
- Any `.md` file in the PR — design docs, READMEs, runbooks, ADRs.
- Docstrings and inline comments in `.py` files.

**Markdown — what to check:**

1. Look at the PR diff for changes to `.md` files.
2. **Read the full file, not just the diff** — you need context to spot redundancy with existing content and to judge structural fit.
3. Examine new or modified text for word-level fat (table below) AND structural problems (next section).

**Word-level patterns to flag:**

| Verbose | Terse |
|---------|-------|
| "in order to" | "to" |
| "for the purpose of" | "to" / "for" |
| "in the event that" | "if" |
| "at this point in time" | "now" |
| "due to the fact that" | "because" |
| "it is important to note that" | (delete, just state the thing) |
| "as mentioned above/previously" | (replace with a link to the actual section; rarely just delete) |
| "This section describes how to..." | (delete, describe it directly) |

**Filler words** (`actually`, `basically`, `simply`, `really`, `just`): question, then suggest removal. These are sometimes load-bearing for tone in user-facing docs ("simply" can soften a step that sounds intimidating). Default to flagging only when the word adds no information AND the surrounding tone doesn't need softening.

**Structural patterns to flag:**

- **Wall of text**: a section longer than ~300 words without a sub-heading, list, table, or code block. Recommend breaking it up.
- **Missing TL;DR / lede**: a document longer than ~500 words that doesn't open with a 1-3 sentence summary. Recommend adding one.
- **Missing examples**: a how-to or reference section that describes a command, API, or pattern without showing it. Recommend a code block.
- **Heading inflation**: a section with one sub-heading under it, or sub-headings that introduce a single paragraph each. Either inline or add siblings.
- **Voice/tense inconsistency**: second-person ("you should run X") mixed with imperative ("run X") inside the same section. Pick one. Imperative is usually shorter; second-person is friendlier for onboarding.
- **Link rot phrasing**: "as mentioned above," "see the section below" — these break when the doc is restructured. Replace with `[explicit link]` to the heading anchor.

**Python docstrings — what to flag:**

1. **Restate-the-signature docstrings:**

   ```python
   # BAD — docstring repeats what the signature already says
   def add(x: int, y: int) -> int:
       """Adds x and y."""
       return x + y
   ```

   Either delete (signature is self-explanatory) or replace with the *why* / contract / non-obvious constraints.

2. **Doc style for public APIs (PEP 257):**

   - Public functions, classes, and modules should have a docstring.
   - First line: imperative mood ("Return", "Compute", "Parse"), one sentence, ends with a period.
   - Multi-line docstrings: summary line, blank line, then details.

3. **No docstrings on trivial private functions** unless the logic is non-obvious. `def _foo(x): """Does foo to x."""` is noise.

4. **WHY vs WHAT in inline comments:**

   ```python
   # BAD — explains what the code does
   # Loop through items and add them to results
   for item in items:
       results.append(item)
   ```

   ```python
   # GOOD — explains why this is non-obvious
   # Items are sorted descending so the first match wins; changing this
   # order breaks the disambiguator's tie-break rule.
   items = sorted(items, key=lambda i: -i.score)
   ```

5. **Stale comments**: comments referencing renamed identifiers, removed code paths, or completed TODOs.

6. **Docstring grep-verify — claims must match the code:**

   When a docstring names specific tables, columns, function names, regex counts, output keys, or field counts, every named symbol must actually appear in the function body or its dependencies. Without this check, docstrings drift over multiple unrelated PRs without anyone re-reading them and end up lying about what the code does.

   **FLAG when a NEW or MODIFIED docstring contains:**

   - A SQL table or column name that doesn't appear in the function's SQL string literal.
   - A function name referenced as called/used that doesn't appear in the function body (grep for the bare name).
   - A regex count ("Three regex patterns") that doesn't match the actual `re.findall(r'^_[A-Z_]+_RE\b', ...)` or equivalent obvious count.
   - An output-shape claim (`{key1, key2, ...}` or `dict[K, V]`) where the keys don't match the actual `SELECT` columns, `return` dict, or class fields.
   - A field count or list ("5 dicts", "the four renderings", "the nine sibling families") that doesn't match the actual `dataclass` field count or list construction.

   **Acceptable (do NOT flag):**

   - Stable conceptual descriptions that don't name specific symbols ("this module owns the per-language enrichment pass").
   - Placeholder names (`<name>_variants`).
   - Forward-looking docs that explicitly say "Phase 2 will add ...".

**Flag content issues if:**

- A sentence can be cut in half without losing meaning.
- The same information is stated twice in different words.
- A code comment restates what the next line of code does.
- Explanatory text explains something already obvious from context.
- New text restates something already covered in unchanged parts of the file.

**Do NOT flag:**

- Necessary detail that aids understanding.
- Examples and code blocks (these should be complete, not abbreviated).
- Repetition that serves as a deliberate reminder (e.g., "NEVER push directly to main" repeated for emphasis).
- Technical precision that requires specific wording.
- Tone-softening filler in user-facing docs where the alternative reads as terse-to-the-point-of-cold.
- Required docstrings on public identifiers even if the function is simple — PEP 257 convention requires them.

**Review approach:**

1. For NEW module docstrings, open the first function below and grep-verify every named symbol / table / regex count / output key.
2. For MODIFIED docstrings, compare diff lines against the function body line-by-line.
3. For each markdown `.md` file: apply word-level + structural checks from the markdown section.
4. For inline comments: flag restate-the-code, encourage WHY over WHAT.

**When flagging, provide:**

- The verbose / inaccurate text.
- A terse / accurate replacement.
- Brief reason (only if not obvious).

## test-isolation-reviewer

Review test changes for **isolation from live infrastructure**. Cortex tests must run against mocks/fixtures, not the live homelab services, for two reasons:

1. **CI doesn't have the infra.** There's no Postgres, DuckDB API, Gmail API, S3/Minio, Ollama, or LiteLLM proxy on a CI runner. A test that reaches one works on a dev box (where the infra exists) and fails or hangs on CI — a false-green that only surfaces at merge time.
2. **Live calls are slow and non-deterministic.** A test that hits Ollama/LLM or walks a real Postgres is seconds-to-minutes and flaky, which discourages running `pytest` before commit.

**Default severity: P2** — usually surfaces as a CI failure rather than a correctness bug, but the fix is same-cycle.

**What to flag:**

1. **Real Postgres.** `psycopg2.connect(...)` / `asyncpg` against a host in a test, instead of the project's mock-connection pattern (a `MagicMock` whose `cursor().__enter__()` returns a mock cursor) or monkeypatching `psycopg2.connect`. Cortex tests use exactly this idiom — a new test that opens a real connection is the flag.
2. **Real HTTP to a service URL.** `httpx`/`requests` calls that resolve `DUCKDB_API_URL`, `OLLAMA_URL`, the LiteLLM proxy, or the Gateway, without a mock/`respx`/monkeypatched client.
3. **Real S3/Minio.** `boto3` clients/resources in a test without `moto` (or an injected fake). A bare `boto3.client("s3")` that talks to a live endpoint.
4. **Real Gmail.** Building a Gmail `service` / calling `users().messages()` without a mock.
5. **Reliance on env-configured hosts.** A test that only works because `POSTGRES_HOST` / `DUCKDB_API_URL` / `OLLAMA_URL` happen to be set in the dev shell. CI won't have them; the test should inject its dependency, not read the ambient env.

**Review approach:**

1. Walk the diff for test files (`tests/test_*.py`, `conftest.py`).
2. Grep new tests for `psycopg2.connect`, `asyncpg`, `boto3`, `httpx.`, `requests.`, `OLLAMA_URL`, `DUCKDB_API_URL`, `build(` (Gmail), and any `10.5.2.` / homelab host. Each is a review point — confirm the call is mocked, monkeypatched, `moto`-wrapped, or pointed at an in-memory/`tmp_path` resource.
3. Note any test in the `--durations` output over ~1s as a possible live-infra leak.

**Do NOT flag:**

- Tests using `MagicMock`/`monkeypatch` connections, `moto`, `respx`, or an in-memory/`tmp_path` substitute — that's the correct pattern.
- Pure-logic tests that never touch I/O.
- A test explicitly marked as an integration test that is gated out of the default CI run (verify the marker/gating actually exists; if it doesn't, flag it).

---

## migration-idempotency-reviewer

Review schema changes for **idempotency**. Cortex applies its schema by running `ensure_schema()` / `ensure_*_schema()` functions on **every service and worker startup** — there is no one-shot migration runner. The same DDL re-executes on every boot and against databases at different starting states, so each statement must be safe to run repeatedly and safe against tables that already hold rows.

**Default severity: P1** — a non-idempotent statement crashes the service on its *second* startup (or on a redeploy), which is a production outage, not a test failure.

**What to flag:**

1. **`CREATE TABLE` without `IF NOT EXISTS`** in a startup/ensure-schema path.
2. **`ADD COLUMN` without `IF NOT EXISTS`** — errors on the second run.
3. **`CREATE INDEX` without `IF NOT EXISTS`** (or `CREATE UNIQUE INDEX` without it).
4. **`ADD CONSTRAINT` not preceded by `DROP CONSTRAINT IF EXISTS`** — re-adding a named constraint fails on the second run. The correct cortex idiom is `DROP CONSTRAINT IF EXISTS <name>` then `ADD CONSTRAINT <name> ...` (see the `rule_proposals_status_check` migration).
5. **`ADD COLUMN ... NOT NULL` without a `DEFAULT`** — fails on any table that already has rows. Add a default (or backfill then set NOT NULL in a guarded step).
6. **Destructive DDL in a startup path** — `DROP TABLE`, `DROP COLUMN`, `TRUNCATE`, or a type change that can lose data. These don't belong in an idempotent ensure-schema function; they need a deliberate, one-off, reviewed migration.
7. **Unguarded data backfills** — an `UPDATE`/`INSERT ... SELECT` in ensure-schema that isn't a no-op on re-run (e.g. re-stamping rows every boot, or inserting duplicates without `ON CONFLICT DO NOTHING`).

**Review approach:**

1. For each SQL DDL statement in the diff, determine whether it runs in a startup/ensure-schema path (grep for the enclosing function name and its call sites) or a one-off script.
2. If startup: verify every statement is individually idempotent per the list above.
3. For a new `NOT NULL` column, confirm there's a `DEFAULT` or a guarded backfill-then-constrain sequence.
4. For a renamed/re-added constraint or index, confirm the drop-then-add guard.

**Do NOT flag:**

- DDL in a clearly one-shot migration script that is explicitly not part of the startup path (but do confirm it's actually run out-of-band, not imported into an ensure-schema function).
- Idempotent statements already using `IF NOT EXISTS` / `IF EXISTS` / `ON CONFLICT` guards.
- `CREATE OR REPLACE FUNCTION` / `CREATE OR REPLACE VIEW` (idempotent by construction).

---

## homelab-values-reviewer

Enforce CLAUDE.md's **no hardcoded homelab values** and **no hardcoded LLM models/prompts** rules. These repos are public; personal infrastructure details and un-configurable model choices must not leak into code.

**Default severity: P2** (P1 if it's a credential or a `deny`-listed secret).

**What to flag:**

1. **Homelab IPs** — string literals matching `10.5.2.\d+` (or other private-range homelab addresses) anywhere in `src/`.
2. **Homelab hostnames** — `ares`, `hades`, `gaia`, `macbook`, the `.evilsoft` domain, `ares.evilsoft`, etc., as literals in code.
3. **Env defaults that embed homelab values** — e.g. `os.getenv("OLLAMA_URL", "http://10.5.2.31:11434")`. The CLAUDE.md rule is explicit: if a service URL is required, make it *required* (raise if unset) — don't fall back to a value that exposes the internal network.
4. **Hardcoded LLM model names** — literals like `"gemma3n:e4b"`, `"phi3:mini"`, `"gpt-4..."`, `"claude-..."`, `"llama..."` passed to an LLM call. Model choice must come from YAML rules config (per-intent `model`) or an env var (`LLM_MODEL`, `CLASSIFICATION_MODEL`, ...).
5. **Hardcoded prompts** — a multi-line/templated prompt string literal passed into an LLM call, rather than sourced from config (`LLM_PROMPT`, YAML `prompt`, etc.).

**Review approach:**

1. Grep the non-test `src/` diff for `10\.5\.2\.`, `evilsoft`, `\b(ares|hades|gaia)\b`, and `os.getenv(...,` defaults containing a URL/IP.
2. Grep for likely model tokens (`:mini`, `:e4b`, `gpt-`, `claude-`, `gemma`, `llama`, `phi3`) in code that reaches an LLM client.
3. For any LLM call, confirm `model=` and `prompt=` are parameters/config lookups, not literals.

**Do NOT flag:**

- **Deployment configs** (`~/HomeLab/`, compose files, Portainer stack env) — homelab values *belong* there, not in code.
- **Tests and fixtures** using example/RFC-5737 IPs or obviously-fake hosts (`example.com`, `db`, `localhost`).
- **Docs / CLAUDE.md** that reference the real infra intentionally.
- **YAML rules config** (`rules.yaml`, `rules.example.yaml`) carrying `model`/`prompt` — that's the *correct* place for them.
- Env reads with **no** homelab default (`os.environ["OLLAMA_URL"]` or `os.getenv("OLLAMA_URL")` returning None) — those are compliant.

---


## clock-discipline-reviewer

Review any date or time that reaches the database for **which clock produced
it**. Postgres rows are stamped by the *server*; a value computed in the Python
process comes from a *different* clock, in a possibly different timezone, and
the two agree only by coincidence.

That coincidence is what makes this class dangerous. It tests green on every box
where client and server are both UTC, passes review because the code reads
naturally, and fails on the first deployment where they are not. It is not a
race that shows up under load — it is a correctness bug that shows up under
*deployment*.

**Default severity: P1** when the value routes or creates a partition, because
the failure is silent data loss: rows route to a partition that does not exist,
or a self-heal creates the wrong day and the insert fails for a partition that
was never missing. **P2** for filters and reported timestamps, where the result
is wrong answers rather than lost writes.

**What to flag:**

1. **`date.today()`, `datetime.now()`, `datetime.utcnow()`, `time.time()` whose
   value reaches SQL** — as a bound parameter, an interpolated partition name, a
   date range, or a cutoff. The partition case is the worst: the partition key is
   `created_at TIMESTAMPTZ DEFAULT NOW()`, a value the server produces, so a
   client-derived partition date is a second clock deciding where a server-dated
   row lives.
2. **A single statement mixing both clocks** — e.g. `WHERE claimed_at < %s` with
   a Python cutoff, while the same query computes `NOW() - claimed_at`. The
   filter and the arithmetic then disagree.
3. **Naive datetimes compared against `TIMESTAMPTZ`.** `datetime.now()` is naive
   local time; the column is timezone-aware. The comparison is doubly wrong and
   silently so.
4. **A tolerance or window that narrows a race instead of removing it.** Creating
   only "today" leaves a retry that crosses midnight still broken; creating today
   *and* tomorrow in one pass removes it.
5. **Tests that assert the client clock.** A test written against `date.today()`
   cannot distinguish correct behaviour from the bug, and will keep passing after
   a regression. Fixtures should use a fixed date that is deliberately *not*
   today.

**The fix, not a suggestion:** derive the value from the same clock that produces
the data — `CURRENT_DATE`, `NOW()`, `clock_timestamp()`, or an
`INTERVAL` expression evaluated server-side. If Python needs the value, fetch it
(`SELECT CURRENT_DATE`) rather than computing it.

**Do NOT flag:**

- Clocks that never reach the database: log timestamps, in-process rate-limiter
  windows, sleep durations, metrics emitted to Prometheus, elapsed-time
  measurement.
- One-shot scripts and migrations where the operator's local date is the
  intended input.
- `clock_timestamp()` vs `NOW()` distinctions inside a transaction — both are
  server-side and that choice is about statement-versus-transaction time, not
  clock provenance.

**Review approach:**

1. `grep -n "date.today()\|datetime.now()\|utcnow()\|time.time()"` over the diff.
2. For each hit, trace whether the value reaches a cursor. If it does not, skip it.
3. If it does, ask what column or expression it is compared against or used to
   name. Anything derived from `NOW()` on the server is a finding.
4. Check the tests for the same construct — a client-clock fixture hides the bug
   it is meant to catch.

**History:** `ops.py` created write-path partitions from `date.today()` while
`created_at` came from server `NOW()`, and `partitions.py` did the same in
scheduled maintenance. Both were invisible on ares and hades because both are
UTC. Found by review, not by tests — two of the tests asserted the client clock
and would have kept passing.

---

## transactional-decision-reviewer

**The database is the arbiter.** Existence, uniqueness, ordering, atomicity, the
current time — these are things Postgres knows authoritatively and this process
does not. Code that decides them in Python is holding a second opinion that can
disagree with the only one that counts, and the disagreement is silent.

Review code that **acts on a guess about database state** instead of asking
inside a transaction. When a decision depends on whether a row, table, or
partition exists, and the cost of checking is a cheap query on an
already-open connection, the check belongs in the transaction rather than being
inferred from an error code or a lock outcome.

The failure this catches is specific: an exception is treated as evidence of
something it does not actually prove, so the code takes the wrong branch and
reports success. That reads as defensive programming and behaves as a lie.

**Default severity: P2.** **P1** when the wrong branch is reported as success,
because the caller then proceeds on a false belief.

**What to flag:**

0. **A decision Postgres could have made, made in Python instead.** A
   read-modify-write where one statement would do; a uniqueness check racing
   ahead of a constraint; ordering imposed after the fact rather than by `ORDER
   BY`; a timestamp or date computed locally (see clock-discipline-reviewer).
   The test is whether the database could arbitrate authoritatively in one
   statement — if so, the Python version is a second opinion.
1. **An error code read as proof of state.** `DuplicateTable` treated as "it
   exists, therefore fine" — it proves a *name* is taken, not that the object is
   the thing you wanted. `LockNotAvailable` treated as "someone else is doing my
   work" — it proves contention, and the holder may be doing something else
   entirely, including dropping the object you assumed it was creating.
2. **A guess where a query was already affordable.** The connection is open, the
   transaction is live, and the check is an indexed catalogue or primary-key
   lookup. Prefer: attempt, and on ambiguity ask; or check, then act, with the
   idempotent guard to cover the window.
3. **Compensating logic built on the guess** — retries, backoff, or "assume it
   worked" paths that exist only because the code declined to look.

**Where a guess is legitimate — do NOT flag:**

- **When checking would contend.** The point is to avoid guessing, not to add
  locking. A check that itself takes a heavy lock (`SELECT FOR UPDATE` on a hot
  row, anything needing `ACCESS EXCLUSIVE`) is worse than the guess it replaces.
  Catalogue reads, PK lookups and `to_regclass` are cheap and take no such lock;
  prefer those.
- **When the check cannot be authoritative.** If the answer can change between
  the check and the act and the act is not idempotent, checking buys false
  confidence. Say so rather than adding a check that reads as a guarantee.
- **Hot paths where the extra round trip is measurable** and the guess is
  documented as an accepted approximation.
- **Advisory-lock serialisation** already used to make a decision safe — that is
  the correct pattern, not a guess.

**Review approach:**

1. Find `except` clauses that set a state variable, return a status, or log
   success, rather than re-raising or handling a genuine error.
1b. And find statements whose own result is discarded before success is
   reported -- a DELETE or UPDATE whose `rowcount` is never read, an INSERT
   whose RETURNING is ignored. This is the same defect without an exception to
   notice: the database said how many rows it touched and the code did not
   listen. Grepping for `except` alone misses it, which is how it survived here.
2. For each, ask what the exception actually proves, and whether the code's next
   action assumes more than that.
3. If more: can the truth be obtained with a cheap query on the open connection?
   If yes, that is the finding. If the only check would contend or could not be
   authoritative, leave it and say why.
4. Check the tests: a mock that always returns the happy answer cannot tell a
   correct concession from a lucky one.

**History:** the queue's write-path partition self-heal treated both
`DuplicateTable` and `LockNotAvailable` as "the partition exists". Neither is:
the first can be a same-named relation that is not a partition of this queue,
and the second can be `ALTER TABLE` or `DROP TABLE` holding the parent, with a
longer lock timeout than the self-heal's own. It now asks `pg_inherits` after
either failure -- a cheap catalogue read on the connection it already holds.

---

## watermark-integrity-reviewer

The watermark is the only thing standing between an incremental sync and silent
data loss. Everything else in this repo fails loudly: a bad parse raises, a dead
API call retries, a missing partition self-heals. A watermark that moves too far
fails by **not fetching something**, and there is no error, no retry, and no row
to notice missing. The next run starts after the gap and the gap is permanent.

Review any code that reads, computes, or writes `granola_sync_state`, or that
decides which window to scan.

**Default severity: P1.** A watermark defect loses data that is still available
from the API today and may not be tomorrow — Granola only returns notes that
have a generated summary, and access can be revoked.

**What to flag:**

1. **The watermark advancing after a run that had errors.** If any note in the
   pass failed to ingest, `last_synced_at` must stay where it was. Recording
   `last_poll_at` and `last_error` is right; moving the watermark is not. The
   failed notes are inside the old window and outside the new one.

2. **The overlap window baked into stored state.** `last_synced_at` is the
   high-water mark of *what was successfully synced*; the overlap is applied
   when the window is **read**, never subtracted before the write. Storing
   `now - overlap` makes the overlap compound on every run — a 60-minute overlap
   applied 24 times has walked the watermark a day backwards, and the scan grows
   without bound.

3. **A watermark taken from the client clock.** The window is compared against
   the API's `updated_at`, and the row is stamped by Postgres. See
   `clock-discipline-reviewer` — this is that bug in its most expensive form.

4. **A watermark advanced to "now" rather than to the start of the scan.** Notes
   edited *during* the pass fall between the scan start and the write. Capture
   the timestamp before listing, advance to that.

5. **A partial failure that narrows the next scan.** Any early `return`, `break`,
   or exception path that still reaches the watermark write. Trace every exit
   from the poll function, not just the happy one.

6. **Dropping the overlap because dedup exists.** Idempotent ingest makes
   re-scanning cheap; it does not make skipping safe. The two solve different
   problems and one is not a licence to remove the other.

7. **Tests that assert a watermark advanced without asserting it did not
   advance on failure.** The negative case is the one that matters, and it is
   the one usually missing.

**Do NOT flag:**

- A deliberate `--full` or `--since` override that ignores the watermark; that is
  an operator instruction, not an incremental run.
- Backfill jobs, which are bounded by explicit dates and carry no watermark.
- The first run, where the watermark is NULL and the whole history is in scope.

**Review approach:**

1. Find every write to `granola_sync_state.last_synced_at`.
2. For each, walk backwards to every path that can reach it. Ask: could this run
   have skipped or failed a note and still get here?
3. Find every read. Confirm the overlap is applied at read time and the stored
   value is untouched by it.
4. Check the tests for a failure-path assertion.

---

## export-destructiveness-reviewer

The export worker **deletes files**. It is the only component in this repo that
destroys anything, and it operates on a directory inside the user's Obsidian
vault, alongside notes it did not create.

The dangerous shape is deletion driven by *absence*. "The database has no row
for this file, therefore remove it" turns every query bug, every mid-migration
state, and every empty result into a bulk delete of the user's notes. Deletion
must be driven by **presence of a tombstone** — a `granola_notes_raw` row with
`deleted_at` set, naming the files it owns.

**Default severity: P1** for anything that widens what can be deleted or weakens
the link between a deletion and a tombstoned row. **P2** for a path that is
merely unbounded rather than wrong.

**What to flag:**

1. **A deletion justified by a missing row.** Any `os.remove` / `unlink` /
   `shutil.rmtree` reached because a lookup returned nothing. An empty result
   set is indistinguishable from a failed query, a wrong `WHERE`, or a database
   that has not finished migrating.

2. **A deletion not rooted in the configured export directory.** Every path must
   be resolved (`Path.resolve()`) and confirmed to be under
   `ExportConfig.notes_dir` or `transcripts_dir` before the call. A note title
   reaches the filename, and titles come from an external API — `../` is not
   hypothetical.

3. **`rmtree` at all.** The unit of deletion is a file this worker wrote and
   whose path is recorded. Removing a directory removes things it did not write.

4. **A filesystem walk as the source of truth.** Diffing the tree against the
   database and deleting the difference makes every unrelated file in the vault
   a deletion candidate. Iterate over tombstoned rows instead.

5. **Deleting before writing on a rename.** A retitled note changes filename. The
   new file is written first and confirmed, then the old is removed — the
   reverse leaves a window where the note exists nowhere.

6. **Losing the record of which files a note owned.** Once the file list is gone
   the worker cannot clean up after that note ever again. This is why
   `tombstone()` sets `deleted_at` rather than deleting the row.

7. **A dry-run flag that does not actually suppress the write.** If the code has
   one, verify it gates the destructive call and not merely the log line.

**Do NOT flag:**

- Overwriting a file the worker owns — that is the normal update path.
- Writing to a temp file and `replace()`-ing it; atomic replace is correct.
- Deletions in tests operating on `tmp_path`.

**Review approach:**

1. `grep -n "remove\|unlink\|rmtree\|\.replace(" ` over the diff.
2. For each destructive call, answer in one sentence: *which tombstoned row
   authorises this?* If the answer involves absence, it is a P1.
3. Confirm the resolved path is checked against the export root, and that the
   check happens after resolution, not before.
4. Confirm the file list came from recorded state, not from a directory listing.

---
# Guidelines

A reviewer pack for Python projects. Each section below is an independent reviewer prompt with its own scope, set of patterns to flag, and disposition.

## How the pack runs

**This file is consumed BY the `pr-review-loop` skill — do not run these reviewers directly.** If you've read this file and are about to spawn the agents yourself (outside the skill), stop and invoke `pr-review-loop` instead: the skill owns the parts this file doesn't define — agents posting findings as PR line comments, independent validation of posted findings, per-thread replies, per-agent retirement, CI gating, and exit conditions. Hand-spawning from this file skips all of that and breaks the PR audit trail.

Each reviewer runs independently and reports findings without coordination. A reviewer's silence on something is not an endorsement — it just means that reviewer didn't see anything in its scope.

**Per-reviewer file scope:**

| Reviewer | Files in scope |
|----------|----------------|
| `test-coverage-reviewer` | `*.py` |
| `error-handling-reviewer` | `*.py` (production code) |
| `logging-reviewer` | `*.py` (rules differ for CLI entry points vs libraries/services) |
| `complexity-reviewer` | `*.py` (skips `tests/`) |
| `concurrency-reviewer` | `*.py` touching `threading`, `multiprocessing`, `asyncio`, `concurrent.futures`, or `queue` |
| `resource-leak-reviewer` | `*.py` |
| `external-process-reviewer` | `*.py` touching `subprocess`, `os.system`, `shutil.which` |
| `dead-code-reviewer` | `*.py` |
| `import-reviewer` | `*.py` |
| `importlib-resources-reviewer` | `*.py` in packaged modules (skips `tests/` and ad-hoc scripts) |
| `dataclass-decorator-reviewer` | `*.py` using `dataclasses.field` |
| `typing-consistency-reviewer` | `*.py` files that already use type annotations |
| `clarity-reviewer` | `*.md`, `*.py` (docstrings and comments) |

Skip reviewers whose file scope doesn't match the PR diff.

## Tooling assumed in CI

Reviewers do not duplicate work already done by CI. Each Python project running this pack should have these in CI:

- `ruff check .` — lints, import sorting (`I`), unused imports (`F401`), unused vars (`F841`), and more
- `ruff format --check .` — formatter (Black-compatible)
- `mypy .` or `pyright .` — type checker, strict mode preferred
- `pytest -v` with `--cov` — tests with coverage tracking
- `bandit -r .` — security linter (catches `shell=True`, weak crypto, etc.)
- `vulture .` — dead-code detector
- `ruff check --select C901 --max-complexity 10 .` — mccabe complexity ceiling (see `complexity-reviewer`)

A missing tool is itself a **P1** finding on the first PR that brushes against its scope. For example, if CI doesn't run `bandit` and the PR touches `subprocess`, `external-process-reviewer` flags the CI gap as well as the code.

## Severity convention

Every finding must be tagged with a beads-style priority:

| Priority | Disposition | Examples |
|----------|-------------|----------|
| **P1** | Blocking — must fix before merge | Silent exception swallow, new `threading.Lock` without justification, missing `@dataclass` on a class using `field()`, leaked DB connection, command injection via `shell=True` |
| **P2** | Should fix in this PR | Missing `raise ... from`, missing `subprocess` timeout, missing tests for new branches, mccabe complexity floor violation, function-level import |
| **P3** | Advisory — deferrable with a beads ticket | Complexity heuristic findings, dead code, clarity nits, markdown structural suggestions |

**Default severity per reviewer:**

- `concurrency-reviewer`, `resource-leak-reviewer`: **P1** by default.
- `error-handling-reviewer`: **P2** by default; **P1** for silent exception swallows and `return` in `finally`.
- `logging-reviewer`: **P2** by default; **P1** for sensitive data in log messages.
- `dataclass-decorator-reviewer`: **P1** by default (it's a correctness bug that escapes type checkers).
- `test-coverage-reviewer`, `external-process-reviewer`, `importlib-resources-reviewer`, `typing-consistency-reviewer`: **P2** by default; specific flags inside each reviewer may be P1.
- `complexity-reviewer`: **P2** for mccabe floor violations; **P3** for heuristic findings.
- `import-reviewer`, `dead-code-reviewer`, `clarity-reviewer`: **P3** by default.

A reviewer may promote or demote a specific finding from its default, but must state why.

## Output format

Findings must be structured. Use this template:

```
[<reviewer>] [<severity>] <one-line title>

File: path/to/file.py:LINE
Quote:
    <1-5 lines of code or text being flagged>

Issue: <one or two sentences on what's wrong>
Suggested fix:
    <concrete diff or rewritten code/text>
Reason (optional): <only if not obvious>
```

Example:

```
[concurrency-reviewer] [P1] New threading.Lock without justification

File: cache/store.py:18
Quote:
    class Cache:
        def __init__(self):
            self._lock = threading.Lock()
            self._data: dict[str, Any] = {}

Issue: New threading.Lock protects mutable shared state. Per the pack's design
philosophy, threading locks are flagged by default — move the data into a worker
process behind a queue, or into an asyncio task that owns the state, unless this
falls into "Tolerated uses" in concurrency-reviewer.
Suggested fix: Use multiprocessing.Process + queue.Queue for cross-process shared
state, or an asyncio task that owns the dict and exposes async methods.
```

Structured findings are diff-able, easy to triage, and easy to deduplicate when multiple reviewers flag the same line.

## Deferring findings with beads

To defer a P2 or P3 finding to a follow-up:

1. Create a beads ticket capturing reviewer name, severity, file, and quote.
2. Link the ticket in the PR description or as a reply to the reviewer's comment.
3. The reviewer accepts the deferral only when the beads ticket exists.

**P1 findings are not deferrable** — they must be fixed in-PR.

---
