# About these docs

This is a documentation portal with a deliberate structure, built and
versioned like code. This page records the editorial contract so
contributions keep the shape.

## Audience

- **Primary**: .NET developers building services with HostLoom.
  Assumed: comfortable with C#, `async`/`await`, and
  `Microsoft.Extensions.DependencyInjection`/`Hosting`; no prior
  messaging-framework experience required.
- **Secondary**: operators running HostLoom services (health, metrics,
  broker topology), and contributors to HostLoom itself.
- Broker knowledge is *not* assumed by tutorials; broker-specific how-to
  guides assume you can run `docker compose` and know your broker's basic
  vocabulary (queue, exchange, topic, consumer group).

## Structure: Diátaxis

The documentation follows [Diátaxis](https://diataxis.fr/), which
organizes content around four reader needs:

| Section | Reader is… | Register |
| --- | --- | --- |
| Tutorials | learning, hands-on | a lesson: steps on rails, minimal explanation |
| How-to guides | working toward a goal | a recipe: assumes competence, gets to the point |
| Reference | looking something up | facts: terse, complete, no persuasion |
| Explanation | building understanding | discussion: the why, trade-offs, boundaries |

Each page has **one primary purpose**. A sentence or two of context or a
supporting example is fine; when a page starts genuinely serving a second
need — a how-to accumulating rationale, reference sprouting steps — that
content moves to the section where it belongs, linked from where it was
tempting to inline it.

How-to guides follow a consistent recipe: *before you begin → install →
configure → verify → troubleshoot → related pages*.

## Workflow: docs-as-code

- Source lives in `docs/` as Markdown, versioned with the code it
  documents, reviewed through the same pull requests.
- The site is built by [MkDocs](https://www.mkdocs.org/) with the
  [Material](https://squidfunk.github.io/mkdocs-material/) theme;
  navigation is defined in `mkdocs.yml`.
- `strict: true` is the quality gate: broken links and orphaned nav
  entries fail the build instead of shipping.
- The Python toolchain is managed by [uv](https://docs.astral.sh/uv/) —
  `pyproject.toml` and `uv.lock` at the repository root pin it
  (Python 3.14).

## Building locally

```text
uv sync               # once, installs mkdocs + theme into .venv
uv run mkdocs serve   # live preview at http://127.0.0.1:8000
uv run mkdocs build   # strict build into site/
```

## Code samples

- **Real names, verified against source.** Types, methods, options, and
  defaults come from the code or tests, never from memory. When the API
  changes, the docs change in the same pull request.
- **Tutorial code must compile and run** exactly as shown, from an empty
  directory, with every file's name and placement stated. How-to and
  explanation fragments may elide surrounding code, but must state what
  they elide ("inside the `Pipe.Create` callback").
- Keep code lines under roughly 100 characters so they read without
  horizontal scrolling.
- Aspiration, not yet reality: extract tutorial samples into checked-in
  projects compiled in CI, so drift fails a build instead of a reader.

## Voice and language

- Write for a global audience: plain sentences, simple words, no idioms
  that do not travel. Contractions are fine.
- Address the reader as "you"; use "we" only for decisions the project
  made.
- Lead with what the reader gets, not with history or internals.
- Explanation pages may take a position, and several do — state the
  trade-off and the alternative, not just the conclusion.

## Terminology

Used consistently, never interchangeably:

- **address** — the logical name a *request* is sent to; **topic** — the
  logical name an *event* is published to; **subscription** — a named,
  independent consumer of a topic.
- **handler** processes requests or events; **behavior** wraps a request
  handler; **filter** is a pipeline element; the **receive pipeline** is
  the filter chain wrapping inbound delivery.
- **fault** — a handler failure encoded on the wire; **redelivery** — the
  broker's re-offer of a message; **retry** — an in-process re-attempt.

## Versioning

The docs describe the code at the same git revision — they are versioned
*with* the product, and a release tag snapshots both. Pages do not pin
package versions in install commands; `CHANGELOG.md` is the record of
what changed when.

## Sources

The approach draws on [Diátaxis](https://diataxis.fr/), the
[Material for MkDocs](https://squidfunk.github.io/mkdocs-material/)
documentation, established docs-as-code practice, and the
[Google developer documentation style guide](https://developers.google.com/style)
and [Microsoft style guide](https://learn.microsoft.com/style-guide/welcome/)
for voice, procedures, and code-sample conventions.
