# Alopex CLI TUI Guide

The TUI provides an interactive preview of SELECT results. It is optimized for
keyboard navigation and fast search.

## Getting started

Run a SELECT query with `--tui`:

```bash
alopex sql --tui "SELECT id, name, created_at FROM users"
```

Use a server profile:

```bash
alopex --profile prod sql --tui "SELECT id, name FROM users"
```

If the terminal is not a TTY (for example, piping output), the CLI falls back to
batch output while preserving `--output` formatting.

## Layout overview

The screen is divided into three areas:

- Results table (top)
- Detail panel (middle, optional)
- Status bar (bottom)

Example (text-only layout):

```
+-------------------- Results --------------------+
| #  id  name         created_at                  |
| 1  10  alice        2024-01-01T00:00:00Z         |
| 2  11  bob          2024-01-02T00:00:00Z         |
+-------------------------------------------------+
+-------------------- Detail ---------------------+
| JSON                                               
| {"id":10,"name":"alice",...}                    
|                                                   
| YAML                                               
| id: 10                                             
| name: alice                                        
+-------------------------------------------------+
+-------------------- Status ---------------------+
| Connection: local | Rows: 2 | Status: ready ... |
+-------------------------------------------------+
```

## Keybindings

General navigation:

- `q` / `Esc`: quit
- `?`: toggle help
- `hjkl` or arrow keys: move selection
- `g` / `G`: jump to top/bottom
- `Ctrl+d` / `Ctrl+u`: page down/up

Search:

- `/`: enter search mode
- `n`: next match
- `N`: previous match
- `Enter`: confirm search
- `Esc`: cancel search

Detail panel:

- `Enter`: toggle detail panel
- `J` / `K`: scroll detail panel down/up

## Examples

Search for a name:

1. Press `/`
2. Type `alice`
3. Press `n` to jump to the next match

View JSON/YAML detail:

1. Navigate to a row
2. Press `Enter`
3. Use `J`/`K` to scroll

## Troubleshooting

- "TUI requires a TTY": run without `--tui` or use a real terminal.
- Rows not visible: increase the terminal height or shrink columns by selecting
  fewer fields.
- Large result sets: use `--max-rows`/`--limit` or filter with `WHERE` to limit data.
- Search slow: narrow the dataset or reduce text-heavy columns.
