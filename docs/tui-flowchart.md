# TUI UI Flowchart and Keybind Summary (v0.4.2)

This document summarizes the TUI flow (results and admin) and keybindings.

## CLI -> UI Mode Flow

```mermaid
flowchart TD
    A[CLI Entry] --> B[Resolve UI Mode]
    B -->|UiMode::Batch| C[Batch Output]
    B -->|UiMode::Tui| D{Command Provided?}
    D -->|No| E[Admin UI]
    D -->|Yes| F[Execute Command]
    F --> G[Collect Rows/Columns]
    G --> H[render_output]
    H --> I[TuiApp]
    I -->|q/Esc| J[Exit TUI]
    I -->|a| E
    E -->|a/q/Esc| I
    I -->|Run fails| C
```

## Results TUI Flow

```mermaid
flowchart TD
    R0[TuiApp] --> R1[Table View]
    R0 --> R2[Detail Panel]
    R0 --> R3[Status Bar]
    R1 --> R4[Search]
    R4 --> R1
    R0 -->|a| A1[Admin UI]
    A1 -->|a| R0
```

## Admin TUI Flow

```mermaid
flowchart TD
    A0[Admin UI] --> A1[Resources Tree]
    A0 --> A2[Actions List]
    A0 --> A3[Detail/Input]
    A0 --> A4[Status/Preview]
    A1 -->|Enter/select| A5[Sync Target + Fill Fields]
    A2 -->|Up/Down| A6[Select Action]
    A3 -->|e/r| A7[Edit Fields or Raw Params]
    A3 -->|o| A8[Open Selection Overlay]
    A7 -->|Enter| A9[Execute Action]
    A9 --> A4
```

## Keybinds (Results TUI)

| Key | Action |
| --- | --- |
| q / Esc | Quit |
| ? | Toggle help |
| / | Search mode |
| n / N | Next/Prev match |
| h j k l | Move selection / scroll columns |
| g / G | Jump top/bottom |
| Ctrl+d / Ctrl+u | Page down/up |
| Enter | Toggle detail panel |
| J / K | Scroll detail panel |
| a | Admin console (when available) |

## Keybinds (Admin TUI)

| Area | Key | Action |
| --- | --- | --- |
| Global | q / Esc / a | Exit admin |
| Global | ? | Toggle help |
| Global | h / l | Focus left/right |
| Resources | j / k | Move selection |
| Resources | g / G | Jump top/bottom |
| Resources | Ctrl+d / Ctrl+u | Page down/up |
| Resources | / | Search |
| Resources | Enter | Apply selection |
| Resources | R | Reload |
| Detail/Input | Up / Down | Move action selection |
| Detail/Input | Tab / Shift+Tab | Move active field |
| Detail/Input | e | Edit active field |
| Detail/Input | r | Toggle raw params |
| Detail/Input | o | Open list for active field |
| Detail/Input | Enter | Execute action |
| Status/Preview | j / k | Scroll |
| Status/Preview | g / G | Jump top/bottom |
| Status/Preview | Ctrl+d / Ctrl+u | Page down/up |
