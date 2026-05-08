from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

from textual.app import App, ComposeResult
from textual.binding import Binding
from textual.containers import Horizontal, Vertical
from textual.screen import ModalScreen
from textual.widgets import Button, DataTable, Footer, Header, Label, RichLog, Static, Tree

CSS = """
Screen {
    layout: vertical;
    background: #0d1b2a;
}

#main_pane {
    layout: horizontal;
    height: 1fr;
    background: #0d1b2a;
}

#tree_panel {
    width: 44;
    border: double #00d4ff;
    padding: 0 1;
    background: #162033;
}

#detail_panel {
    width: 1fr;
    border: solid #1e3448;
    padding: 0 2;
    background: #1a2a3a;
}

#tree_label {
    height: 1;
    background: #00d4ff;
    color: #0d1b2a;
    text-style: bold;
    padding: 0 1;
}

#detail_label {
    height: 1;
    color: #00d4ff;
    background: #1a2a3a;
    border-bottom: solid #1e3448;
    text-style: bold;
    padding: 0 1;
}

#detail_content {
    height: 1fr;
}

#status_bar {
    height: 1;
    padding: 0 1;
    background: #162033;
    color: #5a7a8a;
    border-top: solid #1e3448;
}

Tree {
    background: transparent;
    color: #cce8f0;
}

.tree--cursor {
    background: #1a2a3a;
    color: #00d4ff;
}

.tree--guides {
    color: #1e3448;
}

DataTable {
    background: #1a2a3a;
    height: 1fr;
}

.datatable--header {
    background: #162033;
    color: #00d4ff;
    text-style: bold;
}

.datatable--cursor {
    background: #0d1b2a;
    color: #00d4ff;
}

.datatable--hover {
    background: #1e3448;
}
"""

STEP_DETAIL_CSS = """
ModalScreen {
    align: center middle;
}

#modal_container {
    width: 80%;
    height: 80%;
    border: double #00d4ff;
    background: #162033;
    padding: 1 2;
}

#modal_log {
    height: 1fr;
    background: #0d1b2a;
    border: solid #1e3448;
}

#modal_buttons {
    height: 3;
    align: center middle;
}

.modal_section_header {
    color: #ffb84d;
    text-style: bold;
}
"""


class StepDetailModal(ModalScreen):
    """Modal showing full step command + output."""

    CSS = STEP_DETAIL_CSS
    BINDINGS = [
        Binding("escape", "dismiss", "Close"),
        Binding("q", "dismiss", "Close"),
    ]

    def __init__(self, step_name: str, step_data: dict) -> None:
        super().__init__()
        self._step_name = step_name
        self._step_data = step_data

    def compose(self) -> ComposeResult:
        with Vertical(id="modal_container"):
            yield Label("Step Execution Details", classes="modal_section_header", id="modal_header")
            yield RichLog(id="modal_log", highlight=True, markup=True)
            with Horizontal(id="modal_buttons"):
                yield Button("Close [Esc]", id="btn_close")

    def on_mount(self) -> None:
        log = self.query_one("#modal_log", RichLog)
        d = self._step_data

        state = str(d.get("state", "?"))
        if "." in state:
            state = state.split(".")[-1]

        _state_colors = {
            "DONE": "#00ff88",
            "FAILED": "#ff3860",
            "SKIPPED": "#ffb84d",
            "RUNNING": "#4d90fe",
        }
        state_color = _state_colors.get(state, "#cce8f0")
        log.write(f"[bold]State:[/bold] [{state_color}]{state}[/{state_color}]")
        log.write(f"[bold #cce8f0]{self._step_name}[/bold #cce8f0]")

        cmd = d.get("command")
        if cmd:
            log.write(f"\n[bold #ffb84d]Command:[/bold #ffb84d]\n  {cmd}")

        stdout = d.get("stdout")
        if stdout and stdout.strip():
            log.write(f"\n[bold #ffb84d]stdout:[/bold #ffb84d]\n{stdout.strip()}")

        stderr = d.get("stderr")
        if stderr and stderr.strip():
            log.write(f"\n[bold #ff3860]stderr:[/bold #ff3860]\n{stderr.strip()}")

        error = d.get("error")
        if error and error.strip():
            log.write(f"\n[bold #ff3860]error:[/bold #ff3860]\n{error.strip()}")

        outputs = d.get("outputs", {})
        if outputs:
            log.write(f"\n[bold #ffb84d]Outputs:[/bold #ffb84d]")
            for k, v in outputs.items():
                if k != "output":
                    log.write(f"  {k}: {v}")

    def on_button_pressed(self, event: Button.Pressed) -> None:
        self.dismiss()


def _scan_runs(runs_dir: Path) -> dict[str, list[tuple[str, dict]]]:
    """Scan runs_dir for provenance.json files.

    Returns: {workflow_name: [(run_id, provenance_dict), ...]} sorted newest first.
    """
    result: dict[str, list[tuple[str, dict]]] = {}

    for child in sorted(runs_dir.iterdir()):
        if not child.is_dir():
            continue
        prov_file = child / "provenance.json"
        if not prov_file.exists():
            continue
        try:
            prov = json.loads(prov_file.read_text())
        except Exception:
            continue

        wf_name = prov.get("workflow_name", child.name)
        run_id = prov.get("run_id", child.name)

        if wf_name not in result:
            result[wf_name] = []
        result[wf_name].append((run_id, prov))

    # Sort each workflow's runs newest first
    for wf_name in result:
        result[wf_name].sort(key=lambda x: x[0], reverse=True)

    return result


_STATUS_ICONS = {
    "SUCCESS": ("✓", "#00ff88"),
    "FAILED": ("✗", "#ff3860"),
    "RUNNING": ("●", "#4d90fe"),
    "PENDING": ("○", "#5a7a8a"),
}


class RunHistoryBrowser(App):
    """Interactive TUI for browsing past cdo-flow run history."""

    TITLE = "cdo-flow history"
    CSS = CSS
    BINDINGS = [
        Binding("q", "quit", "Quit"),
        Binding("i", "inspect_step", "Inspect step"),
        Binding("r", "refresh", "Refresh"),
    ]

    def __init__(self, runs_dir: Path = Path("./runs")) -> None:
        super().__init__()
        self._runs_dir = runs_dir
        self._runs_data: dict[str, list[tuple[str, dict]]] = {}
        self._selected_prov: dict | None = None
        self._selected_step: str | None = None

    def compose(self) -> ComposeResult:
        yield Header(show_clock=True)
        with Horizontal(id="main_pane"):
            with Vertical(id="tree_panel"):
                yield Label("Runs", id="tree_label")
                yield Tree("Workflows", id="run_tree")
            with Vertical(id="detail_panel"):
                yield Label("Run Details", id="detail_label")
                yield DataTable(id="detail_table", cursor_type="row")
        yield Label("", id="status_bar")
        yield Footer()

    def on_mount(self) -> None:
        self._runs_data = _scan_runs(self._runs_dir)

        tree = self.query_one("#run_tree", Tree)
        tree.root.expand()

        for wf_name, runs in self._runs_data.items():
            wf_node = tree.root.add(
                f"[bold #00d4ff]{wf_name}[/bold #00d4ff]  [#5a7a8a]({len(runs)} run(s))[/#5a7a8a]",
                expand=True,
            )
            for run_id, prov in runs:
                status = prov.get("status", "?")
                icon, color = _STATUS_ICONS.get(status, ("?", "#cce8f0"))
                label = f"[{color}]{icon}[/{color}] [italic]{run_id}[/italic]"
                wf_node.add_leaf(label, data={"run_id": run_id, "wf_name": wf_name})

        table = self.query_one("#detail_table", DataTable)
        table.add_columns("Step", "Status", "Duration", "Outputs")

        if not self._runs_data:
            self._update_status(f"No runs found in {self._runs_dir}")
        else:
            total = sum(len(runs) for runs in self._runs_data.values())
            self._update_status(
                f"{len(self._runs_data)} workflow(s), {total} run(s) in {self._runs_dir}"
            )

    def on_tree_node_selected(self, event: Tree.NodeSelected) -> None:
        node = event.node
        data = node.data
        if data is None:
            return

        wf_name = data.get("wf_name")
        run_id = data.get("run_id")
        if not wf_name or not run_id:
            return

        runs = self._runs_data.get(wf_name, [])
        prov = next((p for rid, p in runs if rid == run_id), None)
        if prov is None:
            return

        self._selected_prov = prov
        self._selected_step = None
        self._populate_detail(prov)

    def _populate_detail(self, prov: dict) -> None:
        table = self.query_one("#detail_table", DataTable)
        table.clear()

        wf_name = prov.get("workflow_name", "?")
        run_id = prov.get("run_id", "?")
        status = prov.get("status", "?")
        start = prov.get("start_time", "")
        end_t = prov.get("end_time", "")
        try:
            s = datetime.fromisoformat(start)
            e = datetime.fromisoformat(end_t)
            total_dur = f"{(e - s).total_seconds():.1f}s"
        except Exception:
            total_dur = "-"

        status_color = "#00ff88" if status == "SUCCESS" else "#ff3860"
        self.query_one("#detail_label", Label).update(
            f"[bold #00d4ff]▶ {wf_name}[/bold #00d4ff]  [#5a7a8a]{run_id}[/#5a7a8a] — "
            f"[{status_color}]{status}[/{status_color}]  {total_dur}"
        )

        steps_data = prov.get("steps", {})
        _state_colors = {
            "DONE": "#00ff88",
            "FAILED": "#ff3860",
            "SKIPPED": "#ffb84d",
            "RUNNING": "#4d90fe",
        }
        for step_name, rec in steps_data.items():
            state = str(rec.get("state", "?"))
            if "." in state:
                state = state.split(".")[-1]
            try:
                s_t = rec.get("start_time")
                e_t = rec.get("end_time")
                if s_t and e_t:
                    dur = f"{(datetime.fromisoformat(e_t) - datetime.fromisoformat(s_t)).total_seconds():.2f}s"
                else:
                    dur = "-"
            except Exception:
                dur = "-"

            outputs = rec.get("outputs", {})
            out_str = ", ".join(k for k in outputs if k != "output") if outputs else "-"
            color = _state_colors.get(state, "#cce8f0")
            table.add_row(
                step_name,
                f"[{color}]{state}[/{color}]",
                dur,
                out_str,
                key=step_name,
            )

    def on_data_table_row_selected(self, event: DataTable.RowSelected) -> None:
        self._selected_step = str(event.row_key.value)
        self._update_status(
            f"Selected step: {self._selected_step}  [#00d4ff][I][/#00d4ff] to inspect details"
        )

    def action_inspect_step(self) -> None:
        if self._selected_prov is None or self._selected_step is None:
            self._update_status("Select a run and a step first, then press [I]")
            return
        step_data = self._selected_prov.get("steps", {}).get(self._selected_step, {})
        self.push_screen(StepDetailModal(self._selected_step, step_data))

    def action_refresh(self) -> None:
        tree = self.query_one("#run_tree", Tree)
        tree.clear()
        self.on_mount()

    def _update_status(self, text: str) -> None:
        self.query_one("#status_bar", Label).update(f"[#5a7a8a]{text}[/#5a7a8a]")

    def action_quit(self) -> None:
        self.exit()
