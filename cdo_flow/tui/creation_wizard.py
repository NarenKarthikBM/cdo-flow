from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml
from textual.app import App, ComposeResult
from textual.binding import Binding
from textual.containers import Horizontal, ScrollableContainer, Vertical
from textual.screen import ModalScreen, Screen
from textual.widgets import (
    Button,
    Footer,
    Header,
    Input,
    Label,
    ListItem,
    ListView,
    Select,
    Static,
)

# ── CDO operator catalogue ─────────────────────────────────────────────────────

CDO_OPERATORS: list[tuple[str, str]] = [
    ("remapbil",     "Bilinear remapping"),
    ("remapcon",     "Conservative remapping"),
    ("remapnn",      "Nearest neighbor remapping"),
    ("selname",      "Select variables by name"),
    ("selyear",      "Select years"),
    ("selmon",       "Select months"),
    ("seldate",      "Select date range"),
    ("seltimestep",  "Select timesteps"),
    ("yearmean",     "Yearly mean"),
    ("monmean",      "Monthly mean"),
    ("daymean",      "Daily mean"),
    ("timmean",      "Time mean"),
    ("timstd",       "Time standard deviation"),
    ("timmax",       "Time maximum"),
    ("timmin",       "Time minimum"),
    ("yearsum",      "Yearly sum"),
    ("expr",         "Evaluate expression"),
    ("addc",         "Add constant"),
    ("mulc",         "Multiply by constant"),
    ("divc",         "Divide by constant"),
    ("subc",         "Subtract constant"),
    ("merge",        "Merge files"),
    ("cat",          "Concatenate files"),
    ("mergetime",    "Merge by time axis"),
    ("splitmon",     "Split by month"),
    ("splityear",    "Split by year"),
    ("ymonmean",     "Monthly climatology mean"),
    ("ymonstd",      "Monthly climatology std dev"),
    ("ymonsub",      "Subtract monthly climatology"),
    ("ydaymean",     "Daily climatology mean"),
    ("ydaysub",      "Subtract daily climatology"),
    ("setrtomiss",   "Set range to missing"),
    ("setmissval",   "Set missing value"),
    ("maskregion",   "Mask a region"),
    ("masklonlatbox","Mask lat-lon box"),
    ("sellevel",     "Select levels"),
    ("setname",      "Set variable name"),
    ("setunit",      "Set unit"),
    ("setlevel",     "Set level"),
    ("setgridtype",  "Set grid type"),
    ("griddes",      "Print grid description"),
    ("sinfo",        "Short dataset info"),
    ("ntime",        "Number of timesteps"),
    ("nvar",         "Number of variables"),
    ("showyear",     "Show years in file"),
    ("showname",     "Show variable names"),
]

# ── YAML builder ───────────────────────────────────────────────────────────────

def _build_yaml(wf_data: dict) -> str:
    """Convert accumulated wizard state to a YAML string."""
    doc: dict[str, Any] = {}

    if wf_data.get("name"):
        doc["name"] = wf_data["name"]
    if wf_data.get("description"):
        doc["description"] = wf_data["description"]
    if wf_data.get("run_dir"):
        doc["run_dir"] = wf_data["run_dir"]
    if wf_data.get("cdo_threads"):
        try:
            doc["cdo_options"] = {"threads": int(wf_data["cdo_threads"])}
        except ValueError:
            pass
    if wf_data.get("declared_inputs"):
        raw = [s.strip() for s in wf_data["declared_inputs"].split(",") if s.strip()]
        if raw:
            doc["inputs"] = raw
    if wf_data.get("output_path"):
        doc["output_path"] = wf_data["output_path"]

    params = wf_data.get("params", [])
    if params:
        doc["params"] = {k: v for k, v in params if k}

    steps = wf_data.get("steps", [])
    if steps:
        doc["steps"] = []
        for s in steps:
            step_dict: dict[str, Any] = {}
            if s.get("id"):
                step_dict["id"] = s["id"]
            step_dict["type"] = s.get("type", "python")

            if s.get("inputs"):
                inp_pairs = [p.strip() for p in s["inputs"].split(",") if p.strip()]
                parsed: dict[str, str] = {}
                for pair in inp_pairs:
                    if "=" in pair:
                        k, _, v = pair.partition("=")
                        parsed[k.strip()] = v.strip()
                    else:
                        parsed[pair] = pair
                if parsed:
                    step_dict["inputs"] = parsed

            if step_dict["type"] == "cdo":
                operators: list[str] = s.get("operators", [])
                if operators:
                    chain = []
                    for op_str in operators:
                        parts = op_str.split(None, 1)
                        op = parts[0]
                        args = parts[1].split() if len(parts) > 1 else []
                        chain.append({"op": op, "args": args} if args else {"op": op})
                    step_dict["operator_chain"] = chain
            elif step_dict["type"] == "python" and s.get("script"):
                step_dict["script"] = s["script"]

            if s.get("output"):
                step_dict["output"] = s["output"]
            if s.get("keep") in ("false", "False", "no", "0"):
                step_dict["keep"] = False

            doc["steps"].append(step_dict)

    return yaml.dump(doc, default_flow_style=False, sort_keys=False)


# ── Step breadcrumb widget ─────────────────────────────────────────────────────

class StepBreadcrumb(Static):
    """Progress breadcrumb showing which wizard screen is active."""

    DEFAULT_CSS = """
    StepBreadcrumb {
        height: 1;
        background: #162033;
        border-bottom: solid #1e3448;
        padding: 0 2;
    }
    """

    def __init__(self, current_step: int) -> None:
        labels = []
        for i, name in enumerate(["1: Metadata", "2: Steps", "3: Save"], start=1):
            if i == current_step:
                labels.append(f"[bold #00d4ff][ {name} ][/bold #00d4ff]")
            elif i < current_step:
                labels.append(f"[#00ff88]  {name}  [/#00ff88]")
            else:
                labels.append(f"[#5a7a8a]  {name}  [/#5a7a8a]")
        super().__init__(" → ".join(labels))


# ── Dynamic row widgets ────────────────────────────────────────────────────────

class ParamRow(Horizontal):
    """A key=value row with a delete button for the params section."""

    DEFAULT_CSS = """
    ParamRow {
        height: 3;
        margin-bottom: 0;
        background: #162033;
    }
    ParamRow Input {
        width: 1fr;
        margin-right: 1;
    }
    ParamRow Button {
        width: 3;
        min-width: 3;
    }
    """

    def __init__(self, row_id: int) -> None:
        super().__init__(id=f"param_row_{row_id}", classes="param_row")
        self._row_id = row_id

    def compose(self) -> ComposeResult:
        yield Input(placeholder="key", id=f"pk_{self._row_id}", classes="param_key")
        yield Input(placeholder="value", id=f"pv_{self._row_id}", classes="param_val")
        yield Button("×", id=f"pdel_{self._row_id}", classes="del_btn")

    def get_kv(self) -> tuple[str, str]:
        k = self.query_one(f"#pk_{self._row_id}", Input).value.strip()
        v = self.query_one(f"#pv_{self._row_id}", Input).value.strip()
        return k, v

    def set_kv(self, k: str, v: str) -> None:
        self.query_one(f"#pk_{self._row_id}", Input).value = k
        self.query_one(f"#pv_{self._row_id}", Input).value = v


class OperatorRow(Horizontal):
    """A row showing a CDO operator with a delete button."""

    DEFAULT_CSS = """
    OperatorRow {
        height: 3;
        margin-bottom: 0;
        background: #1a2a3a;
        border-bottom: dashed #1e3448;
    }
    OperatorRow Static {
        width: 1fr;
        padding: 1 0;
        color: #00d4ff;
    }
    OperatorRow Button {
        width: 3;
        min-width: 3;
    }
    """

    def __init__(self, op_str: str, row_id: int) -> None:
        super().__init__(id=f"op_row_{row_id}", classes="op_row")
        self._op_str = op_str
        self._row_id = row_id

    def compose(self) -> ComposeResult:
        yield Static(self._op_str, classes="op_label", id=f"op_label_{self._row_id}")
        yield Button("×", id=f"odel_{self._row_id}", classes="del_btn")


# ── CDO Operator Picker Modal ──────────────────────────────────────────────────

class CdoPickerModal(ModalScreen):
    """Searchable modal for selecting a CDO operator + args."""

    BINDINGS = [Binding("escape", "dismiss_none", "Cancel")]

    DEFAULT_CSS = """
    CdoPickerModal {
        align: center middle;
    }
    CdoPickerModal > Vertical {
        width: 70;
        height: 34;
        border: double #00d4ff;
        background: #162033;
        padding: 1 2;
    }
    #modal_title {
        text-style: bold;
        color: #00d4ff;
        height: 1;
        margin-bottom: 1;
    }
    #op_list {
        height: 1fr;
        border: none;
        background: #0d1b2a;
        margin-bottom: 1;
    }
    #op_list ListItem.--highlight {
        background: #1a2a3a;
        border-left: thick #00d4ff;
    }
    #modal_buttons {
        height: 3;
        layout: horizontal;
        align: center middle;
    }
    """

    def __init__(self) -> None:
        super().__init__()
        self._selected_op: str = ""

    def compose(self) -> ComposeResult:
        with Vertical():
            yield Label(
                f"Select CDO Operator  [#5a7a8a]({len(CDO_OPERATORS)} available)[/#5a7a8a]",
                id="modal_title",
            )
            yield Input(placeholder="Search operators...", id="op_search")
            yield ListView(
                *[
                    ListItem(Label(f"{op}  —  {desc}"), name=op)
                    for op, desc in CDO_OPERATORS
                ],
                id="op_list",
            )
            yield Label("Args (space-separated, optional)", classes="field_label")
            yield Input(placeholder="e.g.  r360x180", id="op_args")
            with Horizontal(id="modal_buttons"):
                yield Button("Select", id="btn_select", variant="primary")
                yield Button("Cancel", id="btn_cancel")

    async def on_input_changed(self, event: Input.Changed) -> None:
        if event.input.id != "op_search":
            return
        query = event.value.lower()
        lv = self.query_one("#op_list", ListView)
        filtered = [
            ListItem(Label(f"{op}  —  {desc}"), name=op)
            for op, desc in CDO_OPERATORS
            if query in op.lower() or query in desc.lower()
        ]
        await lv.clear()
        if filtered:
            await lv.mount(*filtered)

    def on_list_view_highlighted(self, event: ListView.Highlighted) -> None:
        if event.item is not None and event.item.name:
            self._selected_op = event.item.name

    async def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "btn_select":
            op = self._selected_op
            if not op:
                # Try the currently highlighted item
                lv = self.query_one("#op_list", ListView)
                if lv.highlighted_child is not None and lv.highlighted_child.name:
                    op = lv.highlighted_child.name
            if op:
                args = self.query_one("#op_args", Input).value.strip()
                self.dismiss((op, args))
        elif event.button.id == "btn_cancel":
            self.dismiss(None)

    def action_dismiss_none(self) -> None:
        self.dismiss(None)


# ── CSS ────────────────────────────────────────────────────────────────────────

CSS = """
Screen {
    layout: vertical;
    background: #0d1b2a;
}

.screen_title {
    display: none;
}

#outer {
    layout: horizontal;
    height: 1fr;
}

/* Form + preview panels (Metadata & Save screens) */
#form_panel {
    width: 60;
    border: double #00d4ff;
    padding: 1 2;
    overflow-y: auto;
    background: #162033;
}

.section_title {
    color: #00d4ff;
    text-style: bold;
    margin-bottom: 1;
    height: 1;
}

.field_label {
    color: #5a7a8a;
    text-style: italic;
    height: 1;
    margin-top: 1;
}

.section_divider {
    height: 1;
    color: #1e3448;
    margin: 1 0;
}

Input {
    margin-bottom: 0;
}

Select {
    margin-bottom: 0;
}

#preview_panel {
    width: 1fr;
    border: double #00d4ff;
    padding: 1 2;
    overflow-y: auto;
    background: #162033;
}

#preview_title {
    color: #00d4ff;
    text-style: bold;
    height: 1;
    margin-bottom: 1;
}

#preview_content {
    color: #cce8f0;
    text-style: italic;
}

/* Params sub-section */
#params_container {
    margin-top: 1;
}

#btn_add_param {
    margin-top: 1;
    width: auto;
}

/* Steps screen panels */
#steps_panel {
    width: 36;
    border: double #00d4ff;
    padding: 1;
    overflow-y: auto;
    background: #162033;
}

#editor_panel {
    width: 1fr;
    border: solid #1e3448;
    padding: 1 2;
    overflow-y: auto;
    background: #1a2a3a;
}

.steps_panel_buttons {
    height: 3;
    layout: horizontal;
    margin-top: 1;
}

/* Operators sub-section */
#operators_container {
    margin-top: 1;
    border: dashed #00d4ff;
    padding: 0 1;
    min-height: 3;
    background: #0d1b2a;
}

#btn_add_operator {
    margin-top: 1;
    width: auto;
}

/* Editor action buttons */
.editor_buttons {
    height: 3;
    layout: horizontal;
    margin-top: 1;
}

/* Save status */
#save_status {
    height: 3;
    padding: 1 2;
    margin-top: 1;
}

#save_status.saved-success {
    background: #0d2a1a;
    border: solid #00ff88;
    color: #00ff88;
    text-style: bold;
}

/* Shared button row */
#buttons {
    height: 4;
    layout: horizontal;
    align: center middle;
    padding: 0 2;
    background: #162033;
    border-top: solid #1e3448;
}

Button {
    margin: 0 1;
}
"""


# ── Screen 1: Workflow Metadata ────────────────────────────────────────────────

class MetadataScreen(Screen):
    BINDINGS = [Binding("q", "app.quit", "Quit")]

    def __init__(self) -> None:
        super().__init__()
        self._param_counter = 0

    def compose(self) -> ComposeResult:
        yield Header()
        yield StepBreadcrumb(1)
        yield Label("  Step 1 / 3 — Workflow Metadata", classes="screen_title")
        with Horizontal(id="outer"):
            with ScrollableContainer(id="form_panel"):
                yield Label("Workflow name  [bold red]*[/bold red]", classes="field_label")
                yield Input(placeholder="e.g. cmip6_prep", id="wf_name")
                yield Label("Description", classes="field_label")
                yield Input(placeholder="Short description (optional)", id="wf_desc")
                yield Label("Run directory", classes="field_label")
                yield Input(placeholder="./runs", id="wf_run_dir")
                yield Label("CDO threads", classes="field_label")
                yield Input(placeholder="1", id="wf_threads")
                yield Label("Output path (copy final output here after run)", classes="field_label")
                yield Input(placeholder="e.g. /data/results/out.nc", id="wf_output_path")
                yield Static("─" * 40, classes="section_divider")
                yield Label("Workflow-level input slots (comma-separated)", classes="field_label")
                yield Input(placeholder="e.g. data, reference", id="wf_inputs")
                yield Label("Workflow params", classes="section_title")
                yield Label("Default param values overridable via -p at runtime", classes="field_label")
                yield Vertical(id="params_container")
                yield Button("+ Add Param", id="btn_add_param")
            with Vertical(id="preview_panel"):
                yield Label("YAML Preview", id="preview_title")
                yield Static("", id="preview_content")
        with Horizontal(id="buttons"):
            yield Button("Next →", id="btn_next", variant="primary")
            yield Button("Quit", id="btn_quit", variant="error")
        yield Footer()

    def on_input_changed(self) -> None:
        self._update_preview()

    def _collect_params(self) -> list[tuple[str, str]]:
        result = []
        for row in self.query(ParamRow):
            k, v = row.get_kv()
            if k:
                result.append((k, v))
        return result

    def _collect(self) -> dict:
        return {
            **self.app._wf_data,
            "name": self.query_one("#wf_name", Input).value,
            "description": self.query_one("#wf_desc", Input).value,
            "run_dir": self.query_one("#wf_run_dir", Input).value,
            "cdo_threads": self.query_one("#wf_threads", Input).value,
            "declared_inputs": self.query_one("#wf_inputs", Input).value,
            "output_path": self.query_one("#wf_output_path", Input).value,
            "params": self._collect_params(),
        }

    def _update_preview(self) -> None:
        self.query_one("#preview_content", Static).update(_build_yaml(self._collect()))

    async def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "btn_add_param":
            container = self.query_one("#params_container", Vertical)
            row = ParamRow(self._param_counter)
            self._param_counter += 1
            await container.mount(row)
        elif event.button.id and event.button.id.startswith("pdel_"):
            row_id = int(event.button.id.split("_")[-1])
            self.query_one(f"#param_row_{row_id}").remove()
            self._update_preview()
        elif event.button.id == "btn_next":
            name = self.query_one("#wf_name", Input).value.strip()
            if not name:
                self.query_one("#wf_name", Input).focus()
                return
            self.app._wf_data.update(self._collect())
            await self.app.push_screen(StepsScreen())
        elif event.button.id == "btn_quit":
            self.app.exit()


# ── Screen 2: Steps Manager ────────────────────────────────────────────────────

class StepsScreen(Screen):
    BINDINGS = [Binding("q", "app.quit", "Quit")]

    STEP_TYPES = [("Python step", "python"), ("CDO step", "cdo")]

    def __init__(self) -> None:
        super().__init__()
        self._selected_idx: int | None = None  # None = new step
        self._op_counter = 0

    def compose(self) -> ComposeResult:
        yield Header()
        yield StepBreadcrumb(2)
        yield Label("  Step 2 / 3 — Steps Manager", classes="screen_title")
        with Horizontal(id="outer"):
            # ── Left: step list ─────────────────────────────────────────────
            with Vertical(id="steps_panel"):
                yield Label("[bold]Steps[/bold]", classes="section_title")
                yield ListView(id="steps_list")
                with Horizontal(classes="steps_panel_buttons"):
                    yield Button("↑", id="btn_move_up")
                    yield Button("↓", id="btn_move_dn")
                    yield Button("×", id="btn_del_step", variant="error")
                yield Button("+ New Step", id="btn_new_step", variant="primary")
            # ── Right: step editor ──────────────────────────────────────────
            with ScrollableContainer(id="editor_panel"):
                yield Label("[bold]Step Editor[/bold]", classes="section_title")
                yield Label("Type", classes="field_label")
                yield Select(self.STEP_TYPES, id="step_type", value="python", allow_blank=False)
                yield Label("Step ID  [bold red]*[/bold red]", classes="field_label")
                yield Input(placeholder="e.g. regrid", id="step_id")
                yield Label("Inputs  (key=slot, comma-separated)", classes="field_label")
                yield Input(placeholder="e.g. data=data, ref=reference", id="step_inputs")
                # Python section
                with Vertical(id="script_section"):
                    yield Label("Script path", classes="field_label")
                    yield Input(placeholder="e.g. scripts/process.py", id="step_script")
                # CDO operators section
                with Vertical(id="operators_section"):
                    yield Label(
                        "Operator chain  [#5a7a8a](CDO pipeline)[/#5a7a8a]",
                        classes="field_label",
                    )
                    yield Vertical(id="operators_container")
                    yield Button("+ Add Operator", id="btn_add_operator")
                yield Label("Output filename", classes="field_label")
                yield Input(placeholder="e.g. output.nc", id="step_output")
                yield Label("Keep intermediate output", classes="field_label")
                yield Select(
                    [("true", "true"), ("false", "false")],
                    id="step_keep",
                    value="true",
                    allow_blank=False,
                )
                with Horizontal(classes="editor_buttons"):
                    yield Button("Apply Step", id="btn_apply", variant="success")
                    yield Button("Cancel", id="btn_cancel_edit")
        with Horizontal(id="buttons"):
            yield Button("← Back", id="btn_back")
            yield Button("Next →", id="btn_next", variant="primary")
            yield Button("Quit", id="btn_quit", variant="error")
        yield Footer()

    def on_mount(self) -> None:
        self._refresh_step_list_sync()
        self._update_type_visibility()

    def _get_type_value(self) -> str:
        val = self.query_one("#step_type", Select).value
        return "python" if val is Select.NULL else str(val)

    def _update_type_visibility(self) -> None:
        is_cdo = self._get_type_value() == "cdo"
        self.query_one("#script_section").display = not is_cdo
        self.query_one("#operators_section").display = is_cdo

    def on_select_changed(self, event: Select.Changed) -> None:
        if event.select.id == "step_type":
            self._update_type_visibility()

    def _refresh_step_list_sync(self) -> None:
        """Sync helper that schedules an async list refresh."""
        self.call_later(self._async_refresh_step_list)

    async def _async_refresh_step_list(self) -> None:
        lv = self.query_one("#steps_list", ListView)
        await lv.clear()
        steps = self.app._wf_data.get("steps", [])
        items = [
            ListItem(Label(f"{s.get('id', '?')}  [{s.get('type', '?')}]"))
            for s in steps
        ]
        if items:
            await lv.mount(*items)

    def _load_step_to_editor(self, idx: int) -> None:
        steps = self.app._wf_data.get("steps", [])
        if idx < 0 or idx >= len(steps):
            return
        self._selected_idx = idx
        s = steps[idx]
        self.query_one("#step_id", Input).value = s.get("id", "")
        self.query_one("#step_inputs", Input).value = s.get("inputs", "")
        self.query_one("#step_script", Input).value = s.get("script", "")
        self.query_one("#step_output", Input).value = s.get("output", "")

        keep_val = s.get("keep", "true")
        self.query_one("#step_keep", Select).value = str(keep_val).lower() if keep_val else "true"

        type_val = s.get("type", "python")
        self.query_one("#step_type", Select).value = type_val
        self._update_type_visibility()

        # Reload operator rows
        self.call_later(self._async_load_operators, list(s.get("operators", [])))

    async def _async_load_operators(self, operators: list[str]) -> None:
        container = self.query_one("#operators_container", Vertical)
        for row in list(self.query(OperatorRow)):
            await row.remove()
        for op_str in operators:
            row = OperatorRow(op_str, self._op_counter)
            self._op_counter += 1
            await container.mount(row)

    def _clear_editor(self) -> None:
        self._selected_idx = None
        self.query_one("#step_id", Input).value = ""
        self.query_one("#step_inputs", Input).value = ""
        self.query_one("#step_script", Input).value = ""
        self.query_one("#step_output", Input).value = ""
        self.query_one("#step_type", Select).value = "python"
        self.query_one("#step_keep", Select).value = "true"
        self._update_type_visibility()
        self.call_later(self._async_load_operators, [])

    def _collect_operators(self) -> list[str]:
        return [row._op_str for row in self.query(OperatorRow)]

    def _collect_editor_step(self) -> dict:
        type_val = self._get_type_value()
        keep_val = self.query_one("#step_keep", Select).value
        return {
            "id": self.query_one("#step_id", Input).value.strip(),
            "type": type_val,
            "inputs": self.query_one("#step_inputs", Input).value.strip(),
            "script": self.query_one("#step_script", Input).value.strip(),
            "operators": self._collect_operators(),
            "output": self.query_one("#step_output", Input).value.strip(),
            "keep": "false" if (
                keep_val is not Select.NULL and str(keep_val) in ("false", "False")
            ) else "true",
        }

    def on_list_view_highlighted(self, event: ListView.Highlighted) -> None:
        if event.list_view.id == "steps_list":
            idx = event.list_view.index
            if idx is not None:
                self._load_step_to_editor(idx)

    async def on_button_pressed(self, event: Button.Pressed) -> None:
        bid = event.button.id

        if bid == "btn_new_step":
            self._clear_editor()

        elif bid == "btn_apply":
            step = self._collect_editor_step()
            if not step["id"]:
                self.query_one("#step_id", Input).focus()
                return
            steps = list(self.app._wf_data.get("steps", []))
            if self._selected_idx is None:
                steps.append(step)
                self._selected_idx = len(steps) - 1
            else:
                steps[self._selected_idx] = step
            self.app._wf_data["steps"] = steps
            await self._async_refresh_step_list()

        elif bid == "btn_cancel_edit":
            self._clear_editor()

        elif bid == "btn_add_operator":
            await self.app.push_screen(CdoPickerModal(), self._on_op_picked)

        elif bid and bid.startswith("odel_"):
            row_id = int(bid.split("_")[-1])
            self.query_one(f"#op_row_{row_id}").remove()

        elif bid == "btn_move_up":
            lv = self.query_one("#steps_list", ListView)
            idx = lv.index
            if idx is not None and idx > 0:
                steps = list(self.app._wf_data.get("steps", []))
                steps[idx - 1], steps[idx] = steps[idx], steps[idx - 1]
                self.app._wf_data["steps"] = steps
                await self._async_refresh_step_list()
                self._selected_idx = idx - 1

        elif bid == "btn_move_dn":
            lv = self.query_one("#steps_list", ListView)
            idx = lv.index
            steps = list(self.app._wf_data.get("steps", []))
            if idx is not None and idx < len(steps) - 1:
                steps[idx], steps[idx + 1] = steps[idx + 1], steps[idx]
                self.app._wf_data["steps"] = steps
                await self._async_refresh_step_list()
                self._selected_idx = idx + 1

        elif bid == "btn_del_step":
            lv = self.query_one("#steps_list", ListView)
            idx = lv.index
            steps = list(self.app._wf_data.get("steps", []))
            if idx is not None and 0 <= idx < len(steps):
                steps.pop(idx)
                self.app._wf_data["steps"] = steps
                self._clear_editor()
                await self._async_refresh_step_list()

        elif bid == "btn_back":
            await self.app.pop_screen()

        elif bid == "btn_next":
            steps = self.app._wf_data.get("steps", [])
            if not steps:
                return  # require at least one step
            await self.app.push_screen(SaveScreen())

        elif bid == "btn_quit":
            self.app.exit()

    def _on_op_picked(self, result: tuple[str, str] | None) -> None:
        if result is None:
            return
        op, args = result
        op_str = f"{op} {args}".strip()
        self.call_later(self._async_add_op_row, op_str)

    async def _async_add_op_row(self, op_str: str) -> None:
        container = self.query_one("#operators_container", Vertical)
        row = OperatorRow(op_str, self._op_counter)
        self._op_counter += 1
        await container.mount(row)


# ── Screen 3: Save ─────────────────────────────────────────────────────────────

class SaveScreen(Screen):
    BINDINGS = [Binding("q", "app.quit", "Quit")]

    def compose(self) -> ComposeResult:
        yield Header()
        yield StepBreadcrumb(3)
        yield Label("  Step 3 / 3 — Save Workflow", classes="screen_title")
        with Horizontal(id="outer"):
            with Vertical(id="form_panel"):
                yield Label("Output file path", classes="field_label")
                yield Input(value=str(self.app._output_path), id="output_path")
                yield Label("", id="save_status")
            with Vertical(id="preview_panel"):
                yield Label("Final YAML", id="preview_title")
                yield Static(_build_yaml(self.app._wf_data), id="preview_content")
        with Horizontal(id="buttons"):
            yield Button("← Back", id="btn_back")
            yield Button("Save", id="btn_save", variant="success")
            yield Button("Quit", id="btn_quit", variant="error")
        yield Footer()

    async def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "btn_back":
            await self.app.pop_screen()
        elif event.button.id == "btn_save":
            path_str = self.query_one("#output_path", Input).value.strip()
            if not path_str:
                return
            out_path = Path(path_str)
            try:
                out_path.parent.mkdir(parents=True, exist_ok=True)
                out_path.write_text(_build_yaml(self.app._wf_data))
                status_lbl = self.query_one("#save_status", Label)
                status_lbl.update(
                    f"[bold]✓ Saved successfully[/bold]\n[#5a7a8a]{out_path}[/#5a7a8a]"
                )
                status_lbl.add_class("saved-success")
                self.set_timer(2.0, self.app.exit)
            except Exception as exc:
                self.query_one("#save_status", Label).update(f"[#ff3860]Error: {exc}[/#ff3860]")
        elif event.button.id == "btn_quit":
            self.app.exit()


# ── App ────────────────────────────────────────────────────────────────────────

class WorkflowCreationWizard(App):
    """Interactive three-screen wizard for creating workflow YAML files."""

    TITLE = "cdo-flow create"
    CSS = CSS

    def __init__(self, output_path: Path = Path("workflow.yml")) -> None:
        super().__init__()
        self._output_path = output_path
        self._wf_data: dict = {
            "steps": [],
            "params": [],
        }

    async def on_mount(self) -> None:
        await self.push_screen(MetadataScreen())
