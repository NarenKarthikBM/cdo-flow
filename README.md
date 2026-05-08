# cdo-flow

Workflow orchestration layer for CDO-based climate analysis, built on top of [python-cdo-wrapper](https://pypi.org/project/python-cdo-wrapper/).

## Install

```bash
pip install cdo-flow
```

Requires [CDO](https://mpimet.mpg.de/cdo) to be installed for steps that execute CDO operators.

## Quick start

```python
from cdo_flow import Workflow, python_step, cdo_step
from python_cdo_wrapper.query import CDOQueryTemplate

# Pure Python step
@python_step
def write_data(ctx):
    ctx.output("result.txt").write_text("hello")

# CDO step via decorator
@cdo_step
def regrid(ctx, cdo):
    cdo.query(ctx.inputs["data"]).remap_bil("r360x180").to_file(ctx.output("regridded.nc"))

# Inline CDO chain
wf = Workflow(name="demo", run_dir="./runs")
wf.add_step("write", write_data)
wf.add_step(
    "select_tas",
    chain=CDOQueryTemplate().select_var("tas").year_mean(),
    inputs={"data": "/path/to/input.nc"},
    output=["tas_annual.nc"],
)
result = wf.run()
```

## YAML workflows

```yaml
name: my_workflow
steps:
  - id: select
    type: cdo
    inputs:
      data: /path/to/input.nc
    operator_chain:
      - op: selname
        args: [tas]
      - op: yearmean
    output: tas_annual.nc
```

```bash
cdo-flow validate my_workflow.yml
cdo-flow run my_workflow.yml --dry-run
cdo-flow run my_workflow.yml
```

## License

MIT
