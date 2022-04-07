# dagstermvp
A deployable, configurable, testable Minimum Viable Product for ML using Dagster

Clone this repo:

```git clone https://github.com/bakerwho/dagster-mvp```

Run this code:

1. Edit the lines in `.example_envrc` and `dagster-exec/workspace.yaml` to point to the correct paths
2. Run `source .example_envrc` to set required ENV variables, including `$DAGSTER_HOME`
3. Run using CLI:
    `dagster job execute clean_string_job`
4. Run the Dagit UI:
```dagit
```
