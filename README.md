# dagster-mvp
A deployable, configurable, testable Minimum Viable Product for ML using [Dagster](https://dagster.io/).

Dagster is a tool for building and orchestrating data pipelines. It works well in production too. While learning this tool, I found myself hunting for an simple example of a runnable Dagster pipeline. I couldn't find one, so I built it myself.

This simple pipeline has 3 steps, each of which is written as a Dagster `op`:
1. select 1 of 2 sentences from a dict using a key
2. apply upper- or lower-case ‘normalization’
3. strip the sentence of all punctuation

We build the pipeline as a Dagster `graph` object that chains these 3 `op`s. We then configure the `graph` into a runnable `job`, and run the `job`. We also build a `scheduler` that can run this job every minute.

### Clone this repo:

```
git clone https://github.com/bakerwho/dagster-mvp
```

### Run this code:

1. Edit the lines in `.example_envrc` and `dagster-exec/workspace.yaml` to point to the correct paths
2. Run `source .example_envrc` to set required ENV variables, including `$DAGSTER_HOME`
3. Run using CLI:
    `dagster job execute -f pipeline_1.py -a clean_string_job`
4. Run the Dagit UI:

```
dagit
```

You can now run the job `clean_string_job` from the UI. You can even edit the run configuration in-browser.
