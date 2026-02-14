# dbt-pal

dbt-pal is **P**ython **A**dapter **L**ayer

A dbt adapter for running Python models without Dataproc or BigQuery DataFrames.

- SQL models work the same as dbt-bigquery
- Python models are executed in the process running dbt, and the results are written to BigQuery
- The only supported data platform is BigQuery

Inspired by [dbt-fal](https://github.com/fal-ai/dbt-fal), but this is an unrelated project with no guaranteed compatibility.

## Usage

### Installation

```
pip install dbt-pal
```

### Prerequisites

- Python >= 3.11
- dbt-core >= 1.11.0
- dbt-bigquery >= 1.11.0
- Authentication to BigQuery must be configured (e.g. `gcloud auth application-default login`)

### profiles.yml Configuration

Create a `target` with `type: pal` and specify the target name of the actual BigQuery `target` in `db_profile` field.

```yaml
my_project:
  target: pal
  outputs:
    pal:
      type: pal
      db_profile: bq
    bq:
      type: bigquery
      method: oauth
      project: my-project
      dataset: my_dataset
      location: asia-northeast1
```

## Limitations

- Only table materialization is supported
- Python models are executed in the process running dbt, so the data size that can be handled depends on the memory of that process

## License

Apache License 2.0.  
This project was created by modifying code from [dbt-fal](https://github.com/fal-ai/dbt-fal).
