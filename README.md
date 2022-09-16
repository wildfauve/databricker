# Databricker

Databricker is a set of CLI commands which enables deployment of Python applications to a Databricks cluster.  It supports 3 modes of deployment, a job, a library and a cluster library.

Databricker only supports a few specific scenarios.  Its not designed as a general purpose Databricks CI/CD tool.  It uses the version 2.0 APIs, relies on the databricks-cli, and requires that the python app is built using poetry. 

## Setting up You App to Support Databricker

Ensure databricks-cli is installed in dev:

```shell
poetry install databrick-cli --dev
```

Install databricker

```shell
poetry install git+https://github.com/wildfauve/jobsworth.git#main --dev
```


Create the `_infra` folder at the project root.  Its a good idea to exclude this folder in the `pyproject.toml`.  In that folder create a job, library, or cluster library toml file, usually this will be called `infra.toml`

Add a databricker CLI wrapper to the `_infra` folder.  Typically this will be called `cli`.  Add the following code.  Both `dist` and `pyproject` default to the params shown here.

```python
from databricker import infra

infra.configurator()(infra_config_file="_infra/infra.toml", dist="dist", pyproject="pyproject.toml")

def infra_cli():
    infra.init_cli()
```

Finally add the cli wrapper as a script to the `pyproject.toml` file.

```toml
[tool.poetry.scripts]
infra = "_infra.cli:infra_cli"
```


## Building a Infrastructure TOML File

The build and deploy pipeline relies on the `infra.toml` file containing specific tables which describe the type of pipeline to run.  Current the pipelines supported are:
+ Spark jobs.  These will have a table named `job`.
+ Libraries.  These will have a table named `library`.
+ Cluster Libraries. These will have a table named `cluster-library`

### Job

An example TOML file for a job looks like this...

```toml
[job]
name = "job"
task_key = "job"
package_name = "job_name"
entry_point = "job_main"
parameters = ["--all-batches"]
schedule.cron = "0 0 * * * ?"
schedule.pause_status = "UNPAUSED"
schedule.tz = "UTC"

[cluster]
url = "https://example.databricks.com"
type = "newCluster"
spark_version = "11.1.x-scala2.12"
num_workers = 1
node_type_id = "Standard_DS3_v2"

[emailNotifications]
on_failure = "admin@example.com"

[artefacts]
root = "dbfs:/artifacts/job/job/dist"
maven_artefacts = ["java-artefact-1"]
whl_artefacts = ["python-wheel-1"]
```

### Libraries

Libraries are common code that are copied to DBFS, but not installed on the cluster.  Their deployment location are added to job artefacts.

Example of a library toml:

```toml
[library]
name = "lib-name"

[cluster]
url = "https://example.databricks.com"

[artefacts]
root = "dbfs:/artifacts/common/python"
```


### Cluster Libraries

Cluster libraries are like [libraries](#libraries) but are also installed on a cluster.  This allows them to be used with notebooks using simple import commands.

Example of a cluster library toml:

```toml
[cluster_library]
name = "a_lib"

[cluster]
url = "https://example.databricks.com"
cluster_id = "spark_cluster_1"

[artefacts]
root = "dbfs:/artifacts/common/python"
```


## Running Databricker Commands

To get help:

```shell
poetry run infra
```

### Building and Deploying

Depending on the type of app, build and deploy performs the following:
+ Runs `poetry version`.  This can be disabled with the `--no-version` option
+ Runs `poetry build`.
+ Copies the built artefact to DBFS using the `databricks-cli`
+ For a job, the job configuration is updated with the new artefact location.
+ For a cluster library, the arefact is installed on the cluster.

```shell
poetry run infra build-deploy
```

To disable running the `poetry version` command:

```shell
poetry run infra build-deploy --no-version
```

