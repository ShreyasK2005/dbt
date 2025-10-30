# dbt_quickstart

## Developer Environment Setup

### dbt Core Installation
The existing requirements.txt file in the repository contains packages needed such as dbt-core and dbt-snowflake.  These will be installed in the next section.

```
dbt-core
dbt-snowflake
```

dbt-snowflake requires Python 3.8+

### Python Virtual Environment

Development environment should be using Python 3.8+ (repository was tested with 3.13)

Install a python virtual environment in the project folder
```
python -m venv .venv
```

Activate the virtual environment
```
source .venv/bin/activate
```

Install dependencies using requirements.txt file containing all required python packages for dbt installation
```
pip install -r requirements.txt
```


Deactivate the virtual environment when necessary
```
deactivate
```


### Set up dbt profile

Copy sample_profile.yml to ~/.dbt/profiles.yml
Replace vales for <snowflake_account>, <developer_name>, <user_id>, <private_key_path> and <private_key_passphrase>

## Execute dbt commands

Compile code
```
dbt compile
```

Build all models
```
dbt build
```

Build individual models
```
dbt build --select <model_name>
```

## dbt Parse Error

A bug exists that causes a parse error when using snapshots in dbt.  When this occurs, an error message similar to the one below will appear:

```
16:58:32  Unable to do partial parsing because an error occurred. Switching to full reparse.
16:58:32  Encountered an error:
Parsing Error
  at path ['raw_code']: None is not of type 'string'
```

This is an open issue with dbt (https://github.com/dbt-labs/dbt-core/issues/11164).

The workaround is to disable partial parsing:

```
dbt parse --no-partial-parse
```

## Container deployments

Included dockerfile has configuration required for building a dbt container

container_profile/profiles.yml file configured with envrionment variable references to be passed into the container at runtime

Database credentials should be managed in a key vault


## Resources:
- Learn more about dbt [in the docs](https://docs.getdbt.com/docs/introduction)
- Check out [Discourse](https://discourse.getdbt.com/) for commonly asked questions and answers
- Join the [chat](https://community.getdbt.com/) on Slack for live discussions and support
- Find [dbt events](https://events.getdbt.com) near you
- Check out [the blog](https://blog.getdbt.com/) for the latest news on dbt's development and best practices
