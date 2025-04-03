# FastAPI Service Template

## Requirements

- Python 3.10+
- [Poetry](https://python-poetry.org/)

## Installation

### Clone the project

```shell
git clone git@github.com:ducminh-phan/fastapi-service-template.git
cd fastapi-service-template
```

### Set up virtual environment

```shell
pyenv local 3.10.9
pyenv exec python3 -m venv venv
source ./venv/bin/activate
```

### Install dependencies

```shell
poetry install
```

### Install `pre-commit` hooks

- Install `pre-commit`: https://pre-commit.com/ (should be installed globally)
- Install `pre-commit` hooks:

  ```shell
  make install-git-hooks
  ```

## Running

Inside the virtual environment, run

```shell
make run
```

### Run tests

Inside the virtual environment, run

```shell
make test
```
