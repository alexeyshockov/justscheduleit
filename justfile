#!/usr/bin/env -S just --justfile
# ^ A shebang isn't required, but allows a justfile to be executed
#   like a script, with `./justfile test`, for example.

default: sync-deps install-editable
move: upgrade-deps sync-deps install-editable

upgrade-pip:
    pip install --upgrade pip wheel hatch pip-tools \
      --disable-pip-version-check

install-editable:
    pip install -e .



package-version: upgrade-pip
    hatch version

package-build: upgrade-pip
    hatch build



upgrade-deps: upgrade-pip
    pip-compile -v --upgrade --resolver=backtracking --strip-extras --allow-unsafe \
      --all-extras \
      -o requirements.txt \
      pyproject.toml

sync-deps: upgrade-pip
    pip-sync requirements.txt



check-style:
    ruff check justscheduleit examples
check-types:
    pyright --pythonpath $(which python) justscheduleit examples
check: check-style check-types


format:
    ruff check --fix justscheduleit examples
    ruff format justscheduleit examples
