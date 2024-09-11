#!/usr/bin/env -S just --justfile
# ^ A shebang isn't required, but allows a justfile to be executed
#   like a script, with `./justfile test`, for example.

default: sync-deps install-editable
move: upgrade-deps sync-deps install-editable

upgrade-pip:
    pip install --upgrade --disable-pip-version-check pip wheel hatch pip-tools

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



alias fix-style := format
format:
    ruff check --fix justscheduleit examples
    ruff format justscheduleit examples



# $> just why starlette
# starlette==0.27.0
# ├── fastapi==0.101.1 [requires: starlette>=0.27.0,<0.28.0]
# │   └── fastapi-rfc7807==0.5.0 [requires: fastapi]
# └── fastapi-rfc7807==0.5.0 [requires: starlette]
why package:
    pipdeptree -r -p {{package}}
