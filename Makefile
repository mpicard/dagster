# Makefile oddities:
# - Commands must start with literal tab characters (\t), not spaces.
# - Multi-command rules (like `black` below) by default terminate as soon as a command has a non-0
#   exit status. Prefix the command with "-" to instruct make to continue to the next command
#   regardless of the preceding command's exit status.

# NOTE: See pyproject.toml [tool.black] for majority of black config. Only include/exclude options
# and format targets should be specified here. Note there are separate pyproject.toml for the root
# and examples/docs_snippets.
#
# NOTE: Use `extend-exclude` instead of `exclude`. If `exclude` is provided, it stops black from
# reading gitignore. `extend-exclude` is layered on top of gitignore. See:
#   https://black.readthedocs.io/en/stable/usage_and_configuration/file_collection_and_discovery.html#gitignore
black:
	black --fast \
    --extend-exclude="examples/docs_snippets|snapshots" \
    examples integration_tests helm python_modules .buildkite
	black --fast \
    examples/docs_snippets

check_black:
	black --check --fast \
    --extend-exclude="examples/docs_snippets|snapshots" \
    examples integration_tests helm python_modules .buildkite
	black --check --fast \
    examples/docs_snippets

# The `--exit-zero` for the first command means `make` will run the second
# (docs_snippets-specific) command even if `ruff` has unfixable errors from the
# first command.
ruff:
	ruff --fix \
		--exit-zero \
		--extend-exclude="*/__generated__/*,*/dagster_airflow/vendor/*,*/snapshots/*,examples/docs_snippets/*.py" \
		--config=pyproject.toml \
		`git ls-files '*.py'`
	ruff --fix \
		--line-length=88 \
		--config=pyproject.toml \
		`git ls-files 'examples/docs_snippets/*.py'`

check_ruff:
	ruff \
		--extend-exclude="*/__generated__/*,*/dagster_airflow/vendor/*,*/snapshots/*,examples/docs_snippets/*.py" \
		--config=pyproject.toml \
		`git ls-files '*.py'`
	ruff \
		--line-length=88 \
		--config=pyproject.toml \
		`git ls-files 'examples/docs_snippets/*.py'`

yamllint:
	yamllint -c .yamllint.yaml --strict \
    `git ls-files 'helm/*.yml' 'helm/*.yaml' ':!:helm/**/templates/*.yml' ':!:helm/**/templates/*.yaml'`

install_dev_python_modules:
	python scripts/install_dev_python_modules.py -qqq

install_dev_python_modules_verbose:
	python scripts/install_dev_python_modules.py

graphql:
	cd js_modules/dagit/; make generate-graphql; make generate-perms

sanity_check:
#NOTE:  fails on nonPOSIX-compliant shells (e.g. CMD, powershell)
	@echo Checking for prod installs - if any are listed below reinstall with 'pip -e'
	@! (pip list --exclude-editable | grep -e dagster -e dagit)

rebuild_dagit: sanity_check
	cd js_modules/dagit/; yarn install && yarn build

rebuild_dagit_with_profiling: sanity_check
	cd js_modules/dagit/; yarn install && yarn build-with-profiling

dev_install: install_dev_python_modules_verbose rebuild_dagit

dev_install_quiet: install_dev_python_modules rebuild_dagit

graphql_tests:
	pytest python_modules/dagster-graphql/dagster_graphql_tests/graphql/ -s -vv

check_manifest:
	check-manifest python_modules/dagster
	check-manifest python_modules/dagit
	check-manifest python_modules/dagster-graphql
	ls python_modules/libraries | xargs -n 1 -Ipkg check-manifest python_modules/libraries/pkg
