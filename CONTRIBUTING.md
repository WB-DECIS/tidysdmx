# Contributing

Contributions are welcome, and they are greatly appreciated! Every little bit
helps, and credit will always be given.

## Types of Contributions

### Report Bugs

If you are reporting a bug, please include:

* Your operating system name and version.
* Any details about your local setup that might be helpful in troubleshooting.
* Detailed steps to reproduce the bug.

### Fix Bugs

Look through the GitHub issues for bugs. Anything tagged with "bug" and "help
wanted" is open to whoever wants to implement it.

### Implement Features

Look through the GitHub issues for features. Anything tagged with "enhancement"
and "help wanted" is open to whoever wants to implement it.

### Write Documentation

You can never have enough documentation! Please feel free to contribute to any
part of the documentation, such as the official docs, docstrings, or even
on the web in blog posts, articles, and such.

### Submit Feedback

If you are proposing a feature:

* Explain in detail how it would work.
* Keep the scope as narrow as possible, to make it easier to implement.
* Remember that this is a volunteer-driven project, and that contributions
  are welcome :)

## Get Started!

Ready to contribute? Here's how to set up `tidysdmx` for local development.

1. Download a copy of `tidysdmx` locally or clone the repo.

2. Create and activate a conda environment for `tidysdmx`:

    - Using conda
        ```console
        $ conda create -n tidysdmx python=3.11.9
        $ conda activate tidysdmx
        ```
    - Using `venv`
        ```console
        $ python -m venv tidysdmx
        $ ./tidysdmx/Scripts/activate
        ```

3. Install `tidysdmx` using `poetry`:

    ```console
    $ poetry install
    ```

    Then install the pre-commit hooks. The pre-push hook runs the unit tests, so
    install it explicitly with `--hook-type pre-push` in addition to the default
    commit-stage hooks:

    ```console
    $ pre-commit install
    $ pre-commit install --hook-type pre-push
    ```

4. Use `git` (or similar) to create a branch for local development and make your changes:

    ```console
    $ git checkout -b name-of-your-bugfix-or-feature
    ```

5. When you're done making changes, run the same checks CI runs:

    ```console
    $ poetry run ruff check .            # lint
    $ poetry run ruff format --check .   # formatting
    $ poetry run pytest -m "not integration" --cov   # unit lane + coverage
    ```

    The unit lane is hermetic (no network) and is what CI gates on.

6. Commit your changes and open a pull request.

## Running the test lanes

- **Unit lane (default, hermetic):** `poetry run pytest -m "not integration"`.
  This is what CI runs on every push. Cassette-backed tests (FMR schemas and
  structure maps replayed from committed pickles) run here — they open no
  network connection.
- **Integration lane (opt-in, needs network):** `poetry run pytest -m integration`.
  These hit a live FMR registry. Point them at a different registry by setting
  `TIDYSDMX_TEST_FMR_URL` (defaults to the World Bank QA registry).

## Test cassettes

FMR responses are recorded as pickled pysdmx objects under
`tests/fixtures/cassettes/` so the unit lane runs offline. Regenerate them when
you bump `pysdmx` or when the upstream artefacts change:

```console
$ poetry run python -m tests.fixtures.fxtr_schemas   # schema cassettes
$ poetry run python -m tests.fixtures.fxtr_mapping   # structure-map cassette
```

Commit the regenerated `.pkl` files. Missing cassettes fail loudly under CI
(the loader refuses to silently live-fetch), and `tests/test_cassettes.py`
asserts each one still unpickles to the expected pysdmx type.

## Pull Request Guidelines

Before you submit a pull request, check that it meets these guidelines:

1. The pull request should include additional tests if appropriate.
2. If the pull request adds functionality, the docs should be updated.
3. The pull request should work for all currently supported operating systems and versions of Python.
4. Commit messages should follow [Conventional Commits](https://www.conventionalcommits.org/)
   (`feat:`, `fix:`, `docs:`, …) — they drive automated versioning and the changelog.

## Releasing

Releases to PyPI are automated. See [RELEASING.md](RELEASING.md).

## Code of Conduct

Please note that the `tidysdmx` project is released with a
Code of Conduct. By contributing to this project you agree to abide by its terms.
