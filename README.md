# Usage
The project uses [https://github.com/astral-sh/uv](https://github.com/astral-sh/uv) as a project manager, once installed run:
```
$ uv sync
```
to install all the required dependencies.

To run the scripts just run:
```shell
$ uv run scriptname.py
```
The scripts not in the **data** folder are [marimo notebooks](https://marimo.io/) and once marimo is installed run:
```shell
$ uv run marimo edit scriptname.py
```
to open the notebook in edit mode

