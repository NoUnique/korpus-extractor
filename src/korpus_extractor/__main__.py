from typing import Any, Callable, Literal
from typing_extensions import Annotated

from functools import wraps

import typer
from merge_args import merge_args
from rich.console import Console

from korpus_extractor import version

from .aihub_extractor import AIHubExtractor
from .modu_extractor import ModuExtractor

app = typer.Typer(
    name="korpus-extractor",
    help="Extractor for obtaining sentences and paragraphs from compressed Korean corpora files.",
    add_completion=True,
    context_settings={"help_option_names": ["-h", "--help"]},
)
console = Console()


# wrapper pattern for common arguments (https://github.com/tiangolo/typer/issues/296)
def cmd_extractor(
    func: Callable,
) -> Any:
    @merge_args(func)
    @wraps(func)
    def wrapper(
        ctx: typer.Context,
        input_path: str = typer.Option(..., "-i", "--input", metavar="PATH", help="Input file or directory."),
        output_path: str = typer.Option(..., "-o", "--output", metavar="PATH", help="Output file path."),
        extraction_type: Literal["sentence", "document"] = typer.Option(
            "sentence",
            "-t",
            "--type",
            metavar="TYPE",
            help="Type of extraction (sentence or document).",
        ),
        **kwargs,
    ):
        return func(ctx=ctx, **kwargs)

    return wrapper


def version_callback(print_version: bool) -> None:
    """Print the version of the package."""
    if print_version:
        console.print(f"[bold blue]korpus-extractor[/] version: [bold red]{version}[/]")
        raise typer.Exit()


@app.callback(no_args_is_help=True)
def callback(
    ctx: typer.Context,
    print_version: bool = typer.Option(
        None,
        "-v",
        "--version",
        callback=version_callback,
        is_eager=True,
        help="Prints the version of the korpus-extractor package.",
    ),
) -> None:
    return


@app.command(no_args_is_help=True)
@cmd_extractor
def modu(ctx: typer.Context):
    """Modu Corpus Extractor"""
    kwargs = ctx.params
    extractor = ModuExtractor()
    extractor.extract(kwargs.pop("input_path"), kwargs.pop("output_path"), **kwargs)


@app.command(no_args_is_help=True)
@cmd_extractor
def aihub(ctx: typer.Context) -> None:
    """AI Hub Corpus Extractor"""
    kwargs = ctx.params
    extractor = AIHubExtractor()
    extractor.extract(kwargs.pop("input_path"), kwargs.pop("output_path"), **kwargs)


if __name__ == "__main__":
    app()
