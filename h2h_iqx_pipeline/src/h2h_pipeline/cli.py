import argparse
from pathlib import Path

from .config_loader import load_config
from .pipeline import run_pipeline


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="H2H → IQX bulk import pipeline (file-based prototype)"
    )

    subparsers = parser.add_subparsers(dest="command", required=True)

    run_parser = subparsers.add_parser("run", help="Run the pipeline for a month")
    run_parser.add_argument(
        "--month",
        required=True,
        help="Target month in YYYY-MM format (e.g. 2025-12)",
    )
    run_parser.add_argument(
        "--input-root",
        required=True,
        type=Path,
        help="Root directory containing monthly Vet Talents folders",
    )
    run_parser.add_argument(
        "--config",
        required=True,
        type=Path,
        help="Path to YAML configuration file",
    )

    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    config = load_config(args.config)

    run_pipeline(
        month=args.month,
        input_root=args.input_root,
        config=config,
    )


if __name__ == "__main__":
    main()
