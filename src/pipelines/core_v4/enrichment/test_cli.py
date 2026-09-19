import argparse

import pytest

from pipelines.core_v4.enrichment.cli import add_common_args, resolve


def parse(*argv, **kw):
    parser = argparse.ArgumentParser()
    add_common_args(parser, **kw)
    return resolve(parser.parse_args(["--variant", "limit", *argv]))


def test_tier_default_and_values():
    assert parse().tier is None
    assert parse("--entity", "work", "--tier", "all").tier is None
    assert parse("--entity", "work", "--tier", "0").tier == 0
    assert parse("--entity", "work", "--tier", "1").tier == 1


def test_tier_needs_entity_work():
    for argv in (["--tier", "0"], ["--entity", "project", "--tier", "1"]):
        with pytest.raises(SystemExit, match="--entity work"):
            parse(*argv)


def test_no_tier_flag_without_entities():
    parser = argparse.ArgumentParser()
    add_common_args(parser, entities=False, text=False)
    assert "--tier" not in parser.format_help()
