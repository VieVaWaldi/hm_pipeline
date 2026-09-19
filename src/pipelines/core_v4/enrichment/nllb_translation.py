"""
NLLB glue: reads project/work text fields from the core_v4 staging duckdb (read-only), detects each
field's language (fastText LID-218, the source `language` label is ignored), translates the
non-English ones to English and writes two side outputs (see README.md in this folder):

    nllb/<entity>/       id, field, text_en, src_lang      translated fields only
    nllb/seen/<entity>/  id, field, src_lang, translated   every processed field (also the resume key)

Downstream text enrichments read COALESCE(text_en, original) through text_sources.py, which refuses
to run before both outputs carry a `_SUCCESS`. Phase 3 assemble applies "overwrite the original text".

A field is translated when its LID label is not eng_Latn, P(label) >= --min-prob, the text is at
least 20 characters, and NLLB supports the language. Long texts are truncated (--max-chars).

Resume: rows whose id already appears in nllb/seen are skipped. Parallel: --shard I/N.
Needs a GPU node for real runs (compute nodes have no internet: run
`python -m enrichment.nllb_translator.download` on the login node first).

Usage:
    uv run python -m pipelines.core_v4.enrichment.nllb_translation --entity project
    uv run python -m pipelines.core_v4.enrichment.nllb_translation --entity work --shard 2/8
    uv run python -m pipelines.core_v4.enrichment.nllb_translation --test 50      # dry run, no writes
"""

import argparse
import logging
import time
from typing import Dict, List, Optional, Sequence, Tuple

import duckdb
import pyarrow as pa

from common.log.logger import setup_logging
from enrichment.nllb_translator.language_id import DEFAULT_MIN_PROB, LanguageIdentifier, should_translate
from enrichment.nllb_translator.translator import DEFAULT_BATCH_TOKENS, DEFAULT_BEAM_SIZE, NllbTranslator
from pipelines.core_v4.enrichment.cli import add_common_args, resolve
from pipelines.core_v4.enrichment.side_outputs import SCHEMAS, Shard, SideOutput
from pipelines.core_v4.enrichment.text_sources import NLLB_FIELDS, Entity, field_sql, open_staging

CHUNK_ROWS = 20_000  # rows per streamed batch: one part file per output per batch

Row = Tuple[int, str, Optional[str]]  # (id, field, text)


def field_batches(
    con: duckdb.DuckDBPyConnection,
    entity: Entity,
    fields: Sequence[str],
    *,
    batch_size: int = CHUNK_ROWS,
    shard: Shard = Shard(),
    exclude_ids_sql: Optional[str] = None,
    limit: Optional[int] = None,
):
    """Streams (id, <one column per field>) Arrow batches for rows with at least one non-empty field.
    Same reading rules as text_sources.text_batches (one cursor, no OFFSET, resume by anti-join), but
    the fields stay separate: NLLB translates them one by one."""
    exprs = {f: field_sql(entity, f) for f in fields}
    where = [" OR ".join(f"length(trim({e})) > 0" for e in exprs.values())]
    if shard.sql("e.id"):
        where.append(shard.sql("e.id"))
    if exclude_ids_sql:
        where.append(f"NOT EXISTS (SELECT 1 FROM ({exclude_ids_sql}) d WHERE d.id = e.id)")
    select = ", ".join(f"{e} AS {f}" for f, e in exprs.items())
    sql = f"SELECT e.id, {select} FROM {entity} e WHERE {' AND '.join('(' + w + ')' for w in where)}"
    if limit is not None:
        sql += f" LIMIT {int(limit)}"
    cursor = con.cursor()
    try:
        yield from cursor.execute(sql).to_arrow_reader(batch_size)
    finally:
        cursor.close()


# NLLB occasionally appends invented sentences to short inputs ("... This is the first time I've seen
# this."): an output far longer than its source is dropped and the field stays untranslated.
MAX_LENGTH_RATIO = 3.0
MAX_LENGTH_SLACK = 40


def _hallucinated(source: str, translation: str) -> bool:
    # Compare on the truncated source the translator saw would need its limit; the raw length only
    # makes the check more lenient for long texts, which is the safe direction.
    return len(translation) > MAX_LENGTH_RATIO * len(source) + MAX_LENGTH_SLACK


def process_rows(
    rows: Sequence[Row],
    lid,
    translator,
    min_prob: float = DEFAULT_MIN_PROB,
) -> Tuple[List[dict], List[dict]]:
    """LID + translation of one batch of (id, field, text). Returns (nllb rows, seen rows).

    `lid` needs .predict(text) -> (label|None, prob); `translator` needs .supported_languages and
    .translate(texts, src_langs) -> list[str]. Both are injected so this is testable without models."""
    nllb: List[dict] = []
    seen: List[dict] = []
    todo: List[Tuple[int, str, str, str]] = []  # (id, field, text, lang)
    for id_, field, text in rows:
        if text is None or not text.strip():
            continue  # nothing to process, and not "seen" either: an empty field has no row
        label, prob = lid.predict(text)
        if should_translate(label, prob, min_prob) and label in translator.supported_languages:
            todo.append((id_, field, text, label))
        else:
            seen.append({"id": id_, "field": field, "src_lang": label, "translated": False})
    if todo:
        outs = translator.translate([t[2] for t in todo], [t[3] for t in todo])
        for (id_, field, text, lang), out in zip(todo, outs):
            ok = bool(out.strip()) and not _hallucinated(text, out)
            seen.append({"id": id_, "field": field, "src_lang": lang, "translated": ok})
            if ok:
                nllb.append({"id": id_, "field": field, "text_en": out, "src_lang": lang})
    return nllb, seen


def _batch_to_rows(batch: pa.RecordBatch, fields: Sequence[str]) -> List[Row]:
    ids = batch.column("id").to_pylist()
    columns = {f: batch.column(f).to_pylist() for f in fields}
    return [(id_, f, columns[f][i]) for i, id_ in enumerate(ids) for f in fields]


def run_entity(
    con: duckdb.DuckDBPyConnection,
    entity: Entity,
    fields: Sequence[str],
    lid,
    translator,
    enrichment_dir: str,
    *,
    shard: Shard = Shard(),
    limit: Optional[int] = None,
    dry_run: bool = False,
    min_prob: float = DEFAULT_MIN_PROB,
    chunk_rows: int = CHUNK_ROWS,
) -> Dict[str, int]:
    """Translates every not-yet-seen row of `entity`. `_SUCCESS` is only written for a full,
    non-dry, unlimited run. Returns counters."""
    nllb_out = SideOutput(enrichment_dir, "nllb", entity, shard=shard)
    seen_out = SideOutput(enrichment_dir, "nllb/seen", entity, shard=shard)
    if not dry_run:
        nllb_out.begin()
        seen_out.begin()
    counters = {"rows": 0, "fields_seen": 0, "translated": 0}
    start = time.perf_counter()
    for batch in field_batches(
        con, entity, fields, batch_size=chunk_rows, shard=shard, exclude_ids_sql=seen_out.done_ids_sql(), limit=limit
    ):
        nllb_rows, seen_rows = process_rows(_batch_to_rows(batch, fields), lid, translator, min_prob)
        counters["rows"] += batch.num_rows
        counters["fields_seen"] += len(seen_rows)
        counters["translated"] += sum(1 for r in seen_rows if r["translated"])
        if dry_run:
            for r in nllb_rows[:5]:
                logging.info(f"[dry run] {r['src_lang']} {r['field']}: {r['text_en'][:120]!r}")
        else:
            # nllb first, seen last: seen is the resume key, so a kill in between just redoes the batch
            nllb_out.write(pa.Table.from_pylist(nllb_rows, schema=SCHEMAS["nllb"]))
            seen_out.write(pa.Table.from_pylist(seen_rows, schema=SCHEMAS["nllb/seen"]))
        elapsed = time.perf_counter() - start
        logging.info(
            f"[{entity}] {counters['rows']:,} rows, {counters['translated']:,} fields translated, "
            f"{counters['rows'] / elapsed:.0f} rows/s"
        )
    if not dry_run and limit is None:
        nllb_out.finish()
        seen_out.finish()
    return counters


def main(argv: Optional[list] = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    add_common_args(parser, text=False)
    parser.add_argument("--model", default="1.3B", choices=["600M", "1.3B"])
    parser.add_argument("--backend", default="ctranslate2", choices=["ctranslate2", "transformers"])
    parser.add_argument("--quantization", default="float16", help="CTranslate2 weight type of the converted model")
    parser.add_argument("--device", default="cuda", choices=["cuda", "cpu"])
    parser.add_argument("--min-prob", type=float, default=DEFAULT_MIN_PROB, help="minimum LID probability to translate")
    parser.add_argument("--max-chars", type=int, default=1500, help="truncate each field to this many characters")
    parser.add_argument("--beam-size", type=int, default=DEFAULT_BEAM_SIZE)
    parser.add_argument("--batch-tokens", type=int, default=DEFAULT_BATCH_TOKENS, help="source tokens per translate batch")
    parser.add_argument("--inter-threads", type=int, default=1, help="CTranslate2 parallel batches on one GPU")
    parser.add_argument("--ungrouped", action="store_true", help="mix source languages within a batch (default: group by language)")
    parser.add_argument("--fields", nargs="+", default=None, help="text fields to translate (default: title + summary/description)")
    parser.add_argument("--chunk-rows", type=int, default=CHUNK_ROWS)
    args = parser.parse_args(argv)
    setup_logging("core_v4", "nllb_translation")
    cfg = resolve(args)

    lid = LanguageIdentifier()
    translator = NllbTranslator(
        args.model, backend=args.backend, device=args.device, quantization=args.quantization, max_chars=args.max_chars,
        beam_size=args.beam_size, batch_tokens=args.batch_tokens, group_by_language=not args.ungrouped,
        **({"inter_threads": args.inter_threads} if args.backend == "ctranslate2" else {}),
    )
    con = open_staging(cfg.db)
    for entity in cfg.entities:
        fields = args.fields or NLLB_FIELDS[entity]
        counters = run_entity(
            con, entity, fields, lid, translator, cfg.enrichment_dir,
            shard=cfg.shard, limit=cfg.limit, dry_run=cfg.dry_run, min_prob=args.min_prob, chunk_rows=args.chunk_rows,
        )
        s = translator.stats
        logging.info(
            f"[{entity}] done {counters}; {s.chunks:,} chunks, {s.tokens_per_second:.0f} src tokens/s, "
            f"{s.chunks_per_second:.1f} chunks/s"
        )


if __name__ == "__main__":
    main()
