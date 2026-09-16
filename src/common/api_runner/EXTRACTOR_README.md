# Extraction

Extractions are split into checkpoints, which are updated after each extraction iteration. The `run_extraction.py`
script continues running extraction loops as long as individual extractors indicate they have more data to extract
via `should_continue()`. This continuation logic is domain-specific.

All extractors are configured in [config_queries.json](../../../config/config_queries.json). An extractors starts
from `checkpoint_start` as defined in the config.
All extractors must inherit the `IExtractor`interface which sets up all parameters, path and the checkpoints.

**Examples:**

- **Arxiv**: Uses `submittedDate` and continues until `datetime.now()` is reached. When the final checkpoint is reached,
  the checkpoint is *not* advanced, allowing the next run to collect newly submitted papers within the same time window.
- **Cordis**: Uses `startDate` and continues until `datetime.now() + 10 years` to collect projects starting in the
  future. When the final checkpoint is reached, the checkpoint is reset to 5 years ago to collect project updates in the
  next run, overwriting previously extracted projects with their latest state.

## Saving data

- Each API response (publication, project etc.) is called a **record_dataset**.
- Each record_dataset gets its own folder, named: `YYYY-MM-DD(optional: _HH-MM)-sanitize(title)`.
- This folder has to be saved in `self.data_path` which is preconfigured in `i_extractor.py`.
- Attachments are saved under `YYYY-MM-DD_HH-MM-record_dataset/attachments/sanitize(title)`
- When extracting the same data again, it will overwrite existing data if given the same name. This also updates the
  file `mtime` which is important for the loader checkpoint

## Checkpoint Notes

- When extracting, 1 day should be removed in `get_checkpoint_end(minus_1_day=True)` to not have the checkpoint runs
  overlap each other.
- In simple cases, like submission dates which can not lie in the future, after `should_continue()` is reached the
  checkpoint should not be saved, to collect updates within the checkpoint range in the next run.
- In more complicated cases, like projects which get updated over time and are planned for the future,
  after `should_continue()` returns false, an old checkpoint should be saved to overwrite existing record_datasets with
  relevant updates. It is important to pick an existing checkpoint which points to the right data, so that it can be overwritten.

## Sleep between requests

- Generally advised between pagination and next extraction. Each extraction automatically
  uses `sleep_between_extractions=5`.
- A retry mechanism with exponential backoff decorator is available for `make_get_request(enable_retry=True)`.
