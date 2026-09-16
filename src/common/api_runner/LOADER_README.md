# Data Loader

The data loader takes the raw source files and transforms them into SQL ALchemy objects which are added to
the current database session. The models will be commited and loaded into the database in batches.

## Checkpoint

All files to be loaded are generated using `yield_all_documents`
in [yield_documents.py](../../lib/file_handling/yield_documents.py).
Given a source directory this function returns the `file_path` and the `last_file_update_time`.
We use the `last_file_update_time` as the basis for the loader checkpoint mechanism.

A files `last_file_update_time` is compared to `cp_last_update_time` given a margin for error,
when the `last_file_update_time` < `cp_last_update_time` the file is skipped.
Therefor the files do not have to be generated in order.
After walking through all files the latest `last_file_update_time` is being saved as the new `cp_last_update_time`
for the current source.

**There are 2 Scenarios**:

* Only load newly added files.
* Load all files that have changed.

The 2nd scenario only works when the extractor overwrites the files that need to be updated
and the updated timestamp of the file changed.

**Possible issues**:

* We can only save the cp after walking through all files, so an error no progress is saved and you have to process all
  files again. Which is alright because of get_or_create
* Different time zones: Local Dev and Cluster are both in germany

## Checkpoint Validation

After processing all files the number of processed files, skipped files and rows in the database are reported.