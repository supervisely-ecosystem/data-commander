# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

**Data Commander** is a headless Supervisely platform app for data management: copy, move, merge projects/datasets/items, and transfer accepted items from labeling jobs. Runs inside Supervisely ecosystem via Docker; all inputs come from environment variables.

## Running Locally

Requires two env files:
- `local.env` — app state (action, source, destination, options, items)
- `~/supervisely.env` — Supervisely API credentials (`SERVER_ADDRESS`, `API_TOKEN`)

```bash
python src/main.py
```

`TASK_ID` must be set in `local.env` — required by the Supervisely framework.

## Architecture

### Entry Point

`src/main.py` (~3200 lines) is a single monolithic file. Execution flow:
1. `extract_state_from_env()` — parses `modal.state.*` env vars into a nested `state` dict
2. `main()` routes to: `copy_or_move()`, `transfer_labeled_items()`, or `merge()`
3. `sly.main_wrapper("Data Commander", main)` wraps execution for Supervisely framework

### Key Classes

- `JSONKEYS` — string constants for all JSON/env key names
- `Source` / `Destination` — location context (team/workspace/project/dataset + level detection)
- `Options` — transfer config: `preserve_src_date`, `clone_annotations`, `conflict_resolution_mode`, `preserve_structure`, `transfer_mode`
- `CreatedDataset` — result of dataset cloning with `conflict_resolution_result` (`copied`/`skipped`/`replaced`/`renamed`)
- `Level` — enum-like: `TEAM`, `WORKSPACE`, `PROJECT`, `DATASET`

### Item Type Clone Functions

Each media type handles annotations separately:
- `clone_images_with_annotations()` — batch upload via `api_utils.images_bulk_add()`
- `clone_videos_with_annotations()`
- `clone_volumes_with_annotations()`
- `clone_pointclouds_with_annotations()`
- `clone_pointcloud_episodes_with_annotations()`
- `clone_meshes_with_annotations()` — server-side copy via `api.mesh.upload_ids()`; annotations follow the image model (IDs inline, no `KeyIdMap`)

### Dataset Tree Helpers

Hierarchical dataset operations use tree structures (dict-of-dicts keyed by dataset ID):
- `tree_from_list()` — builds nested tree from flat `DatasetInfo` list
- `flatten_tree()` / `flatten_tree_sorted_name()` — converts tree back to ordered list
- `create_dataset_recursively()` — mirrors src tree at destination, returns `CreatedDataset` list
- `_find_tree()`, `find_children_in_tree()` — tree traversal utilities

### Conflict Resolution

Controlled by `options.conflictResolutionMode`:
- `skip` — leave existing item untouched
- `rename` — append timestamp suffix to new item name
- `replace` — delete existing, upload new; implemented by `replace_project()` / `replace_dataset()`

Three separate top-level copy strategies: `copy_project_with_replace()`, `copy_project_with_skip()`, `copy_project()` (for rename mode).

### Transfer Labeled Items Logic

`transfer_labeled_items()` → `transfer_from_project()` / `transfer_from_dataset()`:
- Only items from **completed** labeling jobs with **accepted** review status
- Item is skipped if it has any sibling job still in-progress (even if accepted in another completed job)
- Creates a backup version of destination project after transfer (images only)
- `ensure_datasets_deletion()` handles cleanup of empty datasets after move

### Concurrency

`run_in_executor()` wraps `ThreadPoolExecutor(max_workers=5)` — used for parallel dataset-level operations. `env_lock` (threading.Lock) guards env-level shared state.

### `src/api_utils.py`

Thin wrappers around Supervisely SDK API calls adding parameters not exposed in the official SDK:
- `create_project()` — adds `created_at`, `updated_at`, `created_by` preservation
- `create_dataset()` — same date/author preservation
- `images_bulk_add()` — bulk image upload preserving metadata, hash/link, description
- `images_get_list()` — fetches extended fields including `created_by`, `description`

### `supervisely/` Directory

Local editable copy of the Supervisely SDK (pinned to `6.73.537`). Used for local dev; Docker uses the installed package from the base image `supervisely/base-py-sdk:6.73.537`.

## Environment Variable Format

`extract_state_from_env()` parses `modal.state.*` keys into a nested dict. Values auto-cast: JSON arrays via `ast.literal_eval`, booleans from `"true"`/`"false"` strings.

```
modal.state.action = "copy"  # copy | move | transfer_labeled_items | merge
modal.state.source.team.id = 8
modal.state.source.project.id = 123
modal.state.destination.project.id = 456
modal.state.options.cloneAnnotations = true
modal.state.options.conflictResolutionMode = "replace"  # skip | rename | replace
modal.state.options.preserveSrcDate = false
modal.state.options.preserveStructure = true
modal.state.items = [{"id":123,"type":"image"}]  # optional: scope to specific items/datasets/jobs
TASK_ID = 57919
```

`items` type values: `image`, `video`, `volume`, `pointcloud`, `pointcloud_episode`, `mesh`, `dataset`, `job`, `queue`, `collection`

A `collection` item (entities collection ID) is resolved via `api.entities_collection.get_items()` into a flat list of images processed by a dedicated path (`copy_collection_items_to_dataset()` / `move_collection_items_to_dataset()`) — all images land in the destination dataset without preserving source structure. Works for both `default` and `aiSearch` collection types. Same-named images from different datasets are disambiguated upfront by `disambiguate_collection_names()`: every image involved in a name conflict gets `_<src dataset ID>` appended before the extension. Conflicts with pre-existing destination images are still handled by `conflictResolutionMode`. Auto-created filter collections (named like `Filtered entities <timestamp>`) are renamed by `rename_filtered_collection()` to `Data Commander (Task <ID>)` when processed.

## Docker

```bash
docker build -t supervisely/data-commander:1.0.6 .
```

Base image `supervisely/base-py-sdk:6.73.537` includes all dependencies. `dev_requirements.txt` adds only the supervisely package for local dev.
