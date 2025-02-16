from typing import List, Dict

datasets = [
    {"name": "DS1", "parent": None},
    {"name": "DS1-1", "parent": "DS1"},
    {"name": "DS1-1-1", "parent": "DS1-1"},
    {"name": "DS2", "parent": None},
    {"name": "DS2-1", "parent": "DS2"},
    {"name": "DS2-1-1", "parent": "DS2-1"},
    {"name": "DS2-1-1-1", "parent": "DS2-1-1"},
    {"name": "DS2-2", "parent": "DS2"},
    {"name": "DS2-2-1", "parent": "DS2-2"},
    {"name": "DS2-2-1-1", "parent": "DS2-2-1"},
    {"name": "DS2-2-1-1-1", "parent": "DS2-2-1-1"},
]


empty = [
    "DS1",
    "DS1-1-1",
    "DS2",
    "DS2-1",
    "DS2-1-1",
    "DS2-1-1-1",
    "DS2-2",
    "DS2-2-1",
    "DS2-2-1-1-1",
]


def find_parent(id: str, dst_datasets: List[Dict]):
    """Find parent dataset info by id in list of datasets"""
    for ds in dst_datasets:
        if ds["name"] == id:
            return ds
    return None


def get_parents_chain(ds: Dict, dst_datasets: List[Dict]):
    """
    Get chain of parent datasets for defined dataset.
    Returns list of datasets from bottom to top.
    """
    chain = []
    while ds is not None:
        chain.append(ds)
        ds = find_parent(ds["parent"], dst_datasets)
    return chain


def ensure_dataset_deletion(
    ds: Dict,
    dst_datasets: List[Dict],
    dataset_deletion_map: Dict[str, bool],
    empty_datasets: List[Dict],
):

    keep = False
    chain = get_parents_chain(ds, dst_datasets)
    for ds in chain:  # from bottom to top
        if keep:
            dataset_deletion_map[ds["name"]] = False
        elif ds["name"] not in empty_datasets:
            keep = True
            dataset_deletion_map[ds["name"]] = False


# dataset_deletion_map = {ds["name"]: True for ds in datasets}

# for ds in datasets:
#     ensure_dataset_deletion(ds, datasets, dataset_deletion_map, empty)

# for item, value in dataset_deletion_map.items():
#     print(f"{item}: {value}")
from supervisely.api.api import Api
import supervisely as sly

def flatten_tree(tree: Dict):
    result = []

    def _dfs(tree: Dict):
        for ds in sorted(tree.keys(), key=lambda obj: obj.id):
            children = tree[ds]
            result.append(ds)
            _dfs(children)

    _dfs(tree)
    return result


def flatten_tree_by_map(tree: Dict, map: Dict):
    result = []

    def _dfs(tree: Dict):
        for ds in sorted(tree.keys(), key=lambda obj: int(obj)):
            children = tree[ds]
            result.append(map[ds])
            _dfs(children)

    _dfs(tree)
    return result


def tree_from_list(datasets: List[sly.DatasetInfo]) -> Dict[int, Dict]:
    parent_map = {}

    # ----------------------------------- Grouping Ds By Parent_id ----------------------------------- #
    for dataset in datasets:
        parent_map.setdefault(dataset.parent_id, []).append(dataset)

    def build_tree(parent_id: int) -> Dict[int, Dict]:
        return {dataset.id: build_tree(dataset.id) for dataset in parent_map.get(parent_id, [])}

    return build_tree(None)


api = Api.from_env()

dataset_tree = api.dataset.get_tree(172)
flatten = flatten_tree(dataset_tree)
project_map = {ds.id: ds for ds in flatten}
restored_tree = tree_from_list(flatten)
flatten_restored = flatten_tree_by_map(restored_tree, project_map)
assert flatten == flatten_restored
