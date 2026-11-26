import math
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, List, Optional, Tuple

import pandas as pd
import supervisely as sly
from supervisely.api.api import ApiField
from supervisely.api.pointcloud.pointcloud_api import PointcloudInfo

POINT_CLOUD_OCTREE = "point_cloud_octree"


def create_project(
    api: sly.Api,
    workspace_id: int,
    name: str,
    type: sly.ProjectType = sly.ProjectType.IMAGES,
    description: Optional[str] = "",
    settings: Dict = None,
    custom_data: Dict = None,
    readme: str = None,
    change_name_if_conflict: Optional[bool] = False,
    created_at: Optional[str] = None,
    updated_at: Optional[str] = None,
    created_by: Optional[str] = None,
) -> sly.Project:

    effective_name = api.project._get_effective_new_name(
        parent_id=workspace_id,
        name=name,
        change_name_if_conflict=change_name_if_conflict,
    )
    data = {
        ApiField.WORKSPACE_ID: workspace_id,
        ApiField.NAME: effective_name,
        ApiField.DESCRIPTION: description,
        ApiField.TYPE: str(type),
    }
    if settings is not None:
        data[ApiField.SETTINGS] = settings
    if custom_data is not None:
        data[ApiField.CUSTOM_DATA] = custom_data
    if readme is not None:
        data[ApiField.README] = readme
    if created_at is not None:
        data[ApiField.CREATED_AT] = created_at
    if updated_at is not None:
        data[ApiField.UPDATED_AT] = updated_at
    if created_by is not None:
        data[ApiField.CREATED_BY_ID[0][0]] = created_by
    try:
        response = api.post("projects.add", data)
    except Exception as e:
        if "Some users not found in team" in str(e):
            raise ValueError(
                "Unable to create a project. Project creator is not a member of the destination team."
            ) from e
        else:
            raise e
    return api.project._convert_json_info(response.json())


def get_project_activity(api: sly.Api, project_id: int):
    activity = api.post("projects.activity", {ApiField.ID: project_id}).json()
    df = pd.DataFrame(activity)
    return df


def images_get_list(api: sly.Api, dataset_id, image_ids=None):
    api_fields = [
        ApiField.ID,
        ApiField.NAME,
        ApiField.HASH,
        ApiField.DATASET_ID,
        ApiField.CREATED_AT,
        ApiField.UPDATED_AT,
        ApiField.META,
        ApiField.PATH_ORIGINAL,
        ApiField.CREATED_BY_ID[0][0],
        ApiField.DESCRIPTION,
    ]
    if image_ids is None:
        img_infos = api.image.get_list(
            dataset_id, fields=api_fields, force_metadata_for_links=False
        )
    else:
        img_infos = api.image.get_info_by_id_batch(
            ids=image_ids, fields=api_fields, force_metadata_for_links=False
        )
    return img_infos


def create_dataset(
    api: sly.Api,
    project_id: int,
    name: str,
    description: Optional[str] = "",
    custom_data: Dict = None,
    change_name_if_conflict: Optional[bool] = False,
    parent_id: Optional[int] = None,
    created_at: Optional[str] = None,
    updated_at: Optional[str] = None,
    created_by: Optional[int] = None,
):
    effective_name = api.dataset._get_effective_new_name(
        project_id=project_id,
        name=name,
        change_name_if_conflict=change_name_if_conflict,
        parent_id=parent_id,
    )
    data = {
        ApiField.PROJECT_ID: project_id,
        ApiField.NAME: effective_name,
        ApiField.DESCRIPTION: description,
        ApiField.PARENT_ID: parent_id,
    }
    if custom_data is not None:
        data[ApiField.CUSTOM_DATA] = custom_data
    if created_at is not None:
        data[ApiField.CREATED_AT] = created_at
    if updated_at is not None:
        data[ApiField.UPDATED_AT] = updated_at
    if created_by is not None:
        data[ApiField.CREATED_BY_ID[0][0]] = created_by
    try:
        response = api.post(
            "datasets.add",
            data,
        )
    except Exception as e:
        if "Some users not found in team" in str(e):
            raise ValueError(
                "Unable to create a dataset. Dataset creator is not a member of the destination team."
            ) from e
        else:
            raise e
    return api.dataset._convert_json_info(response.json())


def images_bulk_add(
    api: sly.Api,
    dataset_id: int,
    names: List[str],
    image_infos: List[sly.ImageInfo],
    perserve_dates: bool = False,
):
    img_data = []
    for name, img_info in zip(names, image_infos):
        img_json = {
            ApiField.NAME: name,
            ApiField.META: img_info.meta,
        }
        if perserve_dates:
            img_json[ApiField.CREATED_AT] = img_info.created_at
            img_json[ApiField.UPDATED_AT] = img_info.updated_at
            img_json[ApiField.CREATED_BY_ID[0][0]] = img_info.created_by
        if img_info.link is not None:
            img_json[ApiField.LINK] = img_info.link
        elif img_info.hash is not None:
            img_json[ApiField.HASH] = img_info.hash
        if img_info.description is not None:
            img_json[ApiField.DESCRIPTION] = img_info.description
        img_data.append(img_json)

    try:
        response = api.post(
            "images.bulk.add",
            {
                ApiField.DATASET_ID: dataset_id,
                ApiField.IMAGES: img_data,
                ApiField.FORCE_METADATA_FOR_LINKS: False,
                ApiField.SKIP_VALIDATION: True,
            },
        )
    except Exception as e:
        if "Some users are not members of the destination group" in str(e):
            raise ValueError(
                "Unable to add images. Image creator is not a member of the destination team."
            ) from e
        else:
            raise e

    results = []
    for info_json in response.json():
        info_json_copy = info_json.copy()
        if info_json.get(ApiField.MIME, None) is not None:
            info_json_copy[ApiField.EXT] = info_json[ApiField.MIME].split("/")[1]
        # results.append(self.InfoType(*[info_json_copy[field_name] for field_name in self.info_sequence()]))
        results.append(api.image._convert_json_info(info_json_copy))
    return results


def is_octree_pointcloud(info: PointcloudInfo) -> bool:
    """A streamed point cloud: an octree (`meta.octree`) rather than one file; it cannot be added by hash."""
    return bool((info.meta or {}).get("octree"))


def pointclouds_copy(
    api: sly.Api, dataset_id: int, names: List[str], infos: List[PointcloudInfo]
) -> List[PointcloudInfo]:
    """Add copies of existing point clouds to a dataset by ID; a copy shares its source's data (and octree)."""
    response = api.post(
        "entities.bulk.add",
        {
            ApiField.DATASET_ID: dataset_id,
            "entities": [
                {ApiField.ENTITY_ID: info.id, ApiField.TITLE: name, ApiField.META: info.meta}
                for name, info in zip(names, infos)
            ],
        },
    )
    ids = [entity[ApiField.ID] for entity in response.json()]
    filters = [{ApiField.FIELD: ApiField.ID, ApiField.OPERATOR: "in", ApiField.VALUE: ids}]
    copied = {info.name: info for info in api.pointcloud.get_list(dataset_id, filters=filters)}
    return [copied[name] for name in names]


# figures.bulk.add has no limit on the number of figures; it checks the cell file of each
# point_cloud_octree pointer with its own query, one after another. 1000 pointers (~280 KB)
# take about 1 s per call, so a retried call stays short and a cloud of 40k figures needs 40.
OCTREE_FIGURES_BATCH_SIZE = 1000


def pointcloud_octree_figures_copy(
    api: sly.Api,
    dst_id: int,
    figures: List[Dict],
    src_key_id_map: sly.KeyIdMap,
    dst_key_id_map: sly.KeyIdMap,
) -> None:
    """Add the `point_cloud_octree` figures of a downloaded source annotation to the copy `dst_id`.

    The SDK has no octree geometry. A figure's geometry points at a cell file of the octree the copy
    shares, so it is passed on as downloaded. An object maps from its source ID to its key in
    `src_key_id_map`, and from the key to the copied object in `dst_key_id_map`. The annotation
    lists the figures by priority and leaves the priority out; the server assigns it again in the
    same order.
    """
    figures = [
        {
            ApiField.OBJECT_ID: dst_key_id_map.get_object_id(
                src_key_id_map.get_object_key(figure[ApiField.OBJECT_ID])
            ),
            ApiField.GEOMETRY_TYPE: POINT_CLOUD_OCTREE,
            ApiField.GEOMETRY: figure[ApiField.GEOMETRY],
            ApiField.NN_CREATED: figure.get(ApiField.NN_CREATED, False),
        }
        for figure in figures
    ]
    for batch in sly.batched(figures, batch_size=OCTREE_FIGURES_BATCH_SIZE):
        api.post("figures.bulk.add", {ApiField.ENTITY_ID: dst_id, ApiField.FIGURES: batch})


RELATED_IMAGE_FIGURES = "figures on related images"
POINTCLOUD_COPY_CHECKS = ("figures", "objects", "tags", "related images", RELATED_IMAGE_FIGURES)


def _list_all(api: sly.Api, method: str, data: Dict, per_page: int) -> List[Dict]:
    return api.pointcloud.get_list_all_pages(
        method, {**data, "per_page": per_page}, convert_json_info_cb=lambda x: x
    )


# An "in" filter takes at most PAGINATION_LIMIT values (500 by default).
ID_FILTER_MAX = 500


def _id_filter(field: str, ids: List[int]) -> List[Dict]:
    return [{ApiField.FIELD: field, ApiField.OPERATOR: "in", ApiField.VALUE: ids}]


def _figures(api: sly.Api, dataset_id: int, entity_ids: List[int]) -> List[Dict]:
    """The figures on the entities, without geometry: `id`, `entityId`, `objectId`."""
    return _list_all(
        api,
        "figures.list",
        {
            ApiField.DATASET_ID: dataset_id,
            ApiField.FIELDS: [ApiField.ID, ApiField.ENTITY_ID, ApiField.OBJECT_ID],
            ApiField.FILTER: _id_filter(ApiField.ENTITY_ID, entity_ids),
        },
        per_page=20000,
    )


def _object_tag_counts(api: sly.Api, dataset_id: int, object_ids: List[int]) -> Dict[int, int]:
    """Tags per object: listed by ID, 500 per request, or the dataset's objects 5000 per page
    when that takes fewer requests."""
    if len(object_ids) == 0:
        return {}
    data = {ApiField.DATASET_ID: dataset_id, ApiField.FIELDS: [ApiField.ID]}
    total = api.post("annotation-objects.list", {**data, "per_page": 1}).json()["total"]
    if math.ceil(len(object_ids) / ID_FILTER_MAX) > math.ceil(total / 5000):
        objects = _list_all(api, "annotation-objects.list", data, per_page=5000)
    else:
        objects = [
            obj
            for batch in sly.batched(object_ids, batch_size=ID_FILTER_MAX)
            for obj in _list_all(
                api,
                "annotation-objects.list",
                {**data, ApiField.FILTER: _id_filter(ApiField.ID, batch)},
                per_page=5000,
            )
        ]
    return {obj[ApiField.ID]: len(obj[ApiField.TAGS]) for obj in objects}


def pointclouds_contents(api: sly.Api, dataset_id: int, ids: List[int]) -> Dict[int, Dict[str, int]]:
    """Count, per point cloud of the dataset, what its copy has to reproduce (`POINTCLOUD_COPY_CHECKS`).

    Figures of every geometry type, `point_cloud_octree` included; the objects those figures use;
    the tags on the point cloud and on those objects; its related images and the figures on them.
    Everything is listed without geometry, so the requests grow with these counts only.
    """
    infos = []
    figures = Counter()
    objects = defaultdict(set)
    related = Counter()
    image_figures = Counter()
    for batch in sly.batched(ids, batch_size=100):
        infos += api.pointcloud.get_list(dataset_id, filters=_id_filter(ApiField.ID, batch))
        for figure in _figures(api, dataset_id, batch):
            figures[figure[ApiField.ENTITY_ID]] += 1
            if figure[ApiField.OBJECT_ID] is not None:
                objects[figure[ApiField.ENTITY_ID]].add(figure[ApiField.OBJECT_ID])
        images = api.pointcloud.get_list_related_images_batch(dataset_id, batch)
        parents = {image[ApiField.ID]: image[ApiField.ENTITY_ID] for image in images}
        related.update(parents.values())
        for image_batch in sly.batched(list(parents), batch_size=ID_FILTER_MAX):
            for figure in _figures(api, dataset_id, image_batch):
                image_figures[parents[figure[ApiField.ENTITY_ID]]] += 1
    object_tags = _object_tag_counts(api, dataset_id, sorted(set().union(*objects.values())))
    return {
        info.id: {
            "figures": figures[info.id],
            "objects": len(objects[info.id]),
            "tags": len(info.tags) + sum(object_tags[o] for o in objects[info.id]),
            "related images": related[info.id],
            RELATED_IMAGE_FIGURES: image_figures[info.id],
        }
        for info in infos
    }


def pointclouds_copy_mismatches(
    api: sly.Api,
    src_dataset_id: int,
    dst_dataset_id: int,
    copies: Dict[int, int],
    checks: Tuple[str, ...] = POINTCLOUD_COPY_CHECKS,
) -> Dict[int, List[str]]:
    """Compare each copied point cloud (`copies`: source ID -> copy ID) with its source.

    Returns the differences per source ID, e.g. `["figures 21 -> 20"]`; empty when every copy matches.
    """
    with ThreadPoolExecutor(max_workers=1) as pool:
        src_task = pool.submit(pointclouds_contents, api, src_dataset_id, list(copies.keys()))
        dst = pointclouds_contents(api, dst_dataset_id, list(copies.values()))
        src = src_task.result()
    mismatches = {}
    for src_id, dst_id in copies.items():
        diff = [
            f"{check} {src[src_id][check]} -> {dst[dst_id][check]}"
            for check in checks
            if src[src_id][check] != dst[dst_id][check]
        ]
        if len(diff) > 0:
            mismatches[src_id] = diff
    return mismatches
