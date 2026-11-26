import math
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, List, Optional, Set, Tuple

import pandas as pd
import supervisely as sly
from supervisely.api.api import ApiField
from supervisely.api.pointcloud.pointcloud_api import PointcloudInfo


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


# The bulk write routes take any number of rows (the API's body limit is 128 MB by default), and
# each request costs ~0.4 s on top of the server's time (the SDK opens a connection per request),
# so batches are large. figures.bulk.add checks the cell file of each point_cloud_octree pointer
# with its own query, one after another: 5000 pointers (~1.7 MB) take about 5 s per call.
WRITE_BATCH_SIZE = 5000
# Index figures of an ordinary point cloud file; their indices are uploaded separately.
POINT_CLOUD_INDICES = "point_cloud"
# Other geometry the server keeps in a file of its own: figures.bulk.add ignores a posted geometry
# of these types and figures.list returns only the file's path, so a copy would be empty.
UNSUPPORTED_GEOMETRY_TYPES = ("mask_3d", "mesh", "closed_surface_mesh", "alpha_mask")


def _post_batched(api: sly.Api, method: str, data: Dict, field: str, items: List[Dict]) -> List[Dict]:
    """Post `items` as `field` in batches of `WRITE_BATCH_SIZE`; the created rows, in order."""
    created = []
    for batch in sly.batched(items, batch_size=WRITE_BATCH_SIZE):
        created += api.post(method, {**data, field: batch}).json()
    return created


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


def _figures(
    api: sly.Api,
    dataset_id: int,
    entity_ids: List[int],
    fields: Tuple[str, ...] = (ApiField.ID, ApiField.ENTITY_ID, ApiField.OBJECT_ID),
) -> List[Dict]:
    """The figures on the entities, without geometry unless `fields` has it."""
    return [
        figure
        for batch in sly.batched(entity_ids, batch_size=ID_FILTER_MAX)
        for figure in _list_all(
            api,
            "figures.list",
            {
                ApiField.DATASET_ID: dataset_id,
                ApiField.FIELDS: list(fields),
                ApiField.FILTER: _id_filter(ApiField.ENTITY_ID, batch),
            },
            per_page=20000,
        )
    ]


def _count(api: sly.Api, method: str, dataset_id: int) -> int:
    # recursive by default for objects: the dataset's route would also count its child datasets'
    data = {ApiField.DATASET_ID: dataset_id, ApiField.FIELDS: [ApiField.ID], "recursive": False}
    return api.post(method, {**data, "per_page": 1}).json()["total"]


def objects_count(api: sly.Api, dataset_id: int) -> int:
    """The number of objects in the dataset, its child datasets not included."""
    return _count(api, "annotation-objects.list", dataset_id)


def pointclouds_count(api: sly.Api, dataset_id: int) -> int:
    """The number of point clouds in the dataset, its child datasets not included."""
    return _count(api, "point-clouds.list", dataset_id)


def _objects(api: sly.Api, dataset_id: int, object_ids: List[int]) -> List[Dict]:
    """The dataset's objects with these IDs (`id`, `classId`, `tags`): listed by ID, 500 per
    request, or the dataset's objects 5000 per page when that takes fewer requests."""
    if len(object_ids) == 0:
        return []
    data = {
        ApiField.DATASET_ID: dataset_id,
        ApiField.FIELDS: [ApiField.ID, ApiField.CLASS_ID],
        "recursive": False,
    }
    if math.ceil(len(object_ids) / ID_FILTER_MAX) > math.ceil(objects_count(api, dataset_id) / 5000):
        wanted = set(object_ids)
        return [
            obj
            for obj in _list_all(api, "annotation-objects.list", data, per_page=5000)
            if obj[ApiField.ID] in wanted
        ]
    return [
        obj
        for batch in sly.batched(object_ids, batch_size=ID_FILTER_MAX)
        for obj in _list_all(
            api,
            "annotation-objects.list",
            {**data, ApiField.FILTER: _id_filter(ApiField.ID, batch)},
            per_page=5000,
        )
    ]


def _cloud_tags(api: sly.Api, dataset_id: int, ids: List[int]) -> Dict[int, List[Dict]]:
    """The tags of the point clouds with their `customData` and `meta`, which `point-clouds.list`
    leaves out."""
    return {
        entity[ApiField.ID]: entity[ApiField.TAGS]
        for batch in sly.batched(ids, batch_size=ID_FILTER_MAX)
        for entity in _list_all(
            api,
            "entities.list",
            {
                ApiField.DATASET_ID: dataset_id,
                ApiField.FIELDS: [ApiField.ID, ApiField.TAGS],
                ApiField.FILTER: _id_filter(ApiField.ID, batch),
            },
            per_page=ID_FILTER_MAX,
        )
    }


def _require(found, wanted, message: str) -> None:
    missing = sorted(set(wanted) - set(found))
    if len(missing) > 0:
        raise RuntimeError(f"{message}: {', '.join(map(str, missing[:10]))}")


def pointclouds_annotations_copy(
    api: sly.Api,
    src_dataset_id: int,
    dst_dataset_id: int,
    dst_project_id: int,
    copies: Dict[int, int],
    class_ids: Dict[int, int],
    tag_ids: Dict[int, int],
) -> Set[int]:
    """Copy the annotations of point clouds (`copies`: source ID -> copy ID) to their copies in
    another dataset; return the IDs of the source objects copied.

    Read: the figures with geometry (`figures.list`; a `point_cloud_octree` geometry is a pointer
    to a cell file of the octree the copy shares; an index figure holds its indices, written before
    platform 6.16.20, or a stored file downloaded apart), the objects they use with their tags
    (`annotation-objects.list`), and the point clouds' tags (`entities.list`). Not
    `point-clouds.annotations.bulk.info`: it repeats in each annotation every object of the dataset
    that is not bound to a point cloud (the SDK and the labeling tool bind none), 306 MB for 50
    clouds of a dataset of 30,000 objects.

    Write, one call per `WRITE_BATCH_SIZE` rows for all the copies together: each copy gets its own
    copy of every object its figures use (`annotation-objects.bulk.add` returns the objects in the
    order given), then the tags of those objects and of the point cloud, then the figures by
    priority, which the server assigns again in the same order; index figures without geometry
    and their indices uploaded after them. Classes and tags map to the destination project's by
    ID (`class_ids`, `tag_ids`: source ID -> destination ID). Everything is checked before the
    first write.
    """
    fields = (
        ApiField.ID,
        ApiField.ENTITY_ID,
        ApiField.OBJECT_ID,
        ApiField.CLASS_ID,
        ApiField.GEOMETRY_TYPE,
        ApiField.GEOMETRY,
        ApiField.NN_CREATED,
        ApiField.PRIORITY,
        ApiField.TRACK_ID,
    )
    figures = defaultdict(list)
    for figure in sorted(
        _figures(api, src_dataset_id, list(copies), fields),
        key=lambda f: (f[ApiField.PRIORITY] is None, f[ApiField.PRIORITY] or 0, f[ApiField.ID]),
    ):
        figures[figure[ApiField.ENTITY_ID]].append(figure)
    all_figures = [f for fs in figures.values() for f in fs]
    _require(
        set(),
        {
            f"{f[ApiField.GEOMETRY_TYPE]} (point cloud {f[ApiField.ENTITY_ID]})"
            for f in all_figures
            if f[ApiField.GEOMETRY_TYPE] in UNSUPPORTED_GEOMETRY_TYPES
        },
        "Figures of these types cannot be copied yet",
    )
    used = {f[ApiField.OBJECT_ID] for f in all_figures} - {None}
    objects = {obj[ApiField.ID]: obj for obj in _objects(api, src_dataset_id, sorted(used))}
    _require(objects, used, f"Objects of the figures not found in dataset {src_dataset_id}")
    cloud_tags = _cloud_tags(api, src_dataset_id, list(copies))
    _require(
        class_ids,
        {obj[ApiField.CLASS_ID] for obj in objects.values()}
        | {f[ApiField.CLASS_ID] for f in all_figures if f[ApiField.OBJECT_ID] is None},
        "Source classes with no class of the same name in the destination project (IDs)",
    )
    _require(
        tag_ids,
        {tag[ApiField.TAG_ID] for obj in objects.values() for tag in obj[ApiField.TAGS]}
        | {tag[ApiField.TAG_ID] for tags in cloud_tags.values() for tag in tags},
        "Source tags with no tag of the same name in the destination project (IDs)",
    )
    indices, stored = {}, []
    for f in all_figures:
        if f[ApiField.GEOMETRY_TYPE] == POINT_CLOUD_INDICES:
            inline = (f[ApiField.GEOMETRY] or {}).get("indices")
            if isinstance(inline, list):
                indices[f[ApiField.ID]] = inline
            else:
                stored.append(f[ApiField.ID])
    if len(stored) > 0:
        indices.update(zip(stored, api.pointcloud.figure.download_indices_batch(stored)))

    keys = []  # (copy ID, source object ID), in the order the objects are added
    for src_id, dst_id in copies.items():
        cloud_objects = dict.fromkeys(f[ApiField.OBJECT_ID] for f in figures[src_id])
        keys += [(dst_id, obj_id) for obj_id in cloud_objects if obj_id is not None]
    created = _post_batched(
        api,
        "annotation-objects.bulk.add",
        {ApiField.DATASET_ID: dst_dataset_id},
        ApiField.ANNOTATION_OBJECTS,
        [{ApiField.CLASS_ID: class_ids[objects[obj_id][ApiField.CLASS_ID]]} for _, obj_id in keys],
    )
    object_ids = {key: obj[ApiField.ID] for key, obj in zip(keys, created)}

    def _tag(tag: Dict, field: str, target_id: int, extra: Tuple[str, ...]) -> Dict:
        row = {
            ApiField.TAG_ID: tag_ids[tag[ApiField.TAG_ID]],
            field: target_id,
            ApiField.VALUE: tag.get(ApiField.VALUE),
        }
        row.update({key: tag[key] for key in extra if tag.get(key)})
        return row

    tags_data = {ApiField.PROJECT_ID: dst_project_id}
    object_tags = [
        _tag(tag, ApiField.OBJECT_ID, new_id, (ApiField.CUSTOM_DATA,))
        for (_, obj_id), new_id in object_ids.items()
        for tag in objects[obj_id][ApiField.TAGS]
    ]
    _post_batched(api, "annotation-objects.tags.bulk.add", tags_data, ApiField.TAGS, object_tags)
    entity_tags = [
        _tag(tag, ApiField.ENTITY_ID, dst_id, (ApiField.CUSTOM_DATA, ApiField.META))
        for src_id, dst_id in copies.items()
        for tag in cloud_tags.get(src_id, [])
    ]
    _post_batched(api, "tags.entities.bulk.add", tags_data, ApiField.TAGS, entity_tags)

    rows, index_rows = [], {}
    for src_id, dst_id in copies.items():
        for figure in figures[src_id]:
            row = {
                ApiField.ENTITY_ID: dst_id,
                ApiField.GEOMETRY_TYPE: figure[ApiField.GEOMETRY_TYPE],
                ApiField.NN_CREATED: bool(figure.get(ApiField.NN_CREATED)),
            }
            if figure.get(ApiField.TRACK_ID) is not None:
                row[ApiField.TRACK_ID] = figure[ApiField.TRACK_ID]
            if figure[ApiField.OBJECT_ID] is None:
                row[ApiField.CLASS_ID] = class_ids[figure[ApiField.CLASS_ID]]
            else:
                row[ApiField.OBJECT_ID] = object_ids[(dst_id, figure[ApiField.OBJECT_ID])]
            if figure[ApiField.GEOMETRY_TYPE] == POINT_CLOUD_INDICES:
                index_rows[len(rows)] = indices[figure[ApiField.ID]]
            else:
                row[ApiField.GEOMETRY] = figure[ApiField.GEOMETRY]
            rows.append(row)
    created = _post_batched(
        api, "figures.bulk.add", {ApiField.DATASET_ID: dst_dataset_id}, ApiField.FIGURES, rows
    )
    if len(index_rows) > 0:
        api.pointcloud.figure.upload_indices_batch(
            [created[i][ApiField.ID] for i in index_rows], list(index_rows.values())
        )
    return set(objects)


def _object_tag_counts(api: sly.Api, dataset_id: int, object_ids: List[int]) -> Dict[int, int]:
    """Tags per object."""
    return {
        obj[ApiField.ID]: len(obj[ApiField.TAGS]) for obj in _objects(api, dataset_id, object_ids)
    }


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
