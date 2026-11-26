from typing import Dict, List, Optional

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
    copied = {info.id: info for info in api.pointcloud.get_list(dataset_id, filters=filters)}
    return [copied[id] for id in ids]


def pointcloud_octree_figures_copy(
    api: sly.Api,
    src_id: int,
    dst_id: int,
    src_key_id_map: sly.KeyIdMap,
    dst_key_id_map: sly.KeyIdMap,
) -> None:
    """Add the `point_cloud_octree` figures of point cloud `src_id` to its copy `dst_id`.

    The SDK has no octree geometry. A figure points at a cell file of the octree the copy shares,
    so the copy's figures take the source pointers unchanged. An object maps from its source ID
    to its key in `src_key_id_map`, and from the key to the copied object in `dst_key_id_map`.
    """
    labels = api.get("point-clouds.octree.labels.figures", {ApiField.ID: src_id}).json()
    figures = [
        {
            ApiField.OBJECT_ID: dst_key_id_map.get_object_id(
                src_key_id_map.get_object_key(figure[ApiField.OBJECT_ID])
            ),
            ApiField.GEOMETRY_TYPE: POINT_CLOUD_OCTREE,
            ApiField.GEOMETRY: {
                "version": 2,
                "treeRevision": labels["treeRevision"],
                "file": figure["file"],
            },
        }
        for figure in labels["figures"]
    ]
    for batch in sly.batched(figures, batch_size=1000):
        api.post("figures.bulk.add", {ApiField.ENTITY_ID: dst_id, ApiField.FIGURES: batch})
