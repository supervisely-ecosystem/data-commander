from ast import Dict
from typing import List, Optional
import pandas as pd
import supervisely as sly
from supervisely.api.api import ApiField


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
    if created_by is not None:
        data[ApiField.CREATED_BY_ID[0][0]] = created_by
    response = api.post("projects.add", data)
    return api.project._convert_json_info(response.json())


def get_project_activity(api: sly.Api, project_id: int):
    activity = api.post("projects.activity", {ApiField.ID: project_id}).json()
    df = pd.DataFrame(activity)
    return df


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
            # img_json[ApiField.CREATED_BY_ID[0][0]] = img_info.
        if img_info.link is not None:
            img_json[ApiField.LINK] = img_info.link
        elif img_info.hash is not None:
            img_json[ApiField.HASH] = img_info.hash
        img_data.append(img_json)

    response = api.post(
        "images.bulk.add",
        {
            ApiField.DATASET_ID: dataset_id,
            ApiField.IMAGES: img_data,
            ApiField.FORCE_METADATA_FOR_LINKS: False,
            ApiField.SKIP_VALIDATION: True,
        },
    )
    results = []
    for info_json in response.json():
        info_json_copy = info_json.copy()
        if info_json.get(ApiField.MIME, None) is not None:
            info_json_copy[ApiField.EXT] = info_json[ApiField.MIME].split("/")[1]
        # results.append(self.InfoType(*[info_json_copy[field_name] for field_name in self.info_sequence()]))
        results.append(api.image._convert_json_info(info_json_copy))
    return results
