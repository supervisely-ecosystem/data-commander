import ast
from concurrent.futures import ThreadPoolExecutor, as_completed, wait
from datetime import datetime
from queue import Queue
import os
from typing import Dict, List, Tuple, Union, Optional, Any
from dotenv import load_dotenv
from supervisely import logger
import supervisely as sly
from tqdm import tqdm


import api_utils as api_utils


load_dotenv("local.env")
load_dotenv(os.path.expanduser("~/supervisely.env"))


DATE_FORMAT = "%Y-%m-%dT%H:%M:%S.%fZ"
UPLOAD_IMAGES_BATCH_SIZE = 1000

api = sly.Api(ignore_task_id=True)
executor = ThreadPoolExecutor(max_workers=5)
merged_meta = None


class JSONKEYS:
    ACTION = "action"
    ACTION_COPY = "copy"
    ACTION_MOVE = "move"
    SOURCE = "source"
    DESTINATION = "destination"
    OPTIONS = "options"
    ITEMS = "items"
    TEAM = "team"
    WORKSPACE = "workspace"
    PROJECT = "project"
    DATASET = "dataset"
    ID = "id"
    NAME = "name"
    TYPE = "type"
    PRESERVE_SRC_DATE = "preserveSrcDate"
    CLONE_ANNOTATIONS = "cloneAnnotations"
    CONFLICT_RESOLUTION_MODE = "conflictResolutionMode"
    CONFLICT_SKIP = "skip"
    CONFLICT_RENAME = "rename"
    CONFLICT_REPLACE = "replace"
    IMAGE = "image"
    VIDEO = "video"
    VOLUME = "volume"
    POINTCLOUD = "pointcloud"
    POINTCLOUD_EPISODE = "pointcloud_episode"
    REVIEW_STATUS = "reviewStatus"
    ACCEPTED = "accepted"
    REJECTED = "rejected"
    REVIEWED = "reviewed"
    COMPLETED = "completed"
    DONE = "done"
    NONE = "none"
    JOB = "labelingJob"
    QUEUE = "labelingQueue"
    PRESERVE_SRC_STRUCTURE = "preserveStructure"


class Level:
    TEAM = JSONKEYS.TEAM
    WORKSPACE = JSONKEYS.WORKSPACE
    PROJECT = JSONKEYS.PROJECT
    DATASET = JSONKEYS.DATASET


class Destination:

    def __init__(
        self,
        team_id: int,
        team_name: str,
        workspace_id: int,
        workspace_name: str,
        project_id: int,
        project_name: str,
        project_type: str,
        dataset_id: int,
        dataset_name: str,
    ):
        self.team_id = team_id
        self.team_name = team_name
        self.workspace_id = workspace_id
        self.workspace_name = workspace_name
        self.project_id = project_id
        self.project_name = project_name
        self.project_type = project_type
        self.dataset_id = dataset_id
        self.dataset_name = dataset_name
        self.level = self.detect_level()
        self.same_src_structure = False
        self.info = self.get_info()

    def get_info(self):
        if self.level == Level.DATASET:
            return api.dataset.get_info_by_id(self.dataset_id)
        if self.level == Level.PROJECT:
            return api.project.get_info_by_id(self.project_id)
        if self.level == Level.WORKSPACE:
            return api.workspace.get_info_by_id(self.workspace_id)
        return None

    def detect_level(self):
        if self.dataset_id is not None:
            return Level.DATASET
        if self.project_id is not None:
            return Level.PROJECT
        if self.workspace_id is not None:
            return Level.WORKSPACE
        if self.team_id is not None:
            return Level.TEAM
        return None

    def from_dict(d: Dict[Any, Dict]):
        return Destination(
            d.get(JSONKEYS.TEAM, {}).get(JSONKEYS.ID, None),
            d.get(JSONKEYS.TEAM, {}).get(JSONKEYS.NAME, None),
            d.get(JSONKEYS.WORKSPACE, {}).get(JSONKEYS.ID, None),
            d.get(JSONKEYS.WORKSPACE, {}).get(JSONKEYS.NAME, None),
            d.get(JSONKEYS.PROJECT, {}).get(JSONKEYS.ID, None),
            d.get(JSONKEYS.PROJECT, {}).get(JSONKEYS.NAME, None),
            d.get(JSONKEYS.PROJECT, {}).get(JSONKEYS.TYPE, None),
            d.get(JSONKEYS.DATASET, {}).get(JSONKEYS.ID, None),
            d.get(JSONKEYS.DATASET, {}).get(JSONKEYS.NAME, None),
        )


class Source:

    def __init__(
        self,
        team_id: int,
        team_name: str,
        workspace_id: int,
        workspace_name: str,
        project_id: int,
        project_name: str,
        project_type: str,
        dataset_id: int,
        dataset_name: str,
        items: List[Dict] = None,
    ):
        self.team_id = team_id
        self.team_name = team_name
        self.workspace_id = workspace_id
        self.workspace_name = workspace_name
        self.project_id = project_id
        self.project_name = project_name
        self.project_type = project_type
        self.dataset_id = dataset_id
        self.dataset_name = dataset_name
        self.items = items
        self.level = self.detect_level()
        self.info = self.get_info()

    def get_info(self):
        if self.level == Level.DATASET:
            return api.dataset.get_info_by_id(self.dataset_id)
        if self.level == Level.PROJECT:
            return api.project.get_info_by_id(self.project_id)
        return None

    def detect_level(self):
        if self.dataset_id is not None:
            return Level.DATASET
        if self.project_id is not None:
            return Level.PROJECT
        return None

    def from_dict(d: Dict[Any, Dict]):
        return Source(
            d.get(JSONKEYS.TEAM, {}).get(JSONKEYS.ID, None),
            d.get(JSONKEYS.TEAM, {}).get(JSONKEYS.NAME, None),
            d.get(JSONKEYS.WORKSPACE, {}).get(JSONKEYS.ID, None),
            d.get(JSONKEYS.WORKSPACE, {}).get(JSONKEYS.NAME, None),
            d.get(JSONKEYS.PROJECT, {}).get(JSONKEYS.ID, None),
            d.get(JSONKEYS.PROJECT, {}).get(JSONKEYS.NAME, None),
            d.get(JSONKEYS.PROJECT, {}).get(JSONKEYS.TYPE, None),
            d.get(JSONKEYS.DATASET, {}).get(JSONKEYS.ID, None),
            d.get(JSONKEYS.DATASET, {}).get(JSONKEYS.NAME, None),
            d.get(JSONKEYS.ITEMS, []),
        )


class Options:
    """
    Options for the cloning process.
    """

    def __init__(
        self,
        preserve_src_date: bool,
        clone_annotations: bool,
        conflict_resolution_mode: str,
        preserve_structure: bool = True,
    ):
        """
        :param preserve_src_date: Preserve the source date of the items.
        :type preserve_src_date: bool
        :param clone_annotations: Clone the annotations of the items.
        :type clone_annotations: bool
        :param conflict_resolution_mode: The conflict resolution mode.
        :type conflict_resolution_mode: str
        :param preserve_structure: Preserve the source structure of the items.
        :type preserve_structure: bool"""
        self.preserve_src_date = preserve_src_date
        self.clone_annotations = clone_annotations
        self.conflict_resolution_mode = conflict_resolution_mode
        self.preserve_structure = preserve_structure

    def __str__(self):
        return f"preserve_structure: {self.preserve_structure}, preserve_src_date: {self.preserve_src_date}, clone_annotations: {self.clone_annotations}, conflict_resolution_mode: {self.conflict_resolution_mode}"

    def from_dict(d: Dict[Any, Any]):
        return Options(
            d.get(JSONKEYS.PRESERVE_SRC_DATE, False),
            d.get(JSONKEYS.CLONE_ANNOTATIONS, False),
            d.get(JSONKEYS.CONFLICT_RESOLUTION_MODE, JSONKEYS.CONFLICT_RENAME),
            d.get(JSONKEYS.PRESERVE_SRC_STRUCTURE, True),
        )

    def to_dict(self):
        return {
            JSONKEYS.PRESERVE_SRC_DATE: self.preserve_src_date,
            JSONKEYS.CLONE_ANNOTATIONS: self.clone_annotations,
            JSONKEYS.CONFLICT_RESOLUTION_MODE: self.conflict_resolution_mode,
            JSONKEYS.PRESERVE_SRC_STRUCTURE: self.preserve_structure,
        }


def extract_state_from_env():
    """
    This is a helper function to extract the state from the environment variables.
    The state is provided by the modal window in Supervisely.

    :State example:

         .. code-block:: json

            {
                "items": [
                    {
                        "id": 329,
                        "name": "DS2",
                        "type": "dataset"
                    }
                ],
                "action": "copy",
                "source": {
                    "team": {
                        "id": 10,
                        "name": "Team of admin"
                    },
                    "project": {
                        "id": 172,
                        "name": "Purple Collectible",
                        "type": "images"
                    },
                    "workspace": {
                        "id": 9,
                        "name": "First Workspace"
                    }
                },
                "options": {
                    "preserveSrcDate": false,
                    "cloneAnnotations": true,
                    "conflictResolutionMode": "rename"
                },
                "destination": {
                    "team": {
                        "id": 10,
                        "name": "Team of admin"
                    },
                    "project": {
                        "id": 173,
                        "name": "New Purple Collectible",
                        "type": "images"
                    },
                    "workspace": {
                        "id": 9,
                        "name": "First Workspace"
                    }
                }
            }
    """

    base = "modal.state"
    state = {}
    for key, value in os.environ.items():
        state_part = state
        if key.startswith(base):
            key = key.replace(base + ".", "")
            parts = key.split(".")
            while len(parts) > 1:
                part = parts.pop(0)
                state_part.setdefault(part, {})
                state_part = state_part[part]
            part = parts.pop(0)
            if value[0] == "[" or value.isdigit():
                state_part[part] = ast.literal_eval(value)
            elif value in ["True", "true", "False", "false"]:
                state_part[part] = value in ["True", "true"]
            else:
                state_part[part] = value
    return state


def rename(info, new_name, to_remove_info):
    api.image.rename(to_remove_info.id, to_remove_info.name + "__to_remove")
    api.image.rename(info.id, new_name)
    return info, to_remove_info


def maybe_replace(src: List, dst: List, to_rename: Dict, existing: Dict):
    if len(to_rename) == 0:
        return src, dst
    rename_tasks = []
    for dst_image_info in dst:
        if dst_image_info.name in to_rename:
            new_name = to_rename[dst_image_info.name]
            to_remove_info = existing[new_name]
            rename_tasks.append(executor.submit(rename, dst_image_info, new_name, to_remove_info))
    to_remove = []
    for task in as_completed(rename_tasks):
        _, to_remove_info = task.result()
        to_remove.append(to_remove_info)
    run_in_executor(
        api.image.remove_batch, [info.id for info in to_remove], batch_size=len(to_remove)
    )
    return src, dst


def clone_images_with_annotations(
    image_infos: List[sly.ImageInfo],
    dst_dataset_id: int,
    project_meta: sly.ProjectMeta,
    options,
    progress_cb=None,
) -> List[sly.ImageInfo]:
    existing = run_in_executor(api.image.get_list, dst_dataset_id)
    existing = {info.name: info for info in existing}
    if options[JSONKEYS.CONFLICT_RESOLUTION_MODE] == JSONKEYS.CONFLICT_SKIP:
        len_before = len(image_infos)
        image_infos = [info for info in image_infos if info.name not in existing]
        if progress_cb is not None:
            progress_cb(len_before - len(image_infos))

    if len(image_infos) == 0:
        return []

    def _copy_imgs(
        names,
        infos,
    ):
        uploaded = api_utils.images_bulk_add(
            api, dst_dataset_id, names, infos, perserve_dates=options[JSONKEYS.PRESERVE_SRC_DATE]
        )
        return infos, uploaded

    def _copy_anns(src: List[sly.ImageInfo], dst: List[sly.ImageInfo]):
        try:
            api.annotation.copy_batch_by_ids(
                [i.id for i in src],
                [i.id for i in dst],
                save_source_date=options[JSONKEYS.PRESERVE_SRC_DATE],
            )
        except Exception as e:
            if "Some users are not members of the destination group" in str(e):
                raise ValueError(
                    "Unable to copy annotations. Annotation creator is not a member of the destination team."
                ) from e
            else:
                raise e

        return src, dst

    to_rename = {}  # {new_name: old_name}
    upload_images_tasks = []
    for src_image_infos_batch in sly.batched(image_infos, UPLOAD_IMAGES_BATCH_SIZE):
        names = [info.name for info in src_image_infos_batch]
        now = datetime.now().strftime("%Y_%m_%d_%H_%M_%S")

        if options[JSONKEYS.CONFLICT_RESOLUTION_MODE] in [
            JSONKEYS.CONFLICT_RENAME,
            JSONKEYS.CONFLICT_REPLACE,
        ]:
            for i, name in enumerate(names):
                if name in existing:
                    names[i] = (
                        ".".join(name.split(".")[:-1]) + "_" + now + "." + name.split(".")[-1]
                    )
                    if options[JSONKEYS.CONFLICT_RESOLUTION_MODE] == JSONKEYS.CONFLICT_REPLACE:
                        to_rename[names[i]] = name
        upload_images_tasks.append(
            executor.submit(
                _copy_imgs,
                names=names,
                infos=src_image_infos_batch,
            )
        )

    replace_tasks = []
    local_executor = ThreadPoolExecutor(max_workers=5)
    src_id_to_dst_image_info = {}
    for task in as_completed(upload_images_tasks):
        src_image_infos, uploaded_images_infos = task.result()
        for s, d in zip(src_image_infos, uploaded_images_infos):
            src_id_to_dst_image_info[s.id] = d
        if options[JSONKEYS.CLONE_ANNOTATIONS]:
            upload_anns_tasks = []
            for src_batch, dst_batch in zip(
                sly.batched(src_image_infos), sly.batched(uploaded_images_infos)
            ):
                upload_anns_tasks.append(executor.submit(_copy_anns, src_batch, dst_batch))
            for task in as_completed(upload_anns_tasks):
                src_batch, dst_batch = task.result()
                replace_tasks.append(
                    local_executor.submit(maybe_replace, src_batch, dst_batch, to_rename, existing)
                )
        else:
            replace_tasks.append(
                local_executor.submit(
                    maybe_replace, src_image_infos, uploaded_images_infos, to_rename, existing
                )
            )

    for task in as_completed(replace_tasks):
        src, _ = task.result()
        if progress_cb is not None:
            progress_cb(len(src))

    return [src_id_to_dst_image_info[info.id] for info in image_infos]


def clone_videos_with_annotations(
    video_infos: List[sly.api.video_api.VideoInfo],
    dst_dataset_id: int,
    project_meta: sly.ProjectMeta,
    options,
    progress_cb=None,
) -> List[sly.api.video_api.VideoInfo]:
    if len(video_infos) == 0:
        return []

    existing = run_in_executor(api.video.get_list, dst_dataset_id)
    existing = {info.name: info for info in existing}
    if options[JSONKEYS.CONFLICT_RESOLUTION_MODE] == JSONKEYS.CONFLICT_SKIP:
        len_before = len(video_infos)
        video_infos = [info for info in video_infos if info.name not in existing]
        if progress_cb is not None:
            progress_cb(len_before - len(video_infos))

    if len(video_infos) == 0:
        return []

    def _copy_videos(
        names: List[str],
        ids: List[int],
        metas: List[Dict],
        infos: List[sly.api.video_api.VideoInfo],
    ):
        uploaded = api.video.upload_ids(
            dst_dataset_id,
            names=names,
            ids=ids,
            metas=metas,
            infos=infos,
        )
        return infos, uploaded

    def _copy_anns(src: List[sly.api.video_api.VideoInfo], dst: List[sly.api.video_api.VideoInfo]):
        anns_jsons = run_in_executor(
            api.video.annotation.download_bulk, src_dataset_id, [info.id for info in src]
        )
        dst_ids = [info.id for info in dst]
        tasks = []
        for ann_json, dst_id in zip(anns_jsons, dst_ids):
            key_id_map = sly.KeyIdMap()
            ann = sly.VideoAnnotation.from_json(ann_json, project_meta, key_id_map)
            tasks.append(executor.submit(api.video.annotation.append, dst_id, ann, key_id_map))
        for task in as_completed(tasks):
            task.result()
        return src, dst

    def _maybe_copy_anns_and_replace(src, dst):
        if options[JSONKEYS.CLONE_ANNOTATIONS]:
            src, dst = _copy_anns(src, dst)
        return maybe_replace(src, dst, to_rename, existing)

    src_dataset_id = video_infos[0].dataset_id
    to_rename = {}
    upload_videos_tasks = []
    for src_video_infos in sly.batched(video_infos):
        names = [info.name for info in src_video_infos]
        ids = [info.id for info in src_video_infos]
        metas = [info.meta for info in src_video_infos]
        now = datetime.now().strftime("%Y_%m_%d_%H_%M_%S")

        if options[JSONKEYS.CONFLICT_RESOLUTION_MODE] in [
            JSONKEYS.CONFLICT_RENAME,
            JSONKEYS.CONFLICT_REPLACE,
        ]:
            for i, name in enumerate(names):
                if name in existing:
                    names[i] = (
                        ".".join(name.split(".")[:-1]) + "_" + now + "." + name.split(".")[-1]
                    )
                    if options[JSONKEYS.CONFLICT_RESOLUTION_MODE] == JSONKEYS.CONFLICT_REPLACE:
                        to_rename[names[i]] = name

        upload_videos_tasks.append(
            executor.submit(
                _copy_videos,
                names=names,
                ids=ids,
                metas=metas,
                infos=src_video_infos,
            )
        )

    local_executor = ThreadPoolExecutor(max_workers=5)
    replace_tasks = []
    src_id_to_dst_info = {}
    for task in as_completed(upload_videos_tasks):
        src_video_infos, uploaded_video_infos = task.result()
        for s, d in zip(src_video_infos, uploaded_video_infos):
            src_id_to_dst_info[s.id] = d
        replace_tasks.append(
            local_executor.submit(
                _maybe_copy_anns_and_replace, src_video_infos, uploaded_video_infos
            )
        )

    for task in as_completed(replace_tasks):
        src, _ = task.result()
        if progress_cb is not None:
            progress_cb(len(src))

    return [src_id_to_dst_info[info.id] for info in video_infos]


def clone_volumes_with_annotations(
    volume_infos: List[sly.api.volume_api.VolumeInfo],
    dst_dataset_id: int,
    project_meta: sly.ProjectMeta,
    options,
    progress_cb=None,
) -> List[sly.api.volume_api.VolumeInfo]:
    existing = run_in_executor(api.volume.get_list, dst_dataset_id)
    existing = {info.name: info for info in existing}
    if options[JSONKEYS.CONFLICT_RESOLUTION_MODE] == JSONKEYS.CONFLICT_SKIP:
        volume_infos = [info for info in volume_infos if info.name not in existing]

    if len(volume_infos) == 0:
        return []

    def _copy_volumes(
        names: List[str],
        hashes: List[str],
        metas: List[Dict],
        infos: List[sly.api.volume_api.VolumeInfo],
    ):
        uploaded = api.volume.upload_hashes(
            dataset_id=dst_dataset_id,
            names=names,
            hashes=hashes,
            metas=metas,
        )
        return infos, uploaded

    def _copy_anns(
        src: List[sly.api.volume_api.VolumeInfo], dst: List[sly.api.volume_api.VolumeInfo]
    ):
        ann_jsons = run_in_executor(
            api.volume.annotation.download_bulk, src_dataset_id, [info.id for info in src]
        )
        tasks = []
        for ann_json, dst_info in zip(ann_jsons, dst):
            key_id_map = sly.KeyIdMap()
            ann = sly.VolumeAnnotation.from_json(ann_json, project_meta, key_id_map)
            tasks.append(
                executor.submit(
                    api.volume.annotation.append, dst_info.id, ann, key_id_map, volume_info=dst_info
                )
            )
        for task in as_completed(tasks):
            task.result()
        return src, dst

    def _maybe_copy_anns_and_replace(src, dst):
        if options[JSONKEYS.CLONE_ANNOTATIONS]:
            src, dst = _copy_anns(src, dst)
        return maybe_replace(src, dst, to_rename, existing)

    src_dataset_id = volume_infos[0].dataset_id
    to_rename = {}
    upload_volumes_tasks = []
    for src_volume_infos in sly.batched(volume_infos):
        names = [info.name for info in src_volume_infos]
        hashes = [info.hash for info in src_volume_infos]
        metas = [info.meta for info in src_volume_infos]
        now = datetime.now().strftime("%Y_%m_%d_%H_%M_%S")

        if options[JSONKEYS.CONFLICT_RESOLUTION_MODE] in [
            JSONKEYS.CONFLICT_RENAME,
            JSONKEYS.CONFLICT_REPLACE,
        ]:
            for i, name in enumerate(names):
                if name in existing:
                    names[i] = (
                        ".".join(name.split(".")[:-1]) + "_" + now + "." + name.split(".")[-1]
                    )
                    if options[JSONKEYS.CONFLICT_RESOLUTION_MODE] == JSONKEYS.CONFLICT_REPLACE:
                        to_rename[names[i]] = name

        upload_volumes_tasks.append(
            executor.submit(
                _copy_volumes,
                names=names,
                hashes=hashes,
                metas=metas,
                infos=src_volume_infos,
            )
        )

    local_executor = ThreadPoolExecutor(max_workers=5)
    replace_tasks = []
    src_id_to_dst_info = {}
    for task in as_completed(upload_volumes_tasks):
        src_volume_infos, uploaded_volume_infos = task.result()
        for s, d in zip(src_volume_infos, uploaded_volume_infos):
            src_id_to_dst_info[s.id] = d
        replace_tasks.append(
            local_executor.submit(
                _maybe_copy_anns_and_replace, src_volume_infos, uploaded_volume_infos
            )
        )

    for task in as_completed(replace_tasks):
        src, _ = task.result()
        if progress_cb is not None:
            progress_cb(len(src))

    return [src_id_to_dst_info[info.id] for info in volume_infos]


def clone_pointclouds_with_annotations(
    pointcloud_infos: List[sly.api.pointcloud_api.PointcloudInfo],
    dst_dataset_id: int,
    project_meta: sly.ProjectMeta,
    options,
    progress_cb=None,
) -> List[sly.api.pointcloud_api.PointcloudInfo]:
    existing = api.pointcloud.get_list(dst_dataset_id)
    existing = {info.name: info for info in existing}
    if options[JSONKEYS.CONFLICT_RESOLUTION_MODE] == JSONKEYS.CONFLICT_SKIP:
        pointcloud_infos = [info for info in pointcloud_infos if info.name not in existing]

    if len(pointcloud_infos) == 0:
        return []

    def _copy_pointclouds(names, hashes, metas, infos):
        uploaded = api.pointcloud.upload_hashes(
            dataset_id=dst_dataset_id,
            names=names,
            hashes=hashes,
            metas=metas,
        )
        return infos, uploaded

    def _copy_anns(src, dst):
        src_ids = [info.id for info in src]
        dst_ids = [info.id for info in dst]
        ann_jsons = run_in_executor(
            api.pointcloud.annotation.download_bulk, src_dataset_id, src_ids
        )
        tasks = []
        for ann_json, dst_id in zip(ann_jsons, dst_ids):
            key_id_map = sly.KeyIdMap()
            ann = sly.PointcloudAnnotation.from_json(ann_json, project_meta, key_id_map)
            tasks.append(executor.submit(api.pointcloud.annotation.append, dst_id, ann, key_id_map))
        for task in as_completed(tasks):
            task.result()
        return src, dst

    def _maybe_copy_anns_and_replace(src, dst):
        if options[JSONKEYS.CLONE_ANNOTATIONS]:
            src, dst = _copy_anns(src, dst)
        return maybe_replace(src, dst, to_rename, existing)

    src_dataset_id = pointcloud_infos[0].dataset_id
    copy_pointcloud_tasks = []
    to_rename = {}
    for src_pcd_infos in sly.batched(pointcloud_infos):
        names = [info.name for info in src_pcd_infos]
        hashes = [info.hash for info in src_pcd_infos]
        metas = [info.meta for info in src_pcd_infos]
        now = datetime.now().strftime("%Y_%m_%d_%H_%M_%S")
        if options[JSONKEYS.CONFLICT_RESOLUTION_MODE] in [
            JSONKEYS.CONFLICT_RENAME,
            JSONKEYS.CONFLICT_REPLACE,
        ]:
            for i, name in enumerate(names):
                if name in existing:
                    names[i] = (
                        ".".join(name.split(".")[:-1]) + "_" + now + "." + name.split(".")[-1]
                    )
                    if options[JSONKEYS.CONFLICT_RESOLUTION_MODE] == JSONKEYS.CONFLICT_REPLACE:
                        to_rename[names[i]] = name

        copy_pointcloud_tasks.append(
            executor.submit(
                _copy_pointclouds,
                names=names,
                hashes=hashes,
                metas=metas,
                infos=src_pcd_infos,
            )
        )
    local_executor = ThreadPoolExecutor(max_workers=5)
    replace_tasks = []
    src_id_to_dst_info = {}
    for task in as_completed(copy_pointcloud_tasks):
        src_pcd_infos, uploaded_pcd_infos = task.result()
        for s, d in zip(src_pcd_infos, uploaded_pcd_infos):
            src_id_to_dst_info[s.id] = d
        replace_tasks.append(
            local_executor.submit(_maybe_copy_anns_and_replace, src_pcd_infos, uploaded_pcd_infos)
        )

    for task in as_completed(replace_tasks):
        src, _ = task.result()
        if progress_cb is not None:
            progress_cb(len(src))

    return [src_id_to_dst_info[info.id] for info in pointcloud_infos]


def clone_pointcloud_episodes_with_annotations(
    pointcloud_episode_infos: List[sly.api.pointcloud_api.PointcloudInfo],
    dst_dataset_id: int,
    project_meta: sly.ProjectMeta,
    options,
    progress_cb=None,
) -> List[sly.api.pointcloud_api.PointcloudInfo]:
    existing = api.pointcloud_episode.get_list(dst_dataset_id)
    existing = {info.name: info for info in existing}
    if options[JSONKEYS.CONFLICT_RESOLUTION_MODE] == JSONKEYS.CONFLICT_SKIP:
        pointcloud_episode_infos = [
            info for info in pointcloud_episode_infos if info.name not in existing
        ]

    if len(pointcloud_episode_infos) == 0:
        return []

    src_dataset_id = pointcloud_episode_infos[0].dataset_id
    frame_to_pointcloud_ids = {}

    def _upload_hashes(infos):
        names = [info.name for info in infos]
        hashes = [info.hash for info in infos]
        metas = [info.meta for info in infos]
        now = datetime.now().strftime("%Y_%m_%d_%H_%M_%S")
        to_remove = []
        to_rename = {}
        for i, name in enumerate(names):
            if name in existing:
                if options[JSONKEYS.CONFLICT_RESOLUTION_MODE] == JSONKEYS.CONFLICT_RENAME:
                    names[i] = (
                        ".".join(name.split(".")[:-1]) + "_" + now + "." + name.split(".")[-1]
                    )
                elif options[JSONKEYS.CONFLICT_RESOLUTION_MODE] == JSONKEYS.CONFLICT_REPLACE:
                    names[i] = (
                        ".".join(name.split(".")[:-1]) + "_" + now + "." + name.split(".")[-1]
                    )
                    to_remove.append(name)
                    to_rename[names[i]] = name
        dst_infos = api.pointcloud_episode.upload_hashes(
            dataset_id=dst_dataset_id,
            names=names,
            hashes=hashes,
            metas=metas,
        )
        if to_remove:
            rm_ids = [info.id for info in existing if info.name in to_remove]
            run_in_executor(api.image.remove_batch, rm_ids)
        if to_rename:
            rename_tasks = []
            for dst_info in dst_infos:
                if dst_info.name in to_rename:
                    rename_tasks.append(
                        executor.submit(api.image.edit, dst_info.id, name=to_rename[dst_info.name])
                    )
            for task in as_completed(rename_tasks):
                info = task.result()
                dst_infos = [info if info.id == dst_info.id else dst_info for dst_info in dst_infos]
        return {src.id: dst for src, dst in zip(infos, dst_infos)}

    def _upload_single(src_id, dst_info):
        frame_to_pointcloud_ids[dst_info.meta["frame"]] = dst_info.id

        rel_images = api.pointcloud_episode.get_list_related_images(id=src_id)
        if len(rel_images) != 0:
            rimg_infos = []
            for rel_img in rel_images:
                rimg_infos.append(
                    {
                        sly.api.ApiField.ENTITY_ID: dst_info.id,
                        sly.api.ApiField.NAME: rel_img[sly.api.ApiField.NAME],
                        sly.api.ApiField.HASH: rel_img[sly.api.ApiField.HASH],
                        sly.api.ApiField.META: rel_img[sly.api.ApiField.META],
                    }
                )
            api.pointcloud_episode.add_related_images(rimg_infos)

        if progress_cb is not None:
            progress_cb()

    copy_imgs_tasks = []
    for batch in sly.batched(pointcloud_episode_infos):
        copy_imgs_tasks.append(executor.submit(_upload_hashes, batch))

    dst_infos_dict = {}
    if not options[JSONKEYS.CLONE_ANNOTATIONS]:
        for task in as_completed(copy_imgs_tasks):
            src_to_dst_dict = task.result()
            dst_infos_dict.update(src_to_dst_dict)
            if progress_cb is not None:
                progress_cb(len(dst_infos_dict))
        return [dst_infos_dict[src.id] for src in pointcloud_episode_infos]

    key_id_map = sly.KeyIdMap()
    ann_json = api.pointcloud_episode.annotation.download(src_dataset_id)
    ann = sly.PointcloudEpisodeAnnotation.from_json(
        data=ann_json, project_meta=project_meta, key_id_map=key_id_map
    )

    copy_anns_tasks = []
    for task in as_completed(copy_imgs_tasks):
        src_to_dst_dict = task.result()
        dst_infos_dict.update(src_to_dst_dict)
        if options[JSONKEYS.CLONE_ANNOTATIONS]:
            for src_id, dst_info in src_to_dst_dict.items():
                copy_anns_tasks.append(executor.submit(_upload_single, src_id, dst_info))

    for task in as_completed(copy_anns_tasks):
        task.result()
    api.pointcloud_episode.annotation.append(
        dataset_id=dst_dataset_id,
        ann=ann,
        frame_to_pointcloud_ids=frame_to_pointcloud_ids,
        key_id_map=key_id_map,
    )
    return [dst_infos_dict[src.id] for src in pointcloud_episode_infos]


def clone_items(
    src_dataset_id,
    dst_dataset_id,
    project_type,
    project_meta,
    options,
    progress_cb=None,
    src_infos=None,
):
    if project_type == str(sly.ProjectType.IMAGES):
        if src_infos is None:
            src_infos = run_in_executor(api_utils.images_get_list, api, src_dataset_id)
        clone_f = clone_images_with_annotations
    elif project_type == str(sly.ProjectType.VIDEOS):
        if src_infos is None:
            src_infos = run_in_executor(api.video.get_list, src_dataset_id)
        clone_f = clone_videos_with_annotations
    elif project_type == str(sly.ProjectType.VOLUMES):
        if src_infos is None:
            src_infos = run_in_executor(api.volume.get_list, src_dataset_id)
        clone_f = clone_volumes_with_annotations
    elif project_type == str(sly.ProjectType.POINT_CLOUDS):
        if src_infos is None:
            src_infos = run_in_executor(api.pointcloud.get_list, src_dataset_id)
        clone_f = clone_pointclouds_with_annotations
    elif project_type == str(sly.ProjectType.POINT_CLOUD_EPISODES):
        if src_infos is None:
            src_infos = run_in_executor(api.pointcloud_episode.get_list, src_dataset_id)
        clone_f = clone_pointcloud_episodes_with_annotations
    else:
        raise NotImplementedError(
            "Cloning for project type {} is not implemented".format(project_type)
        )

    dst_infos = clone_f(src_infos, dst_dataset_id, project_meta, options, progress_cb)
    logger.info(
        "Cloned %d items",
        len(dst_infos),
        extra={"src_dataset_id": src_dataset_id, "dst_dataset_id": dst_dataset_id},
    )
    return dst_infos


def create_dataset_recursively(
    project_type: sly.ProjectType,
    project_meta: sly.ProjectMeta,
    dataset_info: sly.DatasetInfo,
    children: Dict[sly.DatasetInfo, Dict],
    dst_project_id: int,
    dst_dataset_id: int,
    options: Dict,
    should_clone_items: bool = True,
    progress_cb=None,
):
    logger.info(
        "Creating dataset and its children",
        extra={
            "dataset_name": dataset_info.name,
            "children": [child.name for child in children],
            "destination_dataset": dst_dataset_id,
        },
    )
    tasks_queue = Queue()
    local_executor = ThreadPoolExecutor()
    perserve_date = options.get(JSONKEYS.PRESERVE_SRC_DATE, False)

    def _create_rec(
        dataset_info: sly.DatasetInfo, children: Dict[sly.DatasetInfo, Dict], dst_parent_id: int
    ):
        created_id = None
        created_info = None
        if dataset_info is not None:
            if options[JSONKEYS.CONFLICT_RESOLUTION_MODE] == JSONKEYS.CONFLICT_SKIP:
                existing = run_in_executor(
                    api.dataset.get_list, dst_project_id, parent_id=dst_parent_id
                )
                if any(ds.name == dataset_info.name for ds in existing):
                    return
            created_info = run_in_executor(
                api_utils.create_dataset,
                api,
                dst_project_id,
                dataset_info.name,
                dataset_info.description,
                change_name_if_conflict=True,
                parent_id=dst_parent_id,
                created_at=dataset_info.created_at if perserve_date else None,
                updated_at=dataset_info.updated_at if perserve_date else None,
                created_by=dataset_info.created_by if perserve_date else None,
            )

            created_id = created_info.id
            if should_clone_items:
                clone_items(
                    dataset_info.id, created_id, project_type, project_meta, options, progress_cb
                )
            if options[JSONKEYS.CONFLICT_RESOLUTION_MODE] == JSONKEYS.CONFLICT_REPLACE:
                if created_info.name != dataset_info.name:
                    existing = run_in_executor(
                        api.dataset.get_info_by_name,
                        dst_project_id,
                        name=dataset_info.name,
                        parent_id=dst_dataset_id,
                    )
                    created_info = run_in_executor(replace_dataset, existing, created_info)
            # to update items count
            created_info = run_in_executor(api.dataset.get_info_by_id, created_id)
            logger.info(
                "Created Dataset",
                extra={
                    "dataset_id": created_id,
                    "dataset_name": created_info.name,
                    "items_count": created_info.items_count,
                },
            )

        if children is None:
            return created_info
        for child, subchildren in children.items():
            tasks_queue.put(local_executor.submit(_create_rec, child, subchildren, created_id))
        return created_info

    def _consume():
        while not tasks_queue.empty():
            task = tasks_queue.get()
            results.append(task.result())

    results = [
        _create_rec(
            dataset_info,
            children,
            dst_dataset_id,
        )
    ]
    _consume()
    local_executor.shutdown()
    return results


def create_project(
    src_project_info: sly.ProjectInfo, dst_workspace_id: int, project_type: str, options: Dict
) -> Tuple[sly.ProjectInfo, sly.ProjectMeta]:
    created_at = None
    created_by = None
    updated_at = None
    if options.get(JSONKEYS.PRESERVE_SRC_DATE, False):
        created_at = src_project_info.created_at
        created_by = src_project_info.created_by_id
        updated_at = src_project_info.updated_at
    dst_project_info = api_utils.create_project(
        api,
        dst_workspace_id,
        src_project_info.name,
        type=project_type,
        description=src_project_info.description,
        settings=src_project_info.settings,
        custom_data=src_project_info.custom_data,
        readme=src_project_info.readme,
        change_name_if_conflict=True,
        created_at=created_at,
        updated_at=updated_at,
        created_by=created_by,
    )
    logger.info(
        "Created project",
        extra={"project_id": dst_project_info.id, "project_name": dst_project_info.name},
    )
    return dst_project_info


def merge_project_meta(src_project_id, dst_project_id):
    src_project_meta = sly.ProjectMeta.from_json(api.project.get_meta(src_project_id))
    if src_project_id == dst_project_id:
        return src_project_meta
    dst_project_meta = sly.ProjectMeta.from_json(api.project.get_meta(dst_project_id))
    changed = False
    for obj_class in src_project_meta.obj_classes:
        dst_obj_class: sly.ObjClass = dst_project_meta.obj_classes.get(obj_class.name)
        if dst_obj_class is None:
            dst_project_meta = dst_project_meta.add_obj_class(obj_class)
            changed = True
        elif dst_obj_class.geometry_type != obj_class.geometry_type:
            dst_obj_class = dst_obj_class.clone(geometry_type=sly.AnyGeometry)
            dst_project_meta = dst_project_meta.delete_obj_class(obj_class.name)
            dst_project_meta = dst_project_meta.add_obj_class(dst_obj_class)
            changed = True
    for tag_meta in src_project_meta.tag_metas:
        dst_tag_meta = dst_project_meta.get_tag_meta(tag_meta.name)
        if dst_tag_meta is None:
            dst_project_meta = dst_project_meta.add_tag_meta(tag_meta)
            changed = True
        elif dst_tag_meta.value_type != tag_meta.value_type:
            raise ValueError("Tag Metas are incompatible")
        elif dst_tag_meta.possible_values != tag_meta.possible_values:
            all_possible_values = list(set(dst_tag_meta.possible_values + tag_meta.possible_values))
            dst_tag_meta = dst_tag_meta.clone(possible_values=all_possible_values)
            dst_project_meta = dst_project_meta.delete_tag_meta(tag_meta.name)
            dst_project_meta = dst_project_meta.add_tag_meta(dst_tag_meta)
            changed = True
    return (
        api.project.update_meta(dst_project_id, dst_project_meta) if changed else dst_project_meta
    )


def _find_tree(tree: Dict, src_dataset_ids: List[int]):
    if any(ds.id in src_dataset_ids for ds in tree):
        return {k: v for k, v in tree.items() if k.id in src_dataset_ids}
    for children in tree.values():
        found = _find_tree(children, src_dataset_ids)
        if found is not None:
            return found


def _count_items_in_tree(tree):
    count = 0
    for dataset, children in tree.items():
        count += dataset.items_count
        count += _count_items_in_tree(children)
    return count


def _get_all_parents(dataset: sly.DatasetInfo, dataset_infos: List[sly.DatasetInfo]):
    if dataset.parent_id is None:
        return []
    for dataset_info in dataset_infos:
        if dataset_info.id == dataset.parent_id:
            return [dataset_info] + _get_all_parents(dataset_info, dataset_infos)
    return []


def flatten_tree(tree: Dict):
    result = []

    def _dfs(tree: Dict):
        for ds in sorted(tree.keys(), key=lambda obj: obj.name):
            children = tree[ds]
            result.append(ds)
            _dfs(children)

    _dfs(tree)
    return result


def flatten_tree_by_map(tree: Dict, map: Dict):
    result = []

    def _dfs(tree: Dict):
        for ds in sorted(tree.keys(), key=lambda obj: map[obj].name):
            children = tree[ds]
            result.append(map[ds])
            _dfs(children)

    _dfs(tree)
    return result


def tree_from_list(datasets: List[sly.DatasetInfo]) -> Dict[int, Dict]:
    parent_map = {}
    ds_ids = [ds.id for ds in datasets]
    # Grouping datasets by parent_id
    for dataset in datasets:
        parent_map.setdefault(dataset.parent_id, []).append(dataset)

    def build_tree(parent_id: int) -> Dict[int, Dict]:
        return {dataset.id: build_tree(dataset.id) for dataset in parent_map.get(parent_id, [])}

    # Find root nodes (datasets whose parent_id is not in the list of ids)
    root_nodes = [dataset for dataset in datasets if dataset.parent_id not in ds_ids]

    # Build the tree starting from root nodes
    tree = {}
    for root in root_nodes:
        tree[root.id] = build_tree(root.id)

    return tree


def find_children_in_tree(tree: Dict, parent_id: int):
    return [ds for ds in flatten_tree(tree) if ds.parent_id == parent_id]


def replace_project(src_project_info: sly.ProjectInfo, dst_project_info: sly.ProjectInfo):
    """Remove src_rpoject_info and change name of dst_project_info to src_project_info.name"""
    api.project.update(src_project_info.id, name=src_project_info.name + "__to_remove")
    api.project.remove(src_project_info.id)
    return api.project.update(dst_project_info.id, name=src_project_info.name)


def replace_dataset(src_dataset_info: sly.DatasetInfo, dst_dataset_info: sly.DatasetInfo):
    """Remove src_dataset_info and change name of dst_dataset_info to src_dataset_info.name"""
    api.dataset.update(src_dataset_info.id, name=src_dataset_info.name + "__to_remove")
    api.dataset.remove(src_dataset_info.id)
    return api.dataset.update(dst_dataset_info.id, name=src_dataset_info.name)


def run_in_executor(func, *args, **kwargs):
    return executor.submit(func, *args, **kwargs).result()


def copy_project_with_replace(
    src_project_info: sly.ProjectInfo,
    dst_workspace_id: int,
    dst_project_id: int,
    dst_dataset_id: int,
    options: Dict,
    progress_cb=None,
    existing_projects=None,
    datasets_tree=None,
):
    if dst_project_id is None and dst_workspace_id == src_project_info.workspace_id:
        logger.warning(
            "Copying project to the same workspace with replace. Skipping",
            extra={"project_id": src_project_info.id},
        )
        progress_cb(src_project_info.items_count)
        return []
    perserve_date = options.get(JSONKEYS.PRESERVE_SRC_DATE, False)
    project_type = src_project_info.type
    created_datasets = []
    if datasets_tree is None:
        datasets_tree = run_in_executor(api.dataset.get_tree, src_project_info.id)
    if dst_project_id is not None:
        # copy project to existing project or existing dataset
        project_meta = merge_project_meta(src_project_info.id, dst_project_id)
        created_dataset = run_in_executor(
            api.dataset.create,
            dst_project_id,
            src_project_info.name,
            src_project_info.description,
            change_name_if_conflict=True,
            parent_id=dst_dataset_id,
            created_at=src_project_info.created_at if perserve_date else None,
            updated_at=src_project_info.updated_at if perserve_date else None,
            created_by=src_project_info.created_by_id if perserve_date else None,
        )
        existing_datasets = find_children_in_tree(datasets_tree, parent_id=dst_dataset_id)
        for ds, children in datasets_tree.items():
            created_datasets.extend(
                create_dataset_recursively(
                    project_type=project_type,
                    project_meta=project_meta,
                    dataset_info=ds,
                    children=children,
                    dst_project_id=dst_project_id,
                    dst_dataset_id=created_dataset.id,
                    options=options,
                    progress_cb=progress_cb,
                )
            )
        if src_project_info.name in [ds.name for ds in existing_datasets]:
            existing = [ds for ds in existing_datasets if ds.name == src_project_info.name][0]
            run_in_executor(replace_dataset, existing, created_dataset)
    else:
        if existing_projects is None:
            existing_projects = run_in_executor(api.project.get_list, dst_workspace_id)
        created_project = run_in_executor(
            create_project,
            src_project_info,
            dst_workspace_id,
            project_type=project_type,
            options=options,
        )
        project_meta = run_in_executor(api.project.get_meta, src_project_info.id)
        project_meta = sly.ProjectMeta.from_json(project_meta)
        run_in_executor(api.project.update_meta, created_project.id, project_meta)
        for ds, children in datasets_tree.items():
            created_datasets.extend(
                create_dataset_recursively(
                    project_type=project_type,
                    project_meta=project_meta,
                    dataset_info=ds,
                    children=children,
                    dst_project_id=created_project.id,
                    dst_dataset_id=None,
                    options=options,
                    progress_cb=progress_cb,
                )
            )
        if src_project_info.name in [pr.name for pr in existing_projects]:
            existing = [pr for pr in existing_projects if pr.name == src_project_info.name][0]
            logger.info(
                "Replacing project",
                extra={"existing_project_id": existing.id, "new_project_id": created_project.id},
            )
            run_in_executor(replace_project, existing, created_project)
    return created_datasets


def copy_project_with_skip(
    src_project_info: sly.ProjectInfo,
    dst_workspace_id: int,
    dst_project_id: int,
    dst_dataset_id: int,
    options: Dict,
    progress_cb=None,
    existing_projects=None,
    datasets_tree=None,
):
    perserve_date = options.get(JSONKEYS.PRESERVE_SRC_DATE, False)
    project_type = src_project_info.type
    created_datasets = []
    if dst_project_id is not None:
        if datasets_tree is None:
            datasets_tree = run_in_executor(api.dataset.get_tree, src_project_info.id)
        existing_datasets = find_children_in_tree(datasets_tree, parent_id=dst_dataset_id)
        if src_project_info.name in [ds.name for ds in existing_datasets]:
            progress_cb(src_project_info.items_count)
            logger.info("Dataset with the same name already exists. Skipping")
            return []
        project_meta = run_in_executor(merge_project_meta, src_project_info.id, dst_project_id)
        created_dataset = run_in_executor(
            api.dataset.create,
            dst_project_id,
            src_project_info.name,
            src_project_info.description,
            change_name_if_conflict=True,
            parent_id=dst_dataset_id,
            created_at=src_project_info.created_at if perserve_date else None,
            updated_at=src_project_info.updated_at if perserve_date else None,
            created_by=src_project_info.created_by_id if perserve_date else None,
        )
        for ds, children in datasets_tree.items():
            created_datasets.extend(
                create_dataset_recursively(
                    project_type=project_type,
                    project_meta=project_meta,
                    dataset_info=ds,
                    children=children,
                    dst_project_id=dst_project_id,
                    dst_dataset_id=created_dataset.id,
                    options=options,
                    progress_cb=progress_cb,
                )
            )
    else:
        if src_project_info.name in [pr.name for pr in existing_projects]:
            progress_cb(src_project_info.items_count)
            logger.info("Project with the same name already exists. Skipping")
            return []
        created_project = run_in_executor(
            create_project,
            src_project_info,
            dst_workspace_id,
            project_type=project_type,
            options=options,
        )
        project_meta = run_in_executor(api.project.get_meta, src_project_info.id)
        project_meta = sly.ProjectMeta.from_json(project_meta)
        run_in_executor(api.project.update_meta, created_project.id, project_meta)
        if datasets_tree is None:
            datasets_tree = run_in_executor(api.dataset.get_tree, src_project_info.id)
        for ds, children in datasets_tree.items():
            created_datasets.extend(
                create_dataset_recursively(
                    project_type=project_type,
                    project_meta=project_meta,
                    dataset_info=ds,
                    children=children,
                    dst_project_id=created_project.id,
                    dst_dataset_id=None,
                    options=options,
                    progress_cb=progress_cb,
                )
            )
    return created_datasets


def copy_project(
    src_project_info: sly.ProjectInfo,
    dst_workspace_id: int,
    dst_project_id: int,
    dst_dataset_id: int,
    options: Dict,
    progress_cb=None,
    existing_projects=None,
    datasets_tree=None,
):
    if datasets_tree is None:
        datasets_tree = run_in_executor(api.dataset.get_tree, src_project_info.id)
    if options[JSONKEYS.CONFLICT_RESOLUTION_MODE] == JSONKEYS.CONFLICT_REPLACE:
        return copy_project_with_replace(
            src_project_info,
            dst_workspace_id,
            dst_project_id,
            dst_dataset_id,
            options,
            progress_cb,
            existing_projects,
            datasets_tree,
        )
    if options[JSONKEYS.CONFLICT_RESOLUTION_MODE] == JSONKEYS.CONFLICT_SKIP:
        return copy_project_with_skip(
            src_project_info,
            dst_workspace_id,
            dst_project_id,
            dst_dataset_id,
            options,
            progress_cb,
            existing_projects,
            datasets_tree,
        )
    perserve_date = options.get(JSONKEYS.PRESERVE_SRC_DATE, False)
    project_type = src_project_info.type
    created_datasets = []
    if dst_project_id is not None:
        project_meta = run_in_executor(merge_project_meta, src_project_info.id, dst_project_id)
        created_dataset = run_in_executor(
            api.dataset.create,
            dst_project_id,
            src_project_info.name,
            src_project_info.description,
            change_name_if_conflict=True,
            parent_id=dst_dataset_id,
            created_at=src_project_info.created_at if perserve_date else None,
            updated_at=src_project_info.updated_at if perserve_date else None,
            created_by=src_project_info.created_by_id if perserve_date else None,
        )
        for ds, children in datasets_tree.items():
            created_datasets.extend(
                create_dataset_recursively(
                    project_type=project_type,
                    project_meta=project_meta,
                    dataset_info=ds,
                    children=children,
                    dst_project_id=dst_project_id,
                    dst_dataset_id=created_dataset.id,
                    options=options,
                    progress_cb=progress_cb,
                )
            )
    else:
        created_project = run_in_executor(
            create_project,
            src_project_info,
            dst_workspace_id,
            project_type=project_type,
            options=options,
        )
        project_meta = run_in_executor(api.project.get_meta, src_project_info.id)
        project_meta = sly.ProjectMeta.from_json(project_meta)
        run_in_executor(api.project.update_meta, created_project.id, project_meta)
        datasets_tree = run_in_executor(api.dataset.get_tree, src_project_info.id)
        for ds, children in datasets_tree.items():
            created_datasets.extend(
                create_dataset_recursively(
                    project_type=project_type,
                    project_meta=project_meta,
                    dataset_info=ds,
                    children=children,
                    dst_project_id=created_project.id,
                    dst_dataset_id=None,
                    options=options,
                    progress_cb=progress_cb,
                )
            )
    return created_datasets


def move_project(
    src_project_info: sly.ProjectInfo,
    dst_workspace_id: int,
    dst_project_id: int,
    dst_dataset_id: int,
    options: Dict,
    progress_cb=None,
    existing_projects=None,
):
    if dst_project_id is None and src_project_info.workspace_id == dst_workspace_id:
        logger.warning(
            "Moving project to the same workspace. Skipping",
            extra={"project_id": src_project_info.id},
        )
        progress_cb(src_project_info.items_count)
        return []
    datasets_tree = run_in_executor(api.dataset.get_tree, src_project_info.id)
    created_datasets = copy_project(
        src_project_info=src_project_info,
        dst_workspace_id=dst_workspace_id,
        dst_project_id=dst_project_id,
        dst_dataset_id=dst_dataset_id,
        options=options,
        progress_cb=progress_cb,
        existing_projects=existing_projects,
        datasets_tree=datasets_tree,
    )
    if dst_project_id == src_project_info.id or dst_dataset_id in [
        ds.id for ds in flatten_tree(datasets_tree)
    ]:
        logger.warning(
            "Moving project to itself. Skipping deletion", extra={"project_id": dst_project_id}
        )
        return created_datasets
    logger.info("Removing source project", extra={"project_id": src_project_info.id})
    run_in_executor(api.project.remove, src_project_info.id)
    return created_datasets


def copy_dataset_tree(
    datasets_tree: Dict,
    project_type: str,
    project_meta: sly.ProjectMeta,
    dst_project_id: int,
    dst_dataset_id: int,
    options: Dict,
    progress_cb=None,
):
    created_datasets = []
    with sly.ApiContext(api, project_meta=project_meta):
        with ThreadPoolExecutor() as ds_executor:
            tasks = []
            for dataset, children in datasets_tree.items():
                tasks.append(
                    ds_executor.submit(
                        create_dataset_recursively,
                        project_type,
                        project_meta,
                        dataset,
                        children,
                        dst_project_id,
                        dst_dataset_id,
                        options,
                        progress_cb=progress_cb,
                    )
                )
            for task in as_completed(tasks):
                created_datasets.extend(task.result())
    return created_datasets


def move_datasets_tree(
    datasets_tree: Dict,
    project_type: str,
    project_meta: sly.ProjectMeta,
    dst_project_id: int,
    dst_dataset_id: int,
    options: Dict,
    progress_cb=None,
):
    creted_datasets = copy_dataset_tree(
        datasets_tree,
        project_type,
        project_meta,
        dst_project_id,
        dst_dataset_id,
        options,
        progress_cb,
    )
    if dst_dataset_id in [ds.id for ds in flatten_tree(datasets_tree)]:
        logger.warning(
            "Moving dataset to itself. Skipping deletion", extra={"dataset_id": dst_dataset_id}
        )
        return creted_datasets
    logger.info("Removing source datasets", extra={"dataset_id": dst_dataset_id})
    run_in_executor(api.dataset.remove_batch, [ds.id for ds in flatten_tree(datasets_tree)])
    return creted_datasets


def get_item_infos(dataset_id: int, item_ids: List[int], project_type: str):
    filters = [{"field": "id", "operator": "in", "value": item_ids}]
    if project_type == str(sly.ProjectType.IMAGES):
        return api_utils.images_get_list(api, dataset_id, item_ids)
    if project_type == str(sly.ProjectType.VIDEOS):
        return api.video.get_info_by_id_batch(item_ids)
    if project_type == str(sly.ProjectType.VOLUMES):
        return api.volume.get_list(dataset_id, filters)
    if project_type == str(sly.ProjectType.POINT_CLOUDS):
        return api.pointcloud.get_list(dataset_id, filters)
    if project_type == str(sly.ProjectType.POINT_CLOUD_EPISODES):
        return api.pointcloud_episode.get_list(dataset_id, filters)
    else:
        raise ValueError("Unknown item type")


def copy_items_to_dataset(
    items: List[Dict],
    project_type: str,
    src_dataset_id: int,
    dst_dataset_id: int,
    project_meta: sly.ProjectMeta,
    options: Dict,
    progress_cb=None,
):
    item_ids = [item[JSONKEYS.ID] for item in items]
    item_infos = get_item_infos(src_dataset_id, item_ids, project_type)
    created_item_infos = clone_items(
        src_dataset_id,
        dst_dataset_id,
        project_type=project_type,
        project_meta=project_meta,
        options=options,
        progress_cb=progress_cb,
        src_infos=item_infos,
    )
    return created_item_infos


def delete_items(item_infos: List):
    if len(item_infos) == 0:
        return
    item_ids = [info.id for info in item_infos]
    api.image.remove_batch(item_ids)


def move_items_to_dataset(
    items: List[Dict],
    project_type: str,
    src_dataset_id: int,
    dst_dataset_id: int,
    project_meta: sly.ProjectMeta,
    options: Dict,
    progress_cb=None,
):
    item_ids = [item[JSONKEYS.ID] for item in items]
    item_infos = get_item_infos(src_dataset_id, item_ids, project_type)
    created_item_infos = clone_items(
        src_dataset_id,
        dst_dataset_id,
        project_type=project_type,
        project_meta=project_meta,
        options=options,
        progress_cb=progress_cb,
        src_infos=item_infos,
    )
    delete_items(item_infos)
    return created_item_infos


def copy_or_move(state: Dict, move: bool = False):
    source = state[JSONKEYS.SOURCE]
    destination = state[JSONKEYS.DESTINATION]
    options = state[JSONKEYS.OPTIONS]
    items = state[JSONKEYS.ITEMS]

    src_team_id = source[JSONKEYS.TEAM][JSONKEYS.ID]
    src_workspace_id = source.get(JSONKEYS.WORKSPACE, {}).get(JSONKEYS.ID, None)
    src_project_id = source.get(JSONKEYS.PROJECT, {}).get(JSONKEYS.ID, None)
    src_dataset_id = source.get(JSONKEYS.DATASET, {}).get(JSONKEYS.ID, None)

    dst_team_id = destination[JSONKEYS.TEAM][JSONKEYS.ID]
    dst_workspace_id = destination.get(JSONKEYS.WORKSPACE, {}).get(JSONKEYS.ID, None)
    dst_project_id = destination.get(JSONKEYS.PROJECT, {}).get(JSONKEYS.ID, None)
    dst_dataset_id = destination.get(JSONKEYS.DATASET, {}).get(JSONKEYS.ID, None)

    if len(items) == 0:
        raise ValueError("Items list is empty")

    progress = sly.Progress(message="Cloning items", total_cnt=1)

    def _progress_cb(n=1):
        if n == 0:
            return
        for _ in range(n - 1):
            progress.iter_done()
        progress.iter_done_report()

    project_items = [item for item in items if item[JSONKEYS.TYPE] == JSONKEYS.PROJECT]
    dataset_items = [item for item in items if item[JSONKEYS.TYPE] == JSONKEYS.DATASET]
    image_items = [item for item in items if item[JSONKEYS.TYPE] == JSONKEYS.IMAGE]

    items_to_create = 0
    src_project_infos: Dict[int, sly.ProjectInfo] = {}
    for item in project_items:
        project_id = item[JSONKEYS.ID]
        project_info = api.project.get_info_by_id(project_id, raise_error=True)
        src_project_infos[project_id] = project_info
        items_to_create += project_info.items_count
        # project meta is merged in the copy function
    existing_projects = None
    if (
        len(project_items) > 0
        and options[JSONKEYS.CONFLICT_RESOLUTION_MODE] != JSONKEYS.CONFLICT_RENAME
    ):
        existing_projects = api.project.get_list(dst_workspace_id)

    project_meta = None
    datasets_tree = None
    if len(dataset_items) > 0:
        src_project_infos.setdefault(
            src_project_id, api.project.get_info_by_id(src_project_id, raise_error=True)
        )
        src_dataset_ids = [item[JSONKEYS.ID] for item in dataset_items]
        datasets_tree = api.dataset.get_tree(src_project_id)
        datasets_tree = _find_tree(datasets_tree, src_dataset_ids)
        items_to_create += _count_items_in_tree(datasets_tree)
        project_meta = merge_project_meta(src_project_id, dst_project_id)

    if len(image_items) > 0:
        src_project_infos.setdefault(
            src_project_id, api.project.get_info_by_id(src_project_id, raise_error=True)
        )
        items_to_create += len(image_items)
        if project_meta is None:
            project_meta = merge_project_meta(src_project_id, dst_project_id)

    progress.total = items_to_create
    progress.report_progress()
    logger.info("Total items: %d", items_to_create)

    if len(project_items) > 0:
        f = move_project if move else copy_project
        with ThreadPoolExecutor() as local_executor:
            tasks = []
            for item in project_items:
                item_project_info = src_project_infos[item[JSONKEYS.ID]]
                tasks.append(
                    local_executor.submit(
                        f,
                        src_project_info=item_project_info,
                        dst_workspace_id=dst_workspace_id,
                        dst_project_id=dst_project_id,
                        dst_dataset_id=dst_dataset_id,
                        options=options,
                        progress_cb=_progress_cb,
                        existing_projects=existing_projects,
                    )
                )
            for task in as_completed(tasks):
                task.result()
    if len(dataset_items) > 0:
        project_type = src_project_infos[src_project_id].type
        if move:
            move_datasets_tree(
                datasets_tree,
                project_type=project_type,
                project_meta=project_meta,
                dst_project_id=dst_project_id,
                dst_dataset_id=dst_dataset_id,
                options=options,
                progress_cb=_progress_cb,
            )
        else:
            copy_dataset_tree(
                datasets_tree,
                project_type=project_type,
                project_meta=project_meta,
                dst_project_id=dst_project_id,
                dst_dataset_id=dst_dataset_id,
                options=options,
                progress_cb=_progress_cb,
            )
    if len(image_items) > 0:
        project_type = src_project_infos[src_project_id].type
        if move:
            if src_dataset_id == dst_dataset_id:
                return
            move_items_to_dataset(
                image_items,
                project_type,
                src_dataset_id,
                dst_dataset_id,
                project_meta,
                options,
                _progress_cb,
            )
        else:
            copy_items_to_dataset(
                image_items,
                project_type,
                src_dataset_id,
                dst_dataset_id,
                project_meta,
                options,
                _progress_cb,
            )


# ------------------------------------ Transfer Labeled Images ----------------------------------- #


def sync_call(coro):
    """
    This function is used to run asynchronous functions in synchronous context.

    :param coro: Asynchronous function.
    :type coro: Coroutine
    :return: Result of the asynchronous function.
    :rtype: Any
    """
    import asyncio

    loop = sly.utils.get_or_create_event_loop()

    if loop.is_running():
        future = asyncio.run_coroutine_threadsafe(coro, loop=loop)
        return future.result()
    else:
        return loop.run_until_complete(coro)


def fetch_images_sync(images_generator):
    image_infos = []

    async def fetch_images():
        async for image_batch in images_generator:
            for image in image_batch:
                image_infos.append(image)

    sync_call(fetch_images())
    return image_infos


def find_parent(id: int, dst_datasets: List[sly.DatasetInfo]):
    """Find parent dataset info by id in list of datasets"""
    for ds in dst_datasets:
        if ds.id == id:
            return ds
    return None


def get_parents_chain(ds: sly.DatasetInfo, dst_datasets: List[sly.DatasetInfo]):
    """
    Get chain of parent datasets for defined dataset.
    Returns list of datasets from bottom to top.
    """
    chain = []
    while ds is not None:
        chain.append(ds)
        ds = find_parent(ds.parent_id, dst_datasets)
    return chain


def ensure_datasets_deletion(
    dst_datasets: List[sly.DatasetInfo], empty_datasets: List[sly.DatasetInfo]
) -> Dict[int, bool]:
    """
    Determines which datasets should be deleted.
    A dataset is deleted if it is empty and all its children are also deleted.

    :param dst_datasets: List of all datasets in the project.
    :param empty_datasets: List of empty datasets.
    :return: A dictionary {dataset_id: should_delete (True/False)}.
    """

    # Initialize deletion map with all datasets marked as True
    dataset_deletion_map = {ds.id: True for ds in dst_datasets}
    dst_ids = [ds.id for ds in dst_datasets]

    # Build parent-child relationships
    parent_map = {ds.id: ds.parent_id for ds in dst_datasets}
    child_map = {ds.id: [] for ds in dst_datasets}

    for ds in dst_datasets:
        if ds.parent_id in dst_ids:
            child_map[ds.parent_id].append(ds.id)

    # Function to calculate the depth of a dataset
    def get_depth(ds_id):
        depth = 0
        while parent_map.get(ds_id, None) is not None:
            ds_id = parent_map[ds_id]
            depth += 1
        return depth

    # Sort datasets by depth (deepest first)
    sorted_datasets = sorted(dst_datasets, key=lambda ds: get_depth(ds.id), reverse=True)

    # Process datasets from bottom to top
    for ds in sorted_datasets:
        if ds not in empty_datasets:
            dataset_deletion_map[ds.id] = False  # Keep non-empty datasets
        else:
            # Check if all children are marked for deletion
            children = child_map.get(ds.id, [])
            if any(not dataset_deletion_map[child] for child in children):
                dataset_deletion_map[ds.id] = False  # Keep if any child must stay

        # Use parent_map to update parent status
        parent_id = parent_map.get(ds.id)
        if parent_id is not None and parent_id in dataset_deletion_map:
            # Parent should only be deleted if all its children are marked for deletion
            if all(dataset_deletion_map[child] for child in child_map[parent_id]):
                dataset_deletion_map[parent_id] = True
            else:
                dataset_deletion_map[parent_id] = False

    for ds in dst_datasets:
        logger.info(f"Dataset ID: {ds.id}, Name: {ds.name}, Delete: {dataset_deletion_map[ds.id]}")

    return dataset_deletion_map


def process_tli_dataset(
    src_dataset: Union[int, sly.DatasetInfo],
    destination: Destination,
    options: Options,
    update_meta: bool = True,
    src_project: Optional[Union[int, sly.ProjectInfo]] = None,
    target_dataset: Optional[Union[int, sly.DatasetInfo]] = None,
    parent_project: Optional[Union[int, sly.ProjectInfo]] = None,
    parent_dataset: Optional[Union[int, sly.DatasetInfo]] = None,
):
    """
    Transfer labeled images from source dataset to destination dataset or project.

    :param src_ds_id: source dataset ID
    :param destination: destination object with information about destination project or dataset
    :param options: options for transfer
    :param update_meta: update meta information in destination project
    :param src_project_info: source project information to optimize processing
    :param parent_project: Parent destination project ID or project information, in wich destination dataset will be created
    :param parent_dataset: Parent destination dataset ID or dataset information in wich destination dataset will be created
    :param target_dataset: Destination dataset ID or dataset information.
                        If provided, destination dataset already exists and will be used as destination.
                        This parameter is used for recursive processing of datasets when transferring items from project.
    """
    global merged_meta

    create_dataset = False
    create_project = False

    # Case src = dataset, dst = dataset
    # 1. get labeling jobs
    # 2. extract images (all images from same dataset guaranteed)
    # 3. create dataset in dst and copy items

    # TODO: handle destination project
    if destination.level == JSONKEYS.PROJECT and not any([parent_dataset, parent_project]):
        create_project = True
        # process creation project
        parent_project = destination.info
    elif destination.level == JSONKEYS.DATASET and not any([parent_dataset, parent_project]):
        create_dataset = True
        # process creation dataset
        parent_dataset = destination.info
    elif parent_dataset is not None:
        create_dataset = True
        if isinstance(parent_dataset, int):
            parent_dataset = api.dataset.get_info_by_id(parent_dataset)
        # process existing dataset

    elif parent_project is not None:
        create_project = True
        # process existing project
    elif target_dataset is not None:
        pass
    else:
        raise ValueError("Destination is not provided")

    if create_dataset and create_project:
        raise ValueError("Destination could not be the project and dataset at the same time")

    if isinstance(parent_project, int):
        parent_project = api.project.get_info_by_id(parent_project)

    if isinstance(target_dataset, int):
        target_dataset = api.dataset.get_info_by_id(target_dataset)

    if isinstance(src_dataset, int):
        src_dataset = api.dataset.get_info_by_id(src_dataset)

    logger.info(f"Start processing dataset ID: {src_dataset.id} with name '{src_dataset.name}'")
    if src_project is None:
        logger.info("Source project info is not provided. Getting it from dataset info")
        src_project = api.project.get_info_by_id(src_dataset.project_id)
    elif isinstance(src_project, int):
        src_project = api.project.get_info_by_id(src_project)

    jobs_list = api.labeling_job.get_list(
        team_id=src_project.team_id,
        project_id=src_project.id,
        dataset_id=src_dataset.id,
    )
    if len(jobs_list) == 0:
        logger.info(
            f"Dataset ID: {src_dataset.id} with name '{src_dataset.name}' has no jobs. Skipping"
        )
        return []
    completed_jobs = [
        job
        for job in jobs_list
        if job.status == JSONKEYS.COMPLETED and job.accepted_images_count != 0
    ]

    if len(completed_jobs) == 0:
        return []

    awaiting_jobs = [job for job in jobs_list if job.status != JSONKEYS.COMPLETED]
    completed_items = []
    intersecting_items = []
    for job in completed_jobs:
        logger.info(f"Collecting accepted images from job ID: {job.id} with name '{job.name}'")
        job_info = api.labeling_job.get_info_by_id(job.id)
        items = [
            item for item in job_info.entities if item[JSONKEYS.REVIEW_STATUS] == JSONKEYS.ACCEPTED
        ]
        if len(items) != 0:
            completed_items.extend(items)

    completed_ids = list(set([item[JSONKEYS.ID] for item in completed_items]))
    for job in awaiting_jobs:
        logger.info(f"Collecting intersecting images from job ID: {job.id} with name '{job.name}'")
        job_info = api.labeling_job.get_info_by_id(job.id)
        intersecting_items.extend(
            [item for item in job_info.entities if item[JSONKEYS.ID] in completed_ids]
        )
    intersecting_ids = list(set([item[JSONKEYS.ID] for item in intersecting_items]))
    move_ids = list(set(completed_ids) - set(intersecting_ids))

    if len(move_ids) == 0:
        logger.info("No images to transfer")
        return []

    if update_meta:
        logger.info("Merging destination project meta with meta from source project")
        merged_meta = merge_project_meta(src_project.id, target_dataset.project_id)
        logger.info("Meta has been updated")

    # TODO: create dataset with rename if conflict
    # Handle case when dataset is already created from process_tli_project
    if not target_dataset and create_dataset:
        if options.conflict_resolution_mode == JSONKEYS.CONFLICT_RENAME:
            original_description = (
                f"Original description: {src_project.description}"
                if src_project.description
                else ""
            )
            logger.info("Conflict resolution mode is set to 'rename'. Create dataset with new name")
            run_in_executor(
                api.dataset.create,
                parent_dataset.project_id,
                src_project.name,
                f"Dataset created from project ID: {src_project.id} with name '{src_project.name}'. {original_description}",
                change_name_if_conflict=True,
                parent_id=destination.dataset_id,
                created_at=src_project.created_at if options.preserve_src_date else None,
                updated_at=src_project.updated_at if options.preserve_src_date else None,
                created_by=src_project.created_by_id if options.preserve_src_date else None,
            )
        else:
            raise NotImplementedError("Conflict resolution mode is not implemented")

    logger.info(
        f"Start transferring images for dataset ID: {src_dataset.id} with name '{src_dataset.name}'"
    )
    filters = [{"field": "id", "operator": "in", "value": move_ids}]
    images_generator = api.image.get_list_generator_async(
        dataset_id=src_dataset.id, filters=filters
    )
    image_infos = fetch_images_sync(images_generator)
    progress_clone = tqdm(desc="Transfering images", total=len(image_infos))
    created_items = clone_images_with_annotations(
        image_infos=image_infos,
        dst_dataset_id=target_dataset.id,
        project_meta=merged_meta,
        options=options.to_dict(),
        progress_cb=progress_clone,
    )
    logger.info(
        f"Finished transferring images for dataset ID: {src_dataset.id} with name '{src_dataset.name}'"
    )
    logger.info(f"Start removing {len(move_ids)} images from source dataset")
    progress_move = tqdm(total=len(move_ids), desc="Removing images from source dataset")
    # TODO Uncomment
    # api.image.remove_batch(move_ids, progress_cb=progress_move)

    logger.info(f"Finished processing dataset ID: {src_dataset.id} with name '{src_dataset.name}'")
    return created_items


def process_tli_project(
    src_project: Union[sly.ProjectInfo, int],
    destination: Destination,
    options: Options,
):
    global merged_meta

    if isinstance(src_project, int):
        src_project = api.project.get_info_by_id(src_project)
    message = f"Start processing project ID: {src_project.id} with name '{src_project.name}'"
    logger.info(
        f"{message}, resulting in a single dataset"
        if not options.preserve_structure
        else f"{message} with keeping structure"
    )

    perserve_date = options.preserve_src_date
    project_type = src_project.type
    original_description = (
        f"Original description: {src_project.description}" if src_project.description else ""
    )
    if (
        destination.level == Level.WORKSPACE
        and options.conflict_resolution_mode == JSONKEYS.CONFLICT_RENAME
    ):
        logger.info(f"Destination is workspace. Creating project with name '{src_project.name}'")
        dst_project = api.project.create(
            workspace_id=destination.workspace_id,
            name=src_project.name,
            description=f"Project created from project ID: {src_project.id} with name '{src_project.name}'. {original_description}",
            type=project_type,
            change_name_if_conflict=True,
        )
        dst_dataset = None
        logger.info(f"Project created with ID: {dst_project.id} and name '{dst_project.name}'")
    elif (
        destination.level == Level.PROJECT
        and options.conflict_resolution_mode == JSONKEYS.CONFLICT_RENAME
    ):
        logger.info(f"Destination is project. Creating dataset with name '{src_project.name}'")
        dst_project = destination.info
        dst_dataset = run_in_executor(
            api.dataset.create,
            dst_project.id,
            src_project.name,
            f"Dataset created from project ID: {src_project.id} with name '{src_project.name}'. {original_description}",
            change_name_if_conflict=True,
            parent_id=None,
        )
        logger.info(f"Dataset created with ID: {dst_dataset.id} and name '{dst_dataset.name}'")
    elif (
        destination.level == Level.DATASET
        and options.conflict_resolution_mode == JSONKEYS.CONFLICT_RENAME
    ):
        logger.info(f"Destination is dataset. Creating dataset with name '{src_project.name}'")
        dst_project = api.project.get_info_by_id(destination.info.project_id)
        dst_dataset = run_in_executor(
            api.dataset.create,
            dst_project.id,
            src_project.name,
            f"Dataset created from project ID: {src_project.id} with name '{src_project.name}'. {original_description}",
            change_name_if_conflict=True,
            parent_id=destination.info.id,
        )
        logger.info(f"Dataset created with ID: {dst_dataset.id} and name '{dst_dataset.name}'")

    merged_meta = run_in_executor(merge_project_meta, src_project.id, dst_project.id)

    created_datasets = []
    src_datasets_tree = run_in_executor(api.dataset.get_tree, src_project.id)
    src_datasets: List[sly.DatasetInfo] = flatten_tree(src_datasets_tree)
    logger.info("Start creating empty destination datasets")
    progress_create_ds = sly.Progress(total_cnt=len(src_datasets), message="Creating datasets")
    for ds, children in src_datasets_tree.items():
        created_datasets.extend(
            create_dataset_recursively(
                project_type=project_type,
                project_meta=merged_meta,
                dataset_info=ds,
                children=children,
                dst_project_id=dst_project.id,
                dst_dataset_id=dst_dataset.id if dst_dataset else None,
                options=options.to_dict(),
                should_clone_items=False,
                progress_cb=progress_create_ds,
            )
        )
    ids_map = {ds.id: ds for ds in created_datasets}
    dst_datasets = tree_from_list(created_datasets)
    dst_datasets: List[sly.DatasetInfo] = flatten_tree_by_map(dst_datasets, ids_map)

    progress_project = tqdm(total=len(src_datasets), desc="Processing datasets")
    empty_dst_datasets = []
    for src_ds, dst_ds in zip(src_datasets, dst_datasets):
        created_items = process_tli_dataset(
            src_dataset=src_ds,
            destination=destination,
            options=options,
            update_meta=False,
            target_dataset=dst_ds,
        )
        if len(created_items) == 0:
            empty_dst_datasets.append(dst_ds)
        progress_project(1)

    dataset_deletion_map = ensure_datasets_deletion(
        dst_datasets=dst_datasets,
        empty_datasets=empty_dst_datasets,
    )
    for ds_id, to_delete in dict(
        sorted(dataset_deletion_map.items(), key=lambda item: int(item[0]), reverse=True)
    ).items():
        if to_delete:
            logger.info(f"Removing dataset ID: {ds_id} with name '{ids_map[ds_id].name}' ")
            try:
                run_in_executor(api.dataset.remove, ds_id)
            except Exception as e:
                logger.info(
                    f"⚠️ Failed to remove dataset ID: {ds_id} with name '{ids_map[ds_id].name}'. It seems that dataset already removed"
                )

    logger.info(f"Finished processing project ID: {src_project.id} with name '{src_project.name}'")


def process_tli_job(
    job_id: int,
    destination: Destination,
    options: Options,
):
    job_info = api.labeling_job.get_info_by_id(job_id)
    process_tli_dataset(job_info.dataset_id, destination, options=options, update_meta=True)


def process_tli_queue(
    queue_id: int,
    destination: Destination,
    options: Options,
):
    jobs_list = api.labeling_job.get_list(queue_ids=[queue_id])
    for job_info in jobs_list:
        process_tli_job(job_info.id, destination=destination, options=options)


def transfer_labeled_items(state: Dict):
    source: dict = state[JSONKEYS.SOURCE]
    destination: dict = state[JSONKEYS.DESTINATION]
    options: dict = state[JSONKEYS.OPTIONS]
    items: dict = state[JSONKEYS.ITEMS]

    src_team_id = source[JSONKEYS.TEAM][JSONKEYS.ID]
    src_workspace_id = source.get(JSONKEYS.WORKSPACE, {}).get(JSONKEYS.ID, None)
    src_project_id = source.get(JSONKEYS.PROJECT, {}).get(JSONKEYS.ID, None)
    src_dataset_id = source.get(JSONKEYS.DATASET, {}).get(JSONKEYS.ID, None)

    # dst_team_id = destination[JSONKEYS.TEAM][JSONKEYS.ID]
    # dst_workspace_id = destination.get(JSONKEYS.WORKSPACE, {}).get(JSONKEYS.ID, None)
    # dst_project_id = destination.get(JSONKEYS.PROJECT, {}).get(JSONKEYS.ID, None)
    # dst_dataset_id = destination.get(JSONKEYS.DATASET, {}).get(JSONKEYS.ID, None)
    destination = Destination.from_dict(destination)
    options = Options.from_dict(options)
    source[JSONKEYS.ITEMS] = items
    source = Source.from_dict(source)

    if len(source.items) == 0:
        raise ValueError("Items list is empty")

    project_items = [item for item in source.items if item[JSONKEYS.TYPE] == JSONKEYS.PROJECT]
    dataset_items = [item for item in source.items if item[JSONKEYS.TYPE] == JSONKEYS.DATASET]
    job_items = [item for item in source.items if item[JSONKEYS.TYPE] == JSONKEYS.JOB]
    queue_items = [item for item in source.items if item[JSONKEYS.TYPE] == JSONKEYS.QUEUE]

    if len(project_items) > 0:
        for item in project_items:
            process_tli_project(item[JSONKEYS.ID], destination=destination, options=options)
    if len(dataset_items) > 0:
        for item in dataset_items:
            process_tli_dataset(
                src_dataset=item[JSONKEYS.ID], destination=destination, options=options
            )
    if len(job_items) > 0:
        for item in job_items:
            process_tli_job(item[JSONKEYS.ID], destination=destination)
    if len(queue_items) > 0:
        for item in queue_items:
            process_tli_queue(item[JSONKEYS.ID])


# ----------------------------------------- Main Section ----------------------------------------- #


def main():
    state = extract_state_from_env()
    logger.info("State:", extra=state)
    action = state[JSONKEYS.ACTION]
    if action == "move":
        copy_or_move(state, move=True)
    elif action == "copy":
        copy_or_move(state)
    elif action == "transfer_labeled_items":
        transfer_labeled_items(state)
    else:
        raise ValueError(f"Unsupported action: {action}")


if __name__ == "__main__":
    sly.main_wrapper("Data Commander", main)
