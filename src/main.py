import ast
from concurrent.futures import ThreadPoolExecutor, as_completed, wait
from queue import Queue
import os
from typing import Dict, List, Tuple
from dotenv import load_dotenv

import supervisely as sly


import src.api_utils as api_utils


load_dotenv("local.env")
load_dotenv(os.path.expanduser("~/supervisely.env"))


DATE_FORMAT = "%Y-%m-%dT%H:%M:%S.%fZ"
UPLOAD_IMAGES_BATCH_SIZE = 1000


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


api = sly.Api()
executor = ThreadPoolExecutor(max_workers=5)


def extract_state_from_env():
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


def clone_images_with_annotations(
    image_infos: List[sly.ImageInfo],
    dst_dataset_id: int,
    project_meta: sly.ProjectMeta,
    options,
    progress_cb=None,
) -> sly.ImageInfo:
    if len(image_infos) == 0:
        return []

    def _copy_imgs():
        dst_image_infos = api.image.upload_ids(
            dst_dataset_id,
            names=[info.name for info in image_infos],
            ids=[info.id for info in image_infos],
            metas=[info.meta for info in image_infos],
            batch_size=UPLOAD_IMAGES_BATCH_SIZE,
            force_metadata_for_links=False,
            infos=image_infos,
            skip_validation=True,  # TODO: check if it is needed
            conflict_resolution=options[JSONKEYS.CONFLICT_RESOLUTION_MODE],
        )
        return dst_image_infos

    def _upload_anns(img_ids, anns):
        api.annotation.upload_jsons(img_ids, anns, skip_bounds_validation=True)
        return len(img_ids)

    src_dataset_id = image_infos[0].dataset_id
    copy_imgs_task = executor.submit(_copy_imgs)
    download_anns_tasks = []

    sly.logger.info("clone annotations: %s", options[JSONKEYS.CLONE_ANNOTATIONS] == True)
    if not options[JSONKEYS.CLONE_ANNOTATIONS]:
        dst_image_infos = copy_imgs_task.result()
        if progress_cb is not None:
            progress_cb(len(dst_image_infos))
        sly.logger.info("Clone annotations is disabled")
        return dst_image_infos

    for batch in sly.batched(image_infos):
        download_anns_tasks.append(
            executor.submit(
                api.annotation.download_batch,
                src_dataset_id,
                [info.id for info in batch],
                force_metadata_for_links=False,
            )
        )
    dst_image_infos = copy_imgs_task.result()
    src_to_dst_id_map = {src.id: dst.id for src, dst in zip(image_infos, dst_image_infos)}
    upload_tasks = []
    for task in as_completed(download_anns_tasks):
        ann_infos_batch: List[sly.api.annotation_api.AnnotationInfo] = task.result()
        upload_tasks.append(
            executor.submit(
                _upload_anns,
                [src_to_dst_id_map[ann_info.image_id] for ann_info in ann_infos_batch],
                [ann_info.annotation for ann_info in ann_infos_batch],
            )
        )
    for task in as_completed(upload_tasks):
        uploaded = task.result()
        if progress_cb is not None:
            progress_cb(uploaded)
    return dst_image_infos


def clone_videos_with_annotations(
    video_infos: List[sly.api.video_api.VideoInfo],
    dst_dataset_id: int,
    project_meta: sly.ProjectMeta,
    options,
    progress_cb=None,
) -> sly.api.video_api.VideoInfo:
    if len(video_infos) == 0:
        return []

    src_dataset_id = video_infos[0].dataset_id

    def _copy_videos(src_infos):
        dst_infos = api.video.upload_ids(
            dst_dataset_id,
            names=[info.name for info in src_infos],
            ids=[info.id for info in src_infos],
            infos=src_infos,
        )
        return {src_info.id: dst_info for src_info, dst_info in zip(src_infos, dst_infos)}

    def _copy_anns(src_ids, dst_ids):
        anns_jsons = api.video.annotation.download_bulk(src_dataset_id, src_ids)
        for ann_json, dst_id in zip(anns_jsons, dst_ids):
            key_id_map = sly.KeyIdMap()
            ann = sly.VideoAnnotation.from_json(ann_json, project_meta, key_id_map)
            api.video.annotation.append(dst_id, ann, key_id_map)
            if progress_cb is not None:
                progress_cb(1)
        return len(src_ids)

    copy_videos_tasks = []
    for batch in sly.batched(video_infos):
        copy_videos_tasks.append(executor.submit(_copy_videos, batch))

    dst_infos_dict = {}
    upload_anns_tasks = []
    for task in as_completed(copy_videos_tasks):
        src_to_dst_map = task.result()
        dst_infos_dict.update(src_to_dst_map)
        if options[JSONKEYS.CLONE_ANNOTATIONS]:
            src_ids_batch = list(src_to_dst_map.keys())
            upload_anns_tasks.append(
                executor.submit(
                    _copy_anns,
                    src_ids_batch,
                    [src_to_dst_map[src_id].id for src_id in src_ids_batch],
                )
            )
        elif progress_cb is not None:
            progress_cb(len(src_to_dst_map))
    if len(upload_anns_tasks) > 0:
        wait(upload_anns_tasks)
    return [dst_infos_dict[src.id] for src in video_infos]


def clone_volumes_with_annotations(
    volume_infos: List[sly.api.volume_api.VolumeInfo],
    dst_dataset_id: int,
    project_meta: sly.ProjectMeta,
    options,
    progress_cb=None,
) -> sly.api.volume_api.VolumeInfo:
    if len(volume_infos) == 0:
        return []

    src_dataset_id = volume_infos[0].dataset_id

    def _copy_volumes(infos):
        dst_volumes = api.volume.upload_hashes(
            dataset_id=dst_dataset_id,
            names=[info.name for info in infos],
            hashes=[info.hash for info in infos],
            metas=[info.meta for info in infos],
        )
        return {src.id: dst for src, dst in zip(infos, dst_volumes)}

    def _copy_anns(src_ids, dst_ids):
        ann_jsons = api.volume.annotation.download_bulk(src_dataset_id, src_ids)
        for ann_json, dst_id in zip(ann_jsons, dst_ids):
            key_id_map = sly.KeyIdMap()
            ann = sly.VolumeAnnotation.from_json(ann_json, project_meta, key_id_map)
            api.volume.annotation.append(dst_id, ann, key_id_map)
            if progress_cb is not None:
                progress_cb()
        return len(src_ids)

    copy_volumes_tasks = []
    for batch in sly.batched(volume_infos):
        copy_volumes_tasks.append(executor.submit(_copy_volumes, batch))
    upload_anns_tasks = []
    dst_infos_dict = {}
    for task in as_completed(copy_volumes_tasks):
        src_to_dst_map = task.result()
        dst_infos_dict.update(src_to_dst_map)
        if options[JSONKEYS.CLONE_ANNOTATIONS]:
            src_ids_batch = list(src_to_dst_map.keys())
            upload_anns_tasks.append(
                executor.submit(
                    _copy_anns,
                    src_ids_batch,
                    [src_to_dst_map[src_id].id for src_id in src_to_dst_map],
                )
            )
        elif progress_cb is not None:
            progress_cb(len(src_to_dst_map))
    if len(upload_anns_tasks) > 0:
        wait(upload_anns_tasks)
    return [dst_infos_dict[src.id] for src in volume_infos]


def clone_pointclouds_with_annotations(
    pointcloud_infos: [sly.api.pointcloud_api.PointcloudInfo],
    dst_dataset_id: int,
    project_meta: sly.ProjectMeta,
    options,
    progress_cb=None,
) -> sly.api.pointcloud_api.PointcloudInfo:
    if len(pointcloud_infos) == 0:
        return []

    src_dataset_id = pointcloud_infos[0].dataset_id

    def _copy_pointclouds(infos):
        dst_infos = api.pointcloud.upload_hashes(
            dataset_id=dst_dataset_id,
            names=[info.name for info in infos],
            hashes=[info.hash for info in infos],
            metas=[info.meta for info in infos],
        )
        return {src.id: dst for src, dst in zip(infos, dst_infos)}

    def _copy_anns(src_ids, dst_ids):
        ann_jsons = api.pointcloud.annotation.download_bulk(src_dataset_id, src_ids)
        for ann_json, dst_id in zip(ann_jsons, dst_ids):
            key_id_map = sly.KeyIdMap()
            ann = sly.PointcloudAnnotation.from_json(ann_json, project_meta, key_id_map)
            api.pointcloud.annotation.append(dst_id, ann, key_id_map)
            if progress_cb is not None:
                progress_cb()
        return len(src_ids)

    copy_pointcloud_tasks = []
    for batch in sly.batched(pointcloud_infos):
        copy_pointcloud_tasks.append(executor.submit(_copy_pointclouds, batch))
    upload_anns_tasks = []
    dst_infos_dict = {}
    for task in as_completed(copy_pointcloud_tasks):
        src_to_dst_map = task.result()
        dst_infos_dict.update(src_to_dst_map)
        if options[JSONKEYS.CLONE_ANNOTATIONS]:
            src_ids_batch = list(src_to_dst_map.keys())
            upload_anns_tasks.append(
                executor.submit(
                    _copy_anns,
                    src_ids_batch,
                    [src_to_dst_map[src_id].id for src_id in src_to_dst_map],
                )
            )
        elif progress_cb is not None:
            progress_cb(len(src_to_dst_map))
    if len(upload_anns_tasks) > 0:
        wait(upload_anns_tasks)
    return [dst_infos_dict[src.id] for src in pointcloud_infos]


def clone_pointcloud_episodes_with_annotations(
    pointcloud_episode_infos: List[sly.api.pointcloud_api.PointcloudInfo],
    dst_dataset_id: int,
    project_meta: sly.ProjectMeta,
    options,
    progress_cb=None,
):
    if len(pointcloud_episode_infos) == 0:
        return []

    src_dataset_id = pointcloud_episode_infos[0].dataset_id

    key_id_map = sly.KeyIdMap()
    ann_json = api.pointcloud_episode.annotation.download(src_dataset_id)
    ann = sly.PointcloudEpisodeAnnotation.from_json(
        data=ann_json, project_meta=project_meta, key_id_map=key_id_map
    )
    frame_to_pointcloud_ids = {}

    def _upload_hashes(infos):
        dst_infos = api.pointcloud_episode.upload_hashes(
            dataset_id=dst_dataset_id,
            names=[info.name for info in infos],
            hashes=[info.hash for info in infos],
            metas=[info.meta for info in infos],
        )
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

    copy_anns_tasks = []
    dst_infos_dict = {}
    for task in as_completed(copy_imgs_tasks):
        src_to_dst_dict = task.result()
        dst_infos_dict.update(src_to_dst_dict)
        if options[JSONKEYS.CLONE_ANNOTATIONS]:
            for src_id, dst_info in src_to_dst_dict.items():
                copy_anns_tasks.append(executor.submit(_upload_single, src_id, dst_info))

    wait(copy_anns_tasks)
    api.pointcloud_episode.annotation.append(
        dataset_id=dst_dataset_id,
        ann=ann,
        frame_to_pointcloud_ids=frame_to_pointcloud_ids,
        key_id_map=key_id_map,
    )
    return [dst_infos_dict[src.id] for src in pointcloud_episode_infos]


def clone_items(
    src_dataset_id, dst_dataset_id, project_type, project_meta, options, progress_cb=None
):
    if project_type == str(sly.ProjectType.IMAGES):
        src_infos = api.image.get_list(src_dataset_id)
        clone_f = clone_images_with_annotations
    elif project_type == str(sly.ProjectType.VIDEOS):
        src_infos = api.video.get_list(src_dataset_id)
        clone_f = clone_videos_with_annotations
    elif project_type == str(sly.ProjectType.VOLUMES):
        src_infos = api.volume.get_list(src_dataset_id)
        clone_f = clone_volumes_with_annotations
    elif project_type == str(sly.ProjectType.POINT_CLOUDS):
        src_infos = api.pointcloud.get_list(src_dataset_id)
        clone_f = clone_pointclouds_with_annotations
    elif project_type == str(sly.ProjectType.POINT_CLOUD_EPISODES):
        src_infos = api.pointcloud_episode.get_list(src_dataset_id)
        clone_f = clone_pointcloud_episodes_with_annotations
    else:
        raise NotImplementedError(
            "Cloning for project type {} is not implemented".format(project_type)
        )

    dst_infos = clone_f(src_infos, dst_dataset_id, project_meta, options, progress_cb)
    sly.logger.info(
        "Cloned %d images",
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
    sly.logger.info(
        "Creating dataset and its children",
        extra={
            "dataset_name": dataset_info.name,
            "children": [child.name for child in children],
            "destination_dataset": dst_dataset_id,
        },
    )
    tasks_queue = Queue()
    local_executor = ThreadPoolExecutor()

    def _create_rec(
        dataset_info: sly.DatasetInfo, children: Dict[sly.DatasetInfo, Dict], dst_parent_id: int
    ):
        created_id = None
        created_info = None
        if dataset_info is not None:
            if options[JSONKEYS.CONFLICT_RESOLUTION_MODE] == JSONKEYS.CONFLICT_SKIP:
                existing = run_in_executor(
                    api.dataset.get_list, dst_project_id, parent_id=dst_dataset_id
                )
                if any(ds.name == dataset_info.name for ds in existing):
                    return
            created_info = run_in_executor(
                api.dataset.create,
                dst_project_id,
                dataset_info.name,
                dataset_info.description,
                change_name_if_conflict=True,
                parent_id=dst_parent_id,
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
                    created_info = replace_dataset(existing, dataset_info)
            # to update items count
            created_info = run_in_executor(api.dataset.get_info_by_id, created_id)
            sly.logger.info(
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
    if options.get(JSONKEYS.PRESERVE_SRC_DATE, False):
        created_at = src_project_info.created_at
        created_by = src_project_info.created_by_id
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
        created_by=created_by,
    )
    sly.logger.info(
        "Created project",
        extra={"project_id": dst_project_info.id, "project_name": dst_project_info.name},
    )
    return dst_project_info


def merge_project_meta(src_project_id, dst_project_id):
    src_project_meta = sly.ProjectMeta.from_json(api.project.get_meta(src_project_id))
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
        for ds, children in tree.items():
            result.append(ds)
            _dfs(children)

    _dfs(tree)
    return result


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
        sly.logger.warning(
            "Copying project to the same workspace with replace. Skipping",
            extra={"project_id": src_project_info.id},
        )
        progress_cb(src_project_info.items_count)
        return []
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
            sly.logger.info(
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
    project_type = src_project_info.type
    created_datasets = []
    if dst_project_id is not None:
        if datasets_tree is None:
            datasets_tree = run_in_executor(api.dataset.get_tree, src_project_info.id)
        existing_datasets = find_children_in_tree(datasets_tree, parent_id=dst_dataset_id)
        if src_project_info.name in [ds.name for ds in existing_datasets]:
            progress_cb(src_project_info.items_count)
            sly.logger.info("Dataset with the same name already exists. Skipping")
            return []
        project_meta = run_in_executor(merge_project_meta, src_project_info.id, dst_project_id)
        created_dataset = run_in_executor(
            api.dataset.create,
            dst_project_id,
            src_project_info.name,
            src_project_info.description,
            change_name_if_conflict=True,
            parent_id=dst_dataset_id,
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
            sly.logger.info("Project with the same name already exists. Skipping")
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
        sly.logger.warning(
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
        sly.logger.warning(
            "Moving project to itself. Skipping deletion", extra={"project_id": dst_project_id}
        )
        return created_datasets
    sly.logger.info("Removing source project", extra={"project_id": src_project_info.id})
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
        sly.logger.warning(
            "Moving dataset to itself. Skipping deletion", extra={"dataset_id": dst_dataset_id}
        )
        return creted_datasets
    sly.logger.info("Removing source datasets", extra={"dataset_id": dst_dataset_id})
    run_in_executor(api.dataset.remove_batch, [ds.id for ds in flatten_tree(datasets_tree)])
    return creted_datasets


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
        for _ in range(n - 1):
            progress.iter_done()
        progress.iter_done_report()

    items_to_create = 0
    created_datasets = []
    item_type = items[0][JSONKEYS.TYPE]
    if item_type == JSONKEYS.PROJECT:
        items_project_infos = []
        for item in items:
            src_project_id = item[JSONKEYS.ID]
            src_project_info = api.project.get_info_by_id(src_project_id)
            if src_project_info is None:
                sly.logger.error("Project with id=%d not found", src_project_id)
                raise ValueError("Project not found")
            items_project_infos.append(src_project_info)
            items_to_create += src_project_info.items_count
        progress.total = items_to_create
        progress.report_progress()
        sly.logger.info("Total items: %d", items_to_create)

        existing_projects = None
        if options[JSONKEYS.CONFLICT_RESOLUTION_MODE] != JSONKEYS.CONFLICT_RENAME:
            existing_projects = api.project.get_list(dst_workspace_id)
        f = move_project if move else copy_project
        with ThreadPoolExecutor() as local_executor:
            tasks = []
            for item_project_info in items_project_infos:
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
                created_datasets.extend(task.result())

    elif item_type == JSONKEYS.DATASET:
        src_project_info = api.project.get_info_by_id(src_project_id)
        project_type = src_project_info.type
        project_meta = merge_project_meta(src_project_id, dst_project_id)
        src_dataset_ids = [item[JSONKEYS.ID] for item in items]
        datasets_tree = api.dataset.get_tree(src_project_id)
        datasets_tree = _find_tree(datasets_tree, src_dataset_ids)

        items_to_create = _count_items_in_tree(datasets_tree)
        progress.total = items_to_create
        progress.report_progress()
        sly.logger.info("Total items: %d", items_to_create)
        if move:
            created_datasets = move_datasets_tree(
                datasets_tree,
                project_type=project_type,
                project_meta=project_meta,
                dst_project_id=dst_project_id,
                dst_dataset_id=dst_dataset_id,
                options=options,
                progress_cb=_progress_cb,
            )
        else:
            created_datasets = copy_dataset_tree(
                datasets_tree,
                project_type=project_type,
                project_meta=project_meta,
                dst_project_id=dst_project_id,
                dst_dataset_id=dst_dataset_id,
                options=options,
                progress_cb=_progress_cb,
            )
    else:
        raise ValueError(f"Unsupported item type: {item_type}")


def main():
    state = extract_state_from_env()
    sly.logger.info("State:", extra=state)
    action = state[JSONKEYS.ACTION]
    if action == "move":
        copy_or_move(state, move=True)
    elif action == "copy":
        copy_or_move(state)
    else:
        raise ValueError(f"Unsupported action: {action}")


if __name__ == "__main__":
    sly.main_wrapper("Data Commander", main)
