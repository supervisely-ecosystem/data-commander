FROM supervisely/base-py-sdk:6.73.537

# TEMPORARY: meshes support requires the unreleased mesh SDK.
# Remove this and bump the base image once the mesh changes are released
# (SDK branch: refactor/mesh-image-annotation-model).
RUN pip install --no-cache-dir --force-reinstall --no-deps \
    "supervisely @ git+https://github.com/supervisely/supervisely.git@refactor/mesh-image-annotation-model"
