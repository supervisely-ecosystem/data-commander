FROM supervisely/base-py-sdk:6.73.580

# TEMPORARY: meshes support requires the unreleased mesh SDK.
# Pinned to a commit on branch refactor/mesh-image-annotation-model — pip caches
# branch refs and may not refetch, so pin the exact commit. Remove this and bump
# the base image once the mesh changes are released.
RUN pip install --no-cache-dir --force-reinstall --no-deps \
    "supervisely @ git+https://github.com/supervisely/supervisely.git@ca91828ac6275fae2351a361a041f0dd66b485f1"
