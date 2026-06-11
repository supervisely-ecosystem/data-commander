FROM supervisely/base-py-sdk:6.73.580

# TEMPORARY: meshes support requires the unreleased mesh SDK.
# Pinned to a commit on branch refactor/mesh-image-annotation-model — pip caches
# branch refs and may not refetch, so pin the exact commit. Remove this and bump
# the base image once the mesh changes are released.
RUN RELEASE_VERSION="0.0.0+bfbde02" \
    python3 -m pip install --no-cache-dir --force-reinstall --no-deps \
    "https://github.com/supervisely/supervisely/archive/bfbde02112d94051a6e26a2385ba78924fb5b6f6.tar.gz"