import os

import pkg_resources


def extract_package_resources_file(resources_path: str, destination_dir: str) -> str:

    os.makedirs(destination_dir, exist_ok=True)

    outfile = os.path.join(destination_dir, os.path.basename(resources_path))

    if os.path.exists(outfile):
        return outfile

    with open(outfile, 'wb+') as f:
        f.write(pkg_resources.resource_string('dsp', resources_path))

    return outfile
