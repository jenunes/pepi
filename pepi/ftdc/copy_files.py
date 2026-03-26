import os
import shutil
import logging

logger = logging.getLogger(__name__)

src_repo = "/home/jean/repositories/mongodb_ftdc_viewer"
dst_repo = "/home/jean/repositories/pepi/pepi/ftdc"

def copy_item(src, dst):
    if os.path.isdir(src):
        if os.path.exists(dst):
            shutil.rmtree(dst)
        shutil.copytree(src, dst)
        logger.info("Copied directory %s -> %s", src, dst)
    elif os.path.isfile(src):
        os.makedirs(os.path.dirname(dst), exist_ok=True)
        shutil.copy2(src, dst)
        logger.info("Copied file %s -> %s", src, dst)

# Copy grafana dashboards
copy_item(
    os.path.join(src_repo, "grafana/dashboards"),
    os.path.join(dst_repo, "grafana/dashboards")
)

# Copy ftdc_exporter
copy_item(
    os.path.join(src_repo, "ftdc_exporter"),
    os.path.join(dst_repo, "ftdc_exporter")
)

logger.info("FTDC components copied successfully.")
