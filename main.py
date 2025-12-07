import logging

from crawler.logger import setup_logger
from crawler.pipeline import run_pipeline_for_group


def main():
    setup_logger(level=logging.DEBUG, logfile="crawler.log")

    log = logging.getLogger(__name__)
    log.info("Vk bot detection started")

    run_pipeline_for_group(
        # group_name=["borsch", "igm", "rhymes", "schrodinger_humor", "pravdashowtop"],
        group_name=["borsch"],
        max_posts=5,
        max_comments_per_post=300,
        db_path="vk.sqlite",
        fast_mode=True,
        collect_from_db=False,
    )


if __name__ == "__main__":
    main()
