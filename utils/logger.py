import logging
import os
from logging.handlers import RotatingFileHandler


def setup_logger():
    """
    프로젝트 전체에서 사용할 로거(Logger)를 설정합니다.
    로그는 콘솔(화면)에도 나오고, 파일(pipeline.log)로도 저장됩니다.
    """
    # 1. 로거 생성
    logger = logging.getLogger("LoL_Pipeline")
    logger.setLevel(logging.INFO)

    # 2. 포맷 설정 (시간 - 레벨 - 메시지)
    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")

    # 중복 생성 방지
    if logger.hasHandlers():
        return logger

    # 3. 콘솔 핸들러 (화면에 출력)
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)

    # 4. 파일 핸들러 (로그 저장)
    if not os.path.exists("logs"):
        os.makedirs("logs")

    file_handler = RotatingFileHandler(
        "logs/pipeline.log", maxBytes=1024 * 1024 * 5, backupCount=5, encoding="utf-8"
    )
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    return logger
