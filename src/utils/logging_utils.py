import logging
import sys
from pathlib import Path
import datetime

def setup_logging(logs_root: Path | None = None, level: int = logging.INFO) -> Path:
    """Configure root logger to write both to console and a timestamped log file.

    Returns the path to the created log file.
    """
    if logs_root is None:
        # project_root/../ logs directory (assuming utils is under src/)
        logs_root = Path(__file__).resolve().parents[2] / "logs"
    logs_root.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file_path = logs_root / f"run_{timestamp}.log"

    # File handler
    file_handler = logging.FileHandler(log_file_path, encoding="utf-8")
    file_handler.setLevel(level)
    file_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))

    # Console handler (keep simple format, suppress verbose merge messages)
    console_handler = logging.StreamHandler(sys.__stdout__)
    console_handler.setLevel(level)
    console_handler.setFormatter(logging.Formatter("%(message)s"))

    class ExcludeHelpersFilter(logging.Filter):
        def filter(self, record: logging.LogRecord):
            # Drop messages from utils.helpers (node/rel merge spam) so that
            # console output stays concise. They still go to the file handler.
            return not record.name.startswith("utils.helpers")

    console_handler.addFilter(ExcludeHelpersFilter())

    logging.basicConfig(level=level, handlers=[file_handler, console_handler], force=True)

    # Redirect print statements to the logger for consistency
    class PrintLogger:
        def write(self, msg):
            msg = msg.rstrip()
            if msg:
                logging.info(msg)
        def flush(self):
            pass
    sys.stdout = PrintLogger()
    sys.stderr = PrintLogger()

    logging.info(f"Logs are being written to {log_file_path}")
    return log_file_path 