# logger_diagnostico.py (ou dentro de run_optimizer.py)
import logging

diagnostico_logger = logging.getLogger("diagnostico_modelo")
diagnostico_logger.setLevel(logging.ERROR)

file_handler = logging.FileHandler("diagnostico_modelo.log")
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
file_handler.setFormatter(formatter)

diagnostico_logger.addHandler(file_handler)