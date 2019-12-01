import os
import logging

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

profile = os.environ.get("PROFILE", "develop")

# neo4j
neo_sche = os.environ.get("NEO_SCHE", "bolt")
neo_host = os.environ.get("NEO_HOST", "localhost")
neo_port = os.environ.get("NEO_PORT", 7688)
neo_user = os.environ.get("NEO_USER", "neo4j")
neo_pass = os.environ.get("NEO_PASS", "password")

# model

extract_model = os.environ.get("EXTRACT_MODEL")

# logging

logger = logging.getLogger('NLMLayer')
log_file_path = os.path.join(ROOT, "log", "NLMLayer.log")

if profile == "develop":
    log_level = logging.DEBUG
else:
    log_level = logging.INFO

logger.setLevel(log_level)


ch = logging.StreamHandler()
ch.setLevel(log_level)
ch_format = logging.Formatter('%(name)s - %(levelname)s - %(message)s')
ch.setFormatter(ch_format)

# store errors
fh = logging.FileHandler(log_file_path)
fh.setLevel(logging.ERROR)
fh_format = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
fh.setFormatter(fh_format)

logger.addHandler(ch)
logger.addHandler(fh)

if __name__ == '__main__':
    print(ROOT)