import json
import time
import logging
import azure.functions as func
from typing import Dict

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize the Function App (safe global setup)
app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)

# Hardcoded Azure Blob Storage details
STORAGE_ACCOUNT_URL = "https://ncf2025largediststore.blob.core.windows.net"
SAS_TOKEN = "?se=2025-12-31T23%3A59%3A59Z&sp=rwdlacup&spr=https&sv=2022-11-02&ss=b&srt=sco&sig=20%2BBrYp7tqcdumFaqdHa1oe7ksA2rktNRU%2BBrh6qX3A%3D"
DEFAULT_FILE_NAME = "spy_file_10_MB.txt"
CONTAINER_NAME = "spy-files"
# Default values
DEFAULT_START = 0
DEFAULT_END = 1_000  # 10 MB in bytes
@app.route(route="count_verbs", methods=["GET", "POST"])
def count_verbs(req: func.HttpRequest) -> func.HttpResponse:
    function_start_time = time.time()
    logger.info(f"Processing request: {req.params}")

    try:
        params = req.params
        file_name = params.get('file_name', DEFAULT_FILE_NAME)
        start = params.get('start', DEFAULT_START)
        end = params.get('end', DEFAULT_END)

        try:
            start = int(start)
            end = int(end)
        except (ValueError, TypeError) as param_e:
            logger.error(f"Invalid parameters: {str(param_e)}")
            return func.HttpResponse(
                f"Invalid start or end parameters: {str(param_e)}. Must be integers.",
                status_code=400
            )

        try:
            import spacy
            nlp = spacy.load("en_core_web_sm", disable=["ner", "parser"])
            logger.info("SpaCy model loaded successfully")
        except Exception as e:
            logger.warning(f"SpaCy load failed: {str(e)}, attempting download")
            try:
                from spacy.cli import download
                download("en_core_web_sm")
                nlp = spacy.load("en_core_web_sm", disable=["ner", "parser"])
                logger.info("SpaCy model downloaded and loaded")
            except Exception as download_e:
                logger.error(f"Failed to load or download SpaCy model: {str(e)}. Download error: {str(download_e)}")
                return func.HttpResponse(
                    f"Failed to load or download SpaCy model: {str(e)}. Download error: {str(download_e)}",
                    status_code=500
                )

        try:
            from azure.storage.blob import BlobServiceClient
            from azure.core.credentials import AzureSasCredential
            blob_service_client = BlobServiceClient(account_url=STORAGE_ACCOUNT_URL, credential=AzureSasCredential(SAS_TOKEN))
            container_client = blob_service_client.get_container_client(CONTAINER_NAME )
            blob_client = container_client.get_blob_client(file_name)

            blob_properties = blob_client.get_blob_properties()
            total_file_size = blob_properties.size
            logger.info(f"Blob properties fetched: size={total_file_size}")

            start = max(0, start)
            end = min(end, total_file_size)

            if start >= end or start >= total_file_size:
                logger.error(f"Invalid range: start={start}, end={end}, file_size={total_file_size}")
                return func.HttpResponse(
                    f"Invalid range: start ({start}) must be less than end ({end}) and within file size ({total_file_size})",
                    status_code=400
                )

        except Exception as blob_e:
            logger.error(f"Blob Storage error: {str(blob_e)}")
            return func.HttpResponse(
                f"Failed to initialize Blob Storage client or access blob: {str(blob_e)}",
                status_code=500
            )

        total_verbs = 0
        verb_counts = {
            'past': 0,
            'present': 0,
            'future': 0,
            'base': 0,
            'gerund': 0,
            'participle': 0
        }

        try:
            range_length = end - start
            logger.info(f"Downloading single range: offset={start}, length={range_length}")
            blob_stream = blob_client.download_blob(offset=start, length=range_length)
            raw_data = blob_stream.readall()

            try:
                text = raw_data.decode("utf-8")
            except UnicodeDecodeError as decode_e:
                logger.warning(f"Decoding error: {str(decode_e)}, using ignore")
                text = raw_data.decode("utf-8", errors="ignore")

            try:
                doc = nlp(text)
                tagged = [(token.text, token.tag_) for token in doc]
                for word, tag in tagged:
                    if tag == 'VBD':
                        verb_counts['past'] += 1
                    elif tag in ['VBP', 'VBZ']:
                        verb_counts['present'] += 1
                    elif tag == 'VB':
                        verb_counts['base'] += 1
                    elif tag == 'VBG':
                        verb_counts['gerund'] += 1
                    elif tag == 'VBN':
                        verb_counts['participle'] += 1

                for i in range(len(tagged) - 1):
                    if tagged[i][0].lower() == 'will' and tagged[i + 1][1].startswith('VB'):
                        verb_counts['future'] += 1

                total_verbs = sum(verb_counts.values())
                logger.info(f"Processed range: {total_verbs} verbs found")
            except Exception as nlp_e:
                logger.error(f"SpaCy processing error: {str(nlp_e)}")
                return func.HttpResponse(
                    f"Failed to process text with SpaCy: {str(nlp_e)}",
                    status_code=500
                )

        except Exception as stream_e:
            logger.error(f"Download error: {str(stream_e)}")
            return func.HttpResponse(
                f"Failed to download or process blob range: {str(stream_e)}",
                status_code=500
            )

        execution_time = time.time() - function_start_time
        logger.info(f"Function completed: total_verbs={total_verbs}, time={execution_time}s")

        return func.HttpResponse(
            json.dumps({
                "verb_count": total_verbs,
                "detailed_counts": verb_counts,
                "file_name": file_name,
                "start": start,
                "end": end,
                "total_file_size": total_file_size,
                "execution_time_seconds": execution_time
            }),
            mimetype="application/json",
            status_code=200
        )

    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        return func.HttpResponse(
            f"Unexpected error in function execution: {str(e)}",
            status_code=500
        )
