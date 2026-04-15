import sys
import traceback
import logging
import json
import uuid
import boto3
import time
from urllib.parse import unquote_plus

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize AWS Clients
textract = boto3.client("textract")
s3 = boto3.client("s3")
dynamodb = boto3.resource("dynamodb")

# Replace with your actual DynamoDB table name
DYNAMO_TABLE_NAME = "TextractResults"
table = dynamodb.Table(DYNAMO_TABLE_NAME)

def process_error() -> dict:
    """Handles error logging."""
    ex_type, ex_value, ex_traceback = sys.exc_info()
    traceback_string = traceback.format_exception(ex_type, ex_value, ex_traceback)
    error_msg = json.dumps(
        {
            "errorType": ex_type.__name__,
            "errorMessage": str(ex_value),
            "stackTrace": traceback_string,
        }
    )
    return error_msg

def extract_text(response: dict, extract_by="LINE") -> list:
    """Extracts text from Textract response."""
    text = []
    for block in response.get("Blocks", []):
        if block["BlockType"] == extract_by:
            text.append(block["Text"])
    return text

def save_to_dynamodb(filename, bucketname, extracted_text):
    """Saves extracted text to DynamoDB."""
    item = {
        "id": str(uuid.uuid4()),  # Unique ID for the record
        "filename": filename,
        "bucket": bucketname,
        "extracted_text": "\n".join(extracted_text),
    }
    table.put_item(Item=item)
    logger.info(f"Saved to DynamoDB: {item}")

def get_textract_results(job_id):
    """Polls Textract for the results of an asynchronous job."""
    while True:
        response = textract.get_document_text_detection(JobId=job_id)
        status = response["JobStatus"]

        if status in ["SUCCEEDED", "FAILED"]:
            return response if status == "SUCCEEDED" else None

        logger.info("Waiting for Textract to finish processing...")
        time.sleep(5)  # Wait before checking again

def lambda_handler(event, context):
    try:
        if "Records" in event:
            file_obj = event["Records"][0]
            bucketname = str(file_obj["s3"]["bucket"]["name"])
            filename = unquote_plus(str(file_obj["s3"]["object"]["key"]))

            logger.info(f"Processing file: {filename} from bucket: {bucketname}")

            # Check file extension to determine processing method
            if filename.lower().endswith((".jpg", ".jpeg", ".png", ".tiff")):
                logger.info("Detected image file, using detect_document_text API.")
                response = textract.detect_document_text(
                    Document={
                        "S3Object": {
                            "Bucket": bucketname,
                            "Name": filename,
                        }
                    }
                )
                extracted_text = extract_text(response, extract_by="LINE")

            elif filename.lower().endswith(".pdf"):
                logger.info("Detected PDF file, using start_document_text_detection API.")
                response = textract.start_document_text_detection(
                    DocumentLocation={
                        "S3Object": {
                            "Bucket": bucketname,
                            "Name": filename,
                        }
                    }
                )
                job_id = response["JobId"]
                logger.info(f"Textract Job ID: {job_id}")

                # Wait and get the results
                response = get_textract_results(job_id)
                if response:
                    extracted_text = extract_text(response, extract_by="LINE")
                else:
                    logger.error("Textract failed to process the PDF.")
                    return {"statusCode": 500, "body": json.dumps("Textract failed to process the PDF.")}

            else:
                logger.error(f"Unsupported file format: {filename}")
                return {"statusCode": 400, "body": json.dumps("Unsupported file format. Only JPG, PNG, TIFF, and PDF are allowed.")}

            # Save extracted text to DynamoDB
            if extracted_text:
                logger.info(f"Extracted Text: {extracted_text}")
                save_to_dynamodb(filename, bucketname, extracted_text)
                return {"statusCode": 200, "body": json.dumps("Document processed and saved to DynamoDB!")}
            else:
                logger.warning("No text was extracted from the document.")
                return {"statusCode": 400, "body": json.dumps("No text extracted from document.")}

    except Exception:
        error_msg = process_error()
        logger.error(error_msg)
        return {"statusCode": 500, "body": json.dumps("Error processing the document!")}
