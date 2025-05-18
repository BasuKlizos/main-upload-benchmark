from prometheus_client import CollectorRegistry, Counter, Histogram, push_to_gateway

registry = CollectorRegistry()

# Upload Metrics
UPLOAD_REQUESTS = Counter("upload_requests_total", "Total upload requests received",registry=registry)
UPLOAD_SUCCESS = Counter("upload_success_total", "Total successful uploads",registry=registry)
UPLOAD_FAILURE = Counter("upload_failure_total", "Total failed uploads",registry=registry)

FILE_COUNT = Counter("files_uploaded_total", "Total number of files uploaded",registry=registry)
ZIP_FILES = Counter("zip_files_uploaded_total", "Total ZIP files uploaded",registry=registry)
# PDF_FILES = Counter("pdf_files_uploaded_total", "Total PDF files uploaded")
# DOCX_FILES = Counter("docx_files_uploaded_total", "Total DOCX files uploaded")
CREATED_FILES = Counter("created_files", "Number of files created for processing (PDF or DOCX)", registry=registry)
UNSUPPORTED_FILES = Counter("unsupported_files_total", "Total unsupported files attempted",registry=registry)

UPLOAD_DURATION = Histogram("upload_duration_seconds", "Upload request duration",registry=registry)
PROCESS_DURATION = Histogram("batch_process_duration_seconds", "Batch processing duration",registry=registry)

EMAIL_SENT = Counter("email_sent_total", "Emails sent after processing", registry=registry)

# File/Chunk Processing
FILE_PROCESS_DURATION = Histogram("file_process_duration_seconds", "Time taken to process a single file", registry=registry)
CHUNK_PROCESS_DURATION = Histogram("chunk_process_duration_seconds", "Time taken to process a file chunk", registry=registry)

FILES_PROCESSED = Counter("files_processed_total", "Total number of files processed",registry=registry)
FILES_FAILED = Counter("files_failed_total", "Total number of files that failed processing", registry=registry)

CHUNKS_PROCESSED = Counter("chunks_processed_total", "Total number of file chunks processed", registry=registry)
CHUNKS_FAILED = Counter("chunks_failed_total", "Total number of file chunks that failed",registry=registry)