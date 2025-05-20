from prometheus_client import Counter, Histogram

# Upload Metrics
UPLOAD_REQUESTS = Counter("upload_requests_total", "Total upload requests received")
UPLOAD_SUCCESS = Counter("upload_success_total", "Total successful uploads")
UPLOAD_FAILURE = Counter("upload_failure_total", "Total failed uploads")

FILE_COUNT = Counter("files_uploaded_total", "Total number of files uploaded")
ZIP_FILES = Counter("zip_files_uploaded_total", "Total ZIP files uploaded")
# PDF_FILES = Counter("pdf_files_uploaded_total", "Total PDF files uploaded")
# DOCX_FILES = Counter("docx_files_uploaded_total", "Total DOCX files uploaded")
CREATED_FILES = Counter("created_files", "Number of files created for processing (PDF or DOCX)")
UNSUPPORTED_FILES = Counter("unsupported_files_total", "Total unsupported files attempted")

UPLOAD_DURATION = Histogram("upload_duration_seconds", "Upload request duration")
PROCESS_DURATION = Histogram("batch_process_duration_seconds", "Batch processing duration")

EMAIL_SENT = Counter("email_sent_total", "Emails sent after processing")

# File/Chunk Processing
FILE_PROCESS_DURATION = Histogram("file_process_duration_seconds", "Time taken to process a single file")
CHUNK_PROCESS_DURATION = Histogram("chunk_process_duration_seconds", "Time taken to process a file chunk")

FILES_PROCESSED = Counter("files_processed_total", "Total number of files processed")
FILES_FAILED = Counter("files_failed_total", "Total number of files that failed processing")

CHUNKS_PROCESSED = Counter("chunks_processed_total", "Total number of file chunks processed")
CHUNKS_FAILED = Counter("chunks_failed_total", "Total number of file chunks that failed")

