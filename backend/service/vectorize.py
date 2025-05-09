import asyncio

from backend.config import settings
from backend.logging_config.logger import logger
from backend.service.openai_client import _create_embeddings
from pinecone import Pinecone, ServerlessSpec


class _VectorDBService(object):
    def __init__(self):
        """Initialize Pinecone client"""
        self.pc = Pinecone(api_key=settings.PINECONE_API_KEY)
        self._initialize_index()

    def _initialize_index(self):
        """Initialize Pinecone index if it doesn't exist"""
        try:
            if not self.pc.has_index(settings.PINECONE_INDEX_NAME):
                logger.info(f"Creating new Pinecone index: {settings.PINECONE_INDEX_NAME}")
                self.pc.create_index(
                    name=settings.PINECONE_INDEX_NAME,
                    dimension=1536,  # OpenAI embeddings dimension
                    metric="cosine",
                    spec=ServerlessSpec(cloud="aws", region=settings.PINECONE_REGION),
                )
            self.index = self.pc.Index(settings.PINECONE_INDEX_NAME)
            logger.info(f"Successfully initialized Pinecone index: {settings.PINECONE_INDEX_NAME}")
        except Exception as e:
            logger.error(f"Error initializing Pinecone index: {str(e)}")
            raise

    async def embed_and_store(self, doc: dict, embedding_text: str, job_title: str) -> None:
        """Efficiently embed and store a single document in Pinecone"""
        try:
            # Generate embedding for the text
            vector = await _create_embeddings(embedding_text)

            # Prepare metadata with efficient string operations and company_id
            metadata = {
                "_id": doc.get("_id"),
                "company_id": doc.get("company_id", ""),
                "name": doc.get("name", ""),
                "score": doc.get("compatibility_analysis", {}).get("overall_score", 0),
                "position": job_title,
                "date": doc.get("date", ""),
                "phone": doc.get("primary_phone", ""),
                "status": doc.get("status", ""),
            }

            # Single upsert operation
            await asyncio.to_thread(self.index.upsert, [(doc.get("_id"), vector, metadata)])
            logger.info(f"Successfully embedded and stored document: {doc.get('_id')}")

        except Exception as e:
            logger.error(f"Error embedding document {doc.get('name', 'Unknown')}: {str(e)}")
            raise Exception(f"Error embedding document: {str(e)}")

    async def search(self, query: str, company_id: str, pageSize: int = 10) -> list:
        """Search for candidates using semantic search"""
        try:
            # Generate embedding for the query
            query_vector = await _create_embeddings(query)

            # Metadata filter: skills_match > 70 AND education_fit = 80
            results = self.index.query(
                vector=query_vector,
                top_k=pageSize,
                include_metadata=True,
                filter={
                    "company_id": company_id,
                },
            )

            return results

        except Exception as e:
            logger.error(f"Error during candidate search: {str(e)}", exc_info=True)
            raise


vector_service = _VectorDBService()
