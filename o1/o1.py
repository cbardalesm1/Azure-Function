import asyncio
import aiohttp
import logging
from typing import List, Dict
import urllib3
from azure.storage.blob.aio import BlobServiceClient
from azure.core.credentials import AzureSasCredential
import matplotlib.pyplot as plt

# Disable SSL warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

CHUNK_SIZE = 100_000
BATCH_SIZE = 20
STORAGE_ACCOUNT_URL = "https://ncf2025largediststore.blob.core.windows.net"
SAS_TOKEN = "?se=2025-12-31T23%3A59%3A59Z&sp=rwdlacup&spr=https&sv=2022-11-02&ss=b&srt=sco&sig=20%2BBrYp7tqcdumFaqdHa1oe7ksA2rktNRU%2BBrh6qX3A%3D"
DEFAULT_FILE_NAME = "spy_file_10_MB.txt"

class DistributedOrchestrator:
    def __init__(self, endpoints: List[str], chunk_size: int = CHUNK_SIZE):
        self.endpoints = endpoints
        self.chunk_size = chunk_size
        self.current_endpoint = 0
        self.total_verb_count = 0

# NEW: Dictionary to track individual verb categories
        self.verb_counts = {
            'past': 0,
            'present': 0,
            'future': 0
        }


    def _get_next_endpoint(self):
        endpoint = self.endpoints[self.current_endpoint]
        self.current_endpoint = (self.current_endpoint + 1) % len(self.endpoints)
        return endpoint

    async def process_chunk(self, session: aiohttp.ClientSession, chunk: Dict, file_name: str) -> Dict:
        endpoint = self._get_next_endpoint()
        params = {"file_name": file_name, "start": chunk["start"], "end": chunk["end"]}
        try:
            logging.info(f"Sending chunk {chunk['chunk_id']} (start: {chunk['start']}, end: {chunk['end']})")
            async with session.get(endpoint, params=params) as response:
                response_data = await response.json()
                logging.info(f"Chunk {chunk['chunk_id']} processed: {response_data}")

# NEW: Accumulate verb counts by category
                self.total_verb_count += response_data.get('verb_count', 0)
                for category in self.verb_counts:
                    self.verb_counts[category] += response_data.get('detailed_counts', {}).get(category, 0)
                    
                return response_data

        except Exception as e:
            logging.error(f"Chunk {chunk['chunk_id']} error: {e}")
            return {}

    async def fetch_file_size(self, file_name: str) -> int:
        blob_service_client = BlobServiceClient(account_url=STORAGE_ACCOUNT_URL, credential=AzureSasCredential(SAS_TOKEN))
        blob_client = blob_service_client.get_blob_client(container="spy-files", blob=file_name)
        properties = await blob_client.get_blob_properties()
        file_size = properties.size
        logging.info(f"File size: {file_size} bytes")
        return file_size

    async def process_file(self, file_name: str, batch_size: int = BATCH_SIZE) -> List[Dict]:
        file_size = await self.fetch_file_size(file_name)

        chunks = [
            {
                'chunk_id': i,
                'start': start,
                'end': min(start + self.chunk_size, file_size)
            }
            for i, start in enumerate(range(0, file_size, self.chunk_size))
        ]

        results = []
        async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=False)) as session:
            for i in range(0, len(chunks), BATCH_SIZE):
                batch_chunks = chunks[i:i + BATCH_SIZE]
                tasks = [self.process_chunk(session, chunk, file_name) for chunk in batch_chunks]
                batch_results = await asyncio.gather(*tasks, return_exceptions=True)
                results.extend([r for r in batch_results if not isinstance(r, Exception)])

        return results

# Example Usage
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    endpoints = [
        "https://funfinal.azurewebsites.net/api/count_verbs?source"
    ]

    orchestrator = DistributedOrchestrator(endpoints)
    results = asyncio.run(orchestrator.process_file(DEFAULT_FILE_NAME))

    total_spaces = sum(result.get('space_count', 0) for result in results)

    print(f"Total verbs processed: {orchestrator.total_verb_count}")
# NEW: Display detailed verb category counts
    print("Verb breakdown:")
    for category, count in orchestrator.verb_counts.items():
        print(f"  {category.capitalize()} verbs: {count}")




# Creating the histogram
categories = list(orchestrator.verb_counts.keys())
counts = list(orchestrator.verb_counts.values())


plt.bar(categories, counts, color=['#4CAF50', '#2196F3', '#FF9800'])

# Adding labels and title
plt.xlabel('Verb Tenses')
plt.ylabel('Count')
plt.title('Verb Breakdown Histogram')

# Displaying the values on top of each bar
for i, value in enumerate(counts):
    plt.text(i, value + 1000, str(value), ha='center')

plt.show()