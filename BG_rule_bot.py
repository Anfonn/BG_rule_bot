import os
import io
import requests
from bs4 import BeautifulSoup
from google.cloud import storage
from google.cloud import vision_v1 as vision
import json
import re
import logging
import fnmatch

# Initialize logger
logging.basicConfig(level=logging.INFO)

# Define service account path
SERVICE_ACCOUNT_PATH = '...'
os.environ["OPENAI_API_KEY"] = "..."

class Downloader:
    def __init__(self, game):
        self.game = game
        self.game_directory = self.sanitize_file_title(game)
        self.url = f'https://en.1jour-1jeu.com/rules/search?q={self.game}'
        self.storage_client = storage.Client.from_service_account_json(SERVICE_ACCOUNT_PATH)

    def crawler(self):
        try:
            response = requests.get(self.url)
            response.raise_for_status()
        except requests.exceptions.HTTPError as err:
            logging.error(f"HTTP error occurred: {err}")
        except Exception as err:
            logging.error(f"Other error occurred: {err}")
        else:
            soup = BeautifulSoup(response.text, 'html.parser')
            files = []
            for link in soup.find_all('a', class_='dark-link'):
                file_url = link.get('href')
                if file_url.endswith('.pdf'):
                    file_title = link.text
                    files.append((file_title, file_url))
            return files

    @staticmethod
    def sanitize_file_title(file_title):
        invalid_chars = ["<", ">", ":", "\"", "/", "\\", "|", "?", "*"]
        for char in invalid_chars:
            file_title = file_title.replace(char, "_")
        return file_title

    def download(self, file_title, file_url):
        sanitized_title = self.sanitize_file_title(file_title)
        blob_name = f'{self.game_directory}/{sanitized_title}.pdf'
        bucket = self.storage_client.bucket('bg_rule_bot')  # Set the bucket name to a static string
        blob = bucket.blob(blob_name)
        # Check if the bucket exists and if not, create it
        if not bucket.exists():
            logging.info(f"Bucket {bucket.name} does not exist. Creating...")
            bucket = self.storage_client.create_bucket(bucket.name)
            logging.info(f"Bucket {bucket.name} created.")
        blob = bucket.blob(blob_name)
        try:
            response = requests.get(file_url, stream=True)
            response.raise_for_status()
        except requests.exceptions.HTTPError as err:
            logging.error(f"HTTP error occurred: {err}")
        except Exception as err:
            logging.error(f"Other error occurred: {err}")
        else:
            response.raw.decode_content = True
            chunk_size = 1024 * 1024  # 1 MB
            with io.BytesIO() as file_obj:
                for chunk in response.iter_content(chunk_size):
                    if chunk:
                        file_obj.write(chunk)
                file_obj.seek(0)
                blob.upload_from_file(file_obj)

class Rulebook:
    def __init__(self, title, game_directory):
        self.title = title
        self.game_directory = game_directory
        self.path = f'gs://bg_rule_bot/{game_directory}/{Downloader.sanitize_file_title(title)}.pdf'

    @staticmethod
    def search_Rulebook(game):
        storage_client = storage.Client.from_service_account_json(SERVICE_ACCOUNT_PATH)
        bucket = storage_client.bucket('bg_rule_bot')
        sanitized_game = Downloader.sanitize_file_title(game)
        blobs = bucket.list_blobs(prefix=sanitized_game)
        matching_blobs = [blob for blob in blobs if sanitized_game in blob.name]
        if matching_blobs:
            logging.info(f"The following rulebooks match the keyword '{game}':")
            for blob in matching_blobs:
                logging.info(blob.name.split('/')[-1])  # Only print the blob name (not the full path)
        else:
            logging.info(f"No rulebooks found matching the keyword '{game}'.")

    def perform_ocr(self):
        client = vision.ImageAnnotatorClient.from_service_account_file(SERVICE_ACCOUNT_PATH)
        # Create a GcsSource object
        gcs_source_uri = self.path
        gcs_source = vision.GcsSource(uri=gcs_source_uri)
        # Create an InputConfig object
        input_config = vision.InputConfig(gcs_source=gcs_source, mime_type='application/pdf')
        # Create a GcsDestination object
        gcs_destination_uri = f'{self.path[:-4]}_output/'
        gcs_destination = vision.GcsDestination(uri=gcs_destination_uri)
        # Create an OutputConfig object
        output_config = vision.OutputConfig(gcs_destination=gcs_destination, batch_size=1)
        # Create a Feature object
        feature = vision.Feature(type_='DOCUMENT_TEXT_DETECTION')
        # Create an AsyncAnnotateFileRequest object
        async_request = vision.AsyncAnnotateFileRequest(features=[feature], input_config=input_config, output_config=output_config)
        operation = client.async_batch_annotate_files(requests=[async_request])
        logging.info('Waiting for the operation to finish.')
        operation.result(timeout=420)

    @staticmethod
    def clean_text_generic(text):
        # Remove non-alphanumeric or regular special characters (preserve spaces, periods, commas, hyphens, apostrophes)
        text = re.sub(r"[^a-zA-Z0-9\s.,'()-]", "", text)
        # Replace multiple spaces with a single space
        text = re.sub(r"\s+", " ", text)
        # Remove inconsistent quotation marks
        text = text.replace("\"", "")
        text = text.replace("\'", "")
        # De-hyphenate words at line breaks
        text = text.replace(" - ", " ")
        return text

    def create_text_files(self):
        storage_client = storage.Client.from_service_account_json(SERVICE_ACCOUNT_PATH)
        match = re.match(r'gs://([^/]+)/(.+)', f'{self.path[:-4]}_output/')
        bucket_name = match.group(1)
        prefix = match.group(2)
        bucket = storage_client.get_bucket(bucket_name)
        blob_list = list(bucket.list_blobs(prefix=prefix))
        # Filter blob_list to include only .json files
        blob_list = [blob for blob in blob_list if fnmatch.fnmatch(blob.name, '*output-*.json')]
        logging.info('Output files:')
        page_texts = {}
        for blob in blob_list:
            json_string = blob.download_as_text()
            response = json.loads(json_string)
            page_text = ''
            for image_response in response['responses']:
                for page in image_response['fullTextAnnotation']['pages']:
                    for block in page['blocks']:
                        for paragraph in block['paragraphs']:
                            for word in paragraph['words']:
                                word_text = ''.join([symbol['text'] for symbol in word['symbols']])
                                page_text += ' ' + word_text
            # Clean the page text
            page_text = self.clean_text_generic(page_text)
            # Extract the page number from the blob name
            blob_name = blob.name
            page_number = int(blob_name.split('-')[1])
            page_texts[page_number] = page_text
            # Save the cleaned page's text to a new blob
            text_blob = bucket.blob(f'{prefix}_page_{page_number}.txt')
            text_blob.upload_from_string(page_text)
    
    def create_pages(self):
        self.perform_ocr()
        self.create_text_files()

class GameWorkflow:
    @staticmethod
    def run_workflow():
        game = input("Enter the name of a game to search for: ")       
        Rulebook.search_Rulebook(game) # Search for the game on the GCS bucket
        choice = input("Do you want to search for the game on the internet? (yes/no) ")
        if choice.lower() == 'yes':
            downloader = Downloader(game) # Create a Downloader instance
            files = downloader.crawler() # Search for the game on the website
            if not files:
                logging.info("No games found.")
                return
            logging.info("Found the following games:")
            for i, (file_title, _) in enumerate(files):
                logging.info(f"{i+1}. {file_title}")
            selections = input("Enter the numbers of the games you want to download, separated by spaces: ")
            selections = [int(x) for x in selections.split()]
            for selection in selections: # Download and perform OCR on the selected games
                file_title, file_url = files[selection - 1]
                logging.info(f"Downloading and performing OCR on {file_title}...")
                downloader.download(file_title, file_url)
                rulebook = Rulebook(file_title, downloader.game_directory)
                rulebook.create_pages()
                logging.info(f"Processing for {file_title} is completed!")
            logging.info("All selected games are ready!")

if __name__ == "__main__":
    GameWorkflow.run_workflow()



#def rulebook_chat(self):
#    loader = DirectoryLoader(self.title, show_progress=True)
#    rulebook_pages = loader.load()
#    text_splitter = RecursiveCharacterTextSplitter(chunk_size=500, chunk_overlap=0, separators=[" ", ",", "\n"])
#    documents = text_splitter.split_documents(rulebook_pages)
#    embedding = OpenAIEmbeddings()
#    metadata = [{"source": str(i)} for i in range(len(documents))]
#    vectorstore = Chroma.from_documents(documents=documents, embedding=embedding, metadata=metadata)
#    query = input("What is your question? ")
#    memory = ConversationBufferMemory(memory_key="chat_history", return_messages=True)
#    qa = ConversationalRetrievalChain.from_llm(OpenAI(temperature=0), vectorstore.as_retriever(), memory=memory)
#    result = qa({"question": query})
#    result["answer"]
