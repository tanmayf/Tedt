
import os
import asyncio
import zipfile
import re
from pyrogram import Client, filters, types
from pyrogram.handlers import MessageHandler
from pymongo import MongoClient
from io import BytesIO
import logging
import concurrent.futures

# Configuration (replace with your actual values)
API_ID = 23904398  # Your Telegram API ID
API_HASH = "c55934364222dc3d4155320d2ced1359"  # Your Telegram API Hash
BOT_TOKEN = "7658008644:AAGtekRQRLwbu9b-bW6mJ2-w1BZGVeHJVDE"  # Your Bot Token
MONGO_URI = "mongodb+srv://orzipdrin:w3WCKeMMR8Qg12zQ@cluster0.7p8qh.mongodb.net/?retryWrites=true&w=majority"  # Your MongoDB connection string
DB_NAME = "zip_extractor_bot"
COLLECTION_NAME = "user_files"
DOWNLOAD_DIR = "downloads"  # Directory to store downloaded files

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

# Initialize Pyrogram Client
app = Client("zip_extractor_bot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)

# Initialize MongoDB client
mongo_client = MongoClient(MONGO_URI)
db = mongo_client[DB_NAME]
collection = db[COLLECTION_NAME]

# Create download directory if it doesn't exist
if not os.path.exists(DOWNLOAD_DIR):
    os.makedirs(DOWNLOAD_DIR)


# Function to process the file extraction in a separate thread
def extract_zip_and_upload(user_id, zip_file_path, download_dir):
    try:
        output_dir = os.path.join(download_dir, f"extracted_{user_id}")
        os.makedirs(output_dir, exist_ok=True)

        with zipfile.ZipFile(zip_file_path, "r") as zip_ref:
            zip_ref.extractall(output_dir)

        extracted_files = [
            os.path.join(output_dir, f) for f in os.listdir(output_dir)
        ]
        return extracted_files, None  # Return files and no error
    except Exception as e:
        logger.error(f"Error extracting zip for user {user_id}: {e}")
        return None, str(e)  # Return None and the error message


# Handlers
@app.on_message(filters.command("merge"))
async def merge_command(client, message):
    user_id = message.from_user.id
    # Initialize user's file list in the database
    collection.update_one(
        {"user_id": user_id}, {"$set": {"files": [], "state": "waiting_for_files"}}, upsert=True
    )
    await message.reply_text(
        "Please send all parts of the split ZIP file, then type /done when finished."
    )


@app.on_message(filters.document)
async def file_upload(client, message):
    user_id = message.from_user.id
    user_data = collection.find_one({"user_id": user_id})

    if user_data and user_data.get("state") == "waiting_for_files":
        file_name = message.document.file_name
        if re.match(r".*\.zip\.\d{3}$", file_name):  # Check for .zip.001, .zip.002, etc.
            file_path = os.path.join(DOWNLOAD_DIR, str(user_id), file_name)
            os.makedirs(os.path.dirname(file_path), exist_ok=True)  # Ensure directory exists

            try:
                await message.download(file_path)  # Download file to the designated path

                # Update database with the downloaded file
                collection.update_one(
                    {"user_id": user_id}, {"$push": {"files": file_path}}
                )
                await message.reply_text(f"File '{file_name}' received.")
            except Exception as e:
                logger.error(f"Error downloading file for user {user_id}: {e}")
                await message.reply_text(f"Error downloading file: {e}")

        else:
            await message.reply_text(
                "Invalid file name.  Please send files with the format .zip.001, .zip.002, etc."
            )


@app.on_message(filters.command("done"))
async def done_command(client, message):
    user_id = message.from_user.id
    user_data = collection.find_one({"user_id": user_id})

    if not user_data or user_data.get("state") != "waiting_for_files":
        await message.reply_text(
            "Please start by using the /merge command first, and then send the files."
        )
        return

    files = user_data.get("files")

    if not files:
        await message.reply_text("No files received. Please upload the split ZIP files.")
        return

    # Sort files to ensure correct merging order
    files.sort()

    try:
        # Create the merged zip file
        merged_zip_path = os.path.join(DOWNLOAD_DIR, str(user_id), "merged.zip")
        os.makedirs(os.path.dirname(merged_zip_path), exist_ok=True)

        with open(merged_zip_path, "wb") as merged_zip:
            for part in files:
                with open(part, "rb") as part_file:
                    while True:
                        chunk = part_file.read(4096)  # Adjust chunk size if needed
                        if not chunk:
                            break
                        merged_zip.write(chunk)

        await message.reply_text("Merging files... Please wait.")

        # Extract ZIP in a separate thread
        loop = asyncio.get_event_loop()
        with concurrent.futures.ThreadPoolExecutor() as pool:
            extracted_files, error_message = await loop.run_in_executor(
                pool, extract_zip_and_upload, user_id, merged_zip_path, DOWNLOAD_DIR
            )

        if error_message:
            await message.reply_text(f"Error extracting ZIP: {error_message}")
            return

        if extracted_files:
            await message.reply_text(
                "ZIP extraction complete. Uploading files..."
            )  # Optional: Display upload progress if needed

            for file_path in extracted_files:
                try:
                    await client.send_document(
                        chat_id=message.chat.id, document=file_path
                    )
                except Exception as e:
                    logger.error(
                        f"Error uploading file {file_path} for user {user_id}: {e}"
                    )
                    await message.reply_text(
                        f"Error uploading {os.path.basename(file_path)}: {e}"
                    )
            await message.reply_text("All files uploaded.")
        else:
            await message.reply_text("No files were extracted.")

    except Exception as e:
        logger.error(f"Error merging or extracting files for user {user_id}: {e}")
        await message.reply_text(f"An error occurred: {e}")

    finally:
        # Cleanup: Reset user's data in the database. Important!

@app.on_message(filters.command("done"))
async def done_command(client, message):
    user_id = message.from_user.id
    user_data = collection.find_one({"user_id": user_id})

    if not user_data or user_data.get("state") != "waiting_for_files":
        await message.reply_text(
            "Please start by using the /merge command first, and then send the files."
        )
        return

    files = user_data.get("files")

    if not files:
        await message.reply_text("No files received. Please upload the split ZIP files.")
        return

    # Sort files to ensure correct merging order
    files.sort()

    try:
        # Create the merged zip file
        merged_zip_path = os.path.join(DOWNLOAD_DIR, str(user_id), "merged.zip")
        os.makedirs(os.path.dirname(merged_zip_path), exist_ok=True)

        with open(merged_zip_path, "wb") as merged_zip:
            for part in files:
                with open(part, "rb") as part_file:
                    while True:
                        chunk = part_file.read(4096)  # Adjust chunk size if needed
                        if not chunk:
                            break
                        merged_zip.write(chunk)

        await message.reply_text("Merging files... Please wait.")

        # Extract ZIP in a separate thread
        loop = asyncio.get_event_loop()
        with concurrent.futures.ThreadPoolExecutor() as pool:
            extracted_files, error_message = await loop.run_in_executor(
                pool, extract_zip_and_upload, user_id, merged_zip_path, DOWNLOAD_DIR
            )

        if error_message:
            await message.reply_text(f"Error extracting ZIP: {error_message}")
            return

        if extracted_files:
            await message.reply_text(
                "ZIP extraction complete. Uploading files..."
            )
            for file_path in extracted_files:
                try:
                    await client.send_document(
                        chat_id=message.chat.id, document=file_path
                    )
                except Exception as e:
                    logger.error(
                        f"Error uploading file {file_path} for user {user_id}: {e}"
                    )
                    await message.reply_text(
                        f"Error uploading {os.path.basename(file_path)}: {e}"
                    )
            await message.reply_text("All files uploaded.")
        else:
            await message.reply_text("No files were extracted.")

    except Exception as e:
        logger.error(f"Error merging or extracting files for user {user_id}: {e}")
        await message.reply_text(f"An error occurred: {e}")

    finally:
        # Cleanup: Reset user's data in the database. Important!
        collection.update_one(
            {"user_id": user_id}, {"$set": {"files": [], "state": "idle"}}
        )
        # OPTIONAL: Clean up downloaded files to save space. Be VERY careful with this.
        try:
            user_download_dir = os.path.join(DOWNLOAD_DIR, str(user_id))
            if os.path.exists(user_download_dir):
                for root, dirs, files in os.walk(user_download_dir):
                    for file in files:
                        os.remove(os.path.join(root, file))
                    for dir in dirs:
                        os.rmdir(os.path.join(root, dir))
                os.rmdir(user_download_dir) # Remove the user's directory after cleaning up
        except Exception as e:
            logger.warning(f"Error cleaning up files for user {user_id}: {e}")
# Error Handler (optional, but good practice)
@app.on_message(filters.command("start"))
async def start_command(client, message):
    await message.reply_text("Welcome! Use /merge to start extracting split ZIP files.")


@app.on_message(filters.command("help"))
async def help_command(client, message):
    await message.reply_text(
        "Use /merge to start, then upload ZIP parts. Use /done when finished."
    )


@app.on_message(filters.command("cancel"))
async def cancel_command(client, message):
    user_id = message.from_user.id
    user_data = collection.find_one({"user_id": user_id})

    if user_data and user_data.get("state") == "waiting_for_files":
        # Reset user's data in the database
        collection.update_one(
            {"user_id": user_id}, {"$set": {"files": [], "state": "idle"}}
        )
        await message.reply_text("Operation cancelled. You can start again with /merge")
    else:
        await message.reply_text("No operation in progress to cancel.")


@app.on_message()
async def echo(client, message):
    # Catch-all for handling unexpected messages while in "waiting_for_files" state
    user_id = message.from_user.id
    user_data = collection.find_one({"user_id": user_id})
    if user_data and user_data.get("state") == "waiting_for_files":
        await message.reply_text(
            "Please send only .zip.xxx files or use /done when finished, or /cancel to quit"
        )


# Run the bot
if __name__ == "__main__":
    print("Bot started!")
    app.run()
