Subtitle Translator
A Python GUI application for processing and translating subtitles in MKV video files.

What it does
Renames MKV files with FileBot
Extracts subtitle tracks from MKV files
Cleans subtitle files
Translates subtitles using Google Translate
Embeds translated subtitles back into the MKV file
Requirements
Before using this app, you need to install:

Python 3.8 or higher - Download from https://www.python.org/downloads/
FileBot - Download from https://www.filebot.net/
MKVToolNix - Download from https://mkvtoolnix.download/
Installation Steps
Step 1: Install Python libraries
Open Terminal (Mac) or Command Prompt (Windows) and run:


pip install pysrt chardet requests tkinterdnd2
Step 2: Download the script
Download Translate_embed_sub_Final.py from this repository (click the file, then click "Download")

Step 3: Update file paths (IMPORTANT!)
Open the Python file in a text editor and find these lines near the top:

FILEBOT_PATH - Change to where FileBot is installed on your computer
MKVTOOLNIX_APP_PATH - Change to where MKVToolNix is installed on your computer
How to Use
Run the app: Double-click Translate_embed_sub_Final.py or run in terminal:


python Translate_embed_sub_Final.py
The GUI window will open with several steps:

Step 0: Rename and extract - Select your MKV folder and let it extract subtitles
Step 1: Clean subtitles - Cleans up subtitle formatting
Step 2: Translate - Choose source and target languages, then translate
Step 3: Embed - Puts translated subtitles back into your MKV files
Easy mode: Click "Run All Steps" to do everything automatically!

Troubleshooting
If you get errors about FileBot or MKVToolNix not found, check that the paths in the script match where you installed them
Make sure all Python libraries are installed correctly
The app needs internet connection for translation and cleaning
Questions?
Open an issue on this GitHub repository if you need help!
