#!/usr/bin/env python3
"""
Subtitle Processor & Translator (MKV Support)

Workflow overview
-----------------
Step 0 : Rename MKV files with FileBot and extract subtitle tracks (mkvextract)
Step 1 : Clean subtitle files through the SubtitleTools API
Step 2 : Translate cleaned subtitles via Google Translate web endpoints
Step 3 : Embed translated subtitles back into the source MKV (overwrite)

A “Run All Steps” button executes the entire pipeline automatically.

Requirements
------------
- Python 3.8+
- pip install pysrt chardet requests tkinterdnd2
- FileBot installed (macOS path assumed below, adjust as needed)
- MKVToolNix installed (macOS .app path assumed below, adjust as needed)

This script uses Tkinter for the GUI and relies on external command-line tools
(FileBot, mkvmerge, mkvextract) plus an online API (SubtitleTools) for cleaning.
"""

import os
import re
import json
import time
import random
import traceback
import subprocess
import collections
import concurrent.futures
import multiprocessing
import platform
from threading import Thread, Lock

import tkinter as tk
from tkinter import (
    filedialog,
    messagebox,
    colorchooser,
    scrolledtext,
    Toplevel,
    ttk,
)

import pysrt
import chardet
import requests

# ---------------------------------------------------------------------------
# Drag & Drop support
# ---------------------------------------------------------------------------
try:
    from tkinterdnd2 import TkinterDnD, DND_FILES
except ImportError:
    messagebox.showerror(
        "Missing Dependency",
        "The 'tkinterdnd2' library is required for drag-and-drop functionality.\n"
        "Install it via: pip install tkinterdnd2",
    )
    raise SystemExit(1)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
CLEANER_API_KEY = "Bearer st_live_f2e2e7c533019ef83c611e"  # Replace with your own key if needed.

FILEBOT_PATH = "/Applications/FileBot.app/Contents/MacOS/filebot.sh"
MKVTOOLNIX_APP_PATH = "/Applications/MKVToolNix-89.0.app"
MKVTOOLNIX_EXEC_PATH = os.path.join(MKVTOOLNIX_APP_PATH, "Contents/MacOS")
MKVMERGE_PATH = os.path.join(MKVTOOLNIX_EXEC_PATH, "mkvmerge")
MKVEXTRACT_PATH = os.path.join(MKVTOOLNIX_EXEC_PATH, "mkvextract")

MAX_FAILURES_PER_LINE = 3  # Maximum translation attempts per subtitle line.

ISO_639_2_CODES = {
    "af": "afr", "sq": "alb", "am": "amh", "ar": "ara", "az": "aze", "be": "bel",
    "bg": "bul", "bn": "ben", "bs": "bos", "ca": "cat", "ceb": "ceb", "co": "cos",
    "cs": "ces", "cze": "ces", "cy": "cym", "da": "dan", "de": "ger", "ger": "ger",
    "deu": "ger", "el": "ell", "en": "eng", "eo": "epo", "es": "spa", "et": "est",
    "eu": "eus", "fa": "fas", "fi": "fin", "fil": "fil", "fr": "fre", "fre": "fre",
    "fra": "fre", "ga": "gle", "gl": "glg", "gu": "guj", "ha": "hau", "haw": "haw",
    "he": "heb", "iw": "heb", "hi": "hin", "hmn": "hmn", "hr": "hrv", "hu": "hun",
    "hy": "hye", "id": "ind", "ig": "ibo", "is": "isl", "it": "ita", "ja": "jpn",
    "jw": "jav", "ka": "kat", "kk": "kaz", "km": "khm", "kn": "kan", "ko": "kor",
    "ku": "kur", "ky": "kir", "la": "lat", "lb": "ltz", "lo": "lao", "lt": "lit",
    "lv": "lav", "mg": "mlg", "mi": "mao", "mk": "mkd", "ml": "mal", "mn": "mon",
    "mr": "mar", "ms": "msa", "mt": "mlt", "my": "mya", "ne": "nep", "nl": "dut",
    "dut": "dut", "nld": "dut", "no": "nor", "ny": "nya", "or": "ori", "pa": "pan",
    "pl": "pol", "ps": "pus", "pt": "por", "ro": "rum", "rum": "rum", "ron": "rum",
    "ru": "rus", "rw": "kin", "sd": "snd", "si": "sin", "sk": "slk", "sl": "slv",
    "sm": "smo", "sn": "sna", "so": "som", "sq": "alb", "sr": "srp", "st": "sot",
    "su": "sun", "sv": "swe", "sw": "swa", "ta": "tam", "te": "tel", "tg": "tgk",
    "th": "tha", "ti": "tir", "tk": "tuk", "tl": "tgl", "tr": "tur", "tt": "tat",
    "ug": "uig", "uk": "ukr", "ur": "urd", "uz": "uzb", "vi": "vie", "xh": "xho",
    "yi": "yid", "yo": "yor", "zh": "chi", "zh-cn": "chi", "zh-tw": "chi", "zu": "zul",
}

# ---------------------------------------------------------------------------
# Helper utilities
# ---------------------------------------------------------------------------
def run_command(command, working_dir=None):
    """Run an external command and return (success_flag, stdout_or_error)."""
    try:
        startupinfo = None
        if platform.system() == "Windows":
            startupinfo = subprocess.STARTUPINFO()
            startupinfo.dwFlags |= subprocess.STARTF_USESHOWWINDOW
            startupinfo.wShowWindow = subprocess.SW_HIDE

        process = subprocess.Popen(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            encoding="utf-8",
            errors="replace",
            cwd=working_dir,
            startupinfo=startupinfo,
        )
        stdout, stderr = process.communicate()
        if process.returncode != 0:
            if command[0] == FILEBOT_PATH and "Processed" in stdout:
                return True, stdout
            return False, f"Error (code {process.returncode}):\n{stderr}\n{stdout}"
        return True, stdout
    except FileNotFoundError:
        return False, f"Command not found: '{command[0]}'. Check installation/path."
    except Exception as exc:
        return False, f"Exception while running command: {exc}"


# ---------------------------------------------------------------------------
# Dialog for selecting subtitle tracks from MKV
# ---------------------------------------------------------------------------
class TrackSelectionDialog(Toplevel):
    def __init__(self, parent, tracks, mkv_filename):
        super().__init__(parent)
        self.title(f"Select Subtitles – {os.path.basename(mkv_filename)}")
        self.geometry("500x360")
        self.transient(parent)
        self.grab_set()

        self.tracks = tracks
        self.selected_tracks = []
        self.check_vars = []

        main_frame = ttk.Frame(self, padding=12)
        main_frame.pack(fill="both", expand=True)

        ttk.Label(
            main_frame,
            text="Select subtitle tracks to extract:",
            font="-weight bold",
        ).pack(anchor="w", pady=(0, 10))

        canvas = tk.Canvas(main_frame)
        scrollbar = ttk.Scrollbar(main_frame, orient="vertical", command=canvas.yview)
        scroll_frame = ttk.Frame(canvas)

        scroll_frame.bind(
            "<Configure>",
            lambda _: canvas.configure(scrollregion=canvas.bbox("all")),
        )

        canvas.create_window((0, 0), window=scroll_frame, anchor="nw")
        canvas.configure(yscrollcommand=scrollbar.set)

        canvas.pack(side="left", fill="both", expand=True)
        scrollbar.pack(side="right", fill="y")

        for track in self.tracks:
            var = tk.BooleanVar()
            self.check_vars.append((track["id"], var))

            lang = track["properties"].get(
                "language_ietf", track["properties"].get("language", "und")
            )
            codec = track.get("codec", "unknown")
            name = track["properties"].get("track_name", "No Name")
            label = f"Track {track['id']}: {lang.upper()} / {codec} – {name}"

            cb = ttk.Checkbutton(scroll_frame, text=label, variable=var)
            cb.pack(anchor="w", padx=6, pady=3)

            if codec == "SubRip/SRT" and lang.lower() in {"en", "eng"}:
                cb.invoke()

        button_frame = ttk.Frame(self, padding=12)
        button_frame.pack(fill="x")

        ttk.Button(button_frame, text="Extract Selected", command=self.on_ok).pack(
            side="right", padx=6
        )
        ttk.Button(button_frame, text="Cancel", command=self.on_cancel).pack(
            side="right"
        )

        self.protocol("WM_DELETE_WINDOW", self.on_cancel)
        self.wait_window(self)

    def on_ok(self):
        for track_id, var in self.check_vars:
            if var.get():
                self.selected_tracks.append(track_id)
        self.destroy()

    def on_cancel(self):
        self.selected_tracks.clear()
        self.destroy()


# ---------------------------------------------------------------------------
# Main GUI application
# ---------------------------------------------------------------------------
class CombinedSubtitleProcessorGUI:
    def __init__(self, root):
        self.root = root
        self.root.title("Subtitle Processor & Translator (MKV Support)")

        # File tracking
        self.selected_files = []
        self.cleaned_files = []
        self.output_files = []
        self.embedding_results = []
        self.srt_to_mkv_map = {}
        self.processed_mkv_paths = set()

        # Process flags
        self.translation_in_progress = False
        self.cleaning_in_progress = False
        self.mkv_processing_in_progress = False
        self.embedding_in_progress = False
        self.run_all_in_progress = False
        self.run_all_current_step = None

        # Settings
        self.subtitle_color = "#FFFF00"  # default yellow
        self.translation_cache = {}
        self.cache_hits = 0
        self.request_count = 0
        self.error_count = 0
        self.untranslated_line_details = {}
        self.final_untranslated_count = 0

        self.detect_system()
        self.setup_gui()
        self.check_external_tools()

        self.api_endpoints = [
            "https://translate.googleapis.com/translate_a/single",
            "https://clients5.google.com/translate_a/t",
            "https://translate.google.com/translate_a/single",
        ]
        self.current_endpoint_index = 0
        self.endpoint_lock = Lock()

    # ------------------------------------------------------------------
    # Startup checks and GUI setup
    # ------------------------------------------------------------------
    def detect_system(self):
        self.is_apple_silicon = (
            platform.system() == "Darwin"
            and platform.machine().lower().startswith(("arm", "m1", "m2", "m3", "m4"))
        )
        try:
            cpu_count = multiprocessing.cpu_count()
        except NotImplementedError:
            cpu_count = 4
        if self.is_apple_silicon:
            self.recommended_workers = min(40, cpu_count * 5)
        else:
            self.recommended_workers = min(20, cpu_count * 3)

    def check_external_tools(self):
        self.log("Checking external tools...")
        missing = []

        if not os.path.exists(FILEBOT_PATH):
            missing.append(f"FileBot not found at {FILEBOT_PATH}")
        if not os.path.exists(MKVMERGE_PATH):
            missing.append(f"mkvmerge not found at {MKVMERGE_PATH}")
        if not os.path.exists(MKVEXTRACT_PATH):
            missing.append(f"mkvextract not found at {MKVEXTRACT_PATH}")

        if missing:
            msg = " • " + "\n • ".join(missing)
            self.log(f"ERROR:\n{msg}")
            messagebox.showerror(
                "Missing External Tools",
                "The following tools are required but were not found:\n\n"
                f"{msg}\n\nPlease adjust paths in the script.",
                parent=self.root,
            )
        else:
            self.log("All external tools located successfully.")

    def setup_gui(self):
        self.root.geometry("870x800")
        self.root.minsize(780, 680)
        self.root.grid_columnconfigure(0, weight=1)
        self.root.grid_rowconfigure(0, weight=1)

        self.notebook = ttk.Notebook(self.root)
        self.notebook.grid(row=0, column=0, sticky="nsew", padx=10, pady=10)

        # Tabs
        self.workflow_tab = ttk.Frame(self.notebook)
        self.settings_tab = ttk.Frame(self.notebook)
        self.cleaning_tab = ttk.Frame(self.notebook)
        self.log_tab = ttk.Frame(self.notebook)

        self.notebook.add(self.workflow_tab, text="Workflow")
        self.notebook.add(self.settings_tab, text="Translation Settings")
        self.notebook.add(self.cleaning_tab, text="Cleaning Settings")
        self.notebook.add(self.log_tab, text="Logs")

        # Layout configuration per tab
        self.workflow_tab.grid_rowconfigure(0, weight=1)
        self.workflow_tab.grid_columnconfigure(0, weight=1)

        self.settings_tab.grid_columnconfigure(0, weight=1)
        self.cleaning_tab.grid_columnconfigure(0, weight=1)
        self.log_tab.grid_rowconfigure(0, weight=1)
        self.log_tab.grid_columnconfigure(0, weight=1)

        # Build individual tabs
        self.create_workflow_tab()
        self.create_settings_tab()
        self.create_cleaning_tab()
        self.create_log_tab()

        # Status bar initialization
        try:
            cpu_count = multiprocessing.cpu_count()
        except NotImplementedError:
            cpu_count = 4
        status = f"Ready – Drag & Drop *.srt / *.mkv files (Detected CPU cores: {cpu_count})"
        if self.is_apple_silicon:
            status += " – Apple Silicon optimized"
        self.status_label.config(text=status)

        self.root.protocol("WM_DELETE_WINDOW", self.on_closing)

    # ------------------------------------------------------------------
    # Tab builders
    # ------------------------------------------------------------------
    def create_workflow_tab(self):
        # File list
        file_frame = ttk.LabelFrame(
            self.workflow_tab,
            text="Files – drag & drop *.mkv or *.srt here (or use Add)",
            padding=12,
        )
        file_frame.grid(row=0, column=0, sticky="nsew", pady=(5, 10))
        file_frame.grid_rowconfigure(0, weight=1)
        file_frame.grid_columnconfigure(0, weight=1)

        self.files_listbox = tk.Listbox(file_frame, selectmode=tk.EXTENDED, bg="#f2f2f2")
        file_scroll = ttk.Scrollbar(file_frame, orient="vertical", command=self.files_listbox.yview)
        self.files_listbox.configure(yscrollcommand=file_scroll.set)
        self.files_listbox.grid(row=0, column=0, sticky="nsew")
        file_scroll.grid(row=0, column=1, sticky="ns")

        self.files_listbox.drop_target_register(DND_FILES)
        self.files_listbox.dnd_bind("<<Drop>>", self.handle_drop)

        # File buttons
        btn_frame = ttk.Frame(file_frame)
        btn_frame.grid(row=1, column=0, columnspan=2, pady=(8, 0))

        self.add_button = ttk.Button(btn_frame, text="Add Files", command=self.browse_files)
        self.add_button.pack(side="left", padx=4)
        self.remove_button = ttk.Button(btn_frame, text="Remove Selected", command=self.remove_selected)
        self.remove_button.pack(side="left", padx=4)
        self.clear_button = ttk.Button(btn_frame, text="Clear All", command=self.clear_files)
        self.clear_button.pack(side="left", padx=4)

        # Step buttons
        steps_frame = ttk.Frame(self.workflow_tab)
        steps_frame.grid(row=1, column=0, sticky="ew", pady=5)
        for i in range(5):
            steps_frame.grid_columnconfigure(i, weight=1)

        self.mkv_button = ttk.Button(
            steps_frame,
            text="Step 0: Process MKV(s)",
            command=self.start_mkv_processing,
            state="disabled",
        )
        self.clean_button = ttk.Button(
            steps_frame,
            text="Step 1: Clean Subtitles",
            command=self.start_cleaning,
            state="disabled",
        )
        self.translate_button = ttk.Button(
            steps_frame,
            text="Step 2: Translate Subtitles",
            command=self.start_translation,
            state="disabled",
        )
        self.embed_button = ttk.Button(
            steps_frame,
            text="Step 3: Embed Subtitles",
            command=self.start_embedding,
            state="disabled",
        )
        self.run_all_button = ttk.Button(
            steps_frame,
            text="Run All Steps",
            command=self.run_all_steps,
            state="disabled",
        )

        self.mkv_button.grid(row=0, column=0, padx=4, sticky="ew")
        self.clean_button.grid(row=0, column=1, padx=4, sticky="ew")
        self.translate_button.grid(row=0, column=2, padx=4, sticky="ew")
        self.embed_button.grid(row=0, column=3, padx=4, sticky="ew")
        self.run_all_button.grid(row=0, column=4, padx=4, sticky="ew")

        # Progress section
        progress_frame = ttk.LabelFrame(self.workflow_tab, text="Progress", padding=12)
        progress_frame.grid(row=2, column=0, sticky="ew", pady=(0, 10))
        progress_frame.grid_columnconfigure(0, weight=1)

        self.status_label = ttk.Label(progress_frame, text="Status: Idle")
        self.status_label.grid(row=0, column=0, sticky="w", pady=(0, 6))
        self.stats_label = ttk.Label(progress_frame, text="")
        self.stats_label.grid(row=1, column=0, sticky="w")

        self.progress_var = tk.DoubleVar()
        self.progress_bar = ttk.Progressbar(progress_frame, maximum=100, variable=self.progress_var)
        self.progress_bar.grid(row=2, column=0, sticky="ew", pady=(8, 4))

        self.progress_label = ttk.Label(progress_frame, text="")
        self.progress_label.grid(row=3, column=0, sticky="w")
        self.time_label = ttk.Label(progress_frame, text="")
        self.time_label.grid(row=4, column=0, sticky="w")
        self.detailed_stats = ttk.Label(progress_frame, text="")
        self.detailed_stats.grid(row=5, column=0, sticky="w", pady=(6, 0))
        self.output_label = ttk.Label(progress_frame, text="")
        self.output_label.grid(row=6, column=0, sticky="w", pady=(4, 0))

        # Final buttons
        final_buttons = ttk.Frame(self.workflow_tab)
        final_buttons.grid(row=3, column=0, sticky="ew", pady=(0, 10))
        final_buttons.grid_columnconfigure(0, weight=1)
        final_buttons.grid_columnconfigure(1, weight=1)

        self.open_button = ttk.Button(
            final_buttons,
            text="Open Output Folder",
            command=self.open_output_folder,
            state="disabled",
        )
        self.delete_temp_button = ttk.Button(
            final_buttons,
            text="Delete Temporary Files",
            command=self.delete_temp_files,
            state="disabled",
        )
        self.open_button.grid(row=0, column=0, sticky="ew", padx=4)
        self.delete_temp_button.grid(row=0, column=1, sticky="ew", padx=4)

    def create_settings_tab(self):
        # Language frame
        lang_frame = ttk.LabelFrame(self.settings_tab, text="Language Selection", padding=12)
        lang_frame.grid(row=0, column=0, sticky="ew", padx=10, pady=(10, 8))
        lang_frame.grid_columnconfigure(1, weight=1)

        ttk.Label(lang_frame, text="API:").grid(row=0, column=0, sticky="w", padx=4, pady=4)
        self.api_var = tk.StringVar(value="Google Translate API")
        api_combo = ttk.Combobox(
            lang_frame,
            textvariable=self.api_var,
            values=["Google Translate API"],
            state="readonly",
        )
        api_combo.grid(row=0, column=1, sticky="ew", padx=4, pady=4)
        api_combo.bind("<<ComboboxSelected>>", self.update_languages)

        self.languages = self.get_supported_languages()
        sorted_langs = sorted(self.languages.keys())

        ttk.Label(lang_frame, text="From:").grid(row=1, column=0, sticky="w", padx=4, pady=4)
        self.source_lang = tk.StringVar(value="Auto")
        self.source_combo = ttk.Combobox(
            lang_frame,
            textvariable=self.source_lang,
            values=["Auto"] + sorted_langs,
            state="readonly",
        )
        self.source_combo.grid(row=1, column=1, sticky="ew", padx=4, pady=4)

        ttk.Label(lang_frame, text="To:").grid(row=2, column=0, sticky="w", padx=4, pady=4)
        self.target_lang = tk.StringVar(
            value="Russian" if "Russian" in sorted_langs else (sorted_langs[0] if sorted_langs else "")
        )
        self.target_combo = ttk.Combobox(
            lang_frame,
            textvariable=self.target_lang,
            values=sorted_langs,
            state="readonly",
        )
        self.target_combo.grid(row=2, column=1, sticky="ew", padx=4, pady=4)

        ttk.Label(lang_frame, text="Output Naming Convention:").grid(
            row=3, column=0, sticky="w", padx=4, pady=4
        )
        self.naming_var = tk.StringVar(value="Original.language")
        naming_combo = ttk.Combobox(
            lang_frame,
            textvariable=self.naming_var,
            values=["Original.language", "Original_language", "language.Original"],
            state="readonly",
        )
        naming_combo.grid(row=3, column=1, sticky="ew", padx=4, pady=4)

        # Formatting options
        formatting_frame = ttk.LabelFrame(self.settings_tab, text="Formatting", padding=12)
        formatting_frame.grid(row=1, column=0, sticky="ew", padx=10, pady=(0, 8))
        formatting_frame.grid_columnconfigure(1, weight=1)

        ttk.Label(formatting_frame, text="Subtitle Color:").grid(row=0, column=0, sticky="w", padx=4, pady=4)
        color_frame = ttk.Frame(formatting_frame)
        color_frame.grid(row=0, column=1, sticky="w", padx=4, pady=4)

        self.color_preview = tk.Label(color_frame, text="       ", bg=self.subtitle_color, relief="solid", borderwidth=1)
        self.color_preview.pack(side="left", padx=(0, 6))
        ttk.Button(color_frame, text="Choose", command=self.choose_color).pack(side="left")

        ttk.Label(formatting_frame, text="Save Format:").grid(row=1, column=0, sticky="w", padx=4, pady=4)
        self.format_var = tk.StringVar(value="Standard SRT")
        format_combo = ttk.Combobox(
            formatting_frame,
            textvariable=self.format_var,
            values=["Standard SRT", "ASS/SSA Compatible"],
            state="readonly",
        )
        format_combo.grid(row=1, column=1, sticky="ew", padx=4, pady=4)

        # Performance
        perf_frame = ttk.LabelFrame(self.settings_tab, text="Performance & Retry", padding=12)
        perf_frame.grid(row=2, column=0, sticky="ew", padx=10, pady=(0, 8))
        perf_frame.grid_columnconfigure(1, weight=1)

        ttk.Label(perf_frame, text="Parallel Workers:").grid(row=0, column=0, sticky="w", padx=4, pady=4)
        self.workers_var = tk.StringVar(value=str(self.recommended_workers))
        workers_combo = ttk.Combobox(
            perf_frame,
            textvariable=self.workers_var,
            values=[str(i) for i in range(1, max(8, self.recommended_workers * 2) + 1)],
            state="readonly",
            width=6,
        )
        workers_combo.grid(row=0, column=1, sticky="w", padx=4, pady=4)

        ttk.Label(perf_frame, text="Request Throttling:").grid(row=1, column=0, sticky="w", padx=4, pady=4)
        self.throttle_var = tk.StringVar(value="Adaptive")
        throttle_combo = ttk.Combobox(
            perf_frame,
            textvariable=self.throttle_var,
            values=["None", "Mild", "Moderate", "Adaptive"],
            state="readonly",
            width=10,
        )
        throttle_combo.grid(row=1, column=1, sticky="w", padx=4, pady=4)

        ttk.Label(perf_frame, text="Auto Retry Passes:").grid(row=2, column=0, sticky="w", padx=4, pady=4)
        self.retry_passes_var = tk.StringVar(value="3")
        retry_combo = ttk.Combobox(
            perf_frame,
            textvariable=self.retry_passes_var,
            values=[str(i) for i in range(6)],
            state="readonly",
            width=6,
        )
        retry_combo.grid(row=2, column=1, sticky="w", padx=4, pady=4)
        ttk.Label(
            perf_frame,
            text="(Maximum number of additional retry passes on failed lines)",
        ).grid(row=2, column=2, sticky="w", padx=(6, 4), pady=4)

        self.debug_var = tk.BooleanVar(value=False)
        ttk.Checkbutton(
            perf_frame,
            variable=self.debug_var,
            text="Enable API response debugging",
        ).grid(row=3, column=0, columnspan=3, sticky="w", padx=4, pady=4)

        # Cache information
        cache_frame = ttk.LabelFrame(self.settings_tab, text="Translation Cache", padding=12)
        cache_frame.grid(row=3, column=0, sticky="ew", padx=10, pady=(0, 8))
        cache_frame.grid_columnconfigure(0, weight=1)

        self.cache_label = ttk.Label(cache_frame, text="Cache: 0 entries")
        self.cache_label.grid(row=0, column=0, sticky="w")
        ttk.Button(cache_frame, text="Clear Cache", command=self.clear_cache).grid(row=0, column=1, sticky="e")

        # Test translation button
        test_frame = ttk.Frame(self.settings_tab, padding=10)
        test_frame.grid(row=4, column=0, sticky="sew", padx=10, pady=(0, 10))
        ttk.Button(test_frame, text="Test Translation", command=self.test_translation).pack()

    def create_cleaning_tab(self):
        cleaner_frame = ttk.LabelFrame(self.cleaning_tab, text="Subtitle Cleaning Options", padding=12)
        cleaner_frame.grid(row=0, column=0, sticky="nsew", padx=10, pady=10)
        cleaner_frame.grid_columnconfigure(0, weight=1)

        self.strip_curly_var = tk.BooleanVar(value=True)
        self.strip_square_var = tk.BooleanVar(value=True)
        self.strip_parentheses_var = tk.BooleanVar(value=True)
        self.strip_speaker_labels_var = tk.BooleanVar(value=True)
        self.strip_music_notes_var = tk.BooleanVar(value=True)
        self.remove_uppercase_sdh_var = tk.BooleanVar(value=True)
        self.remove_watermarks_var = tk.BooleanVar(value=True)
        self.remove_formatting_var = tk.BooleanVar(value=True)

        options = [
            ("Strip curly braces { } and content", self.strip_curly_var),
            ("Strip square brackets [ ] and content", self.strip_square_var),
            ("Strip parentheses ( ) and content", self.strip_parentheses_var),
            ("Strip speaker labels (e.g., 'JOHN:')", self.strip_speaker_labels_var),
            ("Strip music cues (♪)", self.strip_music_notes_var),
            ("Remove uppercase SDH cues", self.remove_uppercase_sdh_var),
            ("Remove watermarks / ads", self.remove_watermarks_var),
            ("Remove basic formatting tags (<i>, <b>, <u>, <font>)", self.remove_formatting_var),
        ]

        for row, (text, var) in enumerate(options):
            ttk.Checkbutton(cleaner_frame, text=text, variable=var).grid(
                row=row, column=0, sticky="w", padx=4, pady=2
            )

        temp_frame = ttk.LabelFrame(self.cleaning_tab, text="Temporary Files", padding=12)
        temp_frame.grid(row=1, column=0, sticky="ew", padx=10, pady=(0, 10))
        temp_frame.grid_columnconfigure(0, weight=1)

        self.auto_delete_temp_var = tk.BooleanVar(value=True)
        ttk.Checkbutton(
            temp_frame,
            text="Automatically delete *_cleaned.srt files after successful translation",
            variable=self.auto_delete_temp_var,
        ).grid(row=0, column=0, sticky="w", padx=4, pady=4)

        ttk.Label(
            temp_frame,
            text="If unchecked, cleaned files will be retained in the working folder.",
        ).grid(row=1, column=0, sticky="w", padx=4, pady=4)

        api_frame = ttk.LabelFrame(self.cleaning_tab, text="Cleaning API Info", padding=12)
        api_frame.grid(row=2, column=0, sticky="ew", padx=10, pady=(0, 10))

        masked_key = (
            f"{CLEANER_API_KEY[:10]}...{CLEANER_API_KEY[-4:]}"
            if len(CLEANER_API_KEY) > 14
            else CLEANER_API_KEY
        )
        ttk.Label(
            api_frame,
            text=(
                "Subtitle cleaning uses SubtitleTools.com (internet required).\n"
                f"Configured API Key: {masked_key}"
            ),
            justify="left",
        ).grid(row=0, column=0, sticky="w")

        test_clean_frame = ttk.Frame(self.cleaning_tab, padding=10)
        test_clean_frame.grid(row=3, column=0, sticky="sew", padx=10, pady=(0, 10))
        ttk.Button(test_clean_frame, text="Test Cleaning", command=self.test_cleaning).pack()

    def create_log_tab(self):
        self.log_area = scrolledtext.ScrolledText(
            self.log_tab,
            wrap=tk.WORD,
            state="disabled",
            height=20,
            font=("Courier New", 9),
        )
        self.log_area.grid(row=0, column=0, sticky="nsew", padx=10, pady=10)

        log_btn_frame = ttk.Frame(self.log_tab)
        log_btn_frame.grid(row=1, column=0, sticky="e", padx=10, pady=(0, 10))

        ttk.Button(log_btn_frame, text="Clear Log", command=self.clear_log).pack(side="left", padx=4)
        ttk.Button(log_btn_frame, text="Save Log", command=self.save_log).pack(side="left", padx=4)

    # ------------------------------------------------------------------
    # Logging utilities
    # ------------------------------------------------------------------
    def log(self, message):
        timestamp = time.strftime("%H:%M:%S")
        entry = f"[{timestamp}] {message}\n"
        print(entry.strip())
        if hasattr(self, "log_area") and self.log_area.winfo_exists():
            def append_log():
                self.log_area.config(state="normal")
                self.log_area.insert("end", entry)
                self.log_area.config(state="disabled")
                self.log_area.see("end")
            self.root.after(0, append_log)

    def clear_log(self):
        if hasattr(self, "log_area") and self.log_area.winfo_exists():
            self.log_area.config(state="normal")
            self.log_area.delete("1.0", "end")
            self.log_area.config(state="disabled")
            self.log("Log cleared by user.")

    def save_log(self):
        if not hasattr(self, "log_area") or not self.log_area.winfo_exists():
            messagebox.showwarning("Warning", "Log area not available.", parent=self.root)
            return
        content = self.log_area.get("1.0", "end").strip()
        if not content:
            messagebox.showinfo("Info", "Log is empty.", parent=self.root)
            return

        default_name = f"SubTranslator_Log_{time.strftime('%Y%m%d_%H%M%S')}.log"
        path = filedialog.asksaveasfilename(
            title="Save Log",
            defaultextension=".log",
            filetypes=[
                ("Log files", "*.log"),
                ("Text files", "*.txt"),
                ("All files", "*.*"),
            ],
            initialfile=default_name,
            parent=self.root,
        )
        if path:
            try:
                with open(path, "w", encoding="utf-8") as f:
                    f.write(content + "\n")
                self.log(f"Log saved to {path}")
                messagebox.showinfo("Success", f"Log saved to:\n{path}", parent=self.root)
            except Exception as exc:
                self.log(f"Error saving log: {exc}")
                messagebox.showerror("Error", f"Could not save log:\n{exc}", parent=self.root)

    # ------------------------------------------------------------------
    # File list management
    # ------------------------------------------------------------------
    def handle_drop(self, event):
        try:
            paths = self.root.tk.splitlist(event.data)
            added = 0
            skipped = 0
            for path in paths:
                if os.path.isfile(path) and path.lower().endswith((".srt", ".mkv")):
                    norm_path = os.path.normcase(path)
                    if any(os.path.normcase(p) == norm_path for p in self.selected_files):
                        self.log(f"Skipped duplicate: {os.path.basename(path)}")
                        skipped += 1
                        continue
                    self.selected_files.append(path)
                    self.files_listbox.insert("end", os.path.basename(path))
                    added += 1
                else:
                    self.log(f"Skipped unsupported item: {path}")
                    skipped += 1
            if added:
                self.log(f"Added {added} file(s) via drag & drop.")
                self.reset_outputs_after_file_change()
            elif skipped:
                self.log("No new valid files added via drag & drop.")
        except Exception as exc:
            self.log(f"Error handling drop: {exc}")
            traceback.print_exc()
            messagebox.showerror("Drop Error", f"Failed to process dropped files:\n{exc}", parent=self.root)

    def browse_files(self):
        initial_dir = getattr(self, "last_browse_dir", os.path.expanduser("~"))
        paths = filedialog.askopenfilenames(
            title="Select Subtitle or Video Files",
            filetypes=[
                ("Supported files", "*.srt *.mkv"),
                ("MKV files", "*.mkv"),
                ("SRT files", "*.srt"),
                ("All files", "*.*"),
            ],
            initialdir=initial_dir,
            parent=self.root,
        )
        if not paths:
            return
        self.last_browse_dir = os.path.dirname(paths[0])
        added = 0
        skipped = 0
        for path in paths:
            norm = os.path.normcase(path)
            if any(os.path.normcase(p) == norm for p in self.selected_files):
                self.log(f"Skipped duplicate: {os.path.basename(path)}")
                skipped += 1
            else:
                self.selected_files.append(path)
                self.files_listbox.insert("end", os.path.basename(path))
                added += 1
        if added:
            self.log(f"Added {added} file(s).")
            self.reset_outputs_after_file_change()
        elif skipped:
            self.log("No new files added (duplicates).")

    def remove_selected(self):
        indices = self.files_listbox.curselection()
        if not indices:
            return
        to_remove = {self.files_listbox.get(i) for i in indices}
        new_selected = []
        for path in self.selected_files:
            if os.path.basename(path) in to_remove:
                self.srt_to_mkv_map.pop(os.path.abspath(path), None)
            else:
                new_selected.append(path)
        for i in reversed(indices):
            self.files_listbox.delete(i)
        removed = len(self.selected_files) - len(new_selected)
        self.selected_files = new_selected
        self.log(f"Removed {removed} file(s).")
        self.reset_outputs_after_file_change()

    def clear_files(self):
        if not self.selected_files:
            self.log("File list already empty.")
            return
        self.selected_files.clear()
        self.cleaned_files.clear()
        self.output_files.clear()
        self.embedding_results.clear()
        self.srt_to_mkv_map.clear()
        if self.files_listbox.winfo_exists():
            self.files_listbox.delete(0, "end")
        self.progress_var.set(0)
        self.progress_label.config(text="")
        self.time_label.config(text="")
        self.detailed_stats.config(text="")
        self.output_label.config(text="")
        self.status_label.config(text="Ready")
        self.log("Cleared all files.")
        self.update_button_states()

    def reset_outputs_after_file_change(self):
        self.cleaned_files = []
        self.output_files = []
        self.embedding_results = []
        self.update_button_states()
        self.open_button.config(state="disabled")
        self.delete_temp_button.config(state="disabled")

    def register_srt_mkv_mapping(self, srt_path, mkv_path):
        if not srt_path:
            return
        abs_srt = os.path.abspath(srt_path)
        self.srt_to_mkv_map[abs_srt] = os.path.abspath(mkv_path) if mkv_path else None

    def update_button_states(self):
        has_files = bool(self.selected_files)
        has_mkv = any(f.lower().endswith(".mkv") for f in self.selected_files)
        has_raw_srt = any(f.lower().endswith(".srt") and not f.lower().endswith("_cleaned.srt") for f in self.selected_files)
        has_cleaned = any(f.lower().endswith("_cleaned.srt") for f in self.selected_files)
        has_translated = any(f.lower().endswith(".srt") for f in self.output_files)

        embed_ready = False
        if has_translated:
            for srt in self.output_files:
                mkv = self.srt_to_mkv_map.get(os.path.abspath(srt))
                if mkv and os.path.exists(mkv):
                    embed_ready = True
                    break

        process_running = any([
            self.mkv_processing_in_progress,
            self.cleaning_in_progress,
            self.translation_in_progress,
            self.embedding_in_progress,
            self.run_all_in_progress,
        ])

        def set_state(widget, condition):
            widget.config(state="normal" if condition else "disabled")

        set_state(self.mkv_button, has_mkv and not process_running)
        set_state(self.clean_button, has_raw_srt and not process_running)
        set_state(self.translate_button, has_cleaned and not process_running)
        set_state(self.embed_button, embed_ready and not process_running)
        set_state(self.run_all_button, has_files and not process_running)

        set_state(self.add_button, not process_running)
        set_state(self.remove_button, has_files and not process_running)
        set_state(self.clear_button, has_files and not process_running)

        set_state(self.open_button, (self.output_files or self.embedding_results) and not process_running)

        temp_exists = bool(self.cleaned_files) or any(f.lower().endswith("_cleaned.srt") for f in self.selected_files)
        set_state(self.delete_temp_button, temp_exists and not process_running)

    # ------------------------------------------------------------------
    # Translation cache management
    # ------------------------------------------------------------------
    def clear_cache(self):
        if not self.translation_cache:
            self.status_label.config(text="Translation cache already empty.")
            self.log("Cache already empty.")
            return
        if messagebox.askyesno("Clear Cache", "Clear cached translations?", parent=self.root):
            self.translation_cache.clear()
            self.cache_hits = 0
            self.cache_label.config(text="Cache: 0 entries")
            self.status_label.config(text="Translation cache cleared.")
            self.log("Translation cache cleared.")

    def choose_color(self):
        result = colorchooser.askcolor(color=self.subtitle_color, title="Choose subtitle color", parent=self.root)
        if result and result[1]:
            self.subtitle_color = result[1]
            self.color_preview.config(bg=self.subtitle_color)
            self.log(f"Subtitle color set to {self.subtitle_color}")

    # ------------------------------------------------------------------
    # MKV processing (Step 0)
    # ------------------------------------------------------------------
    def start_mkv_processing(self):
        if self.mkv_processing_in_progress or self.cleaning_in_progress or self.translation_in_progress or self.embedding_in_progress:
            messagebox.showwarning("Busy", "Another process is already running.", parent=self.root)
            return

        mkv_files = [f for f in self.selected_files if f.lower().endswith(".mkv")]
        if not mkv_files:
            if not self.run_all_in_progress:
                messagebox.showinfo("No MKV Files", "No MKV files to process.", parent=self.root)
            else:
                self.log("Run All: No MKVs to process.")
            return

        self.mkv_processing_in_progress = True
        self.configure_ui_for_processing(True)
        self.status_label.config(text="Processing MKV(s)...")
        self.progress_var.set(0)
        Thread(target=self.process_mkv_files_thread, args=(mkv_files,), daemon=True).start()

    def process_mkv_files_thread(self, mkv_files):
        all_extracted_srts = []
        processed_mkv_paths = []
        existing_srts = [f for f in self.selected_files if f.lower().endswith(".srt")]

        num_files = len(mkv_files)
        start_time = time.time()

        for idx, mkv_path in enumerate(mkv_files, start=1):
            if not self.mkv_processing_in_progress:
                break

            filename = os.path.basename(mkv_path)
            self.log(f"MKV Step {idx}/{num_files}: {filename}")

            self.root.after(
                0,
                lambda i=idx, n=num_files, fn=filename: self.status_label.config(
                    text=f"MKV Processing {i}/{n}: {fn}"
                ),
            )
            self.root.after(
                0,
                lambda pct=((idx - 1) / num_files) * 100: self.progress_var.set(pct),
            )
            self.root.after(0, lambda: self.progress_label.config(text="Renaming with FileBot..."))

            renamed_path = self.rename_with_filebot(mkv_path)
            if not renamed_path:
                self.log(f"Skipping {filename}; FileBot failed or was skipped.")
                processed_mkv_paths.append(mkv_path)
                continue

            self.processed_mkv_paths.add(os.path.abspath(renamed_path))
            self.root.after(0, lambda: self.progress_label.config(text="Reading subtitle tracks..."))
            tracks = self.get_mkv_tracks(renamed_path)
            if not tracks:
                self.log(f"No subtitle tracks found in {os.path.basename(renamed_path)}.")
                processed_mkv_paths.append(mkv_path)
                continue

            self.log(f"{len(tracks)} subtitle track(s) found in {filename}.")
            dialog = TrackSelectionDialog(self.root, tracks, renamed_path)
            selections = dialog.selected_tracks
            if not selections:
                self.log("No tracks selected (or dialog cancelled).")
                processed_mkv_paths.append(mkv_path)
                continue

            self.root.after(
                0,
                lambda num=len(selections): self.progress_label.config(
                    text=f"Extracting {num} track(s)..."
                ),
            )
            extracted = self.extract_srts_from_mkv(renamed_path, selections, tracks)
            for srt_path in extracted:
                self.register_srt_mkv_mapping(srt_path, renamed_path)
            all_extracted_srts.extend(extracted)
            processed_mkv_paths.append(mkv_path)

        self.root.after(
            0,
            lambda: self.finalize_mkv_processing(processed_mkv_paths, all_extracted_srts, existing_srts),
        )

    def finalize_mkv_processing(self, processed_mkvs, new_srts, existing_srts):
        self.log("Finalizing MKV processing...")
        combined = sorted(set(existing_srts + new_srts))

        self.selected_files = combined
        if self.files_listbox.winfo_exists():
            self.files_listbox.delete(0, "end")
            for path in self.selected_files:
                self.files_listbox.insert("end", os.path.basename(path))

        self.progress_var.set(100)
        self.progress_label.config(text="")
        self.status_label.config(text=f"MKV Processing complete: extracted {len(new_srts)} file(s).")
        if not self.run_all_in_progress:
            messagebox.showinfo(
                "MKV Processing Complete",
                f"Finished MKV processing.\nExtracted {len(new_srts)} subtitle file(s).",
                parent=self.root,
            )

        self.mkv_processing_in_progress = False
        self.configure_ui_for_processing(False)
        self.update_button_states()

        if self.run_all_in_progress:
            self.root.after(200, lambda: self.initiate_next_run_all_step("clean"))

    def rename_with_filebot(self, filepath):
        self.log(f"Running FileBot on {filepath}")
        directory = os.path.dirname(filepath)
        command = [
            FILEBOT_PATH,
            "-rename",
            filepath,
            "--output",
            directory,
            "--format",
            "{n} ({y})/{n} ({y}){' - '+s00e00}",
            "-non-strict",
            "--conflict",
            "skip",
        ]
        success, output = run_command(command, working_dir=directory)
        if not success:
            self.log(f"FileBot error: {output}")
            return None

        match = re.search(r"\[(MOVE|RENAME)\].*? to \[(.*?)\]", output)
        if match:
            new_path = match.group(2)
            if not os.path.isabs(new_path):
                new_path = os.path.join(directory, new_path)
            if os.path.exists(new_path):
                return new_path

        if os.path.exists(filepath):
            self.log("FileBot completed without moving the file; using original path.")
            return filepath

        self.log("Unable to determine FileBot output path.")
        self.log(f"FileBot raw output:\n{output}")
        return None

    def get_mkv_tracks(self, mkv_path):
        success, output = run_command([MKVMERGE_PATH, "-J", mkv_path])
        if not success:
            self.log(f"mkvmerge error:\n{output}")
            return []
        try:
            data = json.loads(output)
            return [t for t in data.get("tracks", []) if t.get("type") == "subtitles"]
        except json.JSONDecodeError:
            self.log("Failed to parse mkvmerge output as JSON.")
            return []

    def extract_srts_from_mkv(self, mkv_path, track_ids, all_tracks):
        extracted = []
        directory = os.path.dirname(mkv_path)
        base_name = os.path.splitext(os.path.basename(mkv_path))[0]

        for track_id in track_ids:
            info = next((t for t in all_tracks if t["id"] == track_id), None)
            if not info:
                continue
            lang = info["properties"].get(
                "language", info["properties"].get("language_ietf", "und")
            )
            target = os.path.join(directory, f"{base_name}.{lang}.srt")
            counter = 1
            while os.path.exists(target):
                target = os.path.join(directory, f"{base_name}.{lang} ({counter}).srt")
                counter += 1

            command = [MKVEXTRACT_PATH, "tracks", mkv_path, f"{track_id}:{target}"]
            success, output = run_command(command)
            if success and os.path.exists(target):
                self.log(f"Extracted track {track_id} -> {os.path.basename(target)}")
                extracted.append(target)
            else:
                self.log(f"Failed to extract track {track_id}:\n{output}")
        return extracted

    # ------------------------------------------------------------------
    # Cleaning (Step 1)
    # ------------------------------------------------------------------
    def start_cleaning(self):
        if self.mkv_processing_in_progress or self.cleaning_in_progress or self.translation_in_progress or self.embedding_in_progress:
            messagebox.showwarning("Busy", "Another process is already running.", parent=self.root)
            return

        srt_files = [f for f in self.selected_files if f.lower().endswith(".srt")]
        if not srt_files:
            if not self.run_all_in_progress:
                messagebox.showinfo("No SRT Files", "No subtitle files to clean.", parent=self.root)
            else:
                self.log("Run All: No SRT files to clean.")
            return

        files_to_clean = [f for f in srt_files if not f.lower().endswith("_cleaned.srt")]
        if not files_to_clean:
            if not self.run_all_in_progress:
                confirm = messagebox.askyesno(
                    "Already Cleaned?",
                    "All SRT files appear to be already cleaned (filename ends with _cleaned).\n"
                    "Would you like to clean them again?",
                    parent=self.root,
                )
                if not confirm:
                    self.log("Cleaning skipped; files already marked as cleaned.")
                    self.update_button_states()
                    return
                files_to_clean = srt_files
            else:
                self.log("Run All: Using existing cleaned files.")

        self.cleaning_in_progress = True
        self.configure_ui_for_processing(True)
        self.cleaned_files = []
        self.status_label.config(text="Cleaning subtitles...")
        self.progress_var.set(0)
        Thread(target=self.clean_subtitles_thread, args=(files_to_clean,), daemon=True).start()

    def clean_subtitles_thread(self, files_to_clean):
        total = len(files_to_clean)
        cleaned = 0
        failed = []
        start_time = time.time()

        options = {
            "stripCurly": self.strip_curly_var.get(),
            "stripSquare": self.strip_square_var.get(),
            "stripParentheses": self.strip_parentheses_var.get(),
            "stripSpeakerLabels": self.strip_speaker_labels_var.get(),
            "stripCuesWithMusicNote": self.strip_music_notes_var.get(),
            "removeUppercaseSDH": self.remove_uppercase_sdh_var.get(),
            "removeWatermarks": self.remove_watermarks_var.get(),
            "removeFormatting": self.remove_formatting_var.get(),
        }
        self.log(f"Cleaning options: {options}")

        for idx, path in enumerate(files_to_clean, start=1):
            if not self.cleaning_in_progress:
                break

            filename = os.path.basename(path)
            self.root.after(0, lambda i=idx, n=total, fn=filename: self.progress_label.config(
                text=f"Cleaning {i}/{n}: {fn}"
            ))
            self.root.after(0, lambda pct=(idx - 1) / total * 100: self.progress_var.set(pct))

            try:
                cleaned_path = self.clean_single_file(path, options)
                if cleaned_path and os.path.exists(cleaned_path):
                    self.cleaned_files.append(cleaned_path)
                    self.register_srt_mkv_mapping(cleaned_path, self.srt_to_mkv_map.get(os.path.abspath(path)))
                    cleaned += 1
                    self.log(f"Cleaned: {filename} -> {os.path.basename(cleaned_path)}")
                else:
                    failed.append(filename)
            except Exception as exc:
                failed.append(filename)
                self.log(f"ERROR cleaning {filename}: {exc}")

        total_time = time.time() - start_time
        self.root.after(0, lambda: self.finalize_cleaning(cleaned, total, failed, total_time))

    def clean_single_file(self, filepath, options):
        url = "https://subtitletools.com/api/v1/srt-cleaner"
        headers = {"Authorization": CLEANER_API_KEY}
        filename = os.path.basename(filepath)
        self.log(f"Cleaning '{filename}' via API...")

        if not os.path.exists(filepath):
            raise FileNotFoundError(f"Input file not found: {filepath}")
        if os.path.getsize(filepath) == 0:
            raise ValueError("Input subtitle file is empty.")

        with open(filepath, "rb") as src:
            files = {"subtitle": (filename, src, "application/x-subrip")}
            response = requests.post(url, headers=headers, data=options, files=files, timeout=60)

        if response.status_code != 200:
            raise RuntimeError(f"Cleaning API error {response.status_code}: {response.text[:200]}")

        payload = response.json()
        if payload.get("errors"):
            raise RuntimeError(f"Cleaning API returned errors: {payload['errors']}")

        download_url = payload.get("download_url")
        if not download_url:
            raise RuntimeError("Cleaning API did not return a download URL.")

        self.log(f"Downloading cleaned subtitle: {filename}")
        dl_response = requests.get(download_url, headers=headers, stream=True, timeout=60)
        dl_response.raise_for_status()

        out_dir = os.path.dirname(filepath)
        base_name = os.path.splitext(filename)[0]
        if base_name.lower().endswith("_cleaned"):
            base_name = base_name[:-8]
        cleaned_name = f"{base_name}_cleaned.srt"
        cleaned_path = os.path.join(out_dir, cleaned_name)

        with open(cleaned_path, "wb") as out_file:
            for chunk in dl_response.iter_content(chunk_size=8192):
                if chunk:
                    out_file.write(chunk)

        return cleaned_path

    def finalize_cleaning(self, cleaned, total, failed, total_time):
        self.cleaning_in_progress = False
        self.progress_var.set(100)
        summary = f"Cleaning complete: {cleaned}/{total} cleaned"
        if failed:
            summary += f", {len(failed)} failed"
        self.status_label.config(text=summary)
        self.time_label.config(text=f"Total time: {total_time:.1f}s")

        new_file_list = set(self.selected_files)
        new_file_list.update(self.cleaned_files)
        self.selected_files = sorted(new_file_list)
        if self.files_listbox.winfo_exists():
            self.files_listbox.delete(0, "end")
            for path in self.selected_files:
                self.files_listbox.insert("end", os.path.basename(path))

        if failed and not self.run_all_in_progress:
            messagebox.showwarning(
                "Cleaning Issues",
                f"Cleaning finished with errors:\nFailed files:\n - " + "\n - ".join(failed),
                parent=self.root,
            )
        elif not self.run_all_in_progress:
            messagebox.showinfo("Cleaning Complete", summary, parent=self.root)

        self.configure_ui_for_processing(False)
        self.update_button_states()

        if self.run_all_in_progress:
            self.root.after(200, lambda: self.initiate_next_run_all_step("translate"))

    def delete_temp_files(self):
        files_to_delete = list(self.cleaned_files)
        additional_cleaned = [
            os.path.join(os.path.dirname(f), f)
            for f in self.files_listbox.get(0, "end")
            if f.lower().endswith("_cleaned.srt")
        ]
        files_to_delete.extend([f for f in additional_cleaned if os.path.exists(f)])

        if not files_to_delete:
            self.log("No temporary cleaned files found.")
            return

        if not messagebox.askyesno(
            "Delete Temporary Files",
            f"Delete {len(files_to_delete)} cleaned subtitle file(s)?",
            parent=self.root,
        ):
            return

        deleted = 0
        for path in set(files_to_delete):
            try:
                if os.path.exists(path):
                    os.remove(path)
                    deleted += 1
                    self.srt_to_mkv_map.pop(os.path.abspath(path), None)
            except Exception as exc:
                self.log(f"Failed to delete {path}: {exc}")

        self.cleaned_files = [f for f in self.cleaned_files if not os.path.exists(f)]
        self.log(f"Deleted {deleted} cleaned file(s).")
        self.update_button_states()

    def test_cleaning(self):
        self.log("Running cleaning API self-test...")
        temp_dir = os.path.join(os.path.expanduser("~"), "SubTranslatorTempTest")
        os.makedirs(temp_dir, exist_ok=True)
        sample_path = os.path.join(temp_dir, "test_clean_input.srt")
        cleaned_path = None

        sample_content = (
            "1\n00:00:01,000 --> 00:00:04,000\n[MUSIC] <i>Hello</i> (whisper)\n"
            "2\n00:00:05,000 --> 00:00:07,000\nJOHN: Check out MY-SITE.COM!\n"
        )
        try:
            with open(sample_path, "w", encoding="utf-8") as f:
                f.write(sample_content)

            options = {
                "stripCurly": self.strip_curly_var.get(),
                "stripSquare": self.strip_square_var.get(),
                "stripParentheses": self.strip_parentheses_var.get(),
                "stripSpeakerLabels": self.strip_speaker_labels_var.get(),
                "stripCuesWithMusicNote": self.strip_music_notes_var.get(),
                "removeUppercaseSDH": self.remove_uppercase_sdh_var.get(),
                "removeWatermarks": self.remove_watermarks_var.get(),
                "removeFormatting": self.remove_formatting_var.get(),
            }
            cleaned_path = self.clean_single_file(sample_path, options)
            encoding = self.detect_encoding(cleaned_path)
            with open(cleaned_path, "r", encoding=encoding) as f:
                cleaned_content = f.read()

            messagebox.showinfo(
                "Cleaning Test Results",
                f"--- Original ---\n{sample_content}\n\n--- Cleaned ---\n{cleaned_content}",
                parent=self.root,
            )
        except Exception as exc:
            self.log(f"Cleaning test failed: {exc}")
            traceback.print_exc()
            messagebox.showerror("Cleaning Test Failed", f"Error: {exc}", parent=self.root)
        finally:
            for path in (sample_path, cleaned_path):
                if path and os.path.exists(path):
                    try:
                        os.remove(path)
                    except Exception:
                        pass
            if os.path.isdir(temp_dir) and not os.listdir(temp_dir):
                os.rmdir(temp_dir)
            self.status_label.config(text="Ready")

    # ------------------------------------------------------------------
    # Translation (Step 2)
    # ------------------------------------------------------------------
    def start_translation(self):
        if self.mkv_processing_in_progress or self.cleaning_in_progress or self.translation_in_progress or self.embedding_in_progress:
            messagebox.showwarning("Busy", "Another process is already running.", parent=self.root)
            return

        cleaned_srts = [f for f in self.selected_files if f.lower().endswith("_cleaned.srt")]
        if not cleaned_srts:
            if not self.run_all_in_progress:
                messagebox.showerror(
                    "No Cleaned Files",
                    "No cleaned subtitle files found. Run Step 1 first.",
                    parent=self.root,
                )
            else:
                self.log("Run All: No cleaned files to translate.")
            return

        try:
            self.max_workers = max(1, int(self.workers_var.get()))
        except ValueError:
            self.max_workers = self.recommended_workers
            self.workers_var.set(str(self.max_workers))

        self.translation_in_progress = True
        self.configure_ui_for_processing(True)
        self.output_files = []
        self.embedding_results = []
        self.status_label.config(text="Starting translation...")
        self.progress_var.set(0)
        self.progress_label.config(text="")
        self.time_label.config(text="")
        self.detailed_stats.config(text="")
        self.output_label.config(text="")

        self.request_count = 0
        self.error_count = 0
        self.cache_hits = 0
        self.untranslated_line_details = {}
        self.final_untranslated_count = 0

        Thread(target=self.translate_files_thread, args=(cleaned_srts,), daemon=True).start()

    def test_translation(self):
        sample_text = "This is a test."
        target_name = self.target_lang.get()
        source_name = self.source_lang.get()

        if not target_name or target_name not in self.languages:
            messagebox.showerror("Invalid target language", "Please select a valid target language.", parent=self.root)
            return
        if source_name != "Auto" and source_name not in self.languages:
            messagebox.showerror("Invalid source language", "Please select a valid source language.", parent=self.root)
            return

        self.status_label.config(text="Testing translation API...")
        self.root.update_idletasks()
        try:
            translated = self.translate_text_direct(sample_text, target_name, source_name, max_retries=1)
            if translated and translated.strip() and translated.strip() != sample_text.strip():
                messagebox.showinfo(
                    "Translation Test Successful",
                    f"Original: {sample_text}\nTranslated ({target_name}): {translated}",
                    parent=self.root,
                )
                self.log(f"Translation test successful: '{translated}'")
            elif translated == sample_text:
                messagebox.showwarning(
                    "Translation Test",
                    f"Translation returned original text: {translated}\n"
                    "The API may be throttled or source/target languages might match.",
                    parent=self.root,
                )
                self.log("Translation test returned original text (possible throttling or identical language).")
            else:
                messagebox.showerror(
                    "Translation Test Failed",
                    f"Translation failed or returned empty result:\n{translated}",
                    parent=self.root,
                )
                self.log("Translation test failed or returned empty result.")
        except Exception as exc:
            self.log(f"Translation test error: {exc}")
            traceback.print_exc()
            messagebox.showerror("Translation Test Error", f"Error during test:\n{exc}", parent=self.root)
        finally:
            self.status_label.config(text="Ready")

    def translate_files_thread(self, files):
        start_time = time.time()
        try:
            if not files:
                raise ValueError("No files to translate.")
            source_lang = self.source_lang.get()
            target_lang = self.target_lang.get()

            self.output_files = []
            total_subs = 0
            valid_files = []
            self.untranslated_line_details = {}
            self.final_untranslated_count = 0

            for idx, fp in enumerate(files, start=1):
                if not self.translation_in_progress:
                    break
                fn = os.path.basename(fp)
                self.root.after(
                    0,
                    lambda i=idx, n=len(files), name=fn: self.progress_label.config(
                        text=f"Analyzing {i}/{n}: {name}"
                    ),
                )
                try:
                    encoding = self.detect_encoding(fp)
                    subs = pysrt.open(fp, encoding=encoding)
                    count = len(subs)
                    if count > 0:
                        total_subs += count
                        valid_files.append({"path": fp, "count": count, "encoding": encoding})
                        self.log(f"Analyzed {fn}: {count} subtitles")
                    else:
                        self.log(f"Skipping {fn}; no subtitles found.")
                except Exception as exc:
                    self.log(f"Failed to analyze {fn}: {exc}")

            if not self.translation_in_progress:
                raise InterruptedError("Translation cancelled during analysis.")
            if not valid_files:
                raise ValueError("No valid subtitle files after analysis.")

            self.root.after(0, lambda: self.status_label.config(text="Translating subtitles..."))

            overall_stats = {"processed": 0, "total": total_subs}
            translation_start = time.time()

            for idx, info in enumerate(valid_files, start=1):
                if not self.translation_in_progress:
                    break
                fp = info["path"]
                fn = os.path.basename(fp)
                encoding = info["encoding"]
                self.root.after(
                    0,
                    lambda i=idx, n=len(valid_files), name=fn: self.progress_label.config(
                        text=f"Translating {i}/{n}: {name}"
                    ),
                )
                out_path = self.get_output_path(fp, target_lang)
                self.log(f"Translating {fn} -> {os.path.basename(out_path)}")

                self.register_srt_mkv_mapping(out_path, self.srt_to_mkv_map.get(os.path.abspath(fp)))

                try:
                    _, processed_count, untranslated = self.process_file_parallel(
                        fp, out_path, source_lang, target_lang, encoding, overall_stats, translation_start
                    )
                    if os.path.exists(out_path):
                        if out_path not in self.output_files:
                            self.output_files.append(out_path)
                        self.register_srt_mkv_mapping(out_path, self.srt_to_mkv_map.get(os.path.abspath(fp)))
                    self.log(f"File complete: {fn} – {processed_count} lines translated, {untranslated} pending")
                except InterruptedError:
                    raise
                except Exception as exc:
                    self.log(f"Error processing {fn}: {exc}")
                    traceback.print_exc()
                    if not self.run_all_in_progress:
                        self.root.after(
                            0,
                            lambda name=fn, err=exc: messagebox.showerror(
                                "Translation Error",
                                f"Failed to translate {name}:\n{err}",
                                parent=self.root,
                            ),
                        )

            if not self.translation_in_progress:
                raise InterruptedError("Translation cancelled mid-process.")

            # Retry loop
            try:
                max_retries = int(self.retry_passes_var.get())
            except ValueError:
                max_retries = 3

            current_pass = 0
            while self.untranslated_line_details and current_pass < max_retries:
                current_pass += 1
                if not self.translation_in_progress:
                    raise InterruptedError("Cancelled during retry.")
                self.log(f"Retry pass {current_pass}/{max_retries} – {len(self.untranslated_line_details)} lines")
                self.root.after(
                    0,
                    lambda p=current_pass, m=max_retries: self.status_label.config(
                        text=f"Auto-retrying translation (pass {p}/{m})..."
                    ),
                )
                retry_success, remaining = self.retry_untranslated_lines_sequential(current_pass)
                self.log(f"Retry pass {current_pass} complete – success: {retry_success}, remaining: {remaining}")
                if remaining == 0:
                    break
                time.sleep(1)

            self.final_untranslated_count = len(self.untranslated_line_details)

            total_time = time.time() - start_time
            translation_time = time.time() - translation_start

            self.root.after(
                0,
                lambda: self.finalize_translation(
                    files_ok=len(self.output_files),
                    files_total=len(valid_files),
                    subs_ok=overall_stats["processed"],
                    subs_total=overall_stats["total"],
                    total_duration=total_time,
                    translation_duration=translation_time,
                    final_untranslated=self.final_untranslated_count,
                    cancelled=False,
                ),
            )

        except InterruptedError:
            total_time = time.time() - start_time
            self.root.after(
                0,
                lambda: self.finalize_translation(
                    files_ok=len(self.output_files),
                    files_total=len(files),
                    subs_ok=0,
                    subs_total=0,
                    total_duration=total_time,
                    translation_duration=0,
                    final_untranslated=len(self.untranslated_line_details),
                    cancelled=True,
                ),
            )
        except Exception as exc:
            self.log(f"Translation failed: {exc}")
            traceback.print_exc()
            if self.root.winfo_exists():
                self.root.after(
                    0,
                    lambda: messagebox.showerror(
                        "Translation Error",
                        f"Translation failed:\n{exc}",
                        parent=self.root,
                    ),
                )
                self.root.after(0, lambda: self.status_label.config(text="Translation failed."))
                self.root.after(0, lambda: self.configure_ui_for_processing(False))
                self.root.after(0, self.update_button_states)
            self.translation_in_progress = False

    def translate_text_direct(self, text, target_lang, source_lang="Auto", max_retries=3):
        stripped = text.strip()
        if not stripped:
            return text

        try:
            target_code = self.languages[target_lang]
            source_code = "auto" if source_lang == "Auto" else self.languages[source_lang]
        except KeyError:
            return text

        cache_key = f"{source_code}|{target_code}|{stripped}"
        if cache_key in self.translation_cache:
            self.cache_hits += 1
            return self.translation_cache[cache_key]

        def throttle_delay():
            mode = self.throttle_var.get()
            error_ratio = (self.error_count / self.request_count) if self.request_count else 0
            delay = 0.0
            if mode == "Mild":
                delay = 0.05
            elif mode == "Moderate":
                delay = 0.15
            elif mode == "Adaptive":
                if error_ratio > 0.5:
                    delay = 1.0
                elif error_ratio > 0.3:
                    delay = 0.5
                elif error_ratio > 0.15:
                    delay = 0.25
                elif error_ratio > 0.05:
                    delay = 0.1
                else:
                    delay = 0.05
            if delay > 0:
                delay += random.uniform(0, delay * 0.25)
            return delay

        attempt = 0
        result = text
        success = False
        raw_response = ""
        response_obj = None

        while attempt <= max_retries:
            if not self.translation_in_progress:
                break

            delay = throttle_delay()
            if delay:
                time.sleep(delay)

            self.request_count += 1
            try:
                endpoint = self.get_next_endpoint()
                params = {
                    "client": "gtx",
                    "sl": source_code,
                    "tl": target_code,
                    "dt": "t",
                    "q": stripped,
                }
                headers = {
                    "User-Agent": (
                        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) "
                        "Chrome/92.0.4515.131 Safari/537.36"
                    )
                }
                response_obj = requests.get(endpoint, params=params, headers=headers, timeout=15)
                raw_response = response_obj.text

                if response_obj.status_code == 200:
                    data = response_obj.json()
                    translated = ""
                    if isinstance(data, list):
                        if data and isinstance(data[0], list):
                            translated = "".join(
                                seg[0] for seg in data[0] if isinstance(seg, list) and seg and isinstance(seg[0], str)
                            )
                        elif data and isinstance(data[0], str):
                            translated = data[0]
                    elif isinstance(data, dict) and "sentences" in data:
                        translated = "".join(seg.get("trans", "") for seg in data.get("sentences", []))

                    if translated and translated.strip() and translated.strip() != stripped:
                        result = translated
                        success = True
                        break
                    else:
                        self.log(
                            f"WARN: Translation attempt {attempt + 1} returned empty/identical text for '{stripped[:40]}...'"
                        )
                        time.sleep(0.5 + attempt * 0.5)
                elif response_obj.status_code in {429, 500, 502, 503, 504}:
                    self.log(f"WARN: HTTP {response_obj.status_code} (attempt {attempt + 1}); retrying...")
                    time.sleep(0.5 + attempt * 0.5)
                else:
                    self.log(f"ERROR: HTTP {response_obj.status_code}: {raw_response[:200]}")
                    self.error_count += 1
                    break
            except requests.exceptions.Timeout:
                self.log(f"Timeout on translation attempt {attempt + 1}")
                time.sleep(0.5 + attempt * 0.5)
            except Exception as exc:
                self.log(f"Translation request error: {exc}")
                traceback.print_exc()
                self.error_count += 1
                break

            attempt += 1

        if not success:
            self.error_count += 1
            result = text

        if self.debug_var.get() and response_obj is not None:
            status = response_obj.status_code
            self.log(
                f"DEBUG Translation:\nStatus: {status}\nInput: '{stripped[:80]}...'\n"
                f"Output: '{result[:80]}...'\nRaw: '{raw_response[:200]}...'"
            )

        if success:
            self.translation_cache[cache_key] = result

        return result

    def get_next_endpoint(self):
        with self.endpoint_lock:
            endpoint = self.api_endpoints[self.current_endpoint_index]
            self.current_endpoint_index = (self.current_endpoint_index + 1) % len(self.api_endpoints)
            return endpoint

    def translate_subtitle(self, task):
        sub = task["sub"]
        source_lang = task["source_lang"]
        target_lang = task["target_lang"]
        original_text = sub.text

        if not self.translation_in_progress:
            return sub.index, original_text, False, original_text

        translated = self.translate_text_direct(original_text, target_lang, source_lang, max_retries=3)
        if translated and translated.strip() != original_text.strip():
            formatted = translated
            if self.subtitle_color and self.subtitle_color.upper() != "#FFFFFF":
                if self.format_var.get() == "ASS/SSA Compatible":
                    color = self.subtitle_color
                    ass_color = f"&H{color[5:7]}{color[3:5]}{color[1:3]}&"
                    formatted = f"{{\\c{ass_color}}}{translated}{{\\c&HFFFFFF&}}"
                else:
                    formatted = f'<font color="{self.subtitle_color}">{translated}</font>'
            return sub.index, formatted, True, original_text
        return sub.index, original_text, False, original_text

    def update_progress(self, percent, stats, elapsed, current_file=None):
        def apply():
            self.progress_var.set(percent)
            processed = stats["processed"]
            total = stats["total"]

            details = []
            if self.request_count:
                error_ratio = (self.error_count / self.request_count) * 100
                details.append(f"API Err: {self.error_count} ({error_ratio:.1f}%)")
            if processed:
                details.append(f"Cache hit: {self.cache_hits / processed * 100:.1f}%")
            details.append(f"Pending: {len(self.untranslated_line_details)}")

            self.progress_label.config(text=f"Subtitles processed: {processed}/{total} ({percent:.1f}%)")
            if elapsed > 0 and processed:
                rate = processed / elapsed
                remaining = total - processed
                if rate > 0:
                    eta = remaining / rate
                    if eta < 60:
                        eta_str = f"{eta:.0f}s"
                    elif eta < 3600:
                        eta_str = f"{eta / 60:.1f}m"
                    else:
                        eta_str = f"{eta / 3600:.1f}h"
                else:
                    eta_str = "N/A"
                self.time_label.config(text=f"Elapsed: {elapsed:.1f}s | ETA: {eta_str} | Speed: {rate:.1f} sub/s")
            else:
                self.time_label.config(text=f"Elapsed: {elapsed:.1f}s")

            self.detailed_stats.config(text=" | ".join(details))
            if current_file:
                self.output_label.config(text=f"Current output: {os.path.basename(current_file)}")
        self.root.after(0, apply)

    def process_file_parallel(self, file_path, output_path, source_lang, target_lang, encoding, stats, start_time):
        subs = pysrt.open(file_path, encoding=encoding)
        total_subs = len(subs)
        if total_subs == 0:
            self.save_subtitles(pysrt.SubRipFile(), output_path)
            return 0, 0, 0

        tasks = [
            {"sub": sub, "source_lang": source_lang, "target_lang": target_lang}
            for sub in subs
        ]

        processed = 0
        untranslated = 0
        results = {}

        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = {executor.submit(self.translate_subtitle, task): task["sub"].index for task in tasks}
            for future in concurrent.futures.as_completed(futures):
                if not self.translation_in_progress:
                    for f in futures:
                        f.cancel()
                    executor.shutdown(wait=False, cancel_futures=True)
                    raise InterruptedError("Translation cancelled.")
                index = futures[future]
                try:
                    idx, text, success, original = future.result()
                    results[idx] = text
                    processed += 1
                    if not success:
                        untranslated += 1
                        key = (output_path, idx)
                        info = self.untranslated_line_details.get(key, {"failure_count": 0})
                        info["original_text"] = original
                        info["failure_count"] = info.get("failure_count", 0) + 1
                        self.untranslated_line_details[key] = info
                    else:
                        key = (output_path, idx)
                        if key in self.untranslated_line_details:
                            del self.untranslated_line_details[key]
                    stats["processed"] += 1
                    percent = (stats["processed"] / max(1, stats["total"])) * 100
                    elapsed = time.time() - start_time
                    if processed % max(1, total_subs // 20) == 0 or processed == total_subs:
                        self.update_progress(percent, stats, elapsed, output_path)
                except concurrent.futures.CancelledError:
                    pass
                except Exception as exc:
                    self.log(f"Translation error: {exc}")
                    untranslated += 1
                    original = next((sub.text for sub in subs if sub.index == index), "")
                    key = (output_path, index)
                    info = self.untranslated_line_details.get(key, {"failure_count": 0})
                    info["original_text"] = original
                    info["failure_count"] = info.get("failure_count", 0) + 1
                    self.untranslated_line_details[key] = info
                    results[index] = original

        for sub in subs:
            if sub.index in results:
                sub.text = results[sub.index]

        self.save_subtitles(subs, output_path)
        return sum(len(sub.text) for sub in subs), processed, untranslated

    def retry_untranslated_lines_sequential(self, pass_number):
        if not self.untranslated_line_details:
            return 0, 0

        grouped = collections.defaultdict(list)
        for key, details in self.untranslated_line_details.items():
            grouped[key[0]].append((key, details))

        next_untranslated = {}
        successes = 0

        for output_path, items in grouped.items():
            if not os.path.exists(output_path):
                self.log(f"Retry pass {pass_number}: output file missing ({output_path})")
                for key, details in items:
                    details["failure_count"] = MAX_FAILURES_PER_LINE
                    next_untranslated[key] = details
                continue

            subs = pysrt.open(output_path, encoding="utf-8-sig")
            modified = False

            for key, details in items:
                if not self.translation_in_progress:
                    raise InterruptedError("Cancelled during retry pass.")
                _, index = key
                failure_count = details.get("failure_count", 1)
                if failure_count >= MAX_FAILURES_PER_LINE:
                    next_untranslated[key] = details
                    continue

                sub = next((s for s in subs if s.index == index), None)
                if not sub:
                    details["failure_count"] = MAX_FAILURES_PER_LINE
                    next_untranslated[key] = details
                    continue

                original_text = details["original_text"]
                if sub.text.strip() != original_text.strip():
                    continue  # Already changed

                translated = self.translate_text_direct(
                    original_text, self.target_lang.get(), self.source_lang.get(), max_retries=3
                )
                if translated.strip() and translated.strip() != original_text.strip():
                    successes += 1
                    modified = True
                    formatted = translated
                    if self.subtitle_color and self.subtitle_color.upper() != "#FFFFFF":
                        if self.format_var.get() == "ASS/SSA Compatible":
                            color = self.subtitle_color
                            ass_color = f"&H{color[5:7]}{color[3:5]}{color[1:3]}&"
                            formatted = f"{{\\c{ass_color}}}{translated}{{\\c&HFFFFFF&}}"
                        else:
                            formatted = f'<font color="{self.subtitle_color}">{translated}</font>'
                    sub.text = formatted
                else:
                    details["failure_count"] = failure_count + 1
                    next_untranslated[key] = details

            if modified:
                self.save_subtitles(subs, output_path)

        self.untranslated_line_details = next_untranslated
        remaining = len(next_untranslated)
        return successes, remaining

    def finalize_translation(
        self,
        files_ok,
        files_total,
        subs_ok,
        subs_total,
        total_duration,
        translation_duration,
        final_untranslated,
        cancelled=False,
    ):
        self.translation_in_progress = False
        percent = (subs_ok / max(1, subs_total)) * 100 if subs_total else 0
        self.progress_var.set(percent)
        status = "Translation cancelled." if cancelled else "Translation finished."
        self.status_label.config(text=f"{status} {files_ok}/{files_total} file(s) output.")
        self.progress_label.config(text=f"Subtitles processed: {subs_ok}/{subs_total}")
        self.time_label.config(
            text=f"Total: {total_duration:.1f}s | Translation: {translation_duration:.1f}s | Cache hits: {self.cache_hits}"
        )
        error_ratio = (self.error_count / self.request_count * 100) if self.request_count else 0
        self.detailed_stats.config(
            text=f"API calls: {self.request_count} | Errors: {self.error_count} ({error_ratio:.1f}%) | Untranslated: {final_untranslated}"
        )

        if self.output_files:
            first_dir = os.path.dirname(self.output_files[0])
            self.output_label.config(text=f"Output stored in: {os.path.basename(first_dir)}")

        if not cancelled:
            if final_untranslated or error_ratio > 10 or files_ok < files_total:
                level = messagebox.showwarning
                title = "Translation Complete (with issues)"
            else:
                level = messagebox.showinfo
                title = "Translation Complete"
            if not self.run_all_in_progress:
                output_names = "\n - ".join(os.path.basename(f) for f in self.output_files)
                msg = (
                    f"Files processed: {files_ok}/{files_total}\n"
                    f"Subtitles: {subs_ok}/{subs_total}\n"
                    f"Untranslated (final): {final_untranslated}\n"
                    f"API errors: {self.error_count} ({error_ratio:.1f}%)\n\n"
                    f"Outputs:\n - {output_names}"
                )
                level(title, msg, parent=self.root)

        if self.output_files and self.cleaned_files and self.auto_delete_temp_var.get():
            self.delete_temp_files()

        self.configure_ui_for_processing(False)
        self.update_button_states()

        if self.run_all_in_progress:
            self.root.after(200, lambda: self.initiate_next_run_all_step("embed"))

    # ------------------------------------------------------------------
    # Embedding (Step 3)
    # ------------------------------------------------------------------
    def start_embedding(self):
        if self.mkv_processing_in_progress or self.cleaning_in_progress or self.translation_in_progress or self.embedding_in_progress:
            messagebox.showwarning("Busy", "Another process is already running.", parent=self.root)
            return

        translated_srts = [os.path.abspath(f) for f in self.output_files if f.lower().endswith(".srt")]
        if not translated_srts:
            if not self.run_all_in_progress:
                messagebox.showerror("No Translated Files", "Translate subtitles before embedding.", parent=self.root)
            else:
                self.log("Run All: No translated subtitles for embedding.")
            return

        tasks = collections.defaultdict(list)
        for srt in translated_srts:
            mkv = self.srt_to_mkv_map.get(srt)
            if mkv and os.path.exists(mkv):
                tasks[mkv].append(srt)
            else:
                self.log(f"No MKV mapping for {os.path.basename(srt)}; skipping.")

        if not tasks:
            if not self.run_all_in_progress:
                messagebox.showerror(
                    "No MKV Targets",
                    "No MKV files found for embedding translated subtitles.",
                    parent=self.root,
                )
            else:
                self.log("Run All: No MKV targets for embedding.")
            return

        self.embedding_in_progress = True
        self.configure_ui_for_processing(True)
        self.embedding_results = []
        self.status_label.config(text="Embedding subtitles into MKVs...")
        self.progress_var.set(0)
        Thread(target=self.embed_subtitles_thread, args=(tasks,), daemon=True).start()

    def embed_subtitles_thread(self, tasks):
        total = len(tasks)
        processed = 0
        successes = 0
        failures = []
        start_time = time.time()
        lang_code = self.get_embed_language_code()
        track_name = self.target_lang.get()

        for mkv_path, srt_paths in tasks.items():
            if not self.embedding_in_progress:
                break
            processed += 1
            base = os.path.basename(mkv_path)
            self.root.after(
                0,
                lambda i=processed, n=total, name=base: self.progress_label.config(
                    text=f"Embedding {i}/{n}: {name}"
                ),
            )
            self.root.after(
                0,
                lambda pct=(processed - 1) / total * 100: self.progress_var.set(pct),
            )

            directory = os.path.dirname(mkv_path)
            tmp_output = os.path.join(directory, f"{os.path.splitext(base)[0]}__embed_tmp.mkv")
            counter = 1
            while os.path.exists(tmp_output):
                tmp_output = os.path.join(directory, f"{os.path.splitext(base)[0]}__embed_tmp({counter}).mkv")
                counter += 1

            command = [MKVMERGE_PATH, "-o", tmp_output, mkv_path]
            for srt in srt_paths:
                command.extend(
                    [
                        "--language",
                        f"0:{lang_code}",
                        "--track-name",
                        f"0:{track_name}",
                        srt,
                    ]
                )

            success, output = run_command(command, working_dir=directory)
            if success and os.path.exists(tmp_output):
                try:
                    backup = mkv_path + ".bak"
                    if os.path.exists(backup):
                        os.remove(backup)
                    os.replace(mkv_path, backup)
                    os.replace(tmp_output, mkv_path)
                    os.remove(backup)
                    self.embedding_results.append(mkv_path)
                    successes += 1
                except Exception as exc:
                    self.log(f"Failed to replace original MKV: {exc}")
                    failures.append(base)
                    if os.path.exists(tmp_output):
                        os.remove(tmp_output)
                    if os.path.exists(backup):
                        os.replace(backup, mkv_path)
            else:
                failures.append(base)
                if os.path.exists(tmp_output):
                    os.remove(tmp_output)
                self.log(f"Embedding failed for {base}:\n{output}")

        total_time = time.time() - start_time
        self.root.after(
            0,
            lambda: self.finalize_embedding(total, successes, failures, total_time),
        )

    def finalize_embedding(self, total, successes, failures, total_time):
        self.embedding_in_progress = False
        status = f"Embedding complete: {successes}/{total} MKV(s) updated."
        if failures:
            status += f" Failures: {len(failures)}."
        self.status_label.config(text=status)
        self.progress_label.config(text=status)
        self.time_label.config(text=f"Total time: {total_time:.1f}s")

        if failures and not self.run_all_in_progress:
            messagebox.showwarning(
                "Embedding Issues",
                f"{status}\n\nFailed MKVs:\n - " + "\n - ".join(failures),
                parent=self.root,
            )
        elif not failures and not self.run_all_in_progress:
            messagebox.showinfo("Embedding Complete", f"{status}\nMKVs overwritten.", parent=self.root)

        if self.embedding_results:
            dir_name = os.path.dirname(self.embedding_results[0])
            self.output_label.config(text=f"Embedded files located in: {os.path.basename(dir_name)}")

        self.configure_ui_for_processing(False)
        self.update_button_states()

        if self.run_all_in_progress:
            self.run_all_complete()

    def get_embed_language_code(self, language_name=None, fallback=None):
        target = language_name or self.target_lang.get()
        code = self.languages.get(target, fallback or "en")
        normalized = code.lower().split("-")[0]
        return ISO_639_2_CODES.get(normalized, normalized[:3])

    # ------------------------------------------------------------------
    # Run All workflow
    # ------------------------------------------------------------------
    def run_all_steps(self):
        if not self.selected_files:
            messagebox.showerror("No Files", "Add MKV/SRT files before running the workflow.", parent=self.root)
            return
        if any([
            self.mkv_processing_in_progress,
            self.cleaning_in_progress,
            self.translation_in_progress,
            self.embedding_in_progress,
            self.run_all_in_progress,
        ]):
            messagebox.showwarning("Busy", "Another process is running.", parent=self.root)
            return

        self.run_all_in_progress = True
        self.configure_ui_for_processing(True)
        self.status_label.config(text="Run All: Starting pipeline...")
        self.root.after(200, lambda: self.initiate_next_run_all_step("mkv"))

    def initiate_next_run_all_step(self, step):
        if not self.run_all_in_progress:
            return

        self.run_all_current_step = step
        self.log(f"Run All: Beginning step '{step}'")

        if step == "mkv":
            if any(f.lower().endswith(".mkv") for f in self.selected_files):
                self.start_mkv_processing()
            else:
                self.root.after(200, lambda: self.initiate_next_run_all_step("clean"))
        elif step == "clean":
            if any(f.lower().endswith(".srt") and not f.lower().endswith("_cleaned.srt") for f in self.selected_files):
                self.start_cleaning()
            else:
                self.root.after(200, lambda: self.initiate_next_run_all_step("translate"))
        elif step == "translate":
            if any(f.lower().endswith("_cleaned.srt") for f in self.selected_files):
                self.start_translation()
            else:
                self.root.after(200, lambda: self.initiate_next_run_all_step("embed"))
        elif step == "embed":
            translated_srts = [f for f in self.output_files if f.lower().endswith(".srt")]
            has_target = False
            for srt in translated_srts:
                mkv = self.srt_to_mkv_map.get(os.path.abspath(srt))
                if mkv and os.path.exists(mkv):
                    has_target = True
                    break
            if has_target:
                self.start_embedding()
            else:
                self.run_all_complete()
        else:
            self.run_all_complete()

    def run_all_complete(self):
        self.run_all_in_progress = False
        self.run_all_current_step = None
        self.status_label.config(text="Run All: Complete.")
        self.configure_ui_for_processing(False)
        self.update_button_states()

        if self.embedding_results:
            dir_name = os.path.dirname(self.embedding_results[0])
            self.output_label.config(text=f"Run All outputs in: {os.path.basename(dir_name)}")
        elif self.output_files:
            dir_name = os.path.dirname(self.output_files[0])
            self.output_label.config(text=f"Run All outputs in: {os.path.basename(dir_name)}")
        else:
            self.output_label.config(text="Run All completed with no outputs.")

    # ------------------------------------------------------------------
    # General utilities
    # ------------------------------------------------------------------
    def detect_encoding(self, file_path, read_bytes=1024 * 1024):
        try:
            with open(file_path, "rb") as f:
                data = f.read(read_bytes)
        except FileNotFoundError:
            self.log(f"Encoding detection: file not found ({file_path})")
            return "utf-8"

        if not data:
            return "utf-8"
        if data.startswith(b"\xef\xbb\xbf"):
            return "utf-8-sig"
        if data.startswith(b"\xff\xfe") or data.startswith(b"\xfe\xff"):
            return "utf-16"

        detection = chardet.detect(data)
        encoding = detection.get("encoding")
        confidence = detection.get("confidence", 0)

        if encoding and confidence > 0.8:
            return "utf-8" if encoding.lower() == "ascii" else encoding

        for fallback in ("utf-8", "latin-1"):
            try:
                data.decode(fallback)
                return fallback
            except UnicodeDecodeError:
                continue
        return "utf-8"

    def get_output_path(self, input_path, target_lang):
        try:
            directory = os.path.dirname(input_path)
            base, ext = os.path.splitext(os.path.basename(input_path))
            if base.lower().endswith("_cleaned"):
                base = base[:-8]

            parts = base.split(".")
            existing_lang = ""
            if len(parts) > 1 and parts[-1].isalpha() and len(parts[-1]) in {2, 3}:
                existing_lang = parts[-1]
                core = ".".join(parts[:-1])
            else:
                core = base

            code = self.languages.get(target_lang, target_lang[:2].lower())
            naming = self.naming_var.get()

            if naming == "Original_language":
                output_name = f"{core}_{code}{ext}"
            elif naming == "language.Original":
                output_name = f"{code}.{core}{ext}"
            else:
                output_name = f"{core}.{code}{ext}"

            return os.path.join(directory, output_name)
        except Exception as exc:
            self.log(f"Failed to build output path: {exc}")
            fallback = f"{input_path}.{target_lang[:2].lower()}"
            return fallback

    def save_subtitles(self, subs, output_path):
        try:
            os.makedirs(os.path.dirname(output_path), exist_ok=True)
            subs.save(output_path, encoding="utf-8-sig")
            return True
        except Exception as exc:
            self.log(f"Failed to save subtitles: {exc}")
            return False

    def get_supported_languages(self):
        return {
            'Afrikaans': 'af', 'Albanian': 'sq', 'Amharic': 'am', 'Arabic': 'ar', 'Armenian': 'hy',
            'Azerbaijani': 'az', 'Basque': 'eu', 'Belarusian': 'be', 'Bengali': 'bn', 'Bosnian': 'bs',
            'Bulgarian': 'bg', 'Catalan': 'ca', 'Cebuano': 'ceb', 'Chinese (Simplified)': 'zh-CN',
            'Chinese (Traditional)': 'zh-TW', 'Corsican': 'co', 'Croatian': 'hr', 'Czech': 'cs',
            'Danish': 'da', 'Dutch': 'nl', 'English': 'en', 'Esperanto': 'eo', 'Estonian': 'et',
            'Finnish': 'fi', 'French': 'fr', 'Frisian': 'fy', 'Galician': 'gl', 'Georgian': 'ka',
            'German': 'de', 'Greek': 'el', 'Gujarati': 'gu', 'Haitian Creole': 'ht', 'Hausa': 'ha',
            'Hawaiian': 'haw', 'Hebrew': 'iw', 'Hindi': 'hi', 'Hmong': 'hmn', 'Hungarian': 'hu',
            'Icelandic': 'is', 'Igbo': 'ig', 'Indonesian': 'id', 'Irish': 'ga', 'Italian': 'it',
            'Japanese': 'ja', 'Javanese': 'jw', 'Kannada': 'kn', 'Kazakh': 'kk', 'Khmer': 'km',
            'Kinyarwanda': 'rw', 'Korean': 'ko', 'Kurdish': 'ku', 'Kyrgyz': 'ky', 'Lao': 'lo',
            'Latin': 'la', 'Latvian': 'lv', 'Lithuanian': 'lt', 'Luxembourgish': 'lb', 'Macedonian': 'mk',
            'Malagasy': 'mg', 'Malay': 'ms', 'Malayalam': 'ml', 'Maltese': 'mt', 'Maori': 'mi',
            'Marathi': 'mr', 'Mongolian': 'mn', 'Myanmar (Burmese)': 'my', 'Nepali': 'ne',
            'Norwegian': 'no', 'Nyanja (Chichewa)': 'ny', 'Odia (Oriya)': 'or', 'Pashto': 'ps',
            'Persian': 'fa', 'Polish': 'pl', 'Portuguese': 'pt', 'Punjabi': 'pa', 'Romanian': 'ro',
            'Russian': 'ru', 'Samoan': 'sm', 'Scots Gaelic': 'gd', 'Serbian': 'sr', 'Sesotho': 'st',
            'Shona': 'sn', 'Sindhi': 'sd', 'Sinhala (Sinhalese)': 'si', 'Slovak': 'sk', 'Slovenian': 'sl',
            'Somali': 'so', 'Spanish': 'es', 'Sundanese': 'su', 'Swahili': 'sw', 'Swedish': 'sv',
            'Tagalog (Filipino)': 'tl', 'Tajik': 'tg', 'Tamil': 'ta', 'Tatar': 'tt', 'Telugu': 'te',
            'Thai': 'th', 'Turkish': 'tr', 'Turkmen': 'tk', 'Ukrainian': 'uk', 'Urdu': 'ur',
            'Uyghur': 'ug', 'Uzbek': 'uz', 'Vietnamese': 'vi', 'Welsh': 'cy', 'Xhosa': 'xh',
            'Yiddish': 'yi', 'Yoruba': 'yo', 'Zulu': 'zu'
        }

    def update_languages(self, _event=None):
        self.languages = self.get_supported_languages()
        sorted_langs = sorted(self.languages.keys())
        self.source_combo["values"] = ["Auto"] + sorted_langs
        self.target_combo["values"] = sorted_langs
        if self.source_lang.get() != "Auto" and self.source_lang.get() not in self.languages:
            self.source_lang.set("Auto")
        if self.target_lang.get() not in self.languages and sorted_langs:
            self.target_lang.set(sorted_langs[0])

    def open_output_folder(self):
        targets = self.embedding_results if self.embedding_results else self.output_files
        if not targets:
            messagebox.showinfo("Info", "No output files available yet.", parent=self.root)
            return
        directory = os.path.dirname(os.path.abspath(targets[0]))
        try:
            if platform.system() == "Windows":
                os.startfile(directory)
            elif platform.system() == "Darwin":
                subprocess.run(["open", directory], check=False)
            else:
                subprocess.run(["xdg-open", directory], check=False)
        except Exception as exc:
            self.log(f"Failed to open folder: {exc}")
            messagebox.showerror("Error", f"Could not open directory:\n{exc}", parent=self.root)

    def configure_ui_for_processing(self, processing_started):
        disable = processing_started or any([
            self.mkv_processing_in_progress,
            self.cleaning_in_progress,
            self.translation_in_progress,
            self.embedding_in_progress,
            self.run_all_in_progress,
        ])
        state = "disabled" if disable else "normal"

        for btn in [
            self.mkv_button,
            self.clean_button,
            self.translate_button,
            self.embed_button,
            self.run_all_button,
            self.add_button,
            self.remove_button,
            self.clear_button,
            self.open_button,
            self.delete_temp_button,
        ]:
            if btn:
                btn.config(state=state)

        for tab_index in range(len(self.notebook.tabs())):
            tab_id = self.notebook.tabs()[tab_index]
            label = self.notebook.tab(tab_id, "text")
            if label in {"Translation Settings", "Cleaning Settings"}:
                self.notebook.tab(tab_id, state="disabled" if disable else "normal")

        if not disable:
            self.update_button_states()

    def run_all_complete(self):
        self.run_all_in_progress = False
        self.run_all_current_step = None
        self.status_label.config(text="Run All: Complete.")
        self.configure_ui_for_processing(False)
        self.update_button_states()

        if self.embedding_results:
            dir_name = os.path.dirname(self.embedding_results[0])
            self.output_label.config(text=f"Run All outputs in: {os.path.basename(dir_name)}")
        elif self.output_files:
            dir_name = os.path.dirname(self.output_files[0])
            self.output_label.config(text=f"Run All outputs in: {os.path.basename(dir_name)}")
        else:
            self.output_label.config(text="Run All completed with no outputs.")

    def on_closing(self):
        if any([
            self.mkv_processing_in_progress,
            self.cleaning_in_progress,
            self.translation_in_progress,
            self.embedding_in_progress,
            self.run_all_in_progress,
        ]):
            if messagebox.askokcancel(
                "Quit",
                "A process is currently running. Quit anyway?\nPartial results may remain.",
                parent=self.root,
            ):
                self.translation_in_progress = False
                self.cleaning_in_progress = False
                self.mkv_processing_in_progress = False
                self.embedding_in_progress = False
                self.run_all_in_progress = False
                time.sleep(0.2)
                self.root.destroy()
        else:
            self.root.destroy()


# ---------------------------------------------------------------------------
# Application entry point
# ---------------------------------------------------------------------------
def main():
    multiprocessing.freeze_support()
    root = TkinterDnD.Tk()
    app = CombinedSubtitleProcessorGUI(root)
    root.mainloop()


if __name__ == "__main__":
    main()