"""Startup GUI screen for the NIMBY passenger tool."""

from __future__ import annotations

from pathlib import Path

from line_selection_screen import ensure_line_table, run_line_selection_screen


def should_launch_startup_screen(argv: list[str]) -> bool:
    return not argv


def run_startup_screen() -> int:
    import tkinter as tk
    from tkinter import filedialog

    while True:
        root = tk.Tk()
        root.title("NIMBY Rails Passenger Analytics Tool")
        root.configure(bg="#1f1f1f")
        root.geometry("960x540")
        root.minsize(800, 450)

        selected: dict[str, str] = {"path": ""}

        container = tk.Frame(root, bg="#1f1f1f")
        container.pack(expand=True)

        headline = tk.Label(
            container,
            text="NIMBY Rails Passenger Analytics Tool",
            fg="#f5f5f5",
            bg="#1f1f1f",
            font=("Arial", 30, "bold"),
        )
        headline.pack(pady=(0, 60))

        button_row = tk.Frame(container, bg="#1f1f1f")
        button_row.pack()

        button_style = {
            "font": ("Arial", 18, "bold"),
            "width": 12,
            "height": 2,
            "fg": "#f3f3f3",
            "bg": "#3a3a3a",
            "activebackground": "#555555",
            "activeforeground": "#ffffff",
            "bd": 4,
            "relief": tk.RAISED,
            "cursor": "hand2",
        }

        def create_new() -> None:
            file_path = filedialog.asksaveasfilename(
                title="新規プロジェクトファイルを作成",
                defaultextension=".db",
                filetypes=(("DB file", "*.db"), ("All files", "*.*")),
            )
            if not file_path:
                return
            ensure_line_table(Path(file_path))
            selected["path"] = file_path
            root.destroy()

        def select_file() -> None:
            file_path = filedialog.askopenfilename(
                title="既存ファイルを選択",
                filetypes=(("DB file", "*.db"), ("All files", "*.*")),
            )
            if not file_path:
                return
            selected["path"] = file_path
            root.destroy()

        tk.Button(button_row, text="New", command=create_new, **button_style).pack(side=tk.LEFT, padx=26)
        tk.Button(button_row, text="Select File", command=select_file, **button_style).pack(side=tk.LEFT, padx=26)

        root.mainloop()

        if not selected["path"]:
            break
        if run_line_selection_screen(selected["path"]) != "main_menu":
            break

    return 0
