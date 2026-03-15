"""Line selection GUI screen for the NIMBY passenger tool."""

from __future__ import annotations

from pathlib import Path
import sqlite3


def ensure_line_table(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(path) as conn:
        conn.execute("CREATE TABLE IF NOT EXISTS lines (line_id TEXT PRIMARY KEY)")


def load_line_ids(path: Path) -> list[str]:
    ensure_line_table(path)
    with sqlite3.connect(path) as conn:
        rows = conn.execute("SELECT line_id FROM lines ORDER BY line_id").fetchall()
    return [row[0] for row in rows]


def add_line(path: Path, line_id: str) -> None:
    ensure_line_table(path)
    with sqlite3.connect(path) as conn:
        conn.execute("INSERT INTO lines(line_id) VALUES (?)", (line_id,))


def build_line_selection_title(db_path: Path) -> str:
    return f"路線選択画面（DBファイル名: {db_path.name}）"


def run_line_selection_screen(project_path: str) -> str:
    import tkinter as tk
    from tkinter import messagebox, simpledialog

    db_path = Path(project_path)

    root = tk.Tk()
    screen_title = build_line_selection_title(db_path)
    root.title(screen_title)
    root.configure(bg="#1f1f1f")
    root.geometry("960x540")
    root.minsize(800, 450)

    action = {"next": "exit"}

    title = tk.Label(
        root,
        text=screen_title,
        fg="#f5f5f5",
        bg="#1f1f1f",
        font=("Arial", 28, "bold"),
    )
    title.pack(anchor="w", padx=30, pady=(16, 10))

    body = tk.Frame(root, bg="#1f1f1f")
    body.pack(fill=tk.BOTH, expand=True, padx=30, pady=(0, 20))

    left = tk.Frame(body, bg="#3a3a3a", width=220)
    left.pack(side=tk.LEFT, fill=tk.Y)
    left.pack_propagate(False)

    right = tk.Frame(body, bg="#000000", bd=1, relief=tk.SUNKEN)
    right.pack(side=tk.LEFT, fill=tk.BOTH, expand=True, padx=(28, 0))

    line_list = tk.Listbox(
        right,
        bg="#000000",
        fg="#ffffff",
        highlightthickness=0,
        selectbackground="#1f3b66",
        selectforeground="#ffffff",
        font=("Arial", 20, "bold"),
        activestyle="none",
        bd=0,
    )
    line_list.pack(fill=tk.BOTH, expand=True, padx=12, pady=12)

    def refresh_lines() -> None:
        line_list.delete(0, tk.END)
        for line_id in load_line_ids(db_path):
            line_list.insert(tk.END, line_id)

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

    def create_line() -> None:
        line_id = simpledialog.askstring("New Line", "路線名を入力してください", parent=root)
        if not line_id:
            return
        try:
            add_line(db_path, line_id.strip())
        except sqlite3.IntegrityError:
            messagebox.showerror("Error", f"同名の路線が既に存在します: {line_id}")
            return
        refresh_lines()

    def return_to_main_menu() -> None:
        action["next"] = "main_menu"
        root.destroy()

    tk.Button(left, text="New Line", command=create_line, **button_style).pack(padx=14, pady=(24, 10))
    tk.Button(left, text="← Main Menu", command=return_to_main_menu, **button_style).pack(side=tk.BOTTOM, padx=14, pady=18)

    refresh_lines()
    root.mainloop()
    return action["next"]
