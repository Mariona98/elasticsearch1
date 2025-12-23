from elasticsearch import Elasticsearch, helpers
import pandas as pd
import math
import os
import tkinter as tk
from tkinter import filedialog, messagebox, ttk
from datetime import datetime

# ===============================
# CONNECT TO ELASTICSEARCH
# ===============================

es = Elasticsearch("http://localhost:9200", verify_certs=False)
index_name = "posts"

if not es.ping():
    raise RuntimeError("Cannot connect to ElasticSearch")

# ===============================
# CLEANING FUNCTIONS
# ===============================

def clean_value(value):
    if value is None:
        return None
    if isinstance(value, float):
        if math.isnan(value) or math.isinf(value):
            return None
    if isinstance(value, str):
        v = value.strip()
        if v.lower() in ["nan", "null", "none", "n/a", "na", ""]:
            return None
    return value


def clean_document(doc):
    return {k: clean_value(v) for k, v in doc.items()}

# ===============================
# INDEX DEFINITION
# ===============================

index_body = {
    "settings": {
        "analysis": {
            "analyzer": {
                "post_text_analyzer": {
                    "type": "standard",
                    "stopwords": "_english_"
                }
            }
        }
    },
    "mappings": {
        "properties": {
            "status_message": {"type": "text", "analyzer": "post_text_analyzer"},
            "link_name": {"type": "keyword"},
            "status_type": {"type": "keyword"},
            "status_link": {"type": "keyword"},
            "status_published": {"type": "date"},
            "num_reactions": {"type": "integer"}
        }
    }
}

if not es.indices.exists(index=index_name):
    es.indices.create(index=index_name, body=index_body)

# ===============================
# BACKEND FUNCTIONS
# ===============================

def import_posts(csv_file):
    if not os.path.exists(csv_file):
        messagebox.showerror("Error", "CSV file not found")
        return

    try:
        df = pd.read_csv(csv_file)
    except Exception as e:
        messagebox.showerror("Error", f"Cannot read CSV:\n{e}")
        return

    actions = []
    for _, row in df.iterrows():
        doc = clean_document(row.to_dict())
        actions.append({"_index": index_name, "_source": doc})

    helpers.bulk(es, actions)
    es.indices.refresh(index=index_name)
    messagebox.showinfo("Success", f"Imported {len(actions)} posts")

# -------------------------------
# SEARCH WITH FILTERS & BOOLEAN
# -------------------------------

def search_posts(query, min_likes, max_likes, date_from, date_to, k):
    must = []
    filter_clauses = []

    if query.strip():
        must.append({
            "query_string": {
                "query": query,
                "fields": ["status_message"]
            }
        })

    if min_likes or max_likes:
        likes_range = {}
        if min_likes:
            likes_range["gte"] = int(min_likes)
        if max_likes:
            likes_range["lte"] = int(max_likes)
        filter_clauses.append({"range": {"num_reactions": likes_range}})

    if date_from or date_to:
        date_range = {}
        if date_from:
            date_range["gte"] = date_from
        if date_to:
            date_range["lte"] = date_to
        filter_clauses.append({"range": {"status_published": date_range}})

    body = {
        "size": k,
        "query": {
            "bool": {
                "must": must if must else [{"match_all": {}}],
                "filter": filter_clauses
            }
        },
        "highlight": {
            "fields": {
                "status_message": {}
            }
        }
    }

    return es.search(index=index_name, body=body)["hits"]["hits"]

# -------------------------------
# SIMILARITY SEARCH
# -------------------------------

def find_similar(post_id, k):
    body = {
        "size": k,
        "query": {
            "more_like_this": {
                "fields": ["status_message"],
                "like": [{"_index": index_name, "_id": post_id}],
                "min_term_freq": 1,
                "min_doc_freq": 1
            }
        }
    }
    return es.search(index=index_name, body=body)["hits"]["hits"]

# -------------------------------
# DELETE
# -------------------------------

def delete_posts(ids):
    actions = [
        {"_op_type": "delete", "_index": index_name, "_id": pid}
        for pid in ids
    ]
    helpers.bulk(es, actions, ignore_status=[404])
    es.indices.refresh(index=index_name)

# ===============================
# GUI
# ===============================

root = tk.Tk()
root.title("TrASH Search Engine")

# --- Import CSV ---
tk.Button(root, text="Import CSV", command=lambda: import_posts(
    filedialog.askopenfilename(filetypes=[("CSV Files", "*.csv")])
)).pack(pady=5)

# --- Query controls ---
controls = tk.Frame(root)
controls.pack(pady=5)

tk.Label(controls, text="Query").grid(row=0, column=0)
query_entry = tk.Entry(controls, width=40)
query_entry.grid(row=0, column=1, columnspan=3)

tk.Label(controls, text="Min Likes").grid(row=1, column=0)
min_likes = tk.Entry(controls, width=8)
min_likes.grid(row=1, column=1)

tk.Label(controls, text="Max Likes").grid(row=1, column=2)
max_likes = tk.Entry(controls, width=8)
max_likes.grid(row=1, column=3)

tk.Label(controls, text="From Date").grid(row=2, column=0)
date_from = tk.Entry(controls, width=12)
date_from.grid(row=2, column=1)

tk.Label(controls, text="To Date").grid(row=2, column=2)
date_to = tk.Entry(controls, width=12)
date_to.grid(row=2, column=3)

tk.Label(controls, text="Top-K").grid(row=3, column=0)
top_k = tk.Entry(controls, width=8)
top_k.insert(0, "10")
top_k.grid(row=3, column=1)

# --- Results ---
results = ttk.Treeview(root, columns=("score", "content"), show="headings")
results.heading("score", text="Score")
results.heading("content", text="Snippet")
results.column("score", width=80, anchor=tk.CENTER)
results.column("content", width=700)
results.pack(fill=tk.BOTH, expand=True)

# --- Actions ---

def run_search():
    results.delete(*results.get_children())
    hits = search_posts(
        query_entry.get(),
        min_likes.get(),
        max_likes.get(),
        date_from.get(),
        date_to.get(),
        int(top_k.get())
    )

    for h in hits:
        snippet = h.get("highlight", {}).get("status_message", [h["_source"].get("status_message", "")])[0]
        results.insert("", tk.END, iid=h["_id"], values=(round(h["_score"], 2), snippet))

tk.Button(root, text="Search", command=run_search).pack(pady=5)

def delete_selected():
    ids = results.selection()
    if ids and messagebox.askyesno("Confirm", "Delete selected posts?"):
        delete_posts(ids)
        run_search()

tk.Button(root, text="Delete Selected", command=delete_selected).pack(pady=5)

def similar_posts():
    sel = results.selection()
    if not sel:
        return
    results.delete(*results.get_children())
    hits = find_similar(sel[0], int(top_k.get()))
    for h in hits:
        results.insert("", tk.END, iid=h["_id"],
                       values=(round(h["_score"], 2), h["_source"].get("status_message", "")[:300]))

tk.Button(root, text="Find Similar", command=similar_posts).pack(pady=5)

root.mainloop()
