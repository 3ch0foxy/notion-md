#!/usr/bin/env python3
import requests
import json
import argparse
import os
from multiprocessing import Pool

# Function to safely extract image URLs
def get_image(block):
    content = block.get("content", {})
    return content.get("file", {}).get("url", "") or content.get("external", {}).get("url", "")

# Function to handle rich text annotations in Notion
def parse_annotations(annotations, text):
    if annotations.get("code"): text = f"`{text}`"
    if annotations.get("bold"): text = f"**{text}**"
    if annotations.get("italic"): text = f"*{text}*"
    if annotations.get("strikethrough"): text = f"~~{text}~~"
    if annotations.get("underline"): text = f"<u>{text}</u>"
    if "background" in annotations.get("color", ""): text = f"<mark>{text}</mark>"
    return text

# Function to parse Notion block types into Markdown
def parse_block_type(block, numbered_list_index, depth):
    block_type = block.get("type", "")
    result = ""

    if block_type == "divider":
        return "---"
    if block_type == "image":
        return get_image(block)
    
    rich_text = block.get("content", {}).get("rich_text", [])
    for text_data in rich_text:
        text = parse_annotations(text_data["annotations"], text_data["plain_text"])
        if text_data.get("href"):
            text = f"[{text}]({text_data['href']})"
        result += text

    # Formatting based on block type
    format_map = {
        "heading_1": f"# {result}",
        "heading_2": f"## {result}",
        "heading_3": f"### {result}",
        "code": f"```{block.get('content', {}).get('language', '')}\n{result}\n```",
        "bulleted_list_item": f"- {result}",
        "numbered_list_item": f"{numbered_list_index}. {result}",
        "to_do": f"- {'[x]' if block.get('content', {}).get('checked') else '[ ]'} {result}",
        "quote": f"> {result}",
    }
    return "\t" * depth + format_map.get(block_type, result)

# Recursively renders Notion page content
def render_page(blocks, depth=0):
    page = ""
    numbered_list_index = 0
    for block in blocks:
        numbered_list_index = numbered_list_index + 1 if block.get("type") == "numbered_list_item" else 0
        text = parse_block_type(block, numbered_list_index, depth)
        if text:
            page += f"\n\n{text}"
        if block.get("children"):
            page += render_page(block["children"], depth + 1)
    return page

# Fetches child blocks recursively
def query_blocks(page_id, start_cursor=None, blocks=None):
    result = blocks if blocks else []
    url = f"https://api.notion.com/v1/blocks/{page_id}/children"
    if start_cursor:
        url += f"?start_cursor={start_cursor}"
    
    response = requests.get(url, headers=headers).json()
    
    for item in response.get("results", []):
        children = query_blocks(item["id"]) if item.get("has_children") else []
        result.append({"id": item["id"], "type": item["type"], "content": item.get(item["type"], {}), "children": children})
    
    return query_blocks(page_id, response.get("next_cursor"), result) if response.get("has_more") else result

# Parses frontmatter data safely
def parse_frontmatter(properties):
    return json.dumps({
        "categories": [item["name"] for item in properties.get("Categories", {}).get("multi_select", [])],
        "date": properties.get("Date", {}).get("date", {}).get("start", ""),
        "tags": [item["name"] for item in properties.get("Tags", {}).get("multi_select", [])],
        "title": properties.get("Title", {}).get("title", [{}])[0].get("plain_text", "Untitled"),
        "url": properties.get("URL", {}).get("url", ""),
    })

# Fetches pages from the Notion database
def query_db(db_id):
    result = {}
    url = f"https://api.notion.com/v1/databases/{db_id}/query"
    response = requests.post(url, headers=headers).json()

    for item in response.get("results", []):
        if args.hugo and item.get("properties", {}).get("Published", {}).get("checkbox"):
            result[item["id"]] = parse_frontmatter(item["properties"])
        else:
            result[item["id"]] = ""

    return query_db(db_id, response.get("next_cursor"), result) if response.get("has_more") else result

# Multithreading for page rendering
def multi_thread(page_items):
    page_id, frontmatter = page_items
    blocks = query_blocks(page_id)
    content = render_page(blocks)
    with open(f"{args.content}/{page_id}.md", "w") as file:
        file.write(frontmatter)
        file.write(content)

# Directory validation
def valid_dir(target):
    if os.path.exists(target):
        return target
    raise argparse.ArgumentTypeError(f"The directory '{target}' does not exist")

# Main script execution
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fetch Notion pages and convert them to Markdown")
    parser.add_argument("--static", type=valid_dir, help="Static path folder", required=True)
    parser.add_argument("--url", type=str, help="URL for static files", required=True)
    parser.add_argument("--content", type=valid_dir, help="Content path folder", required=True)
    parser.add_argument("--db", type=str, help="Database ID", required=True)
    parser.add_argument("--key", type=str, help="Notion API key", required=True)
    parser.add_argument("--hugo", action=argparse.BooleanOptionalAction, help="Add page frontmatter for Hugo")

    args = parser.parse_args()

    headers = {
        "accept": "application/json",
        "content-type": "application/json",
        "Notion-Version": "2022-06-28",
        "Authorization": f"Bearer {args.key}"
    }

    pages = query_db(args.db)
    thread_count = min(os.cpu_count(), 3)

    with Pool(thread_count) as p:
        p.map(multi_thread, pages.items())
