#!/usr/bin/env python3
import requests
import json
import argparse
import os
from multiprocessing import Pool

# Function to safely extract image URLs and save the image locally
def get_image(block):
    url = block.get("content", {}).get("file", {}).get("url", "")
    if not url:
        return ""
    filename = f"{block['id']}.{url.split('/')[-1].split('?')[0].split('.')[-1]}"
    image_data = requests.get(url).content
    with open(f"{args.static}/{filename}", "wb") as file:
        file.write(image_data)
    image_path = os.path.join(args.url, filename)
    return f"![]({image_path}#center)"

# Function to handle rich text annotations in Notion
def parse_annotations(annotations, text):
    if annotations.get("code"):
        text = f"`{text}`"
    if annotations.get("bold"):
        text = f"**{text}**"
    if annotations.get("italic"):
        text = f"*{text}*"
    if annotations.get("strikethrough"):
        text = f"~~{text}~~"
    if annotations.get("underline"):
        text = f"<u>{text}</u>"
    if "background" in annotations.get("color", ""):
        text = f"<mark>{text}</mark>"
    return text

# Function to parse a Notion block into Markdown format
def parse_block_type(block, numbered_list_index, depth):
    block_type = block.get("type", "")
    result = ""

    if block_type == "divider":
        return "---"
    if block_type == "image":
        return get_image(block)
    
    rich_texts = block.get("content", {}).get("rich_text", [])
    for text_data in rich_texts:
        text = parse_annotations(text_data.get("annotations", {}), text_data.get("plain_text", ""))
        if text_data.get("href"):
            text = f"[{text}]({text_data['href']})"
        result += text

    if result:
        if block_type == "heading_1":
            result = f"# {result}"
        elif block_type == "heading_2":
            result = f"## {result}"
        elif block_type == "heading_3":
            result = f"### {result}"
        elif block_type == "code":
            result = f"```{block.get('content', {}).get('language', '')}\n{result}\n```"
        elif block_type == "bulleted_list_item":
            result = f"- {result}"
        elif block_type == "numbered_list_item":
            result = f"{numbered_list_index}. {result}"
        elif block_type == "to_do":
            if block.get("content", {}).get("checked"):
                result = f"- [x] {result}"
            else:
                result = f"- [ ] {result}"
        elif block_type == "quote":
            result = f"> {result}"
        result = "\t" * depth + result
    return result

# Recursively renders Notion page content into Markdown
def render_page(blocks, depth=0):
    page = ""
    numbered_list_index = 0
    for block in blocks:
        if block.get("type") == "numbered_list_item":
            numbered_list_index += 1
        else:
            numbered_list_index = 0
        text = parse_block_type(block, numbered_list_index, depth)
        if text:
            page += f"\n\n{text}"
        if block.get("children"):
            page += render_page(block["children"], depth + 1)
    return page

# Fetches the child blocks from a Notion page recursively
def query_blocks(page_id, start_cursor=None, blocks=None):
    result = blocks if blocks else []
    url = f"https://api.notion.com/v1/blocks/{page_id}/children"
    if start_cursor:
        url += f"?start_cursor={start_cursor}"
    response = requests.get(url, headers=headers).json()
    for item in response.get("results", []):
        children = query_blocks(item["id"]) if item.get("has_children") else []
        result.append({
            "id": item["id"],
            "type": item["type"],
            "content": item.get(item["type"], {}),
            "children": children
        })
    if response.get("has_more"):
        result = query_blocks(page_id, response.get("next_cursor"), result)
    return result

# Parses the frontmatter data from a Notion page using your database attributes.
# Notice: The "Summary" attribute has been completely removed.
def parse_frontmatter(properties):
    return json.dumps({
        "title": properties.get("Title", {}).get("title", [{}])[0].get("plain_text", "Untitled"),
        "date": properties.get("Date", {}).get("date", {}).get("start", ""),
        "tags": [item["name"] for item in properties.get("Tags", {}).get("multi_select", [])],
        "categories": [item["name"] for item in properties.get("Categories", {}).get("multi_select", [])],
        "url": properties.get("URL", {}).get("url", ""),
        "published": properties.get("Published", {}).get("checkbox", False)
    })

# Queries your Notion database and creates a mapping for page IDs to frontmatter.
def query_db(db_id):
    result = {}
    url = f"https://api.notion.com/v1/databases/{db_id}/query"
    response = requests.post(url, headers=headers).json()
    for item in response.get("results", []):
        if args.hugo and item.get("properties", {}).get("Published", {}).get("checkbox"):
            result[item["id"]] = parse_frontmatter(item.get("properties", {}))
        else:
            result[item["id"]] = ""
    while response.get("has_more"):
        data = {"start_cursor": response.get("next_cursor")}
        response = requests.post(url, headers=headers, data=json.dumps(data)).json()
        for item in response.get("results", []):
            if args.hugo and item.get("properties", {}).get("Published", {}).get("checkbox"):
                result[item["id"]] = parse_frontmatter(item.get("properties", {}))
            else:
                result[item["id"]] = ""
    return result

# Renders a page concurrently using multiprocessing
def multi_thread(page_items):
    page_id, frontmatter = page_items
    blocks = query_blocks(page_id)
    content = render_page(blocks)
    with open(f"{args.content}/{page_id}.md", "w") as file:
        file.write(frontmatter)
        file.write(content)

# Validates if a provided directory exists
def valid_dir(target):
    if os.path.exists(target):
        return target
    raise argparse.ArgumentTypeError(f"The directory '{target}' does not exist")

# Main script execution: Parse arguments and execute the workflow
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
