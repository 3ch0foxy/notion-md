#!/usr/bin/env python3
import requests
import json
import argparse
import os

from multiprocessing import Pool


# PaperMod don't show image's caption
def get_image(block):
    content = block.get("content", {})
    # If the block has a 'file' key and it contains a URL, use that.
    if "file" in content and content["file"].get("url"):
        return content["file"]["url"]
    # Alternatively, check for an 'external' key (for externally hosted images)
    elif "external" in content and content["external"].get("url"):
        return content["external"]["url"]
    # Otherwise, return an empty string (or handle it as needed)
    return ""


def parse_annotations(annotations, text):
    if annotations["code"]:
        text = f"`{text}`"
    if annotations["bold"]:
        text = f"**{text}**"
    if annotations["italic"]:
        text = f"*{text}*"
    if annotations["strikethrough"]:
        text = f"~~{text}~~"
    if annotations["underline"]:
        text = f"<u>{text}</u>"
    if "background" in annotations["color"]:
        text = f"<mark>{text}</mark>"
    return text


def parse_block_type(block, numbered_list_index, depth):
    if block["type"] == "divider":
        return "---"
    if block["type"] == "image":
        return get_image(block)
    
    result = ""
    content = block.get("content", {})
    # Only iterate if 'rich_text' is present
    if "rich_text" in content:
        for rich_text in content["rich_text"]:
            text = parse_annotations(rich_text["annotations"], rich_text["plain_text"])
            if rich_text.get("href"):
                text = f"[{text}]({rich_text['href']})"
            result += text
    else:
        # If 'rich_text' is missing, return an empty string or handle it as needed.
        result = ""
    
    if result:
        if block["type"] == "heading_1":
            result = f"# {result}"
        elif block["type"] == "heading_2":
            result = f"## {result}"
        elif block["type"] == "heading_3":
            result = f"### {result}"
        elif block["type"] == "code":
            result = f"```{block['content'].get('language', '')}\n{result}\n```"
        elif block["type"] == "bulleted_list_item":
            result = f"- {result}"
        elif block["type"] == "numbered_list_item":
            result = f"{numbered_list_index}. {result}"
        elif block["type"] == "to_do":
            if block["content"].get("checked"):
                result = f"- [x] {result}"
            else:
                result = f"- [ ] {result}"
        if block["type"] == "quote":
            result = f"> {result}"
        result = "\t" * depth + result
    return result



def render_page(blocks, depth):
    page = ""
    numbered_list_index = 0
    for block in blocks:
        if block["type"] == "numbered_list_item":
            numbered_list_index += 1
        else:
            numbered_list_index = 0
        text = parse_block_type(block, numbered_list_index, depth)
        if text:
            page += f"\n\n{text}"
        if block["children"]:
            page += render_page(block["children"], depth + 1)
    return page


def query_blocks(page_id, start_cursor=None, blocks=None):
    if blocks:
        result = blocks
    else:
        result = []
    if start_cursor:
        url = f"https://api.notion.com/v1/blocks/{page_id}/children?start_cursor={start_cursor}"
    else:
        url = f"https://api.notion.com/v1/blocks/{page_id}/children"
    response = requests.get(url, headers=headers).json()
    for item in response["results"]:
        children = []
        if item["has_children"]:
            children = query_blocks(item["id"])
        result.append({
            "id": item["id"],
            "type": item["type"],
            "content": item[item["type"]],
            "children": children
        })
    if response["has_more"]:
        result = query_blocks(page_id, response["next_cursor"], result)
    return result


def parse_frontmatter(properties):
    # Optional: If you don't use Categories, this will default to an empty list.
    categories = []
    if "Categories" in properties:
        categories = [item["name"] for item in properties["Categories"].get("multi_select", [])]
    
    # Build the frontmatter using only the fields you have.
    frontmatter = {
        "categories": categories,  # Will be an empty list if there's no Categories property.
        "date": properties["Date"]["date"]["start"] if "Date" in properties else "",
        "tags": [item["name"] for item in properties["Tags"].get("multi_select", [])] if "Tags" in properties else [],
        "title": properties["Title"]["title"][0]["plain_text"] if "Title" in properties and properties["Title"]["title"] else "Untitled",
        "url": properties["URL"]["url"] if "URL" in properties else ""
    }
    return json.dumps(frontmatter)



def query_db(db_id):
    result = {}
    url = f"https://api.notion.com/v1/databases/{db_id}/query"
    response = requests.post(url, headers=headers).json()
    for item in response["results"]:
        if args.hugo:
            if item["properties"]["Published"]["checkbox"]:
                result[item["id"]] = parse_frontmatter(item["properties"])
        else :
            result[item["id"]] = ""
    while response["has_more"]:
        data = {"start_cursor": response["next_cursor"]}
        response = requests.post(url, headers=headers, data=data).json()
        for item in response["results"]:
            if args.hugo:
                if item["properties"]["Published"]["checkbox"]:
                    result[item["id"]] = parse_frontmatter(item["properties"])
            else :
                result[item["id"]] = ""
    return result


def multi_thread(page_items):
    page_id = page_items[0]
    frontmatter = page_items[1]
    blocks = query_blocks(page_id)
    content = render_page(blocks, 0)
    with open(f"{args.content}/{page_id}.md", "w") as file:
        file.write(frontmatter)
        file.write(content)


def valid_dir(target):
    if os.path.exists(target):
        return target
    raise argparse.ArgumentTypeError(f"The directory '{target}' does not exist")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Get Notion pages from specified database and convert it to Markdown")

    parser.add_argument("--static", type=valid_dir, help="static path folder", required=True)
    parser.add_argument("--url", type=str, help="URL for static files", required=True)
    parser.add_argument("--content", type=valid_dir, help="content path folder", required=True)
    parser.add_argument("--db", type=str, help="database ID", required=True)
    parser.add_argument("--key", type=str, help="Notion API key", required=True)
    parser.add_argument("--hugo", action=argparse.BooleanOptionalAction, help="add page front matter for Hugo")

    args = parser.parse_args()

    headers = {
        "accept": "application/json",
        "content-type": "application/json",
        "Notion-Version": "2022-06-28",
        "Authorization": f"Bearer {args.key}"
    }

    pages = query_db(args.db)

    # Notion's API limit to 3 requests per second
    thread = min(os.cpu_count(), 3)

    with Pool(thread) as p:
        p.map(multi_thread, pages.items())
