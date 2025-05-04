# notion-md-fetcher

Script to fetch pages from Notion database and convert it to Markdown using official [Notion API](https://developers.notion.com/).

## How to use

```bash
script.py --content CONTENT_FOLDER --static STATIC_FOLDER --url STATIC_URL --db NOTION_DB_ID --key NOTION_API_KEY [--hugo]
```

`CONTENT_FOLDER` path where the pages will be saved

`STATIC_FOLDER` path where the static files will be saved

`STATIC_URL` URL from which the static files will be accessible from the Markdown file

`NOTION_DB_ID` Notion database id where the pages to fetch are located

`NOTION_API_KEY` Notion API key

To get `NOTION_DB_ID` and `NOTION_API_KEY` you can refer to steps 1 to 3 of [this article](https://developers.notion.com/docs/create-a-notion-integration).

## Use it for Hugo

This script can be used for [Hugo](https://gohugo.io/), for that use [this database template](https://malsius.notion.site/3602f007cbae4b76a4998a78caba0079?v=548ff3a915134267960b702e4b70047a) and add `--hugo` flag.

It will add the front matter to each page and fetch only published ones.

## Features

- [x] heading
- [x] divider
- [x] image 
- [x] code block
- [x] inline annotation (link, code, bold, italic, strikethrough, underline, highlight)
- [x] bulleted list
- [x] numbered list
- [x] to do list
- [x] quote
- [x] block children

### For Hugo

- [x] convert page properties to front matter
- [x] only fetch published pages