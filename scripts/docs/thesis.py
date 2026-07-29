#!/usr/bin/env python3
"""Build and validate the canonical Markdown master thesis."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import socket
import subprocess
import sys
import tempfile
import time
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from zipfile import ZIP_DEFLATED, ZipFile

ROOT = Path(__file__).resolve().parents[2]
SOURCE = ROOT / "docs/master_thesis_report.md"
TEMPLATE = ROOT / "docs/2026 Master thesis template.docx"
SUPPORT = ROOT / "docs/thesis"
BIBLIOGRAPHY = SUPPORT / "references.bib"
CSL = SUPPORT / "university-numeric.csl"
FIGURE = SUPPORT / "architecture.svg"
BUILD_DIR = ROOT / "build/thesis"
DERIVED_FIGURE = BUILD_DIR / "architecture.png"
GENERATED_DOCX = BUILD_DIR / "master_thesis.generated.docx"
GENERATED_PDF = BUILD_DIR / "master_thesis.generated.pdf"
FINAL_DOCX = BUILD_DIR / "master_thesis.docx"
FINAL_PDF = BUILD_DIR / "master_thesis.pdf"
MANIFEST = BUILD_DIR / "build-manifest.json"
VISUAL_REVIEW = BUILD_DIR / "visual-review.json"

W_NS = "http://schemas.openxmlformats.org/wordprocessingml/2006/main"
W = f"{{{W_NS}}}"
ET.register_namespace("w", W_NS)
TABLE_INDEX_TOKEN = "THESIS_TABLE_INDEX_PLACEHOLDER"
FIGURE_INDEX_TOKEN = "THESIS_FIGURE_INDEX_PLACEHOLDER"

EXPECTED_PANDOC = "3.10.1"
EXPECTED_LIBREOFFICE_PREFIX = "24.2"
WORD_RE = re.compile(r"\b[\w’'-]+\b", re.UNICODE)
CITATION_RE = re.compile(r"(?<![\w@])@([A-Za-z0-9_:-]+)")
BIB_ENTRY_RE = re.compile(r"(?m)^@([A-Za-z]+)\{([^,\s]+),")


@dataclass
class CheckResult:
    errors: list[str]
    warnings: list[str]
    facts: list[str]

    def error(self, message: str) -> None:
        self.errors.append(message)

    def warning(self, message: str) -> None:
        self.warnings.append(message)

    def fact(self, message: str) -> None:
        self.facts.append(message)


def run(
    command: list[str],
    *,
    cwd: Path = ROOT,
    check: bool = True,
    capture: bool = True,
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        command,
        cwd=cwd,
        check=check,
        text=True,
        stdout=subprocess.PIPE if capture else None,
        stderr=subprocess.PIPE if capture else None,
    )


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def words(text: str) -> int:
    text = re.sub(r"```.*?```", " ", text, flags=re.DOTALL)
    text = re.sub(r"<[^>]+>", " ", text)
    return len(WORD_RE.findall(text))


def section(text: str, start: str, end: str | None) -> str:
    start_index = text.index(start) + len(start)
    end_index = text.index(end, start_index) if end else len(text)
    return text[start_index:end_index]


def parse_bib_entries(text: str) -> dict[str, str]:
    matches = list(BIB_ENTRY_RE.finditer(text))
    entries: dict[str, str] = {}
    for index, match in enumerate(matches):
        end = matches[index + 1].start() if index + 1 < len(matches) else len(text)
        entries[match.group(2)] = text[match.start() : end]
    return entries


def heading_ids(markdown: str) -> set[str]:
    ids = set(re.findall(r'<a\s+id="([^"]+)"', markdown))
    for title in re.findall(r"(?m)^#{1,6}\s+(.+?)\s*$", markdown):
        clean = re.sub(r"[^\w\s-]", "", title.lower(), flags=re.UNICODE)
        ids.add(re.sub(r"[-\s]+", "-", clean).strip("-"))
    return ids


def check_links(markdown: str, result: CheckResult) -> None:
    ids = heading_ids(markdown)
    for target in re.findall(r"!?\[[^\]]*\]\(([^)]+)\)", markdown):
        target = target.split()[0].strip("<>")
        if target.startswith(("http://", "https://", "mailto:")):
            continue
        if target.startswith("#"):
            if target[1:] not in ids:
                result.error(f"Broken document anchor: {target}")
            continue
        relative, _, anchor = target.partition("#")
        path = (SOURCE.parent / relative).resolve()
        if not path.exists():
            result.error(f"Broken local link: {target}")
        elif anchor and path == SOURCE.resolve() and anchor not in ids:
            result.error(f"Broken local link anchor: {target}")


def check_template(result: CheckResult) -> None:
    if not TEMPLATE.exists():
        result.error(f"Missing university template: {TEMPLATE.relative_to(ROOT)}")
        return
    with ZipFile(TEMPLATE) as archive:
        names = set(archive.namelist())
        document = archive.read("word/document.xml").decode("utf-8")
        styles = archive.read("word/styles.xml").decode("utf-8")
    if document.count("<w:sectPr") != 3:
        result.error(
            "University template must contain exactly three section properties"
        )
    if "TOC" not in document or "<w:instrText" not in document:
        result.error("University template does not contain a TOC field")
    footers = {name for name in names if name.startswith("word/footer")}
    if len(footers) != 3:
        result.error("University template must retain three footer parts")
    for style in ("Heading1", "Heading2", "Heading3"):
        if style not in styles:
            result.error(f"University template is missing style {style}")
    result.fact("Template invariants: 3 section properties, TOC field, 3 footers")


def check_content() -> CheckResult:
    result = CheckResult(errors=[], warnings=[], facts=[])
    required = (SOURCE, TEMPLATE, BIBLIOGRAPHY, CSL, FIGURE)
    for path in required:
        if not path.exists():
            result.error(f"Missing required input: {path.relative_to(ROOT)}")
    if result.errors:
        return result

    markdown = SOURCE.read_text(encoding="utf-8")
    bibliography = BIBLIOGRAPHY.read_text(encoding="utf-8")

    chapter_specs = [
        ("Introduction", "# I. INTRODUCTION", "# II. OBJECTIVES", 1300, 1500),
        ("Objectives", "# II. OBJECTIVES", "# III. MATERIALS AND METHODS", 100, 150),
        (
            "Materials and Methods",
            "# III. MATERIALS AND METHODS",
            "# IV. RESULTS AND DISCUSSION",
            1800,
            2100,
        ),
        (
            "Results and Discussion",
            "# IV. RESULTS AND DISCUSSION",
            "# V. CONCLUSION AND PERSPECTIVE",
            4000,
            4500,
        ),
        (
            "Conclusion and Perspective",
            "# V. CONCLUSION AND PERSPECTIVE",
            "# REFERENCES",
            700,
            900,
        ),
    ]
    body_words = 0
    for name, start, end, minimum, maximum in chapter_specs:
        try:
            count = words(section(markdown, start, end))
        except ValueError:
            result.error(f"Missing chapter boundary for {name}")
            continue
        body_words += count
        result.fact(f"{name}: {count} words")
        if not minimum <= count <= maximum:
            result.error(f"{name} must be {minimum}-{maximum} words; found {count}")
    result.fact(f"Thesis body: {body_words} words")
    if not 8000 <= body_words <= 10000:
        result.error(f"Thesis body must be 8000-10000 words; found {body_words}")

    try:
        abstract_block = section(markdown, "# ABSTRACT", "# I. INTRODUCTION")
        abstract_text, keyword_text = abstract_block.split("**Keywords:**", maxsplit=1)
        abstract_words = words(abstract_text)
        result.fact(f"Abstract: {abstract_words} words")
        if abstract_words > 250:
            result.error(f"Abstract must be at most 250 words; found {abstract_words}")
        keyword_line = " ".join(keyword_text.split("---", maxsplit=1)[0].split())
        keywords = [item.strip() for item in keyword_line.split(";") if item.strip()]
        result.fact(f"Keywords: {len(keywords)}")
        if len(keywords) != 6:
            result.error(
                f"Abstract must contain exactly six keywords; found {len(keywords)}"
            )
    except (ValueError, IndexError):
        result.error("Abstract or keyword block is malformed")

    entries = parse_bib_entries(bibliography)
    cited = set(CITATION_RE.findall(markdown))
    result.fact(f"Bibliography entries: {len(entries)}")
    result.fact(f"Unique cited references: {len(cited)}")
    if len(entries) != 24:
        result.error(
            f"Bibliography must contain exactly 24 entries; found {len(entries)}"
        )
    missing = cited - entries.keys()
    unused = entries.keys() - cited
    if missing:
        result.error(
            f"Citations without bibliography entries: {', '.join(sorted(missing))}"
        )
    if unused:
        result.error(f"Uncited bibliography entries: {', '.join(sorted(unused))}")

    expected_groups = {
        "thesis-systems-paper": 8,
        "thesis-methodology": 5,
        "thesis-authoritative-documentation": 8,
        "thesis-standard-dataset-security": 3,
    }
    for group, expected in expected_groups.items():
        actual = bibliography.count(group)
        result.fact(f"{group}: {actual}")
        if actual != expected:
            result.error(f"{group} must contain {expected} entries; found {actual}")
    for key, entry in entries.items():
        is_paper = "thesis-systems-paper" in entry or "thesis-methodology" in entry
        if is_paper and "pages =" not in entry:
            result.error(f"Scholarly reference lacks pagination/article number: {key}")
        if is_paper and "doi =" not in entry and "url =" not in entry:
            result.error(f"Scholarly reference lacks DOI or canonical URL: {key}")
        if "url =" in entry and "urldate =" not in entry:
            result.error(f"Online reference lacks access date: {key}")

    if re.search(r"(?m)(?<!\\)\[(?:\d+)(?:[,– -]+\d+)*\]", markdown):
        result.error("Manual numeric citations remain; use Pandoc citation keys")
    if markdown.count("```") % 2:
        result.error("Unbalanced fenced code blocks")
    check_links(markdown, result)

    stale_patterns = (
        r"\bawaiting acceptance\b",
        r"\buntested\b",
        r"\bresults? pending\b",
        r"\bexperiment pending\b",
    )
    for pattern in stale_patterns:
        if re.search(pattern, markdown, flags=re.IGNORECASE):
            result.error(f"Stale research-status language matches: {pattern}")

    required_claims = (
        "H1 is partially supported",
        "H2 is partially supported",
        "H3 is partially supported",
        "139.91% to 185.43%",
        "69.44% to 83.67%",
        "13 of 16",
        "10 of 16",
        "USD 0.97383823",
        "does not isolate object storage or network overhead as the cause",
    )
    for claim in required_claims:
        if claim not in markdown:
            result.error(f"Required accepted-evidence boundary is missing: {claim}")

    try:
        parsed = run(
            [
                "pandoc",
                str(SOURCE),
                "--from=markdown",
                "--to=native",
                "--citeproc",
                f"--bibliography={BIBLIOGRAPHY}",
                f"--csl={CSL}",
                f"--resource-path={SOURCE.parent}",
            ]
        )
        if parsed.stderr.strip():
            result.warning(f"Pandoc parse warning: {parsed.stderr.strip()}")
        result.fact("Pandoc citation and Markdown parse: passed")
    except (FileNotFoundError, subprocess.CalledProcessError) as error:
        detail = getattr(error, "stderr", "") or str(error)
        result.error(f"Pandoc citation/Markdown parse failed: {detail.strip()}")

    placeholders = len(re.findall(r"<TODO:|`<TODO:", markdown))
    result.fact(
        f"Personal/template placeholders allowed at content gate: {placeholders}"
    )
    check_template(result)
    return result


def tool_versions() -> dict[str, str]:
    pandoc = run(["pandoc", "--version"]).stdout.splitlines()[0]
    libreoffice = run(["libreoffice", "--version"]).stdout.strip()
    return {"pandoc": pandoc, "libreoffice": libreoffice}


def paragraph_text(element: ET.Element) -> str:
    return "".join(node.text or "" for node in element.findall(f".//{W}t")).strip()


def set_paragraph_style(element: ET.Element, style_id: str) -> None:
    properties = element.find(f"./{W}pPr")
    if properties is None:
        properties = ET.Element(f"{W}pPr")
        element.insert(0, properties)
    style = properties.find(f"./{W}pStyle")
    if style is None:
        style = ET.Element(f"{W}pStyle")
        properties.insert(0, style)
    style.set(f"{W}val", style_id)


def set_page_break_before(element: ET.Element) -> None:
    properties = element.find(f"./{W}pPr")
    if properties is None:
        properties = ET.Element(f"{W}pPr")
        element.insert(0, properties)
    if properties.find(f"./{W}pageBreakBefore") is None:
        properties.append(ET.Element(f"{W}pageBreakBefore"))


def token_paragraph(token: str, *, page_break: bool = False) -> ET.Element:
    paragraph = ET.Element(f"{W}p")
    if page_break:
        set_page_break_before(paragraph)
    run = ET.SubElement(paragraph, f"{W}r")
    text = ET.SubElement(run, f"{W}t")
    text.text = token
    return paragraph


def remove_descendants(element: ET.Element, tag: str) -> None:
    for parent in element.iter():
        for child in list(parent):
            if child.tag == f"{W}{tag}":
                parent.remove(child)


def clone_style(
    styles_root: ET.Element,
    source_id: str,
    target_id: str,
    display_name: str,
    *,
    remove_outline: bool = False,
) -> None:
    source = styles_root.find(f".//{W}style[@{W}styleId='{source_id}']")
    if source is None:
        raise RuntimeError(f"DOCX style {source_id} is missing")
    clone = ET.fromstring(ET.tostring(source))
    clone.set(f"{W}styleId", target_id)
    name = clone.find(f"./{W}name")
    if name is None:
        name = ET.Element(f"{W}name")
        clone.insert(0, name)
    name.set(f"{W}val", display_name)
    remove_descendants(clone, "numPr")
    if remove_outline:
        remove_descendants(clone, "outlineLvl")
    styles_root.append(clone)


def add_pandoc_compact_style(styles_root: ET.Element) -> None:
    if styles_root.find(f".//{W}style[@{W}styleId='Compact']") is not None:
        return
    style = ET.Element(
        f"{W}style",
        {f"{W}type": "paragraph", f"{W}customStyle": "1", f"{W}styleId": "Compact"},
    )
    ET.SubElement(style, f"{W}name", {f"{W}val": "Compact"})
    ET.SubElement(style, f"{W}basedOn", {f"{W}val": "BodyText"})
    ET.SubElement(style, f"{W}qFormat")
    properties = ET.SubElement(style, f"{W}pPr")
    ET.SubElement(properties, f"{W}spacing", {f"{W}after": "36", f"{W}before": "36"})
    styles_root.append(style)


def strip_template_numbering(styles_root: ET.Element) -> None:
    for style_id in ("Heading2", "Heading3"):
        style = styles_root.find(f".//{W}style[@{W}styleId='{style_id}']")
        if style is not None:
            remove_descendants(style, "numPr")


def replace_between(
    body: ET.Element,
    start_text: str,
    end_text: str,
    replacements: list[ET.Element],
    *,
    include_start: bool,
) -> None:
    children = list(body)
    start = next(
        index
        for index, child in enumerate(children)
        if paragraph_text(child) == start_text
    )
    end = next(
        index
        for index, child in enumerate(children[start + 1 :], start + 1)
        if paragraph_text(child) == end_text
    )
    first = start if include_start else start + 1
    for child in children[first:end]:
        body.remove(child)
    for offset, replacement in enumerate(replacements):
        body.insert(first + offset, replacement)


def add_section_to_previous_paragraph(
    body: ET.Element, heading_text: str, section: ET.Element
) -> None:
    children = list(body)
    heading_index = next(
        index
        for index, child in enumerate(children)
        if paragraph_text(child) == heading_text
    )
    previous = next(
        child for child in reversed(children[:heading_index]) if child.tag == f"{W}p"
    )
    properties = previous.find(f"./{W}pPr")
    if properties is None:
        properties = ET.Element(f"{W}pPr")
        previous.insert(0, properties)
    old = properties.find(f"./{W}sectPr")
    if old is not None:
        properties.remove(old)
    properties.append(ET.fromstring(ET.tostring(section)))


def normalize_docx_tables(document: ET.Element) -> None:
    """Make Pandoc tables portable to LibreOffice and the university template."""
    for table in document.findall(f".//{W}tbl"):
        properties = table.find(f"./{W}tblPr")
        if properties is None:
            properties = ET.Element(f"{W}tblPr")
            table.insert(0, properties)
        style = properties.find(f"./{W}tblStyle")
        if style is None:
            style = ET.Element(f"{W}tblStyle")
            properties.insert(0, style)
        # Pandoc requests style "Table", which the supplied template does not
        # define. LibreOffice then imports its cells as vertically stacked text.
        style.set(f"{W}val", "TableGrid")

        grid = table.find(f"./{W}tblGrid")
        columns = [] if grid is None else grid.findall(f"./{W}gridCol")
        widths = [int(column.get(f"{W}w", "1")) for column in columns]
        if not widths:
            continue
        target_width = 9000
        scale = target_width / sum(widths)
        normalized = [max(360, round(width * scale)) for width in widths]
        normalized[-1] += target_width - sum(normalized)
        for column, width in zip(columns, normalized, strict=True):
            column.set(f"{W}w", str(width))

        table_width = properties.find(f"./{W}tblW")
        if table_width is None:
            table_width = ET.SubElement(properties, f"{W}tblW")
        table_width.set(f"{W}type", "dxa")
        table_width.set(f"{W}w", str(target_width))
        layout = properties.find(f"./{W}tblLayout")
        if layout is None:
            layout = ET.SubElement(properties, f"{W}tblLayout")
        layout.set(f"{W}type", "fixed")

        for row in table.findall(f"./{W}tr"):
            cells = row.findall(f"./{W}tc")
            for cell, width in zip(cells, normalized, strict=False):
                cell_properties = cell.find(f"./{W}tcPr")
                if cell_properties is None:
                    cell_properties = ET.Element(f"{W}tcPr")
                    cell.insert(0, cell_properties)
                cell_width = cell_properties.find(f"./{W}tcW")
                if cell_width is None:
                    cell_width = ET.Element(f"{W}tcW")
                    cell_properties.insert(0, cell_width)
                cell_width.set(f"{W}type", "dxa")
                cell_width.set(f"{W}w", str(width))


def finalize_docx_structure(raw_docx: Path, output_docx: Path) -> None:
    with ZipFile(raw_docx) as archive:
        parts = {name: archive.read(name) for name in archive.namelist()}
    with ZipFile(TEMPLATE) as archive:
        template_document = ET.fromstring(archive.read("word/document.xml"))

    document = ET.fromstring(parts["word/document.xml"])
    styles = ET.fromstring(parts["word/styles.xml"])
    body = document.find(f"./{W}body")
    template_body = template_document.find(f"./{W}body")
    if body is None or template_body is None:
        raise RuntimeError("DOCX document body is missing")

    sections = template_body.findall(f".//{W}sectPr")
    if len(sections) != 3:
        raise RuntimeError("University template no longer has exactly three sections")

    strip_template_numbering(styles)
    add_pandoc_compact_style(styles)
    normalize_docx_tables(document)
    clone_style(styles, "Caption", "TableCaption", "Table Caption")
    clone_style(styles, "Caption", "FigureCaption", "Figure Caption")
    clone_style(
        styles, "Heading1", "CoverHeading1", "Cover Heading 1", remove_outline=True
    )
    clone_style(
        styles, "Heading2", "CoverHeading2", "Cover Heading 2", remove_outline=True
    )

    native_toc = next((child for child in list(body) if child.tag == f"{W}sdt"), None)
    if native_toc is None:
        raise RuntimeError("Pandoc did not create a native table of contents")
    body.remove(native_toc)
    replace_between(
        body,
        "TABLE OF CONTENTS",
        "ACKNOWLEDGEMENTS",
        [token_paragraph("", page_break=True), native_toc],
        include_start=True,
    )
    replace_between(
        body,
        "LIST OF TABLES",
        "LIST OF FIGURES",
        [token_paragraph(TABLE_INDEX_TOKEN)],
        include_start=False,
    )
    replace_between(
        body,
        "LIST OF FIGURES",
        "ABSTRACT",
        [token_paragraph(FIGURE_INDEX_TOKEN)],
        include_start=False,
    )

    cover_styles = {
        "UNIVERSITY OF SCIENCE AND TECHNOLOGY OF HANOI": "CoverHeading1",
        "DEPARTMENT OF INFORMATION AND COMMUNICATION TECHNOLOGY": "CoverHeading2",
        "MASTER THESIS": "CoverHeading1",
        "Trade-off Analysis and Optimization of a Hybrid Lakehouse Architecture Using Cloud Object Storage and Metadata Catalogs": "CoverHeading2",
        "SUPERVISOR CERTIFICATION": "CoverHeading1",
    }
    page_break_headings = {
        "SUPERVISOR CERTIFICATION",
        "LIST OF ABBREVIATIONS",
        "LIST OF TABLES",
        "LIST OF FIGURES",
        "ABSTRACT",
        "II. OBJECTIVES",
        "III. MATERIALS AND METHODS",
        "IV. RESULTS AND DISCUSSION",
        "V. CONCLUSION AND PERSPECTIVE",
        "REFERENCES",
        "APPENDICES",
    }
    in_research_body = False
    for element in list(body):
        text = paragraph_text(element)
        if text in cover_styles:
            set_paragraph_style(element, cover_styles[text])
        if text == "I. INTRODUCTION":
            in_research_body = True
        if in_research_body and re.match(r"^Table \d+\.", text):
            set_paragraph_style(element, "TableCaption")
        elif in_research_body and re.match(r"^Figure \d+\.", text):
            set_paragraph_style(element, "FigureCaption")
        if text in page_break_headings:
            set_page_break_before(element)

    add_section_to_previous_paragraph(body, "ACKNOWLEDGEMENTS", sections[0])
    add_section_to_previous_paragraph(body, "I. INTRODUCTION", sections[1])
    final_section = body.find(f"./{W}sectPr")
    if final_section is not None:
        body.remove(final_section)
    body.append(ET.fromstring(ET.tostring(sections[2])))

    parts["word/document.xml"] = ET.tostring(
        document, encoding="utf-8", xml_declaration=True
    )
    parts["word/styles.xml"] = ET.tostring(
        styles, encoding="utf-8", xml_declaration=True
    )
    with ZipFile(output_docx, "w", compression=ZIP_DEFLATED) as archive:
        for name, content in parts.items():
            archive.writestr(name, content)


def uno_finalize(docx: Path, pdf: Path, port: int) -> int:
    try:
        import uno
        from com.sun.star.beans import PropertyValue
    except ImportError:
        print("UNO bridge is unavailable in this Python interpreter", file=sys.stderr)
        return 1

    local_context = uno.getComponentContext()
    resolver = local_context.ServiceManager.createInstanceWithContext(
        "com.sun.star.bridge.UnoUrlResolver", local_context
    )
    remote_context = None
    for _ in range(100):
        try:
            remote_context = resolver.resolve(
                f"uno:socket,host=localhost,port={port};urp;StarOffice.ComponentContext"
            )
            break
        except Exception:
            time.sleep(0.1)
    if remote_context is None:
        print("Could not connect to the LibreOffice UNO listener", file=sys.stderr)
        return 1

    service_manager = remote_context.ServiceManager
    desktop = service_manager.createInstanceWithContext(
        "com.sun.star.frame.Desktop", remote_context
    )
    hidden = PropertyValue()
    hidden.Name = "Hidden"
    hidden.Value = True
    document = desktop.loadComponentFromURL(
        uno.systemPathToFileUrl(str(docx)), "_blank", 0, (hidden,)
    )
    if document is None:
        print("LibreOffice could not open the generated DOCX", file=sys.stderr)
        return 1

    def insert_caption_index(token: str, style_name: str) -> None:
        descriptor = document.createSearchDescriptor()
        descriptor.SearchString = token
        match = document.findFirst(descriptor)
        if match is None:
            raise RuntimeError(f"Missing DOCX index placeholder: {token}")
        index = document.createInstance("com.sun.star.text.ContentIndex")
        index.Title = ""
        index.Level = 1
        index.CreateFromOutline = False
        index.CreateFromMarks = False
        index.CreateFromLevelParagraphStyles = True
        uno.invoke(
            index.LevelParagraphStyles,
            "replaceByIndex",
            (0, uno.Any("[]string", (style_name,))),
        )
        match.Text.insertTextContent(match, index, True)

    try:
        insert_caption_index(TABLE_INDEX_TOKEN, "Table Caption")
        insert_caption_index(FIGURE_INDEX_TOKEN, "Figure Caption")
        indexes = document.getDocumentIndexes()
        for index_number in range(indexes.getCount()):
            indexes.getByIndex(index_number).update()
        document.getTextFields().refresh()
        document.store()
        pdf_filter = PropertyValue()
        pdf_filter.Name = "FilterName"
        pdf_filter.Value = "writer_pdf_Export"
        document.storeToURL(uno.systemPathToFileUrl(str(pdf)), (pdf_filter,))
    finally:
        document.close(True)
        desktop.terminate()
    return 0


def libreoffice_finalize(docx: Path, pdf: Path) -> list[str]:
    system_python = Path("/usr/bin/python3")
    if not system_python.exists():
        raise RuntimeError("System Python required for LibreOffice UNO is missing")
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        port = listener.getsockname()[1]
    with tempfile.TemporaryDirectory(prefix="thesis-lo-") as profile:
        profile_uri = Path(profile).resolve().as_uri()
        listener_command = [
            "libreoffice",
            "--headless",
            f"-env:UserInstallation={profile_uri}",
            f"--accept=socket,host=localhost,port={port};urp;StarOffice.ComponentContext",
            "--norestore",
            "--nodefault",
            "--nofirststartwizard",
        ]
        process = subprocess.Popen(
            listener_command,
            cwd=ROOT,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        command = [
            str(system_python),
            str(Path(__file__).resolve()),
            "_uno-finalize",
            "--docx",
            str(docx),
            "--pdf",
            str(pdf),
            "--port",
            str(port),
        ]
        try:
            completed = run(command)
        finally:
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                process.terminate()
                process.wait(timeout=10)
        if completed.stderr.strip():
            print(completed.stderr.strip(), file=sys.stderr)
        if not pdf.exists():
            raise RuntimeError("LibreOffice UNO export did not create the PDF")
        return listener_command


def check_submission() -> CheckResult:
    result = check_content()
    if result.errors:
        return result
    markdown = SOURCE.read_text(encoding="utf-8")
    placeholders = re.findall(r"<TODO:[^>]+>", markdown)
    if placeholders:
        result.error(
            f"Submission gate has {len(placeholders)} unresolved TODO placeholders"
        )
    instructional = (
        "Do not retain this instruction",
        "Required updates before submission",
        "Draft status",
    )
    for phrase in instructional:
        if phrase in markdown:
            result.error(
                f"Submission text still contains instructional/draft content: {phrase}"
            )
    for artifact in (FINAL_DOCX, FINAL_PDF):
        if not artifact.exists():
            result.error(
                f"Missing final submission artifact: {artifact.relative_to(ROOT)}"
            )
    if not MANIFEST.exists():
        result.error("Missing build manifest")
    else:
        manifest = json.loads(MANIFEST.read_text(encoding="utf-8"))
        sources = manifest.get("sources", {})
        for path in (SOURCE, TEMPLATE, BIBLIOGRAPHY, CSL, FIGURE):
            relative = str(path.relative_to(ROOT))
            if sources.get(relative) != sha256(path):
                result.error(f"Build manifest hash is missing or stale: {relative}")
    if not VISUAL_REVIEW.exists():
        result.error("Missing explicit PDF visual-review record")
    else:
        review = json.loads(VISUAL_REVIEW.read_text(encoding="utf-8"))
        if review.get("approved") is not True:
            result.error("PDF visual review has not been explicitly approved")
        required_areas = {
            "cover",
            "certification",
            "front matter",
            "chapter starts",
            "tables",
            "figures",
            "references",
            "appendices",
        }
        reviewed = set(review.get("reviewed_areas", []))
        missing_areas = required_areas - reviewed
        if missing_areas:
            result.error(f"Visual review omits: {', '.join(sorted(missing_areas))}")
    return result


def print_result(gate: str, result: CheckResult) -> int:
    print(f"Thesis {gate} gate")
    for fact in result.facts:
        print(f"  PASS  {fact}")
    for warning in result.warnings:
        print(f"  WARN  {warning}")
    for error in result.errors:
        print(f"  FAIL  {error}")
    if result.errors:
        print(f"Gate result: FAILED ({len(result.errors)} error(s))")
        return 1
    print("Gate result: PASSED")
    return 0


def build() -> int:
    content = check_content()
    status = print_result("content", content)
    if status:
        return status
    versions = tool_versions()
    pandoc_version = versions["pandoc"].removeprefix("pandoc ")
    libreoffice_version = versions["libreoffice"].removeprefix("LibreOffice ")
    if pandoc_version != EXPECTED_PANDOC:
        print(
            f"Build requires Pandoc {EXPECTED_PANDOC}; found {pandoc_version}",
            file=sys.stderr,
        )
        return 1
    if not libreoffice_version.startswith(EXPECTED_LIBREOFFICE_PREFIX):
        print(
            f"Build requires LibreOffice {EXPECTED_LIBREOFFICE_PREFIX}.x; "
            f"found {libreoffice_version}",
            file=sys.stderr,
        )
        return 1

    BUILD_DIR.mkdir(parents=True, exist_ok=True)
    for stale_artifact in (DERIVED_FIGURE, GENERATED_DOCX, GENERATED_PDF):
        stale_artifact.unlink(missing_ok=True)
    raster_command = [
        "convert",
        "-background",
        "white",
        "-density",
        "144",
        str(FIGURE),
        "-resize",
        "1600x827",
        str(DERIVED_FIGURE),
    ]
    run(raster_command)
    render_source = SOURCE.read_text(encoding="utf-8").replace(
        "thesis/architecture.svg", str(DERIVED_FIGURE)
    )
    # Markdown thematic breaks are source separators, not pagination commands.
    # The university reference document maps them to hard page breaks, so remove
    # them only from the derived render source and add controlled breaks below.
    render_source = re.sub(r"(?m)^---\s*$", "", render_source)
    temporary_source = BUILD_DIR / ".master_thesis.render.md"
    temporary_source.write_text(render_source, encoding="utf-8")
    with tempfile.TemporaryDirectory(prefix="thesis-docx-") as temporary_directory:
        raw_docx = Path(temporary_directory) / "pandoc.docx"
        command = [
            "pandoc",
            str(temporary_source),
            "--from=markdown",
            "--to=docx",
            "--standalone",
            "--toc",
            "--toc-depth=3",
            "--citeproc",
            f"--bibliography={BIBLIOGRAPHY}",
            f"--csl={CSL}",
            f"--resource-path={SOURCE.parent}",
            f"--reference-doc={TEMPLATE}",
            "--metadata=link-citations:true",
            f"--output={raw_docx}",
        ]
        try:
            completed = run(command)
            finalize_docx_structure(raw_docx, GENERATED_DOCX)
        finally:
            temporary_source.unlink(missing_ok=True)
    if completed.stderr.strip():
        print(completed.stderr.strip(), file=sys.stderr)
    libreoffice_command = libreoffice_finalize(GENERATED_DOCX, GENERATED_PDF)
    if not GENERATED_PDF.exists():
        writer_module = Path("/usr/lib/libreoffice/program/libswdlo.so")
        if not writer_module.exists():
            print(
                "LibreOffice is present but its Writer module is missing; "
                "install the matching libreoffice-writer package and rerun build.",
                file=sys.stderr,
            )
        else:
            print("LibreOffice did not produce the expected PDF", file=sys.stderr)
        inputs = (SOURCE, TEMPLATE, BIBLIOGRAPHY, CSL, FIGURE)
        partial_manifest = {
            "schema_version": 1,
            "built_at_utc": datetime.now(timezone.utc).isoformat(),
            "canonical_source": str(SOURCE.relative_to(ROOT)),
            "sources": {str(path.relative_to(ROOT)): sha256(path) for path in inputs},
            "tools": versions,
            "commands": {
                "rasterize_figure": raster_command,
                "pandoc": [
                    str(SOURCE) if item == str(temporary_source) else item
                    for item in command
                ],
            },
            "artifacts": {
                str(GENERATED_DOCX.relative_to(ROOT)): sha256(GENERATED_DOCX),
            },
            "gate_status": {
                "content": "passed",
                "build": "blocked: LibreOffice Writer module missing",
                "submission": "not attempted",
            },
        }
        MANIFEST.write_text(
            json.dumps(partial_manifest, indent=2) + "\n", encoding="utf-8"
        )
        print(f"Wrote partial {MANIFEST.relative_to(ROOT)}")
        return 1

    inputs = (SOURCE, TEMPLATE, BIBLIOGRAPHY, CSL, FIGURE)
    manifest = {
        "schema_version": 1,
        "built_at_utc": datetime.now(timezone.utc).isoformat(),
        "canonical_source": str(SOURCE.relative_to(ROOT)),
        "sources": {str(path.relative_to(ROOT)): sha256(path) for path in inputs},
        "tools": versions,
        "commands": {
            "rasterize_figure": raster_command,
            "pandoc": [
                str(SOURCE) if item == str(temporary_source) else item
                for item in command
            ],
            "libreoffice": [
                "<LibreOffice UNO listener and system Python field refresh>",
                *libreoffice_command,
            ],
        },
        "artifacts": {
            str(GENERATED_DOCX.relative_to(ROOT)): sha256(GENERATED_DOCX),
            str(GENERATED_PDF.relative_to(ROOT)): sha256(GENERATED_PDF),
        },
        "gate_status": {
            "content": "passed",
            "submission": "requires personal fields, final template pass, and explicit visual approval",
        },
    }
    MANIFEST.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    print(f"Built {GENERATED_DOCX.relative_to(ROOT)}")
    print(f"Built {GENERATED_PDF.relative_to(ROOT)}")
    print(f"Wrote {MANIFEST.relative_to(ROOT)}")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    check_parser = subparsers.add_parser("check", help="validate a thesis gate")
    check_parser.add_argument(
        "--gate", required=True, choices=("content", "submission")
    )
    subparsers.add_parser("build", help="build generated DOCX and PDF artifacts")
    uno_parser = subparsers.add_parser("_uno-finalize", help=argparse.SUPPRESS)
    uno_parser.add_argument("--docx", required=True, type=Path)
    uno_parser.add_argument("--pdf", required=True, type=Path)
    uno_parser.add_argument("--port", required=True, type=int)
    args = parser.parse_args()
    if args.command == "build":
        return build()
    if args.command == "_uno-finalize":
        return uno_finalize(args.docx, args.pdf, args.port)
    result = check_content() if args.gate == "content" else check_submission()
    return print_result(args.gate, result)


if __name__ == "__main__":
    raise SystemExit(main())
