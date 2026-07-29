#!/usr/bin/env python3
"""Build and validate the canonical Markdown master thesis."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import subprocess
import sys
import tempfile
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from zipfile import ZipFile

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
    temporary_source = BUILD_DIR / ".master_thesis.render.md"
    temporary_source.write_text(render_source, encoding="utf-8")
    command = [
        "pandoc",
        str(temporary_source),
        "--from=markdown",
        "--to=docx",
        "--standalone",
        "--citeproc",
        f"--bibliography={BIBLIOGRAPHY}",
        f"--csl={CSL}",
        f"--resource-path={SOURCE.parent}",
        f"--reference-doc={TEMPLATE}",
        "--metadata=link-citations:true",
        f"--output={GENERATED_DOCX}",
    ]
    try:
        completed = run(command)
    finally:
        temporary_source.unlink(missing_ok=True)
    if completed.stderr.strip():
        print(completed.stderr.strip(), file=sys.stderr)

    with tempfile.TemporaryDirectory(prefix="thesis-lo-") as profile:
        profile_uri = Path(profile).resolve().as_uri()
        converted = run(
            [
                "libreoffice",
                "--headless",
                f"-env:UserInstallation={profile_uri}",
                "--convert-to",
                "pdf",
                "--outdir",
                str(BUILD_DIR),
                str(GENERATED_DOCX),
            ]
        )
        if converted.stdout.strip():
            print(converted.stdout.strip())
        if converted.stderr.strip():
            print(converted.stderr.strip(), file=sys.stderr)
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
                "libreoffice",
                "--headless",
                "-env:UserInstallation=<temporary-profile>",
                "--convert-to",
                "pdf",
                "--outdir",
                str(BUILD_DIR),
                str(GENERATED_DOCX),
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
    args = parser.parse_args()
    if args.command == "build":
        return build()
    result = check_content() if args.gate == "content" else check_submission()
    return print_result(args.gate, result)


if __name__ == "__main__":
    raise SystemExit(main())
