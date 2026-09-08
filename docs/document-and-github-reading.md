# Reading files and GitHub repositories

In Telegram, attach a file and add a caption such as “Explain this” or “Check the totals.” Ray reads it immediately. Without a caption, Ray gives a short summary. Follow-up questions can use the recent file text in the same selected project. `/forget` clears recent chat and reference text; existing engineering task records remain.

Paste a public GitHub repository link and say what to check. Ray reads the default branch at a recorded commit, lists the files, and inspects a sample of README, configuration and source files. It can request more files from that same listing for a closer look. File and folder links are also supported. This does not execute repository code, install dependencies, run tests or modify GitHub.

| Input | What Ray reads | Limits |
|---|---|---|
| MD, TXT, CSV | Text | UTF-8 or BOM-marked UTF-16 |
| DOCX | Paragraphs and table text, plus headers, footers and notes | Pictures, text boxes and page layout are not inspected |
| PDF | Embedded text, identified by page | No OCR; scanned pages need a text-recognized copy. Password-protected PDFs need an unlocked copy |
| XLSX | Cell values and formula text, identified by sheet and cell | Does not calculate formulas or inspect charts/pictures; includes hidden sheets and says so |
| XLS | Saved cell values, identified by sheet and cell | Formula expressions are not available through this reader |
| Public GitHub | File listing and selected source at one commit | Anonymous GitHub request limits apply; private repositories, issue pages and pull-request pages are not supported |

Telegram files can be up to 20 MB. Each document has an 80,000-character extraction budget, at most 200 PDF pages, 40 sheets, 5,000 rows and 100 columns per sheet. Truncation and unreadable sections are reported explicitly. DOCX/XLSX archives also have decompression limits. Parsing runs in a separate process with a 30-second deadline.

GitHub accepts up to two pasted links per turn. A read returns up to 3,000 file entries and initially reads up to ten files, each up to 500 KB, with 12,000 characters per file. Two additional rounds can read up to six named files each. Large trees and omitted source are marked. GitHub links are requested through fixed HTTPS API endpoints; redirects, symlink contents and external links in files are not followed. No GitHub token is needed or requested in chat.

Only extracted, redacted reference text is saved in Ray's separate state database. Raw Telegram attachments are held temporarily in memory and are not copied into the project repository. Stored references are scoped to the Telegram actor, selected project and policy binding, with bounded retention of recent items. They are passed as untrusted reference material, separately from the user's direct request. File contents cannot grant approval, enable writes or change policy.

Repository-local DOCX, PDF, XLSX and XLS files are also available as bounded source evidence during engineering tasks. The terminal supports `ray --project <config> read-file <path>` and `ray --project <config> github <public-repository-url>`; these save reference text for subsequent local tasks and print what was read.

The implementation uses [Telegram getFile](https://core.telegram.org/bots/api#getfile), [GitHub Git trees](https://docs.github.com/en/rest/git/trees), [GitHub Git blobs](https://docs.github.com/en/rest/git/blobs), [pypdf](https://pypdf.readthedocs.io/en/stable/user/extract-text.html), openpyxl, xlrd and defusedxml. Runtime dependencies are pinned in `pyproject.toml` and the Windows lock file.

## Verification — 6 September 2026

- Full suite: `.venv/Scripts/python.exe -m pytest tests -q` — **328 passed in 147.92 seconds**. Package dependency checks and `git diff --check` also passed.
- Tests cover real DOCX, PDF, XLSX and XLS parsing, string identifiers, formulas, hidden sheets, protected/damaged files, explicit reading limits, unsafe archives, credentials, actor/project separation, follow-up reads, forgetting, Telegram authorization and downloads, GitHub URL boundaries, pinned source reads, and interrupted-task recovery. A regression confirms that an inspected folder's Python package cannot replace Ray's document worker.
- A real Codex conversation read the generated XLS fixture and answered with customer ID **0012** and value **25**, without offering or starting engineering work.
- A real GitHub API read inspected `octocat/Hello-World` at commit `7fd1a60b01f91b314f59955a4e4d4e80d8edf11d`. A real Codex conversation then returned the actual README text, **Hello World!**, without executing repository code.
- The idle private Telegram gateway was reloaded and fresh successful polling was observed. Telegram attachment download/routing was tested locally with controlled API responses; no live attachment-delivery test or unsolicited test message was sent.

The earlier Silver task's saved host receipts showed successful notebook creation, pipeline update and pipeline execution. Its final task state was completed. The source-validation repair now checks SourceFile definitions in memory, and status reporting keeps a task in progress while Fabric work remains pending. Those fixes do not establish untested concurrency or partial-failure recovery of the Silver Spark notebook itself.
