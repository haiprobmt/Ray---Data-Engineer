from ray_de.telegram_format import render, formatted_chunks, units, task_message


def test_entities_use_utf16_offsets_and_keep_untrusted_markup_literal():
    text, spans = render('✅ **Passed**\n`a < b`\n<b>literal</b>\n```sql\nSELECT ** FROM x\n```')
    assert text == '✅ Passed\na < b\n<b>literal</b>\nSELECT ** FROM x\n'
    assert spans[0] == {"type": "bold", "offset": 2, "length": 6}
    assert [span["type"] for span in spans] == ["bold", "code", "pre"]
    _, emoji = render('😀 **ok**')
    assert emoji[0]["offset"] == 3


def test_long_formatted_messages_keep_all_text_and_valid_spans():
    source = "😀 ```\n" + "long sentence 😀\n" * 700 + "```"
    plain, _ = render(source)
    pieces = list(formatted_chunks(source))
    assert len(pieces) > 1
    assert "".join(text for text, _ in pieces) == plain
    for text, spans in pieces:
        assert units(text) <= 3500
        assert all(0 <= e["offset"] < e["offset"] + e["length"] <= units(text) for e in spans)


def test_summary_preserves_failed_actions_and_current_error():
    report = {"status": "blocked", "model_message": "Old success", "message": "Readback failed.",
              "cloud_plans": [{"state": "UNCERTAIN"}], "host_validation": ["Validation 1: exit 0"]}
    message = task_message(report)
    assert "Blocked" in message and "Readback failed" in message and "needs verification" in message
    assert "Old success" not in message and "Validation 1" not in message
    assert "/details" in message


def test_details_explains_review_block_before_background_evidence():
    from ray_de.telegram_format import task_details
    report = {"review": {"verdict": "REWORK", "summary": "Fix before deployment.",
                          "findings": ["[P1] Preserve partition columns.", "[P1] Sanitize parser errors."]},
              "evidence": ["Long source inventory"]}
    text = task_details("project", {"id": "task", "status": "BLOCKED"}, report, technical=True)
    assert "Preserve partition columns" in text and "Sanitize parser errors" in text
    assert text.index("Review") < text.index("Evidence")


def test_default_details_explains_current_work_without_dumping_previous_review():
    from ray_de.telegram_format import task_details
    report = {"phase": "authoring", "continue_work": True,
              "review": {"verdict": "PASS_WITH_COMMENTS", "summary": "Persisted multiset comparisons.",
                         "findings": ["Immutable batch snapshots; publication-last semantics."]},
              "evidence": ["119 fixture dispositions", "Validation 1: exit 0"],
              "cloud_plans": [{"id": "receipt", "state": "SUCCEEDED", "operation": "create_item",
                               "item_type": "Notebook", "remote": {"kind": "created_item", "id": "notebook-id"}}]}
    text = task_details("long-project-id", {"id": "long-task-id", "status": "WORKING"}, report)
    assert "working" in text.lower() and "Created a notebook" in text
    assert "/details technical" in text
    assert all(value not in text for value in ("multiset", "publication-last", "dispositions", "long-task-id", "PASS_WITH_COMMENTS", "exit 0"))
    assert len(text.split()) <= 150


def test_plain_details_keeps_user_risks_and_distinguishes_code_checks_from_live_checks():
    from ray_de.telegram_format import task_details
    report = {"phase": "source_ready", "user_summary": "The Silver code is ready. It has not run in Fabric yet.",
              "next_step": "Add it to the pipeline, then run the pipeline.",
              "user_notes": ["Negative amounts are saved separately for review."],
              "review": {"verdict": "PASS_WITH_COMMENTS", "user_summary": "The code passed the checks using sample data.",
                         "user_notes": ["Reports must use one completed batch at a time."]}}
    text = task_details("project", {"id": "task", "status": "WAITING"}, report)
    assert "has not run in Fabric" in text and "Negative amounts" in text
    assert "one completed batch" in text and "Add it to the pipeline" in text
    assert "Done" not in text


def test_running_job_overrides_stale_saved_completion_claim():
    from ray_de.telegram_format import task_details
    report = {"message": "Everything finished", "phase": "finished",
              "cloud_plans": [{"id": "job-plan", "state": "EXECUTING", "operation": "run_job", "item_type": "DataPipeline"}]}
    text = task_details("project", {"id": "task", "status": "COMPLETED"}, report)
    assert "pipeline is running" in text.lower()
    assert "Everything finished" not in text and "Done" not in text
