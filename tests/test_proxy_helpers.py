import json
import unittest

from cpa_protocol_proxy import (
    build_bootstrap_chunk,
    first_sse_frame,
    openai_frame_has_visible_output,
    openai_frame_is_terminal,
    sse_data,
)


def sse(payload):
    if isinstance(payload, str):
        return f"data: {payload}\n\n".encode()
    return f"data: {json.dumps(payload, separators=(',', ':'))}\n\n".encode()


class SseHelperTests(unittest.TestCase):
    def test_first_sse_frame_handles_lf_and_preserves_rest(self):
        frame, rest = first_sse_frame(b"data: one\n\ndata: two\n\n")

        self.assertEqual(frame, b"data: one\n\n")
        self.assertEqual(rest, b"data: two\n\n")

    def test_first_sse_frame_handles_crlf(self):
        frame, rest = first_sse_frame(b"data: one\r\n\r\nrest")

        self.assertEqual(frame, b"data: one\r\n\r\n")
        self.assertEqual(rest, b"rest")

    def test_sse_data_joins_multiline_data_fields(self):
        self.assertEqual(sse_data(b"event: chunk\ndata: hello\ndata: world\n\n"), "hello\nworld")


class OpenAIStreamDetectionTests(unittest.TestCase):
    def test_detects_text_content_as_visible_output(self):
        frame = sse({"choices": [{"delta": {"content": "ok"}}]})

        self.assertTrue(openai_frame_has_visible_output(frame))

    def test_detects_tool_calls_as_visible_output(self):
        frame = sse({"choices": [{"delta": {"tool_calls": [{"index": 0}]}}]})

        self.assertTrue(openai_frame_has_visible_output(frame))

    def test_detects_refusal_and_audio_as_visible_output(self):
        fields = ("refusal", "audio")
        for field in fields:
            with self.subTest(field=field):
                frame = sse({"choices": [{"delta": {field: "visible"}}]})
                self.assertTrue(openai_frame_has_visible_output(frame))

    def test_reasoning_only_is_not_user_visible_by_default(self):
        for field in ("reasoning_content", "reasoning"):
            with self.subTest(field=field):
                frame = sse({"choices": [{"delta": {field: "hidden"}}]})
                self.assertFalse(openai_frame_has_visible_output(frame))
                self.assertTrue(openai_frame_has_visible_output(frame, treat_reasoning_as_output=True))

    def test_role_only_chunk_is_not_visible_output(self):
        frame = sse({"choices": [{"delta": {"role": "assistant"}}]})

        self.assertFalse(openai_frame_has_visible_output(frame))

    def test_done_and_finish_reason_are_terminal(self):
        self.assertTrue(openai_frame_is_terminal(sse("[DONE]")))
        self.assertTrue(openai_frame_is_terminal(sse({"choices": [{"finish_reason": "stop"}]})))

    def test_usage_only_chunk_is_not_terminal_without_done(self):
        frame = sse({"choices": [], "usage": {"completion_tokens": 0}})

        self.assertFalse(openai_frame_is_terminal(frame))


class BootstrapChunkTests(unittest.TestCase):
    def test_bootstrap_chunk_uses_upstream_metadata_and_choice_indexes(self):
        request_body = json.dumps({"model": "claude-test"}).encode()
        first_frame = sse({
            "id": "chatcmpl-upstream",
            "object": "chat.completion.chunk",
            "created": 123,
            "model": "upstream-model",
            "choices": [{"index": 3, "delta": {"content": "ok"}}],
        })

        bootstrap = build_bootstrap_chunk(request_body, first_frame)
        payload = json.loads(sse_data(bootstrap))

        self.assertEqual(payload["id"], "chatcmpl-upstream")
        self.assertEqual(payload["created"], 123)
        self.assertEqual(payload["model"], "upstream-model")
        self.assertEqual(payload["choices"], [
            {"index": 3, "delta": {"role": "assistant"}, "finish_reason": None}
        ])

    def test_bootstrap_chunk_skips_done_frame(self):
        self.assertIsNone(build_bootstrap_chunk(b'{"model":"x"}', sse("[DONE]")))


if __name__ == "__main__":
    unittest.main()
