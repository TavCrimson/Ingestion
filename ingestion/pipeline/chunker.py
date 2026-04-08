"""Heading-aware sliding-window text chunker."""
from __future__ import annotations

import re
from dataclasses import dataclass

import tiktoken

_HEADING_RE = re.compile(r"^(#{1,6})\s+(.+)$", re.MULTILINE)
_UNDERLINE_HEADING_RE = re.compile(r"^(.+)\n[=\-]{3,}\s*$", re.MULTILINE)

DEFAULT_CHUNK_TOKENS = 512
DEFAULT_OVERLAP_TOKENS = 50


@dataclass
class TextChunk:
    text: str
    chunk_index: int
    char_start: int
    char_end: int
    heading_context: str


def _get_encoding():
    try:
        return tiktoken.get_encoding("cl100k_base")
    except Exception:
        return None


def _token_count(text: str, enc) -> int:
    if enc is None:
        return len(text) // 4
    return len(enc.encode(text))


def chunk_text(
    text: str,
    chunk_tokens: int = DEFAULT_CHUNK_TOKENS,
    overlap_tokens: int = DEFAULT_OVERLAP_TOKENS,
) -> list[TextChunk]:
    enc = _get_encoding()

    # Build a map of char_offset -> heading text
    heading_map: list[tuple[int, str]] = []
    for m in _HEADING_RE.finditer(text):
        heading_map.append((m.start(), m.group(2).strip()))
    for m in _UNDERLINE_HEADING_RE.finditer(text):
        heading_map.append((m.start(), m.group(1).strip()))
    heading_map.sort(key=lambda x: x[0])

    def current_heading(offset: int) -> str:
        h = ""
        for pos, title in heading_map:
            if pos <= offset:
                h = title
            else:
                break
        return h

    # Split into sentences/paragraphs first to avoid breaking mid-sentence
    paragraphs = re.split(r"\n{2,}", text)

    chunks: list[TextChunk] = []
    current_parts: list[str] = []
    current_tokens = 0
    current_char_start = 0
    char_offset = 0
    chunk_index = 0

    for para in paragraphs:
        para_tokens = _token_count(para, enc)
        # If a single paragraph exceeds chunk size, split by sentence
        if para_tokens > chunk_tokens:
            sentences = re.split(r"(?<=[.!?])\s+", para)
            for sent in sentences:
                sent_tokens = _token_count(sent, enc)
                if current_tokens + sent_tokens > chunk_tokens and current_parts:
                    chunk_text_str = "\n\n".join(current_parts)
                    char_end = char_offset
                    chunks.append(TextChunk(
                        text=chunk_text_str,
                        chunk_index=chunk_index,
                        char_start=current_char_start,
                        char_end=char_end,
                        heading_context=current_heading(current_char_start),
                    ))
                    chunk_index += 1
                    # Overlap: keep last overlap_tokens worth of parts
                    overlap_text = _trim_to_tokens(current_parts, overlap_tokens, enc)
                    current_parts = [overlap_text] if overlap_text else []
                    current_tokens = _token_count(overlap_text, enc) if overlap_text else 0
                    current_char_start = char_end
                current_parts.append(sent)
                current_tokens += sent_tokens
                char_offset += len(sent) + 1
        else:
            if current_tokens + para_tokens > chunk_tokens and current_parts:
                chunk_text_str = "\n\n".join(current_parts)
                char_end = char_offset
                chunks.append(TextChunk(
                    text=chunk_text_str,
                    chunk_index=chunk_index,
                    char_start=current_char_start,
                    char_end=char_end,
                    heading_context=current_heading(current_char_start),
                ))
                chunk_index += 1
                overlap_text = _trim_to_tokens(current_parts, overlap_tokens, enc)
                current_parts = [overlap_text] if overlap_text else []
                current_tokens = _token_count(overlap_text, enc) if overlap_text else 0
                current_char_start = char_end
            current_parts.append(para)
            current_tokens += para_tokens
            char_offset += len(para) + 2  # account for paragraph break

    # Flush remaining
    if current_parts:
        chunk_text_str = "\n\n".join(current_parts)
        chunks.append(TextChunk(
            text=chunk_text_str,
            chunk_index=chunk_index,
            char_start=current_char_start,
            char_end=len(text),
            heading_context=current_heading(current_char_start),
        ))

    return chunks


def _trim_to_tokens(parts: list[str], max_tokens: int, enc) -> str:
    """Return the tail of parts that fits within max_tokens."""
    combined = "\n\n".join(parts)
    if _token_count(combined, enc) <= max_tokens:
        return combined
    # Take from the end
    words = combined.split()
    while words and _token_count(" ".join(words), enc) > max_tokens:
        words = words[len(words) // 2:]
    return " ".join(words)
