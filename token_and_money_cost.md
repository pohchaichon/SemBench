# Token Structure and Cost Calculation for LiteLLM Models

## Models Tested
- `vertex_ai/gemini-2.5-pro`
- `gpt-5-mini`

## Token Structure

### Prompt Tokens
```json
"prompt_tokens_details": {
  "text_tokens": 11,        // Input text tokens
  "audio_tokens": null,     // Audio input tokens (Gemini only)
  "cached_tokens": 0,       // Previously cached tokens
  "image_tokens": null      // Image input tokens
}
```

### Completion Tokens
```json
"completion_tokens_details": {
  "reasoning_tokens": 873,  // Internal thinking/CoT tokens
  "text_tokens": 77,       // Final output text tokens
  "audio_tokens": null     // Audio output tokens
}
```

## Key Logic

### Token Calculation Formula
```
Total Cost = (prompt_tokens × prompt_rate) + (completion_tokens × completion_rate)

Where:
completion_tokens = reasoning_tokens + text_tokens + audio_tokens
```

### Model Differences

**Gemini-2.5-pro:**
- Explicit separation: `reasoning_tokens` (873) + `text_tokens` (77) = `completion_tokens` (950)
- Audio tokens tracked separately when present
- Perfect token accounting

**GPT-5-mini:**
- `text_tokens: null` (not tracked separately)
- `completion_tokens` (216) includes both reasoning (128) and text tokens
- Unaccounted tokens = actual text output tokens (216 - 128 = 88)

### Important Notes

1. **Reasoning Token Billing**: Reasoning tokens are **included** in completion_tokens, not billed separately
2. **Audio Token Limitations**: 
   - LiteLLM with `vertex_ai/gemini-2.5-pro` shows `audio_tokens: null`
   - Need native Google SDK + audio-capable model (e.g., `gemini-2.5-flash`) to see audio tokens
3. **Cost Efficiency**: GPT-5-mini ≈ 5x cheaper than Gemini-2.5-pro

## Original Response Examples

### vertex_ai/gemini-2.5-pro Response
```json
{
  "choices": [{
    "message": {
      "content": "Hello!\n\n2 + 2 equals **4**.\n\nIn mathematics, addition is the process of combining quantities..."
    },
    "finish_reason": "stop"
  }],
  "usage": {
    "completion_tokens": 950,
    "prompt_tokens": 11,
    "total_tokens": 961,
    "completion_tokens_details": {
      "reasoning_tokens": 873,
      "text_tokens": 77,
      "audio_tokens": null
    },
    "prompt_tokens_details": {
      "text_tokens": 11,
      "audio_tokens": null,
      "cached_tokens": null,
      "image_tokens": null
    }
  }
}
```

### gpt-5-mini Response
```json
{
  "choices": [{
    "message": {
      "content": "Hello! \n\n2 + 2 equals 4. \n\nExplanation: if you start with two items..."
    },
    "finish_reason": "stop"
  }],
  "usage": {
    "completion_tokens": 216,
    "prompt_tokens": 17,
    "total_tokens": 233,
    "completion_tokens_details": {
      "reasoning_tokens": 128,
      "text_tokens": null,
      "audio_tokens": 0
    },
    "prompt_tokens_details": {
      "text_tokens": null,
      "audio_tokens": 0,
      "cached_tokens": 0,
      "image_tokens": null
    }
  }
}
```

## Audio Token Access (Native SDK Required)

```python
from google import genai
from google.genai.types import HttpOptions, Part

client = genai.Client(http_options=HttpOptions(api_version="v1"))
response = client.models.generate_content(
    model="gemini-2.5-flash",  # Audio-capable model
    contents=[
        "Summarize this audio:",
        Part.from_uri(
            file_uri="gs://path/to/audio.mp3",
            mime_type="audio/mpeg"
        )
    ]
)

# Expected usage_metadata with audio:
# {
#   "prompt_token_count": 15,
#   "candidates_token_count": 50,
#   "total_token_count": 65,
#   "prompt_tokens_details": {
#     "audio_tokens": 1250,  # Audio tokens here!
#     "text_tokens": 15
#   }
# }
```

## Summary

- **Reasoning tokens** are billed as part of completion tokens
- **Audio tokens** require native SDK and audio-capable models
- **Text tokens** may be tracked separately (Gemini) or bundled (GPT-5-mini)
- **Cost calculation** is consistent: all tokens billed at respective rates