# Grade a maintenance bot's work

A bot that maintains a data pipeline was given the task below. Grade what it did against the rubric.

## The task it was given (may be truncated)

````text
{{task}}
````

## Its answer

```json
{{output}}
```

## Its code changes

```diff
{{diff}}
```

## Rubric

{{rubric}}

## How to grade

Check each rubric item separately. Judge only what the answer and the diff actually show: a claim in the answer is not evidence that the code does it, and a check the bot says it ran is not proof unless the details show its outcome. Be strict and specific.

End with one JSON object and nothing after it:

```json
{"criteria": [{"criterion": "<rubric item, shortened>", "met": true, "note": "<one sentence why>"}], "summary": "<one or two sentences>"}
```
