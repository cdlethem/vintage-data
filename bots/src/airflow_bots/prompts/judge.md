# Grade a maintenance bot's work

A bot that maintains a data pipeline was given the task below. Grade what it did against the rubric. You have no tools and no repository: everything you may use is in this message. Do not try to look anything up; answer straight away with the verdict described at the end.

## The task it was given (may be truncated)

````text
{{task}}
````

## What a person sees (the pull request or issue the bot would publish)

````markdown
{{seen}}
````

## Its structured answer

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

Check each rubric item separately. Judge only what the published text, the answer and the diff actually show: a claim is not evidence that the code does it, and a check the bot says it ran is not proof unless the details show its outcome. Judge the published text as the reviewer would read it. Be strict and specific.

End with one JSON object and nothing after it:

```json
{"criteria": [{"criterion": "<rubric item, shortened>", "met": true, "note": "<one sentence why>"}], "summary": "<one or two sentences>"}
```
