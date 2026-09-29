## How to answer

End your final message with one JSON object (a ```json block is fine) and nothing after it:

```json
{
  "action": "fix | wait | ask | none",
  "title": "PokéAPI extract fails: pagination links changed",
  "summary": "2-4 plain sentences",
  "details": "markdown for a reviewer",
  "question": "",
  "review": true,
  "duplicate_of": null
}
```

- **action**
  - `fix`: you changed files in this checkout and the change solves the problem. The bot commits your changes and opens a pull request. Do not commit or push yourself.
  - `wait`: nothing in this repository should change (for example the upstream service is down or rate-limiting). The issue closes by itself once the task succeeds again.
  - `ask`: a person has to decide or act (credentials, paid access, a judgement call, or a fix you are not confident in). Put the decision in `question`.
  - `none`: there is nothing worth reporting.
- **title**: under 80 characters. Name the affected thing and the problem in everyday words. No run IDs, hashes, or internal jargon.
- **summary**: written for someone seeing this for the first time: what is broken, what it affects, the cause (say "likely" when unsure), and what you did about it.
- **details**: the evidence (quote the key log lines), what you changed and why, and the commands you actually ran to check it with their outcome. Never claim a check passed unless you ran it.
- **question**: only for `ask`: one concrete question, with the answer you recommend.
- **review**: for `fix`, `false` only when the change is small, low-risk and verified; otherwise `true` so a person looks before it merges.
- **duplicate_of**: the number of an open issue listed above that already covers the same root cause, otherwise `null`.
