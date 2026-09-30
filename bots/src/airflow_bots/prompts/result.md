## How to answer

End your final message with one JSON object (a ```json block is fine) and nothing after it:

```json
{
  "action": "fix | wait | ask | none",
  "title": "PokéAPI extract fails: pagination links changed",
  "summary": "2-4 plain sentences",
  "details": "markdown for a reviewer",
  "decision": {
    "question": "Should the fetcher accept a smaller page size on the last page of results?",
    "why": "Accepting it loosens a check that guards against silently skipping records; a person should confirm that trade.",
    "routine": false,
    "options": [
      {"choice": "Accept a smaller limit on the last page only", "pros": "...", "cons": "..."},
      {"choice": "Stop paginating once the reported total is reached", "pros": "...", "cons": "..."},
      {"choice": "Leave the check as it is", "pros": "...", "cons": "..."}
    ]
  },
  "duplicate_of": null
}
```

- **action**
  - `fix`: you changed files in this checkout and the change solves the problem. The bot commits your changes and opens a pull request. Do not commit or push yourself.
  - `wait`: nothing in this repository should change (for example the upstream service is down or rate-limiting). The issue closes by itself once the task is healthy again.
  - `ask`: a person has to decide or act before anything can change (credentials, paid access, a judgement call you should not make alone).
  - `none`: there is nothing worth reporting.
- **title**: under 80 characters. Name the affected thing and the problem in everyday words. No run IDs, hashes, or internal jargon.
- **summary**: written for someone seeing this for the first time: what is broken, what it affects, the cause (say "likely" when unsure), and what you did about it.
- **details**: the evidence (quote the key log lines), what you changed and why, and the commands you actually ran to check it with their outcome. Never claim a check passed unless you ran it.
- **decision**: required for `fix` and `ask`, `null` otherwise. It is what the reviewer reads first, so make it stand on its own for someone who has not read the code.
  - `question`: the one judgement the outcome depends on, asked so that a person can answer it without reading the diff.
  - `why`: why this needs a person: which risk, behaviour change, or unknown you cannot settle yourself. For a routine fix, say why no judgement is involved.
  - `routine` (`fix` only): routine fixes merge automatically with nobody looking, so be strict. `true` only for a small, verified change with nothing to weigh, such as correcting a typo, an import, or a field name the source renamed. Changes to retries, timeouts, rate-limit or error handling, validation or parsing rules, schedules, or what data is collected are **never** routine, and neither is anything your own `why` hesitates about. When unsure, it is not routine.
  - `options`: at least two real alternatives a sensible engineer would consider, including leaving things as they are when that is viable. **List first the option your change implements (for `fix`) or the one you recommend (for `ask`)**; the bot marks the first option as the chosen one. For each, give concrete `pros` and `cons` for this pipeline (data completeness, correctness, load on the source, maintenance).
- **duplicate_of**: the number of an open issue listed above that already covers the same root cause, otherwise `null`.
