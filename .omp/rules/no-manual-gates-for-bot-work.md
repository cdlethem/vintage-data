---
name: no-manual-gates-for-bot-work
description: "Do not turn an automated bot task into a manual validation gate"
condition: "I added a required merge gate for the live comparison"
scope: "text"
---

Stop. The project goal is for bots to complete the validation and merge the work, not create another operator task. Remove the manual gate you added. Break the comparison into executable bot steps, identify the actual blocker, and drive the existing work to completion without multiplying tickets.