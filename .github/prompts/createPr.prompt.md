---
name: createPr
description: Describe when to use this prompt
---
#microsoft/azure-devops-mcp create a PR for this branch. The project is mssql-rs, the repository is mssql-rs and target branch is the `development` branch. 
If the MCP server is not available, then try to use Az CLI. 

Do not use unicode characters or superlatives in the PR description.

The PR description should be created using the changes from the current branch and the target branch. The changes should be listed in a bullet point format.

Do not list metrics like lines of code changed or number of files changed. Instead, focus on the functional changes and improvements.