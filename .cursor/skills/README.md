# Cursor Skills

This directory contains reusable Cursor skills for this project. Skills are markdown files that define reusable prompts and workflows that can be invoked in Cursor.

## Best Practices for Cursor Skills

### 1. **Single Responsibility**
- Each skill should focus on one specific task or workflow
- Keep skills focused and avoid mixing multiple unrelated concerns
- If a skill becomes too complex, break it into smaller, composable skills

### 2. **Clear Naming**
- Use descriptive, action-oriented names (e.g., `add-timeout-to-script.md`, `update-changelog.md`)
- Use kebab-case for file names (e.g., `check-worker-status.md`)
- Avoid generic names like `helper.md` or `utils.md`

### 3. **Structured Format**
- Start with a clear title and description
- Include context about when to use the skill
- Provide clear, step-by-step instructions
- Include examples where helpful
- Specify any prerequisites or requirements

### 4. **Context-Aware**
- Reference project-specific patterns and conventions
- Include relevant file paths and directory structures
- Reference existing code patterns in the project
- Consider the project's architecture and conventions

### 5. **Reusable Parameters**
- Use placeholders like `{{variable}}` for dynamic values when appropriate
- Make it clear what needs to be customized
- Provide examples with actual values

### 6. **Error Handling**
- Include guidance on error handling
- Mention common pitfalls and how to avoid them
- Include validation steps

### 7. **Documentation**
- Each skill should be self-documenting
- Include usage examples
- Mention related skills or dependencies

### 8. **Project-Specific Rules**
- Skills should follow the project's `.cursorrules` conventions
- Reference project-specific patterns (e.g., timeout requirements, changelog format)
- Align with the project's coding standards

## Skill File Structure

```markdown
# Skill Name

## Description
Brief description of what this skill does and when to use it.

## Context
When and why you would use this skill.

## Steps
1. Step one
2. Step two
...

## Example
Example usage or output.

## Notes
Additional considerations, warnings, or tips.
```

## Usage

In Cursor, you can invoke skills by referencing them in your prompts or using the skills feature. Skills help maintain consistency and speed up common workflows.
