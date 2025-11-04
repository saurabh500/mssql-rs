# Converting Markdown Presentation to PowerPoint

This guide explains how to convert the fuzz-testing-presentation.md file to a PowerPoint presentation.

## Method 1: Pandoc (Recommended)

Pandoc is the most powerful and flexible tool for converting markdown to PowerPoint.

### Installation

**Linux/Ubuntu**:
```bash
sudo apt-get update
sudo apt-get install pandoc
```

**macOS**:
```bash
brew install pandoc
```

**Windows**:
Download from: https://pandoc.org/installing.html

### Basic Conversion

```bash
cd /home/saurabh/work/mssql-tds/docs

pandoc fuzz-testing-presentation.md \
  -o fuzz-testing-presentation.pptx \
  -t pptx
```

### Advanced Conversion with Custom Styling

```bash
pandoc fuzz-testing-presentation.md \
  -o fuzz-testing-presentation.pptx \
  -t pptx \
  --slide-level=2 \
  --reference-doc=template.pptx
```

**Options explained**:
- `-o`: Output file
- `-t pptx`: Target format (PowerPoint)
- `--slide-level=2`: Use `##` headers for new slides
- `--reference-doc`: Use custom PowerPoint template

### Creating a Reference Template

1. Create a blank PowerPoint with your preferred:
   - Color scheme
   - Fonts
   - Slide layouts
   - Company branding

2. Save as `template.pptx`

3. Use with pandoc:
   ```bash
   pandoc fuzz-testing-presentation.md \
     -o fuzz-testing-presentation.pptx \
     --reference-doc=template.pptx
   ```

## Method 2: Marp

Marp is designed specifically for creating presentations from markdown.

### Installation

```bash
npm install -g @marp-team/marp-cli
```

### Conversion

```bash
cd /home/saurabh/work/mssql-tds/docs

marp fuzz-testing-presentation.md \
  --pptx \
  -o fuzz-testing-presentation.pptx
```

### With Theme

```bash
marp fuzz-testing-presentation.md \
  --theme default \
  --pptx \
  -o fuzz-testing-presentation.pptx
```

**Available themes**:
- default
- gaia
- uncover

### Creating Custom Marp Theme

Add to the top of your markdown:

```markdown
---
marp: true
theme: default
paginate: true
backgroundColor: #fff
---
```

## Method 3: VS Code with Marp Extension

### Installation

1. Open VS Code
2. Install "Marp for VS Code" extension
3. Open fuzz-testing-presentation.md
4. Click "Marp" in status bar
5. Export to PPTX

### Steps

1. Install extension:
   - Press `Ctrl+Shift+X`
   - Search "Marp for VS Code"
   - Install

2. Configure (optional):
   Add to `.vscode/settings.json`:
   ```json
   {
     "markdown.marp.enableHtml": true,
     "markdown.marp.themes": ["./theme.css"]
   }
   ```

3. Export:
   - Open Command Palette (`Ctrl+Shift+P`)
   - Type "Marp: Export Slide Deck"
   - Select "PowerPoint"
   - Choose output location

## Method 4: Online Tools

### Slidev + Export

```bash
npm install -g @slidev/cli

cd /home/saurabh/work/mssql-tds/docs

slidev export fuzz-testing-presentation.md --format pptx
```

### Deckset (macOS only)

1. Purchase Deckset from Mac App Store
2. Open fuzz-testing-presentation.md in Deckset
3. Export as PowerPoint

## Recommended Workflow

For the best results with your presentation:

### Step 1: Install Pandoc

```bash
# Ubuntu/Debian
sudo apt-get install pandoc

# macOS
brew install pandoc
```

### Step 2: Convert to PowerPoint

```bash
cd /home/saurabh/work/mssql-tds/docs

pandoc fuzz-testing-presentation.md \
  -o fuzz-testing-presentation.pptx \
  -t pptx \
  --slide-level=2
```

### Step 3: Customize in PowerPoint

1. Open fuzz-testing-presentation.pptx in PowerPoint
2. Apply company theme
3. Adjust layouts as needed
4. Add images or diagrams
5. Fine-tune formatting

## Customization Tips

### Slide Breaks

The markdown uses `---` for slide breaks. Each `#` heading starts a new slide.

### Code Blocks

Code blocks in markdown convert to code blocks in PowerPoint:

```markdown
```yaml
- script: |
    cargo fuzz run
```
```

### Lists

Bullet points and numbered lists convert automatically:

```markdown
**Benefits**:
- Fast feedback
- Easy integration
- Comprehensive coverage
```

### Images

Add images to slides:

```markdown
![Architecture Diagram](./images/fuzzing-architecture.png)
```

### Tables

Tables convert to PowerPoint tables:

```markdown
| Tier | Duration | Workers |
|------|----------|---------|
| PR   | 60s      | 1       |
| Main | 30m      | 4       |
```

## Troubleshooting

### Pandoc not found

```bash
# Verify installation
pandoc --version

# If not found, reinstall
sudo apt-get install pandoc
```

### Formatting issues

1. Check slide breaks (should be `---`)
2. Verify heading levels (`#` vs `##`)
3. Ensure code blocks use triple backticks

### Code blocks not rendering

Use `--listings` flag:

```bash
pandoc fuzz-testing-presentation.md \
  -o fuzz-testing-presentation.pptx \
  --listings
```

### Custom fonts

Create reference template with desired fonts:

```bash
pandoc fuzz-testing-presentation.md \
  -o fuzz-testing-presentation.pptx \
  --reference-doc=company-template.pptx
```

## Quick Reference

### Pandoc Basic

```bash
pandoc input.md -o output.pptx -t pptx
```

### Pandoc with Template

```bash
pandoc input.md -o output.pptx --reference-doc=template.pptx
```

### Marp

```bash
marp input.md --pptx -o output.pptx
```

### VS Code

1. Install Marp extension
2. Open markdown file
3. Command Palette > "Marp: Export Slide Deck"
4. Choose PowerPoint format

## Additional Resources

- Pandoc Documentation: https://pandoc.org/MANUAL.html
- Marp Documentation: https://marp.app/
- Slidev: https://sli.dev/
- Markdown Guide: https://www.markdownguide.org/

## Example Command for Your Presentation

```bash
cd /home/saurabh/work/mssql-tds/docs

# Simple conversion
pandoc fuzz-testing-presentation.md \
  -o fuzz-testing-presentation.pptx \
  -t pptx

# With metadata
pandoc fuzz-testing-presentation.md \
  -o fuzz-testing-presentation.pptx \
  -t pptx \
  --slide-level=2 \
  -V theme=default

# Open the result
xdg-open fuzz-testing-presentation.pptx  # Linux
open fuzz-testing-presentation.pptx       # macOS
start fuzz-testing-presentation.pptx      # Windows
```
