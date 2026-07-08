# Brand Prompt

Current project assets live in:

- `docs/assets/clio-relay-logo.png`
- `docs/assets/clio-relay-banner.png`

Use this prompt with a stronger image model to create a logo and banner.

```text
Create a logo and GitHub banner for "clio-relay", an open source relay service that connects local research agents to remote clusters.

Context:
- clio-relay is part of the federation layer for clio-agent, but it is usable by any CLI, HTTP, or MCP client.
- It submits remote scientific and engineering work, follows progress, and returns logs, artifacts, and provenance.
- It keeps job state in durable records while network transports only carry bytes.
- It supports cluster work, JARVIS-CD pipelines, SSH forwarding, and frp relay paths.

Visual direction:
- Clear, technical, calm, and trustworthy.
- Show the idea of a local node, a remote cluster, and a clean relay path between them.
- Avoid generic robot faces, chat bubbles, magic sparkles, cloud clip art, and stock AI imagery.
- Avoid heavy gradients and crowded network diagrams.
- Use a simple geometric mark that still works at small sizes.
- The mark should feel suitable for scientific computing and developer infrastructure.

Deliverables:
- Square logo, transparent background, readable at 64 px.
- Wide GitHub banner, 1280 x 640, with the logo and the text "clio-relay".
- Provide both light and dark variants.
- Provide SVG-style vector direction plus raster-ready color guidance.

Palette:
- Neutral base with one clear accent color.
- Prefer deep graphite, off white, and a restrained cyan or green accent.
- Avoid purple-blue AI gradients.
```
