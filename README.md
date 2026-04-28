# Fabric Practice

A repository for practicing and experimenting with [Fabric CLI](https://github.com/danielmiessler/fabric) — an open-source framework for augmenting humans using AI.

## Setup

This project uses Fabric CLI with the following providers:
- **GitHub Models** (default: `openai/gpt-4.1`)
- **Microsoft Copilot** (via Azure AD tenant)

## Getting Started

1. Install Fabric CLI: https://github.com/danielmiessler/fabric#installation
2. Run `fabric --setup` to configure your AI provider
3. List patterns: `fabric -l`
4. Try a pattern: `echo "your text" | fabric --pattern summarize`
