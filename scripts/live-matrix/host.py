"""Compatibility imports for the Claude backend and existing evidence readers."""
import sys
from hostdrivers import claude_host

# Preserve patch targets used by existing hermetic controls.
sys.modules[__name__] = claude_host
