#!/usr/bin/env python3
"""Enable SIZE_TRACE logging for the next run."""

import os

# Add to environment or config to enable SIZE_TRACE
print("To enable SIZE_TRACE logging, add this to your environment:")
print("export SIZE_TRACE_ENABLED=1")
print()
print("Or add this line to phone_bot.py after the imports:")
print("SIZE_TRACE_ENABLED = os.getenv('SIZE_TRACE_ENABLED', '0').strip() in ('1', 'true', 'yes')")
print()
print("The SIZE_TRACE events will show the complete sizing pipeline and help identify where the collapse occurs.")
