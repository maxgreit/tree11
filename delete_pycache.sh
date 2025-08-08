#!/bin/bash

# Script to delete all __pycache__ directories recursively

echo "Searching for __pycache__ directories..."

# Find and remove __pycache__ directories
find . -type d -name "__pycache__" -exec rm -r {} + 2>/dev/null

echo "All __pycache__ directories have been deleted!"
