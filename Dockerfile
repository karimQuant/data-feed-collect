# Use an official Python runtime as a parent image
# Changed from 3.10-slim to 3.11-slim to align with .uv/settings.toml preference
FROM python:3.11-slim

# Set the working directory in the container
WORKDIR /app

# Install uv for dependency management
# We use a specific version to ensure reproducibility of the build process itself
RUN pip install uv==0.2.20

# Copy dependency files and package code needed for installation
# Copy pyproject.toml and README.md
COPY pyproject.toml ./
COPY README.md ./

# Copy the application code directory
# This must be done before uv pip install .
COPY data_feed_collect/ ./data_feed_collect/

# Install dependencies using uv
# uv pip install . reads pyproject.toml and installs the dependencies from the current directory
# --system flag installs into the system site-packages, suitable for Docker
RUN uv pip install --system .

