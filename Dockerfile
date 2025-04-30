# Use an official Python runtime as a parent image
# Changed from 3.9-slim to 3.10-slim to satisfy pyproject.toml requirement
FROM python:3.10-slim

# Set the working directory in the container
WORKDIR /app

# Install uv for dependency management
# We use a specific version to ensure reproducibility of the build process itself
RUN pip install uv==0.2.20

# Copy the dependency file
COPY pyproject.toml ./

# Install dependencies using uv
# uv pip install . reads pyproject.toml and installs the dependencies from the current directory
# --system flag installs into the system site-packages, suitable for Docker
RUN uv pip install --system .

# Copy the rest of the application code
# This assumes your application code is under data_feed_collect/
COPY data_feed_collect/ ./data_feed_collect/

# Command to run the script
# This assumes the script is the main entrypoint for the container
# The script itself handles logging setup and database connection via environment variables
CMD ["python", "data_feed_collect/collectors/yfinance_option_chain.py"]

# Note: This container expects the DATABASE_URL environment variable to be set
# when it is run.
